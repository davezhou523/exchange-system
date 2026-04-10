package aggregator

import (
	"context"
	"log"
	"sync"
	"sync/atomic"
	"time"

	"exchange-system/common/pb/market"
)

// IntervalDef defines a target aggregation interval.
type IntervalDef struct {
	Name     string
	Duration time.Duration
}

// Standard intervals aggregated from 1m klines.
var StandardIntervals = []IntervalDef{
	{Name: "15m", Duration: 15 * time.Minute},
	{Name: "1h", Duration: 1 * time.Hour},
	{Name: "4h", Duration: 4 * time.Hour},
}

// Metrics tracks aggregator statistics.
type Metrics struct {
	Received1m      atomic.Int64
	Emitted15m      atomic.Int64
	Emitted1h       atomic.Int64
	Emitted4h       atomic.Int64
	GapsDetected    atomic.Int64
	KafkaSendErrors atomic.Int64
}

// KlineAggregator aggregates 1m klines into larger intervals.
// Each symbol runs in its own goroutine with its own lock-free state,
// so Kafka IO for one symbol never blocks another.
type KlineAggregator struct {
	intervals []IntervalDef
	producer  KafkaProducer
	metrics   Metrics

	mu      sync.Mutex
	workers map[string]*symbolWorker // symbol -> worker
	wg      sync.WaitGroup
	ctx     context.Context
	cancel  context.CancelFunc
}

// KafkaProducer sends aggregated kline data.
type KafkaProducer interface {
	SendMarketData(ctx context.Context, data interface{}) error
}

// NewKlineAggregator creates a new aggregator for the given target intervals.
func NewKlineAggregator(intervals []IntervalDef, producer KafkaProducer) *KlineAggregator {
	ctx, cancel := context.WithCancel(context.Background())
	a := &KlineAggregator{
		intervals: intervals,
		producer:  producer,
		workers:   make(map[string]*symbolWorker),
		ctx:       ctx,
		cancel:    cancel,
	}
	return a
}

// OnKline dispatches a closed 1m kline to the per-symbol worker goroutine.
func (a *KlineAggregator) OnKline(ctx context.Context, k *market.Kline) {
	if k.Interval != "1m" || !k.IsClosed {
		return
	}
	a.metrics.Received1m.Add(1)

	w := a.getOrCreateWorker(k.Symbol)
	select {
	case w.ch <- k:
	default:
		log.Printf("[aggregator] WARN: %s channel full, dropping 1m kline openTime=%d", k.Symbol, k.OpenTime)
	}
}

// FlushAll waits for all pending klines to be processed, then emits incomplete buckets.
func (a *KlineAggregator) FlushAll(ctx context.Context) {
	// Close all worker channels so goroutines finish current work
	a.mu.Lock()
	for _, w := range a.workers {
		close(w.ch)
	}
	a.mu.Unlock()

	a.wg.Wait()

	// Now emit incomplete buckets
	a.mu.Lock()
	defer a.mu.Unlock()
	for _, w := range a.workers {
		for _, iv := range a.intervals {
			if b, ok := w.buckets[iv.Name]; ok && b.initialized {
				a.emitKline(ctx, w.symbol, iv.Name, b)
			}
		}
	}
}

// GetMetrics returns a snapshot of the current metrics.
func (a *KlineAggregator) GetMetrics() Metrics {
	m := Metrics{}
	m.Received1m.Store(a.metrics.Received1m.Load())
	m.Emitted15m.Store(a.metrics.Emitted15m.Load())
	m.Emitted1h.Store(a.metrics.Emitted1h.Load())
	m.Emitted4h.Store(a.metrics.Emitted4h.Load())
	m.GapsDetected.Store(a.metrics.GapsDetected.Load())
	m.KafkaSendErrors.Store(a.metrics.KafkaSendErrors.Load())
	return m
}

// --- per-symbol worker ---

type symbolWorker struct {
	symbol  string
	ch      chan *market.Kline
	buckets map[string]*bucket // interval name -> bucket
	agg     *KlineAggregator
}

func (a *KlineAggregator) getOrCreateWorker(symbol string) *symbolWorker {
	a.mu.Lock()
	defer a.mu.Unlock()

	if w, ok := a.workers[symbol]; ok {
		return w
	}

	w := &symbolWorker{
		symbol:  symbol,
		ch:      make(chan *market.Kline, 256),
		buckets: make(map[string]*bucket),
		agg:     a,
	}
	a.workers[symbol] = w

	a.wg.Add(1)
	go w.run()

	return w
}

func (w *symbolWorker) run() {
	defer w.agg.wg.Done()

	for k := range w.ch {
		w.processKline(k)
	}
}

func (w *symbolWorker) processKline(k *market.Kline) {
	for _, iv := range w.agg.intervals {
		periodOpenTime := alignToInterval(k.OpenTime, iv.Duration)
		periodCloseTime := periodOpenTime + iv.Duration.Milliseconds() - 1

		b, exists := w.buckets[iv.Name]

		// --- Gap detection ---
		// If the bucket is initialized and the new kline's open time is not
		// the expected next minute, we have a gap.
		if exists && b.initialized {
			expectedNext := b.CloseTime + 1 // next minute after current bucket period
			if k.OpenTime > expectedNext {
				// Gap detected: flush current bucket and log warning
				w.agg.metrics.GapsDetected.Add(1)
				gapDuration := time.Duration(k.OpenTime-expectedNext) * time.Millisecond
				log.Printf("[aggregator] GAP: %s %s — expected openTime=%d got=%d gap=%v",
					w.symbol, iv.Name, expectedNext, k.OpenTime, gapDuration)
				// Flush incomplete bucket (may have missing bars)
				w.agg.emitKline(context.Background(), w.symbol, iv.Name, b)
				delete(w.buckets, iv.Name)
				b = nil
				exists = false
			} else if k.OpenTime < expectedNext {
				// Duplicate or out-of-order, skip
				continue
			}
		}

		// --- Flush completed bucket when period changes ---
		if exists && b.initialized && periodOpenTime != b.OpenTime {
			w.agg.emitKline(context.Background(), w.symbol, iv.Name, b)
			delete(w.buckets, iv.Name)
			b = nil
			exists = false
		}

		// --- Create or update bucket ---
		if !exists || b == nil {
			b = &bucket{
				Open:           k.Open,
				High:           k.High,
				Low:            k.Low,
				Close:          k.Close,
				Volume:         k.Volume,
				QuoteVolume:    k.QuoteVolume,
				TakerBuyVolume: k.TakerBuyVolume,
				TakerBuyQuote:  k.TakerBuyQuote,
				NumTrades:      k.NumTrades,
				FirstTradeID:   k.FirstTradeId,
				LastTradeID:    k.LastTradeId,
				OpenTime:       periodOpenTime,
				CloseTime:      periodCloseTime,
				initialized:    true,
			}
			w.buckets[iv.Name] = b
			continue
		}

		// Update running bucket
		if k.High > b.High {
			b.High = k.High
		}
		if k.Low < b.Low {
			b.Low = k.Low
		}
		b.Close = k.Close
		b.Volume += k.Volume
		b.QuoteVolume += k.QuoteVolume
		b.TakerBuyVolume += k.TakerBuyVolume
		b.TakerBuyQuote += k.TakerBuyQuote
		b.NumTrades += k.NumTrades
		// Guarantee FirstTradeID is always the smallest
		if k.FirstTradeId < b.FirstTradeID {
			b.FirstTradeID = k.FirstTradeId
		}
		// Guarantee LastTradeID is always the largest
		if k.LastTradeId > b.LastTradeID {
			b.LastTradeID = k.LastTradeId
		}
	}
}

// --- bucket ---

type bucket struct {
	Open           float64
	High           float64
	Low            float64
	Close          float64
	Volume         float64
	QuoteVolume    float64
	TakerBuyVolume float64
	TakerBuyQuote  float64
	NumTrades      int32
	FirstTradeID   int64
	LastTradeID    int64
	OpenTime       int64
	CloseTime      int64
	initialized    bool
}

// --- emit (lock-free, called from per-symbol goroutine) ---

func (a *KlineAggregator) emitKline(ctx context.Context, symbol, interval string, b *bucket) {
	k := &market.Kline{
		Symbol:         symbol,
		Interval:       interval,
		OpenTime:       b.OpenTime,
		CloseTime:      b.CloseTime,
		Open:           b.Open,
		High:           b.High,
		Low:            b.Low,
		Close:          b.Close,
		Volume:         b.Volume,
		QuoteVolume:    b.QuoteVolume,
		TakerBuyVolume: b.TakerBuyVolume,
		TakerBuyQuote:  b.TakerBuyQuote,
		NumTrades:      b.NumTrades,
		FirstTradeId:   b.FirstTradeID,
		LastTradeId:    b.LastTradeID,
		IsClosed:       true,
	}

	openStr := time.UnixMilli(k.OpenTime).Format("2006-01-02 15:04:05")
	closeStr := time.UnixMilli(k.CloseTime).Format("15:04:05")
	log.Printf("[aggregated %s] %s | %s-%s | O=%.2f H=%.2f L=%.2f C=%.2f V=%.4f QV=%.4f trades=%d",
		k.Interval, k.Symbol, openStr, closeStr, k.Open, k.High, k.Low, k.Close, k.Volume, k.QuoteVolume, k.NumTrades)

	if err := a.producer.SendMarketData(ctx, k); err != nil {
		a.metrics.KafkaSendErrors.Add(1)
		log.Printf("[aggregator] failed to send %s kline to Kafka: %v", k.Interval, err)
	}

	// Track metric by interval
	switch interval {
	case "15m":
		a.metrics.Emitted15m.Add(1)
	case "1h":
		a.metrics.Emitted1h.Add(1)
	case "4h":
		a.metrics.Emitted4h.Add(1)
	}
}

// alignToInterval truncates a millisecond timestamp to the start of the
// containing interval period.
func alignToInterval(tsMs int64, d time.Duration) int64 {
	dMs := d.Milliseconds()
	return tsMs - (tsMs % dMs)
}
