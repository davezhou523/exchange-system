package aggregator

import (
	"context"
	"encoding/json"
	"log"
	"os"
	"path/filepath"
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
	{Name: "3m", Duration: 3 * time.Minute},
	{Name: "15m", Duration: 15 * time.Minute},
	{Name: "1h", Duration: 1 * time.Hour},
	{Name: "4h", Duration: 4 * time.Hour},
}

// Metrics tracks aggregator statistics.
type Metrics struct {
	Received1m      atomic.Int64
	Emitted3m       atomic.Int64
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
	intervals   []IntervalDef
	producer    KafkaProducer
	metrics     Metrics
	klineLogDir string

	mu      sync.Mutex
	workers map[string]*symbolWorker // symbol -> worker
	wg      sync.WaitGroup
	ctx     context.Context
	cancel  context.CancelFunc

	logMu   sync.Mutex
	logFile map[string]*os.File // "SYMBOL/INTERVAL/DATE" -> file handle
}

// KafkaProducer sends aggregated kline data.
type KafkaProducer interface {
	SendMarketData(ctx context.Context, data interface{}) error
}

// NewKlineAggregator creates a new aggregator for the given target intervals.
func NewKlineAggregator(intervals []IntervalDef, producer KafkaProducer, klineLogDir string) *KlineAggregator {
	ctx, cancel := context.WithCancel(context.Background())
	a := &KlineAggregator{
		intervals:   intervals,
		producer:    producer,
		workers:     make(map[string]*symbolWorker),
		logFile:     make(map[string]*os.File),
		ctx:         ctx,
		cancel:      cancel,
		klineLogDir: klineLogDir,
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

	// Close all log files
	a.logMu.Lock()
	for key, f := range a.logFile {
		_ = f.Close()
		delete(a.logFile, key)
	}
	a.logMu.Unlock()
}

// GetMetrics returns a snapshot of the current metrics.
func (a *KlineAggregator) GetMetrics() Metrics {
	m := Metrics{}
	m.Received1m.Store(a.metrics.Received1m.Load())
	m.Emitted3m.Store(a.metrics.Emitted3m.Load())
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
	buckets map[string]*bucket // interval name -> bucket (incremental aggregation)
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
	// Process each target interval with O(1) incremental aggregation
	for _, iv := range w.agg.intervals {
		requiredMinutes := int(iv.Duration / time.Minute)

		if requiredMinutes == 1 {
			continue
		}

		// Calculate current period based on the latest kline's openTime
		latestPeriodOpenTime := alignToInterval(k.OpenTime, iv.Duration)

		// Check if we already have a bucket for this period
		b, exists := w.buckets[iv.Name]

		// --- Time-driven: flush bucket when kline crosses period boundary ---
		if exists && b.initialized && latestPeriodOpenTime != b.OpenTime {
			// Previous period is complete (time-wise), emit it
			if b.count < requiredMinutes {
				b.dirty = true
				log.Printf("[aggregator] INCOMPLETE: %s %s | period=%s | collected=%d/%d | missing=%d | dirty=true",
					w.symbol, iv.Name,
					time.UnixMilli(b.OpenTime).UTC().Format("2006-01-02T15:04:05"),
					b.count, requiredMinutes, requiredMinutes-b.count)
			}
			w.agg.emitKline(context.Background(), w.symbol, iv.Name, b)
			delete(w.buckets, iv.Name)
			b = nil
			exists = false
		}

		// --- Gap detection: kline jumps far ahead (more than one period) ---
		if exists && b.initialized {
			expectedNext := b.CloseTime + 1
			if k.OpenTime > expectedNext {
				w.agg.metrics.GapsDetected.Add(1)
				gapDuration := time.Duration(k.OpenTime-expectedNext) * time.Millisecond
				log.Printf("[aggregator] GAP: %s %s — expected openTime=%d got=%d gap=%v",
					w.symbol, iv.Name, expectedNext, k.OpenTime, gapDuration)
				if b.count > 0 {
					b.dirty = true
					w.agg.emitKline(context.Background(), w.symbol, iv.Name, b)
				}
				delete(w.buckets, iv.Name)
				b = nil
				exists = false
			}
		}

		// --- Continuity check within current period: verify consecutive 1m klines ---
		if exists && b.initialized && b.count > 0 {
			expectedOpenTime := b.prevOpenTime + 60000
			if k.OpenTime != expectedOpenTime {
				w.agg.metrics.GapsDetected.Add(1)
				log.Printf("[aggregator] CONTINUITY GAP: %s %s — expected openTime=%d got=%d gap=%dms | marking dirty",
					w.symbol, iv.Name, expectedOpenTime, k.OpenTime, k.OpenTime-expectedOpenTime)
				// Mark dirty but continue aggregating (data layer can still emit, strategy layer should skip)
				b.dirty = true
			}
		}

		// --- Create or incrementally update bucket ---
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
				OpenTime:       latestPeriodOpenTime,
				CloseTime:      latestPeriodOpenTime + iv.Duration.Milliseconds() - 1,
				prevOpenTime:   k.OpenTime,
				count:          1,
				initialized:    true,
			}
			w.buckets[iv.Name] = b
		} else {
			// Incremental update: O(1) per kline per interval
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
			if k.FirstTradeId < b.FirstTradeID {
				b.FirstTradeID = k.FirstTradeId
			}
			if k.LastTradeId > b.LastTradeID {
				b.LastTradeID = k.LastTradeId
			}
			b.prevOpenTime = k.OpenTime
			b.count++
		}
	}
}

// klineLogEntry mirrors the protobuf field order for deterministic JSON output.
type klineLogEntry struct {
	Symbol         string  `json:"symbol"`
	Interval       string  `json:"interval"`
	OpenTime       string  `json:"openTime"`
	Open           float64 `json:"open"`
	High           float64 `json:"high"`
	Low            float64 `json:"low"`
	Close          float64 `json:"close"`
	Volume         float64 `json:"volume"`
	CloseTime      string  `json:"closeTime"`
	IsClosed       bool    `json:"isClosed"`
	EventTime      string  `json:"eventTime"`
	FirstTradeId   int64   `json:"firstTradeId"`
	LastTradeId    int64   `json:"lastTradeId"`
	NumTrades      int32   `json:"numTrades"`
	QuoteVolume    float64 `json:"quoteVolume"`
	TakerBuyVolume float64 `json:"takerBuyVolume"`
	TakerBuyQuote  float64 `json:"takerBuyQuote"`
	IsDirty        bool    `json:"isDirty"`
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
	prevOpenTime   int64 // openTime of the last 1m kline added (for continuity check)
	count          int   // number of 1m klines aggregated so far
	dirty          bool  // true if bucket has gaps or incomplete data
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
		IsDirty:        b.dirty,
		EventTime:      b.CloseTime, // Use closeTime as eventTime for aggregated klines (period confirmation time)
	}

	openStr := time.UnixMilli(k.OpenTime).Format("2006-01-02 15:04:05")
	closeStr := time.UnixMilli(k.CloseTime).Format("15:04:05")
	log.Printf("[aggregated %s] %s | %s-%s | O=%.2f H=%.2f L=%.2f C=%.2f V=%.4f QV=%.4f trades=%d",
		k.Interval, k.Symbol, openStr, closeStr, k.Open, k.High, k.Low, k.Close, k.Volume, k.QuoteVolume, k.NumTrades)

	// Persist to jsonl file for verification
	a.writeKlineLog(k)

	if err := a.producer.SendMarketData(ctx, k); err != nil {
		a.metrics.KafkaSendErrors.Add(1)
		log.Printf("[aggregator] failed to send %s kline to Kafka: %v", k.Interval, err)
	}

	// Track metrics
	switch interval {
	case "3m":
		a.metrics.Emitted3m.Add(1)
	case "15m":
		a.metrics.Emitted15m.Add(1)
	case "1h":
		a.metrics.Emitted1h.Add(1)
	case "4h":
		a.metrics.Emitted4h.Add(1)
	}
}

// formatFloat formats a float64 to string with 2 decimal places.
func formatFloat(f float64) float64 {
	return float64(int64(f*100+0.5)) / 100
}

// writeKlineLog appends an aggregated kline as JSON line to a daily log file.
// Format: data/kline/ETHUSDT/3m/2026-04-11.jsonl
func (a *KlineAggregator) writeKlineLog(k *market.Kline) {
	if a.klineLogDir == "" {
		return
	}

	// Round float fields to 2 decimal places for cleaner logs
	k.Volume = formatFloat(k.Volume)
	k.QuoteVolume = formatFloat(k.QuoteVolume)
	k.TakerBuyVolume = formatFloat(k.TakerBuyVolume)
	k.TakerBuyQuote = formatFloat(k.TakerBuyQuote)
	k.Open = formatFloat(k.Open)
	k.High = formatFloat(k.High)
	k.Low = formatFloat(k.Low)
	k.Close = formatFloat(k.Close)

	dateStr := time.UnixMilli(k.CloseTime).Format("2006-01-02")
	dir := filepath.Join(a.klineLogDir, k.Symbol, k.Interval)

	a.logMu.Lock()
	defer a.logMu.Unlock()

	key := k.Symbol + "/" + k.Interval + "/" + dateStr
	f, ok := a.logFile[key]
	if !ok {
		if err := os.MkdirAll(dir, 0o755); err != nil {
			log.Printf("[agg-log] failed to create dir %s: %v", dir, err)
			return
		}
		path := filepath.Join(dir, dateStr+".jsonl")
		var err error
		f, err = os.OpenFile(path, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o644)
		if err != nil {
			log.Printf("[agg-log] failed to open %s: %v", path, err)
			return
		}
		a.logFile[key] = f
	}

	entry := klineLogEntry{
		Symbol:         k.Symbol,
		Interval:       k.Interval,
		OpenTime:       time.UnixMilli(k.OpenTime).UTC().Format("2006-01-02T15:04:05.000Z"),
		Open:           k.Open,
		High:           k.High,
		Low:            k.Low,
		Close:          k.Close,
		Volume:         k.Volume,
		CloseTime:      time.UnixMilli(k.CloseTime).UTC().Format("2006-01-02T15:04:05.000Z"),
		IsClosed:       k.IsClosed,
		EventTime:      time.UnixMilli(k.EventTime).UTC().Format("2006-01-02T15:04:05.000Z"),
		FirstTradeId:   k.FirstTradeId,
		LastTradeId:    k.LastTradeId,
		NumTrades:      k.NumTrades,
		QuoteVolume:    k.QuoteVolume,
		TakerBuyVolume: k.TakerBuyVolume,
		TakerBuyQuote:  k.TakerBuyQuote,
		IsDirty:        k.IsDirty,
	}
	data, err := json.Marshal(entry)
	if err != nil {
		log.Printf("[agg-log] marshal failed: %v", err)
		return
	}
	if _, err := f.Write(append(data, '\n')); err != nil {
		log.Printf("[agg-log] write failed: %v", err)
	}
}

// alignToInterval truncates a millisecond timestamp to the start of the
// containing interval period.
func alignToInterval(tsMs int64, d time.Duration) int64 {
	dMs := d.Milliseconds()
	return tsMs - (tsMs % dMs)
}
