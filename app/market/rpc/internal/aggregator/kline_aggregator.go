package aggregator

import (
	"context"
	"log"
	"sync"
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

// KlineAggregator aggregates 1m klines into larger intervals.
type KlineAggregator struct {
	mu        sync.Mutex
	intervals []IntervalDef
	buckets   map[string]map[string]*bucket // key: symbol -> interval -> bucket
	producer  KafkaProducer
}

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

// KafkaProducer sends aggregated kline data.
type KafkaProducer interface {
	SendMarketData(ctx context.Context, data interface{}) error
}

// NewKlineAggregator creates a new aggregator for the given target intervals.
func NewKlineAggregator(intervals []IntervalDef, producer KafkaProducer) *KlineAggregator {
	return &KlineAggregator{
		intervals: intervals,
		buckets:   make(map[string]map[string]*bucket),
		producer:  producer,
	}
}

// OnKline processes a closed 1m kline and emits aggregated klines when a
// higher timeframe bar completes.
func (a *KlineAggregator) OnKline(ctx context.Context, k *market.Kline) {
	if k.Interval != "1m" || !k.IsClosed {
		return
	}

	a.mu.Lock()
	defer a.mu.Unlock()

	for _, iv := range a.intervals {
		periodOpenTime := alignToInterval(k.OpenTime, iv.Duration)
		periodCloseTime := periodOpenTime + iv.Duration.Milliseconds() - 1

		key := k.Symbol
		if a.buckets[key] == nil {
			a.buckets[key] = make(map[string]*bucket)
		}

		b := a.buckets[key][iv.Name]

		// Check if the bucket needs to be flushed (new period)
		if b != nil && b.initialized && periodOpenTime != b.OpenTime {
			a.emit(ctx, k.Symbol, iv.Name, b)
			b = nil
		}

		if b == nil {
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
			a.buckets[key][iv.Name] = b
			continue
		}

		// Update the running bucket
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
		b.LastTradeID = k.LastTradeId
	}
}

// FlushAll emits any pending buckets (useful on shutdown).
func (a *KlineAggregator) FlushAll(ctx context.Context) {
	a.mu.Lock()
	defer a.mu.Unlock()

	for symbol, intervals := range a.buckets {
		for ivName, b := range intervals {
			if b.initialized {
				a.emit(ctx, symbol, ivName, b)
			}
		}
	}
}

func (a *KlineAggregator) emit(ctx context.Context, symbol, interval string, b *bucket) {
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
		log.Printf("[aggregator] failed to send %s kline to Kafka: %v", k.Interval, err)
	}

	// Reset bucket
	b.initialized = false
}

// alignToInterval truncates a millisecond timestamp to the start of the
// containing interval period.
func alignToInterval(tsMs int64, d time.Duration) int64 {
	dMs := d.Milliseconds()
	return tsMs - (tsMs % dMs)
}
