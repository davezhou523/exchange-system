package runtime

import (
	"testing"
	"time"

	"exchange-system/internal/strategy3/model"
)

func TestCandleStoreUpdate(t *testing.T) {
	store := NewCandleStore()
	start := time.Date(2026, 4, 5, 0, 0, 0, 0, time.UTC)
	candles := candlesFromCloses(start, 15*time.Minute, []float64{100, 101, 102})
	store.Bootstrap("15m", candles[:2], 3)
	store.Update("15m", candles[1])
	store.Update("15m", candles[2])

	snapshot, ok := store.Snapshot()
	if ok {
		t.Fatal("expected incomplete snapshot without h4/h1 bootstrap")
	}

	store.Bootstrap("4h", candlesFromCloses(start, 4*time.Hour, risingSeries(100, 55, 1)), 60)
	store.Bootstrap("1h", candlesFromCloses(start, time.Hour, risingSeries(100, 60, 1)), 65)
	store.Bootstrap("15m", []model.Candle{candles[0], candles[1]}, 3)
	store.Update("15m", candles[2])
	snapshot, ok = store.Snapshot()
	if !ok {
		t.Fatal("expected complete snapshot")
	}
	if len(snapshot.M15) != 3 {
		t.Fatalf("expected 3 m15 candles, got %d", len(snapshot.M15))
	}
}

func candlesFromCloses(start time.Time, step time.Duration, closes []float64) []model.Candle {
	candles := make([]model.Candle, 0, len(closes))
	for i, closePrice := range closes {
		openTime := start.Add(time.Duration(i) * step)
		open := closePrice - 0.4
		high := closePrice + 0.6
		low := closePrice - 0.6
		if i > 0 {
			open = closes[i-1]
		}
		candles = append(candles, model.Candle{
			OpenTime:  openTime,
			CloseTime: openTime.Add(step),
			Open:      open,
			High:      high,
			Low:       low,
			Close:     closePrice,
			Volume:    100,
			Closed:    true,
		})
	}
	return candles
}

func risingSeries(start float64, count int, step float64) []float64 {
	values := make([]float64, 0, count)
	value := start
	for i := 0; i < count; i++ {
		values = append(values, value)
		value += step
	}
	return values
}
