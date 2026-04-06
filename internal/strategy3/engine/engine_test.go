package engine

import (
	"testing"
	"time"

	"exchange-system/internal/strategy3/model"
)

func TestEvaluateEnterLong(t *testing.T) {
	engine := New(DefaultParams())
	now := time.Date(2026, 4, 5, 12, 0, 0, 0, time.UTC)

	h4 := candlesFromCloses(now.Add(-4*time.Hour*59), 4*time.Hour, []float64{
		100, 101, 102, 103, 104, 105, 106, 107, 108, 109,
		110, 111, 112, 113, 114, 115, 116, 117, 118, 119,
		120, 121, 122, 123, 124, 125, 126, 127, 128, 129,
		130, 131, 132, 133, 134, 135, 136, 137, 138, 139,
		140, 141, 142, 143, 144, 145, 146, 147, 148, 149,
		150, 151, 152, 153, 154, 155, 156, 157, 158,
	})
	h1 := candlesFromCloses(now.Add(-time.Hour*64), time.Hour, []float64{
		100, 101, 102, 103, 104, 105, 106, 107, 108, 109,
		110, 111, 112, 113, 114, 115, 116, 117, 118, 119,
		120, 121, 122, 123, 124, 125, 126, 127, 128, 129,
		130, 131, 132, 133, 134, 135, 136, 137, 138, 139,
		140, 141, 142, 143, 144, 145, 146, 147, 148, 149,
		150, 151, 152, 153, 154, 155, 156, 157, 158, 159,
		160, 155, 151, 150, 149.8,
	})
	m15 := candlesFromCloses(now.Add(-15*time.Minute*24), 15*time.Minute, []float64{
		150, 150.3, 150.6, 150.9, 151.2, 151.5, 151.8, 152.1,
		152.4, 152.7, 153.0, 153.3, 153.6, 153.9, 154.2, 154.5,
		154.8, 155.1, 155.4, 155.7, 156.0, 156.3, 156.6, 157.5,
	})

	decision, err := engine.Evaluate(model.Snapshot{
		Symbol:    "BTCUSDT",
		H4:        h4,
		H1:        h1,
		M15:       m15,
		Timestamp: m15[len(m15)-1].CloseTime,
	}, model.State{}, model.Account{Equity: 10000, AvailableCash: 10000})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if decision.Action != model.ActionEnter {
		t.Fatalf("expected enter, got %s with reason %s", decision.Action, decision.Reason)
	}
	if decision.Side != model.SideLong {
		t.Fatalf("expected long, got %s", decision.Side)
	}
	if decision.Quantity <= 0 {
		t.Fatalf("expected positive quantity, got %.4f", decision.Quantity)
	}
}

func TestEvaluateStopLossExit(t *testing.T) {
	engine := New(DefaultParams())
	now := time.Date(2026, 4, 5, 12, 0, 0, 0, time.UTC)
	m15 := candlesFromCloses(now.Add(-15*time.Minute*24), 15*time.Minute, []float64{
		150, 150.3, 150.6, 150.9, 151.2, 151.5, 151.8, 152.1,
		152.4, 152.7, 153.0, 153.3, 153.6, 153.9, 154.2, 154.5,
		154.8, 155.1, 155.4, 155.7, 156.0, 156.3, 156.6, 156.8,
	})
	m15[len(m15)-1].Low = 147
	h4 := candlesFromCloses(now.Add(-4*time.Hour*59), 4*time.Hour, risingSeries(100, 59, 1))
	h1 := candlesFromCloses(now.Add(-time.Hour*64), time.Hour, risingSeries(100, 65, 1))
	state := model.State{
		Position: &model.Position{
			Side:         model.SideLong,
			Quantity:     1,
			EntryPrice:   156,
			StopLoss:     148,
			StopDistance: 8,
			TakeProfits:  []float64{164, 172},
			OpenedAt:     now.Add(-10 * 15 * time.Minute),
			LastBarTime:  m15[len(m15)-2].CloseTime,
		},
	}
	decision, err := engine.Evaluate(model.Snapshot{
		Symbol:    "BTCUSDT",
		H4:        h4,
		H1:        h1,
		M15:       m15,
		Timestamp: m15[len(m15)-1].CloseTime,
	}, state, model.Account{Equity: 10000, AvailableCash: 10000})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if decision.Action != model.ActionExit {
		t.Fatalf("expected exit, got %s", decision.Action)
	}
	if decision.RealizedPnL >= 0 {
		t.Fatalf("expected negative pnl, got %.2f", decision.RealizedPnL)
	}
	if decision.UpdatedState.Position != nil {
		t.Fatal("expected position to be cleared")
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
