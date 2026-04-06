package strategy3

import (
	"testing"
	"time"
)

func TestPaperExchangeApplyDecision(t *testing.T) {
	exchange := NewPaperExchange(10000, 10)
	now := time.Date(2026, 4, 5, 12, 0, 0, 0, time.UTC)

	order, account, err := exchange.ApplyDecision("BTCUSDT", Decision{
		Action:     ActionEnter,
		Side:       SideLong,
		Quantity:   1,
		EntryPrice: 2000,
	}, now)
	if err != nil {
		t.Fatalf("unexpected error on enter: %v", err)
	}
	if order.Status != "filled" {
		t.Fatalf("expected filled order, got %s", order.Status)
	}
	if account.Position == nil {
		t.Fatal("expected position after enter")
	}
	if account.AvailableBalance >= account.WalletBalance {
		t.Fatal("expected margin to be reserved after enter")
	}

	account = exchange.SyncMarkPrice("BTCUSDT", 2050, now.Add(time.Minute))
	if account.UnrealizedPnL <= 0 {
		t.Fatalf("expected positive unrealized pnl, got %.2f", account.UnrealizedPnL)
	}

	_, account, err = exchange.ApplyDecision("BTCUSDT", Decision{
		Action:      ActionExit,
		Side:        SideLong,
		Quantity:    1,
		EntryPrice:  2050,
		RealizedPnL: 50,
	}, now.Add(2*time.Minute))
	if err != nil {
		t.Fatalf("unexpected error on exit: %v", err)
	}
	if account.Position != nil {
		t.Fatal("expected position to be closed")
	}
	if account.WalletBalance <= 10000 {
		t.Fatalf("expected wallet balance to increase, got %.2f", account.WalletBalance)
	}
}

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
	store.Bootstrap("15m", []Candle{candles[0], candles[1]}, 3)
	store.Update("15m", candles[2])
	snapshot, ok = store.Snapshot()
	if !ok {
		t.Fatal("expected complete snapshot")
	}
	if len(snapshot.M15) != 3 {
		t.Fatalf("expected 3 m15 candles, got %d", len(snapshot.M15))
	}
}
