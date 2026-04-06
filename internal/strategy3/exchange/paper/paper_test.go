package paper

import (
	"testing"
	"time"

	"exchange-system/internal/strategy3/model"
)

func TestPaperExchangeApplyDecision(t *testing.T) {
	exchange := New(10000, 10)
	now := time.Date(2026, 4, 5, 12, 0, 0, 0, time.UTC)

	order, account, err := exchange.ApplyDecision("BTCUSDT", model.Decision{
		Action:     model.ActionEnter,
		Side:       model.SideLong,
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

	_, account, err = exchange.ApplyDecision("BTCUSDT", model.Decision{
		Action:      model.ActionExit,
		Side:        model.SideLong,
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
