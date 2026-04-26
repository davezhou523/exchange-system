package strategy

import (
	"testing"

	"exchange-system/app/strategy/rpc/internal/weights"
)

func TestApplyWeightScaleScalesQuantityByPositionBudget(t *testing.T) {
	got, blocked, reason := applyWeightScale(10, weights.Recommendation{
		Symbol:         "BTCUSDT",
		PositionBudget: 0.25,
	})
	if blocked {
		t.Fatalf("blocked = true, want false reason=%s", reason)
	}
	if got != 2.5 {
		t.Fatalf("quantity = %v, want 2.5", got)
	}
}

func TestApplyWeightScaleBlocksWhenTradingPaused(t *testing.T) {
	got, blocked, reason := applyWeightScale(10, weights.Recommendation{
		Symbol:        "BTCUSDT",
		TradingPaused: true,
		PauseReason:   "risk_limit_triggered",
	})
	if !blocked {
		t.Fatal("blocked = false, want true")
	}
	if got != 0 {
		t.Fatalf("quantity = %v, want 0", got)
	}
	if reason != "risk_limit_triggered" {
		t.Fatalf("reason = %s, want risk_limit_triggered", reason)
	}
}

func TestApplyWeightScaleBlocksWhenBudgetTooSmall(t *testing.T) {
	got, blocked, reason := applyWeightScale(0.01, weights.Recommendation{
		Symbol:         "BTCUSDT",
		PositionBudget: 0.05,
	})
	if !blocked {
		t.Fatal("blocked = false, want true")
	}
	if got != 0 {
		t.Fatalf("quantity = %v, want 0", got)
	}
	if reason != "weight_budget_too_small" {
		t.Fatalf("reason = %s, want weight_budget_too_small", reason)
	}
}
