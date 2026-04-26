package marketstate

import (
	"testing"
	"time"
)

func TestAggregateSelectsDominantStateByCount(t *testing.T) {
	now := time.Date(2026, 4, 26, 12, 0, 0, 0, time.UTC)
	got := Aggregate(now, map[string]Result{
		"BTCUSDT": {Symbol: "BTCUSDT", State: MarketStateTrendUp, Confidence: 0.8},
		"ETHUSDT": {Symbol: "ETHUSDT", State: MarketStateTrendUp, Confidence: 0.7},
		"SOLUSDT": {Symbol: "SOLUSDT", State: MarketStateRange, Confidence: 0.9},
	})
	if got.State != MarketStateTrendUp {
		t.Fatalf("state = %s, want trend_up", got.State)
	}
	if got.HealthyCount != 3 || got.UnknownCount != 0 {
		t.Fatalf("healthy=%d unknown=%d, want 3/0", got.HealthyCount, got.UnknownCount)
	}
	if got.StateCounts[string(MarketStateTrendUp)] != 2 {
		t.Fatalf("trend_up count = %d, want 2", got.StateCounts[string(MarketStateTrendUp)])
	}
}

func TestAggregateBreaksTieByConfidenceSum(t *testing.T) {
	now := time.Date(2026, 4, 26, 12, 0, 0, 0, time.UTC)
	got := Aggregate(now, map[string]Result{
		"BTCUSDT": {Symbol: "BTCUSDT", State: MarketStateTrendUp, Confidence: 0.6},
		"ETHUSDT": {Symbol: "ETHUSDT", State: MarketStateRange, Confidence: 0.9},
	})
	if got.State != MarketStateRange {
		t.Fatalf("state = %s, want range", got.State)
	}
}

func TestAggregateReturnsUnknownWhenAllUnknown(t *testing.T) {
	now := time.Date(2026, 4, 26, 12, 0, 0, 0, time.UTC)
	got := Aggregate(now, map[string]Result{
		"BTCUSDT": {Symbol: "BTCUSDT", State: MarketStateUnknown, Confidence: 0},
		"ETHUSDT": {Symbol: "ETHUSDT", State: MarketStateUnknown, Confidence: 0},
	})
	if got.State != MarketStateUnknown {
		t.Fatalf("state = %s, want unknown", got.State)
	}
	if got.Reason != "all_unknown" {
		t.Fatalf("reason = %s, want all_unknown", got.Reason)
	}
	if got.UnknownCount != 2 {
		t.Fatalf("unknown = %d, want 2", got.UnknownCount)
	}
}
