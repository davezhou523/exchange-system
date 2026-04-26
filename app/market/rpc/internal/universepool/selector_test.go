package universepool

import (
	"testing"
	"time"
)

func TestBasicSelectorEvaluatePrefersTrendSymbols(t *testing.T) {
	now := time.Date(2026, 4, 26, 12, 0, 0, 0, time.UTC)
	selector := NewBasicSelector(Config{
		CandidateSymbols:         []string{"ETHUSDT", "SOLUSDT", "XRPUSDT"},
		TrendPreferredSymbols:    []string{"ETHUSDT", "SOLUSDT"},
		RangePreferredSymbols:    []string{"BTCUSDT", "ETHUSDT"},
		BreakoutPreferredSymbols: []string{"BTCUSDT", "SOLUSDT"},
		EvaluateInterval:         30 * time.Second,
		AddScoreThreshold:        0.75,
	})

	snapshots := map[string]Snapshot{
		"ETHUSDT": {Symbol: "ETHUSDT", UpdatedAt: now.Add(-10 * time.Second), LastPrice: 100, Ema21: 99, Ema55: 98, Atr: 0.2, AtrPct: 0.002, Healthy: true},
		"SOLUSDT": {Symbol: "SOLUSDT", UpdatedAt: now.Add(-10 * time.Second), LastPrice: 100, Ema21: 99, Ema55: 98, Atr: 0.2, AtrPct: 0.002, Healthy: true},
		"XRPUSDT": {Symbol: "XRPUSDT", UpdatedAt: now.Add(-10 * time.Second), LastPrice: 100, Ema21: 99, Ema55: 98, Atr: 0.2, AtrPct: 0.002, Healthy: true},
	}

	got := selector.Evaluate(now, snapshots)
	if got.GlobalState != "trend" || got.TrendCount != 3 || got.RangeCount != 0 || got.BreakoutCount != 0 {
		t.Fatalf("global state = %s trend=%d range=%d breakout=%d, want trend/3/0/0", got.GlobalState, got.TrendCount, got.RangeCount, got.BreakoutCount)
	}
	if !got.Symbols["ETHUSDT"].Desired || got.Symbols["ETHUSDT"].Reason != "state_preferred_score_pass" {
		t.Fatalf("ETHUSDT = %+v, want desired state_preferred_score_pass", got.Symbols["ETHUSDT"])
	}
	if !got.Symbols["SOLUSDT"].Desired || got.Symbols["SOLUSDT"].Reason != "state_preferred_score_pass" {
		t.Fatalf("SOLUSDT = %+v, want desired state_preferred_score_pass", got.Symbols["SOLUSDT"])
	}
	if got.Symbols["XRPUSDT"].Desired || got.Symbols["XRPUSDT"].Reason != "state_filtered" {
		t.Fatalf("XRPUSDT = %+v, want filtered", got.Symbols["XRPUSDT"])
	}
}

func TestBasicSelectorEvaluateFallsBackWhenNoPreferredConfig(t *testing.T) {
	now := time.Date(2026, 4, 26, 12, 0, 0, 0, time.UTC)
	selector := &BasicSelector{cfg: normalizeConfig(Config{
		CandidateSymbols:  []string{"ETHUSDT"},
		EvaluateInterval:  30 * time.Second,
		AddScoreThreshold: 0.75,
	})}

	got := selector.Evaluate(now, map[string]Snapshot{
		"ETHUSDT": {
			Symbol: "ETHUSDT", UpdatedAt: now.Add(-10 * time.Second),
			LastPrice: 100, Healthy: true,
		},
	})
	if !got.Symbols["ETHUSDT"].Desired || got.Symbols["ETHUSDT"].Reason != "score_pass" {
		t.Fatalf("ETHUSDT = %+v, want desired score_pass", got.Symbols["ETHUSDT"])
	}
}

func TestBasicSelectorEvaluateValidationMode1mPrefersRangeSooner(t *testing.T) {
	now := time.Date(2026, 4, 26, 12, 0, 0, 0, time.UTC)
	selector := NewBasicSelector(Config{
		ValidationMode:        "1m",
		CandidateSymbols:      []string{"BTCUSDT", "ETHUSDT", "XRPUSDT"},
		RangePreferredSymbols: []string{"BTCUSDT", "ETHUSDT"},
		EvaluateInterval:      30 * time.Second,
		AddScoreThreshold:     0.75,
	})

	got := selector.Evaluate(now, map[string]Snapshot{
		"BTCUSDT": {Symbol: "BTCUSDT", UpdatedAt: now.Add(-5 * time.Second), LastPrice: 100, Ema21: 100.1, Ema55: 100.0, AtrPct: 0.0022, Healthy: true},
		"ETHUSDT": {Symbol: "ETHUSDT", UpdatedAt: now.Add(-5 * time.Second), LastPrice: 100, Ema21: 100.1, Ema55: 100.0, AtrPct: 0.0021, Healthy: true},
		"XRPUSDT": {Symbol: "XRPUSDT", UpdatedAt: now.Add(-5 * time.Second), LastPrice: 100, Ema21: 100.1, Ema55: 100.0, AtrPct: 0.0020, Healthy: true},
	})
	if got.GlobalState != "range" || got.RangeCount != 3 {
		t.Fatalf("global state = %s range=%d, want range/3", got.GlobalState, got.RangeCount)
	}
	if got.Symbols["XRPUSDT"].Reason != "state_filtered" {
		t.Fatalf("XRPUSDT = %+v, want state_filtered", got.Symbols["XRPUSDT"])
	}
}

func TestBasicSelectorEvaluateValidationMode5mUsesSteadierThresholds(t *testing.T) {
	now := time.Date(2026, 4, 26, 12, 0, 0, 0, time.UTC)
	selector := NewBasicSelector(Config{
		ValidationMode:        "5m",
		CandidateSymbols:      []string{"BTCUSDT", "ETHUSDT", "XRPUSDT"},
		RangePreferredSymbols: []string{"BTCUSDT", "ETHUSDT"},
		EvaluateInterval:      30 * time.Second,
		AddScoreThreshold:     0.75,
	})

	got := selector.Evaluate(now, map[string]Snapshot{
		"BTCUSDT": {Symbol: "BTCUSDT", UpdatedAt: now.Add(-5 * time.Second), LastPrice: 100, Ema21: 100.1, Ema55: 100.0, AtrPct: 0.0021, Healthy: true},
		"ETHUSDT": {Symbol: "ETHUSDT", UpdatedAt: now.Add(-5 * time.Second), LastPrice: 100, Ema21: 100.1, Ema55: 100.0, AtrPct: 0.0020, Healthy: true},
		"XRPUSDT": {Symbol: "XRPUSDT", UpdatedAt: now.Add(-5 * time.Second), LastPrice: 100, Ema21: 100.1, Ema55: 100.0, AtrPct: 0.0021, Healthy: true},
	})
	if got.GlobalState != "range" || got.RangeCount != 3 {
		t.Fatalf("global state = %s range=%d, want range/3", got.GlobalState, got.RangeCount)
	}
}
