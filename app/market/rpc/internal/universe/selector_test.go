package universe

import (
	"testing"
	"time"

	"exchange-system/app/market/rpc/internal/marketstate"
	"exchange-system/app/market/rpc/internal/strategyrouter"
	"exchange-system/common/featureengine"
	"exchange-system/common/regimejudge"
)

func TestSelectorEvaluate(t *testing.T) {
	now := time.Date(2026, 4, 26, 7, 0, 0, 0, time.UTC)
	selector := NewSelector(Config{
		CandidateSymbols: []string{"BTCUSDT", "SOLUSDT"},
		RouterConfig: strategyrouter.Config{
			StaticTemplateMap: map[string]string{
				"BTCUSDT": "btc-core",
				"SOLUSDT": "high-beta",
			},
		},
		FreshnessWindow: 3 * time.Minute,
		RequireFinal:    true,
		RequireTradable: true,
		RequireClean:    true,
	})

	got := selector.Evaluate(now, map[string]Snapshot{
		"BTCUSDT": {
			Symbol:      "BTCUSDT",
			UpdatedAt:   now.Add(-time.Minute),
			IsTradable:  true,
			IsFinal:     true,
			IsDirty:     false,
			LastEventMs: now.UnixMilli(),
		},
		"SOLUSDT": {
			Symbol:      "SOLUSDT",
			UpdatedAt:   now.Add(-time.Minute),
			IsTradable:  true,
			IsFinal:     true,
			IsDirty:     true,
			LastEventMs: now.UnixMilli(),
		},
	})

	if len(got) != 2 {
		t.Fatalf("Evaluate() len = %d, want 2", len(got))
	}
	if !got[0].Enabled || got[0].BaseTemplate != "btc-core" || got[0].Template != "btc-core" || got[0].Reason != "healthy_data" {
		t.Fatalf("Evaluate() BTC = %+v, want enabled base=btc-core template=btc-core healthy_data", got[0])
	}
	if got[1].Enabled || got[1].Reason != "dirty_data" {
		t.Fatalf("Evaluate() SOL = %+v, want disabled dirty_data", got[1])
	}
}

func TestSelectorEvaluateStaleSnapshot(t *testing.T) {
	now := time.Date(2026, 4, 26, 7, 0, 0, 0, time.UTC)
	selector := NewSelector(Config{
		CandidateSymbols: []string{"ETHUSDT"},
		RouterConfig: strategyrouter.Config{
			StaticTemplateMap: map[string]string{
				"ETHUSDT": "eth-core",
			},
		},
		FreshnessWindow: 3 * time.Minute,
		RequireFinal:    true,
		RequireTradable: true,
		RequireClean:    true,
	})

	got := selector.Evaluate(now, map[string]Snapshot{
		"ETHUSDT": {
			Symbol:      "ETHUSDT",
			UpdatedAt:   now.Add(-10 * time.Minute),
			IsTradable:  true,
			IsFinal:     true,
			IsDirty:     false,
			LastEventMs: now.Add(-10 * time.Minute).UnixMilli(),
		},
	})

	if len(got) != 1 {
		t.Fatalf("Evaluate() len = %d, want 1", len(got))
	}
	if got[0].Enabled || got[0].Reason != "stale_data" {
		t.Fatalf("Evaluate() = %+v, want disabled stale_data", got[0])
	}
}

func TestSelectorEvaluateBTCTrendTemplate(t *testing.T) {
	now := time.Date(2026, 4, 26, 7, 0, 0, 0, time.UTC)
	selector := NewSelector(Config{
		CandidateSymbols: []string{"BTCUSDT"},
		RouterConfig: strategyrouter.Config{
			StaticTemplateMap: map[string]string{
				"BTCUSDT": "btc-core",
			},
			BTCTrendTemplate:  "btc-trend",
			BTCTrendAtrPctMax: 0.003,
		},
		FreshnessWindow: 3 * time.Minute,
		RequireFinal:    true,
		RequireTradable: true,
		RequireClean:    true,
	})

	got := selector.Evaluate(now, map[string]Snapshot{
		"BTCUSDT": {
			Symbol:      "BTCUSDT",
			UpdatedAt:   now.Add(-time.Minute),
			IsTradable:  true,
			IsFinal:     true,
			IsDirty:     false,
			LastEventMs: now.UnixMilli(),
			Close:       100,
			Atr:         0.2,
			Ema21:       99,
			Ema55:       98,
		},
	})

	if len(got) != 1 {
		t.Fatalf("Evaluate() len = %d, want 1", len(got))
	}
	if !got[0].Enabled || got[0].BaseTemplate != "btc-core" || got[0].Template != "btc-trend" || got[0].Reason != "trend_strong" {
		t.Fatalf("Evaluate() = %+v, want enabled base=btc-core template=btc-trend trend_strong", got[0])
	}
}

func TestSelectorEvaluateBTCTrendTemplateWithMarketState(t *testing.T) {
	now := time.Date(2026, 4, 26, 7, 0, 0, 0, time.UTC)
	selector := NewSelector(Config{
		CandidateSymbols: []string{"BTCUSDT"},
		RouterConfig: strategyrouter.Config{
			StaticTemplateMap: map[string]string{
				"BTCUSDT": "btc-core",
			},
			BTCTrendTemplate:  "btc-trend",
			BTCTrendAtrPctMax: 0.003,
		},
		FreshnessWindow: 3 * time.Minute,
		RequireFinal:    true,
		RequireTradable: true,
		RequireClean:    true,
	})

	got := selector.Evaluate(now, map[string]Snapshot{
		"BTCUSDT": {
			Symbol:      "BTCUSDT",
			UpdatedAt:   now.Add(-time.Minute),
			IsTradable:  true,
			IsFinal:     true,
			IsDirty:     false,
			LastEventMs: now.UnixMilli(),
			Close:       100,
			Atr:         0.2,
			Ema21:       99,
			Ema55:       98,
			MarketState: marketstate.MarketStateTrendUp,
		},
	})

	if len(got) != 1 {
		t.Fatalf("Evaluate() len = %d, want 1", len(got))
	}
	if !got[0].Enabled || got[0].BaseTemplate != "btc-core" || got[0].Template != "btc-trend" || got[0].Reason != "market_state_trend" {
		t.Fatalf("Evaluate() = %+v, want enabled base=btc-core template=btc-trend market_state_trend", got[0])
	}
}

func TestSelectorEvaluateBreakoutTemplateWithMarketState(t *testing.T) {
	now := time.Date(2026, 4, 26, 7, 0, 0, 0, time.UTC)
	selector := NewSelector(Config{
		CandidateSymbols: []string{"ETHUSDT"},
		RouterConfig: strategyrouter.Config{
			StaticTemplateMap: map[string]string{
				"ETHUSDT": "eth-core",
			},
			BreakoutTemplate: "breakout-core",
		},
		FreshnessWindow: 3 * time.Minute,
		RequireFinal:    true,
		RequireTradable: true,
		RequireClean:    true,
	})

	got := selector.Evaluate(now, map[string]Snapshot{
		"ETHUSDT": {
			Symbol:      "ETHUSDT",
			UpdatedAt:   now.Add(-time.Minute),
			IsTradable:  true,
			IsFinal:     true,
			IsDirty:     false,
			LastEventMs: now.UnixMilli(),
			Close:       100,
			Atr:         1,
			Ema21:       99,
			Ema55:       98,
			MarketState: marketstate.MarketStateBreakout,
		},
	})

	if len(got) != 1 {
		t.Fatalf("Evaluate() len = %d, want 1", len(got))
	}
	if !got[0].Enabled || got[0].BaseTemplate != "eth-core" || got[0].Template != "breakout-core" || got[0].Reason != "market_state_breakout" {
		t.Fatalf("Evaluate() = %+v, want enabled base=eth-core template=breakout-core market_state_breakout", got[0])
	}
}

func TestSelectorEvaluateRangeTemplateWithMarketState(t *testing.T) {
	now := time.Date(2026, 4, 26, 7, 0, 0, 0, time.UTC)
	selector := NewSelector(Config{
		CandidateSymbols: []string{"ETHUSDT"},
		RouterConfig: strategyrouter.Config{
			StaticTemplateMap: map[string]string{
				"ETHUSDT": "eth-core",
			},
			RangeTemplate: "range-core",
		},
		FreshnessWindow: 3 * time.Minute,
		RequireFinal:    true,
		RequireTradable: true,
		RequireClean:    true,
	})

	got := selector.Evaluate(now, map[string]Snapshot{
		"ETHUSDT": {
			Symbol:      "ETHUSDT",
			UpdatedAt:   now.Add(-time.Minute),
			IsTradable:  true,
			IsFinal:     true,
			IsDirty:     false,
			LastEventMs: now.UnixMilli(),
			Close:       100,
			Atr:         0.2,
			Ema21:       100.1,
			Ema55:       100.0,
			RangeGate4H: marketstate.RangeGate{
				Passed: true,
				Reason: "range_gate_h4_passed",
			},
			MarketState: marketstate.MarketStateRange,
		},
	})

	if len(got) != 1 {
		t.Fatalf("Evaluate() len = %d, want 1", len(got))
	}
	if !got[0].Enabled || got[0].BaseTemplate != "eth-core" || got[0].Template != "range-core" || got[0].Reason != "market_state_range" {
		t.Fatalf("Evaluate() = %+v, want enabled base=eth-core template=range-core market_state_range", got[0])
	}
}

func TestSelectorEvaluateRangeTemplateRequiresH4Gate(t *testing.T) {
	now := time.Date(2026, 4, 26, 7, 0, 0, 0, time.UTC)
	selector := NewSelector(Config{
		CandidateSymbols: []string{"ETHUSDT"},
		RouterConfig: strategyrouter.Config{
			StaticTemplateMap: map[string]string{
				"ETHUSDT": "eth-core",
			},
			RangeTemplate: "range-core",
		},
		FreshnessWindow: 3 * time.Minute,
		RequireFinal:    true,
		RequireTradable: true,
		RequireClean:    true,
	})

	got := selector.Evaluate(now, map[string]Snapshot{
		"ETHUSDT": {
			Symbol:      "ETHUSDT",
			UpdatedAt:   now.Add(-time.Minute),
			IsTradable:  true,
			IsFinal:     true,
			IsDirty:     false,
			LastEventMs: now.UnixMilli(),
			Close:       100,
			Atr:         0.2,
			Ema21:       100.1,
			Ema55:       100.0,
			RangeGate4H: marketstate.RangeGate{
				Passed: false,
				Reason: "range_gate_h4_failed",
			},
			MarketState: marketstate.MarketStateRange,
			MarketAnalysis: regimejudge.Analysis{
				Healthy:    true,
				Fresh:      true,
				RangeMatch: true,
				Features: featureengine.Features{
					AtrPct: 0.002,
				},
			},
		},
	})

	if len(got) != 1 {
		t.Fatalf("Evaluate() len = %d, want 1", len(got))
	}
	if !got[0].Enabled || got[0].Template != "eth-core" || got[0].Reason != "healthy_data" {
		t.Fatalf("Evaluate() = %+v, want base template when h4 gate failed", got[0])
	}
}

func TestSelectorEvaluateUsesRegimeAnalysisWithoutMarketState(t *testing.T) {
	now := time.Date(2026, 4, 26, 7, 0, 0, 0, time.UTC)
	selector := NewSelector(Config{
		CandidateSymbols: []string{"ETHUSDT", "BTCUSDT"},
		RouterConfig: strategyrouter.Config{
			StaticTemplateMap: map[string]string{
				"ETHUSDT": "eth-core",
				"BTCUSDT": "btc-core",
			},
			RangeTemplate:     "range-core",
			BTCTrendTemplate:  "btc-trend",
			BTCTrendAtrPctMax: 0.003,
		},
		FreshnessWindow: 3 * time.Minute,
		RequireFinal:    true,
		RequireTradable: true,
		RequireClean:    true,
	})

	got := selector.Evaluate(now, map[string]Snapshot{
		"ETHUSDT": {
			Symbol:      "ETHUSDT",
			UpdatedAt:   now.Add(-time.Minute),
			IsTradable:  true,
			IsFinal:     true,
			IsDirty:     false,
			LastEventMs: now.UnixMilli(),
			Close:       100,
			Atr:         0.2,
			RangeGate4H: marketstate.RangeGate{
				Passed: true,
				Reason: "range_gate_h4_passed",
			},
			MarketAnalysis: regimejudge.Analysis{
				Healthy:    true,
				Fresh:      true,
				RangeMatch: true,
				Features: featureengine.Features{
					AtrPct: 0.002,
				},
			},
		},
		"BTCUSDT": {
			Symbol:      "BTCUSDT",
			UpdatedAt:   now.Add(-time.Minute),
			IsTradable:  true,
			IsFinal:     true,
			IsDirty:     false,
			LastEventMs: now.UnixMilli(),
			Close:       100,
			Atr:         0.2,
			MarketAnalysis: regimejudge.Analysis{
				Healthy:         true,
				Fresh:           true,
				BullTrendStrict: true,
				Features: featureengine.Features{
					AtrPct: 0.002,
				},
			},
		},
	})

	if got[0].Template != "range-core" || got[0].Reason != "market_state_range" {
		t.Fatalf("ETHUSDT = %+v, want range-core via analysis", got[0])
	}
	if got[1].Template != "btc-trend" || got[1].Reason != "market_state_trend" {
		t.Fatalf("BTCUSDT = %+v, want btc-trend via analysis", got[1])
	}
}

func TestSelectorEvaluateUsesRouterConfigEntry(t *testing.T) {
	now := time.Date(2026, 4, 26, 7, 0, 0, 0, time.UTC)
	selector := NewSelector(Config{
		CandidateSymbols: []string{"ETHUSDT"},
		FreshnessWindow:  3 * time.Minute,
		RequireFinal:     true,
		RequireTradable:  true,
		RequireClean:     true,
		RouterConfig: strategyrouter.Config{
			StaticTemplateMap: map[string]string{
				"ETHUSDT": "eth-core",
			},
			RangeTemplate: "range-core",
		},
	})
	got := selector.Evaluate(now, map[string]Snapshot{
		"ETHUSDT": {
			Symbol:      "ETHUSDT",
			UpdatedAt:   now.Add(-time.Minute),
			IsTradable:  true,
			IsFinal:     true,
			IsDirty:     false,
			LastEventMs: now.UnixMilli(),
			RangeGate4H: marketstate.RangeGate{
				Passed: true,
				Reason: "range_gate_h4_passed",
			},
			MarketAnalysis: regimejudge.Analysis{
				Healthy:    true,
				Fresh:      true,
				RangeMatch: true,
				Features: featureengine.Features{
					AtrPct: 0.002,
				},
			},
		},
	})
	if len(got) != 1 {
		t.Fatalf("Evaluate() len = %d, want 1", len(got))
	}
	if got[0].Template != "range-core" || got[0].Reason != "market_state_range" {
		t.Fatalf("Evaluate() = %+v, want range-core/market_state_range", got[0])
	}
}

func TestSelectorEvaluateSOLHighBetaSafeAndDisable(t *testing.T) {
	now := time.Date(2026, 4, 26, 7, 0, 0, 0, time.UTC)
	selector := NewSelector(Config{
		CandidateSymbols: []string{"SOLUSDT"},
		RouterConfig: strategyrouter.Config{
			StaticTemplateMap: map[string]string{
				"SOLUSDT": "high-beta",
			},
			HighBetaSafeTemplate:  "high-beta-safe",
			HighBetaSafeSymbols:   []string{"SOLUSDT"},
			HighBetaSafeAtrPct:    0.006,
			HighBetaDisableAtrPct: 0.012,
		},
		FreshnessWindow: 3 * time.Minute,
		RequireFinal:    true,
		RequireTradable: true,
		RequireClean:    true,
	})

	got := selector.Evaluate(now, map[string]Snapshot{
		"SOLUSDT": {
			Symbol:      "SOLUSDT",
			UpdatedAt:   now.Add(-time.Minute),
			IsTradable:  true,
			IsFinal:     true,
			IsDirty:     false,
			LastEventMs: now.UnixMilli(),
			Close:       100,
			Atr:         0.8,
			Ema21:       99,
			Ema55:       98,
		},
	})
	if len(got) != 1 {
		t.Fatalf("Evaluate() len = %d, want 1", len(got))
	}
	if !got[0].Enabled || got[0].BaseTemplate != "high-beta" || got[0].Template != "high-beta-safe" || got[0].Reason != "volatility_high" {
		t.Fatalf("Evaluate() safe = %+v, want enabled base=high-beta template=high-beta-safe volatility_high", got[0])
	}

	got = selector.Evaluate(now, map[string]Snapshot{
		"SOLUSDT": {
			Symbol:      "SOLUSDT",
			UpdatedAt:   now.Add(-time.Minute),
			IsTradable:  true,
			IsFinal:     true,
			IsDirty:     false,
			LastEventMs: now.UnixMilli(),
			Close:       100,
			Atr:         1.5,
			Ema21:       99,
			Ema55:       98,
		},
	})
	if len(got) != 1 {
		t.Fatalf("Evaluate() len = %d, want 1", len(got))
	}
	if got[0].Enabled || got[0].BaseTemplate != "high-beta" || got[0].Template != "high-beta-safe" || got[0].Reason != "volatility_extreme" {
		t.Fatalf("Evaluate() disable = %+v, want disabled base=high-beta template=high-beta-safe volatility_extreme", got[0])
	}
}
