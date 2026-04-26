package universe

import (
	"testing"
	"time"

	"exchange-system/app/strategy/rpc/internal/marketstate"
)

func TestSelectorEvaluate(t *testing.T) {
	now := time.Date(2026, 4, 26, 7, 0, 0, 0, time.UTC)
	selector := NewSelector(Config{
		CandidateSymbols: []string{"BTCUSDT", "SOLUSDT"},
		StaticTemplateMap: map[string]string{
			"BTCUSDT": "btc-core",
			"SOLUSDT": "high-beta",
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
		StaticTemplateMap: map[string]string{
			"ETHUSDT": "eth-core",
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
		StaticTemplateMap: map[string]string{
			"BTCUSDT": "btc-core",
		},
		BTCTrendTemplate:  "btc-trend",
		BTCTrendAtrPctMax: 0.003,
		FreshnessWindow:   3 * time.Minute,
		RequireFinal:      true,
		RequireTradable:   true,
		RequireClean:      true,
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
		StaticTemplateMap: map[string]string{
			"BTCUSDT": "btc-core",
		},
		BTCTrendTemplate:  "btc-trend",
		BTCTrendAtrPctMax: 0.003,
		FreshnessWindow:   3 * time.Minute,
		RequireFinal:      true,
		RequireTradable:   true,
		RequireClean:      true,
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

func TestSelectorEvaluateSOLHighBetaSafeAndDisable(t *testing.T) {
	now := time.Date(2026, 4, 26, 7, 0, 0, 0, time.UTC)
	selector := NewSelector(Config{
		CandidateSymbols: []string{"SOLUSDT"},
		StaticTemplateMap: map[string]string{
			"SOLUSDT": "high-beta",
		},
		HighBetaSafeTemplate:  "high-beta-safe",
		HighBetaSafeSymbols:   []string{"SOLUSDT"},
		HighBetaSafeAtrPct:    0.006,
		HighBetaDisableAtrPct: 0.012,
		FreshnessWindow:       3 * time.Minute,
		RequireFinal:          true,
		RequireTradable:       true,
		RequireClean:          true,
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
