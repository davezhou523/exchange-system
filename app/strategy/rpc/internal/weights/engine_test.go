package weights

import (
	"testing"
	"time"

	"exchange-system/app/strategy/rpc/internal/marketstate"
)

// 验证趋势市会按 70/30 分给趋势桶和突破桶，并在趋势桶内使用 ETH/SOL/BNB 固定配比。
func TestEvaluateTrendStateUsesStrategyMixAndPreferredTrendWeights(t *testing.T) {
	engine := NewEngine(Config{})
	out := engine.Evaluate(time.Unix(0, 0), Inputs{
		MarketState: marketstate.AggregateResult{State: marketstate.MarketStateTrendUp},
		Symbols:     []string{"BNBUSDT", "DOGEUSDT", "ETHUSDT", "SOLUSDT"},
		Templates: map[string]string{
			"ETHUSDT":  "eth-core",
			"SOLUSDT":  "high-beta",
			"BNBUSDT":  "high-beta",
			"DOGEUSDT": "breakout-core",
		},
		SymbolScores: map[string]float64{
			"ETHUSDT":  9,
			"SOLUSDT":  1,
			"BNBUSDT":  1,
			"DOGEUSDT": 1,
		},
	})
	got := recBySymbol(out.Recommendations)

	assertApproxEqual(t, got["ETHUSDT"].StrategyWeight, 0.7)
	assertApproxEqual(t, got["ETHUSDT"].SymbolWeight, 0.4)
	assertApproxEqual(t, got["ETHUSDT"].PositionBudget, 0.28)

	assertApproxEqual(t, got["SOLUSDT"].PositionBudget, 0.21)
	assertApproxEqual(t, got["BNBUSDT"].PositionBudget, 0.21)

	assertApproxEqual(t, got["DOGEUSDT"].StrategyWeight, 0.3)
	assertApproxEqual(t, got["DOGEUSDT"].SymbolWeight, 1.0)
	assertApproxEqual(t, got["DOGEUSDT"].PositionBudget, 0.3)
}

// 验证突破市会优先使用 BTC/DOGE/PEPE 的固定币种配比。
func TestEvaluateBreakoutStateUsesPreferredBreakoutWeights(t *testing.T) {
	engine := NewEngine(Config{})
	out := engine.Evaluate(time.Unix(0, 0), Inputs{
		MarketState: marketstate.AggregateResult{State: marketstate.MarketStateBreakout},
		Symbols:     []string{"BTCUSDT", "DOGEUSDT", "ETHUSDT", "PEPEUSDT"},
		Templates: map[string]string{
			"BTCUSDT":  "breakout-core",
			"DOGEUSDT": "breakout-core",
			"PEPEUSDT": "breakout-core",
			"ETHUSDT":  "eth-core",
		},
		SymbolScores: map[string]float64{
			"BTCUSDT":  1,
			"DOGEUSDT": 9,
			"PEPEUSDT": 7,
			"ETHUSDT":  1,
		},
	})
	got := recBySymbol(out.Recommendations)

	assertApproxEqual(t, got["BTCUSDT"].StrategyWeight, 0.7)
	assertApproxEqual(t, got["DOGEUSDT"].StrategyWeight, 0.7)
	assertApproxEqual(t, got["PEPEUSDT"].StrategyWeight, 0.7)
	assertApproxEqual(t, got["ETHUSDT"].StrategyWeight, 0.3)

	assertApproxEqual(t, got["BTCUSDT"].SymbolWeight, 0.5)
	assertApproxEqual(t, got["DOGEUSDT"].SymbolWeight, 0.25)
	assertApproxEqual(t, got["PEPEUSDT"].SymbolWeight, 0.25)
	assertApproxEqual(t, got["ETHUSDT"].SymbolWeight, 1.0)

	assertApproxEqual(t, got["BTCUSDT"].PositionBudget, 0.35)
	assertApproxEqual(t, got["DOGEUSDT"].PositionBudget, 0.175)
	assertApproxEqual(t, got["PEPEUSDT"].PositionBudget, 0.175)
	assertApproxEqual(t, got["ETHUSDT"].PositionBudget, 0.3)
}

// 验证 YAML 提供的固定币种权重可以覆盖默认 breakout 配比。
func TestEvaluateBreakoutStateUsesConfiguredWeights(t *testing.T) {
	engine := NewEngine(Config{
		BreakoutSymbolWeights: map[string]float64{
			"BTCUSDT":  0.6,
			"DOGEUSDT": 0.3,
			"PEPEUSDT": 0.1,
		},
	})
	out := engine.Evaluate(time.Unix(0, 0), Inputs{
		MarketState: marketstate.AggregateResult{State: marketstate.MarketStateBreakout},
		Symbols:     []string{"BTCUSDT", "DOGEUSDT", "PEPEUSDT"},
		Templates: map[string]string{
			"BTCUSDT":  "breakout-core",
			"DOGEUSDT": "breakout-core",
			"PEPEUSDT": "breakout-core",
		},
	})
	got := recBySymbol(out.Recommendations)

	assertApproxEqual(t, got["BTCUSDT"].SymbolWeight, 0.6)
	assertApproxEqual(t, got["DOGEUSDT"].SymbolWeight, 0.3)
	assertApproxEqual(t, got["PEPEUSDT"].SymbolWeight, 0.1)
}

// 验证 YAML 提供的策略桶配比可以覆盖默认 70/30 资金分配。
func TestEvaluateTrendStateUsesConfiguredStrategyMix(t *testing.T) {
	engine := NewEngine(Config{
		TrendStrategyMix: map[string]float64{
			"trend":    0.6,
			"breakout": 0.4,
		},
	})
	out := engine.Evaluate(time.Unix(0, 0), Inputs{
		MarketState: marketstate.AggregateResult{State: marketstate.MarketStateTrendUp},
		Symbols:     []string{"ETHUSDT", "DOGEUSDT"},
		Templates: map[string]string{
			"ETHUSDT":  "eth-core",
			"DOGEUSDT": "breakout-core",
		},
	})
	got := recBySymbol(out.Recommendations)

	assertApproxEqual(t, got["ETHUSDT"].StrategyWeight, 0.6)
	assertApproxEqual(t, got["DOGEUSDT"].StrategyWeight, 0.4)
	assertApproxEqual(t, got["ETHUSDT"].PositionBudget, 0.6)
	assertApproxEqual(t, got["DOGEUSDT"].PositionBudget, 0.4)
}

// 验证连亏达到阈值后，风险缩放会自动降到 50%。
func TestEvaluateLossStreakHalvesRiskScale(t *testing.T) {
	engine := NewEngine(Config{})
	out := engine.Evaluate(time.Unix(0, 0), Inputs{
		MarketState: marketstate.AggregateResult{State: marketstate.MarketStateTrendUp},
		Symbols:     []string{"ETHUSDT"},
		Templates: map[string]string{
			"ETHUSDT": "eth-core",
		},
		LossStreak: 3,
	})
	got := recBySymbol(out.Recommendations)

	assertApproxEqual(t, got["ETHUSDT"].RiskScale, 0.5)
	assertApproxEqual(t, got["ETHUSDT"].PositionBudget, 0.35)
}

// 验证 ATR 和量能同时放大时，会触发 30 分钟市场降温暂停。
func TestEvaluateCoolingPauseOnAtrAndVolumeSpike(t *testing.T) {
	engine := NewEngine(Config{
		CoolingPauseDuration: 30 * time.Minute,
		AtrSpikeRatioMin:     1.5,
		VolumeSpikeRatioMin:  2.0,
		CoolingMinSamples:    2,
	})
	engine.Evaluate(time.Unix(0, 0), Inputs{
		MarketState:        marketstate.AggregateResult{State: marketstate.MarketStateTrendUp},
		Symbols:            []string{"ETHUSDT"},
		Templates:          map[string]string{"ETHUSDT": "eth-core"},
		AvgAtrPct:          0.01,
		AvgVolume:          100,
		HealthySymbolCount: 2,
	})

	out := engine.Evaluate(time.Unix(60, 0), Inputs{
		MarketState:        marketstate.AggregateResult{State: marketstate.MarketStateTrendUp},
		Symbols:            []string{"ETHUSDT"},
		Templates:          map[string]string{"ETHUSDT": "eth-core"},
		AvgAtrPct:          0.02,
		AvgVolume:          250,
		HealthySymbolCount: 2,
	})

	if !out.MarketPaused {
		t.Fatal("MarketPaused = false, want true")
	}
	if out.MarketPauseReason != "market_cooling_pause" {
		t.Fatalf("MarketPauseReason = %s, want market_cooling_pause", out.MarketPauseReason)
	}
	assertApproxEqual(t, out.AtrSpikeRatio, 2.0)
	assertApproxEqual(t, out.VolumeSpikeRatio, 2.5)
	if out.CoolingUntil.IsZero() {
		t.Fatal("CoolingUntil = zero, want non-zero")
	}
}

// 验证进入市场降温后，在冷却窗口内会持续保持暂停状态。
func TestEvaluateCoolingPausePersistsWithinWindow(t *testing.T) {
	engine := NewEngine(Config{
		CoolingPauseDuration: 30 * time.Minute,
		AtrSpikeRatioMin:     1.5,
		VolumeSpikeRatioMin:  2.0,
		CoolingMinSamples:    2,
	})
	engine.Evaluate(time.Unix(0, 0), Inputs{
		MarketState:        marketstate.AggregateResult{State: marketstate.MarketStateTrendUp},
		Symbols:            []string{"ETHUSDT"},
		Templates:          map[string]string{"ETHUSDT": "eth-core"},
		AvgAtrPct:          0.01,
		AvgVolume:          100,
		HealthySymbolCount: 2,
	})
	engine.Evaluate(time.Unix(60, 0), Inputs{
		MarketState:        marketstate.AggregateResult{State: marketstate.MarketStateTrendUp},
		Symbols:            []string{"ETHUSDT"},
		Templates:          map[string]string{"ETHUSDT": "eth-core"},
		AvgAtrPct:          0.02,
		AvgVolume:          250,
		HealthySymbolCount: 2,
	})

	out := engine.Evaluate(time.Unix(10*60, 0), Inputs{
		MarketState:        marketstate.AggregateResult{State: marketstate.MarketStateTrendUp},
		Symbols:            []string{"ETHUSDT"},
		Templates:          map[string]string{"ETHUSDT": "eth-core"},
		AvgAtrPct:          0.011,
		AvgVolume:          110,
		HealthySymbolCount: 2,
	})
	if !out.MarketPaused {
		t.Fatal("MarketPaused = false, want true during cooling window")
	}
	if out.MarketPauseReason != "market_cooling_pause" {
		t.Fatalf("MarketPauseReason = %s, want market_cooling_pause", out.MarketPauseReason)
	}
}

func recBySymbol(recs []Recommendation) map[string]Recommendation {
	out := make(map[string]Recommendation, len(recs))
	for _, rec := range recs {
		out[rec.Symbol] = rec
	}
	return out
}

func assertApproxEqual(t *testing.T, got, want float64) {
	t.Helper()
	diff := got - want
	if diff < 0 {
		diff = -diff
	}
	if diff > 1e-9 {
		t.Fatalf("got %.10f, want %.10f", got, want)
	}
}
