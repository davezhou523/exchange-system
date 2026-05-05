package strategy

import "testing"

// TestEvaluateFastProtectOn1mReturnsPartialCloseAction 验证 1m 快速保护命中后会返回可执行的部分减仓动作。
func TestEvaluateFastProtectOn1mReturnsPartialCloseAction(t *testing.T) {
	s := NewTrendFollowingStrategy("BTCUSDT", map[string]float64{
		paramExitFastProtectEnabled:         1,
		paramExitFastProtectBearBars1m:      3,
		paramExitFastProtectReducePctEarly:  0.50,
		paramExitFastProtectReducePctProfit: 0.40,
		paramExitFastProtectReducePctTrend:  0.30,
		paramExitFastProtectMaxTriggerCount: 1,
		paramExitPositionEarlyProfitR:       0.8,
		paramExitPositionTrendProfitR:       2.0,
		paramExitFastProtectVolumeSpike1m:   1.5,
		paramExitFastProtectATR1mDropMult:   1.1,
		paramExitFastProtectPullbackATR15m:  0.8,
	}, nil, nil, "", nil)

	s.pos = position{
		side:       sideLong,
		entryPrice: 100,
		stopLoss:   98,
		quantity:   1,
		exitState: ExitRuntimeState{
			InitialRiskDistance: 2,
		},
	}
	s.klines1m = []klineSnapshot{
		{OpenTime: 1, Open: 100.2, Close: 99.6, Low: 99.5, Ema21: 100.0, Atr: 1.0, Volume: 10},
		{OpenTime: 2, Open: 99.6, Close: 99.1, Low: 98.9, Ema21: 99.8, Atr: 1.0, Volume: 11},
		{OpenTime: 3, Open: 99.1, Close: 98.7, Low: 98.4, Ema21: 99.4, Atr: 1.0, Volume: 12},
	}
	s.latest1m = s.klines1m[len(s.klines1m)-1]

	got := s.evaluateFastProtectOn1m()
	if got == nil {
		t.Fatal("evaluateFastProtectOn1m() = nil, want action")
	}
	if got.Code != ExitEventFastProtect1m {
		t.Fatalf("Code = %q, want %q", got.Code, ExitEventFastProtect1m)
	}
	if got.Action != "SELL" {
		t.Fatalf("Action = %q, want SELL", got.Action)
	}
	if got.SignalType != "PARTIAL_CLOSE" {
		t.Fatalf("SignalType = %q, want PARTIAL_CLOSE", got.SignalType)
	}
	if got.ReducePct != 0.50 {
		t.Fatalf("ReducePct = %.2f, want 0.50", got.ReducePct)
	}
	if got.PositionStage != PositionStageEarly {
		t.Fatalf("PositionStage = %q, want %q", got.PositionStage, PositionStageEarly)
	}
	if got.RaiseStopTo != 100 {
		t.Fatalf("RaiseStopTo = %.2f, want 100.00 for break even", got.RaiseStopTo)
	}
	if got.StopRaiseMode != "break_even" {
		t.Fatalf("StopRaiseMode = %q, want break_even", got.StopRaiseMode)
	}
}

// TestEvaluateFastProtectOn1mUsesTrendReducePct 验证盈利趋势段触发快速保护时，会使用更小的保护减仓比例。
func TestEvaluateFastProtectOn1mUsesTrendReducePct(t *testing.T) {
	s := NewTrendFollowingStrategy("BTCUSDT", map[string]float64{
		paramExitFastProtectEnabled:         1,
		paramExitFastProtectBearBars1m:      3,
		paramExitFastProtectReducePctEarly:  0.50,
		paramExitFastProtectReducePctProfit: 0.40,
		paramExitFastProtectReducePctTrend:  0.30,
		paramExitPositionEarlyProfitR:       0.8,
		paramExitPositionTrendProfitR:       2.0,
	}, nil, nil, "", nil)

	s.pos = position{
		side:       sideLong,
		entryPrice: 100,
		stopLoss:   98,
		quantity:   1,
		exitState: ExitRuntimeState{
			InitialRiskDistance: 2,
		},
	}
	s.klines1m = []klineSnapshot{
		{OpenTime: 1, Open: 105.3, Close: 105.0, Low: 104.9, Ema21: 105.4, Atr: 1.0, Volume: 10},
		{OpenTime: 2, Open: 105.0, Close: 104.8, Low: 104.6, Ema21: 105.2, Atr: 1.0, Volume: 12},
		{OpenTime: 3, Open: 104.8, Close: 104.5, Low: 104.3, Ema21: 105.0, Atr: 1.0, Volume: 14},
	}
	s.latest1m = s.klines1m[len(s.klines1m)-1]

	got := s.evaluateFastProtectOn1m()
	if got == nil {
		t.Fatal("evaluateFastProtectOn1m() = nil, want action")
	}
	if got.PositionStage != PositionStageTrendProfit {
		t.Fatalf("PositionStage = %q, want %q", got.PositionStage, PositionStageTrendProfit)
	}
	if got.ReducePct != 0.30 {
		t.Fatalf("ReducePct = %.2f, want 0.30", got.ReducePct)
	}
	if got.StopRaiseMode != "micro_structure_1m" {
		t.Fatalf("StopRaiseMode = %q, want micro_structure_1m", got.StopRaiseMode)
	}
	if got.RaiseStopTo != 104.2 {
		t.Fatalf("RaiseStopTo = %.2f, want 104.20", got.RaiseStopTo)
	}
}

// TestEvaluateFastProtectOn1mHonorsTriggerLimit 验证超过单次持仓允许的快速保护次数后，不会重复触发。
func TestEvaluateFastProtectOn1mHonorsTriggerLimit(t *testing.T) {
	s := NewTrendFollowingStrategy("BTCUSDT", map[string]float64{
		paramExitFastProtectEnabled:         1,
		paramExitFastProtectBearBars1m:      3,
		paramExitFastProtectMaxTriggerCount: 1,
	}, nil, nil, "", nil)

	s.pos = position{
		side:       sideLong,
		entryPrice: 100,
		stopLoss:   98,
		quantity:   1,
		exitState: ExitRuntimeState{
			InitialRiskDistance:     2,
			FastProtectTriggerCount: 1,
		},
	}
	s.klines1m = []klineSnapshot{
		{OpenTime: 1, Open: 100.2, Close: 99.6, Low: 99.5, Ema21: 100.0, Atr: 1.0, Volume: 10},
		{OpenTime: 2, Open: 99.6, Close: 99.1, Low: 98.9, Ema21: 99.8, Atr: 1.0, Volume: 11},
		{OpenTime: 3, Open: 99.1, Close: 98.7, Low: 98.4, Ema21: 99.4, Atr: 1.0, Volume: 12},
	}
	s.latest1m = s.klines1m[len(s.klines1m)-1]

	if got := s.evaluateFastProtectOn1m(); got != nil {
		t.Fatalf("evaluateFastProtectOn1m() = %+v, want nil when trigger limit reached", got)
	}
}

// TestEvaluateEmergencyExitOn1sReturnsCloseWhenWaterfall 验证秒级瀑布式下跌且未盈利时，会触发全平保护。
func TestEvaluateEmergencyExitOn1sReturnsCloseWhenWaterfall(t *testing.T) {
	s := NewTrendFollowingStrategy("BTCUSDT", map[string]float64{
		paramExitEmergency1sEnabled:          1,
		paramExitEmergency1sWindowSec:        5,
		paramExitEmergency1sDropPct:          0.005,
		paramExitEmergency1sVolumeSpikeMult:  2.0,
		paramExitEmergency1sFullExitNoProfit: 1,
		paramExitPositionEarlyProfitR:        0.8,
		paramExitPositionTrendProfitR:        2.0,
	}, nil, nil, "", nil)

	s.pos = position{
		side:       sideLong,
		entryPrice: 100,
		stopLoss:   98,
		quantity:   1,
		exitState: ExitRuntimeState{
			InitialRiskDistance: 2,
		},
	}
	s.secondBars = []SecondBar{
		{OpenTimeMs: 1, CloseTimeMs: 1999, Open: 100.2, High: 100.3, Low: 100.0, Close: 100.1, Volume: 10, IsFinal: true},
		{OpenTimeMs: 2000, CloseTimeMs: 2999, Open: 100.1, High: 100.2, Low: 99.9, Close: 100.0, Volume: 11, IsFinal: true},
		{OpenTimeMs: 3000, CloseTimeMs: 3999, Open: 100.0, High: 100.1, Low: 99.8, Close: 99.9, Volume: 10, IsFinal: true},
		{OpenTimeMs: 4000, CloseTimeMs: 4999, Open: 99.9, High: 100.0, Low: 99.7, Close: 99.8, Volume: 12, IsFinal: true},
		{OpenTimeMs: 5000, CloseTimeMs: 5999, Open: 99.8, High: 99.9, Low: 99.3, Close: 99.5, Volume: 30, IsFinal: true},
	}

	got := s.evaluateEmergencyExitOn1s()
	if got == nil {
		t.Fatal("evaluateEmergencyExitOn1s() = nil, want action")
	}
	if got.Code != ExitEventEmergency1s {
		t.Fatalf("Code = %q, want %q", got.Code, ExitEventEmergency1s)
	}
	if got.SignalType != "CLOSE" {
		t.Fatalf("SignalType = %q, want CLOSE", got.SignalType)
	}
	if got.ReducePct != 1.0 {
		t.Fatalf("ReducePct = %.2f, want 1.00", got.ReducePct)
	}
}

// TestEvaluateEmergencyExitOn1sReturnsPartialCloseWhenInProfit 验证有利润时的秒级极端保护只做保护性减仓。
func TestEvaluateEmergencyExitOn1sReturnsPartialCloseWhenInProfit(t *testing.T) {
	s := NewTrendFollowingStrategy("BTCUSDT", map[string]float64{
		paramExitEmergency1sEnabled:          1,
		paramExitEmergency1sWindowSec:        5,
		paramExitEmergency1sDropPct:          0.005,
		paramExitEmergency1sVolumeSpikeMult:  2.0,
		paramExitEmergency1sFullExitNoProfit: 1,
		paramExitPositionEarlyProfitR:        0.8,
		paramExitPositionTrendProfitR:        2.0,
	}, nil, nil, "", nil)

	s.pos = position{
		side:       sideLong,
		entryPrice: 100,
		stopLoss:   98,
		quantity:   1,
		exitState: ExitRuntimeState{
			InitialRiskDistance: 2,
		},
	}
	s.secondBars = []SecondBar{
		{OpenTimeMs: 1, CloseTimeMs: 1999, Open: 105.0, High: 105.2, Low: 104.9, Close: 105.1, Volume: 8, IsFinal: true},
		{OpenTimeMs: 2000, CloseTimeMs: 2999, Open: 105.1, High: 105.2, Low: 104.8, Close: 105.0, Volume: 8, IsFinal: true},
		{OpenTimeMs: 3000, CloseTimeMs: 3999, Open: 105.0, High: 105.0, Low: 104.7, Close: 104.9, Volume: 9, IsFinal: true},
		{OpenTimeMs: 4000, CloseTimeMs: 4999, Open: 104.9, High: 105.0, Low: 104.6, Close: 104.8, Volume: 9, IsFinal: true},
		{OpenTimeMs: 5000, CloseTimeMs: 5999, Open: 104.8, High: 104.9, Low: 104.3, Close: 104.6, Volume: 20, IsFinal: true},
	}

	got := s.evaluateEmergencyExitOn1s()
	if got == nil {
		t.Fatal("evaluateEmergencyExitOn1s() = nil, want action")
	}
	if got.SignalType != "PARTIAL_CLOSE" {
		t.Fatalf("SignalType = %q, want PARTIAL_CLOSE", got.SignalType)
	}
	if got.ReducePct != 0.5 {
		t.Fatalf("ReducePct = %.2f, want 0.50", got.ReducePct)
	}
	if got.StopRaiseMode != "break_even" {
		t.Fatalf("StopRaiseMode = %q, want break_even", got.StopRaiseMode)
	}
	if got.RaiseStopTo != 100 {
		t.Fatalf("RaiseStopTo = %.2f, want 100.00", got.RaiseStopTo)
	}
}

// TestEvaluateEmergencyExitOn1sSyntheticBarRequiresHigherThreshold 验证 synthetic 秒条会使用更高阈值，普通跌幅不会误触发。
func TestEvaluateEmergencyExitOn1sSyntheticBarRequiresHigherThreshold(t *testing.T) {
	s := NewTrendFollowingStrategy("BTCUSDT", map[string]float64{
		paramExitEmergency1sEnabled:            1,
		paramExitEmergency1sWindowSec:          5,
		paramExitEmergency1sDropPct:            0.005,
		paramExitEmergency1sSyntheticDropMult:  1.5,
		paramExitEmergency1sSyntheticReducePct: 0.35,
		paramExitEmergency1sVolumeSpikeMult:    2.0,
		paramExitEmergency1sFullExitNoProfit:   1,
		paramExitPositionEarlyProfitR:          0.8,
		paramExitPositionTrendProfitR:          2.0,
	}, nil, nil, "", nil)

	s.pos = position{
		side:       sideLong,
		entryPrice: 100,
		stopLoss:   98,
		quantity:   1,
		exitState: ExitRuntimeState{
			InitialRiskDistance: 2,
		},
	}
	s.secondBars = []SecondBar{
		{OpenTimeMs: 1, CloseTimeMs: 1999, Open: 100.2, High: 100.3, Low: 100.0, Close: 100.1, Volume: 10, IsFinal: true},
		{OpenTimeMs: 2000, CloseTimeMs: 2999, Open: 100.1, High: 100.2, Low: 99.9, Close: 100.0, Volume: 11, IsFinal: true},
		{OpenTimeMs: 3000, CloseTimeMs: 3999, Open: 100.0, High: 100.1, Low: 99.8, Close: 99.9, Volume: 10, IsFinal: true},
		{OpenTimeMs: 4000, CloseTimeMs: 4999, Open: 99.9, High: 100.0, Low: 99.7, Close: 99.8, Volume: 12, IsFinal: true},
		{OpenTimeMs: 5000, CloseTimeMs: 5999, Open: 99.8, High: 99.9, Low: 99.55, Close: 99.6, Volume: 0, IsFinal: true, Synthetic: true},
	}

	if got := s.evaluateEmergencyExitOn1s(); got != nil {
		t.Fatalf("evaluateEmergencyExitOn1s() = %+v, want nil when synthetic move is below higher threshold", got)
	}
}

// TestEvaluateEmergencyExitOn1sSyntheticBarNeverFullExit 验证 synthetic 秒条即使破止损，也只做保护性减仓而不直接全平。
func TestEvaluateEmergencyExitOn1sSyntheticBarNeverFullExit(t *testing.T) {
	s := NewTrendFollowingStrategy("BTCUSDT", map[string]float64{
		paramExitEmergency1sEnabled:            1,
		paramExitEmergency1sWindowSec:          5,
		paramExitEmergency1sDropPct:            0.005,
		paramExitEmergency1sSyntheticDropMult:  1.5,
		paramExitEmergency1sSyntheticReducePct: 0.35,
		paramExitEmergency1sVolumeSpikeMult:    2.0,
		paramExitEmergency1sFullExitNoProfit:   1,
		paramExitPositionEarlyProfitR:          0.8,
		paramExitPositionTrendProfitR:          2.0,
	}, nil, nil, "", nil)

	s.pos = position{
		side:       sideLong,
		entryPrice: 100,
		stopLoss:   99.0,
		quantity:   1,
		exitState: ExitRuntimeState{
			InitialRiskDistance: 1,
		},
	}
	s.secondBars = []SecondBar{
		{OpenTimeMs: 1, CloseTimeMs: 1999, Open: 100.2, High: 100.3, Low: 100.0, Close: 100.1, Volume: 10, IsFinal: true},
		{OpenTimeMs: 2000, CloseTimeMs: 2999, Open: 100.1, High: 100.2, Low: 99.9, Close: 100.0, Volume: 11, IsFinal: true},
		{OpenTimeMs: 3000, CloseTimeMs: 3999, Open: 100.0, High: 100.1, Low: 99.8, Close: 99.9, Volume: 10, IsFinal: true},
		{OpenTimeMs: 4000, CloseTimeMs: 4999, Open: 99.9, High: 100.0, Low: 99.4, Close: 99.5, Volume: 12, IsFinal: true},
		{OpenTimeMs: 5000, CloseTimeMs: 5999, Open: 99.5, High: 99.6, Low: 98.2, Close: 98.4, Volume: 0, IsFinal: true, Synthetic: true},
	}

	got := s.evaluateEmergencyExitOn1s()
	if got == nil {
		t.Fatal("evaluateEmergencyExitOn1s() = nil, want action")
	}
	if got.SignalType != "PARTIAL_CLOSE" {
		t.Fatalf("SignalType = %q, want PARTIAL_CLOSE", got.SignalType)
	}
	if got.ReducePct != 0.35 {
		t.Fatalf("ReducePct = %.2f, want 0.35", got.ReducePct)
	}
	if got.ReasonLabel != "1秒极端保护减仓（盘口兜底）" {
		t.Fatalf("ReasonLabel = %q, want synthetic fallback label", got.ReasonLabel)
	}
}
