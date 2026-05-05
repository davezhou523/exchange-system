package strategy

import "testing"

// TestJudgeRangeEntryLongWithH1RangeRegime 验证 4H 震荡、1H 区间成立且 15M 多信号聚合满足时可以做多。
func TestJudgeRangeEntryLongWithH1RangeRegime(t *testing.T) {
	s := newRangeTestStrategy()
	s.klines4h = buildH4RangeHistory()
	s.latest4h = s.klines4h[len(s.klines4h)-1]
	s.klines1h = buildH1RangeHistory()
	s.latest1h = s.klines1h[len(s.klines1h)-1]
	s.klines15m = buildM15LongEntryHistory()
	s.latest15m = s.klines15m[len(s.klines15m)-1]

	got, stats := s.judgeRangeEntry(s.latest15m, s.klines15m, 20)
	if got != entryLong {
		t.Fatalf("judgeRangeEntry() = %v, want entryLong (stats=%+v)", got, stats)
	}
	if !stats.H4OscillationOK || stats.H4Score < 2 {
		t.Fatalf("4h oscillation not ready: %+v", stats)
	}
	if !stats.H1RangeOK || !stats.H1AdxOk || !stats.H1BollWidthOk {
		t.Fatalf("1h regime not ready: %+v", stats)
	}
	if !stats.LongHasBBBounce || stats.LongSignalCount < 2 {
		t.Fatalf("long signal aggregation not satisfied: %+v", stats)
	}
	if !stats.LongRSIOk || !stats.LongRSITurnOk {
		t.Fatalf("long entry filters not satisfied: %+v", stats)
	}
}

// TestJudgeRangeEntryShortWithH1RangeRegime 验证 4H 震荡、1H 区间成立且 15M 多信号聚合满足时可以做空。
func TestJudgeRangeEntryShortWithH1RangeRegime(t *testing.T) {
	s := newRangeTestStrategy()
	s.klines4h = buildH4RangeHistory()
	s.latest4h = s.klines4h[len(s.klines4h)-1]
	s.klines1h = buildH1RangeHistory()
	s.klines1h[len(s.klines1h)-1].Open = s.klines1h[len(s.klines1h)-1].Close + 0.2
	s.latest1h = s.klines1h[len(s.klines1h)-1]
	s.klines15m = buildM15ShortEntryHistory()
	s.latest15m = s.klines15m[len(s.klines15m)-1]

	got, stats := s.judgeRangeEntry(s.latest15m, s.klines15m, 20)
	if got != entryShort {
		t.Fatalf("judgeRangeEntry() = %v, want entryShort (stats=%+v)", got, stats)
	}
	if !stats.ShortHasBBBounce || stats.ShortSignalCount < 2 {
		t.Fatalf("short signal aggregation not satisfied: %+v", stats)
	}
	if !stats.ShortRSIOk || !stats.ShortRSITurnOk {
		t.Fatalf("short entry filters not satisfied: %+v", stats)
	}
}

// TestJudgeRangeEntryRejectsWhenH4IsNotOscillating 验证 4H 震荡评分不足时不会开仓。
func TestJudgeRangeEntryRejectsWhenH4IsNotOscillating(t *testing.T) {
	s := newRangeTestStrategy()
	s.klines4h = buildH4TrendHistory()
	s.latest4h = s.klines4h[len(s.klines4h)-1]
	s.klines1h = buildH1RangeHistory()
	s.latest1h = s.klines1h[len(s.klines1h)-1]
	s.klines15m = buildM15LongEntryHistory()
	s.latest15m = s.klines15m[len(s.klines15m)-1]

	got, stats := s.judgeRangeEntry(s.latest15m, s.klines15m, 20)
	if got != entryNone {
		t.Fatalf("judgeRangeEntry() = %v, want entryNone (stats=%+v)", got, stats)
	}
	if stats.H4OscillationOK {
		t.Fatalf("H4OscillationOK = true, want false (stats=%+v)", stats)
	}
}

// TestJudgeRangeEntryRejectsWithoutRsiTurn 验证 RSI 没有拐头时，即使其它条件接近也不会做多。
func TestJudgeRangeEntryRejectsWithoutRsiTurn(t *testing.T) {
	s := newRangeTestStrategy()
	s.klines4h = buildH4RangeHistory()
	s.latest4h = s.klines4h[len(s.klines4h)-1]
	s.klines1h = buildH1RangeHistory()
	s.latest1h = s.klines1h[len(s.klines1h)-1]
	s.klines15m = buildM15LongNoTurnHistory()
	s.latest15m = s.klines15m[len(s.klines15m)-1]

	got, stats := s.judgeRangeEntry(s.latest15m, s.klines15m, 20)
	if got != entryNone {
		t.Fatalf("judgeRangeEntry() = %v, want entryNone (stats=%+v)", got, stats)
	}
	if !stats.LongRSIOk {
		t.Fatalf("expected oversold context to hold: %+v", stats)
	}
	if stats.LongRSITurnOk {
		t.Fatalf("LongRSITurnOk = true, want false (stats=%+v)", stats)
	}
}

// TestDescribeRangeEntryRejectLongTurnMissing 验证震荡做多被拒时，会优先给出 RSI 未拐头的细分原因。
func TestDescribeRangeEntryRejectLongTurnMissing(t *testing.T) {
	s := newRangeTestStrategy()
	s.klines4h = buildH4RangeHistory()
	s.latest4h = s.klines4h[len(s.klines4h)-1]
	s.klines1h = buildH1RangeHistory()
	s.latest1h = s.klines1h[len(s.klines1h)-1]
	s.klines15m = buildM15LongNoTurnHistory()
	s.latest15m = s.klines15m[len(s.klines15m)-1]

	got, stats := s.judgeRangeEntry(s.latest15m, s.klines15m, 20)
	if got != entryNone {
		t.Fatalf("judgeRangeEntry() = %v, want entryNone (stats=%+v)", got, stats)
	}
	reject := s.describeRangeEntryReject(stats)
	if reject.PrimaryCode != "range_long_rsi_turn_missing" {
		t.Fatalf("PrimaryCode = %q, want range_long_rsi_turn_missing (reject=%+v)", reject.PrimaryCode, reject)
	}
	if len(reject.Codes) == 0 || reject.Codes[0] != "range_long_rsi_turn_missing" {
		t.Fatalf("Codes = %#v, want range_long_rsi_turn_missing first", reject.Codes)
	}
}

// TestJudgeRangeEntryAllowsFakeBreakoutLong 验证跌破 1H 下轨但 4H 仍属震荡时，可按假突破反向做多。
func TestJudgeRangeEntryAllowsFakeBreakoutLong(t *testing.T) {
	s := newRangeTestStrategy()
	s.klines4h = buildH4RangeHistory()
	s.latest4h = s.klines4h[len(s.klines4h)-1]
	s.klines1h = buildH1RangeHistory()
	s.latest1h = s.klines1h[len(s.klines1h)-1]
	s.klines15m = buildM15FakeBreakoutLongHistory()
	s.latest15m = s.klines15m[len(s.klines15m)-1]

	got, stats := s.judgeRangeEntry(s.latest15m, s.klines15m, 20)
	if got != entryLong {
		t.Fatalf("judgeRangeEntry() = %v, want entryLong (stats=%+v)", got, stats)
	}
	if stats.H1Zone != "breakout_down" || !stats.FakeBreakoutLong {
		t.Fatalf("expected fake breakout long, got %+v", stats)
	}
}

// TestJudgeRangeEntryRejectsWhenSignalsBelowMinimum 验证多信号数量不足时不会开仓。
func TestJudgeRangeEntryRejectsWhenSignalsBelowMinimum(t *testing.T) {
	s := newRangeTestStrategy()
	s.klines4h = buildH4RangeHistory()
	s.latest4h = s.klines4h[len(s.klines4h)-1]
	s.klines1h = buildH1RangeHistory()
	s.latest1h = s.klines1h[len(s.klines1h)-1]
	s.klines15m = buildM15WeakLongHistory()
	s.latest15m = s.klines15m[len(s.klines15m)-1]

	got, stats := s.judgeRangeEntry(s.latest15m, s.klines15m, 20)
	if got != entryNone {
		t.Fatalf("judgeRangeEntry() = %v, want entryNone (stats=%+v)", got, stats)
	}
	if stats.LongSignalCount >= 2 {
		t.Fatalf("LongSignalCount = %d, want < 2 (stats=%+v)", stats.LongSignalCount, stats)
	}
}

// TestDescribeRangeEntryRejectMiddleZone 验证区间中部无反转位时，会输出统一的中部区间拒绝原因。
func TestDescribeRangeEntryRejectMiddleZone(t *testing.T) {
	reject := newRangeTestStrategy().describeRangeEntryReject(rangeStats{
		H4OscillationOK:  true,
		H1RangeOK:        true,
		H1AdxOk:          true,
		H1BollWidthOk:    true,
		H1BollWidthMinOk: true,
		H1Zone:           "middle",
	})
	if reject.PrimaryCode != "range_h1_middle_zone" {
		t.Fatalf("PrimaryCode = %q, want range_h1_middle_zone (reject=%+v)", reject.PrimaryCode, reject)
	}
	if len(reject.Descs) != 1 || reject.Descs[0] != "价格位于1小时区间中部，未到边缘反转位" {
		t.Fatalf("Descs = %#v, want middle-zone desc", reject.Descs)
	}
}

// TestJudgeRangeEntryRejectsWhenH1RsiFilterBlocks 验证开启 1H RSI 过滤后，做多不会在高位 RSI 区间追进。
func TestJudgeRangeEntryRejectsWhenH1RsiFilterBlocks(t *testing.T) {
	s := newRangeTestStrategy()
	s.params[paramRangeUseH1RsiFilter] = 1
	s.params[paramRangeH1RsiLongMax] = 45
	s.klines4h = buildH4RangeHistory()
	s.latest4h = s.klines4h[len(s.klines4h)-1]
	s.klines1h = buildH1RangeHistory()
	s.klines1h[len(s.klines1h)-1].Rsi = 58
	s.latest1h = s.klines1h[len(s.klines1h)-1]
	s.klines15m = buildM15LongEntryHistory()
	s.latest15m = s.klines15m[len(s.klines15m)-1]

	got, stats := s.judgeRangeEntry(s.latest15m, s.klines15m, 20)
	if got != entryNone {
		t.Fatalf("judgeRangeEntry() = %v, want entryNone (stats=%+v)", got, stats)
	}
	if stats.H1RsiLongOk {
		t.Fatalf("H1RsiLongOk = true, want false (stats=%+v)", stats)
	}
}

// TestJudgeRangeEntryRejectsWhenH4TrendFilterBlocks 验证开启 4H 趋势方向过滤后，不会逆着 4H EMA 结构开仓。
func TestJudgeRangeEntryRejectsWhenH4TrendFilterBlocks(t *testing.T) {
	s := newRangeTestStrategy()
	s.params[paramRangeUseH4TrendFilter] = 1
	s.klines4h = buildH4RangeHistory()
	s.klines4h[len(s.klines4h)-1].Ema21 = 98.8
	s.klines4h[len(s.klines4h)-1].Ema55 = 100.2
	s.latest4h = s.klines4h[len(s.klines4h)-1]
	s.klines1h = buildH1RangeHistory()
	s.latest1h = s.klines1h[len(s.klines1h)-1]
	s.klines15m = buildM15LongEntryHistory()
	s.latest15m = s.klines15m[len(s.klines15m)-1]

	got, stats := s.judgeRangeEntry(s.latest15m, s.klines15m, 20)
	if got != entryNone {
		t.Fatalf("judgeRangeEntry() = %v, want entryNone (stats=%+v)", got, stats)
	}
	if stats.H4TrendLongOk {
		t.Fatalf("H4TrendLongOk = true, want false (stats=%+v)", stats)
	}
}

// TestBuildRangeTradePlanAppliesMinRRAndTpMethod 验证止盈模式和最小 RR 过滤都会反映到震荡交易计划里。
func TestBuildRangeTradePlanAppliesMinRRAndTpMethod(t *testing.T) {
	s := newRangeTestStrategy()
	s.latest1h = klineSnapshot{Atr: 0.8}
	stats := rangeStats{
		H1BollLower:  99.2,
		H1BollMid:    100.0,
		H1BollUpper:  100.8,
		M15BollLower: 99.4,
		M15BollUpper: 100.6,
	}
	s.params[paramRangeTakeProfitMethod] = 2
	s.params[paramRangeTakeProfitAtrOff] = 0.5
	s.params[paramRangeMinRR] = 0.1
	snap := klineSnapshot{Close: 99.6, Atr: 0.6}

	plan, ok := s.buildRangeTradePlan(entryLong, snap, stats)
	if !ok {
		t.Fatalf("buildRangeTradePlan() rejected valid setup: %+v", plan)
	}
	if plan.TakeProfitMethod != "mid_atr" {
		t.Fatalf("TakeProfitMethod = %s, want mid_atr", plan.TakeProfitMethod)
	}
	if plan.TakeProfit1 <= stats.H1BollMid {
		t.Fatalf("TakeProfit1 = %.2f, want > H1 mid %.2f", plan.TakeProfit1, stats.H1BollMid)
	}

	s.params[paramRangeMinRR] = 10
	if _, ok := s.buildRangeTradePlan(entryLong, snap, stats); ok {
		t.Fatalf("buildRangeTradePlan() = ok, want false when min RR is too high")
	}
}

// TestEvaluateRangeExitDecisionTimeStop 验证开启时间止损后，超过最大持仓K线数会触发平仓。
func TestEvaluateRangeExitDecisionTimeStop(t *testing.T) {
	s := newRangeTestStrategy()
	s.params[paramRangeUseTimeStop] = 1
	s.params[paramRangeMaxBars] = 3
	s.pos = position{
		side:         sideLong,
		entryPrice:   100,
		stopLoss:     98,
		takeProfit1:  101,
		takeProfit2:  102,
		entryBarTime: 3,
	}
	history := []klineSnapshot{
		{OpenTime: 1, Close: 100},
		{OpenTime: 2, Close: 100.2},
		{OpenTime: 3, Close: 100.4},
		{OpenTime: 4, Close: 100.5},
		{OpenTime: 5, Close: 100.6},
	}

	decision := s.evaluateRangeExitDecision("15m", klineSnapshot{Close: 100.6, Atr: 0.6, Rsi: 50}, history)
	if decision.Action != "SELL" {
		t.Fatalf("Action = %s, want SELL (decision=%+v)", decision.Action, decision)
	}
}

// TestEvaluateRangeExitDecisionRsiTakeProfit 验证 rsi 止盈模式下，RSI 反向到阈值会触发平仓。
func TestEvaluateRangeExitDecisionRsiTakeProfit(t *testing.T) {
	s := newRangeTestStrategy()
	s.params[paramRangeTakeProfitMethod] = 3
	s.pos = position{
		side:         sideLong,
		entryPrice:   100,
		quantity:     0.2,
		stopLoss:     98,
		entryBarTime: 1,
	}
	decision := s.evaluateRangeExitDecision("15m", klineSnapshot{Close: 101.2, Atr: 0.6, Rsi: 72}, []klineSnapshot{{OpenTime: 1}, {OpenTime: 2}})
	if decision.Action != "SELL" {
		t.Fatalf("Action = %s, want SELL (decision=%+v)", decision.Action, decision)
	}
}

// TestEvaluateRangeExitDecisionBreakevenAndSteppedTrail 验证保本止损和阶梯追踪会把止损推向盈利区。
func TestEvaluateRangeExitDecisionBreakevenAndSteppedTrail(t *testing.T) {
	s := newRangeTestStrategy()
	s.params[paramRangeUseBreakevenStop] = 1
	s.params[paramRangeUseSteppedTrail] = 1
	s.params[paramRangeTakeProfitMethod] = 0
	s.params[paramRangeTrailStep1Pct] = 0.9
	s.params[paramRangeTrailStep1SL] = 0.65
	s.pos = position{
		side:         sideLong,
		entryPrice:   100,
		quantity:     0.2,
		stopLoss:     98,
		takeProfit1:  101,
		takeProfit2:  104,
		entryBarTime: 1,
	}

	decision := s.evaluateRangeExitDecision("15m", klineSnapshot{Close: 103.7, Atr: 0.6, Rsi: 60}, []klineSnapshot{{OpenTime: 1}, {OpenTime: 2}})
	if decision.Action != "" {
		t.Fatalf("Action = %s, want empty (decision=%+v)", decision.Action, decision)
	}
	if s.pos.stopLoss <= s.pos.entryPrice {
		t.Fatalf("stopLoss = %.2f, want > entryPrice %.2f", s.pos.stopLoss, s.pos.entryPrice)
	}
}

// TestEvaluateRangeExitDecisionSplitCreatesPartialClose 验证 split 模式命中第一目标时会生成 PARTIAL_CLOSE。
func TestEvaluateRangeExitDecisionSplitCreatesPartialClose(t *testing.T) {
	s := newRangeTestStrategy()
	s.params[paramRangeTakeProfitMethod] = 4
	s.params[paramRangeSplitTPRatio] = 0.4
	s.pos = position{
		side:          sideLong,
		entryPrice:    100,
		quantity:      0.25,
		stopLoss:      98,
		takeProfit1:   101,
		takeProfit2:   104,
		entryBarTime:  1,
		partialClosed: false,
	}

	decision := s.evaluateRangeExitDecision("15m", klineSnapshot{Close: 101.1, Atr: 0.6, Rsi: 55}, []klineSnapshot{{OpenTime: 1}, {OpenTime: 2}})
	if decision.SignalType != "PARTIAL_CLOSE" {
		t.Fatalf("SignalType = %s, want PARTIAL_CLOSE (decision=%+v)", decision.SignalType, decision)
	}
	if !decision.Partial {
		t.Fatalf("Partial = false, want true (decision=%+v)", decision)
	}
	if decision.Quantity != 0.1 {
		t.Fatalf("Quantity = %.4f, want 0.1000 (decision=%+v)", decision.Quantity, decision)
	}
}

// newRangeTestStrategy 创建用于震荡入场测试的策略实例。
func newRangeTestStrategy() *TrendFollowingStrategy {
	return NewTrendFollowingStrategy("ETHUSDT", map[string]float64{
		paramStrategyVariant:         2,
		paramRangeH4AdxPeriod:        14,
		paramRangeH4AdxMax:           20,
		paramRangeH4EmaClosenessMax:  0.005,
		paramRangeH4ScoreMin:         2,
		paramRangeH1AdxPeriod:        14,
		paramRangeH1AdxMax:           20,
		paramRangeH1BollPeriod:       20,
		paramRangeH1BollStdDev:       2,
		paramRangeH1BollWidthMaxPct:  0.05,
		paramRangeH1CandleFilter:     1,
		paramRangeM15BollPeriod:      20,
		paramRangeM15BollStdDev:      2.5,
		paramRangeM15BollTouchBuffer: 0.08,
		paramRangeM15RsiLongMax:      30,
		paramRangeM15RsiShortMin:     70,
		paramRangeM15RsiTurnMin:      0.5,
		paramRangeM15RsiConfirmLong:  32,
		paramRangeM15RsiConfirmShort: 68,
		paramRangeLookback:           8,
		paramRangeSignalMinCount:     2,
		paramRangeRequireBBBounce:    1,
		paramRangeUsePinBar:          1,
		paramRangeUseEngulfing:       1,
		paramRangePinBarRatio:        2,
		paramRangeZoneProximity:      0.2,
		paramRangeFakeBreakout:       1,
		paramRangeStopAtrOffset:      0.3,
		paramRangeUseH4TrendFilter:   0,
		paramRangeUseH1RsiFilter:     0,
		paramRangeH1RsiLongMax:       0,
		paramRangeH1RsiShortMin:      0,
		paramRangeMinH1BollWidthPct:  0,
		paramRangeStopMethod:         0,
		paramRangeTakeProfitMethod:   4,
		paramRangeTakeProfitAtrOff:   0,
		paramRangeSplitTPRatio:       0.5,
		paramRangeMinRR:              0,
		paramRangeUseTimeStop:        0,
		paramRangeMaxBars:            0,
		paramRangeUseBreakevenStop:   1,
		paramRangeBreakevenActivate:  0.6,
		paramRangeUseSteppedTrail:    1,
		paramRangeTrailStep1Pct:      0.9,
		paramRangeTrailStep1SL:       0.65,
		paramRangeUseTrailingStop:    0,
		paramRangeTrailAtrMult:       0,
		paramRangeTrailActivate:      0,
	}, nil, nil, "", nil)
}

// buildH4RangeHistory 构造 4H 低 ADX、EMA 收敛、ATR 回落的震荡样本。
func buildH4RangeHistory() []klineSnapshot {
	closes := []float64{
		100.0, 100.2, 99.9, 100.1, 99.8, 100.0, 100.1, 99.9, 100.0, 100.2,
		99.8, 100.0, 100.1, 99.9, 100.0, 100.2, 99.9, 100.1, 99.8, 100.0,
		100.1, 99.9, 100.0, 100.2, 99.8, 100.0, 99.9, 100.1, 99.9, 100.0,
	}
	out := make([]klineSnapshot, 0, len(closes))
	for i, close := range closes {
		out = append(out, klineSnapshot{
			OpenTime: int64(i + 1),
			Open:     close - 0.05,
			High:     close + 0.30,
			Low:      close - 0.30,
			Close:    close,
			Atr:      1.2 - float64(i)*0.01,
			Ema21:    100.00 + 0.01*float64((i%3)-1),
			Ema55:    100.02 + 0.01*float64((i%2)-1),
			Rsi:      50,
		})
	}
	return out
}

// buildH1RangeHistory 构造低 ADX 且布林带收窄的 1H 震荡样本。
func buildH1RangeHistory() []klineSnapshot {
	closes := []float64{
		100.0, 100.3, 99.9, 100.2, 99.8, 100.1, 99.7, 100.2, 99.9, 100.1,
		99.8, 100.0, 99.7, 100.1, 99.9, 100.2, 99.8, 100.0, 99.9, 100.1,
		99.8, 100.0, 99.9, 100.1, 99.8, 100.0, 99.9, 100.1, 99.8, 100.0,
	}
	out := make([]klineSnapshot, 0, len(closes))
	for i, close := range closes {
		out = append(out, klineSnapshot{
			OpenTime: int64(i + 1),
			Open:     close - 0.05,
			High:     close + 0.35,
			Low:      close - 0.35,
			Close:    close,
			Atr:      0.8,
			Rsi:      50,
		})
	}
	return out
}

// buildH4TrendHistory 构造 4H 单边趋势样本，用于验证震荡评分失效。
func buildH4TrendHistory() []klineSnapshot {
	out := make([]klineSnapshot, 0, 30)
	close := 100.0
	for i := 0; i < 30; i++ {
		close += 1.2
		out = append(out, klineSnapshot{
			OpenTime: int64(i + 1),
			Open:     close - 0.8,
			High:     close + 0.7,
			Low:      close - 0.4,
			Close:    close,
			Atr:      0.8 + float64(i)*0.05,
			Ema21:    close - 0.3,
			Ema55:    close - 2.0,
			Rsi:      60,
		})
	}
	return out
}

// buildM15LongEntryHistory 构造触及下轨且 RSI 超卖后拐头向上的 15M 做多样本。
func buildM15LongEntryHistory() []klineSnapshot {
	closes := []float64{
		100.0, 100.2, 99.9, 100.1, 99.8, 100.0, 99.9, 100.1, 99.8, 100.0,
		99.9, 100.1, 99.8, 100.0, 99.9, 100.1, 99.8, 100.0, 97.2, 98.2,
	}
	opens := []float64{
		100.1, 100.1, 100.0, 100.0, 99.9, 100.1, 99.8, 100.0, 99.9, 99.9,
		100.0, 100.0, 99.9, 100.1, 100.0, 100.0, 99.9, 100.1, 97.8, 97.7,
	}
	rsis := []float64{
		49, 50, 48, 49, 47, 48, 49, 50, 48, 49,
		47, 48, 49, 48, 47, 46, 42, 34, 26.5, 28.0,
	}
	return buildEntryHistory(closes, opens, rsis, true)
}

// buildM15LongNoTurnHistory 构造触及下轨但 RSI 仍继续下行的 15M 样本。
func buildM15LongNoTurnHistory() []klineSnapshot {
	closes := []float64{
		100.0, 100.2, 99.9, 100.1, 99.8, 100.0, 99.9, 100.1, 99.8, 100.0,
		99.9, 100.1, 99.8, 100.0, 99.9, 100.1, 99.8, 100.0, 97.2, 97.0,
	}
	opens := []float64{
		100.1, 100.1, 100.0, 100.0, 99.9, 100.1, 99.8, 100.0, 99.9, 99.9,
		100.0, 100.0, 99.9, 100.1, 100.0, 100.0, 99.9, 100.1, 97.8, 97.4,
	}
	rsis := []float64{
		49, 50, 48, 49, 47, 48, 49, 50, 48, 49,
		47, 48, 49, 48, 47, 46, 42, 34, 26.5, 25.8,
	}
	return buildEntryHistory(closes, opens, rsis, true)
}

// buildM15ShortEntryHistory 构造触及上轨且 RSI 超买后拐头向下的 15M 做空样本。
func buildM15ShortEntryHistory() []klineSnapshot {
	closes := []float64{
		100.0, 99.8, 100.1, 99.9, 100.2, 100.0, 100.1, 99.9, 100.2, 100.0,
		100.1, 99.9, 100.2, 100.0, 100.1, 99.9, 100.2, 100.0, 102.8, 101.9,
	}
	opens := []float64{
		99.9, 99.9, 100.0, 100.0, 100.1, 100.1, 100.0, 100.0, 100.1, 100.1,
		100.0, 100.0, 100.1, 100.1, 100.0, 100.0, 100.1, 100.1, 102.2, 102.4,
	}
	rsis := []float64{
		51, 50, 52, 51, 53, 52, 51, 52, 53, 52,
		51, 52, 53, 54, 55, 58, 62, 69, 74.5, 72.8,
	}
	return buildEntryHistory(closes, opens, rsis, false)
}

// buildM15FakeBreakoutLongHistory 构造跌破 1H 下轨但出现反弹信号的假突破做多样本。
func buildM15FakeBreakoutLongHistory() []klineSnapshot {
	closes := []float64{
		100.0, 100.1, 99.9, 100.0, 99.8, 100.0, 99.9, 100.0, 99.9, 100.1,
		99.8, 100.0, 99.9, 100.1, 99.8, 100.0, 99.7, 99.6, 99.0, 99.2,
	}
	opens := []float64{
		100.1, 100.0, 100.0, 100.1, 99.9, 100.1, 100.0, 100.1, 100.0, 100.0,
		99.9, 100.1, 100.0, 100.0, 99.9, 100.1, 99.9, 99.8, 99.3, 98.8,
	}
	rsis := []float64{
		50, 49, 48, 49, 47, 48, 49, 50, 48, 49,
		47, 48, 49, 48, 47, 45, 41, 36, 28, 29,
	}
	return buildEntryHistory(closes, opens, rsis, true)
}

// buildM15WeakLongHistory 构造只有单一超卖信号、缺少 BB 反弹和形态确认的弱做多样本。
func buildM15WeakLongHistory() []klineSnapshot {
	closes := []float64{
		100.0, 100.2, 99.9, 100.1, 99.8, 100.0, 99.9, 100.1, 99.8, 100.0,
		99.9, 100.1, 99.8, 100.0, 99.9, 100.1, 99.8, 100.0, 99.6, 99.5,
	}
	opens := []float64{
		100.1, 100.1, 100.0, 100.0, 99.9, 100.1, 99.8, 100.0, 99.9, 99.9,
		100.0, 100.0, 99.9, 100.1, 100.0, 100.0, 99.9, 100.1, 99.7, 99.6,
	}
	rsis := []float64{
		49, 50, 48, 49, 47, 48, 49, 50, 48, 49,
		47, 48, 49, 48, 47, 46, 42, 34, 28, 29,
	}
	return buildEntryHistory(closes, opens, rsis, false)
}

// buildEntryHistory 根据收盘价、开盘价和 RSI 序列生成当前周期测试样本。
func buildEntryHistory(closes, opens, rsis []float64, longBias bool) []klineSnapshot {
	out := make([]klineSnapshot, 0, len(closes))
	for i, close := range closes {
		high := close + 0.35
		low := close - 0.35
		if i == len(closes)-1 {
			if longBias {
				low = close - 0.8
			} else {
				high = close + 0.8
			}
		}
		out = append(out, klineSnapshot{
			OpenTime: int64(i + 1),
			Open:     opens[i],
			High:     high,
			Low:      low,
			Close:    close,
			Atr:      0.9,
			Rsi:      rsis[i],
		})
	}
	return out
}
