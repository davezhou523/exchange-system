package strategy

import "testing"

// TestJudgeRangeEntryLongNearRangeLow 验证价格贴近区间下沿时，Range 规则可以给出做多信号。
func TestJudgeRangeEntryLongNearRangeLow(t *testing.T) {
	s := NewTrendFollowingStrategy("ETHUSDT", map[string]float64{
		paramStrategyVariant:     2,
		paramRangeLookback:       6,
		paramRangeEntryAtrBuffer: 0.40,
		paramRangeRsiLongMax:     42,
		paramRangeRsiShortMin:    58,
		paramRangeRequireFlatEma: 1,
	}, nil, nil, "", nil)

	s.klines15m = []klineSnapshot{
		{High: 102, Low: 98, Close: 100, Ema21: 100.1, Ema55: 100.0, Atr: 1.2, Rsi: 50},
		{High: 103, Low: 97, Close: 101, Ema21: 100.2, Ema55: 100.1, Atr: 1.1, Rsi: 52},
		{High: 104, Low: 96, Close: 99, Ema21: 100.1, Ema55: 100.0, Atr: 1.0, Rsi: 49},
		{High: 103.5, Low: 95.8, Close: 98, Ema21: 100.0, Ema55: 99.9, Atr: 1.0, Rsi: 46},
		{High: 103.2, Low: 95.6, Close: 97, Ema21: 99.9, Ema55: 99.8, Atr: 0.9, Rsi: 44},
		{High: 100.5, Low: 95.5, Close: 95.8, Ema21: 99.8, Ema55: 99.7, Atr: 1.0, Rsi: 39},
	}
	s.latest15m = s.klines15m[len(s.klines15m)-1]

	got, stats := s.judgeRangeEntry(s.latest15m, s.klines15m, 5)
	if got != entryLong {
		t.Fatalf("judgeRangeEntry() = %v, want entryLong (stats=%+v)", got, stats)
	}
	if !stats.NearRangeLow || !stats.LongRSIOk || !stats.FlatEMAOk {
		t.Fatalf("unexpected range stats: %+v", stats)
	}
}

// TestJudgeRangeEntryRejectsNearMid 验证价格靠近区间中轴时，即使其他条件正常也不允许入场。
func TestJudgeRangeEntryRejectsNearMid(t *testing.T) {
	s := NewTrendFollowingStrategy("ETHUSDT", map[string]float64{
		paramStrategyVariant:         2,
		paramRangeLookback:           6,
		paramRangeEntryAtrBuffer:     0.40,
		paramRangeRsiLongMax:         42,
		paramRangeRsiShortMin:        58,
		paramRangeRequireFlatEma:     1,
		paramRangeMidNoTradeWidthPct: 0.20,
	}, nil, nil, "", nil)

	s.klines15m = []klineSnapshot{
		{High: 102, Low: 98, Close: 100, Ema21: 100.1, Ema55: 100.0, Atr: 1.0, Rsi: 50},
		{High: 103, Low: 97, Close: 101, Ema21: 100.2, Ema55: 100.1, Atr: 1.0, Rsi: 52},
		{High: 104, Low: 96, Close: 99, Ema21: 100.1, Ema55: 100.0, Atr: 1.0, Rsi: 49},
		{High: 103.5, Low: 95.8, Close: 98.8, Ema21: 100.0, Ema55: 99.9, Atr: 1.0, Rsi: 47},
		{High: 103.2, Low: 95.6, Close: 99.4, Ema21: 99.9, Ema55: 99.8, Atr: 1.0, Rsi: 50},
		{High: 100.5, Low: 95.5, Close: 99.7, Ema21: 99.8, Ema55: 99.7, Atr: 1.0, Rsi: 40},
	}
	s.latest15m = s.klines15m[len(s.klines15m)-1]

	got, stats := s.judgeRangeEntry(s.latest15m, s.klines15m, 5)
	if got != entryNone {
		t.Fatalf("judgeRangeEntry() = %v, want entryNone (stats=%+v)", got, stats)
	}
	if !stats.MidBlocked {
		t.Fatalf("MidBlocked = false, want true (stats=%+v)", stats)
	}
}

// TestJudgeRangeEntryRejectsWhenTrendTooStrong 验证均线在窗口内漂移过大时，直接禁止执行震荡入场。
func TestJudgeRangeEntryRejectsWhenTrendTooStrong(t *testing.T) {
	s := NewTrendFollowingStrategy("ETHUSDT", map[string]float64{
		paramStrategyVariant:       2,
		paramRangeLookback:         6,
		paramRangeEntryAtrBuffer:   0.40,
		paramRangeRsiLongMax:       42,
		paramRangeRequireFlatEma:   1,
		paramRangeTrendEmaDriftMax: 0.25,
	}, nil, nil, "", nil)

	s.klines15m = []klineSnapshot{
		{High: 102, Low: 98, Close: 100, Ema21: 98.8, Ema55: 98.7, Atr: 1.2, Rsi: 50},
		{High: 103, Low: 97, Close: 101, Ema21: 98.3, Ema55: 98.2, Atr: 1.1, Rsi: 52},
		{High: 104, Low: 96, Close: 99, Ema21: 97.9, Ema55: 97.8, Atr: 1.0, Rsi: 49},
		{High: 103.5, Low: 95.8, Close: 98, Ema21: 97.4, Ema55: 97.3, Atr: 1.0, Rsi: 46},
		{High: 103.2, Low: 95.6, Close: 97, Ema21: 96.9, Ema55: 96.8, Atr: 0.9, Rsi: 44},
		{High: 100.5, Low: 95.5, Close: 95.8, Ema21: 96.4, Ema55: 96.3, Atr: 1.0, Rsi: 39},
	}
	s.latest15m = s.klines15m[len(s.klines15m)-1]

	got, stats := s.judgeRangeEntry(s.latest15m, s.klines15m, 5)
	if got != entryNone {
		t.Fatalf("judgeRangeEntry() = %v, want entryNone (stats=%+v)", got, stats)
	}
	if !stats.TrendTooStrong {
		t.Fatalf("TrendTooStrong = false, want true (stats=%+v)", stats)
	}
	if !stats.FlatEMAOk {
		t.Fatalf("FlatEMAOk = false, want true so rejection comes from trend filter (stats=%+v)", stats)
	}
}

// TestJudgeRangeEntryRejectsWhenEMATooSteep 验证 EMA 价差过大时，旧有的震荡均线收敛过滤仍然生效。
func TestJudgeRangeEntryRejectsWhenEMATooSteep(t *testing.T) {
	s := NewTrendFollowingStrategy("ETHUSDT", map[string]float64{
		paramStrategyVariant:     2,
		paramRangeLookback:       6,
		paramRangeEntryAtrBuffer: 0.40,
		paramRangeRsiLongMax:     42,
		paramRangeRequireFlatEma: 1,
	}, nil, nil, "", nil)

	s.klines15m = []klineSnapshot{
		{High: 102, Low: 98, Close: 100, Ema21: 101.2, Ema55: 99.2, Atr: 1.2, Rsi: 50},
		{High: 103, Low: 97, Close: 101, Ema21: 101.4, Ema55: 99.1, Atr: 1.1, Rsi: 52},
		{High: 104, Low: 96, Close: 99, Ema21: 101.5, Ema55: 99.0, Atr: 1.0, Rsi: 49},
		{High: 103.5, Low: 95.8, Close: 98, Ema21: 101.7, Ema55: 99.0, Atr: 1.0, Rsi: 46},
		{High: 103.2, Low: 95.6, Close: 97, Ema21: 102.0, Ema55: 99.0, Atr: 0.9, Rsi: 44},
		{High: 100.5, Low: 95.5, Close: 95.8, Ema21: 102.2, Ema55: 99.0, Atr: 1.0, Rsi: 39},
	}
	s.latest15m = s.klines15m[len(s.klines15m)-1]

	got, stats := s.judgeRangeEntry(s.latest15m, s.klines15m, 5)
	if got != entryNone {
		t.Fatalf("judgeRangeEntry() = %v, want entryNone", got)
	}
	if stats.FlatEMAOk {
		t.Fatalf("FlatEMAOk = true, want false (stats=%+v)", stats)
	}
}
