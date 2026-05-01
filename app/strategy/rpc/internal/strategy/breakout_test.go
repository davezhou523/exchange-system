package strategy

import "testing"

func TestJudgeBreakoutEntryLong(t *testing.T) {
	s := NewTrendFollowingStrategy("BTCUSDT", map[string]float64{
		paramStrategyVariant:         1,
		paramBreakoutVolumeRatioMin:  1.2,
		paramBreakoutEntryBufferATR:  0.1,
		paramBreakoutRsiLongMin:      55,
		paramBreakoutRsiShortMax:     45,
		paramBreakoutRequireEmaTrend: 1,
	}, nil, nil, "", nil)

	s.klines15m = []klineSnapshot{
		{High: 100, Low: 95, Volume: 100, Ema21: 98, Ema55: 96},
		{High: 101, Low: 96, Volume: 110, Ema21: 99, Ema55: 97},
		{High: 102, Low: 97, Volume: 120, Ema21: 100, Ema55: 98},
		{High: 103, Low: 98, Volume: 130, Ema21: 101, Ema55: 99},
		{High: 104, Low: 99, Volume: 140, Ema21: 102, Ema55: 100},
		{High: 106, Low: 100, Volume: 220, Close: 107, Rsi: 61, Atr: 2, Ema21: 104, Ema55: 102},
	}
	s.latest15m = s.klines15m[len(s.klines15m)-1]

	got, stats := s.judgeBreakoutEntry(s.latest15m, s.klines15m, 5)
	if got != entryLong {
		t.Fatalf("judgeBreakoutEntry() = %v, want entryLong (stats=%+v)", got, stats)
	}
	if !stats.VolumeOk || !stats.BreakoutUp || !stats.LongRSIOk || !stats.LongEMAOk {
		t.Fatalf("unexpected breakout stats: %+v", stats)
	}
}

func TestJudgeBreakoutEntryRejectsWeakVolume(t *testing.T) {
	s := NewTrendFollowingStrategy("BTCUSDT", map[string]float64{
		paramStrategyVariant:        1,
		paramBreakoutVolumeRatioMin: 1.5,
		paramBreakoutEntryBufferATR: 0.1,
		paramBreakoutRsiLongMin:     55,
	}, nil, nil, "", nil)

	s.klines15m = []klineSnapshot{
		{High: 100, Low: 95, Volume: 100},
		{High: 101, Low: 96, Volume: 100},
		{High: 102, Low: 97, Volume: 100},
		{High: 103, Low: 98, Volume: 100},
		{High: 104, Low: 99, Volume: 100},
		{High: 106, Low: 100, Volume: 120, Close: 107, Rsi: 61, Atr: 2, Ema21: 104, Ema55: 102},
	}
	s.latest15m = s.klines15m[len(s.klines15m)-1]

	got, stats := s.judgeBreakoutEntry(s.latest15m, s.klines15m, 5)
	if got != entryNone {
		t.Fatalf("judgeBreakoutEntry() = %v, want entryNone", got)
	}
	if stats.VolumeOk {
		t.Fatalf("VolumeOk = true, want false (stats=%+v)", stats)
	}
}
