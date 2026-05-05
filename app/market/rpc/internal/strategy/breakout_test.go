package strategy

import (
	"context"
	"path/filepath"
	"testing"
	"time"

	marketpb "exchange-system/common/pb/market"
)

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

func TestCheckBreakoutEntryConditionsLogsGranularRejectReason(t *testing.T) {
	dir := t.TempDir()
	s := NewTrendFollowingStrategy("BTCUSDT", map[string]float64{
		paramStrategyVariant:        1,
		paramDecisionLogEnabled:     1,
		paramBreakoutVolumeRatioMin: 1.5,
		paramBreakoutEntryBufferATR: 0.1,
		paramBreakoutRsiLongMin:     55,
	}, nil, nil, dir, nil)

	s.klines15m = []klineSnapshot{
		{OpenTime: 1, High: 100, Low: 95, Volume: 100},
		{OpenTime: 2, High: 101, Low: 96, Volume: 100},
		{OpenTime: 3, High: 102, Low: 97, Volume: 100},
		{OpenTime: 4, High: 103, Low: 98, Volume: 100},
		{OpenTime: 5, High: 104, Low: 99, Volume: 100},
		{OpenTime: 6, High: 104.5, Low: 100, Volume: 120, Close: 103.8, Rsi: 61, Atr: 2, Ema21: 103, Ema55: 102},
	}
	s.latest15m = s.klines15m[len(s.klines15m)-1]

	now := time.Now().UTC()
	k := &marketpb.Kline{
		Symbol:     "BTCUSDT",
		Interval:   "15m",
		OpenTime:   now.Add(-15 * time.Minute).UnixMilli(),
		CloseTime:  now.UnixMilli(),
		IsTradable: true,
		IsFinal:    true,
	}
	if err := s.checkBreakoutEntryConditions(context.Background(), k); err != nil {
		t.Fatalf("checkBreakoutEntryConditions() error = %v", err)
	}

	entry := readSingleJSONLine(t, filepath.Join(dir, "decision", "BTCUSDT", now.Format("2006-01-02")+".jsonl"))
	if got := entry["reason_code"]; got != "breakout_no_price_break" {
		t.Fatalf("reason_code = %#v, want breakout_no_price_break", got)
	}
	if got := entry["reason"]; got != "价格未突破前高/前低：价格未突破前高/前低；量能不足，未达到放量确认条件" {
		t.Fatalf("reason = %#v, want detailed Chinese description with reject list", got)
	}
	extras, ok := entry["extras"].(map[string]interface{})
	if !ok {
		t.Fatalf("extras = %#v, want object", entry["extras"])
	}
	codes, ok := extras["breakout_reject_codes"].([]interface{})
	if !ok || len(codes) < 2 {
		t.Fatalf("breakout_reject_codes = %#v, want detailed reject list", extras["breakout_reject_codes"])
	}
	if codes[0] != "breakout_no_price_break" || codes[1] != "breakout_volume_low" {
		t.Fatalf("breakout_reject_codes = %#v, want price break + volume reasons", codes)
	}
	descs, ok := extras["breakout_reject_descs"].([]interface{})
	if !ok || len(descs) < 2 {
		t.Fatalf("breakout_reject_descs = %#v, want Chinese reject descriptions", extras["breakout_reject_descs"])
	}
	if descs[0] != "价格未突破前高/前低" || descs[1] != "量能不足，未达到放量确认条件" {
		t.Fatalf("breakout_reject_descs = %#v, want translated descriptions", descs)
	}
}
