package regimejudge

import (
	"testing"
	"time"

	"exchange-system/common/featureengine"
)

// TestAnalyzeBuildsSharedMatches 验证公共判态器能统一产出 breakout/range/trend 命中结果。
func TestAnalyzeBuildsSharedMatches(t *testing.T) {
	now := time.Date(2026, 5, 2, 12, 0, 0, 0, time.UTC)

	rangeOut := Analyze(now, featureengine.Features{
		Symbol:     "BTCUSDT",
		Timeframe:  "1m",
		Close:      100,
		Ema21:      101,
		Ema55:      102,
		AtrPct:     0.001,
		UpdatedAt:  now,
		IsTradable: true,
		IsFinal:    true,
	}, Config{
		FreshnessWindow:   3 * time.Minute,
		RangeAtrPctMax:    0.0015,
		BreakoutAtrPctMin: 0.006,
	})
	if !rangeOut.RangeMatch || rangeOut.BreakoutMatch {
		t.Fatalf("range analysis = %+v, want range_match=true breakout_match=false", rangeOut)
	}
	if !rangeOut.BearTrendStrict || rangeOut.BullTrendStrict {
		t.Fatalf("range trend flags = %+v, want bear strict only", rangeOut)
	}

	breakoutOut := Analyze(now, featureengine.Features{
		Symbol:     "ETHUSDT",
		Timeframe:  "1m",
		Close:      100,
		Ema21:      99,
		Ema55:      98,
		AtrPct:     0.007,
		UpdatedAt:  now,
		IsTradable: true,
		IsFinal:    true,
	}, Config{
		FreshnessWindow:   3 * time.Minute,
		RangeAtrPctMax:    0.0015,
		BreakoutAtrPctMin: 0.006,
	})
	if !breakoutOut.BreakoutMatch || breakoutOut.RangeMatch {
		t.Fatalf("breakout analysis = %+v, want breakout_match=true range_match=false", breakoutOut)
	}
	if !breakoutOut.BullTrendStrict || !breakoutOut.BullTrendAligned {
		t.Fatalf("breakout trend flags = %+v, want bull trend", breakoutOut)
	}
}

// TestAnalyzeRejectsStaleOrUnhealthy 验证公共判态器会统一拦住陈旧或不健康特征。
func TestAnalyzeRejectsStaleOrUnhealthy(t *testing.T) {
	now := time.Date(2026, 5, 2, 12, 0, 0, 0, time.UTC)

	stale := Analyze(now, featureengine.Features{
		Symbol:     "BNBUSDT",
		Timeframe:  "1m",
		Close:      100,
		Ema21:      101,
		Ema55:      102,
		AtrPct:     0.001,
		UpdatedAt:  now.Add(-10 * time.Minute),
		IsTradable: true,
		IsFinal:    true,
	}, Config{FreshnessWindow: 3 * time.Minute, RangeAtrPctMax: 0.0015, BreakoutAtrPctMin: 0.006})
	if stale.Fresh {
		t.Fatalf("fresh = true, want false")
	}

	unhealthy := Analyze(now, featureengine.Features{
		Symbol:     "BNBUSDT",
		Timeframe:  "1m",
		Close:      100,
		Ema21:      101,
		Ema55:      102,
		AtrPct:     0.001,
		UpdatedAt:  now,
		IsDirty:    true,
		IsTradable: true,
		IsFinal:    true,
	}, Config{FreshnessWindow: 3 * time.Minute, RangeAtrPctMax: 0.0015, BreakoutAtrPctMin: 0.006})
	if unhealthy.Healthy {
		t.Fatalf("healthy = true, want false")
	}
}
