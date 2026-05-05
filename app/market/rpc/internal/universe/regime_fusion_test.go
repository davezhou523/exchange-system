package universe

import (
	"testing"
	"time"

	"exchange-system/app/market/rpc/internal/marketstate"
	"exchange-system/common/featureengine"
	"exchange-system/common/regimejudge"
)

func TestFuseRegimesPrefersPrimaryH1State(t *testing.T) {
	now := time.Date(2026, 5, 2, 8, 15, 0, 0, time.UTC)
	h1 := RegimeFrame{
		Interval:    "1h",
		State:       marketstate.MarketStateTrendUp,
		RouteReason: regimejudge.RouteReasonTrend,
		Confidence:  0.75,
		UpdatedAt:   now.Add(-15 * time.Minute),
		Healthy:     true,
		Fresh:       true,
	}
	m15 := RegimeFrame{
		Interval:    "15m",
		State:       marketstate.MarketStateBreakout,
		RouteReason: regimejudge.RouteReasonBreakout,
		Confidence:  0.9,
		UpdatedAt:   now,
		Healthy:     true,
		Fresh:       true,
	}

	state, analysis, fusion := FuseRegimes(
		h1,
		regimejudge.Analysis{
			Healthy:          true,
			Fresh:            true,
			HasTrendFeatures: true,
			BullTrendStrict:  true,
			Features: featureengine.Features{
				Symbol:    "BTCUSDT",
				Timeframe: "1h",
				Close:     100,
				Ema21:     99,
				Ema55:     98,
				AtrPct:    0.002,
				UpdatedAt: now.Add(-15 * time.Minute),
				Healthy:   true,
			},
		},
		m15,
		regimejudge.Analysis{
			Healthy:       true,
			Fresh:         true,
			BreakoutMatch: true,
			Features: featureengine.Features{
				Symbol:    "BTCUSDT",
				Timeframe: "15m",
				Close:     101,
				Ema21:     100,
				Ema55:     99,
				AtrPct:    0.008,
				UpdatedAt: now,
				Healthy:   true,
			},
		},
	)

	if state != marketstate.MarketStateTrendUp {
		t.Fatalf("state = %s, want trend_up", state)
	}
	if !analysis.BullTrendStrict || analysis.BreakoutMatch {
		t.Fatalf("analysis = %+v, want fused bull trend only", analysis)
	}
	if fusion.FusedReason != "h1_primary_dominant" {
		t.Fatalf("fusion.FusedReason = %s, want h1_primary_dominant", fusion.FusedReason)
	}
	if fusion.FusedScore <= 0.5 {
		t.Fatalf("fusion.FusedScore = %.4f, want > 0.5", fusion.FusedScore)
	}
}

func TestFuseRegimesAlignedRange(t *testing.T) {
	now := time.Date(2026, 5, 2, 8, 15, 0, 0, time.UTC)
	frame := RegimeFrame{
		State:       marketstate.MarketStateRange,
		RouteReason: regimejudge.RouteReasonRange,
		Confidence:  0.7,
		UpdatedAt:   now,
		Healthy:     true,
		Fresh:       true,
	}

	state, analysis, fusion := FuseRegimes(
		RegimeFrame{Interval: "1h", State: frame.State, RouteReason: frame.RouteReason, Confidence: frame.Confidence, UpdatedAt: frame.UpdatedAt, Healthy: true, Fresh: true},
		regimejudge.Analysis{Healthy: true, Fresh: true, RangeMatch: true},
		RegimeFrame{Interval: "15m", State: frame.State, RouteReason: frame.RouteReason, Confidence: frame.Confidence, UpdatedAt: frame.UpdatedAt, Healthy: true, Fresh: true},
		regimejudge.Analysis{Healthy: true, Fresh: true, RangeMatch: true},
	)

	if state != marketstate.MarketStateRange {
		t.Fatalf("state = %s, want range", state)
	}
	if !analysis.RangeMatch || analysis.BreakoutMatch || analysis.BullTrendStrict || analysis.BearTrendStrict {
		t.Fatalf("analysis = %+v, want fused range only", analysis)
	}
	if fusion.FusedReason != "timeframes_aligned" {
		t.Fatalf("fusion.FusedReason = %s, want timeframes_aligned", fusion.FusedReason)
	}
}
