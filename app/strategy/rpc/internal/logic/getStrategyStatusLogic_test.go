package logic

import (
	"context"
	"strings"
	"testing"
	"time"

	"exchange-system/app/strategy/rpc/internal/marketstate"
	"exchange-system/app/strategy/rpc/internal/svc"
	"exchange-system/app/strategy/rpc/internal/universe"
	"exchange-system/app/strategy/rpc/internal/weights"
	strategypb "exchange-system/common/pb/strategy"
)

func TestGetStrategyStatusIncludesWeightRecommendation(t *testing.T) {
	svcCtx := &svc.ServiceContext{}
	svcCtxWeight := weights.Recommendation{
		Symbol:         "BTCUSDT",
		Template:       "btc-trend",
		Bucket:         "trend",
		RouteReason:    "market_state_trend",
		Score:          1.2,
		ScoreSource:    "symbol_score",
		BucketBudget:   0.7,
		StrategyWeight: 0.7,
		SymbolWeight:   0.4,
		RiskScale:      1.0,
		PositionBudget: 0.28,
	}
	svcCtx.RecordLatestWeightRecommendation(svcCtxWeight)
	svcCtx.RecordLatestUniverseDesired(universe.DesiredStrategy{
		Symbol:       "BTCUSDT",
		BaseTemplate: "btc-core",
		Template:     "btc-trend",
		Bucket:       "trend",
		Enabled:      true,
		Reason:       "market_state_trend",
	})
	svcCtx.RecordLatestUniverseRuntimeStatus(
		"BTCUSDT",
		"defer_switch",
		"open_position",
		true,
		true,
		true,
		"btc-core",
		"btc-core",
	)
	svcCtx.RecordLatestUniverseSnapshot(universe.Snapshot{
		Symbol: "BTCUSDT",
		Regime1h: universe.RegimeFrame{
			Interval:    "1h",
			State:       marketstate.MarketStateTrendUp,
			Reason:      "ema_bull_alignment",
			RouteReason: "market_state_trend",
			Confidence:  0.75,
			UpdatedAt:   time.Date(2026, 5, 2, 8, 0, 0, 0, time.UTC),
			Healthy:     true,
			Fresh:       true,
		},
		Regime15m: universe.RegimeFrame{
			Interval:    "15m",
			State:       marketstate.MarketStateBreakout,
			Reason:      "atr_pct_high",
			RouteReason: "market_state_breakout",
			Confidence:  0.8,
			UpdatedAt:   time.Date(2026, 5, 2, 8, 15, 0, 0, time.UTC),
			Healthy:     true,
			Fresh:       true,
		},
		Fusion: universe.RegimeFusion{
			PrimaryWeight: 0.7,
			ConfirmWeight: 0.3,
			FusedState:    marketstate.MarketStateTrendUp,
			FusedReason:   "h1_primary_dominant",
			FusedScore:    0.525,
			UpdatedAt:     time.Date(2026, 5, 2, 8, 15, 0, 0, time.UTC),
		},
	})

	logic := NewGetStrategyStatusLogic(context.Background(), svcCtx)
	got, err := logic.GetStrategyStatus(&strategypb.StrategyRequest{StrategyId: "BTCUSDT"})
	if err != nil {
		t.Fatalf("GetStrategyStatus() error = %v", err)
	}
	if got.Status != "STOPPED" {
		t.Fatalf("status = %s, want STOPPED", got.Status)
	}
	if got.Allocator == nil {
		t.Fatal("allocator = nil, want structured allocator status")
	}
	if got.Router == nil {
		t.Fatal("router = nil, want structured route runtime status")
	}
	if got.Allocator.RouteBucket != "trend" ||
		got.Allocator.RouteReason != "market_state_trend" ||
		got.Allocator.ScoreSource != "symbol_score" {
		t.Fatalf("allocator = %+v, want structured weight fields", got.Allocator)
	}
	if !got.Router.Enabled ||
		got.Router.Template != "btc-trend" ||
		got.Router.RouteBucket != "trend" ||
		got.Router.TargetReason != "market_state_trend" ||
		got.Router.BaseTemplate != "btc-core" {
		t.Fatalf("router = %+v, want structured universe view", got.Router)
	}
	if !got.Router.RuntimeEnabled ||
		got.Router.RuntimeTemplate != "btc-core" ||
		got.Router.ApplyAction != "defer_switch" ||
		got.Router.ApplyGateReason != "open_position" ||
		!got.Router.HasStrategy ||
		!got.Router.HasOpenPosition {
		t.Fatalf("router runtime = %+v, want defer_switch/open_position runtime view", got.Router)
	}
	if got.Router.RegimeFusion == nil {
		t.Fatal("router.regime_fusion = nil, want structured fusion status")
	}
	if got.Router.RegimeFusion.FusedState != "trend_up" ||
		got.Router.RegimeFusion.FusedReason != "h1_primary_dominant" ||
		got.Router.RegimeFusion.H1.GetRouteReason() != "market_state_trend" ||
		got.Router.RegimeFusion.M15.GetRouteReason() != "market_state_breakout" {
		t.Fatalf("router.regime_fusion = %+v, want fused trend_up with h1/m15 frames", got.Router.RegimeFusion)
	}
	if got.Router.Warmup == nil {
		t.Fatal("router.warmup = nil, want empty-source warmup completeness view")
	}
	if got.Router.Warmup.Source != "empty" || got.Router.Warmup.Status != "warmup_incomplete" {
		t.Fatalf("router.warmup = %+v, want empty/incomplete warmup", got.Router.Warmup)
	}
	if len(got.Router.Warmup.IncompleteReasons) != 4 {
		t.Fatalf("router.warmup.incomplete_reasons = %+v, want all four intervals marked insufficient", got.Router.Warmup.IncompleteReasons)
	}
	if !strings.Contains(got.Message, "budget=0.2800") ||
		!strings.Contains(got.Message, "bucket=trend") ||
		!strings.Contains(got.Message, "route_reason=market_state_trend") ||
		!strings.Contains(got.Message, "source=symbol_score") {
		t.Fatalf("message = %q, want weight summary", got.Message)
	}
}

func TestGetStrategyStatusIncludesWarmupView(t *testing.T) {
	svcCtx := &svc.ServiceContext{}
	svcCtx.RecordLatestUniverseDesired(universe.DesiredStrategy{
		Symbol:  "ETHUSDT",
		Enabled: true,
	})
	svcCtx.RecordLatestUniverseRuntimeStatus(
		"ETHUSDT",
		"enable",
		"market_state_range",
		true,
		false,
		false,
		"range-core",
		"range-core",
	)
	svcCtx.RecordLatestUniverseSnapshot(universe.Snapshot{Symbol: "ETHUSDT"})
	svcCtx.RecordStrategyWarmupStatus("ETHUSDT", svc.StrategyWarmupStatusView{
		HistoryLen4h:  2,
		HistoryLen1h:  7,
		HistoryLen15m: 20,
		HistoryLen1m:  60,
		Source:        "cache",
	})

	logic := NewGetStrategyStatusLogic(context.Background(), svcCtx)
	got, err := logic.GetStrategyStatus(&strategypb.StrategyRequest{StrategyId: "ETHUSDT"})
	if err != nil {
		t.Fatalf("GetStrategyStatus() error = %v", err)
	}
	if got.Router == nil || got.Router.Warmup == nil {
		t.Fatalf("router.warmup = %+v, want structured warmup view", got.GetRouter().GetWarmup())
	}
	if got.Router.Warmup.Source != "cache" ||
		got.Router.Warmup.Status != "warmup_incomplete" ||
		got.Router.Warmup.HistoryLen_4H != 2 ||
		got.Router.Warmup.HistoryLen_1H != 7 ||
		got.Router.Warmup.HistoryLen_15M != 20 ||
		got.Router.Warmup.HistoryLen_1M != 60 {
		t.Fatalf("router.warmup = %+v, want cache warmup lengths", got.Router.Warmup)
	}
	if len(got.Router.Warmup.IncompleteReasons) != 4 {
		t.Fatalf("router.warmup.incomplete_reasons = %+v, want four insufficient reasons", got.Router.Warmup.IncompleteReasons)
	}
}
