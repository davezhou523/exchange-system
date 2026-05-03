package svc

import (
	"strings"
	"testing"
	"time"

	"exchange-system/app/strategy/rpc/internal/config"
	"exchange-system/app/strategy/rpc/internal/marketstate"
	strategyengine "exchange-system/app/strategy/rpc/internal/strategy"
	"exchange-system/app/strategy/rpc/internal/universe"
	marketpb "exchange-system/common/pb/market"
	strategypb "exchange-system/common/pb/strategy"
)

func TestResolveStrategyParameters(t *testing.T) {
	sc := config.StrategyConfig{
		Name:     "trend-following",
		Symbol:   "BTCUSDT",
		Template: "btc-core",
		Parameters: map[string]float64{
			"leverage": 6,
		},
		Overrides: map[string]float64{
			"risk_per_trade": 0.02,
		},
	}
	templates := map[string]map[string]float64{
		"btc-core": {
			"risk_per_trade": 0.025,
			"max_positions":  1,
			"leverage":       5,
		},
	}

	got, err := resolveStrategyParameters(templates, sc)
	if err != nil {
		t.Fatalf("resolveStrategyParameters() error = %v", err)
	}
	if got["max_positions"] != 1 {
		t.Fatalf("max_positions = %v, want 1", got["max_positions"])
	}
	if got["leverage"] != 6 {
		t.Fatalf("leverage = %v, want 6", got["leverage"])
	}
	if got["risk_per_trade"] != 0.02 {
		t.Fatalf("risk_per_trade = %v, want 0.02", got["risk_per_trade"])
	}
}

func TestResolveStrategyParametersUnknownTemplate(t *testing.T) {
	sc := config.StrategyConfig{
		Name:     "trend-following",
		Symbol:   "SOLUSDT",
		Template: "missing-template",
	}

	_, err := resolveStrategyParameters(nil, sc)
	if err == nil {
		t.Fatal("resolveStrategyParameters() error = nil, want unknown template error")
	}
	if !strings.Contains(err.Error(), "unknown template") {
		t.Fatalf("resolveStrategyParameters() error = %v, want unknown template", err)
	}
}

func TestNormalizeUniverseConfigUsesRouterConfigEntry(t *testing.T) {
	cfg := config.Config{}
	cfg.Universe.CandidateSymbols = []string{"BTCUSDT"}
	cfg.Universe.RouterConfig.StaticTemplateMap = map[string]string{
		"BTCUSDT": "btc-router",
	}
	cfg.Universe.RouterConfig.RangeTemplate = "range-router"
	cfg.Universe.RouterConfig.BTCTrendTemplate = "btc-trend-router"

	got := normalizeUniverseConfig(cfg)
	if got.RouterConfig.StaticTemplateMap["BTCUSDT"] != "btc-router" {
		t.Fatalf("static template = %s, want btc-router", got.RouterConfig.StaticTemplateMap["BTCUSDT"])
	}
	if got.RouterConfig.RangeTemplate != "range-router" {
		t.Fatalf("range template = %s, want range-router", got.RouterConfig.RangeTemplate)
	}
	if got.RouterConfig.BTCTrendTemplate != "btc-trend-router" {
		t.Fatalf("btc trend template = %s, want btc-trend-router", got.RouterConfig.BTCTrendTemplate)
	}
}

func TestNormalizeUniverseConfigBackfillsRouterTemplatesFromStrategies(t *testing.T) {
	cfg := config.Config{}
	cfg.Strategies = []config.StrategyConfig{
		{Symbol: "ETHUSDT", Template: "eth-core"},
	}
	cfg.Universe.RouterConfig.RangeTemplate = "range-core"

	got := normalizeUniverseConfig(cfg)
	if got.RouterConfig.StaticTemplateMap["ETHUSDT"] != "eth-core" {
		t.Fatalf("static template = %s, want eth-core", got.RouterConfig.StaticTemplateMap["ETHUSDT"])
	}
	if got.RouterConfig.RangeTemplate != "range-core" {
		t.Fatalf("range template = %s, want range-core", got.RouterConfig.RangeTemplate)
	}
}

func TestRecordLatestUniverseDesired(t *testing.T) {
	svcCtx := &ServiceContext{}
	svcCtx.RecordLatestUniverseDesired(universe.DesiredStrategy{
		Symbol:       "BTCUSDT",
		BaseTemplate: "btc-core",
		Template:     "btc-trend",
		Bucket:       "trend",
		Enabled:      true,
		Reason:       "market_state_trend",
	})

	got, ok := svcCtx.LatestUniverseDesired("BTCUSDT")
	if !ok {
		t.Fatal("LatestUniverseDesired() ok = false, want true")
	}
	if got.Template != "btc-trend" || got.Bucket != "trend" || got.Reason != "market_state_trend" {
		t.Fatalf("LatestUniverseDesired() = %+v, want btc-trend/trend/market_state_trend", got)
	}
}

func TestRecordLatestUniverseRuntimeStatus(t *testing.T) {
	svcCtx := &ServiceContext{}
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

	got, ok := svcCtx.LatestUniverseApplyResult("BTCUSDT")
	if !ok {
		t.Fatal("LatestUniverseApplyResult() ok = false, want true")
	}
	if got.Action != "defer_switch" || got.Reason != "open_position" || !got.Enabled || !got.HasOpenPosition {
		t.Fatalf("LatestUniverseApplyResult() = %+v, want defer_switch/open_position/true", got)
	}
	if got.RuntimeTemplate != "btc-core" {
		t.Fatalf("RuntimeTemplate = %s, want btc-core", got.RuntimeTemplate)
	}
}

func TestRecordLatestUniverseSnapshot(t *testing.T) {
	svcCtx := &ServiceContext{}
	svcCtx.RecordLatestUniverseSnapshot(universe.Snapshot{
		Symbol: "BTCUSDT",
		Regime1h: universe.RegimeFrame{
			Interval:    "1h",
			State:       marketstate.MarketStateTrendUp,
			RouteReason: "market_state_trend",
			UpdatedAt:   time.Date(2026, 5, 2, 8, 0, 0, 0, time.UTC),
		},
		Fusion: universe.RegimeFusion{
			PrimaryWeight: 0.7,
			ConfirmWeight: 0.3,
			FusedState:    marketstate.MarketStateTrendUp,
			FusedReason:   "h1_only",
			FusedScore:    0.75,
			UpdatedAt:     time.Date(2026, 5, 2, 8, 0, 0, 0, time.UTC),
		},
	})

	got, ok := svcCtx.LatestUniverseSnapshot("BTCUSDT")
	if !ok {
		t.Fatal("LatestUniverseSnapshot() ok = false, want true")
	}
	if got.Fusion.FusedState != marketstate.MarketStateTrendUp ||
		got.Fusion.FusedReason != "h1_only" ||
		got.Regime1h.RouteReason != "market_state_trend" {
		t.Fatalf("LatestUniverseSnapshot() = %+v, want trend_up/h1_only/market_state_trend", got)
	}
}

func TestUpsertStrategyLockedReusesExistingInstance(t *testing.T) {
	svcCtx := &ServiceContext{
		Config: config.Config{
			SignalLogDir: "data/signal",
		},
		strategies: map[string]*strategyengine.TrendFollowingStrategy{},
	}
	existing := strategyengine.NewTrendFollowingStrategy("BTCUSDT", map[string]float64{
		"strategy_variant": 0,
	}, nil, nil, "", nil)
	svcCtx.strategies["BTCUSDT"] = existing

	svcCtx.upsertStrategyLocked(&strategypb.StrategyConfig{
		Symbol:  "BTCUSDT",
		Enabled: true,
		Parameters: map[string]float64{
			"strategy_variant": 2,
		},
	})

	got := svcCtx.strategies["BTCUSDT"]
	if got == nil {
		t.Fatal("strategies[BTCUSDT] = nil, want reused strategy instance")
	}
	if got != existing {
		t.Fatal("upsertStrategyLocked() replaced strategy instance, want in-place reuse")
	}
}

func TestUpsertStrategyLockedHydratesWarmupHistoryOnEnable(t *testing.T) {
	svcCtx := &ServiceContext{
		Config: config.Config{
			SignalLogDir: "data/signal",
		},
		strategies:     map[string]*strategyengine.TrendFollowingStrategy{},
		strategyWarmup: map[string]strategyWarmupState{},
	}
	svcCtx.strategyWarmup["BTCUSDT"] = strategyWarmupState{
		Klines4h: []marketpb.Kline{
			{
				Symbol:   "BTCUSDT",
				Interval: "4h",
				IsClosed: true,
				OpenTime: time.Date(2026, 5, 3, 4, 0, 0, 0, time.UTC).UnixMilli(),
				Close:    98,
				Ema21:    99,
				Ema55:    100,
				Rsi:      45,
				Atr:      2.1,
			},
		},
		Klines1h: []marketpb.Kline{
			{
				Symbol:   "BTCUSDT",
				Interval: "1h",
				IsClosed: true,
				OpenTime: time.Date(2026, 5, 3, 8, 0, 0, 0, time.UTC).UnixMilli(),
				Close:    100,
				Ema21:    99,
				Ema55:    98,
				Rsi:      52,
				Atr:      1.2,
			},
		},
		Klines15m: []marketpb.Kline{
			{
				Symbol:   "BTCUSDT",
				Interval: "15m",
				IsClosed: true,
				OpenTime: time.Date(2026, 5, 3, 8, 45, 0, 0, time.UTC).UnixMilli(),
				Close:    101,
				Ema21:    100,
				Ema55:    99,
				Rsi:      55,
				Atr:      0.8,
			},
		},
	}

	svcCtx.upsertStrategyLocked(&strategypb.StrategyConfig{
		Symbol:  "BTCUSDT",
		Enabled: true,
		Parameters: map[string]float64{
			"strategy_variant": 2,
		},
	})

	got := svcCtx.strategies["BTCUSDT"]
	if got == nil {
		t.Fatal("strategies[BTCUSDT] = nil, want warm-started strategy")
	}
	if got.HistoryLen("4h") != 1 {
		t.Fatalf("HistoryLen(4h) = %d, want 1", got.HistoryLen("4h"))
	}
	if got.HistoryLen("1h") != 1 {
		t.Fatalf("HistoryLen(1h) = %d, want 1", got.HistoryLen("1h"))
	}
	if got.HistoryLen("15m") != 1 {
		t.Fatalf("HistoryLen(15m) = %d, want 1", got.HistoryLen("15m"))
	}
}

func TestStrategyWarmupStatusUsesRuntimeThenCache(t *testing.T) {
	svcCtx := &ServiceContext{
		strategies:     map[string]*strategyengine.TrendFollowingStrategy{},
		strategyWarmup: map[string]strategyWarmupState{},
	}
	strat := strategyengine.NewTrendFollowingStrategy("BTCUSDT", nil, nil, nil, "", nil)
	strat.WarmupKline(&marketpb.Kline{Symbol: "BTCUSDT", Interval: "1h", IsClosed: true})
	strat.WarmupKline(&marketpb.Kline{Symbol: "BTCUSDT", Interval: "15m", IsClosed: true})
	svcCtx.strategies["BTCUSDT"] = strat

	runtimeStatus := svcCtx.StrategyWarmupStatus("BTCUSDT")
	if runtimeStatus.Source != "runtime" ||
		runtimeStatus.Status != "warmup_incomplete" ||
		runtimeStatus.HistoryLen1h != 1 ||
		runtimeStatus.HistoryLen15m != 1 {
		t.Fatalf("StrategyWarmupStatus(runtime) = %+v, want runtime with 1h/15m history", runtimeStatus)
	}
	if len(runtimeStatus.IncompleteReasons) != 4 {
		t.Fatalf("StrategyWarmupStatus(runtime).IncompleteReasons = %+v, want all intervals marked insufficient", runtimeStatus.IncompleteReasons)
	}

	delete(svcCtx.strategies, "BTCUSDT")
	svcCtx.strategyWarmup["BTCUSDT"] = strategyWarmupState{
		Klines4h: []marketpb.Kline{{Symbol: "BTCUSDT", Interval: "4h", IsClosed: true}},
		Klines1m: []marketpb.Kline{{Symbol: "BTCUSDT", Interval: "1m", IsClosed: true}},
	}
	cacheStatus := svcCtx.StrategyWarmupStatus("BTCUSDT")
	if cacheStatus.Source != "cache" ||
		cacheStatus.Status != "warmup_incomplete" ||
		cacheStatus.HistoryLen4h != 1 ||
		cacheStatus.HistoryLen1m != 1 {
		t.Fatalf("StrategyWarmupStatus(cache) = %+v, want cache with 4h/1m history", cacheStatus)
	}
	if len(cacheStatus.IncompleteReasons) != 4 {
		t.Fatalf("StrategyWarmupStatus(cache).IncompleteReasons = %+v, want all intervals marked insufficient", cacheStatus.IncompleteReasons)
	}
}

func TestStrategyWarmupStatusMarksCompleteWhenAllIntervalsReachTargets(t *testing.T) {
	svcCtx := &ServiceContext{
		strategyWarmup: map[string]strategyWarmupState{},
	}
	svcCtx.RecordStrategyWarmupStatus("ETHUSDT", StrategyWarmupStatusView{
		HistoryLen4h:  int32(strategyWarmupLimit("4h")),
		HistoryLen1h:  int32(strategyWarmupLimit("1h")),
		HistoryLen15m: int32(strategyWarmupLimit("15m")),
		HistoryLen1m:  int32(strategyWarmupLimit("1m")),
		Source:        "cache",
	})

	got := svcCtx.StrategyWarmupStatus("ETHUSDT")
	if got.Source != "cache" || got.Status != "warmup_complete" {
		t.Fatalf("StrategyWarmupStatus() = %+v, want cache warmup_complete", got)
	}
	if len(got.IncompleteReasons) != 0 {
		t.Fatalf("StrategyWarmupStatus().IncompleteReasons = %+v, want empty", got.IncompleteReasons)
	}
}
