package svc

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
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

func TestLoadStrategyWarmupStateFromDiskRestoresLatestIntervals(t *testing.T) {
	baseDir := t.TempDir()
	writeWarmupLogForTest(t, baseDir, "SOLUSDT", "4h", "2026-05-03.jsonl", []string{
		`{"symbol":"SOLUSDT","interval":"4h","openTime":"2026-05-03T20:00:00.000Z","closeTime":"2026-05-03T23:59:59.999Z","eventTime":"2026-05-04T00:01:00.000Z","isClosed":true,"open":80,"high":81,"low":79,"close":80.5,"volume":100,"quoteVolume":1000,"takerBuyVolume":50,"takerBuyQuote":500,"isDirty":true,"dirtyReason":"incomplete_bucket","isTradable":false,"isFinal":false,"ema21":81,"ema55":82,"rsi":40,"atr":1.1}`,
	})
	writeWarmupLogForTest(t, baseDir, "SOLUSDT", "4h", "2026-05-04.jsonl", []string{
		`{"symbol":"SOLUSDT","interval":"4h","openTime":"2026-05-04T00:00:00.000Z","closeTime":"2026-05-04T03:59:59.999Z","eventTime":"2026-05-04T04:00:30.000Z","isClosed":true,"open":81,"high":82,"low":80,"close":81.5,"volume":101,"quoteVolume":1001,"takerBuyVolume":51,"takerBuyQuote":501,"isDirty":true,"dirtyReason":"incomplete_bucket","isTradable":false,"isFinal":false,"ema21":83,"ema55":84,"rsi":41,"atr":1.2}`,
		`{"symbol":"SOLUSDT","interval":"4h","openTime":"2026-05-04T00:00:00.000Z","closeTime":"2026-05-04T03:59:59.999Z","eventTime":"2026-05-04T04:01:00.000Z","isClosed":true,"open":81,"high":83,"low":80,"close":82.2,"volume":120,"quoteVolume":1100,"takerBuyVolume":60,"takerBuyQuote":550,"isDirty":false,"dirtyReason":"final_tradable","isTradable":true,"isFinal":true,"ema21":85,"ema55":86,"rsi":55,"atr":1.3}`,
	})
	writeWarmupLogForTest(t, baseDir, "SOLUSDT", "1h", "2026-05-04.jsonl", []string{
		`{"symbol":"SOLUSDT","interval":"1h","openTime":"2026-05-04T03:00:00.000Z","closeTime":"2026-05-04T03:59:59.999Z","eventTime":"2026-05-04T04:01:00.000Z","isClosed":true,"open":82,"high":83,"low":81,"close":82.5,"volume":90,"quoteVolume":900,"takerBuyVolume":45,"takerBuyQuote":450,"isDirty":false,"dirtyReason":"final_tradable","isTradable":true,"isFinal":true,"ema21":84,"ema55":85,"rsi":52,"atr":0.8}`,
	})
	writeWarmupLogForTest(t, baseDir, "SOLUSDT", "15m", "2026-05-04.jsonl", []string{
		`{"symbol":"SOLUSDT","interval":"15m","openTime":"2026-05-04T03:45:00.000Z","closeTime":"2026-05-04T03:59:59.999Z","eventTime":"2026-05-04T04:01:00.000Z","isClosed":true,"open":82.1,"high":82.8,"low":82,"close":82.6,"volume":40,"quoteVolume":400,"takerBuyVolume":20,"takerBuyQuote":200,"isDirty":false,"dirtyReason":"final_tradable","isTradable":true,"isFinal":true,"ema21":83,"ema55":84,"rsi":57,"atr":0.3}`,
	})
	writeWarmupLogForTest(t, baseDir, "SOLUSDT", "1m", "2026-05-04.jsonl", []string{
		`{"symbol":"SOLUSDT","interval":"1m","openTime":"2026-05-04T03:59:00.000Z","closeTime":"2026-05-04T03:59:59.999Z","eventTime":"2026-05-04T04:00:01.000Z","isClosed":true,"open":82.4,"high":82.6,"low":82.3,"close":82.5,"volume":10,"quoteVolume":100,"takerBuyVolume":5,"takerBuyQuote":50,"isDirty":false,"dirtyReason":"final_tradable","isTradable":true,"isFinal":true,"ema21":0,"ema55":0,"rsi":0,"atr":0}`,
	})

	state, err := loadStrategyWarmupStateFromDisk(baseDir, "SOLUSDT")
	if err != nil {
		t.Fatalf("loadStrategyWarmupStateFromDisk() error = %v", err)
	}
	if len(state.Klines4h) != 2 {
		t.Fatalf("len(Klines4h) = %d, want 2", len(state.Klines4h))
	}
	if len(state.Klines1h) != 1 || len(state.Klines15m) != 1 || len(state.Klines1m) != 1 {
		t.Fatalf("unexpected interval lengths: 1h=%d 15m=%d 1m=%d", len(state.Klines1h), len(state.Klines15m), len(state.Klines1m))
	}
	latest4h := state.Klines4h[len(state.Klines4h)-1]
	if !latest4h.IsFinal || !latest4h.IsTradable || latest4h.Close != 82.2 {
		t.Fatalf("latest 4h kline = %+v, want final tradable row", latest4h)
	}
}

func TestUpsertStrategyLockedLoadsWarmupHistoryFromDisk(t *testing.T) {
	baseDir := t.TempDir()
	writeWarmupLogForTest(t, baseDir, "BTCUSDT", "4h", "2026-05-04.jsonl", []string{
		`{"symbol":"BTCUSDT","interval":"4h","openTime":"2026-05-04T00:00:00.000Z","closeTime":"2026-05-04T03:59:59.999Z","eventTime":"2026-05-04T04:01:00.000Z","isClosed":true,"open":95000,"high":95500,"low":94800,"close":95200,"volume":100,"quoteVolume":1000,"takerBuyVolume":50,"takerBuyQuote":500,"isDirty":false,"dirtyReason":"final_tradable","isTradable":true,"isFinal":true,"ema21":94000,"ema55":93000,"rsi":60,"atr":500}`,
	})
	writeWarmupLogForTest(t, baseDir, "BTCUSDT", "1h", "2026-05-04.jsonl", []string{
		`{"symbol":"BTCUSDT","interval":"1h","openTime":"2026-05-04T03:00:00.000Z","closeTime":"2026-05-04T03:59:59.999Z","eventTime":"2026-05-04T04:01:00.000Z","isClosed":true,"open":95100,"high":95300,"low":95000,"close":95200,"volume":80,"quoteVolume":800,"takerBuyVolume":40,"takerBuyQuote":400,"isDirty":false,"dirtyReason":"final_tradable","isTradable":true,"isFinal":true,"ema21":94500,"ema55":93800,"rsi":58,"atr":200}`,
	})
	writeWarmupLogForTest(t, baseDir, "BTCUSDT", "15m", "2026-05-04.jsonl", []string{
		`{"symbol":"BTCUSDT","interval":"15m","openTime":"2026-05-04T03:45:00.000Z","closeTime":"2026-05-04T03:59:59.999Z","eventTime":"2026-05-04T04:01:00.000Z","isClosed":true,"open":95150,"high":95250,"low":95100,"close":95200,"volume":30,"quoteVolume":300,"takerBuyVolume":15,"takerBuyQuote":150,"isDirty":false,"dirtyReason":"final_tradable","isTradable":true,"isFinal":true,"ema21":95000,"ema55":94900,"rsi":56,"atr":80}`,
	})
	writeWarmupLogForTest(t, baseDir, "BTCUSDT", "1m", "2026-05-04.jsonl", []string{
		`{"symbol":"BTCUSDT","interval":"1m","openTime":"2026-05-04T03:59:00.000Z","closeTime":"2026-05-04T03:59:59.999Z","eventTime":"2026-05-04T04:00:01.000Z","isClosed":true,"open":95190,"high":95210,"low":95180,"close":95200,"volume":5,"quoteVolume":50,"takerBuyVolume":2.5,"takerBuyQuote":25,"isDirty":false,"dirtyReason":"final_tradable","isTradable":true,"isFinal":true,"ema21":0,"ema55":0,"rsi":0,"atr":0}`,
	})

	svcCtx := &ServiceContext{
		Config: config.Config{
			SignalLogDir: "data/signal",
			KlineLogDir:  baseDir,
		},
		strategies:     map[string]*strategyengine.TrendFollowingStrategy{},
		strategyWarmup: map[string]strategyWarmupState{},
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
		t.Fatal("strategies[BTCUSDT] = nil, want restored strategy")
	}
	if got.HistoryLen("4h") != 1 || got.HistoryLen("1h") != 1 || got.HistoryLen("15m") != 1 || got.HistoryLen("1m") != 1 {
		t.Fatalf(
			"history lengths = 4h:%d 1h:%d 15m:%d 1m:%d, want all 1",
			got.HistoryLen("4h"),
			got.HistoryLen("1h"),
			got.HistoryLen("15m"),
			got.HistoryLen("1m"),
		)
	}
}

func TestLoadStrategyWarmupStateFromClickHouseRestoresLatestIntervals(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		bodyBytes, err := ioReadAllForTest(r)
		if err != nil {
			t.Fatalf("read request body error = %v", err)
		}
		query := string(bodyBytes)
		w.Header().Set("Content-Type", "application/json")
		switch {
		case strings.Contains(query, "interval = '4h'"):
			fmt.Fprintln(w, `{"symbol":"SOLUSDT","interval":"4h","open_time_ms":"1777852800000","close_time_ms":"1777867199999","event_time_ms":"1777867230000","open":81,"high":82,"low":80,"close":81.5,"volume":101,"quote_volume":1001,"taker_buy_volume":51,"is_closed":1,"is_dirty":1,"dirty_reason":"incomplete_bucket","is_tradable":0,"is_final":0,"ema21":83,"ema55":84,"rsi":41,"atr":1.2}`)
			fmt.Fprintln(w, `{"symbol":"SOLUSDT","interval":"4h","open_time_ms":"1777852800000","close_time_ms":"1777867199999","event_time_ms":"1777867260000","open":81,"high":83,"low":80,"close":82.2,"volume":120,"quote_volume":1100,"taker_buy_volume":60,"is_closed":1,"is_dirty":0,"dirty_reason":"final_tradable","is_tradable":1,"is_final":1,"ema21":85,"ema55":86,"rsi":55,"atr":1.3}`)
			fmt.Fprintln(w, `{"symbol":"SOLUSDT","interval":"4h","open_time_ms":"1777838400000","close_time_ms":"1777852799999","event_time_ms":"1777852860000","open":80,"high":81,"low":79,"close":80.5,"volume":100,"quote_volume":1000,"taker_buy_volume":50,"is_closed":1,"is_dirty":1,"dirty_reason":"incomplete_bucket","is_tradable":0,"is_final":0,"ema21":81,"ema55":82,"rsi":40,"atr":1.1}`)
		case strings.Contains(query, "interval = '1h'"):
			fmt.Fprintln(w, `{"symbol":"SOLUSDT","interval":"1h","open_time_ms":"1777863600000","close_time_ms":"1777867199999","event_time_ms":"1777867260000","open":82,"high":83,"low":81,"close":82.5,"volume":90,"quote_volume":900,"taker_buy_volume":45,"is_closed":1,"is_dirty":0,"dirty_reason":"final_tradable","is_tradable":1,"is_final":1,"ema21":84,"ema55":85,"rsi":52,"atr":0.8}`)
		case strings.Contains(query, "interval = '15m'"):
			fmt.Fprintln(w, `{"symbol":"SOLUSDT","interval":"15m","open_time_ms":"1777866300000","close_time_ms":"1777867199999","event_time_ms":"1777867260000","open":82.1,"high":82.8,"low":82,"close":82.6,"volume":40,"quote_volume":400,"taker_buy_volume":20,"is_closed":1,"is_dirty":0,"dirty_reason":"final_tradable","is_tradable":1,"is_final":1,"ema21":83,"ema55":84,"rsi":57,"atr":0.3}`)
		case strings.Contains(query, "interval = '1m'"):
			fmt.Fprintln(w, `{"symbol":"SOLUSDT","interval":"1m","open_time_ms":"1777867140000","close_time_ms":"1777867199999","event_time_ms":"1777867201000","open":82.4,"high":82.6,"low":82.3,"close":82.5,"volume":10,"quote_volume":100,"taker_buy_volume":5,"is_closed":1,"is_dirty":0,"dirty_reason":"final_tradable","is_tradable":1,"is_final":1,"ema21":0,"ema55":0,"rsi":0,"atr":0}`)
		default:
			t.Fatalf("unexpected query: %s", query)
		}
	}))
	defer server.Close()

	state, err := loadStrategyWarmupStateFromClickHouse(context.Background(), config.ClickHouseConfig{
		Enabled:  true,
		Endpoint: server.URL,
		Database: "exchange_analytics",
	}, "SOLUSDT")
	if err != nil {
		t.Fatalf("loadStrategyWarmupStateFromClickHouse() error = %v", err)
	}
	if len(state.Klines4h) != 2 || len(state.Klines1h) != 1 || len(state.Klines15m) != 1 || len(state.Klines1m) != 1 {
		t.Fatalf("unexpected restored lengths: %+v", state)
	}
	latest4h := state.Klines4h[len(state.Klines4h)-1]
	if !latest4h.IsFinal || !latest4h.IsTradable || latest4h.Close != 82.2 {
		t.Fatalf("latest 4h kline = %+v, want final tradable row", latest4h)
	}
}

func TestEnsureStrategyWarmupLoadedLockedFallsBackToDiskWhenClickHouseFails(t *testing.T) {
	baseDir := t.TempDir()
	writeWarmupLogForTest(t, baseDir, "ETHUSDT", "4h", "2026-05-04.jsonl", []string{
		`{"symbol":"ETHUSDT","interval":"4h","openTime":"2026-05-04T00:00:00.000Z","closeTime":"2026-05-04T03:59:59.999Z","eventTime":"2026-05-04T04:01:00.000Z","isClosed":true,"open":1800,"high":1810,"low":1790,"close":1805,"volume":100,"quoteVolume":1000,"takerBuyVolume":50,"takerBuyQuote":500,"isDirty":false,"dirtyReason":"final_tradable","isTradable":true,"isFinal":true,"ema21":1780,"ema55":1760,"rsi":61,"atr":20}`,
	})
	writeWarmupLogForTest(t, baseDir, "ETHUSDT", "1h", "2026-05-04.jsonl", []string{
		`{"symbol":"ETHUSDT","interval":"1h","openTime":"2026-05-04T03:00:00.000Z","closeTime":"2026-05-04T03:59:59.999Z","eventTime":"2026-05-04T04:01:00.000Z","isClosed":true,"open":1801,"high":1808,"low":1798,"close":1805,"volume":80,"quoteVolume":800,"takerBuyVolume":40,"takerBuyQuote":400,"isDirty":false,"dirtyReason":"final_tradable","isTradable":true,"isFinal":true,"ema21":1790,"ema55":1770,"rsi":58,"atr":10}`,
	})
	writeWarmupLogForTest(t, baseDir, "ETHUSDT", "15m", "2026-05-04.jsonl", []string{
		`{"symbol":"ETHUSDT","interval":"15m","openTime":"2026-05-04T03:45:00.000Z","closeTime":"2026-05-04T03:59:59.999Z","eventTime":"2026-05-04T04:01:00.000Z","isClosed":true,"open":1803,"high":1806,"low":1800,"close":1805,"volume":30,"quoteVolume":300,"takerBuyVolume":15,"takerBuyQuote":150,"isDirty":false,"dirtyReason":"final_tradable","isTradable":true,"isFinal":true,"ema21":1800,"ema55":1795,"rsi":56,"atr":4}`,
	})
	writeWarmupLogForTest(t, baseDir, "ETHUSDT", "1m", "2026-05-04.jsonl", []string{
		`{"symbol":"ETHUSDT","interval":"1m","openTime":"2026-05-04T03:59:00.000Z","closeTime":"2026-05-04T03:59:59.999Z","eventTime":"2026-05-04T04:00:01.000Z","isClosed":true,"open":1804,"high":1805,"low":1803,"close":1805,"volume":5,"quoteVolume":50,"takerBuyVolume":2.5,"takerBuyQuote":25,"isDirty":false,"dirtyReason":"final_tradable","isTradable":true,"isFinal":true,"ema21":0,"ema55":0,"rsi":0,"atr":0}`,
	})

	svcCtx := &ServiceContext{
		Config: config.Config{
			KlineLogDir: baseDir,
			ClickHouse: config.ClickHouseConfig{
				Enabled:  true,
				Endpoint: "http://127.0.0.1:1",
				Timeout:  50 * time.Millisecond,
			},
		},
		strategyWarmup: map[string]strategyWarmupState{},
	}

	svcCtx.ensureStrategyWarmupLoadedLocked("ETHUSDT")

	state := svcCtx.strategyWarmup["ETHUSDT"]
	if !strategyWarmupStateIsComplete(state) {
		t.Fatalf("strategyWarmupState = %+v, want disk fallback to complete warmup", state)
	}
}

func writeWarmupLogForTest(t *testing.T, baseDir, symbol, interval, fileName string, lines []string) {
	t.Helper()
	dir := filepath.Join(baseDir, symbol, interval)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		t.Fatalf("MkdirAll(%s) error = %v", dir, err)
	}
	path := filepath.Join(dir, fileName)
	content := strings.Join(lines, "\n")
	if content != "" {
		content += "\n"
	}
	if err := os.WriteFile(path, []byte(content), 0o644); err != nil {
		t.Fatalf("WriteFile(%s) error = %v", path, err)
	}
}

func ioReadAllForTest(r *http.Request) ([]byte, error) {
	if r == nil || r.Body == nil {
		return nil, nil
	}
	defer r.Body.Close()
	return io.ReadAll(r.Body)
}
