package svc

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"exchange-system/app/strategy/rpc/internal/strategyrouter"
	"exchange-system/common/regimejudge"
	"fmt"
	"io"
	"log"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"

	"exchange-system/app/execution/rpc/executionservice"
	"exchange-system/app/strategy/rpc/internal/config"
	"exchange-system/app/strategy/rpc/internal/kafka"
	"exchange-system/app/strategy/rpc/internal/marketstate"
	strategyengine "exchange-system/app/strategy/rpc/internal/strategy"
	harvestpathmodel "exchange-system/app/strategy/rpc/internal/strategy/harvestpath"
	"exchange-system/app/strategy/rpc/internal/universe"
	"exchange-system/app/strategy/rpc/internal/weights"
	"exchange-system/common/featureengine"
	commonkafka "exchange-system/common/kafka"
	"exchange-system/common/pb/market"
	strategypb "exchange-system/common/pb/strategy"

	"github.com/zeromicro/go-zero/zrpc"
)

type ServiceContext struct {
	Config config.Config

	signalProducer           *kafka.Producer
	harvestPathProducer      *kafka.Producer
	harvestPathLSTMPredictor *harvestpathmodel.LSTMPredictor
	marketConsumer           *kafka.Consumer
	depthConsumer            *kafka.Consumer
	tradeConsumer            *kafka.Consumer
	executionCli             executionservice.ExecutionService

	mu                  sync.RWMutex
	strategies          map[string]*strategyengine.TrendFollowingStrategy
	secondBarAssemblers map[string]*SecondBarAssembler
	secondBarLogMu      sync.Mutex
	secondBarLogAt      map[string]time.Time
	strategyConfigs     map[string]config.StrategyConfig
	universeSelector    *universe.Selector
	marketStateDetector marketstate.Detector
	marketStateConfig   marketstate.Config
	marketStateLogs     *marketstate.LogState
	weightEngine        weights.Engine
	weightLogs          *weights.LogState
	latestWeights       map[string]weights.Recommendation
	latestUniverseView  map[string]universe.DesiredStrategy
	latestUniverseApply map[string]universeApplyResult
	latestUniverseSnap  map[string]universe.Snapshot
	universeSnapshots   map[string]universe.Snapshot
	strategyWarmup      map[string]strategyWarmupState
	universeStates      map[string]universeRuntimeState
	universeLogs        *universeLogState
	analyticsWriter     *strategyClickHouseWriter
	universeStartedAt   time.Time
	cancel              context.CancelFunc
}

type universeRuntimeState struct {
	Template       string
	LastEnabledAt  time.Time
	LastDisabledAt time.Time
}

type universeApplyResult struct {
	Action          string
	Reason          string
	Enabled         bool
	HasStrategy     bool
	HasOpenPosition bool
	CurrentTemplate string
	RuntimeTemplate string
}

type strategyWarmupState struct {
	Klines4h  []market.Kline
	Klines1h  []market.Kline
	Klines15m []market.Kline
	Klines1m  []market.Kline
}

const secondBarDepthFallbackParam = "exit_emergency_1s_depth_fallback"

// StrategyWarmupStatusView 汇总状态查询需要的多周期 warmup 长度，并标记数据来源。
type StrategyWarmupStatusView struct {
	HistoryLen4h      int32
	HistoryLen1h      int32
	HistoryLen15m     int32
	HistoryLen1m      int32
	Source            string
	Status            string
	IncompleteReasons []string
}

// LatestSecondBarStatusView 汇总最近一条秒级行情的来源与关键价格，供状态接口直接确认 fallback 是否在工作。
type LatestSecondBarStatusView struct {
	OpenTimeMs  int64
	CloseTimeMs int64
	Open        float64
	High        float64
	Low         float64
	Close       float64
	Volume      float64
	IsFinal     bool
	Synthetic   bool
	Source      string
}

// resolveStrategyParameters 合并模板参数、显式参数和 overrides，生成最终策略参数。
func resolveStrategyParameters(templates map[string]map[string]float64, sc config.StrategyConfig) (map[string]float64, error) {
	params := make(map[string]float64)
	if sc.Template != "" {
		tmpl, ok := templates[sc.Template]
		if !ok {
			return nil, fmt.Errorf("strategy %s symbol %s references unknown template %q", sc.Name, sc.Symbol, sc.Template)
		}
		for k, v := range tmpl {
			params[k] = v
		}
	}
	for k, v := range sc.Parameters {
		params[k] = v
	}
	for k, v := range sc.Overrides {
		params[k] = v
	}
	return params, nil
}

// normalizeUniverseConfig 为 UniverseSelector 填充保守默认值，保证稀疏配置也能运行。
func normalizeUniverseConfig(cfg config.Config) universe.Config {
	uc := cfg.Universe
	if uc.BootstrapDuration <= 0 {
		uc.BootstrapDuration = 5 * time.Minute
	}
	if uc.EvaluateInterval <= 0 {
		uc.EvaluateInterval = 30 * time.Second
	}
	if uc.FreshnessWindow <= 0 {
		uc.FreshnessWindow = 3 * time.Minute
	}
	if uc.RequireFinal == false && uc.RequireTradable == false && uc.RequireClean == false {
		uc.RequireFinal = true
		uc.RequireTradable = true
		uc.RequireClean = true
	}
	candidates := uc.CandidateSymbols
	if len(candidates) == 0 {
		for _, sc := range cfg.Strategies {
			if sc.Symbol != "" {
				candidates = append(candidates, sc.Symbol)
			}
		}
	}
	return universe.Config{
		CandidateSymbols: candidates,
		FreshnessWindow:  uc.FreshnessWindow,
		RequireFinal:     uc.RequireFinal,
		RequireTradable:  uc.RequireTradable,
		RequireClean:     uc.RequireClean,
		RouterConfig:     buildUniverseRouterConfig(cfg),
	}
}

// buildUniverseRouterConfig 统一从 Universe.RouterConfig 构造路由配置。
func buildUniverseRouterConfig(cfg config.Config) strategyrouter.Config {
	uc := cfg.Universe
	routerCfg := strategyrouter.Config{
		StaticTemplateMap:     cloneRouterStaticTemplateMap(uc.RouterConfig.StaticTemplateMap),
		RangeTemplate:         uc.RouterConfig.RangeTemplate,
		BreakoutTemplate:      uc.RouterConfig.BreakoutTemplate,
		BTCTrendTemplate:      uc.RouterConfig.BTCTrendTemplate,
		BTCTrendAtrPctMax:     uc.RouterConfig.BTCTrendAtrPctMax,
		HighBetaSafeTemplate:  uc.RouterConfig.HighBetaSafeTemplate,
		HighBetaSafeSymbols:   append([]string(nil), uc.RouterConfig.HighBetaSafeSymbols...),
		HighBetaSafeAtrPct:    uc.RouterConfig.HighBetaSafeAtrPct,
		HighBetaDisableAtrPct: uc.RouterConfig.HighBetaDisableAtrPct,
	}
	if routerCfg.StaticTemplateMap == nil {
		routerCfg.StaticTemplateMap = make(map[string]string)
	}
	for _, sc := range cfg.Strategies {
		if sc.Symbol == "" || sc.Template == "" {
			continue
		}
		if _, ok := routerCfg.StaticTemplateMap[sc.Symbol]; !ok {
			routerCfg.StaticTemplateMap[sc.Symbol] = sc.Template
		}
	}
	return routerCfg.Clone()
}

// cloneRouterStaticTemplateMap 复制 Universe 中的静态模板映射，避免运行时共享底层 map。
func cloneRouterStaticTemplateMap(in map[string]string) map[string]string {
	if len(in) == 0 {
		return nil
	}
	out := make(map[string]string, len(in))
	for symbol, template := range in {
		out[symbol] = template
	}
	return out
}

// isUniverseBootstrap 判断当前是否仍处于 Universe 冷启动观测窗口内。
func (s *ServiceContext) isUniverseBootstrap(now time.Time) bool {
	if s == nil || s.universeSelector == nil {
		return false
	}
	startedAt := s.universeStartedAt
	if startedAt.IsZero() {
		return false
	}
	d := s.Config.Universe.BootstrapDuration
	if d <= 0 {
		d = 5 * time.Minute
	}
	return now.Sub(startedAt) < d
}

// cloneStrategyConfig 复制一份策略配置，避免运行时调度直接修改原始配置对象。
func cloneStrategyConfig(sc config.StrategyConfig) config.StrategyConfig {
	out := config.StrategyConfig{
		Name:       sc.Name,
		Symbol:     sc.Symbol,
		Enabled:    sc.Enabled,
		Template:   sc.Template,
		Parameters: make(map[string]float64),
		Overrides:  make(map[string]float64),
	}
	for k, v := range sc.Parameters {
		out.Parameters[k] = v
	}
	for k, v := range sc.Overrides {
		out.Overrides[k] = v
	}
	return out
}

// buildRuntimeStrategyConfig 构造一份用于运行时 Upsert 的策略配置快照。
func (s *ServiceContext) buildRuntimeStrategyConfig(symbol, template string, enabled bool) config.StrategyConfig {
	if s != nil {
		if base, ok := s.strategyConfigs[symbol]; ok {
			cfg := cloneStrategyConfig(base)
			cfg.Enabled = enabled
			if template != "" {
				cfg.Template = template
			}
			return cfg
		}
	}
	return config.StrategyConfig{
		Name:     "trend-following",
		Symbol:   symbol,
		Enabled:  enabled,
		Template: template,
	}
}

// applyStrategyConfig 解析模板参数并把策略更新应用到当前运行时实例表。
func (s *ServiceContext) applyStrategyConfig(sc config.StrategyConfig, now time.Time) error {
	params, err := resolveStrategyParameters(s.Config.Templates, sc)
	if err != nil {
		return err
	}
	s.UpsertStrategy(&strategypb.StrategyConfig{
		Symbol:     sc.Symbol,
		Name:       sc.Name,
		Enabled:    sc.Enabled,
		Parameters: params,
	})
	s.mu.Lock()
	defer s.mu.Unlock()
	state := s.universeStates[sc.Symbol]
	state.Template = sc.Template
	if sc.Enabled {
		state.LastEnabledAt = now
	} else {
		state.LastDisabledAt = now
	}
	s.universeStates[sc.Symbol] = state
	return nil
}

// updateUniverseSnapshot 记录 1m/15m/1h 多周期 K 线快照，供 Universe 融合判态与健康门禁复用。
func (s *ServiceContext) updateUniverseSnapshot(kline *market.Kline) {
	if s == nil || s.universeSelector == nil || kline == nil || kline.Symbol == "" {
		return
	}
	switch kline.Interval {
	case "1m", "15m", "1h":
	default:
		return
	}
	features := featureengine.BuildFromKline(kline)
	frame := universe.KlineFrame{
		Symbol:      features.Symbol,
		Interval:    features.Timeframe,
		UpdatedAt:   features.UpdatedAt,
		LastEventMs: kline.EventTime,
		IsDirty:     kline.IsDirty,
		IsTradable:  kline.IsTradable,
		IsFinal:     kline.IsFinal,
		Close:       features.Close,
		Atr:         features.Atr,
		Volume:      features.Volume,
		Ema21:       features.Ema21,
		Ema55:       features.Ema55,
		Rsi:         features.Rsi,
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	snapshot := s.universeSnapshots[kline.Symbol]
	snapshot.Symbol = features.Symbol
	switch kline.Interval {
	case "1m":
		snapshot.UpdatedAt = features.UpdatedAt
		snapshot.LastEventMs = kline.EventTime
		snapshot.IsDirty = kline.IsDirty
		snapshot.IsTradable = kline.IsTradable
		snapshot.IsFinal = kline.IsFinal
		snapshot.LastInterval = features.Timeframe
		snapshot.Close = features.Close
		snapshot.Atr = features.Atr
		snapshot.Volume = features.Volume
		snapshot.Ema21 = features.Ema21
		snapshot.Ema55 = features.Ema55
		snapshot.Kline1m = frame
	case "15m":
		snapshot.Kline15m = frame
	case "1h":
		snapshot.Kline1h = frame
	}
	s.universeSnapshots[kline.Symbol] = snapshot
}

// runUniverseLoop 周期性运行 Universe 评估和调度逻辑。
func (s *ServiceContext) runUniverseLoop(ctx context.Context) {
	if s == nil || s.universeSelector == nil {
		return
	}
	interval := s.Config.Universe.EvaluateInterval
	if interval <= 0 {
		interval = 30 * time.Second
	}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case now := <-ticker.C:
			s.evaluateUniverse(now.UTC())
		}
	}
}

// cacheStrategyWarmupKline 保存最近多周期K线，供策略实例被重新创建时快速回灌本地缓存。
func (s *ServiceContext) cacheStrategyWarmupKline(kline *market.Kline) {
	if s == nil || kline == nil || kline.Symbol == "" || !kline.IsClosed {
		return
	}
	limit := strategyWarmupLimit(kline.Interval)
	if limit <= 0 {
		return
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	state := s.strategyWarmup[kline.Symbol]
	historyLenBefore := strategyWarmupHistoryLen(state, kline.Interval)
	item := *kline
	switch kline.Interval {
	case "4h":
		state.Klines4h = append(state.Klines4h, item)
		if len(state.Klines4h) > limit {
			state.Klines4h = state.Klines4h[len(state.Klines4h)-limit:]
		}
	case "1h":
		state.Klines1h = append(state.Klines1h, item)
		if len(state.Klines1h) > limit {
			state.Klines1h = state.Klines1h[len(state.Klines1h)-limit:]
		}
	case "15m":
		state.Klines15m = append(state.Klines15m, item)
		if len(state.Klines15m) > limit {
			state.Klines15m = state.Klines15m[len(state.Klines15m)-limit:]
		}
	case "1m":
		state.Klines1m = append(state.Klines1m, item)
		if len(state.Klines1m) > limit {
			state.Klines1m = state.Klines1m[len(state.Klines1m)-limit:]
		}
	default:
		return
	}
	s.strategyWarmup[kline.Symbol] = state
	historyLenAfter := strategyWarmupHistoryLen(state, kline.Interval)
	log.Printf(
		"[strategy-warmup] cache symbol=%s interval=%s history_len_before=%d history_len_after=%d cache_4h_count=%d cache_1h_count=%d cache_15m_count=%d cache_1m_count=%d",
		kline.Symbol,
		kline.Interval,
		historyLenBefore,
		historyLenAfter,
		len(state.Klines4h),
		len(state.Klines1h),
		len(state.Klines15m),
		len(state.Klines1m),
	)
}

// hydrateStrategyWarmupLocked 把 ServiceContext 缓存的最近K线回灌到新策略实例，减少 disable 后重新 enable 的冷启动窗口。
func (s *ServiceContext) hydrateStrategyWarmupLocked(symbol string, strat *strategyengine.TrendFollowingStrategy) {
	if s == nil || strat == nil || symbol == "" {
		return
	}
	historyLenBefore := strategyRuntimeWarmupTotal(strat)
	state, ok := s.strategyWarmup[symbol]
	if !ok {
		log.Printf(
			"[strategy-warmup] hydrate symbol=%s interval=all history_len_before=%d history_len_after=%d hydrate_4h_count=%d hydrate_1h_count=%d hydrate_15m_count=%d hydrate_1m_count=%d",
			symbol,
			historyLenBefore,
			historyLenBefore,
			0,
			0,
			0,
			0,
		)
		return
	}
	for i := range state.Klines4h {
		item := state.Klines4h[i]
		strat.WarmupKline(&item)
	}
	for i := range state.Klines1h {
		item := state.Klines1h[i]
		strat.WarmupKline(&item)
	}
	for i := range state.Klines15m {
		item := state.Klines15m[i]
		strat.WarmupKline(&item)
	}
	for i := range state.Klines1m {
		item := state.Klines1m[i]
		strat.WarmupKline(&item)
	}
	historyLenAfter := strategyRuntimeWarmupTotal(strat)
	log.Printf(
		"[strategy-warmup] hydrate symbol=%s interval=all history_len_before=%d history_len_after=%d hydrate_4h_count=%d hydrate_1h_count=%d hydrate_15m_count=%d hydrate_1m_count=%d",
		symbol,
		historyLenBefore,
		historyLenAfter,
		len(state.Klines4h),
		len(state.Klines1h),
		len(state.Klines15m),
		len(state.Klines1m),
	)
}

// strategyWarmupHistoryLen 返回指定周期当前缓存长度，便于统一输出 warmup 诊断日志。
func strategyWarmupHistoryLen(state strategyWarmupState, interval string) int {
	switch interval {
	case "4h":
		return len(state.Klines4h)
	case "1h":
		return len(state.Klines1h)
	case "15m":
		return len(state.Klines15m)
	case "1m":
		return len(state.Klines1m)
	default:
		return 0
	}
}

// strategyRuntimeWarmupTotal 汇总策略实例当前已回灌的总K线数量，方便对比 hydrate 前后差异。
func strategyRuntimeWarmupTotal(strat *strategyengine.TrendFollowingStrategy) int {
	if strat == nil {
		return 0
	}
	return strat.HistoryLen("4h") +
		strat.HistoryLen("1h") +
		strat.HistoryLen("15m") +
		strat.HistoryLen("1m")
}

// strategyWarmupLimit 返回各周期需要保留的最近K线数量，和策略内部窗口上限保持一致。
func strategyWarmupLimit(interval string) int {
	switch interval {
	case "4h":
		return 60
	case "1h":
		return 70
	case "15m":
		return 80
	case "1m":
		return 120
	default:
		return 0
	}
}

// ensureStrategyWarmupLoadedLocked 在创建策略实例前按需从 ClickHouse 和共享 warmup 目录恢复最近多周期快照。
func (s *ServiceContext) ensureStrategyWarmupLoadedLocked(symbol string) {
	if s == nil || symbol == "" || s.strategyWarmup == nil {
		return
	}
	state, ok := s.strategyWarmup[symbol]
	if ok && strategyWarmupStateIsComplete(state) {
		return
	}

	sources := make([]string, 0, 2)
	clickhouseLoaded, err := loadStrategyWarmupStateFromClickHouse(context.Background(), s.Config.ClickHouse, symbol)
	if err != nil {
		log.Printf("[strategy-warmup] restore symbol=%s source=clickhouse failed: %v", symbol, err)
	} else if strategyWarmupStateHasData(clickhouseLoaded) {
		state = mergeStrategyWarmupState(state, clickhouseLoaded)
		sources = append(sources, "clickhouse")
	}

	if !strategyWarmupStateIsComplete(state) {
		diskLoaded, diskErr := loadStrategyWarmupStateFromDisk(s.Config.SharedWarmupDir, symbol)
		if diskErr != nil {
			log.Printf(
				"[strategy-warmup] restore symbol=%s source=shared_warmup shared_dir=%s failed: %v",
				symbol,
				formatStrategyWarmupSharedDir(s.Config.SharedWarmupDir),
				diskErr,
			)
		} else if strategyWarmupStateHasData(diskLoaded) {
			state = mergeStrategyWarmupState(state, diskLoaded)
			sources = append(sources, "shared_warmup")
		}
	}

	if !strategyWarmupStateHasData(state) {
		log.Printf(
			"[strategy-warmup] restore symbol=%s shared_dir=%s skipped reason=no_clickhouse_or_shared_warmup_history",
			symbol,
			formatStrategyWarmupSharedDir(s.Config.SharedWarmupDir),
		)
		return
	}

	s.strategyWarmup[symbol] = state
	source := strings.Join(sources, "+")
	sharedDir := formatStrategyWarmupSharedDir(s.Config.SharedWarmupDir)
	restoredFromShared := strategyWarmupRestoredFromShared(source)
	log.Printf(
		"[strategy-warmup] symbol=%s interval=all source=%s severity=info event=restore_count shared_dir=%s restored_from_shared=%t %s",
		symbol,
		source,
		sharedDir,
		restoredFromShared,
		formatStrategyWarmupCountFields(state),
	)
	log.Printf(
		"[strategy-warmup] symbol=%s interval=all source=%s severity=info event=restore_sync shared_dir=%s restored_from_shared=%t %s",
		symbol,
		source,
		sharedDir,
		restoredFromShared,
		formatStrategyWarmupSyncFields(state, clickhouseLoaded),
	)
	logStrategyWarmupDeltaEvent(symbol, source, "4h", state.Klines4h, clickhouseLoaded.Klines4h)
	logStrategyWarmupDeltaEvent(symbol, source, "1h", state.Klines1h, clickhouseLoaded.Klines1h)
	logStrategyWarmupDeltaEvent(symbol, source, "15m", state.Klines15m, clickhouseLoaded.Klines15m)
	logStrategyWarmupDeltaEvent(symbol, source, "1m", state.Klines1m, clickhouseLoaded.Klines1m)
}

// loadStrategyWarmupStateFromClickHouse 从 ClickHouse 的 kline_fact 恢复策略启动所需的最近多周期闭 K 线。
func loadStrategyWarmupStateFromClickHouse(ctx context.Context, cfg config.ClickHouseConfig, symbol string) (strategyWarmupState, error) {
	state := strategyWarmupState{}
	symbol = strings.ToUpper(strings.TrimSpace(symbol))
	if symbol == "" {
		return state, nil
	}

	client, err := newStrategyWarmupClickHouseClient(cfg)
	if err != nil {
		return state, err
	}
	if client == nil {
		return state, nil
	}

	if state.Klines4h, err = client.queryLatestWarmupKlines(ctx, symbol, "4h", strategyWarmupLimit("4h")); err != nil {
		return state, err
	}
	if state.Klines1h, err = client.queryLatestWarmupKlines(ctx, symbol, "1h", strategyWarmupLimit("1h")); err != nil {
		return state, err
	}
	if state.Klines15m, err = client.queryLatestWarmupKlines(ctx, symbol, "15m", strategyWarmupLimit("15m")); err != nil {
		return state, err
	}
	if state.Klines1m, err = client.queryLatestWarmupKlines(ctx, symbol, "1m", strategyWarmupLimit("1m")); err != nil {
		return state, err
	}
	return state, nil
}

// loadStrategyWarmupStateFromDisk 从本地 K 线日志恢复策略启动所需的最近多周期闭 K 线。
func loadStrategyWarmupStateFromDisk(baseDir, symbol string) (strategyWarmupState, error) {
	state := strategyWarmupState{}
	baseDir = strings.TrimSpace(baseDir)
	symbol = strings.TrimSpace(symbol)
	if baseDir == "" || symbol == "" {
		return state, nil
	}

	var err error
	if state.Klines4h, err = loadStrategyWarmupIntervalFromDisk(baseDir, symbol, "4h"); err != nil {
		return state, err
	}
	if state.Klines1h, err = loadStrategyWarmupIntervalFromDisk(baseDir, symbol, "1h"); err != nil {
		return state, err
	}
	if state.Klines15m, err = loadStrategyWarmupIntervalFromDisk(baseDir, symbol, "15m"); err != nil {
		return state, err
	}
	if state.Klines1m, err = loadStrategyWarmupIntervalFromDisk(baseDir, symbol, "1m"); err != nil {
		return state, err
	}
	return state, nil
}

// loadStrategyWarmupIntervalFromDisk 读取指定周期最近的本地 jsonl 日志，并按 openTime 去重保留最佳版本。
func loadStrategyWarmupIntervalFromDisk(baseDir, symbol, interval string) ([]market.Kline, error) {
	limit := strategyWarmupLimit(interval)
	if limit <= 0 {
		return nil, nil
	}

	files, err := listStrategyWarmupFiles(baseDir, symbol, interval)
	if err != nil {
		return nil, err
	}
	if len(files) == 0 {
		return nil, nil
	}

	bestByOpenTime := make(map[int64]market.Kline, limit)
	for fileIndex := len(files) - 1; fileIndex >= 0; fileIndex-- {
		file, err := os.Open(files[fileIndex])
		if err != nil {
			return nil, fmt.Errorf("open warmup file %s: %w", files[fileIndex], err)
		}

		scanner := bufio.NewScanner(file)
		for scanner.Scan() {
			kline, ok, err := parseStrategyWarmupKlineLog(scanner.Bytes(), symbol, interval)
			if err != nil {
				_ = file.Close()
				return nil, fmt.Errorf("parse warmup line %s: %w", files[fileIndex], err)
			}
			if !ok {
				continue
			}
			existing, exists := bestByOpenTime[kline.OpenTime]
			if !exists || shouldReplaceStrategyWarmupKline(existing, kline) {
				bestByOpenTime[kline.OpenTime] = kline
			}
		}
		scanErr := scanner.Err()
		_ = file.Close()
		if scanErr != nil {
			return nil, fmt.Errorf("scan warmup file %s: %w", files[fileIndex], scanErr)
		}
		if len(bestByOpenTime) >= limit {
			break
		}
	}

	if len(bestByOpenTime) == 0 {
		return nil, nil
	}

	openTimes := make([]int64, 0, len(bestByOpenTime))
	for openTime := range bestByOpenTime {
		openTimes = append(openTimes, openTime)
	}
	sort.Slice(openTimes, func(i, j int) bool {
		return openTimes[i] < openTimes[j]
	})
	if len(openTimes) > limit {
		openTimes = openTimes[len(openTimes)-limit:]
	}

	result := make([]market.Kline, 0, len(openTimes))
	for _, openTime := range openTimes {
		result = append(result, bestByOpenTime[openTime])
	}
	return result, nil
}

// listStrategyWarmupFiles 返回指定 symbol/interval 的本地历史文件，并按日期升序排列。
func listStrategyWarmupFiles(baseDir, symbol, interval string) ([]string, error) {
	dir := filepath.Join(baseDir, symbol, interval)
	entries, err := os.ReadDir(dir)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil
		}
		return nil, fmt.Errorf("read warmup dir %s: %w", dir, err)
	}

	files := make([]string, 0, len(entries))
	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}
		if filepath.Ext(entry.Name()) != ".jsonl" {
			continue
		}
		files = append(files, filepath.Join(dir, entry.Name()))
	}
	sort.Strings(files)
	return files, nil
}

// parseStrategyWarmupKlineLog 把本地 jsonl 日志还原为可回灌的 protobuf K 线对象。
func parseStrategyWarmupKlineLog(line []byte, symbol, interval string) (market.Kline, bool, error) {
	type klineLogEntry struct {
		Symbol         string  `json:"symbol"`
		Interval       string  `json:"interval"`
		OpenTime       string  `json:"openTime"`
		CloseTime      string  `json:"closeTime"`
		EventTime      string  `json:"eventTime"`
		IsClosed       bool    `json:"isClosed"`
		Open           float64 `json:"open"`
		High           float64 `json:"high"`
		Low            float64 `json:"low"`
		Close          float64 `json:"close"`
		Volume         float64 `json:"volume"`
		QuoteVolume    float64 `json:"quoteVolume"`
		TakerBuyVolume float64 `json:"takerBuyVolume"`
		TakerBuyQuote  float64 `json:"takerBuyQuote"`
		IsDirty        bool    `json:"isDirty"`
		DirtyReason    string  `json:"dirtyReason"`
		IsTradable     bool    `json:"isTradable"`
		IsFinal        bool    `json:"isFinal"`
		Ema21          float64 `json:"ema21"`
		Ema55          float64 `json:"ema55"`
		Rsi            float64 `json:"rsi"`
		Atr            float64 `json:"atr"`
	}

	var entry klineLogEntry
	if err := json.Unmarshal(line, &entry); err != nil {
		return market.Kline{}, false, err
	}
	if entry.Symbol != symbol || entry.Interval != interval || !entry.IsClosed {
		return market.Kline{}, false, nil
	}

	openTime, err := time.Parse(time.RFC3339Nano, entry.OpenTime)
	if err != nil {
		return market.Kline{}, false, fmt.Errorf("parse openTime %q: %w", entry.OpenTime, err)
	}
	closeTime, err := time.Parse(time.RFC3339Nano, entry.CloseTime)
	if err != nil {
		return market.Kline{}, false, fmt.Errorf("parse closeTime %q: %w", entry.CloseTime, err)
	}
	eventTime, err := time.Parse(time.RFC3339Nano, entry.EventTime)
	if err != nil {
		return market.Kline{}, false, fmt.Errorf("parse eventTime %q: %w", entry.EventTime, err)
	}

	return market.Kline{
		Symbol:         entry.Symbol,
		Interval:       entry.Interval,
		OpenTime:       openTime.UnixMilli(),
		CloseTime:      closeTime.UnixMilli(),
		EventTime:      eventTime.UnixMilli(),
		IsClosed:       entry.IsClosed,
		Open:           entry.Open,
		High:           entry.High,
		Low:            entry.Low,
		Close:          entry.Close,
		Volume:         entry.Volume,
		QuoteVolume:    entry.QuoteVolume,
		TakerBuyVolume: entry.TakerBuyVolume,
		TakerBuyQuote:  entry.TakerBuyQuote,
		IsDirty:        entry.IsDirty,
		DirtyReason:    entry.DirtyReason,
		IsTradable:     entry.IsTradable,
		IsFinal:        entry.IsFinal,
		Ema21:          entry.Ema21,
		Ema55:          entry.Ema55,
		Rsi:            entry.Rsi,
		Atr:            entry.Atr,
	}, true, nil
}

// shouldReplaceStrategyWarmupKline 比较同一根 K 线的多个版本，优先保留更完整、更可交易的快照。
func shouldReplaceStrategyWarmupKline(current, candidate market.Kline) bool {
	currentScore := scoreStrategyWarmupKline(current)
	candidateScore := scoreStrategyWarmupKline(candidate)
	if candidateScore != currentScore {
		return candidateScore > currentScore
	}
	return candidate.EventTime >= current.EventTime
}

// scoreStrategyWarmupKline 为本地恢复挑选最佳快照打分，避免 1h/4h 非最终态覆盖最终态。
func scoreStrategyWarmupKline(kline market.Kline) int {
	score := 0
	if kline.IsClosed {
		score += 1
	}
	if kline.IsFinal {
		score += 10
	}
	if kline.IsTradable {
		score += 20
	}
	if !kline.IsDirty {
		score += 5
	}
	if kline.Ema21 != 0 {
		score += 2
	}
	if kline.Ema55 != 0 {
		score += 2
	}
	if kline.Rsi != 0 {
		score += 1
	}
	if kline.Atr != 0 {
		score += 1
	}
	return score
}

// strategyWarmupStateHasData 判断磁盘恢复结果是否至少拿到一类周期数据。
func strategyWarmupStateHasData(state strategyWarmupState) bool {
	return len(state.Klines4h) > 0 ||
		len(state.Klines1h) > 0 ||
		len(state.Klines15m) > 0 ||
		len(state.Klines1m) > 0
}

// formatStrategyWarmupSharedDir 统一输出共享 warmup 目录，未配置时给出占位值，方便日志采集稳定取值。
func formatStrategyWarmupSharedDir(dir string) string {
	dir = strings.TrimSpace(dir)
	if dir == "" {
		return "n/a"
	}
	return dir
}

// strategyWarmupRestoredFromShared 判断本次恢复是否实际使用了共享 warmup 目录，便于线上快速确认 fallback 是否生效。
func strategyWarmupRestoredFromShared(source string) bool {
	for _, item := range strings.Split(strings.TrimSpace(source), "+") {
		if strings.TrimSpace(item) == "shared_warmup" {
			return true
		}
	}
	return false
}

// latestStrategyWarmupOpenTime 返回某个周期当前恢复到的最新开盘时间，便于启动日志核对。
func latestStrategyWarmupOpenTime(klines []market.Kline) string {
	if len(klines) == 0 {
		return ""
	}
	latest := latestStrategyWarmupOpenTimeMillis(klines)
	if latest <= 0 {
		return ""
	}
	return formatAnalyticsTime(time.UnixMilli(latest))
}

type strategyWarmupIntervalSpec struct {
	name        string
	selectKline func(strategyWarmupState) []market.Kline
}

var strategyWarmupIntervalSpecs = []strategyWarmupIntervalSpec{
	{name: "4h", selectKline: func(state strategyWarmupState) []market.Kline { return state.Klines4h }},
	{name: "1h", selectKline: func(state strategyWarmupState) []market.Kline { return state.Klines1h }},
	{name: "15m", selectKline: func(state strategyWarmupState) []market.Kline { return state.Klines15m }},
	{name: "1m", selectKline: func(state strategyWarmupState) []market.Kline { return state.Klines1m }},
}

// renderStrategyWarmupFields 按固定周期顺序渲染 warmup 日志字段，避免多处手工维护字段顺序。
func renderStrategyWarmupFields(render func(spec strategyWarmupIntervalSpec) string) string {
	parts := make([]string, 0, len(strategyWarmupIntervalSpecs))
	for _, spec := range strategyWarmupIntervalSpecs {
		parts = append(parts, render(spec))
	}
	return strings.Join(parts, " ")
}

// formatStrategyWarmupSyncFields 按固定周期顺序输出 latest_open、delta、sync 三元字段，方便日志采集复用统一模板。
func formatStrategyWarmupSyncFields(restored, clickhouse strategyWarmupState) string {
	return renderStrategyWarmupFields(func(spec strategyWarmupIntervalSpec) string {
		restoredKlines := spec.selectKline(restored)
		clickhouseKlines := spec.selectKline(clickhouse)
		return fmt.Sprintf(
			"latest_open_%s=%s delta_vs_clickhouse_%s_min=%s sync_%s=%s",
			spec.name,
			latestStrategyWarmupOpenTime(restoredKlines),
			spec.name,
			strategyWarmupOpenDeltaMinutes(restoredKlines, clickhouseKlines),
			spec.name,
			strategyWarmupOpenDeltaStatus(restoredKlines, clickhouseKlines),
		)
	})
}

// formatStrategyWarmupCountFields 按固定周期顺序输出恢复数量字段，方便与 restore_sync 使用同一套模板思路。
func formatStrategyWarmupCountFields(state strategyWarmupState) string {
	return renderStrategyWarmupFields(func(spec strategyWarmupIntervalSpec) string {
		return fmt.Sprintf("restored_%s=%d", spec.name, len(spec.selectKline(state)))
	})
}

// latestStrategyWarmupOpenTimeMillis 返回某个周期当前恢复到的最新开盘时间戳。
func latestStrategyWarmupOpenTimeMillis(klines []market.Kline) int64 {
	if len(klines) == 0 {
		return 0
	}
	return klines[len(klines)-1].OpenTime
}

// strategyWarmupOpenDeltaMinutes 返回最终恢复结果与 ClickHouse 最新开盘时间的分钟差。
func strategyWarmupOpenDeltaMinutes(restored, clickhouse []market.Kline) string {
	restoredLatest := latestStrategyWarmupOpenTimeMillis(restored)
	clickhouseLatest := latestStrategyWarmupOpenTimeMillis(clickhouse)
	if restoredLatest <= 0 || clickhouseLatest <= 0 {
		return ""
	}
	return fmt.Sprintf("%d", (restoredLatest-clickhouseLatest)/int64(time.Minute/time.Millisecond))
}

// strategyWarmupOpenDeltaStatus 把恢复结果与 ClickHouse 的开盘时间差转成直观状态。
func strategyWarmupOpenDeltaStatus(restored, clickhouse []market.Kline) string {
	restoredLatest := latestStrategyWarmupOpenTimeMillis(restored)
	clickhouseLatest := latestStrategyWarmupOpenTimeMillis(clickhouse)
	switch {
	case restoredLatest <= 0 || clickhouseLatest <= 0:
		return ""
	case restoredLatest == clickhouseLatest:
		return "aligned"
	case restoredLatest < clickhouseLatest:
		return "behind"
	default:
		return "ahead"
	}
}

// logStrategyWarmupDeltaEvent 按差值状态输出异常恢复日志，aligned 保持静默，便于线上聚焦异常周期。
func logStrategyWarmupDeltaEvent(symbol, source, interval string, restored, clickhouse []market.Kline) {
	status := strategyWarmupOpenDeltaStatus(restored, clickhouse)
	switch status {
	case "behind":
		if !shouldWarnStrategyWarmupBehind(interval, restored, clickhouse) {
			return
		}
		log.Printf(
			"[strategy-warmup][WARN] symbol=%s interval=%s source=%s severity=warn event=restore_behind latest_open=%s clickhouse_latest_open=%s delta_min=%s",
			symbol,
			interval,
			source,
			latestStrategyWarmupOpenTime(restored),
			latestStrategyWarmupOpenTime(clickhouse),
			strategyWarmupOpenDeltaMinutes(restored, clickhouse),
		)
	case "ahead":
		log.Printf(
			"[strategy-warmup][INFO] symbol=%s interval=%s source=%s severity=info event=restore_ahead latest_open=%s clickhouse_latest_open=%s delta_min=%s",
			symbol,
			interval,
			source,
			latestStrategyWarmupOpenTime(restored),
			latestStrategyWarmupOpenTime(clickhouse),
			strategyWarmupOpenDeltaMinutes(restored, clickhouse),
		)
	}
}

// shouldWarnStrategyWarmupBehind 仅在恢复结果落后超过 1 根周期时输出 WARN，避免边界时刻的瞬时噪音。
func shouldWarnStrategyWarmupBehind(interval string, restored, clickhouse []market.Kline) bool {
	restoredLatest := latestStrategyWarmupOpenTimeMillis(restored)
	clickhouseLatest := latestStrategyWarmupOpenTimeMillis(clickhouse)
	if restoredLatest <= 0 || clickhouseLatest <= 0 || restoredLatest >= clickhouseLatest {
		return false
	}
	threshold := strategyWarmupIntervalDurationMillis(interval)
	if threshold <= 0 {
		return true
	}
	return clickhouseLatest-restoredLatest > threshold
}

// strategyWarmupIntervalDurationMillis 返回各周期对应的毫秒长度，用于判断是否真的落后超过 1 根周期。
func strategyWarmupIntervalDurationMillis(interval string) int64 {
	switch strings.TrimSpace(interval) {
	case "4h":
		return int64(4 * time.Hour / time.Millisecond)
	case "1h":
		return int64(time.Hour / time.Millisecond)
	case "15m":
		return int64(15 * time.Minute / time.Millisecond)
	case "1m":
		return int64(time.Minute / time.Millisecond)
	default:
		return 0
	}
}

// strategyWarmupStateIsComplete 判断 warmup 是否已经达到各周期目标窗口，避免只拿到少量高周期历史就提前停止 fallback。
func strategyWarmupStateIsComplete(state strategyWarmupState) bool {
	return len(state.Klines4h) >= strategyWarmupLimit("4h") &&
		len(state.Klines1h) >= strategyWarmupLimit("1h") &&
		len(state.Klines15m) >= strategyWarmupLimit("15m") &&
		len(state.Klines1m) >= strategyWarmupLimit("1m")
}

// mergeStrategyWarmupState 以当前缓存为主，按 openTime 合并补齐不足窗口，避免 ClickHouse 已有少量数据时跳过本地历史补足。
func mergeStrategyWarmupState(current, loaded strategyWarmupState) strategyWarmupState {
	return strategyWarmupState{
		Klines4h:  mergeStrategyWarmupKlines(current.Klines4h, loaded.Klines4h, strategyWarmupLimit("4h")),
		Klines1h:  mergeStrategyWarmupKlines(current.Klines1h, loaded.Klines1h, strategyWarmupLimit("1h")),
		Klines15m: mergeStrategyWarmupKlines(current.Klines15m, loaded.Klines15m, strategyWarmupLimit("15m")),
		Klines1m:  mergeStrategyWarmupKlines(current.Klines1m, loaded.Klines1m, strategyWarmupLimit("1m")),
	}
}

// mergeStrategyWarmupKlines 合并两路 warmup K 线，按 openTime 去重并保留更完整的版本，再截取最近窗口。
func mergeStrategyWarmupKlines(current, loaded []market.Kline, limit int) []market.Kline {
	switch {
	case limit <= 0:
		return nil
	case len(current) == 0:
		return sortStrategyWarmupKlines(strategyWarmupBestByOpenTime(loaded), limit)
	case len(loaded) == 0 && len(current) <= limit:
		return current
	}

	bestByOpenTime := strategyWarmupBestByOpenTime(current)
	for i := range loaded {
		item := loaded[i]
		existing, ok := bestByOpenTime[item.OpenTime]
		if !ok || shouldReplaceStrategyWarmupKline(existing, item) {
			bestByOpenTime[item.OpenTime] = item
		}
	}
	return sortStrategyWarmupKlines(bestByOpenTime, limit)
}

// strategyWarmupBestByOpenTime 以 openTime 为键整理一组 K 线，并优先保留更完整的同周期版本。
func strategyWarmupBestByOpenTime(klines []market.Kline) map[int64]market.Kline {
	bestByOpenTime := make(map[int64]market.Kline, len(klines))
	for i := range klines {
		item := klines[i]
		existing, ok := bestByOpenTime[item.OpenTime]
		if !ok || shouldReplaceStrategyWarmupKline(existing, item) {
			bestByOpenTime[item.OpenTime] = item
		}
	}
	return bestByOpenTime
}

// strategyWarmupClickHouseClient 负责从 ClickHouse 查询策略启动恢复所需的 K 线快照。
type strategyWarmupClickHouseClient struct {
	endpoint string
	database string
	username string
	password string
	client   *http.Client
}

// strategyWarmupClickHouseRow 对应 ClickHouse 查询返回的 warmup 行结构。
type strategyWarmupClickHouseRow struct {
	Symbol         string          `json:"symbol"`
	Interval       string          `json:"interval"`
	OpenTimeMs     json.RawMessage `json:"open_time_ms"`
	CloseTimeMs    json.RawMessage `json:"close_time_ms"`
	EventTimeMs    json.RawMessage `json:"event_time_ms"`
	Open           float64         `json:"open"`
	High           float64         `json:"high"`
	Low            float64         `json:"low"`
	Close          float64         `json:"close"`
	Volume         float64         `json:"volume"`
	QuoteVolume    float64         `json:"quote_volume"`
	TakerBuyVolume float64         `json:"taker_buy_volume"`
	IsClosed       uint8           `json:"is_closed"`
	IsDirty        uint8           `json:"is_dirty"`
	DirtyReason    string          `json:"dirty_reason"`
	IsTradable     uint8           `json:"is_tradable"`
	IsFinal        uint8           `json:"is_final"`
	Ema21          float64         `json:"ema21"`
	Ema55          float64         `json:"ema55"`
	Rsi            float64         `json:"rsi"`
	Atr            float64         `json:"atr"`
}

// newStrategyWarmupClickHouseClient 创建 strategy 启动恢复使用的 ClickHouse 查询客户端。
func newStrategyWarmupClickHouseClient(cfg config.ClickHouseConfig) (*strategyWarmupClickHouseClient, error) {
	if !cfg.Enabled {
		return nil, nil
	}
	endpoint := strings.TrimSpace(cfg.Endpoint)
	if endpoint == "" {
		return nil, nil
	}
	timeout := cfg.Timeout
	if timeout <= 0 {
		timeout = 3 * time.Second
	}
	database := strings.TrimSpace(cfg.Database)
	if database == "" {
		database = "exchange_analytics"
	}
	return &strategyWarmupClickHouseClient{
		endpoint: endpoint,
		database: database,
		username: strings.TrimSpace(cfg.Username),
		password: cfg.Password,
		client:   &http.Client{Timeout: timeout},
	}, nil
}

// queryLatestWarmupKlines 查询指定交易对和周期最近的闭 K 线，并按 openTime 去重保留最佳版本。
func (c *strategyWarmupClickHouseClient) queryLatestWarmupKlines(ctx context.Context, symbol, interval string, limit int) ([]market.Kline, error) {
	if c == nil || symbol == "" || interval == "" || limit <= 0 {
		return nil, nil
	}

	queryLimit := limit * 3
	if queryLimit < limit {
		queryLimit = limit
	}
	query := fmt.Sprintf(
		"SELECT symbol, interval, toInt64(toUnixTimestamp64Milli(open_time)) AS open_time_ms, "+
			"toInt64(toUnixTimestamp64Milli(close_time)) AS close_time_ms, toInt64(toUnixTimestamp64Milli(event_time)) AS event_time_ms, "+
			"open, high, low, close, volume, quote_volume, taker_buy_volume, is_closed, is_dirty, dirty_reason, is_tradable, is_final, ema21, ema55, rsi, atr "+
			"FROM %s.kline_fact WHERE symbol = %s AND interval = %s AND is_closed = 1 "+
			"ORDER BY open_time DESC, event_time DESC LIMIT %d FORMAT JSONEachRow",
		c.database,
		quoteClickHouseString(strings.ToUpper(strings.TrimSpace(symbol))),
		quoteClickHouseString(strings.TrimSpace(interval)),
		queryLimit,
	)

	body, err := c.executeQuery(ctx, query)
	if err != nil {
		return nil, err
	}
	if len(bytes.TrimSpace(body)) == 0 {
		return nil, nil
	}

	bestByOpenTime := make(map[int64]market.Kline, limit)
	scanner := bufio.NewScanner(bytes.NewReader(body))
	scanner.Buffer(make([]byte, 0, 1024), 2*1024*1024)
	for scanner.Scan() {
		line := bytes.TrimSpace(scanner.Bytes())
		if len(line) == 0 {
			continue
		}
		var row strategyWarmupClickHouseRow
		if err := json.Unmarshal(line, &row); err != nil {
			return nil, fmt.Errorf("decode warmup row: %w", err)
		}
		kline, ok, err := buildStrategyWarmupKlineFromClickHouseRow(row)
		if err != nil {
			return nil, err
		}
		if !ok {
			continue
		}
		existing, exists := bestByOpenTime[kline.OpenTime]
		if !exists || shouldReplaceStrategyWarmupKline(existing, kline) {
			bestByOpenTime[kline.OpenTime] = kline
		}
	}
	if err := scanner.Err(); err != nil {
		return nil, fmt.Errorf("scan warmup rows: %w", err)
	}
	return sortStrategyWarmupKlines(bestByOpenTime, limit), nil
}

// executeQuery 通过 ClickHouse HTTP 接口执行 warmup 恢复查询。
func (c *strategyWarmupClickHouseClient) executeQuery(ctx context.Context, query string) ([]byte, error) {
	if c == nil {
		return nil, fmt.Errorf("clickhouse client is nil")
	}
	endpoint, err := url.Parse(c.endpoint)
	if err != nil {
		return nil, fmt.Errorf("parse clickhouse endpoint: %w", err)
	}
	queryParams := endpoint.Query()
	if c.database != "" {
		queryParams.Set("database", c.database)
	}
	endpoint.RawQuery = queryParams.Encode()

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint.String(), strings.NewReader(query))
	if err != nil {
		return nil, fmt.Errorf("build clickhouse request: %w", err)
	}
	req.Header.Set("Content-Type", "text/plain; charset=utf-8")
	if c.username != "" {
		req.SetBasicAuth(c.username, c.password)
	}

	resp, err := c.client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("execute clickhouse request: %w", err)
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("read clickhouse response: %w", err)
	}
	if resp.StatusCode >= http.StatusBadRequest {
		return nil, fmt.Errorf("clickhouse status=%d body=%s", resp.StatusCode, strings.TrimSpace(string(body)))
	}
	return body, nil
}

// buildStrategyWarmupKlineFromClickHouseRow 把 ClickHouse 查询结果转换为策略可回灌的 K 线对象。
func buildStrategyWarmupKlineFromClickHouseRow(row strategyWarmupClickHouseRow) (market.Kline, bool, error) {
	openTime, err := parseStrategyJSONInt64(row.OpenTimeMs)
	if err != nil {
		return market.Kline{}, false, fmt.Errorf("parse open_time_ms: %w", err)
	}
	closeTime, err := parseStrategyJSONInt64(row.CloseTimeMs)
	if err != nil {
		return market.Kline{}, false, fmt.Errorf("parse close_time_ms: %w", err)
	}
	eventTime, err := parseStrategyJSONInt64(row.EventTimeMs)
	if err != nil {
		return market.Kline{}, false, fmt.Errorf("parse event_time_ms: %w", err)
	}
	if openTime <= 0 || closeTime <= 0 || eventTime <= 0 || row.IsClosed == 0 {
		return market.Kline{}, false, nil
	}
	return market.Kline{
		Symbol:         strings.ToUpper(strings.TrimSpace(row.Symbol)),
		Interval:       strings.TrimSpace(row.Interval),
		OpenTime:       openTime,
		CloseTime:      closeTime,
		EventTime:      eventTime,
		IsClosed:       row.IsClosed == 1,
		Open:           row.Open,
		High:           row.High,
		Low:            row.Low,
		Close:          row.Close,
		Volume:         row.Volume,
		QuoteVolume:    row.QuoteVolume,
		TakerBuyVolume: row.TakerBuyVolume,
		IsDirty:        row.IsDirty == 1,
		DirtyReason:    strings.TrimSpace(row.DirtyReason),
		IsTradable:     row.IsTradable == 1,
		IsFinal:        row.IsFinal == 1,
		Ema21:          row.Ema21,
		Ema55:          row.Ema55,
		Rsi:            row.Rsi,
		Atr:            row.Atr,
	}, true, nil
}

// sortStrategyWarmupKlines 把去重后的 K 线按时间升序整理，并截取最近 limit 根。
func sortStrategyWarmupKlines(bestByOpenTime map[int64]market.Kline, limit int) []market.Kline {
	if len(bestByOpenTime) == 0 || limit <= 0 {
		return nil
	}
	openTimes := make([]int64, 0, len(bestByOpenTime))
	for openTime := range bestByOpenTime {
		openTimes = append(openTimes, openTime)
	}
	sort.Slice(openTimes, func(i, j int) bool {
		return openTimes[i] < openTimes[j]
	})
	if len(openTimes) > limit {
		openTimes = openTimes[len(openTimes)-limit:]
	}

	result := make([]market.Kline, 0, len(openTimes))
	for _, openTime := range openTimes {
		result = append(result, bestByOpenTime[openTime])
	}
	return result
}

// parseStrategyJSONInt64 兼容 ClickHouse JSONEachRow 返回的数字或字符串整型字段。
func parseStrategyJSONInt64(value json.RawMessage) (int64, error) {
	trimmed := strings.TrimSpace(string(value))
	if trimmed == "" || trimmed == "null" {
		return 0, nil
	}
	var number int64
	if err := json.Unmarshal(value, &number); err == nil {
		return number, nil
	}
	var text string
	if err := json.Unmarshal(value, &text); err != nil {
		return 0, fmt.Errorf("decode int64 raw value %s: %w", trimmed, err)
	}
	text = strings.TrimSpace(text)
	if text == "" {
		return 0, nil
	}
	var parsed int64
	if _, err := fmt.Sscan(text, &parsed); err != nil {
		return 0, fmt.Errorf("scan int64 %q: %w", text, err)
	}
	return parsed, nil
}

// quoteClickHouseString 对字符串做最小转义，避免拼接 SQL 时破坏字面量。
func quoteClickHouseString(value string) string {
	escaped := strings.ReplaceAll(value, "\\", "\\\\")
	escaped = strings.ReplaceAll(escaped, "'", "\\'")
	return "'" + escaped + "'"
}

// evaluateUniverse 基于最新快照生成目标策略集合，并把 diff 应用到运行时。
func (s *ServiceContext) evaluateUniverse(now time.Time) {
	if s == nil || s.universeSelector == nil {
		return
	}
	s.mu.RLock()
	snapshots := make(map[string]universe.Snapshot, len(s.universeSnapshots))
	for k, v := range s.universeSnapshots {
		snapshots[k] = v
	}
	s.mu.RUnlock()

	stateFeatures := make(map[string]featureengine.Features, len(snapshots))
	stateAnalyses := make(map[string]regimejudge.Analysis, len(snapshots))
	stateResults := make(map[string]marketstate.Result, len(snapshots))
	for symbol, snapshot := range snapshots {
		baseEvaluation, hasBase := evaluateUniverseFrame(now, snapshot.Kline1m, s.marketStateConfig)
		h1Evaluation, hasH1 := evaluateUniverseFrame(now, snapshot.Kline1h, s.marketStateConfig)
		m15Evaluation, hasM15 := evaluateUniverseFrame(now, snapshot.Kline15m, s.marketStateConfig)

		if hasH1 {
			snapshot.Regime1h = universe.BuildRegimeFrame("1h", h1Evaluation)
		} else {
			snapshot.Regime1h = universe.RegimeFrame{Interval: "1h"}
		}
		if hasM15 {
			snapshot.Regime15m = universe.BuildRegimeFrame("15m", m15Evaluation)
		} else {
			snapshot.Regime15m = universe.RegimeFrame{Interval: "15m"}
		}

		fusedState, fusedAnalysis, fusion := universe.FuseRegimes(
			snapshot.Regime1h,
			h1Evaluation.Analysis,
			snapshot.Regime15m,
			m15Evaluation.Analysis,
		)
		snapshot.Fusion = fusion
		snapshot.MarketState = fusedState
		snapshot.MarketAnalysis = fusedAnalysis

		result := marketstate.Result{
			Symbol:     snapshot.Symbol,
			State:      fusedState,
			Confidence: fusion.FusedScore,
			Reason:     fusion.FusedReason,
			UpdatedAt:  fusion.UpdatedAt,
		}
		if result.UpdatedAt.IsZero() && hasBase {
			result.UpdatedAt = baseEvaluation.Result.UpdatedAt
		}
		if result.State == marketstate.MarketStateUnknown && hasBase {
			snapshot.MarketState = baseEvaluation.Result.State
			snapshot.MarketAnalysis = baseEvaluation.Analysis
			snapshot.Fusion.FusedState = baseEvaluation.Result.State
			snapshot.Fusion.FusedReason = "fallback_1m_only"
			snapshot.Fusion.FusedScore = baseEvaluation.Result.Confidence
			snapshot.Fusion.UpdatedAt = baseEvaluation.Result.UpdatedAt
			result = baseEvaluation.Result
			result.Reason = "fallback_1m_only"
		}
		if snapshot.Fusion.UpdatedAt.IsZero() {
			snapshot.Fusion.UpdatedAt = result.UpdatedAt
		}
		snapshots[symbol] = snapshot
		stateFeatures[symbol] = snapshot.MarketAnalysis.Features
		stateAnalyses[symbol] = snapshot.MarketAnalysis
		stateResults[symbol] = result
	}
	s.storeLatestUniverseSnapshots(snapshots)
	if s.marketStateLogs != nil {
		for symbol, result := range stateResults {
			s.marketStateLogs.Write(s.Config.SignalLogDir, stateFeatures[symbol], result, now)
		}
		s.marketStateLogs.WriteMeta(s.Config.SignalLogDir, marketstate.BuildMetaLogEntry(now, stateFeatures, stateAnalyses, stateResults), now)
	}

	desired := s.universeSelector.Evaluate(now, snapshots)
	s.storeLatestUniverseDesired(desired)
	if s.weightEngine != nil && s.weightLogs != nil {
		weightInput := s.buildWeightInputs(now, desired, snapshots, stateResults)
		weightOutput := s.weightEngine.Evaluate(now, weightInput)
		s.storeLatestWeightRecommendations(weightOutput)
		for _, rec := range weightOutput.Recommendations {
			s.weightLogs.Write(s.Config.SignalLogDir, rec, now)
		}
		s.weightLogs.WriteMeta(
			s.Config.SignalLogDir,
			weights.BuildMetaLogEntry(weightOutput, len(weightInput.Symbols), "global:"+string(weightInput.MarketState.State), "marketstate.aggregate", now),
			now,
		)
		log.Printf(
			"[weights] evaluate symbols=%d recommendations=%d paused=%v pause_reason=%s market_state=%s source=%s",
			len(weightInput.Symbols),
			len(weightOutput.Recommendations),
			weightOutput.MarketPaused,
			weightOutput.MarketPauseReason,
			weightInput.MarketState.State,
			"marketstate.aggregate",
		)
	}
	results := make(map[string]universeApplyResult, len(desired))
	for _, d := range desired {
		result, err := s.applyUniverseDecision(now, d)
		results[d.Symbol] = result
		if s.universeLogs != nil {
			s.universeLogs.write(s.Config.SignalLogDir, buildUniverseLogEntry(now, d, snapshots[d.Symbol], result), now)
		}
		if err != nil {
			log.Printf("[universe] symbol=%s template=%s enabled=%v err=%v", d.Symbol, d.Template, d.Enabled, err)
		}
	}
	s.storeLatestUniverseApplyResults(results)
	meta := buildUniverseMetaLogEntry(now, desired, results)
	if s.universeLogs != nil {
		s.universeLogs.writeMeta(s.Config.SignalLogDir, meta, now)
	}
	log.Printf(
		"[universe] evaluate candidates=%d enabled=%d healthy=%d stale=%d switch=%d bootstrap=%d",
		meta.CandidateCount,
		meta.EnabledCount,
		meta.HealthyCount,
		meta.StaleCount,
		meta.SwitchCount,
		meta.BootstrapObserveCount,
	)
}

// evaluateUniverseFrame 把某个周期快照转换成单周期判态结果，避免 evaluateUniverse 内部堆积重复样板代码。
func evaluateUniverseFrame(now time.Time, frame universe.KlineFrame, cfg marketstate.Config) (marketstate.Evaluation, bool) {
	input, ok := buildUniverseStateInput(frame)
	if !ok {
		return marketstate.Evaluation{}, false
	}
	features := featureengine.BuildFromSnapshot(input)
	return marketstate.Evaluate(now, features, cfg), true
}

// buildUniverseStateInput 从多周期快照生成统一判态输入，缺少更新时间时直接跳过该周期。
func buildUniverseStateInput(frame universe.KlineFrame) (featureengine.SnapshotValues, bool) {
	if frame.UpdatedAt.IsZero() {
		return featureengine.SnapshotValues{}, false
	}
	return featureengine.SnapshotValues{
		Symbol:     frame.Symbol,
		Timeframe:  frame.Interval,
		Close:      frame.Close,
		Ema21:      frame.Ema21,
		Ema55:      frame.Ema55,
		Atr:        frame.Atr,
		Rsi:        frame.Rsi,
		Volume:     frame.Volume,
		UpdatedAt:  frame.UpdatedAt,
		IsDirty:    frame.IsDirty,
		IsTradable: frame.IsTradable,
		IsFinal:    frame.IsFinal,
	}, true
}

// buildWeightInputs 把当前轮 Universe、快照和 MarketState 结果整理成权重引擎输入。
func (s *ServiceContext) buildWeightInputs(now time.Time, desired []universe.DesiredStrategy, snapshots map[string]universe.Snapshot, stateResults map[string]marketstate.Result) weights.Inputs {
	in := weights.Inputs{
		MarketState:     marketstate.Aggregate(now, buildAnalysisMap(snapshots), stateResults),
		RegimeAnalyses:  make(map[string]regimejudge.Analysis),
		Templates:       make(map[string]string),
		StrategyBuckets: make(map[string]string),
		RouteReasons:    make(map[string]string),
		SymbolScores:    make(map[string]float64),
		UpdatedAt:       now.UTC(),
	}
	atrPctTotal := 0.0
	volumeTotal := 0.0
	for _, d := range desired {
		if !d.Enabled {
			continue
		}
		in.Symbols = append(in.Symbols, d.Symbol)
		in.Templates[d.Symbol] = d.Template
		in.StrategyBuckets[d.Symbol] = d.Bucket
		in.RouteReasons[d.Symbol] = d.Reason
		snap, ok := snapshots[d.Symbol]
		if ok {
			in.RegimeAnalyses[d.Symbol] = snap.MarketAnalysis
		}
		in.SymbolScores[d.Symbol] = scoreDesiredStrategy(d, snap)
		if !ok || snap.Close <= 0 || snap.Atr <= 0 || snap.Volume <= 0 {
			continue
		}
		atrPctTotal += snap.Atr / snap.Close
		volumeTotal += snap.Volume
		in.HealthySymbolCount++
	}
	if in.HealthySymbolCount > 0 {
		in.AvgAtrPct = atrPctTotal / float64(in.HealthySymbolCount)
		in.AvgVolume = volumeTotal / float64(in.HealthySymbolCount)
	}
	return in
}

// buildAnalysisMap 从 Universe 快照中提取统一 Analysis，供全局聚合与权重输入复用。
func buildAnalysisMap(snapshots map[string]universe.Snapshot) map[string]regimejudge.Analysis {
	out := make(map[string]regimejudge.Analysis, len(snapshots))
	for symbol, snap := range snapshots {
		out[symbol] = snap.MarketAnalysis
	}
	return out
}

// scoreDesiredStrategy 基于已产出的统一 Analysis 为权重层提供最小 symbol score。
func scoreDesiredStrategy(d universe.DesiredStrategy, snap universe.Snapshot) float64 {
	switch {
	case snap.MarketAnalysis.BreakoutMatch:
		return 1.15
	case snap.MarketAnalysis.BullTrendStrict || snap.MarketAnalysis.BearTrendStrict:
		if d.Reason == "market_state_trend" {
			return 1.2
		}
		return 1.1
	case snap.MarketAnalysis.RangeMatch:
		return 1.05
	default:
		return 1
	}
}

// buildMarketStateConfig 把 YAML 中的 marketstate 阈值收拢成可复用配置，避免不同调用点各自拼装。
func buildMarketStateConfig(c config.Config) marketstate.Config {
	return marketstate.Config{
		FreshnessWindow:   c.MarketState.FreshnessWindow,
		RangeAtrPctMax:    c.MarketState.RangeAtrPctMax,
		BreakoutAtrPctMin: c.MarketState.BreakoutAtrPctMin,
	}
}

// buildWeightConfig 把 YAML 配置转换成权重引擎配置，便于后续继续扩展状态化资金模型。
func buildWeightConfig(c config.Config) weights.Config {
	return weights.Config{
		DefaultTrendWeight:    c.Weights.DefaultTrendWeight,
		DefaultRangeWeight:    c.Weights.DefaultRangeWeight,
		DefaultBreakoutWeight: c.Weights.DefaultBreakoutWeight,
		DefaultRiskScale:      c.Weights.DefaultRiskScale,
		LossStreakThreshold:   c.Weights.LossStreakThreshold,
		DailyLossSoftLimit:    c.Weights.DailyLossSoftLimit,
		DrawdownSoftLimit:     c.Weights.DrawdownSoftLimit,
		CoolingPauseDuration:  c.Weights.CoolingPauseDuration,
		AtrSpikeRatioMin:      c.Weights.AtrSpikeRatioMin,
		VolumeSpikeRatioMin:   c.Weights.VolumeSpikeRatioMin,
		CoolingMinSamples:     c.Weights.CoolingMinSamples,
		TrendStrategyMix:      cloneWeightMap(c.Weights.TrendStrategyMix),
		BreakoutStrategyMix:   cloneWeightMap(c.Weights.BreakoutStrategyMix),
		RangeStrategyMix:      cloneWeightMap(c.Weights.RangeStrategyMix),
		TrendSymbolWeights:    cloneWeightMap(c.Weights.TrendSymbolWeights),
		BreakoutSymbolWeights: cloneWeightMap(c.Weights.BreakoutSymbolWeights),
		RangeSymbolWeights:    cloneWeightMap(c.Weights.RangeSymbolWeights),
	}
}

// cloneWeightMap 复制 YAML 中的权重映射，避免运行时修改影响原始配置对象。
func cloneWeightMap(in map[string]float64) map[string]float64 {
	if len(in) == 0 {
		return nil
	}
	out := make(map[string]float64, len(in))
	for symbol, weight := range in {
		out[symbol] = weight
	}
	return out
}

// storeLatestWeightRecommendations 缓存最新一轮权重建议，供状态接口等轻量读路径使用。
func (s *ServiceContext) storeLatestWeightRecommendations(out weights.Output) {
	if s == nil {
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	clear(s.latestWeights)
	for _, rec := range out.Recommendations {
		s.latestWeights[rec.Symbol] = rec
	}
}

// storeLatestUniverseDesired 缓存 Universe 当前轮目标状态，供状态接口直接输出结构化路由视图。
func (s *ServiceContext) storeLatestUniverseDesired(desired []universe.DesiredStrategy) {
	if s == nil {
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	clear(s.latestUniverseView)
	for _, item := range desired {
		s.latestUniverseView[item.Symbol] = item
	}
}

// storeLatestUniverseSnapshots 缓存 Universe 当前轮融合后的快照结果，供状态查询直接读取多周期判态。
func (s *ServiceContext) storeLatestUniverseSnapshots(snapshots map[string]universe.Snapshot) {
	if s == nil {
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	clear(s.latestUniverseSnap)
	for symbol, snapshot := range snapshots {
		s.latestUniverseSnap[symbol] = snapshot
	}
}

// LatestUniverseSnapshot 返回某个 symbol 最近一轮融合后的 Universe 快照。
func (s *ServiceContext) LatestUniverseSnapshot(symbol string) (universe.Snapshot, bool) {
	if s == nil || symbol == "" {
		return universe.Snapshot{}, false
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	snapshot, ok := s.latestUniverseSnap[symbol]
	return snapshot, ok
}

// RecordLatestUniverseSnapshot 写入某个 symbol 最近一轮融合快照，供测试和状态接口复用。
func (s *ServiceContext) RecordLatestUniverseSnapshot(snapshot universe.Snapshot) {
	if s == nil || snapshot.Symbol == "" {
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.latestUniverseSnap == nil {
		s.latestUniverseSnap = make(map[string]universe.Snapshot)
	}
	s.latestUniverseSnap[snapshot.Symbol] = snapshot
}

// LatestUniverseDesired 返回某个 symbol 最近一轮 Universe 目标状态快照。
func (s *ServiceContext) LatestUniverseDesired(symbol string) (universe.DesiredStrategy, bool) {
	if s == nil || symbol == "" {
		return universe.DesiredStrategy{}, false
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	desired, ok := s.latestUniverseView[symbol]
	return desired, ok
}

// RecordLatestUniverseDesired 写入某个 symbol 最近一轮 Universe 目标状态，供测试和轻量状态读路径复用。
func (s *ServiceContext) RecordLatestUniverseDesired(desired universe.DesiredStrategy) {
	if s == nil || desired.Symbol == "" {
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.latestUniverseView == nil {
		s.latestUniverseView = make(map[string]universe.DesiredStrategy)
	}
	s.latestUniverseView[desired.Symbol] = desired
}

// storeLatestUniverseApplyResults 缓存 Universe 当前轮实际应用结果，供状态接口输出运行时视角。
func (s *ServiceContext) storeLatestUniverseApplyResults(results map[string]universeApplyResult) {
	if s == nil {
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	clear(s.latestUniverseApply)
	for symbol, result := range results {
		s.latestUniverseApply[symbol] = result
	}
}

// LatestUniverseApplyResult 返回某个 symbol 最近一轮 Universe 实际应用结果。
func (s *ServiceContext) LatestUniverseApplyResult(symbol string) (universeApplyResult, bool) {
	if s == nil || symbol == "" {
		return universeApplyResult{}, false
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	result, ok := s.latestUniverseApply[symbol]
	return result, ok
}

// RecordLatestUniverseApplyResult 写入某个 symbol 最近一轮 Universe 实际应用结果，供测试和轻量读路径复用。
func (s *ServiceContext) RecordLatestUniverseApplyResult(symbol string, result universeApplyResult) {
	if s == nil || symbol == "" {
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.latestUniverseApply == nil {
		s.latestUniverseApply = make(map[string]universeApplyResult)
	}
	s.latestUniverseApply[symbol] = result
}

// RecordLatestUniverseRuntimeStatus 用轻量字段写入一轮 Universe 实际应用结果，便于跨包测试复用。
func (s *ServiceContext) RecordLatestUniverseRuntimeStatus(symbol, action, reason string, enabled, hasStrategy, hasOpenPosition bool, currentTemplate, runtimeTemplate string) {
	s.RecordLatestUniverseApplyResult(symbol, universeApplyResult{
		Action:          action,
		Reason:          reason,
		Enabled:         enabled,
		HasStrategy:     hasStrategy,
		HasOpenPosition: hasOpenPosition,
		CurrentTemplate: currentTemplate,
		RuntimeTemplate: runtimeTemplate,
	})
}

// StrategyWarmupStatus 返回某个 symbol 当前的多周期 warmup 长度，优先使用运行中实例，缺席时回退到缓存。
func (s *ServiceContext) StrategyWarmupStatus(symbol string) StrategyWarmupStatusView {
	if s == nil || symbol == "" {
		return buildStrategyWarmupStatusView(0, 0, 0, 0, "empty")
	}
	s.mu.RLock()
	strat := s.strategies[symbol]
	warmup := s.strategyWarmup[symbol]
	s.mu.RUnlock()
	if strat != nil {
		lens := strat.HistoryWarmupStatus()
		return buildStrategyWarmupStatusView(
			int32(lens.HistoryLen4h),
			int32(lens.HistoryLen1h),
			int32(lens.HistoryLen15m),
			int32(lens.HistoryLen1m),
			"runtime",
		)
	}
	if len(warmup.Klines4h) == 0 &&
		len(warmup.Klines1h) == 0 &&
		len(warmup.Klines15m) == 0 &&
		len(warmup.Klines1m) == 0 {
		return buildStrategyWarmupStatusView(0, 0, 0, 0, "empty")
	}
	return buildStrategyWarmupStatusView(
		int32(len(warmup.Klines4h)),
		int32(len(warmup.Klines1h)),
		int32(len(warmup.Klines15m)),
		int32(len(warmup.Klines1m)),
		"cache",
	)
}

// LatestSecondBarStatus 返回某个 symbol 最近一条秒级行情视图，供状态接口和测试统一读取。
func (s *ServiceContext) LatestSecondBarStatus(symbol string) (LatestSecondBarStatusView, bool) {
	if s == nil || symbol == "" {
		return LatestSecondBarStatusView{}, false
	}
	s.mu.RLock()
	strat := s.strategies[symbol]
	s.mu.RUnlock()
	if strat == nil {
		return LatestSecondBarStatusView{}, false
	}
	bars := strat.RecentSecondBars(1)
	if len(bars) == 0 {
		return LatestSecondBarStatusView{}, false
	}
	bar := bars[0]
	view := LatestSecondBarStatusView{
		OpenTimeMs:  bar.OpenTimeMs,
		CloseTimeMs: bar.CloseTimeMs,
		Open:        bar.Open,
		High:        bar.High,
		Low:         bar.Low,
		Close:       bar.Close,
		Volume:      bar.Volume,
		IsFinal:     bar.IsFinal,
		Synthetic:   bar.Synthetic,
		Source:      "trade",
	}
	if bar.Synthetic {
		view.Source = "depth_fallback"
	}
	return view, true
}

// buildStrategyWarmupStatusView 基于多周期历史长度生成统一 warmup 判定，供 RPC 和网关直接透出恢复状态。
func buildStrategyWarmupStatusView(historyLen4h, historyLen1h, historyLen15m, historyLen1m int32, source string) StrategyWarmupStatusView {
	view := StrategyWarmupStatusView{
		HistoryLen4h:  historyLen4h,
		HistoryLen1h:  historyLen1h,
		HistoryLen15m: historyLen15m,
		HistoryLen1m:  historyLen1m,
		Source:        source,
	}
	view.Status, view.IncompleteReasons = evaluateStrategyWarmupCompleteness(view)
	return view
}

// evaluateStrategyWarmupCompleteness 按各周期目标窗口判断 warmup 是否完整，并返回缺失原因列表。
func evaluateStrategyWarmupCompleteness(view StrategyWarmupStatusView) (string, []string) {
	reasons := make([]string, 0, 4)
	if view.HistoryLen4h < int32(strategyWarmupLimit("4h")) {
		reasons = append(reasons, "insufficient_4h_history")
	}
	if view.HistoryLen1h < int32(strategyWarmupLimit("1h")) {
		reasons = append(reasons, "insufficient_1h_history")
	}
	if view.HistoryLen15m < int32(strategyWarmupLimit("15m")) {
		reasons = append(reasons, "insufficient_15m_history")
	}
	if view.HistoryLen1m < int32(strategyWarmupLimit("1m")) {
		reasons = append(reasons, "insufficient_1m_history")
	}
	if len(reasons) == 0 {
		return "warmup_complete", nil
	}
	return "warmup_incomplete", reasons
}

// RecordStrategyWarmupStatus 写入某个 symbol 的 warmup 缓存长度，供测试和状态查询回退路径复用。
func (s *ServiceContext) RecordStrategyWarmupStatus(symbol string, status StrategyWarmupStatusView) {
	if s == nil || symbol == "" {
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.strategyWarmup == nil {
		s.strategyWarmup = make(map[string]strategyWarmupState)
	}
	s.strategyWarmup[symbol] = strategyWarmupState{
		Klines4h:  make([]market.Kline, int(status.HistoryLen4h)),
		Klines1h:  make([]market.Kline, int(status.HistoryLen1h)),
		Klines15m: make([]market.Kline, int(status.HistoryLen15m)),
		Klines1m:  make([]market.Kline, int(status.HistoryLen1m)),
	}
}

// RecordStrategyInstance 写入某个运行中策略实例，供测试和状态查询复用运行时视图。
func (s *ServiceContext) RecordStrategyInstance(symbol string, strat *strategyengine.TrendFollowingStrategy) {
	if s == nil || symbol == "" || strat == nil {
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.strategies == nil {
		s.strategies = make(map[string]*strategyengine.TrendFollowingStrategy)
	}
	s.strategies[symbol] = strat
}

// LatestWeightRecommendation 返回某个 symbol 最近一轮的权重建议快照。
func (s *ServiceContext) LatestWeightRecommendation(symbol string) (weights.Recommendation, bool) {
	if s == nil || symbol == "" {
		return weights.Recommendation{}, false
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	rec, ok := s.latestWeights[symbol]
	return rec, ok
}

// RecordLatestWeightRecommendation 写入某个 symbol 最近一轮的权重建议快照。
func (s *ServiceContext) RecordLatestWeightRecommendation(rec weights.Recommendation) {
	if s == nil || rec.Symbol == "" {
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.latestWeights == nil {
		s.latestWeights = make(map[string]weights.Recommendation)
	}
	s.latestWeights[rec.Symbol] = rec
}

// applyUniverseDecision 把 selector 的目标状态转换成安全的运行时启停动作。
func (s *ServiceContext) applyUniverseDecision(now time.Time, decision universe.DesiredStrategy) (universeApplyResult, error) {
	result := universeApplyResult{
		Action:  "noop",
		Reason:  decision.Reason,
		Enabled: decision.Enabled,
	}
	if s == nil || decision.Symbol == "" {
		return result, nil
	}
	s.mu.RLock()
	strat := s.strategies[decision.Symbol]
	state := s.universeStates[decision.Symbol]
	s.mu.RUnlock()
	result.HasStrategy = strat != nil
	result.CurrentTemplate = state.Template
	result.RuntimeTemplate = state.Template
	if strat != nil {
		result.HasOpenPosition = strat.HasOpenPosition()
	}

	if !decision.Enabled {
		if decision.Reason == "no_snapshot" && s.isUniverseBootstrap(now) {
			log.Printf("[universe] bootstrap observe symbol=%s template=%s reason=no_snapshot", decision.Symbol, decision.Template)
			result.Action = "bootstrap_observe"
			result.Enabled = strat != nil
			result.Reason = "bootstrap_no_snapshot"
			return result, nil
		}
		if strat == nil {
			result.Action = "noop_absent"
			return result, nil
		}
		if strat.HasOpenPosition() {
			log.Printf("[universe] keep symbol=%s enabled=true reason=open_position", decision.Symbol)
			result.Action = "keep_open_position"
			result.Enabled = true
			result.Reason = "open_position"
			return result, nil
		}
		if s.Config.Universe.MinEnabledDuration > 0 && !state.LastEnabledAt.IsZero() && now.Sub(state.LastEnabledAt) < s.Config.Universe.MinEnabledDuration {
			log.Printf("[universe] defer disable symbol=%s remaining=%s reason=min_enabled_duration gate_reason=%s", decision.Symbol, s.Config.Universe.MinEnabledDuration-now.Sub(state.LastEnabledAt), decision.Reason)
			result.Action = "defer_disable"
			result.Enabled = true
			result.Reason = "min_enabled_duration"
			return result, nil
		}
		cfg := s.buildRuntimeStrategyConfig(decision.Symbol, decision.Template, false)
		if err := s.applyStrategyConfig(cfg, now); err != nil {
			result.Action = "disable_error"
			return result, err
		}
		log.Printf("[universe] disable symbol=%s reason=%s", decision.Symbol, decision.Reason)
		result.Action = "disable"
		result.HasStrategy = false
		result.RuntimeTemplate = decision.Template
		return result, nil
	}

	if strat == nil && s.Config.Universe.CooldownDuration > 0 && !state.LastDisabledAt.IsZero() && now.Sub(state.LastDisabledAt) < s.Config.Universe.CooldownDuration {
		log.Printf("[universe] defer enable symbol=%s remaining=%s reason=cooldown", decision.Symbol, s.Config.Universe.CooldownDuration-now.Sub(state.LastDisabledAt))
		result.Action = "defer_enable"
		result.Enabled = false
		result.Reason = "cooldown"
		return result, nil
	}
	if strat != nil && state.Template == decision.Template {
		result.Action = "keep"
		result.RuntimeTemplate = state.Template
		return result, nil
	}
	if strat != nil && state.Template != "" && state.Template != decision.Template && strat.HasOpenPosition() {
		log.Printf("[universe] defer switch symbol=%s template=%s->%s reason=open_position", decision.Symbol, state.Template, decision.Template)
		result.Action = "defer_switch"
		result.Reason = "open_position"
		return result, nil
	}
	cfg := s.buildRuntimeStrategyConfig(decision.Symbol, decision.Template, true)
	cfg.Overrides = decision.Overrides
	if err := s.applyStrategyConfig(cfg, now); err != nil {
		result.Action = "enable_error"
		return result, err
	}
	result.HasStrategy = true
	result.RuntimeTemplate = decision.Template
	if strat == nil {
		log.Printf("[universe] enable symbol=%s template=%s reason=%s", decision.Symbol, decision.Template, decision.Reason)
		result.Action = "enable"
	} else {
		log.Printf("[universe] switch symbol=%s template=%s->%s reason=%s", decision.Symbol, state.Template, decision.Template, decision.Reason)
		result.Action = "switch"
	}
	return result, nil
}

func NewServiceContext(c config.Config) (*ServiceContext, error) {
	ctx, cancel := context.WithCancel(context.Background())
	c.SharedWarmupDir = resolveSharedWarmupDir(c.SharedWarmupDir)

	signalProducer, err := kafka.NewProducerWithContext(ctx, c.Kafka.Addrs, c.Kafka.Topics.Signal)
	if err != nil {
		cancel()
		return nil, err
	}
	var harvestPathProducer *kafka.Producer
	if c.Kafka.Topics.HarvestPathSignal != "" {
		harvestPathProducer, err = kafka.NewProducerWithContext(ctx, c.Kafka.Addrs, c.Kafka.Topics.HarvestPathSignal)
		if err != nil {
			_ = signalProducer.Close()
			cancel()
			return nil, err
		}
	}

	groupID := c.Kafka.Group
	if groupID == "" {
		if c.Name != "" {
			groupID = c.Name + "-kline"
		} else {
			groupID = "strategy-kline"
		}
	}
	marketConsumer, err := kafka.NewConsumer(c.Kafka.Addrs, groupID, c.Kafka.Topics.Kline, c.KlineLogDir, c.Kafka.InitialOffset)
	if err != nil {
		if harvestPathProducer != nil {
			_ = harvestPathProducer.Close()
		}
		_ = signalProducer.Close()
		cancel()
		return nil, err
	}
	var depthConsumer *kafka.Consumer
	if c.Kafka.Topics.Depth != "" {
		depthGroupID := groupID + "-depth"
		depthConsumer, err = kafka.NewConsumer(c.Kafka.Addrs, depthGroupID, c.Kafka.Topics.Depth, "", c.Kafka.InitialOffset)
		if err != nil {
			_ = marketConsumer.Close()
			if harvestPathProducer != nil {
				_ = harvestPathProducer.Close()
			}
			_ = signalProducer.Close()
			cancel()
			return nil, err
		}
	}
	var tradeConsumer *kafka.Consumer
	if c.Kafka.Topics.Trade != "" {
		tradeGroupID := groupID + "-trade"
		tradeConsumer, err = kafka.NewConsumer(c.Kafka.Addrs, tradeGroupID, c.Kafka.Topics.Trade, "", c.Kafka.InitialOffset)
		if err != nil {
			if depthConsumer != nil {
				_ = depthConsumer.Close()
			}
			_ = marketConsumer.Close()
			if harvestPathProducer != nil {
				_ = harvestPathProducer.Close()
			}
			_ = signalProducer.Close()
			cancel()
			return nil, err
		}
	}

	executionCli := executionservice.NewExecutionService(zrpc.MustNewClient(c.Execution))
	harvestPathLSTMPredictor := harvestpathmodel.NewLSTMPredictor(harvestpathmodel.LSTMPredictorConfig{
		Enabled:      c.HarvestPathLSTM.Enabled,
		PythonBin:    c.HarvestPathLSTM.PythonBin,
		ScriptPath:   c.HarvestPathLSTM.ScriptPath,
		DataDir:      c.HarvestPathLSTM.DataDir,
		ArtifactsDir: c.HarvestPathLSTM.ArtifactsDir,
		Timeout:      time.Duration(c.HarvestPathLSTM.TimeoutMs) * time.Millisecond,
	})
	analyticsWriter, err := newStrategyClickHouseWriter(ctx, c.ClickHouse)
	if err != nil {
		if tradeConsumer != nil {
			_ = tradeConsumer.Close()
		}
		if depthConsumer != nil {
			_ = depthConsumer.Close()
		}
		_ = marketConsumer.Close()
		if harvestPathProducer != nil {
			_ = harvestPathProducer.Close()
		}
		_ = signalProducer.Close()
		cancel()
		return nil, err
	}

	marketStateCfg := buildMarketStateConfig(c)
	svcCtx := &ServiceContext{
		Config:                   c,
		signalProducer:           signalProducer,
		harvestPathProducer:      harvestPathProducer,
		harvestPathLSTMPredictor: harvestPathLSTMPredictor,
		marketConsumer:           marketConsumer,
		depthConsumer:            depthConsumer,
		tradeConsumer:            tradeConsumer,
		executionCli:             executionCli,
		strategies:               make(map[string]*strategyengine.TrendFollowingStrategy),
		secondBarAssemblers:      make(map[string]*SecondBarAssembler),
		secondBarLogAt:           make(map[string]time.Time),
		strategyConfigs:          make(map[string]config.StrategyConfig),
		marketStateDetector:      marketstate.NewDetector(marketStateCfg),
		marketStateConfig:        marketStateCfg,
		marketStateLogs:          marketstate.NewLogState(),
		weightEngine:             weights.NewEngine(buildWeightConfig(c)),
		weightLogs:               weights.NewLogState(),
		latestWeights:            make(map[string]weights.Recommendation),
		latestUniverseView:       make(map[string]universe.DesiredStrategy),
		latestUniverseApply:      make(map[string]universeApplyResult),
		latestUniverseSnap:       make(map[string]universe.Snapshot),
		universeSnapshots:        make(map[string]universe.Snapshot),
		strategyWarmup:           make(map[string]strategyWarmupState),
		universeStates:           make(map[string]universeRuntimeState),
		universeLogs:             newUniverseLogState(),
		analyticsWriter:          analyticsWriter,
		universeStartedAt:        time.Now().UTC(),
		cancel:                   cancel,
	}
	if c.Universe.Enabled {
		svcCtx.universeSelector = universe.NewSelector(normalizeUniverseConfig(c))
	}

	for _, sc := range c.Strategies {
		svcCtx.strategyConfigs[sc.Symbol] = cloneStrategyConfig(sc)
		if !sc.Enabled || sc.Symbol == "" {
			continue
		}
		params, err := resolveStrategyParameters(c.Templates, sc)
		if err != nil {
			if tradeConsumer != nil {
				_ = tradeConsumer.Close()
			}
			_ = marketConsumer.Close()
			if depthConsumer != nil {
				_ = depthConsumer.Close()
			}
			if harvestPathProducer != nil {
				_ = harvestPathProducer.Close()
			}
			_ = signalProducer.Close()
			cancel()
			return nil, err
		}
		svcCtx.upsertStrategyLocked(&strategypb.StrategyConfig{
			Symbol:     sc.Symbol,
			Name:       sc.Name,
			Enabled:    sc.Enabled,
			Parameters: params,
		})
		if svcCtx.universeSelector != nil {
			svcCtx.universeStates[sc.Symbol] = universeRuntimeState{
				Template:      sc.Template,
				LastEnabledAt: time.Now().UTC(),
			}
		}
	}

	svcCtx.mu.RLock()
	empty := len(svcCtx.strategies) == 0
	svcCtx.mu.RUnlock()
	if empty {
		if tradeConsumer != nil {
			_ = tradeConsumer.Close()
		}
		_ = marketConsumer.Close()
		if depthConsumer != nil {
			_ = depthConsumer.Close()
		}
		if harvestPathProducer != nil {
			_ = harvestPathProducer.Close()
		}
		_ = signalProducer.Close()
		cancel()
		return nil, fmt.Errorf("no enabled strategies")
	}

	if err := marketConsumer.StartConsuming(ctx, func(kline *market.Kline) error {
		if kline == nil || kline.Symbol == "" {
			return nil
		}
		svcCtx.updateUniverseSnapshot(kline)
		svcCtx.cacheStrategyWarmupKline(kline)
		svcCtx.mu.RLock()
		strat := svcCtx.strategies[kline.Symbol]
		svcCtx.mu.RUnlock()
		if strat == nil {
			return nil
		}
		svcCtx.reconcileStrategyPosition(ctx, strat, kline)
		return strat.OnKline(ctx, kline)
	}); err != nil {
		_ = marketConsumer.Close()
		if harvestPathProducer != nil {
			_ = harvestPathProducer.Close()
		}
		_ = signalProducer.Close()
		cancel()
		return nil, err
	}
	if depthConsumer != nil {
		if err := depthConsumer.StartConsumingDepth(ctx, func(depth *market.Depth) error {
			return svcCtx.dispatchDepthToStrategy(ctx, depth)
		}); err != nil {
			_ = marketConsumer.Close()
			_ = depthConsumer.Close()
			if tradeConsumer != nil {
				_ = tradeConsumer.Close()
			}
			if harvestPathProducer != nil {
				_ = harvestPathProducer.Close()
			}
			_ = signalProducer.Close()
			cancel()
			return nil, err
		}
	}
	if tradeConsumer != nil {
		if err := tradeConsumer.StartConsumingTrade(ctx, func(trade *market.Trade) error {
			return svcCtx.dispatchTradeToStrategy(ctx, trade)
		}); err != nil {
			_ = marketConsumer.Close()
			if depthConsumer != nil {
				_ = depthConsumer.Close()
			}
			_ = tradeConsumer.Close()
			if harvestPathProducer != nil {
				_ = harvestPathProducer.Close()
			}
			_ = signalProducer.Close()
			cancel()
			return nil, err
		}
	}

	// Periodic lag print for ops/troubleshooting.
	commonkafka.StartConsumerGroupLagReporter(ctx, c.Kafka.Addrs, groupID, c.Kafka.Topics.Kline, 30*time.Second)
	if svcCtx.universeSelector != nil {
		go svcCtx.runUniverseLoop(ctx)
	}

	return svcCtx, nil
}

// resolveSharedWarmupDir 把相对 SharedWarmupDir 解析到仓库根目录，避免 strategy 从错误的工作目录读取 warmup 快照。
func resolveSharedWarmupDir(dir string) string {
	dir = strings.TrimSpace(dir)
	if dir == "" {
		return ""
	}
	if filepath.IsAbs(dir) {
		return filepath.Clean(dir)
	}
	if repoRoot, ok := findExchangeSystemRepoRoot(); ok {
		return filepath.Join(repoRoot, filepath.Clean(dir))
	}
	if abs, err := filepath.Abs(dir); err == nil {
		return abs
	}
	return filepath.Clean(dir)
}

// findExchangeSystemRepoRoot 从当前工作目录向上查找 go.mod，定位 exchange-system 仓库根目录。
func findExchangeSystemRepoRoot() (string, bool) {
	wd, err := os.Getwd()
	if err != nil {
		return "", false
	}
	dir := wd
	for {
		goModPath := filepath.Join(dir, "go.mod")
		content, readErr := os.ReadFile(goModPath)
		if readErr == nil && strings.Contains(string(content), "module exchange-system") {
			return dir, true
		}
		parent := filepath.Dir(dir)
		if parent == dir {
			return "", false
		}
		dir = parent
	}
}

func (s *ServiceContext) reconcileStrategyPosition(ctx context.Context, strat *strategyengine.TrendFollowingStrategy, kline *market.Kline) {
	if s == nil || strat == nil || kline == nil || s.executionCli == nil {
		return
	}
	if kline.Interval != "1m" || !kline.IsFinal {
		return
	}
	if !strat.HasOpenPosition() {
		return
	}

	rpcCtx, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer cancel()

	account, err := s.executionCli.GetAccountInfo(rpcCtx, &executionservice.AccountQuery{IncludePositions: true})
	if err != nil {
		return
	}

	longQty := 0.0
	shortQty := 0.0
	for _, pos := range account.GetPositions() {
		if pos.GetSymbol() != kline.Symbol {
			continue
		}
		amt := pos.GetPositionAmount()
		if amt > 0 {
			longQty += amt
			continue
		}
		if amt < 0 {
			shortQty += -amt
		}
	}

	strat.ReconcilePositionWithExchange(longQty, shortQty)
}

// dispatchDepthToStrategy 把 depth 更新同时喂给订单簿快照和秒级兜底聚合，确保运行时和测试共用一条链路。
func (s *ServiceContext) dispatchDepthToStrategy(ctx context.Context, depth *market.Depth) error {
	if s == nil || depth == nil || depth.Symbol == "" {
		return nil
	}
	s.mu.RLock()
	strat := s.strategies[depth.Symbol]
	assembler := s.secondBarAssemblers[depth.Symbol]
	s.mu.RUnlock()
	if assembler != nil {
		for _, bar := range assembler.OnDepth(depth) {
			if bar == nil || strat == nil {
				continue
			}
			if err := strat.OnSecondBar(ctx, bar); err != nil {
				return err
			}
			s.logSecondBarDispatch(depth.Symbol, bar)
		}
	}
	if strat == nil {
		return nil
	}
	strat.OnDepth(depth)
	return nil
}

// dispatchTradeToStrategy 把 trade 聚合成 final SecondBar 后转给策略，避免回调闭包和测试重复装配逻辑。
func (s *ServiceContext) dispatchTradeToStrategy(ctx context.Context, trade *market.Trade) error {
	if s == nil || trade == nil || trade.Symbol == "" {
		return nil
	}
	s.mu.RLock()
	strat := s.strategies[trade.Symbol]
	assembler := s.secondBarAssemblers[trade.Symbol]
	s.mu.RUnlock()
	if strat == nil || assembler == nil {
		return nil
	}
	for _, bar := range assembler.OnTrade(trade) {
		if bar == nil {
			continue
		}
		if err := strat.OnSecondBar(ctx, bar); err != nil {
			return err
		}
		s.logSecondBarDispatch(trade.Symbol, bar)
	}
	return nil
}

// logSecondBarDispatch 输出秒级行情的调试日志，synthetic 条目强提示，真实成交条目按采样频率输出。
func (s *ServiceContext) logSecondBarDispatch(symbol string, bar *strategyengine.SecondBar) {
	if s == nil || bar == nil || symbol == "" {
		return
	}
	if !bar.Synthetic && !s.shouldLogSecondBarSample(symbol, 5*time.Second) {
		return
	}
	level := "sampled"
	source := "trade"
	if bar.Synthetic {
		level = "synthetic"
		source = "depth_fallback"
	}
	log.Printf(
		"[second-bar][%s] symbol=%s source=%s synthetic=%t open_time=%s close_time=%s open=%.6f high=%.6f low=%.6f close=%.6f volume=%.6f",
		level,
		strings.ToUpper(strings.TrimSpace(symbol)),
		source,
		bar.Synthetic,
		time.UnixMilli(bar.OpenTimeMs).UTC().Format("15:04:05.000"),
		time.UnixMilli(bar.CloseTimeMs).UTC().Format("15:04:05.000"),
		bar.Open,
		bar.High,
		bar.Low,
		bar.Close,
		bar.Volume,
	)
}

// shouldLogSecondBarSample 对真实成交生成的秒条按 symbol 做节流，避免 1s 连续日志过密。
func (s *ServiceContext) shouldLogSecondBarSample(symbol string, interval time.Duration) bool {
	if s == nil || interval <= 0 {
		return false
	}
	symbol = strings.ToUpper(strings.TrimSpace(symbol))
	if symbol == "" {
		return false
	}
	now := time.Now()
	s.secondBarLogMu.Lock()
	defer s.secondBarLogMu.Unlock()
	if s.secondBarLogAt == nil {
		s.secondBarLogAt = make(map[string]time.Time)
	}
	lastAt := s.secondBarLogAt[symbol]
	if !lastAt.IsZero() && now.Sub(lastAt) < interval {
		return false
	}
	s.secondBarLogAt[symbol] = now
	return true
}

func (s *ServiceContext) UpsertStrategy(cfg *strategypb.StrategyConfig) {
	if s == nil || cfg == nil {
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.upsertStrategyLocked(cfg)
}

func (s *ServiceContext) upsertStrategyLocked(cfg *strategypb.StrategyConfig) {
	if cfg == nil || cfg.Symbol == "" {
		return
	}
	if !cfg.Enabled {
		delete(s.strategies, cfg.Symbol)
		delete(s.secondBarAssemblers, cfg.Symbol)
		return
	}
	opts := &strategyengine.RuntimeOptions{
		HarvestPathLSTMPredictor: s.harvestPathLSTMPredictor,
		WeightProvider:           s.LatestWeightRecommendation,
		AnalyticsWriter:          s.analyticsWriter,
	}
	if current, ok := s.strategies[cfg.Symbol]; ok && current != nil {
		s.ensureSecondBarAssemblerLocked(cfg.Symbol, cfg.Parameters)
		current.UpdateRuntimeConfig(cfg.Parameters, s.Config.SignalLogDir, opts)
		return
	}
	s.ensureStrategyWarmupLoadedLocked(cfg.Symbol)
	strat := strategyengine.NewTrendFollowingStrategy(
		cfg.Symbol,
		cfg.Parameters,
		s.signalProducer,
		s.harvestPathProducer,
		s.Config.SignalLogDir,
		opts,
	)
	s.hydrateStrategyWarmupLocked(cfg.Symbol, strat)
	s.strategies[cfg.Symbol] = strat
	s.ensureSecondBarAssemblerLocked(cfg.Symbol, cfg.Parameters)
}

// ensureSecondBarAssemblerLocked 确保每个运行中的交易对都拥有独立的秒级聚合器，避免共享状态串扰。
func (s *ServiceContext) ensureSecondBarAssemblerLocked(symbol string, params map[string]float64) *SecondBarAssembler {
	if s == nil || symbol == "" {
		return nil
	}
	cfg := buildSecondBarAssemblerConfig(params)
	if s.secondBarAssemblers == nil {
		s.secondBarAssemblers = make(map[string]*SecondBarAssembler)
	}
	if assembler, ok := s.secondBarAssemblers[symbol]; ok && assembler != nil {
		assembler.UpdateConfig(cfg)
		return assembler
	}
	assembler := NewSecondBarAssembler(symbol, cfg)
	s.secondBarAssemblers[symbol] = assembler
	return assembler
}

// buildSecondBarAssemblerConfig 从策略参数中提取秒级装配器所需开关，避免 service 层散落魔法字符串判断。
func buildSecondBarAssemblerConfig(params map[string]float64) SecondBarAssemblerConfig {
	return SecondBarAssemblerConfig{
		EnableDepthMidFallback: paramEnabled(params, secondBarDepthFallbackParam),
	}
}

// paramEnabled 把 float64 风格参数统一映射为布尔开关，保持和现有策略参数约定一致。
func paramEnabled(params map[string]float64, key string) bool {
	if len(params) == 0 || key == "" {
		return false
	}
	return params[key] > 0
}

func (s *ServiceContext) StopStrategy(strategyID string) {
	if s == nil {
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	for sym := range s.strategies {
		if sym == strategyID {
			delete(s.strategies, sym)
			return
		}
	}
}

func (s *ServiceContext) HasStrategy(strategyID string) bool {
	if s == nil {
		return false
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	_, ok := s.strategies[strategyID]
	return ok
}

func (s *ServiceContext) Close() error {
	if s == nil {
		return nil
	}
	if s.cancel != nil {
		s.cancel()
	}
	var firstErr error
	if s.marketConsumer != nil {
		if err := s.marketConsumer.Close(); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	if s.depthConsumer != nil {
		if err := s.depthConsumer.Close(); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	if s.tradeConsumer != nil {
		if err := s.tradeConsumer.Close(); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	if s.signalProducer != nil {
		if err := s.signalProducer.Close(); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	if s.harvestPathProducer != nil {
		if err := s.harvestPathProducer.Close(); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	if s.universeLogs != nil {
		if err := s.universeLogs.close(); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	if s.marketStateLogs != nil {
		if err := s.marketStateLogs.Close(); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	if s.weightLogs != nil {
		if err := s.weightLogs.Close(); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	if s.analyticsWriter != nil {
		if err := s.analyticsWriter.Close(); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	return firstErr
}

// strategyAnalyticsEnvelope 封装待写入 ClickHouse 的策略分析事件。
type strategyAnalyticsEnvelope struct {
	table string
	row   interface{}
}

// strategyClickHouseWriter 负责把策略信号与决策异步写入 ClickHouse。
type strategyClickHouseWriter struct {
	endpoint      string
	database      string
	username      string
	password      string
	source        string
	client        *http.Client
	queue         chan strategyAnalyticsEnvelope
	flushInterval time.Duration
	ctx           context.Context
	cancel        context.CancelFunc
	wg            sync.WaitGroup
}

// strategyDecisionFactRow 定义 decision_fact 的 JSONEachRow 行结构。
type strategyDecisionFactRow struct {
	EventTime   string `json:"event_time"`
	Symbol      string `json:"symbol"`
	StrategyID  string `json:"strategy_id"`
	Template    string `json:"template"`
	Interval    string `json:"interval"`
	Stage       string `json:"stage"`
	Decision    string `json:"decision"`
	Reason      string `json:"reason"`
	ReasonCode  string `json:"reason_code"`
	HasPosition uint8  `json:"has_position"`
	IsFinal     uint8  `json:"is_final"`
	IsTradable  uint8  `json:"is_tradable"`
	OpenTime    string `json:"open_time"`
	CloseTime   string `json:"close_time"`
	RouteBucket string `json:"route_bucket"`
	RouteReason string `json:"route_reason"`
	ExtrasJSON  string `json:"extras_json"`
	TraceID     string `json:"trace_id"`
}

// strategySignalFactRow 定义 signal_fact 的 JSONEachRow 行结构。
type strategySignalFactRow struct {
	EventTime       string  `json:"event_time"`
	Symbol          string  `json:"symbol"`
	StrategyID      string  `json:"strategy_id"`
	Template        string  `json:"template"`
	SignalID        string  `json:"signal_id"`
	Action          string  `json:"action"`
	Side            string  `json:"side"`
	SignalType      string  `json:"signal_type"`
	Quantity        float64 `json:"quantity"`
	EntryPrice      float64 `json:"entry_price"`
	StopLoss        float64 `json:"stop_loss"`
	TakeProfitJSON  string  `json:"take_profit_json"`
	Reason          string  `json:"reason"`
	ExitReasonKind  string  `json:"exit_reason_kind"`
	ExitReasonLabel string  `json:"exit_reason_label"`
	RiskReward      float64 `json:"risk_reward"`
	Atr             float64 `json:"atr"`
	TagsJSON        string  `json:"tags_json"`
	TraceID         string  `json:"trace_id"`
}

// newStrategyClickHouseWriter 创建策略分析异步写入器。
func newStrategyClickHouseWriter(parent context.Context, cfg config.ClickHouseConfig) (*strategyClickHouseWriter, error) {
	if !cfg.Enabled {
		return nil, nil
	}
	endpoint := strings.TrimRight(strings.TrimSpace(cfg.Endpoint), "/")
	if endpoint == "" {
		return nil, fmt.Errorf("strategy clickhouse endpoint is required when enabled")
	}
	timeout := cfg.Timeout
	if timeout <= 0 {
		timeout = 3 * time.Second
	}
	queueSize := cfg.QueueSize
	if queueSize <= 0 {
		queueSize = 2048
	}
	flushInterval := cfg.FlushInterval
	if flushInterval <= 0 {
		flushInterval = time.Second
	}
	ctx, cancel := context.WithCancel(parent)
	writer := &strategyClickHouseWriter{
		endpoint:      endpoint,
		database:      strings.TrimSpace(cfg.Database),
		username:      strings.TrimSpace(cfg.Username),
		password:      cfg.Password,
		source:        strings.TrimSpace(cfg.Source),
		client:        &http.Client{Timeout: timeout},
		queue:         make(chan strategyAnalyticsEnvelope, queueSize),
		flushInterval: flushInterval,
		ctx:           ctx,
		cancel:        cancel,
	}
	if writer.database == "" {
		writer.database = "exchange_analytics"
	}
	writer.wg.Add(1)
	go writer.run()
	return writer, nil
}

// WriteSignal 把 signal_fact 事件投递到异步队列。
func (w *strategyClickHouseWriter) WriteSignal(entry strategyengine.StrategySignalAnalyticsEntry) {
	if w == nil {
		return
	}
	row := strategySignalFactRow{
		EventTime:       formatAnalyticsTime(entry.EventTime),
		Symbol:          strings.ToUpper(strings.TrimSpace(entry.Symbol)),
		StrategyID:      strings.TrimSpace(entry.StrategyID),
		Template:        strings.TrimSpace(entry.Template),
		SignalID:        strings.TrimSpace(entry.SignalID),
		Action:          strings.ToUpper(strings.TrimSpace(entry.Action)),
		Side:            strings.ToUpper(strings.TrimSpace(entry.Side)),
		SignalType:      strings.ToUpper(strings.TrimSpace(entry.SignalType)),
		Quantity:        entry.Quantity,
		EntryPrice:      entry.EntryPrice,
		StopLoss:        entry.StopLoss,
		TakeProfitJSON:  analyticsJSONString(entry.TakeProfitJSON, "[]"),
		Reason:          strings.TrimSpace(entry.Reason),
		ExitReasonKind:  strings.TrimSpace(entry.ExitReasonKind),
		ExitReasonLabel: strings.TrimSpace(entry.ExitReasonLabel),
		RiskReward:      entry.RiskReward,
		Atr:             entry.Atr,
		TagsJSON:        analyticsJSONString(entry.TagsJSON, "[]"),
		TraceID:         strings.TrimSpace(entry.TraceID),
	}
	w.enqueue(strategyAnalyticsEnvelope{table: "signal_fact", row: row})
}

// WriteDecision 把 decision_fact 事件投递到异步队列。
func (w *strategyClickHouseWriter) WriteDecision(entry strategyengine.StrategyDecisionAnalyticsEntry) {
	if w == nil {
		return
	}
	row := strategyDecisionFactRow{
		EventTime:   formatAnalyticsTime(entry.EventTime),
		Symbol:      strings.ToUpper(strings.TrimSpace(entry.Symbol)),
		StrategyID:  strings.TrimSpace(entry.StrategyID),
		Template:    strings.TrimSpace(entry.Template),
		Interval:    strings.TrimSpace(entry.Interval),
		Stage:       strings.TrimSpace(entry.Stage),
		Decision:    strings.TrimSpace(entry.Decision),
		Reason:      strings.TrimSpace(entry.Reason),
		ReasonCode:  strings.TrimSpace(entry.ReasonCode),
		HasPosition: boolToUInt8(entry.HasPosition),
		IsFinal:     boolToUInt8(entry.IsFinal),
		IsTradable:  boolToUInt8(entry.IsTradable),
		OpenTime:    formatAnalyticsTime(entry.OpenTime),
		CloseTime:   formatAnalyticsTime(entry.CloseTime),
		RouteBucket: strings.TrimSpace(entry.RouteBucket),
		RouteReason: strings.TrimSpace(entry.RouteReason),
		ExtrasJSON:  analyticsJSONString(entry.ExtrasJSON, "{}"),
		TraceID:     strings.TrimSpace(entry.TraceID),
	}
	w.enqueue(strategyAnalyticsEnvelope{table: "decision_fact", row: row})
}

// Close 停止后台协程并尽量刷完剩余事件。
func (w *strategyClickHouseWriter) Close() error {
	if w == nil {
		return nil
	}
	w.cancel()
	w.wg.Wait()
	return nil
}

// enqueue 把分析事件放入队列，满队列时直接丢弃避免阻塞主流程。
func (w *strategyClickHouseWriter) enqueue(item strategyAnalyticsEnvelope) {
	if w == nil {
		return
	}
	select {
	case w.queue <- item:
	default:
		log.Printf("[strategy-clickhouse] queue full, drop table=%s", item.table)
	}
}

// run 后台按表聚合事件并批量写入 ClickHouse。
func (w *strategyClickHouseWriter) run() {
	defer w.wg.Done()
	ticker := time.NewTicker(w.flushInterval)
	defer ticker.Stop()

	batches := make(map[string][]interface{})
	flush := func() {
		for table, rows := range batches {
			if len(rows) == 0 {
				continue
			}
			if err := w.insertBatch(table, rows); err != nil {
				log.Printf("[strategy-clickhouse] insert batch failed table=%s err=%v", table, err)
			}
			batches[table] = batches[table][:0]
		}
	}

	for {
		select {
		case <-w.ctx.Done():
			for {
				select {
				case item := <-w.queue:
					batches[item.table] = append(batches[item.table], item.row)
				default:
					flush()
					return
				}
			}
		case item := <-w.queue:
			batches[item.table] = append(batches[item.table], item.row)
			if len(batches[item.table]) >= 128 {
				if err := w.insertBatch(item.table, batches[item.table]); err != nil {
					log.Printf("[strategy-clickhouse] insert batch failed table=%s err=%v", item.table, err)
				}
				batches[item.table] = batches[item.table][:0]
			}
		case <-ticker.C:
			flush()
		}
	}
}

// insertBatch 把同一张表的一批行写入 ClickHouse。
func (w *strategyClickHouseWriter) insertBatch(table string, rows []interface{}) error {
	if len(rows) == 0 {
		return nil
	}
	var body bytes.Buffer
	encoder := json.NewEncoder(&body)
	encoder.SetEscapeHTML(false)
	for _, row := range rows {
		if err := encoder.Encode(row); err != nil {
			return fmt.Errorf("encode %s row: %w", table, err)
		}
	}
	query := fmt.Sprintf("INSERT INTO %s.%s FORMAT JSONEachRow", w.database, table)
	req, err := http.NewRequestWithContext(w.ctx, http.MethodPost, w.endpoint+"/?query="+url.QueryEscape(query), &body)
	if err != nil {
		return fmt.Errorf("build request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")
	if w.username != "" {
		req.SetBasicAuth(w.username, w.password)
	}
	resp, err := w.client.Do(req)
	if err != nil {
		return fmt.Errorf("send request: %w", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode >= http.StatusBadRequest {
		msg, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
		return fmt.Errorf("status=%d body=%s", resp.StatusCode, strings.TrimSpace(string(msg)))
	}
	return nil
}

// formatAnalyticsTime 把时间统一格式化为 ClickHouse DateTime64(3) 字符串。
func formatAnalyticsTime(t time.Time) string {
	if t.IsZero() {
		return ""
	}
	return t.UTC().Format("2006-01-02 15:04:05.000")
}

// boolToUInt8 把布尔值转换为 ClickHouse 常用的 UInt8。
func boolToUInt8(v bool) uint8 {
	if v {
		return 1
	}
	return 0
}

// analyticsJSONString 为 JSON 字符串字段提供非空兜底值。
func analyticsJSONString(value, fallback string) string {
	value = strings.TrimSpace(value)
	if value == "" || value == "null" {
		return fallback
	}
	return value
}
