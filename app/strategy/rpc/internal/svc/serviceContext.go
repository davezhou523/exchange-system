package svc

import (
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
	executionCli             executionservice.ExecutionService

	mu                  sync.RWMutex
	strategies          map[string]*strategyengine.TrendFollowingStrategy
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
		executionCli:             executionCli,
		strategies:               make(map[string]*strategyengine.TrendFollowingStrategy),
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
		_ = marketConsumer.Close()
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
			if depth == nil || depth.Symbol == "" {
				return nil
			}
			svcCtx.mu.RLock()
			strat := svcCtx.strategies[depth.Symbol]
			svcCtx.mu.RUnlock()
			if strat == nil {
				return nil
			}
			strat.OnDepth(depth)
			return nil
		}); err != nil {
			_ = marketConsumer.Close()
			_ = depthConsumer.Close()
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
		return
	}
	opts := &strategyengine.RuntimeOptions{
		HarvestPathLSTMPredictor: s.harvestPathLSTMPredictor,
		WeightProvider:           s.LatestWeightRecommendation,
		AnalyticsWriter:          s.analyticsWriter,
	}
	if current, ok := s.strategies[cfg.Symbol]; ok && current != nil {
		current.UpdateRuntimeConfig(cfg.Parameters, s.Config.SignalLogDir, opts)
		return
	}
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
