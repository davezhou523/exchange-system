package svc

import (
	"context"
	"fmt"
	"log"
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
	marketStateLogs     *marketstate.LogState
	weightEngine        weights.Engine
	weightLogs          *weights.LogState
	latestWeights       map[string]weights.Recommendation
	universeSnapshots   map[string]universe.Snapshot
	universeStates      map[string]universeRuntimeState
	universeLogs        *universeLogState
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
	templateMap := make(map[string]string)
	for k, v := range uc.StaticTemplateMap {
		templateMap[k] = v
	}
	for _, sc := range cfg.Strategies {
		if sc.Symbol == "" || sc.Template == "" {
			continue
		}
		if _, ok := templateMap[sc.Symbol]; !ok {
			templateMap[sc.Symbol] = sc.Template
		}
	}
	return universe.Config{
		CandidateSymbols:      candidates,
		StaticTemplateMap:     templateMap,
		FreshnessWindow:       uc.FreshnessWindow,
		RequireFinal:          uc.RequireFinal,
		RequireTradable:       uc.RequireTradable,
		RequireClean:          uc.RequireClean,
		BTCTrendTemplate:      uc.BTCTrendTemplate,
		BTCTrendAtrPctMax:     uc.BTCTrendAtrPctMax,
		HighBetaSafeTemplate:  uc.HighBetaSafeTemplate,
		HighBetaSafeSymbols:   uc.HighBetaSafeSymbols,
		HighBetaSafeAtrPct:    uc.HighBetaSafeAtrPct,
		HighBetaDisableAtrPct: uc.HighBetaDisableAtrPct,
	}
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

// updateUniverseSnapshot 记录某个交易对最近一根 1m K 线的健康状态，供 UniverseSelector 使用。
func (s *ServiceContext) updateUniverseSnapshot(kline *market.Kline) {
	if s == nil || s.universeSelector == nil || kline == nil || kline.Symbol == "" || kline.Interval != "1m" {
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.universeSnapshots[kline.Symbol] = universe.Snapshot{
		Symbol:       kline.Symbol,
		UpdatedAt:    time.Now().UTC(),
		LastEventMs:  kline.EventTime,
		IsDirty:      kline.IsDirty,
		IsTradable:   kline.IsTradable,
		IsFinal:      kline.IsFinal,
		LastInterval: kline.Interval,
		Close:        kline.Close,
		Atr:          kline.Atr,
		Ema21:        kline.Ema21,
		Ema55:        kline.Ema55,
	}
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

// evaluateUniverse 基于最新快照生成目标策略集合，并把 diff 应用到运行时。
func (s *ServiceContext) evaluateUniverse(now time.Time) {
	if s == nil || s.universeSelector == nil {
		return
	}
	s.mu.RLock()
	snapshots := make(map[string]universe.Snapshot, len(s.universeSnapshots))
	stateFeatures := make(map[string]marketstate.Features, len(s.universeSnapshots))
	stateResults := make(map[string]marketstate.Result, len(s.universeSnapshots))
	for k, v := range s.universeSnapshots {
		if s.marketStateDetector != nil {
			features := marketstate.BuildFeaturesFromSnapshotValues(
				v.Symbol,
				v.LastInterval,
				v.Close,
				v.Ema21,
				v.Ema55,
				v.Atr,
				v.IsDirty,
				v.IsTradable,
				v.IsFinal,
				v.UpdatedAt,
			)
			result := s.marketStateDetector.Detect(now, features)
			v.MarketState = result.State
			stateFeatures[k] = features
			stateResults[k] = result
		}
		snapshots[k] = v
	}
	s.mu.RUnlock()
	if s.marketStateLogs != nil {
		for symbol, result := range stateResults {
			s.marketStateLogs.Write(s.Config.SignalLogDir, stateFeatures[symbol], result, now)
		}
		s.marketStateLogs.WriteMeta(s.Config.SignalLogDir, marketstate.BuildMetaLogEntry(now, stateFeatures, stateResults), now)
	}

	desired := s.universeSelector.Evaluate(now, snapshots)
	if s.weightEngine != nil && s.weightLogs != nil {
		weightInput := s.buildWeightInputs(now, desired, stateResults)
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
			"[weights] evaluate symbols=%d recommendations=%d paused=%v market_state=%s source=%s",
			len(weightInput.Symbols),
			len(weightOutput.Recommendations),
			weightOutput.MarketPaused,
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

// buildWeightInputs 把当前轮 Universe 和 MarketState 结果整理成 Phase 5 权重引擎输入。
func (s *ServiceContext) buildWeightInputs(now time.Time, desired []universe.DesiredStrategy, stateResults map[string]marketstate.Result) weights.Inputs {
	in := weights.Inputs{
		MarketState:  marketstate.Aggregate(now, stateResults),
		Templates:    make(map[string]string),
		SymbolScores: make(map[string]float64),
		UpdatedAt:    now.UTC(),
	}
	for _, d := range desired {
		if !d.Enabled {
			continue
		}
		in.Symbols = append(in.Symbols, d.Symbol)
		in.Templates[d.Symbol] = d.Template
		in.SymbolScores[d.Symbol] = scoreDesiredStrategy(d)
	}
	return in
}

// scoreDesiredStrategy 为 Phase 5 第一版提供一个最小 symbol score。
func scoreDesiredStrategy(d universe.DesiredStrategy) float64 {
	switch d.Reason {
	case "market_state_trend":
		return 1.2
	case "trend_strong":
		return 1.1
	default:
		return 1
	}
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
		marketStateDetector:      marketstate.NewDetector(marketstate.Config{}),
		marketStateLogs:          marketstate.NewLogState(),
		weightEngine:             weights.NewEngine(weights.Config{}),
		weightLogs:               weights.NewLogState(),
		latestWeights:            make(map[string]weights.Recommendation),
		universeSnapshots:        make(map[string]universe.Snapshot),
		universeStates:           make(map[string]universeRuntimeState),
		universeLogs:             newUniverseLogState(),
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
	s.strategies[cfg.Symbol] = strategyengine.NewTrendFollowingStrategy(
		cfg.Symbol,
		cfg.Parameters,
		s.signalProducer,
		s.harvestPathProducer,
		s.Config.SignalLogDir,
		&strategyengine.RuntimeOptions{
			HarvestPathLSTMPredictor: s.harvestPathLSTMPredictor,
			WeightProvider:           s.LatestWeightRecommendation,
		},
	)
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
	return firstErr
}
