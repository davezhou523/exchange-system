package svc

import (
	"context"
	"fmt"
	"log"
	"strings"
	"sync"
	"time"

	"exchange-system/app/market/rpc/internal/config"
	"exchange-system/app/market/rpc/internal/kafka"
	"exchange-system/app/market/rpc/internal/marketstate"
	strategyengine "exchange-system/app/market/rpc/internal/strategy"
	harvestpathmodel "exchange-system/app/market/rpc/internal/strategy/harvestpath"
	"exchange-system/app/market/rpc/internal/weights"
	marketpb "exchange-system/common/pb/market"
	strategypb "exchange-system/common/pb/strategy"
)

// StrategyEngine 管理策略实例，接收聚合器发射的 K 线并驱动策略决策。
type StrategyEngine struct {
	Config *config.StrategyEngineConfig

	strategies      map[string]*strategyengine.TrendFollowingStrategy
	strategyConfigs map[string]config.StrategyConfig

	mu                  sync.RWMutex
	signalProducer      *kafka.Producer
	lstmPredictor       *harvestpathmodel.LSTMPredictor
	weightEngine        *weights.DefaultEngine
	marketStateDetector marketstate.Detector
}

// NewStrategyEngine 创建策略引擎实例。
func NewStrategyEngine(cfg *config.StrategyEngineConfig, signalProducer *kafka.Producer) (*StrategyEngine, error) {
	se := &StrategyEngine{
		Config:          cfg,
		strategies:      make(map[string]*strategyengine.TrendFollowingStrategy),
		strategyConfigs: make(map[string]config.StrategyConfig),
		mu:              sync.RWMutex{},
		signalProducer:  signalProducer,
	}

	// 初始化市场状态检测器
	se.marketStateDetector = marketstate.NewDetector(marketstate.Config{
		FreshnessWindow:   cfg.MarketState.FreshnessWindow,
		RangeAtrPctMax:    cfg.MarketState.RangeAtrPctMax,
		BreakoutAtrPctMin: cfg.MarketState.BreakoutAtrPctMin,
	})

	// 初始化 HarvestPath LSTM 预测器
	if cfg.HarvestPathLSTM.Enabled {
		lstmCfg := harvestpathmodel.LSTMPredictorConfig{
			Enabled:      true,
			PythonBin:    cfg.HarvestPathLSTM.PythonBin,
			ScriptPath:   cfg.HarvestPathLSTM.ScriptPath,
			DataDir:      cfg.HarvestPathLSTM.DataDir,
			ArtifactsDir: cfg.HarvestPathLSTM.ArtifactsDir,
			Timeout:      time.Duration(cfg.HarvestPathLSTM.TimeoutMs) * time.Millisecond,
		}
		se.lstmPredictor = harvestpathmodel.NewLSTMPredictor(lstmCfg)
	}

	// 初始化权重引擎
	se.weightEngine = weights.NewEngine(weights.Config{
		DefaultTrendWeight:    cfg.Weights.DefaultTrendWeight,
		DefaultRangeWeight:    cfg.Weights.DefaultRangeWeight,
		DefaultBreakoutWeight: cfg.Weights.DefaultBreakoutWeight,
		DefaultRiskScale:      cfg.Weights.DefaultRiskScale,
		LossStreakThreshold:   cfg.Weights.LossStreakThreshold,
		DailyLossSoftLimit:    cfg.Weights.DailyLossSoftLimit,
		DrawdownSoftLimit:     cfg.Weights.DrawdownSoftLimit,
		CoolingPauseDuration:  cfg.Weights.CoolingPauseDuration,
		AtrSpikeRatioMin:      cfg.Weights.AtrSpikeRatioMin,
		VolumeSpikeRatioMin:   cfg.Weights.VolumeSpikeRatioMin,
		CoolingMinSamples:     cfg.Weights.CoolingMinSamples,
		TrendStrategyMix:      cfg.Weights.TrendStrategyMix,
		BreakoutStrategyMix:   cfg.Weights.BreakoutStrategyMix,
		RangeStrategyMix:      cfg.Weights.RangeStrategyMix,
		TrendSymbolWeights:    cfg.Weights.TrendSymbolWeights,
		BreakoutSymbolWeights: cfg.Weights.BreakoutSymbolWeights,
		RangeSymbolWeights:    cfg.Weights.RangeSymbolWeights,
	})

	log.Printf("[策略引擎] 初始化完成 | signalLogDir=%s", cfg.SignalLogDir)
	return se, nil
}

// OnKline 从聚合器收到发射的 K 线时调用，驱动策略引擎。
func (se *StrategyEngine) OnKline(k *marketpb.Kline) {
	if se == nil || k == nil {
		return
	}
	se.mu.RLock()
	strategy, ok := se.strategies[strings.ToUpper(strings.TrimSpace(k.Symbol))]
	se.mu.RUnlock()
	if !ok {
		return
	}
	if err := strategy.OnKline(context.Background(), k); err != nil {
		log.Printf("[策略引擎] OnKline 失败 | symbol=%s interval=%s err=%v", k.Symbol, k.Interval, err)
	}
}

// weightProvider 为策略引擎提供权重查询的适配函数。
func (se *StrategyEngine) weightProvider(symbol string) (weights.Recommendation, bool) {
	return weights.Recommendation{}, false
}

// StartStrategy 启动指定策略。
func (se *StrategyEngine) StartStrategy(cfg config.StrategyConfig, templates map[string]map[string]float64) error {
	se.mu.Lock()
	defer se.mu.Unlock()

	symbol := strings.ToUpper(strings.TrimSpace(cfg.Symbol))
	name := strings.TrimSpace(cfg.Name)
	if name == "" {
		name = symbol
	}

	if _, exists := se.strategies[symbol]; exists {
		return fmt.Errorf("strategy already running for %s", symbol)
	}

	template := strings.TrimSpace(cfg.Template)
	if template == "" {
		template = strings.ToLower(symbol)
	}
	params, err := resolveStrategyParams(cfg, template, templates, se.Config)
	if err != nil {
		return err
	}

	strategy := strategyengine.NewTrendFollowingStrategy(
		symbol, params,
		se.signalProducer, se.signalProducer,
		se.Config.SignalLogDir,
		&strategyengine.RuntimeOptions{
			HarvestPathLSTMPredictor: se.lstmPredictor,
			WeightProvider:           se.weightProvider,
			AnalyticsWriter:          nil,
		},
	)
	se.strategies[symbol] = strategy
	se.strategyConfigs[symbol] = cfg

	log.Printf("[策略引擎] 策略已启动 | symbol=%s name=%s template=%s", symbol, name, template)
	return nil
}

func resolveStrategyParams(cfg config.StrategyConfig, template string, templates map[string]map[string]float64, engineCfg *config.StrategyEngineConfig) (map[string]float64, error) {
	selectedTemplates := templates
	if len(selectedTemplates) == 0 && engineCfg != nil {
		selectedTemplates = engineCfg.Templates
	}

	params := make(map[string]float64)
	if tmpl, ok := selectedTemplates[template]; ok {
		for k, v := range tmpl {
			params[k] = v
		}
	}
	for k, v := range cfg.Parameters {
		params[k] = v
	}
	for k, v := range cfg.Overrides {
		params[k] = v
	}
	if len(params) == 0 {
		if template != "" {
			return nil, fmt.Errorf("template %s not found for %s", template, strings.ToUpper(strings.TrimSpace(cfg.Symbol)))
		}
		return nil, fmt.Errorf("no parameters configured for %s", strings.ToUpper(strings.TrimSpace(cfg.Symbol)))
	}
	return params, nil
}

// StopStrategy 停止指定策略。
func (se *StrategyEngine) StopStrategy(symbol string) error {
	se.mu.Lock()
	defer se.mu.Unlock()

	symbol = strings.ToUpper(strings.TrimSpace(symbol))
	if _, exists := se.strategies[symbol]; !exists {
		return fmt.Errorf("strategy not found for %s", symbol)
	}
	delete(se.strategies, symbol)
	delete(se.strategyConfigs, symbol)
	log.Printf("[策略引擎] 策略已停止 | symbol=%s", symbol)
	return nil
}

// GetStrategyStatus 获取策略状态。
func (se *StrategyEngine) GetStrategyStatus(symbol string) (*strategypb.StrategyStatus, error) {
	se.mu.RLock()
	defer se.mu.RUnlock()

	symbol = strings.ToUpper(strings.TrimSpace(symbol))
	_, ok := se.strategies[symbol]
	if !ok {
		return nil, fmt.Errorf("strategy not found for %s", symbol)
	}
	return &strategypb.StrategyStatus{
		StrategyId: symbol,
		Status:     "RUNNING",
	}, nil
}

// InitStrategies 从配置中启动所有启用的策略。
func (se *StrategyEngine) InitStrategies(strategies []config.StrategyConfig, templates map[string]map[string]float64) {
	for _, cfg := range strategies {
		if !cfg.Enabled {
			continue
		}
		if err := se.StartStrategy(cfg, templates); err != nil {
			log.Printf("[策略引擎] 启动策略失败 | symbol=%s err=%v", cfg.Symbol, err)
		}
	}
}

// HydrateStrategyWarmup 把启动期恢复出来的历史 K 线回灌到指定策略实例，避免等待首条高周期实时闭 K 才恢复指标。
func (se *StrategyEngine) HydrateStrategyWarmup(symbol string, klines []*marketpb.Kline) int {
	if se == nil || len(klines) == 0 {
		return 0
	}
	se.mu.RLock()
	strategy, ok := se.strategies[strings.ToUpper(strings.TrimSpace(symbol))]
	se.mu.RUnlock()
	if !ok || strategy == nil {
		return 0
	}
	for _, kline := range klines {
		strategy.WarmupKline(kline)
	}
	return len(klines)
}

// StrategyWarmupDiagnostics 返回指定策略当前 warmup 诊断视图，供启动日志和排障接口复用。
func (se *StrategyEngine) StrategyWarmupDiagnostics(symbol string) (strategyengine.WarmupDiagnostics, bool) {
	if se == nil {
		return strategyengine.WarmupDiagnostics{}, false
	}
	se.mu.RLock()
	strategy, ok := se.strategies[strings.ToUpper(strings.TrimSpace(symbol))]
	se.mu.RUnlock()
	if !ok || strategy == nil {
		return strategyengine.WarmupDiagnostics{}, false
	}
	return strategy.WarmupDiagnostics(), true
}

// KlineCount 返回所有策略实例的K线缓存状态。
func (se *StrategyEngine) KlineCount(symbol string) (int, int, int, int) {
	se.mu.RLock()
	defer se.mu.RUnlock()
	strategy, ok := se.strategies[strings.ToUpper(strings.TrimSpace(symbol))]
	if !ok {
		return 0, 0, 0, 0
	}
	status := strategy.HistoryWarmupStatus()
	return status.HistoryLen1m, status.HistoryLen15m, status.HistoryLen1h, status.HistoryLen4h
}

// Close 关闭策略引擎。
func (se *StrategyEngine) Close() {
	se.mu.Lock()
	defer se.mu.Unlock()
	se.strategies = make(map[string]*strategyengine.TrendFollowingStrategy)
	log.Println("[策略引擎] 已关闭")
}
