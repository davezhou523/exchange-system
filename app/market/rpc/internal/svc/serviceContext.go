package svc

import (
	"context"

	"exchange-system/app/market/rpc/internal/aggregator"
	"exchange-system/app/market/rpc/internal/config"
	"exchange-system/app/market/rpc/internal/kafka"
	"exchange-system/app/market/rpc/internal/universepool"
	"exchange-system/app/market/rpc/internal/websocket"
	"exchange-system/common/pb/market"
)

// binanceAPIURL Binance Futures REST API 地址
const binanceAPIURL = "https://fapi.binance.com"

type ServiceContext struct {
	Config config.Config

	marketProducer *kafka.Producer
	depthProducer  *kafka.Producer
	wsClient       *websocket.BinanceWebSocketClient
	agg            *aggregator.KlineAggregator
	universeMgr    *universepool.Manager
	cancel         context.CancelFunc
}

// klineDispatcher 把 websocket 收到的 1m 闭合 K 线同时分发给 UniversePool 和聚合器。
type klineDispatcher struct {
	agg         *aggregator.KlineAggregator
	universeMgr *universepool.Manager
}

// OnKline 在不破坏现有聚合链路的前提下，同步更新动态币池快照缓存。
func (d *klineDispatcher) OnKline(ctx context.Context, k *market.Kline) {
	if d == nil || k == nil {
		return
	}
	if d.agg != nil {
		d.agg.OnKline(ctx, k)
	}
}

func NewServiceContext(c config.Config) (*ServiceContext, error) {
	ctx, cancel := context.WithCancel(context.Background())

	producer, err := kafka.NewProducerWithContext(ctx, c.Kafka.Addrs, c.Kafka.Topics.Kline)
	if err != nil {
		cancel()
		return nil, err
	}
	var depthProducer *kafka.Producer
	if c.Kafka.Topics.Depth != "" {
		depthProducer, err = kafka.NewProducerWithContext(ctx, c.Kafka.Addrs, c.Kafka.Topics.Depth)
		if err != nil {
			_ = producer.Close()
			cancel()
			return nil, err
		}
	}

	agg := aggregator.NewKlineAggregator(aggregator.StandardIntervals, producer, c.KlineLogDir, c.WatermarkDelay, aggregator.IndicatorParams{
		Ema21Period: c.Indicators.Ema21Period,
		Ema55Period: c.Indicators.Ema55Period,
		RsiPeriod:   c.Indicators.RsiPeriod,
		AtrPeriod:   c.Indicators.AtrPeriod,
	})

	// 设置每个周期的独立指标参数（指标粒度与K线周期对齐）
	intervalConfigs := make(map[string]aggregator.IntervalIndicatorConfig)
	for name, cfg := range c.IntervalIndicators {
		intervalConfigs[name] = aggregator.IntervalIndicatorConfig{
			Ema21Period: cfg.Ema21Period,
			Ema55Period: cfg.Ema55Period,
			RsiPeriod:   cfg.RsiPeriod,
			AtrPeriod:   cfg.AtrPeriod,
		}
	}
	agg.SetIntervalIndicators(intervalConfigs)

	// 设置指标计算模式
	// "closed"：当前K线参与计算（默认，适合回测和收盘后下单）
	// "previous"：只用历史K线（适合实盘未收盘提前信号）
	switch c.IndicatorMode {
	case "previous":
		agg.SetIndicatorMode(aggregator.IndicatorPrevious)
	default:
		agg.SetIndicatorMode(aggregator.IndicatorClosed)
	}

	// 设置 K 线发射模式
	// "watermark"：watermark 确认后才发射（默认，保证数据完整性）
	// "immediate"：bucket 完成后立即发射（低延迟，适合交易信号）
	switch c.EmitMode {
	case "immediate":
		agg.SetEmitMode(aggregator.EmitImmediate)
	default:
		agg.SetEmitMode(aggregator.EmitWatermark)
	}

	// 设置延迟容忍窗口（Flink watermark + lateness 模型）
	// 超过 watermark + allowedLateness 的迟到数据将被丢弃
	agg.SetAllowedLateness(c.AllowedLateness)

	// 设置 watermark buffer 最大存活时间
	agg.SetMaxWatermarkBufferAge(c.MaxWatermarkBufferAge)

	// 设置 worker 空闲 GC（清理长时间无数据的 symbol worker，防内存泄漏）
	agg.SetWorkerGC(c.WorkerGC.GCInterval, c.WorkerGC.MaxIdleTime)

	// 设置历史数据预热：启动时从 Binance REST API 拉取历史K线填充 ring buffer
	// 使 EMA/RSI/ATR 从第一根实时K线起就有正确值（而非冷启动的假值）
	warmupper := aggregator.NewBinanceWarmupper(binanceAPIURL, c.Binance.Proxy)
	agg.SetWarmupper(warmupper)
	agg.SetWarmupPages(c.WarmupPages)

	// 校验配置组合安全性：EmitImmediate + IndicatorClosed = 实盘未来函数风险
	// 必须在 SetEmitMode + SetIndicatorMode 之后调用
	agg.ValidateConfig()

	dispatcher := &klineDispatcher{
		agg: agg,
	}

	wsClient := websocket.NewBinanceWebSocketClient(
		c.Binance.WebSocketURL,
		c.Binance.Proxy,
		c.Binance.Symbols,
		c.Binance.Intervals,
		producer,
		depthProducer,
		dispatcher,
	)

	var universeMgr *universepool.Manager
	if c.UniversePool.Enabled {
		universeCfg := universepool.Config{
			Enabled:                  c.UniversePool.Enabled,
			CandidateSymbols:         append([]string(nil), c.UniversePool.CandidateSymbols...),
			AllowList:                append([]string(nil), c.UniversePool.AllowList...),
			BlockList:                append([]string(nil), c.UniversePool.BlockList...),
			ValidationMode:           c.UniversePool.ValidationMode,
			TrendPreferredSymbols:    append([]string(nil), c.UniversePool.TrendPreferredSymbols...),
			RangePreferredSymbols:    append([]string(nil), c.UniversePool.RangePreferredSymbols...),
			BreakoutPreferredSymbols: append([]string(nil), c.UniversePool.BreakoutPreferredSymbols...),
			EvaluateInterval:         c.UniversePool.EvaluateInterval,
			MinActiveDuration:        c.UniversePool.MinActiveDuration,
			MinInactiveDuration:      c.UniversePool.MinInactiveDuration,
			CooldownDuration:         c.UniversePool.CooldownDuration,
			AddScoreThreshold:        c.UniversePool.AddScoreThreshold,
			RemoveScoreThreshold:     c.UniversePool.RemoveScoreThreshold,
			Warmup:                   c.UniversePool.Warmup,
		}
		universeLogger := universepool.NewJSONLLogger(c.UniversePool.LogDir)
		universeMgr = universepool.NewManager(universeCfg, nil, agg, wsClient, universeLogger)
		agg.SetEmitObserver(func(k *market.Kline) {
			if universeMgr != nil {
				universeMgr.UpdateSnapshotFromKline(k)
			}
		})
		dispatcher.universeMgr = universeMgr
	}

	wsClient.StartInBackground(ctx)
	if universeMgr != nil {
		go universeMgr.Start(ctx)
	}

	return &ServiceContext{
		Config:         c,
		marketProducer: producer,
		depthProducer:  depthProducer,
		wsClient:       wsClient,
		agg:            agg,
		universeMgr:    universeMgr,
		cancel:         cancel,
	}, nil
}

func (s *ServiceContext) Close() error {
	if s == nil {
		return nil
	}
	if s.cancel != nil {
		s.cancel()
	}
	if s.agg != nil {
		s.agg.Stop()
	}
	var firstErr error
	if s.wsClient != nil {
		if err := s.wsClient.Close(); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	if s.marketProducer != nil {
		if err := s.marketProducer.Close(); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	if s.depthProducer != nil {
		if err := s.depthProducer.Close(); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	return firstErr
}
