package svc

import (
	"context"
	"fmt"
	"sync"
	"time"

	"exchange-system/app/execution/rpc/executionservice"
	"exchange-system/app/strategy/rpc/internal/config"
	"exchange-system/app/strategy/rpc/internal/kafka"
	strategyengine "exchange-system/app/strategy/rpc/internal/strategy"
	harvestpathmodel "exchange-system/app/strategy/rpc/internal/strategy/harvestpath"
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

	mu         sync.RWMutex
	strategies map[string]*strategyengine.TrendFollowingStrategy
	cancel     context.CancelFunc
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
	marketConsumer, err := kafka.NewConsumer(c.Kafka.Addrs, groupID, c.Kafka.Topics.Kline, c.KlineLogDir)
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
		depthConsumer, err = kafka.NewConsumer(c.Kafka.Addrs, depthGroupID, c.Kafka.Topics.Depth, "")
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
		cancel:                   cancel,
	}

	for _, sc := range c.Strategies {
		if !sc.Enabled || sc.Symbol == "" {
			continue
		}
		svcCtx.upsertStrategyLocked(&strategypb.StrategyConfig{
			Symbol:     sc.Symbol,
			Name:       sc.Name,
			Enabled:    sc.Enabled,
			Parameters: sc.Parameters,
		})
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
	return firstErr
}
