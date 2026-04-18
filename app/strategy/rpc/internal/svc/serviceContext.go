package svc

import (
	"context"
	"fmt"
	"sync"
	"time"

	"exchange-system/app/strategy/rpc/internal/config"
	"exchange-system/app/strategy/rpc/internal/kafka"
	strategyengine "exchange-system/app/strategy/rpc/internal/strategy"
	commonkafka "exchange-system/common/kafka"
	"exchange-system/common/pb/market"
	strategypb "exchange-system/common/pb/strategy"
)

type ServiceContext struct {
	Config config.Config

	signalProducer *kafka.Producer
	marketConsumer *kafka.Consumer

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
		_ = signalProducer.Close()
		cancel()
		return nil, err
	}

	svcCtx := &ServiceContext{
		Config:         c,
		signalProducer: signalProducer,
		marketConsumer: marketConsumer,
		strategies:     make(map[string]*strategyengine.TrendFollowingStrategy),
		cancel:         cancel,
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
		return strat.OnKline(ctx, kline)
	}); err != nil {
		_ = marketConsumer.Close()
		_ = signalProducer.Close()
		cancel()
		return nil, err
	}

	// Periodic lag print for ops/troubleshooting.
	commonkafka.StartConsumerGroupLagReporter(ctx, c.Kafka.Addrs, groupID, c.Kafka.Topics.Kline, 30*time.Second)

	return svcCtx, nil
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
	s.strategies[cfg.Symbol] = strategyengine.NewTrendFollowingStrategy(cfg.Symbol, cfg.Parameters, s.signalProducer, s.Config.SignalLogDir)
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
	if s.signalProducer != nil {
		if err := s.signalProducer.Close(); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	return firstErr
}
