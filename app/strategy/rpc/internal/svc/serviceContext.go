package svc

import (
	"context"
	"fmt"

	"exchange-system/app/strategy/rpc/internal/config"
	"exchange-system/app/strategy/rpc/internal/kafka"
	strategyengine "exchange-system/app/strategy/rpc/internal/strategy"
)

type ServiceContext struct {
	Config config.Config

	signalProducer *kafka.Producer
	marketConsumer *kafka.Consumer
	strategies     map[string]*strategyengine.TrendFollowingStrategy
	cancel         context.CancelFunc
}

func NewServiceContext(c config.Config) (*ServiceContext, error) {
	ctx, cancel := context.WithCancel(context.Background())

	signalProducer, err := kafka.NewProducer(c.Kafka.Addrs, c.Kafka.Topics.Signals)
	if err != nil {
		cancel()
		return nil, err
	}

	marketConsumer, err := kafka.NewConsumer(c.Kafka.Addrs, c.Kafka.Topics.MarketData)
	if err != nil {
		_ = signalProducer.Close()
		cancel()
		return nil, err
	}

	strategies := make(map[string]*strategyengine.TrendFollowingStrategy)
	for _, sc := range c.Strategies {
		if !sc.Enabled {
			continue
		}
		if sc.Symbol == "" {
			continue
		}
		strategies[sc.Symbol] = strategyengine.NewTrendFollowingStrategy(sc.Symbol, sc.Parameters, signalProducer)
	}

	if len(strategies) == 0 {
		_ = marketConsumer.Close()
		_ = signalProducer.Close()
		cancel()
		return nil, fmt.Errorf("no enabled strategies")
	}

	if err := marketConsumer.StartConsuming(ctx, func(data map[string]interface{}) error {
		symbol, ok := data["symbol"].(string)
		if !ok {
			return nil
		}
		if strat, exists := strategies[symbol]; exists {
			return strat.HandleMarketData(ctx, data)
		}
		return nil
	}); err != nil {
		_ = marketConsumer.Close()
		_ = signalProducer.Close()
		cancel()
		return nil, err
	}

	return &ServiceContext{
		Config:         c,
		signalProducer: signalProducer,
		marketConsumer: marketConsumer,
		strategies:     strategies,
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