package svc

import (
	"context"

	"exchange-system/app/market/rpc/internal/aggregator"
	"exchange-system/app/market/rpc/internal/config"
	"exchange-system/app/market/rpc/internal/kafka"
	"exchange-system/app/market/rpc/internal/websocket"
)

type ServiceContext struct {
	Config config.Config

	marketProducer *kafka.Producer
	wsClient       *websocket.BinanceWebSocketClient
	agg            *aggregator.KlineAggregator
	cancel         context.CancelFunc
}

func NewServiceContext(c config.Config) (*ServiceContext, error) {
	ctx, cancel := context.WithCancel(context.Background())

	producer, err := kafka.NewProducerWithContext(ctx, c.Kafka.Addrs, c.Kafka.Topics.Kline)
	if err != nil {
		cancel()
		return nil, err
	}

	agg := aggregator.NewKlineAggregator(aggregator.StandardIntervals, producer)

	wsClient := websocket.NewBinanceWebSocketClient(
		c.Binance.WebSocketURL,
		c.Binance.Proxy,
		c.Binance.Symbols,
		c.Binance.Intervals,
		producer,
		agg,
		c.KlineLogDir,
	)

	wsClient.StartInBackground(ctx)

	return &ServiceContext{
		Config:         c,
		marketProducer: producer,
		wsClient:       wsClient,
		agg:            agg,
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
		s.agg.FlushAll(context.Background())
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
	return firstErr
}
