package svc

import (
	"context"

	"exchange-system/app/market/rpc/internal/config"
	"exchange-system/app/market/rpc/internal/kafka"
	"exchange-system/app/market/rpc/internal/websocket"
)

type ServiceContext struct {
	Config config.Config

	marketProducer *kafka.Producer
	wsClient       *websocket.BinanceWebSocketClient
	cancel         context.CancelFunc
}

func NewServiceContext(c config.Config) (*ServiceContext, error) {
	ctx, cancel := context.WithCancel(context.Background())

	producer, err := kafka.NewProducer(c.Kafka.Addrs, c.Kafka.Topic.MarketData)
	if err != nil {
		cancel()
		return nil, err
	}

	wsClient := websocket.NewBinanceWebSocketClient(
		c.Binance.WebSocketURL,
		c.Binance.Symbols,
		c.Binance.Intervals,
		producer,
	)

	if err := wsClient.Connect(ctx); err != nil {
		_ = producer.Close()
		cancel()
		return nil, err
	}

	if err := wsClient.StartStreaming(ctx); err != nil {
		_ = wsClient.Close()
		_ = producer.Close()
		cancel()
		return nil, err
	}

	return &ServiceContext{
		Config:         c,
		marketProducer: producer,
		wsClient:       wsClient,
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