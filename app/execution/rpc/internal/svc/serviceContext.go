package svc

import (
	"context"
	"fmt"
	"strconv"

	"exchange-system/app/execution/rpc/internal/binance"
	"exchange-system/app/execution/rpc/internal/config"
	"exchange-system/app/execution/rpc/internal/kafka"
)

type ServiceContext struct {
	Config config.Config

	executionService *ExecutionService
	signalConsumer   *kafka.Consumer
	orderProducer    *kafka.Producer
	cancel           context.CancelFunc
}

type RiskConfig struct {
	MaxPositionSize float64
	MaxLeverage     float64
	StopLossPercent float64
}

type ExecutionService struct {
	binanceClient *binance.BinanceClient
	riskConfig    RiskConfig
	orderProducer *kafka.Producer
}

func NewExecutionService(binanceClient *binance.BinanceClient, riskConfig RiskConfig, orderProducer *kafka.Producer) *ExecutionService {
	return &ExecutionService{binanceClient: binanceClient, riskConfig: riskConfig, orderProducer: orderProducer}
}

func (s *ExecutionService) HandleSignal(ctx context.Context, signal map[string]interface{}) error {
	if err := s.riskCheck(ctx, signal); err != nil {
		return err
	}
	orderResp, err := s.executeOrder(ctx, signal)
	if err != nil {
		return err
	}
	orderResult := map[string]interface{}{
		"order_id":    orderResp.OrderID,
		"symbol":      orderResp.Symbol,
		"status":      orderResp.Status,
		"side":        signal["side"],
		"quantity":    signal["quantity"],
		"avg_price":   orderResp.AvgPrice,
		"timestamp":   orderResp.Time,
		"strategy_id": signal["strategy_id"],
	}
	return s.orderProducer.SendMarketData(ctx, orderResult)
}

func (s *ExecutionService) riskCheck(ctx context.Context, signal map[string]interface{}) error {
	accountInfo, err := s.binanceClient.GetAccountInfo(ctx)
	if err != nil {
		return fmt.Errorf("get account info: %v", err)
	}
	quantity, ok := signal["quantity"].(float64)
	if !ok {
		return fmt.Errorf("invalid quantity")
	}
	entryPrice, ok := signal["entry_price"].(float64)
	if !ok {
		return fmt.Errorf("invalid entry_price")
	}
	positionSize := quantity * entryPrice
	totalBalance, err := strconv.ParseFloat(accountInfo.TotalWalletBalance, 64)
	if err != nil {
		return fmt.Errorf("parse wallet balance: %v", err)
	}
	limit := totalBalance * s.riskConfig.MaxPositionSize
	if positionSize > limit {
		return fmt.Errorf("position size exceeds limit: %.6f > %.6f", positionSize, limit)
	}
	return nil
}

func (s *ExecutionService) executeOrder(ctx context.Context, signal map[string]interface{}) (*binance.OrderResponse, error) {
	symbol, ok := signal["symbol"].(string)
	if !ok {
		return nil, fmt.Errorf("invalid symbol")
	}
	action, ok := signal["action"].(string)
	if !ok {
		return nil, fmt.Errorf("invalid action")
	}
	positionSide, ok := signal["side"].(string)
	if !ok {
		return nil, fmt.Errorf("invalid side")
	}
	quantity, ok := signal["quantity"].(float64)
	if !ok {
		return nil, fmt.Errorf("invalid quantity")
	}
	orderSide := "SELL"
	if action == "BUY" {
		orderSide = "BUY"
	}
	return s.binanceClient.CreateOrder(ctx, symbol, orderSide, positionSide, quantity)
}

func NewServiceContext(c config.Config) (*ServiceContext, error) {
	ctx, cancel := context.WithCancel(context.Background())

	orderProducer, err := kafka.NewProducer(c.Kafka.Addrs, c.Kafka.Topics.Orders)
	if err != nil {
		cancel()
		return nil, err
	}

	signalConsumer, err := kafka.NewConsumer(c.Kafka.Addrs, c.Kafka.Topics.Signals)
	if err != nil {
		_ = orderProducer.Close()
		cancel()
		return nil, err
	}

	binanceClient := binance.NewBinanceClient(c.Binance.BaseURL, c.Binance.APIKey, c.Binance.SecretKey)
	execService := NewExecutionService(binanceClient, RiskConfig{
		MaxPositionSize: c.Risk.MaxPositionSize,
		MaxLeverage:     c.Risk.MaxLeverage,
		StopLossPercent: c.Risk.StopLossPercent,
	}, orderProducer)

	if err := signalConsumer.StartConsuming(ctx, func(data map[string]interface{}) error {
		return execService.HandleSignal(ctx, data)
	}); err != nil {
		_ = signalConsumer.Close()
		_ = orderProducer.Close()
		cancel()
		return nil, err
	}

	return &ServiceContext{
		Config:          c,
		executionService: execService,
		signalConsumer:   signalConsumer,
		orderProducer:    orderProducer,
		cancel:           cancel,
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
	if s.signalConsumer != nil {
		if err := s.signalConsumer.Close(); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	if s.orderProducer != nil {
		if err := s.orderProducer.Close(); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	return firstErr
}