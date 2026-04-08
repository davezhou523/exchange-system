package svc

import (
	"context"
	"fmt"
	"strconv"

	"exchange-system/app/execution/rpc/internal/binance"
	"exchange-system/app/execution/rpc/internal/config"
	"exchange-system/app/execution/rpc/internal/kafka"
	strategypb "exchange-system/common/pb/strategy"
)

type ServiceContext struct {
	Config config.Config

	executionService *ExecutionService
	signalConsumer   *kafka.Consumer
	orderProducer    *kafka.Producer
	cancel           context.CancelFunc
}

func (s *ServiceContext) CreateOrder(ctx context.Context, symbol, side, positionSide string, quantity float64) (*binance.OrderResponse, error) {
	if s == nil || s.executionService == nil {
		return nil, fmt.Errorf("execution service not initialized")
	}
	return s.executionService.binanceClient.CreateOrder(ctx, symbol, side, positionSide, quantity)
}

func (s *ServiceContext) GetAccount(ctx context.Context) (*binance.AccountInfo, error) {
	if s == nil || s.executionService == nil {
		return nil, fmt.Errorf("execution service not initialized")
	}
	return s.executionService.binanceClient.GetAccountInfo(ctx)
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

func (s *ExecutionService) HandleSignal(ctx context.Context, sig *strategypb.Signal) error {
	if sig == nil {
		return nil
	}
	if err := s.riskCheck(ctx, sig); err != nil {
		return err
	}
	orderResp, err := s.executeOrder(ctx, sig)
	if err != nil {
		return err
	}
	orderResult := map[string]interface{}{
		"order_id":    orderResp.OrderID,
		"symbol":      orderResp.Symbol,
		"status":      orderResp.Status,
		"side":        sig.GetSide(),
		"quantity":    sig.GetQuantity(),
		"avg_price":   orderResp.AvgPrice,
		"timestamp":   orderResp.Time,
		"strategy_id": sig.GetStrategyId(),
	}
	return s.orderProducer.SendMarketData(ctx, orderResult)
}

func (s *ExecutionService) riskCheck(ctx context.Context, sig *strategypb.Signal) error {
	accountInfo, err := s.binanceClient.GetAccountInfo(ctx)
	if err != nil {
		return fmt.Errorf("get account info: %v", err)
	}
	quantity := sig.GetQuantity()
	entryPrice := sig.GetEntryPrice()
	if quantity <= 0 {
		return fmt.Errorf("invalid quantity")
	}
	if entryPrice <= 0 {
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

func (s *ExecutionService) executeOrder(ctx context.Context, sig *strategypb.Signal) (*binance.OrderResponse, error) {
	symbol := sig.GetSymbol()
	if symbol == "" {
		return nil, fmt.Errorf("invalid symbol")
	}
	action := sig.GetAction()
	positionSide := sig.GetSide()
	quantity := sig.GetQuantity()
	if quantity <= 0 {
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

	groupID := c.Kafka.Group
	if groupID == "" {
		if c.Name != "" {
			groupID = c.Name + "-signal"
		} else {
			groupID = "execution-signal"
		}
	}
	signalConsumer, err := kafka.NewConsumer(c.Kafka.Addrs, groupID, c.Kafka.Topics.Signal)
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

	if err := signalConsumer.StartConsuming(ctx, func(sig *strategypb.Signal) error {
		return execService.HandleSignal(ctx, sig)
	}); err != nil {
		_ = signalConsumer.Close()
		_ = orderProducer.Close()
		cancel()
		return nil, err
	}

	return &ServiceContext{
		Config:           c,
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
