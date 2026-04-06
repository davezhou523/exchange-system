package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"strconv"
	"syscall"

	"exchange-system/internal/execution-service/internal/binance"
	"exchange-system/internal/execution-service/internal/config"
	"exchange-system/internal/execution-service/internal/kafka"
	"github.com/zeromicro/go-zero/core/conf"
)

type ExecutionService struct {
	binanceClient *binance.BinanceClient
	riskConfig    RiskConfig
	orderProducer *kafka.Producer
}

type RiskConfig struct {
	MaxPositionSize float64
	MaxLeverage     float64
	StopLossPercent float64
}

func NewExecutionService(binanceClient *binance.BinanceClient, riskConfig RiskConfig, orderProducer *kafka.Producer) *ExecutionService {
	return &ExecutionService{
		binanceClient: binanceClient,
		riskConfig:    riskConfig,
		orderProducer: orderProducer,
	}
}

func (s *ExecutionService) HandleSignal(ctx context.Context, signal map[string]interface{}) error {
	log.Printf("Received signal: %+v", signal)

	// 风险检查
	if err := s.riskCheck(signal); err != nil {
		log.Printf("Risk check failed: %v", err)
		return err
	}

	// 执行订单
	orderResp, err := s.executeOrder(ctx, signal)
	if err != nil {
		log.Printf("Order execution failed: %v", err)
		return err
	}

	// 发送订单结果到Kafka
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

func (s *ExecutionService) riskCheck(signal map[string]interface{}) error {
	// 获取账户信息
	accountInfo, err := s.binanceClient.GetAccountInfo(context.Background())
	if err != nil {
		return fmt.Errorf("failed to get account info: %v", err)
	}

	// 检查仓位大小
	quantity, ok := signal["quantity"].(float64)
	if !ok {
		return fmt.Errorf("invalid quantity type")
	}

	entryPrice, ok := signal["entry_price"].(float64)
	if !ok {
		return fmt.Errorf("invalid entry price type")
	}

	positionSize := quantity * entryPrice
	totalBalance, err := strconv.ParseFloat(accountInfo.TotalWalletBalance, 64)
	if err != nil {
		return fmt.Errorf("failed to parse wallet balance: %v", err)
	}

	if positionSize > totalBalance*s.riskConfig.MaxPositionSize {
		return fmt.Errorf("position size exceeds limit: %.2f > %.2f",
			positionSize, totalBalance*s.riskConfig.MaxPositionSize)
	}

	return nil
}

func (s *ExecutionService) executeOrder(ctx context.Context, signal map[string]interface{}) (*binance.OrderResponse, error) {
	symbol, ok := signal["symbol"].(string)
	if !ok {
		return nil, fmt.Errorf("invalid symbol type")
	}

	action, ok := signal["action"].(string)
	if !ok {
		return nil, fmt.Errorf("invalid action type")
	}

	side, ok := signal["side"].(string)
	if !ok {
		return nil, fmt.Errorf("invalid side type")
	}

	quantity, ok := signal["quantity"].(float64)
	if !ok {
		return nil, fmt.Errorf("invalid quantity type")
	}

	// 根据action和side确定交易方向
	var orderSide string
	if action == "BUY" {
		orderSide = "BUY"
	} else {
		orderSide = "SELL"
	}

	return s.binanceClient.CreateOrder(ctx, symbol, orderSide, side, quantity)
}

func main() {
	var configFile = flag.String("f", "etc/execution.yaml", "the config file")
	flag.Parse()

	var c config.Config
	conf.MustLoad(*configFile, &c)

	// 创建Binance客户端
	binanceClient := binance.NewBinanceClient(
		c.Binance.BaseURL,
		c.Binance.APIKey,
		c.Binance.SecretKey,
	)

	// 创建Kafka生产者和消费者
	orderProducer, err := kafka.NewProducer(c.Kafka.Addrs, c.Kafka.Topics.Orders)
	if err != nil {
		log.Fatalf("Failed to create order producer: %v", err)
	}
	defer orderProducer.Close()

	signalConsumer, err := kafka.NewConsumer(c.Kafka.Addrs, c.Kafka.Topics.Signals)
	if err != nil {
		log.Fatalf("Failed to create signal consumer: %v", err)
	}
	defer signalConsumer.Close()

	// 创建执行服务
	executionService := NewExecutionService(
		binanceClient,
		RiskConfig{
			MaxPositionSize: c.Risk.MaxPositionSize,
			MaxLeverage:     c.Risk.MaxLeverage,
			StopLossPercent: c.Risk.StopLossPercent,
		},
		orderProducer,
	)

	// 设置上下文和信号处理
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	// 开始消费信号数据
	err = signalConsumer.StartConsuming(ctx, func(data map[string]interface{}) error {
		return executionService.HandleSignal(ctx, data)
	})

	if err != nil {
		log.Fatalf("Failed to start consuming signals: %v", err)
	}

	log.Println("Execution service started successfully")

	// 等待退出信号
	<-ctx.Done()
	log.Println("Execution service shutting down...")
}
