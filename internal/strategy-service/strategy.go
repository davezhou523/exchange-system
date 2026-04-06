package main

import (
	"context"
	"flag"
	"log"
	"os"
	"os/signal"
	"syscall"

	"exchange-system/internal/strategy-service/internal/config"
	"exchange-system/internal/strategy-service/internal/kafka"
	"exchange-system/internal/strategy-service/internal/strategy"
	"github.com/zeromicro/go-zero/core/conf"
)

func main() {
	var configFile = flag.String("f", "etc/strategy.yaml", "the config file")
	flag.Parse()

	var c config.Config
	conf.MustLoad(*configFile, &c)

	// 创建Kafka生产者和消费者
	signalProducer, err := kafka.NewProducer(c.Kafka.Addrs, c.Kafka.Topics.Signals)
	if err != nil {
		log.Fatalf("Failed to create signal producer: %v", err)
	}
	defer signalProducer.Close()

	marketConsumer, err := kafka.NewConsumer(c.Kafka.Addrs, c.Kafka.Topics.MarketData)
	if err != nil {
		log.Fatalf("Failed to create market data consumer: %v", err)
	}
	defer marketConsumer.Close()

	// 创建策略实例
	strategies := make(map[string]*strategy.TrendFollowingStrategy)
	for _, strategyConfig := range c.Strategies {
		if strategyConfig.Enabled {
			strategy := strategy.NewTrendFollowingStrategy(
				strategyConfig.Symbol,
				strategyConfig.Parameters,
				signalProducer,
			)
			strategies[strategyConfig.Symbol] = strategy
			log.Printf("Strategy enabled for symbol: %s", strategyConfig.Symbol)
		}
	}

	// 设置上下文和信号处理
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	// 开始消费市场数据
	err = marketConsumer.StartConsuming(ctx, func(data map[string]interface{}) error {
		symbol, ok := data["symbol"].(string)
		if !ok {
			return nil
		}

		// 将数据传递给对应的策略
		if strategy, exists := strategies[symbol]; exists {
			return strategy.HandleMarketData(ctx, data)
		}

		return nil
	})

	if err != nil {
		log.Fatalf("Failed to start consuming market data: %v", err)
	}

	log.Printf("Strategy service started successfully with %d active strategies", len(strategies))

	// 等待退出信号
	<-ctx.Done()
	log.Println("Strategy service shutting down...")
}
