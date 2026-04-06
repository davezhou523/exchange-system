package main

import (
	"context"
	"flag"
	"log"

	"exchange-system/internal/market-service/internal/config"
	"exchange-system/internal/market-service/internal/kafka"
	"exchange-system/internal/market-service/internal/websocket"
	"github.com/zeromicro/go-zero/core/conf"
)

func main() {
	var configFile = flag.String("f", "etc/market.yaml", "the config file")
	flag.Parse()

	var c config.Config
	conf.MustLoad(*configFile, &c)

	// 创建Kafka生产者
	producer, err := kafka.NewProducer(c.Kafka.Addrs, c.Kafka.Topic.MarketData)
	if err != nil {
		log.Fatalf("Failed to create Kafka producer: %v", err)
	}
	defer producer.Close()

	// 创建Binance WebSocket客户端
	wsClient := websocket.NewBinanceWebSocketClient(
		c.Binance.WebSocketURL,
		c.Binance.Symbols,
		c.Binance.Intervals,
		producer,
	)

	ctx := context.Background()

	// 连接WebSocket
	if err := wsClient.Connect(ctx); err != nil {
		log.Fatalf("Failed to connect to WebSocket: %v", err)
	}
	defer wsClient.Close()

	// 开始流式传输
	if err := wsClient.StartStreaming(ctx); err != nil {
		log.Fatalf("Failed to start streaming: %v", err)
	}

	log.Printf("Market service started successfully. Streaming data for symbols: %v", c.Binance.Symbols)

	// 保持服务运行
	select {}
}
