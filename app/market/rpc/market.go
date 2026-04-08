package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"math/rand"
	"time"

	"exchange-system/app/market/rpc/internal/config"
	"exchange-system/app/market/rpc/internal/kafka"
	"exchange-system/app/market/rpc/internal/server"
	"exchange-system/app/market/rpc/internal/svc"
	"exchange-system/common/pb/market"

	"github.com/zeromicro/go-zero/core/conf"
	"github.com/zeromicro/go-zero/core/service"
	"github.com/zeromicro/go-zero/zrpc"
	"google.golang.org/grpc"
	"google.golang.org/grpc/reflection"
)

var (
	configFile   = flag.String("f", "etc/market.yaml", "the config file")
	mockKafka    = flag.Bool("mock-kafka", false, "mock kafka producer")
	mockCount    = flag.Int("mock-count", 200, "mock kafka producer send count")
	mockInterval = flag.Duration("mock-interval", 0, "mock kafka producer send interval (0 means align to kline tf)")
	mockSymbol   = flag.String("mock-symbol", "MOCKUSDT", "mock kline symbol")
	mockKlineTF  = flag.String("mock-interval-tf", "1m", "mock kline interval(tf)")
)

func main() {
	flag.Parse()

	var c config.Config
	conf.MustLoad(*configFile, &c)

	if *mockKafka {
		if err := runMockKafkaProducer(c, *mockCount, *mockInterval, *mockSymbol, *mockKlineTF); err != nil {
			log.Fatalf("mock kafka producer failed: %v", err)
		}
		return
	}

	svcCtx, err := svc.NewServiceContext(c)
	if err != nil {
		log.Fatalf("failed to init service context: %v", err)
	}
	defer func() {
		_ = svcCtx.Close()
	}()

	s := zrpc.MustNewServer(c.RpcServerConf, func(grpcServer *grpc.Server) {
		market.RegisterMarketServiceServer(grpcServer, server.NewMarketServiceServer(svcCtx))

		if c.Mode == service.DevMode || c.Mode == service.TestMode {
			reflection.Register(grpcServer)
		}
	})
	defer s.Stop()

	fmt.Printf("Starting rpc server at %s...\n", c.ListenOn)
	s.Start()
}

func runMockKafkaProducer(c config.Config, count int, interval time.Duration, symbol string, tf string) error {
	//go run market.go -mock-kafka -mock-interval-tf=1m -mock-interval=0 -mock-count=0

	if count <= 0 {
		count = 20
	}
	if symbol == "" {
		symbol = "MOCKUSDT"
	}
	if tf == "" {
		tf = "1m"
	}

	tfDur, err := parseTFDuration(tf)
	if err != nil {
		return err
	}

	producer, err := kafka.NewProducer(c.Kafka.Addrs, c.Kafka.Topics.Kline)
	if err != nil {
		return err
	}
	defer func() {
		_ = producer.Close()
	}()

	r := rand.New(rand.NewSource(time.Now().UnixNano()))
	base := 1000.0

	log.Printf("[mock] producing kline batch: brokers=%v topic=%s count=%d interval=%s symbol=%s tf=%s", c.Kafka.Addrs, c.Kafka.Topics.Kline, count, interval, symbol, tf)

	for i := 0; i < count; i++ {
		now := time.Now()

		closeTime := now.Truncate(tfDur)
		openTime := closeTime.Add(-tfDur)

		change := (r.Float64() - 0.5) * base * 0.01
		open := base + (r.Float64()-0.5)*base*0.002
		close_ := open + change
		high := max(open, close_) + r.Float64()*base*0.002
		low := min(open, close_) - r.Float64()*base*0.002
		volume := 1 + r.Float64()*10

		k := &market.Kline{
			Symbol:    symbol,
			Interval:  tf,
			OpenTime:  openTime.UnixMilli(),
			CloseTime: closeTime.UnixMilli(),
			Open:      open,
			High:      high,
			Low:       low,
			Close:     close_,
			Volume:    volume,
			IsClosed:  true,
		}

		if err := producer.SendMarketData(context.Background(), k); err != nil {
			return err
		}

		log.Printf("[mock] sent %d/%d: symbol=%s tf=%s close=%.4f openTime=%d closeTime=%d", i+1, count, k.Symbol, k.Interval, k.Close, k.OpenTime, k.CloseTime)
		base = close_

		if interval > 0 {
			time.Sleep(interval)
			continue
		}

		nextClose := closeTime.Add(tfDur)
		sleep := time.Until(nextClose)
		if sleep > 0 {
			time.Sleep(sleep)
		}
	}

	log.Printf("[mock] producing done: count=%d", count)
	return nil
}

func max(a, b float64) float64 {
	if a > b {
		return a
	}
	return b
}

func min(a, b float64) float64 {
	if a < b {
		return a
	}
	return b
}

func parseTFDuration(tf string) (time.Duration, error) {
	switch tf {
	case "1m":
		return time.Minute, nil
	case "15m":
		return 15 * time.Minute, nil
	case "1h":
		return time.Hour, nil
	case "4h":
		return 4 * time.Hour, nil
	default:
		return 0, fmt.Errorf("unsupported tf: %s", tf)
	}
}
