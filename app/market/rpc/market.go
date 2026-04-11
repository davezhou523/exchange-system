package main

import (
	"bufio"
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"math/rand"
	"os"
	"path/filepath"
	"time"

	"exchange-system/app/market/rpc/internal/aggregator"
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
	replayFile   = flag.String("replay", "", "replay 1m kline log file (jsonl) and produce 15m/1h/4h aggregated klines to kafka")
	replaySpeed  = flag.Duration("replay-speed", 0, "replay interval between klines (0 = no delay)")
)

//# 回放单个文件
//go run market.go -replay data/kline/ETHUSDT/2026-04-11.jsonl
//
//# 回放某天所有symbol
//go run market.go -replay "data/kline/*/2026-04-11.jsonl"
//
//# 慢速回放（每条间隔100ms）
//go run market.go -replay data/kline/ETHUSDT/2026-04-11.jsonl -replay-speed=100ms

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

	if *replayFile != "" {
		if err := runReplayKlineLog(c, *replayFile, *replaySpeed); err != nil {
			log.Fatalf("replay failed: %v", err)
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

func runReplayKlineLog(c config.Config, path string, speed time.Duration) error {
	// Support glob pattern, e.g. "data/kline/ETHUSDT/*.jsonl"
	matches, err := filepath.Glob(path)
	if err != nil {
		return fmt.Errorf("invalid path %s: %v", path, err)
	}
	if len(matches) == 0 {
		// treat as single file
		matches = []string{path}
	}

	producer, err := kafka.NewProducer(c.Kafka.Addrs, c.Kafka.Topics.Kline)
	if err != nil {
		return err
	}
	defer func() { _ = producer.Close() }()

	agg := aggregator.NewKlineAggregator(aggregator.StandardIntervals, producer)
	ctx := context.Background()

	totalSent := 0
	for _, fp := range matches {
		sent, err := replayKlineFile(ctx, fp, speed, producer, agg)
		if err != nil {
			return err
		}
		totalSent += sent
	}

	agg.FlushAll(ctx)
	log.Printf("[replay] done: total 1m klines=%d", totalSent)
	return nil
}

func replayKlineFile(ctx context.Context, path string, speed time.Duration, producer *kafka.Producer, agg *aggregator.KlineAggregator) (int, error) {
	f, err := os.Open(path)
	if err != nil {
		return 0, fmt.Errorf("open %s: %v", path, err)
	}
	defer f.Close()

	log.Printf("[replay] reading %s", path)

	sent := 0
	scanner := bufio.NewScanner(f)
	buf := make([]byte, 0, 64*1024)
	scanner.Buffer(buf, 1024*1024)

	for scanner.Scan() {
		line := scanner.Bytes()
		if len(line) == 0 {
			continue
		}

		var entry struct {
			Symbol      string  `json:"symbol"`
			Interval    string  `json:"interval"`
			OpenTime    int64   `json:"openTime"`
			CloseTime   int64   `json:"closeTime"`
			Open        float64 `json:"open"`
			High        float64 `json:"high"`
			Low         float64 `json:"low"`
			Close       float64 `json:"close"`
			Volume      float64 `json:"volume"`
			QuoteVolume float64 `json:"quoteVolume"`
			NumTrades   int32   `json:"numTrades"`
			IsClosed    bool    `json:"isClosed"`
		}
		if err := json.Unmarshal(line, &entry); err != nil {
			log.Printf("[replay] unmarshal failed: %v", err)
			continue
		}

		if entry.Interval != "1m" || !entry.IsClosed {
			continue
		}

		k := &market.Kline{
			Symbol:      entry.Symbol,
			Interval:    entry.Interval,
			OpenTime:    entry.OpenTime,
			CloseTime:   entry.CloseTime,
			Open:        entry.Open,
			High:        entry.High,
			Low:         entry.Low,
			Close:       entry.Close,
			Volume:      entry.Volume,
			QuoteVolume: entry.QuoteVolume,
			NumTrades:   entry.NumTrades,
			IsClosed:    true,
		}

		// send raw 1m to kafka
		if err := producer.SendMarketData(ctx, k); err != nil {
			log.Printf("[replay] kafka send failed: %v", err)
		}

		// feed aggregator to produce 15m/1h/4h
		agg.OnKline(ctx, k)

		sent++
		if speed > 0 {
			time.Sleep(speed)
		}
	}

	if err := scanner.Err(); err != nil {
		return sent, fmt.Errorf("scan %s: %v", path, err)
	}
	return sent, nil
}
