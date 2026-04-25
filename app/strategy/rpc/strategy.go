package main

import (
	"context"
	"encoding/json"
	"exchange-system/app/strategy/rpc/internal/config"
	"exchange-system/app/strategy/rpc/internal/server"
	"exchange-system/app/strategy/rpc/internal/svc"
	commonkafka "exchange-system/common/kafka"
	marketpb "exchange-system/common/pb/market"
	"exchange-system/common/pb/strategy"
	"flag"
	"fmt"
	"log"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/Shopify/sarama"
	"github.com/zeromicro/go-zero/core/conf"
	"github.com/zeromicro/go-zero/core/service"
	"github.com/zeromicro/go-zero/zrpc"
	"google.golang.org/grpc"
	"google.golang.org/grpc/reflection"
)

var (
	configFile        = flag.String("f", "etc/strategy.demo.yaml", "the config file")
	mockKafka         = flag.Bool("mock-kafka", false, "mock kafka consume test")
	mockCount         = flag.Int("mock-count", 200, "mock kafka consumer receive count (0 means forever)")
	mockTimeout       = flag.Duration("mock-timeout", 20*time.Second, "mock kafka consumer timeout (0 means no-timeout)")
	mockOffset        = flag.String("mock-offset", "oldest", "mock kafka consume offset: oldest|newest")
	replayOnce        = flag.Bool("replay-once", false, "append a timestamp suffix to Kafka.Group for one-time replay from oldest")
	replayGroupSuffix = flag.String("replay-group-suffix", "", "append a custom suffix to Kafka.Group, e.g. replay-20260425-0625")
)

func main() {
	flag.Parse()

	var c config.Config
	conf.MustLoad(*configFile, &c)
	if *replayGroupSuffix != "" {
		c.Kafka.Group = appendKafkaGroupSuffix(resolveKafkaGroupBase(c), *replayGroupSuffix)
	}
	if *replayOnce {
		ts := time.Now().UTC().Format("20060102-150405")
		c.Kafka.Group = appendKafkaGroupSuffix(resolveKafkaGroupBase(c), "replay-"+ts)
	}
	if *replayGroupSuffix != "" || *replayOnce {
		log.Printf("strategy kafka group override: group=%s initial_offset=%s", c.Kafka.Group, c.Kafka.InitialOffset)
	}

	if *mockKafka {
		if err := runMockKafkaTest(c, *mockCount, *mockTimeout, *mockOffset); err != nil {
			log.Fatalf("mock kafka test failed: %v", err)
		}
		fmt.Println("mock kafka test ok")
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
		strategy.RegisterStrategyServiceServer(grpcServer, server.NewStrategyServiceServer(svcCtx))

		if c.Mode == service.DevMode || c.Mode == service.TestMode {
			reflection.Register(grpcServer)
		}
	})
	defer s.Stop()

	fmt.Printf("Starting rpc server at %s...\n", c.ListenOn)
	s.Start()
}

func resolveKafkaGroupBase(c config.Config) string {
	if strings.TrimSpace(c.Kafka.Group) != "" {
		return strings.TrimSpace(c.Kafka.Group)
	}
	if strings.TrimSpace(c.Name) != "" {
		return strings.TrimSpace(c.Name) + "-kline"
	}
	return "strategy-kline"
}

func appendKafkaGroupSuffix(base, suffix string) string {
	base = strings.TrimSpace(base)
	suffix = strings.TrimSpace(suffix)
	if base == "" {
		base = "strategy-kline"
	}
	if suffix == "" {
		return base
	}
	return base + "-" + suffix
}

func runMockKafkaTest(c config.Config, want int, timeout time.Duration, offsetMode string) error {
	if want < 0 {
		return fmt.Errorf("invalid -mock-count=%d, want >= 0", want)
	}
	if timeout < 0 {
		return fmt.Errorf("invalid -mock-timeout=%s, want >= 0", timeout)
	}

	topic := c.Kafka.Topics.Kline
	if topic == "" {
		return fmt.Errorf("kafka topic is empty")
	}

	consumerCfg := commonkafka.NewConsumerGroupConfig()
	consumerCfg.Consumer.MaxProcessingTime = 5 * time.Minute
	consumerCfg.Consumer.Fetch.Max = 10 * 1024 * 1024

	consumeOffset := sarama.OffsetOldest
	switch offsetMode {
	case "", "oldest":
		consumeOffset = sarama.OffsetOldest
	case "newest":
		consumeOffset = sarama.OffsetNewest
	default:
		return fmt.Errorf("invalid -mock-offset=%q, want oldest|newest", offsetMode)
	}

	consumer, err := sarama.NewConsumer(c.Kafka.Addrs, consumerCfg)
	if err != nil {
		return err
	}
	defer func() {
		_ = consumer.Close()
	}()

	partitions, err := consumer.Partitions(topic)
	if err != nil {
		return err
	}
	if len(partitions) == 0 {
		return fmt.Errorf("no partitions for topic %s", topic)
	}

	sigCtx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	ctx := sigCtx
	var cancel context.CancelFunc
	if timeout > 0 {
		ctx, cancel = context.WithTimeout(sigCtx, timeout)
		defer cancel()
	}

	msgCh := make(chan *marketpb.Kline, 1024)
	if offsetMode == "newest" && want > 0 && timeout > 0 {
		log.Printf("[mock] note: offset=newest only consumes messages produced AFTER startup; if no producer is running concurrently you will timeout")
	}
	log.Printf("[mock] consuming kafka kline: brokers=%v topic=%s partitions=%v want=%d timeout=%s offset=%s", c.Kafka.Addrs, topic, partitions, want, timeout, offsetMode)

	client, err := sarama.NewClient(c.Kafka.Addrs, consumerCfg)
	if err == nil {
		for _, p := range partitions {
			oldest, e1 := client.GetOffset(topic, p, sarama.OffsetOldest)
			newest, e2 := client.GetOffset(topic, p, sarama.OffsetNewest)
			if e1 == nil && e2 == nil {
				log.Printf("[mock]   offsets: topic=%s partition=%d oldest=%d newest=%d lag=%d", topic, p, oldest, newest, newest-oldest)
			} else {
				log.Printf("[mock]   offsets: topic=%s partition=%d getOffsetErr oldest=%v newest=%v", topic, p, e1, e2)
			}
		}
		_ = client.Close()
	} else {
		log.Printf("[mock]   offsets: new client failed: %v", err)
	}

	for _, p := range partitions {
		pc, err := consumer.ConsumePartition(topic, p, consumeOffset)
		if err != nil {
			return err
		}

		go func(partition int32, pc sarama.PartitionConsumer) {
			defer func() {
				_ = pc.Close()
			}()
			for {
				select {
				case <-ctx.Done():
					return
				case msg := <-pc.Messages():
					if msg == nil {
						continue
					}
					var k marketpb.Kline
					if err := json.Unmarshal(msg.Value, &k); err != nil {
						continue
					}
					select {
					case msgCh <- &k:
					default:
					}
				case <-pc.Errors():
				}
			}
		}(p, pc)
	}

	received := 0
	for want == 0 || received < want {
		select {
		case <-ctx.Done():
			if sigCtx.Err() != nil {
				log.Printf("[mock] stopping: received=%d", received)
				return nil
			}
			return fmt.Errorf("timeout: received %d/%d", received, want)
		case k := <-msgCh:
			received++
			log.Printf("[mock] recv %d/%d: symbol=%s tf=%s close=%.4f closeTime=%d", received, want, k.Symbol, k.Interval, k.Close, k.CloseTime)
		}
	}

	log.Printf("[mock] consume done: received=%d", received)
	return nil
}
