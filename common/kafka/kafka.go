package kafka

import (
	"context"
	"errors"
	"log"
	"reflect"
	"strings"
	"time"

	"github.com/Shopify/sarama"
)

var DefaultKafkaVersion = sarama.V2_8_0_0

func NewProducerConfig() *sarama.Config {
	cfg := sarama.NewConfig()
	cfg.Version = DefaultKafkaVersion

	cfg.Producer.Idempotent = true
	cfg.Producer.RequiredAcks = sarama.WaitForAll
	cfg.Producer.Retry.Max = 30
	cfg.Producer.Retry.Backoff = 500 * time.Millisecond
	cfg.Producer.Return.Successes = true
	cfg.Producer.Return.Errors = true
	cfg.Producer.Compression = sarama.CompressionSnappy
	// HashPartitioner: 按消息Key哈希分区，Producer端以symbol作为Key，
	// 确保同一symbol的消息路由到同一partition，保证消费顺序
	cfg.Producer.Partitioner = sarama.NewHashPartitioner
	cfg.Producer.MaxMessageBytes = 10 * 1024 * 1024
	cfg.Producer.Timeout = 30 * time.Second

	cfg.Net.MaxOpenRequests = 1
	cfg.Metadata.Full = true
	cfg.Metadata.RefreshFrequency = 10 * time.Second
	cfg.Metadata.Retry.Max = 10
	cfg.Metadata.Retry.Backoff = 250 * time.Millisecond
	return cfg
}

func ShouldRetryProduceErr(err error) bool {
	if err == nil {
		return false
	}

	if pe, ok := err.(*sarama.ProducerError); ok {
		return ShouldRetryProduceErr(pe.Err)
	}
	if pes, ok := err.(sarama.ProducerErrors); ok {
		for _, e := range pes {
			if e != nil && ShouldRetryProduceErr(e) {
				return true
			}
		}
		return false
	}

	for e := err; e != nil; e = errors.Unwrap(e) {
		if ke, ok := e.(sarama.KError); ok {
			switch ke {
			case sarama.ErrLeaderNotAvailable,
				sarama.ErrNotLeaderForPartition,
				sarama.ErrNotEnoughReplicas,
				sarama.ErrNotEnoughReplicasAfterAppend,
				sarama.ErrRequestTimedOut,
				sarama.ErrNetworkException,
				sarama.ErrKafkaStorageError,
				sarama.ErrOffsetsLoadInProgress:
				return true
			default:
				return false
			}
		}
	}

	msg := strings.ToLower(err.Error())
	if strings.Contains(msg, "still loading offsets") {
		return true
	}
	if strings.Contains(msg, "offsets") && strings.Contains(msg, "load") {
		return true
	}
	if strings.Contains(msg, "metadata") && strings.Contains(msg, "out of date") {
		return true
	}
	if strings.Contains(msg, "not the leader") {
		return true
	}
	if strings.Contains(msg, "leader") && strings.Contains(msg, "change") {
		return true
	}
	return false
}

func RetryBackoff(attempt int) time.Duration {
	if attempt <= 0 {
		return 200 * time.Millisecond
	}
	backoff := 200 * time.Millisecond
	for i := 0; i < attempt; i++ {
		backoff *= 2
		if backoff >= 5*time.Second {
			return 5 * time.Second
		}
	}
	return backoff
}

func NewSyncProducerWithRetry(ctx context.Context, brokers []string, cfg *sarama.Config) (sarama.SyncProducer, error) {
	if cfg == nil {
		cfg = NewProducerConfig()
	}

	var lastErr error
	for attempt := 0; attempt < 10; attempt++ {
		if ctx != nil {
			select {
			case <-ctx.Done():
				return nil, ctx.Err()
			default:
			}
		}

		p, err := sarama.NewSyncProducer(brokers, cfg)
		if err == nil {
			return p, nil
		}
		lastErr = err
		if !ShouldRetryProduceErr(err) {
			return nil, err
		}
		time.Sleep(RetryBackoff(attempt))
	}
	return nil, lastErr
}

func NewConsumerGroupConfig() *sarama.Config {
	cfg := sarama.NewConfig()
	cfg.Version = DefaultKafkaVersion

	cfg.Consumer.Return.Errors = true
	cfg.Consumer.Offsets.Initial = sarama.OffsetNewest
	cfg.Consumer.Offsets.AutoCommit.Enable = true
	cfg.Consumer.Offsets.AutoCommit.Interval = 1 * time.Second
	cfg.Consumer.Offsets.Retry.Max = 10

	cfg.Consumer.Group.Session.Timeout = 45 * time.Second
	cfg.Consumer.Group.Heartbeat.Interval = 5 * time.Second
	cfg.Consumer.Group.Rebalance.Timeout = 90 * time.Second
	cfg.Consumer.Group.Rebalance.Retry.Max = 10
	cfg.Consumer.Group.Rebalance.Retry.Backoff = 2 * time.Second
	cfg.Consumer.Group.Rebalance.GroupStrategies = []sarama.BalanceStrategy{sarama.BalanceStrategyRange}

	cfg.Metadata.Full = true
	cfg.Metadata.RefreshFrequency = 2 * time.Second
	cfg.Metadata.Retry.Max = 10
	cfg.Metadata.Retry.Backoff = 250 * time.Millisecond
	return cfg
}

type symbolGetter interface {
	GetSymbol() string
}

func ShouldRetryConsumeErr(err error) bool {
	if err == nil {
		return false
	}

	for e := err; e != nil; e = errors.Unwrap(e) {
		if ke, ok := e.(sarama.KError); ok {
			switch ke {
			case sarama.ErrLeaderNotAvailable,
				sarama.ErrNotLeaderForPartition,
				sarama.ErrRequestTimedOut,
				sarama.ErrNetworkException,
				sarama.ErrKafkaStorageError,
				sarama.ErrOffsetsLoadInProgress,
				sarama.ErrRebalanceInProgress,
				sarama.ErrIllegalGeneration,
				sarama.ErrUnknownMemberId:
				return true
			default:
				return false
			}
		}
	}

	msg := strings.ToLower(err.Error())
	if strings.Contains(msg, "metadata") && strings.Contains(msg, "out of date") {
		return true
	}
	if strings.Contains(msg, "not the leader") {
		return true
	}
	if strings.Contains(msg, "no leader") {
		return true
	}
	if strings.Contains(msg, "leader") && strings.Contains(msg, "election") {
		return true
	}
	// 网络断连/DNS解析失败/broker断连 均视为临时错误，可重试
	if strings.Contains(msg, "unreachable network") {
		return true
	}
	if strings.Contains(msg, "getaddrinfow") || strings.Contains(msg, "getaddrinfo") {
		return true
	}
	if strings.Contains(msg, "broker not connected") {
		return true
	}
	if strings.Contains(msg, "wsarecv") || strings.Contains(msg, "wsasend") {
		return true
	}
	if strings.Contains(msg, "connection refused") {
		return true
	}
	if strings.Contains(msg, "no such host") {
		return true
	}
	if strings.Contains(msg, "i/o timeout") {
		return true
	}
	if strings.Contains(msg, "broken pipe") {
		return true
	}
	if strings.Contains(msg, "connection reset") {
		return true
	}
	return false
}

// StartConsumerGroupLagReporter periodically prints consumer lag (newest - committed) per partition.
// This is intended as a lightweight health signal in logs. It recreates client/offsetManager each tick
// to be resilient to broker restarts/leader changes.
func StartConsumerGroupLagReporter(ctx context.Context, brokers []string, groupID, topic string, every time.Duration) {
	if groupID == "" || topic == "" || len(brokers) == 0 {
		return
	}
	if every <= 0 {
		every = 30 * time.Second
	}

	go func() {
		ticker := time.NewTicker(every)
		defer ticker.Stop()

		// Run one immediately so operators see it right after startup.
		for {
			printOnce := func() {
				cfg := NewConsumerGroupConfig()
				cfg.Consumer.Return.Errors = false // reporter only

				client, err := sarama.NewClient(brokers, cfg)
				if err != nil {
					log.Printf("[kafka lag] group=%s topic=%s client err=%v", groupID, topic, err)
					return
				}
				defer func() { _ = client.Close() }()

				partitions, err := client.Partitions(topic)
				if err != nil {
					log.Printf("[kafka lag] group=%s topic=%s partitions err=%v", groupID, topic, err)
					return
				}
				if len(partitions) == 0 {
					log.Printf("[kafka lag] group=%s topic=%s partitions empty", groupID, topic)
					return
				}

				om, err := sarama.NewOffsetManagerFromClient(groupID, client)
				if err != nil {
					log.Printf("[kafka lag] group=%s topic=%s offsetManager err=%v", groupID, topic, err)
					return
				}
				defer func() { _ = om.Close() }()

				for _, p := range partitions {
					newest, err := client.GetOffset(topic, p, sarama.OffsetNewest)
					if err != nil {
						log.Printf("[kafka lag] group=%s topic=%s partition=%d newest err=%v", groupID, topic, p, err)
						continue
					}

					pom, err := om.ManagePartition(topic, p)
					if err != nil {
						log.Printf("[kafka lag] group=%s topic=%s partition=%d manage err=%v", groupID, topic, p, err)
						continue
					}
					committed, _ := pom.NextOffset()
					_ = pom.Close()

					if committed < 0 {
						// No committed offset yet.
						log.Printf("[kafka lag] group=%s topic=%s partition=%d committed=%d newest=%d lag=?",
							groupID, topic, p, committed, newest)
						continue
					}
					lag := newest - committed
					if lag < 0 {
						lag = 0
					}
					log.Printf("[kafka lag] group=%s topic=%s partition=%d committed=%d newest=%d lag=%d",
						groupID, topic, p, committed, newest, lag)
				}
			}

			printOnce()

			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
			}
		}
	}()
}

func ExtractSymbol(data interface{}) string {
	if data == nil {
		return ""
	}
	if sg, ok := data.(symbolGetter); ok {
		return sg.GetSymbol()
	}
	if m, ok := data.(map[string]interface{}); ok {
		if v, ok := m["symbol"].(string); ok {
			return v
		}
	}

	rv := reflect.ValueOf(data)
	if rv.Kind() == reflect.Ptr {
		if rv.IsNil() {
			return ""
		}
		rv = rv.Elem()
	}
	if rv.Kind() != reflect.Struct {
		return ""
	}
	f := rv.FieldByName("Symbol")
	if !f.IsValid() || f.Kind() != reflect.String {
		return ""
	}
	return f.String()
}
