package kafka

import (
	"errors"
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

func NewConsumerGroupConfig() *sarama.Config {
	cfg := sarama.NewConfig()
	cfg.Version = DefaultKafkaVersion

	cfg.Consumer.Return.Errors = true
	cfg.Consumer.Offsets.Initial = sarama.OffsetNewest
	cfg.Consumer.Offsets.AutoCommit.Enable = true
	cfg.Consumer.Offsets.AutoCommit.Interval = 1 * time.Second
	cfg.Consumer.Group.Rebalance.GroupStrategies = []sarama.BalanceStrategy{sarama.BalanceStrategyRange}

	cfg.Metadata.Retry.Max = 10
	cfg.Metadata.Retry.Backoff = 250 * time.Millisecond
	return cfg
}

type symbolGetter interface {
	GetSymbol() string
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
