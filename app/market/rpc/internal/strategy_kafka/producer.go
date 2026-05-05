package strategy_kafka

import (
	"context"
	"encoding/json"
	"log"
	"time"

	commonkafka "exchange-system/common/kafka"

	"github.com/Shopify/sarama"
)

type Producer struct {
	producer sarama.SyncProducer
	topic    string
}

func NewProducer(brokers []string, topic string) (*Producer, error) {
	return NewProducerWithContext(context.Background(), brokers, topic)
}

func NewProducerWithContext(ctx context.Context, brokers []string, topic string) (*Producer, error) {
	config := commonkafka.NewProducerConfig()

	producer, err := commonkafka.NewSyncProducerWithRetry(ctx, brokers, config)
	if err != nil {
		return nil, err
	}

	return &Producer{
		producer: producer,
		topic:    topic,
	}, nil
}

func (p *Producer) SendMarketData(ctx context.Context, data interface{}) error {
	jsonData, err := json.Marshal(data)
	if err != nil {
		return err
	}

	msg := &sarama.ProducerMessage{
		Topic: p.topic,
		Value: sarama.ByteEncoder(jsonData),
	}
	if sym := commonkafka.ExtractSymbol(data); sym != "" {
		msg.Key = sarama.StringEncoder(sym)
	}

	var lastErr error
	for attempt := 0; attempt < 8; attempt++ {
		if ctx != nil {
			select {
			case <-ctx.Done():
				return ctx.Err()
			default:
			}
		}

		partition, offset, err := p.producer.SendMessage(msg)
		if err == nil {
			log.Printf("Signal sent to topic=%s partition=%d offset=%d", p.topic, partition, offset)
			return nil
		}
		lastErr = err
		if !commonkafka.ShouldRetryProduceErr(err) {
			return err
		}
		time.Sleep(commonkafka.RetryBackoff(attempt))
	}
	return lastErr
}

func (p *Producer) Close() error {
	return p.producer.Close()
}
