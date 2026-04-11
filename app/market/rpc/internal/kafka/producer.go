package kafka

import (
	"context"
	"encoding/json"
	"log"
	"time"

	commonkafka "exchange-system/common/kafka"
	marketpb "exchange-system/common/pb/market"

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
			if k, ok := data.(*marketpb.Kline); ok && k != nil && k.Interval == "15m" {
				openStr := time.UnixMilli(k.OpenTime).Format("2006-01-02 15:04:05")
				closeStr := time.UnixMilli(k.CloseTime).Format("15:04:05")
				log.Printf("[kafka 15m] topic=%s partition=%d offset=%d %s | %s-%s | O=%.2f H=%.2f L=%.2f C=%.2f V=%.4f",
					p.topic, partition, offset, k.Symbol, openStr, closeStr, k.Open, k.High, k.Low, k.Close, k.Volume)
				return nil
			}
			log.Printf("Market data sent to topic=%s partition=%d offset=%d data=%s", p.topic, partition, offset, string(jsonData))
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
