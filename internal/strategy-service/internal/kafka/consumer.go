package kafka

import (
	"context"
	"encoding/json"
	"log"

	"github.com/Shopify/sarama"
)

type Consumer struct {
	consumer sarama.Consumer
	topic    string
}

type MarketDataHandler func(data map[string]interface{}) error

func NewConsumer(brokers []string, topic string) (*Consumer, error) {
	config := sarama.NewConfig()
	config.Consumer.Return.Errors = true

	consumer, err := sarama.NewConsumer(brokers, config)
	if err != nil {
		return nil, err
	}

	return &Consumer{
		consumer: consumer,
		topic:    topic,
	}, nil
}

func (c *Consumer) StartConsuming(ctx context.Context, handler MarketDataHandler) error {
	partitionConsumer, err := c.consumer.ConsumePartition(c.topic, 0, sarama.OffsetNewest)
	if err != nil {
		return err
	}

	go func() {
		for {
			select {
			case <-ctx.Done():
				partitionConsumer.Close()
				return
			case msg := <-partitionConsumer.Messages():
				var data map[string]interface{}
				if err := json.Unmarshal(msg.Value, &data); err != nil {
					log.Printf("Failed to unmarshal message: %v", err)
					continue
				}

				if err := handler(data); err != nil {
					log.Printf("Error handling market data: %v", err)
				}
			case err := <-partitionConsumer.Errors():
				log.Printf("Kafka consumer error: %v", err)
			}
		}
	}()

	return nil
}

func (c *Consumer) Close() error {
	return c.consumer.Close()
}
