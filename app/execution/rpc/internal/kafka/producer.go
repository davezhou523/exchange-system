package kafka

import (
	"context"
	"encoding/json"
	"log"

	"github.com/Shopify/sarama"
)

type Producer struct {
	producer sarama.SyncProducer
	topic    string
}

func NewProducer(brokers []string, topic string) (*Producer, error) {
	config := sarama.NewConfig()
	config.Producer.RequiredAcks = sarama.WaitForAll
	config.Producer.Retry.Max = 5
	config.Producer.Return.Successes = true

	producer, err := sarama.NewSyncProducer(brokers, config)
	if err != nil {
		return nil, err
	}

	return &Producer{producer: producer, topic: topic}, nil
}

func (p *Producer) SendMarketData(ctx context.Context, data interface{}) error {
	jsonData, err := json.Marshal(data)
	if err != nil {
		return err
	}

	msg := &sarama.ProducerMessage{Topic: p.topic, Value: sarama.ByteEncoder(jsonData)}
	partition, offset, err := p.producer.SendMessage(msg)
	if err != nil {
		return err
	}

	log.Printf("Order result sent to topic %s, partition %d at offset %d", p.topic, partition, offset)
	return nil
}

func (p *Producer) Close() error {
	return p.producer.Close()
}
