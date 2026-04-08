package kafka

import (
	"context"
	"encoding/json"
	"fmt"
	"log"

	commonkafka "exchange-system/common/kafka"
	"exchange-system/common/pb/market"

	"github.com/Shopify/sarama"
)

type Consumer struct {
	group   sarama.ConsumerGroup
	topic   string
	groupID string
}

type MarketDataHandler func(kline *market.Kline) error

func NewConsumer(brokers []string, groupID string, topic string) (*Consumer, error) {
	if groupID == "" {
		groupID = fmt.Sprintf("cg-%s", topic)
	}
	config := commonkafka.NewConsumerGroupConfig()

	group, err := sarama.NewConsumerGroup(brokers, groupID, config)
	if err != nil {
		return nil, err
	}

	return &Consumer{group: group, topic: topic, groupID: groupID}, nil
}

func (c *Consumer) StartConsuming(ctx context.Context, handler MarketDataHandler) error {
	if c == nil || c.group == nil {
		return fmt.Errorf("consumer group not initialized")
	}
	if handler == nil {
		return fmt.Errorf("handler is nil")
	}

	h := &marketKlineGroupHandler{handler: handler}
	go func() {
		for {
			if err := c.group.Consume(ctx, []string{c.topic}, h); err != nil {
				log.Printf("kafka consume group=%s topic=%s error=%v", c.groupID, c.topic, err)
			}
			if ctx.Err() != nil {
				return
			}
		}
	}()
	return nil
}

type marketKlineGroupHandler struct {
	handler MarketDataHandler
}

func (h *marketKlineGroupHandler) Setup(sarama.ConsumerGroupSession) error   { return nil }
func (h *marketKlineGroupHandler) Cleanup(sarama.ConsumerGroupSession) error { return nil }

func (h *marketKlineGroupHandler) ConsumeClaim(session sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	for msg := range claim.Messages() {
		if msg == nil {
			continue
		}
		var k market.Kline
		if err := json.Unmarshal(msg.Value, &k); err != nil {
			log.Printf("Failed to unmarshal message: %v", err)
			session.MarkMessage(msg, "")
			continue
		}
		if err := h.handler(&k); err != nil {
			log.Printf("Error handling market data: %v", err)
		}
		session.MarkMessage(msg, "")
	}
	return nil
}

func (c *Consumer) Close() error {
	if c == nil || c.group == nil {
		return nil
	}
	return c.group.Close()
}
