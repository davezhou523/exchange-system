package kafka

import (
	"context"
	"encoding/json"
	"fmt"

	commonkafka "exchange-system/common/kafka"
	strategypb "exchange-system/common/pb/strategy"

	"github.com/Shopify/sarama"
	"github.com/zeromicro/go-zero/core/logx"
)

type Consumer struct {
	group   sarama.ConsumerGroup
	topic   string
	groupID string
}

type SignalHandler func(signal *strategypb.Signal) error

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

func (c *Consumer) StartConsuming(ctx context.Context, handler SignalHandler) error {
	if c == nil || c.group == nil {
		return fmt.Errorf("consumer group not initialized")
	}
	if handler == nil {
		return fmt.Errorf("handler is nil")
	}

	h := &signalGroupHandler{handler: handler}
	go func() {
		for {
			if err := c.group.Consume(ctx, []string{c.topic}, h); err != nil {
				logx.WithContext(ctx).Errorf("kafka consume group=%s topic=%s error=%v", c.groupID, c.topic, err)
			}
			if ctx.Err() != nil {
				return
			}
		}
	}()
	return nil
}

type signalGroupHandler struct {
	handler SignalHandler
}

func (h *signalGroupHandler) Setup(sarama.ConsumerGroupSession) error   { return nil }
func (h *signalGroupHandler) Cleanup(sarama.ConsumerGroupSession) error { return nil }

func (h *signalGroupHandler) ConsumeClaim(session sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	for msg := range claim.Messages() {
		if msg == nil {
			continue
		}
		var sig strategypb.Signal
		if err := json.Unmarshal(msg.Value, &sig); err != nil {
			session.MarkMessage(msg, "")
			continue
		}
		if err := h.handler(&sig); err != nil {
			logx.WithContext(session.Context()).Errorf("handle signal failed: %v", err)
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
