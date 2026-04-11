package kafka

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"time"

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

	h := &marketKlineGroupHandler{handler: handler, groupID: c.groupID, topic: c.topic}

	go func() {
		lastTransientLog := time.Time{}
		for {
			select {
			case <-ctx.Done():
				return
			case err, ok := <-c.group.Errors():
				if !ok {
					return
				}
				if err == nil {
					continue
				}
				if commonkafka.ShouldRetryConsumeErr(err) {
					if lastTransientLog.IsZero() || time.Since(lastTransientLog) >= 10*time.Second {
						log.Printf("kafka consumer-group transient error group=%s topic=%s err=%v", c.groupID, c.topic, err)
						lastTransientLog = time.Now()
					}
					continue
				}
				log.Printf("kafka consumer-group error group=%s topic=%s err=%v", c.groupID, c.topic, err)
			}
		}
	}()

	go func() {
		attempt := 0
		for {
			err := c.group.Consume(ctx, []string{c.topic}, h)
			if err != nil {
				log.Printf("kafka consume loop error group=%s topic=%s err=%v", c.groupID, c.topic, err)
				if commonkafka.ShouldRetryConsumeErr(err) {
					sleep := commonkafka.RetryBackoff(attempt)
					if sleep < 2*time.Second {
						sleep = 2 * time.Second
					}
					time.Sleep(sleep)
					if attempt < 8 {
						attempt++
					}
				} else {
					time.Sleep(500 * time.Millisecond)
				}
				continue
			}
			attempt = 0
			if ctx.Err() != nil {
				return
			}
			// Prevent rebalance storm: small cooldown after each Consume cycle
			time.Sleep(2 * time.Second)
		}
	}()
	return nil
}

type marketKlineGroupHandler struct {
	handler MarketDataHandler
	groupID string
	topic   string
}

func (h *marketKlineGroupHandler) Setup(s sarama.ConsumerGroupSession) error {
	log.Printf("kafka consumer-group setup group=%s topic=%s member=%s generation=%d claims=%v", h.groupID, h.topic, s.MemberID(), s.GenerationID(), s.Claims())
	return nil
}

func (h *marketKlineGroupHandler) Cleanup(s sarama.ConsumerGroupSession) error {
	log.Printf("kafka consumer-group cleanup group=%s topic=%s member=%s generation=%d", h.groupID, h.topic, s.MemberID(), s.GenerationID())
	return nil
}

func (h *marketKlineGroupHandler) ConsumeClaim(session sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	for msg := range claim.Messages() {
		if msg == nil {
			continue
		}

		key := ""
		if len(msg.Key) > 0 {
			key = string(msg.Key)
		}

		var k market.Kline
		if err := json.Unmarshal(msg.Value, &k); err != nil {
			log.Printf("kafka unmarshal failed group=%s topic=%s partition=%d offset=%d err=%v", h.groupID, msg.Topic, msg.Partition, msg.Offset, err)
			session.MarkMessage(msg, "")
			continue
		}

		openTime := time.UnixMilli(k.OpenTime).Format("15:04:05")
		closeTime := time.UnixMilli(k.CloseTime).Format("15:04:05")
		log.Printf("[kafka consume] %s %s | %s-%s | O=%.2f H=%.2f L=%.2f C=%.2f V=%.4f closed=%v | partition=%d offset=%d key=%q",
			k.Interval, k.Symbol, openTime, closeTime, k.Open, k.High, k.Low, k.Close, k.Volume, k.IsClosed,
			msg.Partition, msg.Offset, key)
		if err := h.handler(&k); err != nil {
			log.Printf("kafka handler failed group=%s topic=%s partition=%d offset=%d symbol=%s interval=%s err=%v", h.groupID, msg.Topic, msg.Partition, msg.Offset, k.Symbol, k.Interval, err)
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
