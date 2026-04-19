package kafka

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"strings"
	"time"

	commonkafka "exchange-system/common/kafka"

	"github.com/Shopify/sarama"
)

type OrderEvent struct {
	OrderID         string    `json:"order_id"`
	ClientID        string    `json:"client_id"`
	Symbol          string    `json:"symbol"`
	Status          string    `json:"status"`
	SignalType      string    `json:"signal_type"`
	Side            string    `json:"side"`
	PositionSide    string    `json:"position_side"`
	Quantity        float64   `json:"quantity"`
	AvgPrice        float64   `json:"avg_price"`
	Commission      float64   `json:"commission"`
	CommissionAsset string    `json:"commission_asset"`
	Slippage        float64   `json:"slippage"`
	Timestamp       int64     `json:"timestamp"`
	StrategyID      string    `json:"strategy_id"`
	StopLoss        float64   `json:"stop_loss"`
	TakeProfits     []float64 `json:"take_profits"`
	Reason          string    `json:"reason"`
}

type OrderEventHandler func(event *OrderEvent) error

type Consumer struct {
	group   sarama.ConsumerGroup
	topic   string
	groupID string
}

func NewConsumer(brokers []string, groupID string, topic string) (*Consumer, error) {
	if groupID == "" {
		groupID = fmt.Sprintf("cg-%s", topic)
	}
	cfg := commonkafka.NewConsumerGroupConfig()
	group, err := sarama.NewConsumerGroup(brokers, groupID, cfg)
	if err != nil {
		return nil, err
	}
	return &Consumer{group: group, topic: topic, groupID: groupID}, nil
}

func (c *Consumer) StartConsuming(ctx context.Context, handler OrderEventHandler) error {
	if c == nil || c.group == nil {
		return fmt.Errorf("consumer group not initialized")
	}
	if handler == nil {
		return fmt.Errorf("handler is nil")
	}

	h := &orderGroupHandler{groupID: c.groupID, topic: c.topic, handler: handler}
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
						log.Printf("[order kafka] transient error group=%s topic=%s err=%v", c.groupID, c.topic, err)
						lastTransientLog = time.Now()
					}
					continue
				}
				log.Printf("[order kafka] consumer group error group=%s topic=%s err=%v", c.groupID, c.topic, err)
			}
		}
	}()

	go func() {
		attempt := 0
		for {
			err := c.group.Consume(ctx, []string{c.topic}, h)
			if err != nil {
				log.Printf("[order kafka] consume loop error group=%s topic=%s err=%v", c.groupID, c.topic, err)
				sleep := commonkafka.RetryBackoff(attempt)
				if sleep < 2*time.Second {
					sleep = 2 * time.Second
				}
				time.Sleep(sleep)
				if attempt < 8 {
					attempt++
				}
				continue
			}
			attempt = 0
			if ctx.Err() != nil {
				return
			}
			time.Sleep(2 * time.Second)
		}
	}()
	return nil
}

func (c *Consumer) Close() error {
	if c == nil || c.group == nil {
		return nil
	}
	return c.group.Close()
}

type orderGroupHandler struct {
	groupID string
	topic   string
	handler OrderEventHandler
}

func (h *orderGroupHandler) Setup(s sarama.ConsumerGroupSession) error {
	log.Printf("[order kafka] setup group=%s topic=%s member=%s generation=%d claims=%v", h.groupID, h.topic, s.MemberID(), s.GenerationID(), s.Claims())
	return nil
}

func (h *orderGroupHandler) Cleanup(s sarama.ConsumerGroupSession) error {
	log.Printf("[order kafka] cleanup group=%s topic=%s member=%s generation=%d", h.groupID, h.topic, s.MemberID(), s.GenerationID())
	return nil
}

func (h *orderGroupHandler) ConsumeClaim(session sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	for msg := range claim.Messages() {
		if msg == nil {
			continue
		}

		var event OrderEvent
		if err := json.Unmarshal(msg.Value, &event); err != nil {
			log.Printf("[order kafka] unmarshal failed group=%s topic=%s partition=%d offset=%d err=%v", h.groupID, msg.Topic, msg.Partition, msg.Offset, err)
			session.MarkMessage(msg, "")
			continue
		}
		if err := h.handler(&event); err != nil {
			log.Printf("[order kafka] handle failed group=%s topic=%s symbol=%s order_id=%s err=%v", h.groupID, msg.Topic, event.Symbol, event.OrderID, err)
		}
		session.MarkMessage(msg, "")
	}
	return nil
}

func (e *OrderEvent) HasExecution() bool {
	if e == nil {
		return false
	}
	status := strings.ToUpper(strings.TrimSpace(e.Status))
	switch status {
	case "FILLED", "PARTIALLY_FILLED":
		return true
	}
	return e.Quantity > 0 || e.AvgPrice > 0
}
