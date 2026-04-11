package kafka

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"sync"
	"time"

	commonkafka "exchange-system/common/kafka"
	"exchange-system/common/pb/market"

	"github.com/Shopify/sarama"
)

type Consumer struct {
	group        sarama.ConsumerGroup
	topic        string
	groupID      string
	klineLogDir  string
	klineLogMu   sync.Mutex
	klineLogDate map[string]*os.File
}

type MarketDataHandler func(kline *market.Kline) error

func NewConsumer(brokers []string, groupID string, topic string, klineLogDir string) (*Consumer, error) {
	if groupID == "" {
		groupID = fmt.Sprintf("cg-%s", topic)
	}
	config := commonkafka.NewConsumerGroupConfig()

	group, err := sarama.NewConsumerGroup(brokers, groupID, config)
	if err != nil {
		return nil, err
	}

	return &Consumer{
		group:        group,
		topic:        topic,
		groupID:      groupID,
		klineLogDir:  klineLogDir,
		klineLogDate: make(map[string]*os.File),
	}, nil
}

func (c *Consumer) StartConsuming(ctx context.Context, handler MarketDataHandler) error {
	if c == nil || c.group == nil {
		return fmt.Errorf("consumer group not initialized")
	}
	if handler == nil {
		return fmt.Errorf("handler is nil")
	}

	h := &marketKlineGroupHandler{handler: handler, groupID: c.groupID, topic: c.topic, consumer: c}

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
	handler  MarketDataHandler
	groupID  string
	topic    string
	consumer *Consumer
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

		// Save aggregated kline (15m/1h/4h) to log file for verification
		if k.IsClosed && (k.Interval == "15m" || k.Interval == "1h" || k.Interval == "4h") {
			h.consumer.writeKlineLog(&k)
		}
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
	c.klineLogMu.Lock()
	for _, f := range c.klineLogDate {
		_ = f.Close()
	}
	c.klineLogDate = nil
	c.klineLogMu.Unlock()
	return c.group.Close()
}

// klineLogEntry mirrors the protobuf field order for deterministic JSON output.
// Must be consistent with market service's klineLogEntry.
type klineLogEntry struct {
	Symbol         string  `json:"symbol"`
	Interval       string  `json:"interval"`
	OpenTime       string  `json:"openTime"`
	Open           float64 `json:"open"`
	High           float64 `json:"high"`
	Low            float64 `json:"low"`
	Close          float64 `json:"close"`
	Volume         float64 `json:"volume"`
	CloseTime      string  `json:"closeTime"`
	IsClosed       bool    `json:"isClosed"`
	EventTime      string  `json:"eventTime"`
	FirstTradeId   int64   `json:"firstTradeId"`
	LastTradeId    int64   `json:"lastTradeId"`
	NumTrades      int32   `json:"numTrades"`
	QuoteVolume    float64 `json:"quoteVolume"`
	TakerBuyVolume float64 `json:"takerBuyVolume"`
	TakerBuyQuote  float64 `json:"takerBuyQuote"`
	IsDirty        bool    `json:"isDirty"`
	IsTradable     bool    `json:"isTradable"`
}

// formatFloat formats a float64 to 2 decimal places.
func formatFloat(f float64) float64 {
	return float64(int64(f*100+0.5)) / 100
}

// writeKlineLog appends a closed aggregated kline (15m/1h/4h) as JSON line to a daily log file.
// Format: data/kline/ETHUSDT/15m/2026-04-11.jsonl
func (c *Consumer) writeKlineLog(k *market.Kline) {
	if c.klineLogDir == "" {
		return
	}

	// Round float fields to 2 decimal places for cleaner logs
	k.Volume = formatFloat(k.Volume)
	k.QuoteVolume = formatFloat(k.QuoteVolume)
	k.TakerBuyVolume = formatFloat(k.TakerBuyVolume)
	k.TakerBuyQuote = formatFloat(k.TakerBuyQuote)
	k.Open = formatFloat(k.Open)
	k.High = formatFloat(k.High)
	k.Low = formatFloat(k.Low)
	k.Close = formatFloat(k.Close)

	dateStr := time.UnixMilli(k.CloseTime).UTC().Format("2006-01-02")
	dir := filepath.Join(c.klineLogDir, k.Symbol, k.Interval)

	c.klineLogMu.Lock()
	defer c.klineLogMu.Unlock()

	if c.klineLogDate == nil {
		return
	}

	key := k.Symbol + "/" + k.Interval + "/" + dateStr
	f, ok := c.klineLogDate[key]
	if !ok {
		if err := os.MkdirAll(dir, 0o755); err != nil {
			log.Printf("[kline-log] failed to create dir %s: %v", dir, err)
			return
		}
		path := filepath.Join(dir, dateStr+".jsonl")
		var err error
		f, err = os.OpenFile(path, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o644)
		if err != nil {
			log.Printf("[kline-log] failed to open %s: %v", path, err)
			return
		}
		c.klineLogDate[key] = f
	}

	entry := klineLogEntry{
		Symbol:         k.Symbol,
		Interval:       k.Interval,
		OpenTime:       time.UnixMilli(k.OpenTime).UTC().Format("2006-01-02T15:04:05.000Z"),
		Open:           k.Open,
		High:           k.High,
		Low:            k.Low,
		Close:          k.Close,
		Volume:         k.Volume,
		CloseTime:      time.UnixMilli(k.CloseTime).UTC().Format("2006-01-02T15:04:05.000Z"),
		IsClosed:       k.IsClosed,
		EventTime:      time.UnixMilli(k.EventTime).UTC().Format("2006-01-02T15:04:05.000Z"),
		FirstTradeId:   k.FirstTradeId,
		LastTradeId:    k.LastTradeId,
		NumTrades:      k.NumTrades,
		QuoteVolume:    k.QuoteVolume,
		TakerBuyVolume: k.TakerBuyVolume,
		TakerBuyQuote:  k.TakerBuyQuote,
		IsDirty:        k.IsDirty,
		IsTradable:     k.IsTradable,
	}
	data, err := json.Marshal(entry)
	if err != nil {
		log.Printf("[kline-log] marshal failed: %v", err)
		return
	}
	if _, err := f.Write(append(data, '\n')); err != nil {
		log.Printf("[kline-log] write failed: %v", err)
	}
}
