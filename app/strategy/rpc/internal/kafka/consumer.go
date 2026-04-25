package kafka

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"strings"
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
type DepthHandler func(depth *market.Depth) error

func NewConsumer(brokers []string, groupID string, topic string, klineLogDir string, initialOffset string) (*Consumer, error) {
	if groupID == "" {
		groupID = fmt.Sprintf("cg-%s", topic)
	}
	config := commonkafka.NewConsumerGroupConfig()
	offset, err := parseInitialOffset(initialOffset)
	if err != nil {
		return nil, err
	}
	config.Consumer.Offsets.Initial = offset

	group, err := sarama.NewConsumerGroup(brokers, groupID, config)
	if err != nil {
		return nil, err
	}
	log.Printf("kafka consumer init group=%s topic=%s initial_offset=%s", groupID, topic, normalizeInitialOffset(initialOffset))

	return &Consumer{
		group:        group,
		topic:        topic,
		groupID:      groupID,
		klineLogDir:  klineLogDir,
		klineLogDate: make(map[string]*os.File),
	}, nil
}

func parseInitialOffset(initialOffset string) (int64, error) {
	switch normalizeInitialOffset(initialOffset) {
	case "oldest":
		return sarama.OffsetOldest, nil
	case "newest":
		return sarama.OffsetNewest, nil
	default:
		return 0, fmt.Errorf("invalid kafka initial offset %q, want oldest|newest", initialOffset)
	}
}

func normalizeInitialOffset(initialOffset string) string {
	switch initialOffset {
	case "", "newest":
		return "newest"
	case "oldest":
		return "oldest"
	default:
		return initialOffset
	}
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

	go c.consumeLoop(ctx, h)
	return nil
}

func (c *Consumer) StartConsumingDepth(ctx context.Context, handler DepthHandler) error {
	if c == nil || c.group == nil {
		return fmt.Errorf("consumer group not initialized")
	}
	if handler == nil {
		return fmt.Errorf("depth handler is nil")
	}

	h := &marketDepthGroupHandler{handler: handler, groupID: c.groupID, topic: c.topic}
	go c.consumeLoop(ctx, h)
	return nil
}

func (c *Consumer) consumeLoop(ctx context.Context, handler sarama.ConsumerGroupHandler) {
	attempt := 0
	for {
		err := c.group.Consume(ctx, []string{c.topic}, handler)
		if err != nil {
			log.Printf("kafka consume loop error group=%s topic=%s err=%v", c.groupID, c.topic, err)
			if commonkafka.ShouldRetryConsumeErr(err) {
				sleep := commonkafka.RetryBackoff(attempt)
				if sleep < 2*time.Second {
					sleep = 2 * time.Second
				}
				log.Printf("kafka consume retry group=%s topic=%s attempt=%d sleep=%v", c.groupID, c.topic, attempt, sleep)
				time.Sleep(sleep)
				if attempt < 8 {
					attempt++
				}
			} else {
				sleep := commonkafka.RetryBackoff(attempt)
				if sleep < 3*time.Second {
					sleep = 3 * time.Second
				}
				log.Printf("kafka consume non-retryable error, backing off group=%s topic=%s attempt=%d sleep=%v", c.groupID, c.topic, attempt, sleep)
				time.Sleep(sleep)
				if attempt < 8 {
					attempt++
				}
			}
			continue
		}
		attempt = 0
		if ctx.Err() != nil {
			return
		}
		time.Sleep(2 * time.Second)
	}
}

type marketKlineGroupHandler struct {
	handler  MarketDataHandler
	groupID  string
	topic    string
	consumer *Consumer
}

type marketDepthGroupHandler struct {
	handler DepthHandler
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

func (h *marketDepthGroupHandler) Setup(s sarama.ConsumerGroupSession) error {
	log.Printf("kafka depth consumer-group setup group=%s topic=%s member=%s generation=%d claims=%v", h.groupID, h.topic, s.MemberID(), s.GenerationID(), s.Claims())
	return nil
}

func (h *marketDepthGroupHandler) Cleanup(s sarama.ConsumerGroupSession) error {
	log.Printf("kafka depth consumer-group cleanup group=%s topic=%s member=%s generation=%d", h.groupID, h.topic, s.MemberID(), s.GenerationID())
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
		eventTime := time.UnixMilli(k.EventTime).Format("15:04:05.000")

		// 聚合K线（非1m）打印指标值，便于验证指标是否正确传递
		indicatorStr := ""
		if k.Interval != "1m" && (k.Ema21 != 0 || k.Ema55 != 0 || k.Rsi != 0) {
			indicatorStr = fmt.Sprintf(" EMA21=%.2f EMA55=%.2f RSI=%.2f ATR=%.2f", k.Ema21, k.Ema55, k.Rsi, k.Atr)
		}
		stateReason := buildKlineStateReason(&k)
		log.Printf("[kafka consume] symbol=%s interval=%s openTime=%s closeTime=%s eventTime=%s isClosed=%v | open=%.2f high=%.2f low=%.2f close=%.2f | volume=%.4f quoteVolume=%.4f takerBuyVolume=%.4f takerBuyQuote=%.4f firstTradeId=%d lastTradeId=%d numTrades=%d | isDirty=%v dirtyReason=%s isTradable=%v isFinal=%v%s | partition=%d offset=%d key=%q",
			k.Symbol, k.Interval, openTime, closeTime, eventTime, k.IsClosed,
			k.Open, k.High, k.Low, k.Close,
			k.Volume, k.QuoteVolume, k.TakerBuyVolume, k.TakerBuyQuote, k.FirstTradeId, k.LastTradeId, k.NumTrades,
			k.IsDirty, stateReason, k.IsTradable, k.IsFinal, indicatorStr,
			msg.Partition, msg.Offset, key)

		// Short intervals may emit a raw closed event first and a finalized indicator-enriched
		// event later, so keep final-only logging there to avoid duplicate rows. For 1h/4h we
		// also persist non-final rows because they are useful for replay/debugging multi-TF flow.
		if shouldPersistKlineLog(&k) {
			h.consumer.writeKlineLog(&k)
		}
		if err := h.handler(&k); err != nil {
			log.Printf("kafka handler failed group=%s topic=%s partition=%d offset=%d symbol=%s interval=%s err=%v", h.groupID, msg.Topic, msg.Partition, msg.Offset, k.Symbol, k.Interval, err)
		}
		session.MarkMessage(msg, "")
	}
	return nil
}

func (h *marketDepthGroupHandler) ConsumeClaim(session sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	for msg := range claim.Messages() {
		if msg == nil {
			continue
		}
		var depth market.Depth
		if err := json.Unmarshal(msg.Value, &depth); err != nil {
			log.Printf("kafka depth unmarshal failed group=%s topic=%s partition=%d offset=%d err=%v", h.groupID, msg.Topic, msg.Partition, msg.Offset, err)
			session.MarkMessage(msg, "")
			continue
		}
		if err := h.handler(&depth); err != nil {
			log.Printf("kafka depth handler failed group=%s topic=%s partition=%d offset=%d symbol=%s err=%v", h.groupID, msg.Topic, msg.Partition, msg.Offset, depth.Symbol, err)
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
	Symbol    string `json:"symbol"`
	Interval  string `json:"interval"`
	OpenTime  string `json:"openTime"`
	CloseTime string `json:"closeTime"`
	EventTime string `json:"eventTime"`
	IsClosed  bool   `json:"isClosed"`

	Open  float64 `json:"open"`
	High  float64 `json:"high"`
	Low   float64 `json:"low"`
	Close float64 `json:"close"`

	Volume         float64 `json:"volume"`
	QuoteVolume    float64 `json:"quoteVolume"`
	TakerBuyVolume float64 `json:"takerBuyVolume"`
	TakerBuyQuote  float64 `json:"takerBuyQuote"`
	FirstTradeId   int64   `json:"firstTradeId"`
	LastTradeId    int64   `json:"lastTradeId"`
	NumTrades      int32   `json:"numTrades"`

	IsDirty     bool   `json:"isDirty"`
	DirtyReason string `json:"dirtyReason"`
	IsTradable  bool   `json:"isTradable"`
	IsFinal     bool   `json:"isFinal"`

	Ema21 float64 `json:"ema21"`
	Ema55 float64 `json:"ema55"`
	Rsi   float64 `json:"rsi"`
	Atr   float64 `json:"atr"`
}

// formatFloat formats a float64 to 2 decimal places.
func formatFloat(f float64) float64 {
	return float64(int64(f*100+0.5)) / 100
}

func shouldPersistKlineLog(k *market.Kline) bool {
	if k == nil || !k.IsClosed {
		return false
	}
	switch k.Interval {
	case "1m", "3m", "5m", "15m":
		return k.IsFinal
	case "1h", "4h":
		return true
	default:
		return false
	}
}

func buildKlineStateReason(k *market.Kline) string {
	if k == nil {
		return "unknown"
	}

	reasons := make([]string, 0, 3)
	if k.IsDirty {
		if strings.TrimSpace(k.GetDirtyReason()) != "" {
			reasons = append(reasons, strings.TrimSpace(k.GetDirtyReason()))
		} else {
			reasons = append(reasons, "gap_or_incomplete")
		}
	}
	if k.Interval != "1m" && (k.Ema21 == 0 || k.Ema55 == 0 || k.Rsi == 0 || k.Atr == 0) {
		reasons = append(reasons, "indicators_not_ready")
	}
	if len(reasons) == 0 {
		if k.IsFinal && k.IsTradable {
			return "final_tradable"
		}
		if !k.IsFinal {
			return "pending_finalization"
		}
		return "closed_only"
	}
	return strings.Join(reasons, "+")
}

// writeKlineLog appends a consumed closed kline as JSON line to a daily log file.
// Format: data/kline/ETHUSDT/15m/2026-04-11.jsonl
// Note: 1h/4h may include non-final rows for replay/debugging, so downstream analysis must
// inspect isFinal/isTradable/isDirty instead of assuming every row is directly tradable.
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
	k.Ema21 = formatFloat(k.Ema21)
	k.Ema55 = formatFloat(k.Ema55)
	k.Rsi = formatFloat(k.Rsi)
	k.Atr = formatFloat(k.Atr)

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

	stateReason := buildKlineStateReason(k)
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
		DirtyReason:    stateReason,
		IsTradable:     k.IsTradable,
		IsFinal:        k.IsFinal,
		Ema21:          k.Ema21,
		Ema55:          k.Ema55,
		Rsi:            k.Rsi,
		Atr:            k.Atr,
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
