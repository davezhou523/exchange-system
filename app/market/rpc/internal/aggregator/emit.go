package aggregator

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"time"

	"exchange-system/common/pb/market"
)

// --- 数据输出：Kafka 异步发送 + jsonl 日志写入 ---

// klineLogEntry mirrors the protobuf field order for deterministic JSON output.
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
	IsFinal        bool    `json:"isFinal"`
	Ema21          float64 `json:"ema21"`
	Ema55          float64 `json:"ema55"`
	Rsi            float64 `json:"rsi"`
	Atr            float64 `json:"atr"`
}

type bucket struct {
	Open           float64
	High           float64
	Low            float64
	Close          float64
	Volume         float64
	QuoteVolume    float64
	TakerBuyVolume float64
	TakerBuyQuote  float64
	NumTrades      int32
	FirstTradeID   int64
	LastTradeID    int64
	OpenTime       int64
	CloseTime      int64
	prevOpenTime   int64 // openTime of the last 1m kline added (for continuity check)
	count          int   // number of 1m klines aggregated so far
	dirty          bool  // true if bucket has gaps or incomplete data
	initialized    bool

	// Technical indicators (calculated on emit)
	Ema21 float64
	Ema55 float64
	Rsi   float64
	Atr   float64
}

// asyncKafkaSender 异步 Kafka 发送协程。
// 从 asyncSendQueue 取数据并发送 Kafka，将网络 IO 与数据处理解耦。
// 优势：
// 1. worker goroutine 永不被 Kafka 阻塞（入队 O(1)）
// 2. Kafka 临时卡顿不会导致数据处理停滞
// 3. 队列提供缓冲，平滑发送峰值
//
// 降级策略：
// - 队列满时，emitKline 直接丢弃数据并记日志（背压保护）
// - 发送失败时，producer 内部已有重试机制（8次指数退避）
// - Stop() 时会等待队列中剩余数据发完（优雅退出）
func (a *KlineAggregator) asyncKafkaSender() {
	defer a.asyncSenderWg.Done()

	for {
		select {
		case k, ok := <-a.asyncSendQueue:
			if !ok {
				return // channel closed, 退出
			}
			if err := a.producer.SendMarketData(context.Background(), k); err != nil {
				a.metrics.KafkaSendErrors.Add(1)
				log.Printf("[aggregator] async send failed: %s %s | err=%v", k.Interval, k.Symbol, err)
			}
			// Track metrics
			switch k.Interval {
			case "1m":
				a.metrics.Emitted1m.Add(1)
			case "3m":
				a.metrics.Emitted3m.Add(1)
			case "15m":
				a.metrics.Emitted15m.Add(1)
			case "1h":
				a.metrics.Emitted1h.Add(1)
			case "4h":
				a.metrics.Emitted4h.Add(1)
			}
		case <-a.ctx.Done():
			// 上下文取消，drain 队列中剩余数据后再退出
			for {
				select {
				case k, ok := <-a.asyncSendQueue:
					if !ok {
						return
					}
					if err := a.producer.SendMarketData(context.Background(), k); err != nil {
						a.metrics.KafkaSendErrors.Add(1)
						log.Printf("[aggregator] async send failed (drain): %s %s | err=%v", k.Interval, k.Symbol, err)
					}
					switch k.Interval {
					case "1m":
						a.metrics.Emitted1m.Add(1)
					case "3m":
						a.metrics.Emitted3m.Add(1)
					case "15m":
						a.metrics.Emitted15m.Add(1)
					case "1h":
						a.metrics.Emitted1h.Add(1)
					case "4h":
						a.metrics.Emitted4h.Add(1)
					}
				default:
					return
				}
			}
		}
	}
}

func (a *KlineAggregator) emitKline(ctx context.Context, symbol, interval string, b *bucket) {
	// IsFinal 判断：数据已通过 watermark 确认，指标不会再变化，策略层可安全下单
	// - EmitWatermark 模式：经过 watermark 确认后才发射，!dirty 即为最终数据
	// - EmitImmediate 模式：bucket 完成即发，可能因迟到数据导致 dirty 重发
	//   !dirty 表示数据完整无缺口，但仍可能被迟到数据修正
	//   策略层应根据 IsFinal 决定是否下单，而非仅看 IsTradable
	isFinal := !b.dirty

	k := &market.Kline{
		Symbol:         symbol,
		Interval:       interval,
		OpenTime:       b.OpenTime,
		CloseTime:      b.CloseTime,
		Open:           b.Open,
		High:           b.High,
		Low:            b.Low,
		Close:          b.Close,
		Volume:         b.Volume,
		QuoteVolume:    b.QuoteVolume,
		TakerBuyVolume: b.TakerBuyVolume,
		TakerBuyQuote:  b.TakerBuyQuote,
		NumTrades:      b.NumTrades,
		FirstTradeId:   b.FirstTradeID,
		LastTradeId:    b.LastTradeID,
		IsClosed:       true,
		IsDirty:        b.dirty,
		IsTradable:     !b.dirty,
		IsFinal:        isFinal,
		EventTime:      a.timeSource.Now().UnixMilli(),
		Ema21:          b.Ema21,
		Ema55:          b.Ema55,
		Rsi:            b.Rsi,
		Atr:            b.Atr,
	}

	indicatorStr := ""
	if k.Ema21 != 0 || k.Ema55 != 0 || k.Rsi != 0 {
		indicatorStr = fmt.Sprintf(" EMA21=%.2f EMA55=%.2f RSI=%.2f ATR=%.2f", k.Ema21, k.Ema55, k.Rsi, k.Atr)
	}

	openStr := time.UnixMilli(k.OpenTime).UTC().Format("2006-01-02 15:04:05")
	closeStr := time.UnixMilli(k.CloseTime).UTC().Format("15:04:05")
	log.Printf("[aggregated %s] %s | %s-%s | O=%.2f H=%.2f L=%.2f C=%.2f V=%.4f QV=%.4f trades=%d%s",
		k.Interval, k.Symbol, openStr, closeStr, k.Open, k.High, k.Low, k.Close, k.Volume, k.QuoteVolume, k.NumTrades, indicatorStr)

	// Persist to jsonl file for verification
	a.writeKlineLog(k)

	// 异步发送到 Kafka（通过缓冲队列解耦，避免 Kafka 阻塞 worker goroutine）
	// 队列满时丢弃数据并记日志（背压保护），保证数据处理不中断
	select {
	case a.asyncSendQueue <- k:
	default:
		a.metrics.KafkaSendErrors.Add(1)
		log.Printf("[aggregator] ASYNC QUEUE FULL: dropping %s %s | openTime=%d | queue_size=%d",
			k.Interval, k.Symbol, k.OpenTime, len(a.asyncSendQueue))
	}
}

// formatFloat formats a float64 to string with 2 decimal places.
func formatFloat(f float64) float64 {
	return float64(int64(f*100+0.5)) / 100
}

// writeKlineLog appends an aggregated kline as JSON line to a daily log file.
// Format: data/kline/ETHUSDT/3m/2026-04-11.jsonl
func (a *KlineAggregator) writeKlineLog(k *market.Kline) {
	if a.klineLogDir == "" {
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
	dir := filepath.Join(a.klineLogDir, k.Symbol, k.Interval)

	a.logMu.Lock()
	defer a.logMu.Unlock()

	key := k.Symbol + "/" + k.Interval + "/" + dateStr
	f, ok := a.logFile[key]
	if !ok {
		if err := os.MkdirAll(dir, 0o755); err != nil {
			log.Printf("[agg-log] failed to create dir %s: %v", dir, err)
			return
		}
		path := filepath.Join(dir, dateStr+".jsonl")
		var err error
		f, err = os.OpenFile(path, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o644)
		if err != nil {
			log.Printf("[agg-log] failed to open %s: %v", path, err)
			return
		}
		a.logFile[key] = f
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
		IsFinal:        k.IsFinal,
		Ema21:          k.Ema21,
		Ema55:          k.Ema55,
		Rsi:            k.Rsi,
		Atr:            k.Atr,
	}
	data, err := json.Marshal(entry)
	if err != nil {
		log.Printf("[agg-log] marshal failed: %v", err)
		return
	}
	if _, err := f.Write(append(data, '\n')); err != nil {
		log.Printf("[agg-log] write failed: %v", err)
	}
}
