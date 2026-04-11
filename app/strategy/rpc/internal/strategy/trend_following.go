package strategy

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"sync"
	"time"

	"exchange-system/app/strategy/rpc/internal/kafka"
	marketpb "exchange-system/common/pb/market"
)

type TrendFollowingStrategy struct {
	symbol         string
	params         map[string]float64
	producer       *kafka.Producer
	historicalData []map[string]interface{}
	signalLogDir   string
	signalLogMu    sync.Mutex
	signalLogFiles map[string]*os.File
}

func NewTrendFollowingStrategy(symbol string, params map[string]float64, producer *kafka.Producer, signalLogDir string) *TrendFollowingStrategy {
	if params == nil {
		params = map[string]float64{}
	}
	return &TrendFollowingStrategy{
		symbol:         symbol,
		params:         params,
		producer:       producer,
		historicalData: make([]map[string]interface{}, 0),
		signalLogDir:   signalLogDir,
		signalLogFiles: make(map[string]*os.File),
	}
}

func (s *TrendFollowingStrategy) OnKline(ctx context.Context, k *marketpb.Kline) error {
	if k == nil || k.Symbol != s.symbol || !k.IsClosed {
		return nil
	}

	// Dirty data has gaps or incomplete periods — still feed to strategy for indicator calculation,
	// but mark it so that signal generation can skip trading
	data := map[string]interface{}{
		"symbol":     k.Symbol,
		"type":       "kline",
		"interval":   k.Interval,
		"isDirty":    k.IsDirty,
		"isTradable": k.IsTradable,
		"data": map[string]interface{}{
			"c": fmt.Sprintf("%.8f", k.Close),
		},
	}
	return s.HandleMarketData(ctx, data)
}

func (s *TrendFollowingStrategy) HandleMarketData(ctx context.Context, data map[string]interface{}) error {
	if data["symbol"] != s.symbol || data["type"] != "kline" {
		return nil
	}

	s.historicalData = append(s.historicalData, data)
	if len(s.historicalData) > 100 {
		s.historicalData = s.historicalData[1:]
	}

	if len(s.historicalData) >= 50 {
		signal, err := s.generateSignal()
		if err != nil {
			return err
		}
		if signal != nil {
			return s.sendSignal(ctx, signal)
		}
	}

	return nil
}

func (s *TrendFollowingStrategy) generateSignal() (map[string]interface{}, error) {
	// Check if latest kline is not tradable (has gaps or incomplete data)
	latestData := s.historicalData[len(s.historicalData)-1]
	if isTradable, ok := latestData["isTradable"].(bool); !ok || !isTradable {
		log.Printf("[策略] %s 跳过不可交易数据，禁止交易：数据不完整（缺口/不完整周期），指标可能被污染",
			s.symbol)
		return nil, nil
	}

	prices := make([]float64, len(s.historicalData))
	for i, data := range s.historicalData {
		klineData, ok := data["data"].(map[string]interface{})
		if !ok {
			continue
		}
		closeStr, ok := klineData["c"].(string)
		if !ok {
			continue
		}
		var closePrice float64
		_, _ = fmt.Sscanf(closeStr, "%f", &closePrice)
		prices[i] = closePrice
	}

	emaFastPeriod := int(s.params["ema_fast"])
	emaSlowPeriod := int(s.params["ema_slow"])
	rsiPeriod := int(s.params["rsi_period"])
	rsiOverbought := s.params["rsi_overbought"]
	rsiOversold := s.params["rsi_oversold"]

	emaFast := s.calculateEMA(prices, emaFastPeriod)
	emaSlow := s.calculateEMA(prices, emaSlowPeriod)
	rsi := s.calculateRSI(prices, rsiPeriod)

	latestPrice := prices[len(prices)-1]
	stopLoss := s.calculateStopLoss(latestPrice, "BUY") // placeholder, will recalculate
	takeProfits := s.calculateTakeProfits(latestPrice, "BUY")

	var action, side string
	var reason string

	if emaFast > emaSlow && rsi < rsiOverbought {
		action = "BUY"
		side = "LONG"
		stopLoss = s.calculateStopLoss(latestPrice, action)
		takeProfits = s.calculateTakeProfits(latestPrice, action)
		reason = fmt.Sprintf(
			"买入信号：快线EMA(%d)=%.2f > 慢线EMA(%d)=%.2f（差值=%.2f），确认上升趋势；"+
				"RSI(%d)=%.2f < 超买线%.2f，未超买，仍有上涨空间。"+
				"当前价格=%.2f，止损=%.2f（风险2%%），止盈=[%.2f, %.2f, %.2f]（1%%/2%%/3%%）",
			emaFastPeriod, emaFast, emaSlowPeriod, emaSlow, emaFast-emaSlow,
			rsiPeriod, rsi, rsiOverbought,
			latestPrice, stopLoss,
			takeProfits[0], takeProfits[1], takeProfits[2],
		)
	} else if emaFast < emaSlow && rsi > rsiOversold {
		action = "SELL"
		side = "SHORT"
		stopLoss = s.calculateStopLoss(latestPrice, action)
		takeProfits = s.calculateTakeProfits(latestPrice, action)
		reason = fmt.Sprintf(
			"卖出信号：快线EMA(%d)=%.2f < 慢线EMA(%d)=%.2f（差值=%.2f），确认下降趋势；"+
				"RSI(%d)=%.2f > 超卖线%.2f，未超卖，仍有下跌空间。"+
				"当前价格=%.2f，止损=%.2f（风险2%%），止盈=[%.2f, %.2f, %.2f]（1%%/2%%/3%%）",
			emaFastPeriod, emaFast, emaSlowPeriod, emaSlow, emaSlow-emaFast,
			rsiPeriod, rsi, rsiOversold,
			latestPrice, stopLoss,
			takeProfits[0], takeProfits[1], takeProfits[2],
		)
	} else {
		// No signal, log the reason for skipping
		log.Printf("[策略] %s 无信号：EMA_fast(%d)=%.2f, EMA_slow(%d)=%.2f, RSI(%d)=%.2f, 超买线=%.2f, 超卖线=%.2f, 当前价=%.2f",
			s.symbol, emaFastPeriod, emaFast, emaSlowPeriod, emaSlow, rsiPeriod, rsi, rsiOverbought, rsiOversold, latestPrice)
		return nil, nil
	}

	signal := map[string]interface{}{
		"strategy_id":  fmt.Sprintf("trend-following-%s", s.symbol),
		"symbol":       s.symbol,
		"action":       action,
		"side":         side,
		"quantity":     0.01,
		"entry_price":  latestPrice,
		"stop_loss":    stopLoss,
		"take_profits": takeProfits,
		"reason":       reason,
		"timestamp":    time.Now().UnixMilli(),
		"indicators": map[string]interface{}{
			"ema_fast":        emaFast,
			"ema_fast_period": emaFastPeriod,
			"ema_slow":        emaSlow,
			"ema_slow_period": emaSlowPeriod,
			"rsi":             rsi,
			"rsi_period":      rsiPeriod,
		},
	}

	return signal, nil
}

func (s *TrendFollowingStrategy) calculateEMA(prices []float64, period int) float64 {
	if len(prices) == 0 {
		return 0
	}
	if period <= 1 || len(prices) < period {
		return prices[len(prices)-1]
	}

	multiplier := 2.0 / float64(period+1)
	ema := prices[len(prices)-period]
	for i := len(prices) - period + 1; i < len(prices); i++ {
		ema = (prices[i]-ema)*multiplier + ema
	}
	return ema
}

func (s *TrendFollowingStrategy) calculateRSI(prices []float64, period int) float64 {
	if len(prices) == 0 {
		return 50.0
	}
	if period <= 0 || len(prices) <= period {
		return 50.0
	}

	gains := 0.0
	losses := 0.0
	for i := len(prices) - period; i < len(prices)-1; i++ {
		diff := prices[i+1] - prices[i]
		if diff > 0 {
			gains += diff
		} else {
			losses -= diff
		}
	}

	avgGain := gains / float64(period)
	avgLoss := losses / float64(period)
	if avgLoss == 0 {
		return 100.0
	}
	rs := avgGain / avgLoss
	return 100.0 - (100.0 / (1.0 + rs))
}

func (s *TrendFollowingStrategy) calculateStopLoss(price float64, action string) float64 {
	riskPercentage := 0.02
	if action == "BUY" {
		return price * (1 - riskPercentage)
	}
	return price * (1 + riskPercentage)
}

func (s *TrendFollowingStrategy) calculateTakeProfits(price float64, action string) []float64 {
	if action == "BUY" {
		return []float64{price * 1.01, price * 1.02, price * 1.03}
	}
	return []float64{price * 0.99, price * 0.98, price * 0.97}
}

func (s *TrendFollowingStrategy) sendSignal(ctx context.Context, signal map[string]interface{}) error {
	action, _ := signal["action"].(string)
	side, _ := signal["side"].(string)
	entryPrice, _ := signal["entry_price"].(float64)
	stopLoss, _ := signal["stop_loss"].(float64)
	quantity, _ := signal["quantity"].(float64)
	reason, _ := signal["reason"].(string)
	indicators, _ := signal["indicators"].(map[string]interface{})

	tpStr := "[]"
	if tp, ok := signal["take_profits"].([]float64); ok {
		tpStr = fmt.Sprintf("[%.2f, %.2f, %.2f]", tp[0], tp[1], tp[2])
	}

	indicatorsStr := ""
	if indicators != nil {
		indicatorsStr = fmt.Sprintf("指标: EMA_fast=%.2f, EMA_slow=%.2f, RSI=%.2f",
			indicators["ema_fast"], indicators["ema_slow"], indicators["rsi"])
	}

	log.Printf("[策略信号] %s %s | 方向=%s | 价格=%.2f | 数量=%.4f | 止损=%.2f | 止盈=%s | %s | %s",
		s.symbol, action, side, entryPrice, quantity, stopLoss, tpStr, indicatorsStr, reason)

	// Write signal to log file
	s.writeSignalLog(signal)

	return s.producer.SendMarketData(ctx, signal)
}

// signalLogEntry defines the structured signal log format.
type signalLogEntry struct {
	Timestamp   string                 `json:"timestamp"`
	StrategyID  string                 `json:"strategyId"`
	Symbol      string                 `json:"symbol"`
	Action      string                 `json:"action"`
	Side        string                 `json:"side"`
	EntryPrice  float64                `json:"entryPrice"`
	Quantity    float64                `json:"quantity"`
	StopLoss    float64                `json:"stopLoss"`
	TakeProfits []float64              `json:"takeProfits"`
	Reason      string                 `json:"reason"`
	Indicators  map[string]interface{} `json:"indicators"`
	IsTradable  bool                   `json:"isTradable"`
}

// writeSignalLog appends a signal as JSON line to a daily log file.
// Format: data/signal/ETHUSDT/2026-04-11.jsonl
func (s *TrendFollowingStrategy) writeSignalLog(signal map[string]interface{}) {
	if s.signalLogDir == "" {
		return
	}

	now := time.Now().UTC()
	dateStr := now.Format("2006-01-02")
	dir := filepath.Join(s.signalLogDir, s.symbol)

	s.signalLogMu.Lock()
	defer s.signalLogMu.Unlock()

	key := s.symbol + "/" + dateStr
	f, ok := s.signalLogFiles[key]
	if !ok {
		if err := os.MkdirAll(dir, 0o755); err != nil {
			log.Printf("[signal-log] failed to create dir %s: %v", dir, err)
			return
		}
		path := filepath.Join(dir, dateStr+".jsonl")
		var err error
		f, err = os.OpenFile(path, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o644)
		if err != nil {
			log.Printf("[signal-log] failed to open %s: %v", path, err)
			return
		}
		s.signalLogFiles[key] = f
	}

	action, _ := signal["action"].(string)
	side, _ := signal["side"].(string)
	entryPrice, _ := signal["entry_price"].(float64)
	quantity, _ := signal["quantity"].(float64)
	stopLoss, _ := signal["stop_loss"].(float64)
	reason, _ := signal["reason"].(string)
	indicators, _ := signal["indicators"].(map[string]interface{})
	strategyID, _ := signal["strategy_id"].(string)

	var takeProfits []float64
	if tp, ok := signal["take_profits"].([]float64); ok {
		takeProfits = tp
	}

	// Check if latest data is tradable
	isTradable := true
	if len(s.historicalData) > 0 {
		if tradable, ok := s.historicalData[len(s.historicalData)-1]["isTradable"].(bool); ok {
			isTradable = tradable
		}
	}

	entry := signalLogEntry{
		Timestamp:   now.Format("2006-01-02T15:04:05.000Z"),
		StrategyID:  strategyID,
		Symbol:      s.symbol,
		Action:      action,
		Side:        side,
		EntryPrice:  entryPrice,
		Quantity:    quantity,
		StopLoss:    stopLoss,
		TakeProfits: takeProfits,
		Reason:      reason,
		Indicators:  indicators,
		IsTradable:  isTradable,
	}

	data, err := json.Marshal(entry)
	if err != nil {
		log.Printf("[signal-log] marshal failed: %v", err)
		return
	}
	if _, err := f.Write(append(data, '\n')); err != nil {
		log.Printf("[signal-log] write failed: %v", err)
	}
}
