package strategy

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	_ "math"
	"time"

	"exchange-system/internal/strategy-service/internal/kafka"
)

type TrendFollowingStrategy struct {
	symbol         string
	params         map[string]float64
	producer       *kafka.Producer
	historicalData []map[string]interface{}
}

func NewTrendFollowingStrategy(symbol string, params map[string]float64, producer *kafka.Producer) *TrendFollowingStrategy {
	return &TrendFollowingStrategy{
		symbol:         symbol,
		params:         params,
		producer:       producer,
		historicalData: make([]map[string]interface{}, 0),
	}
}

func (s *TrendFollowingStrategy) HandleMarketData(ctx context.Context, data map[string]interface{}) error {
	// 只处理对应交易对的K线数据
	if data["symbol"] != s.symbol || data["type"] != "kline" {
		return nil
	}

	// 添加历史数据
	s.historicalData = append(s.historicalData, data)

	// 保持历史数据大小
	if len(s.historicalData) > 100 {
		s.historicalData = s.historicalData[1:]
	}

	// 当有足够数据时执行策略
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
	// 提取价格数据
	prices := make([]float64, len(s.historicalData))
	for i, data := range s.historicalData {
		if klineData, ok := data["data"].(map[string]interface{}); ok {
			if closeStr, ok := klineData["c"].(string); ok {
				var closePrice float64
				fmt.Sscanf(closeStr, "%f", &closePrice)
				prices[i] = closePrice
			}
		}
	}

	// 计算技术指标
	emaFast := s.calculateEMA(prices, int(s.params["ema_fast"]))
	emaSlow := s.calculateEMA(prices, int(s.params["ema_slow"]))
	rsi := s.calculateRSI(prices, int(s.params["rsi_period"]))

	// 生成信号
	var action, side, reason string

	// 趋势判断
	if emaFast > emaSlow && rsi < s.params["rsi_overbought"] {
		action = "BUY"
		side = "LONG"
		reason = "Uptrend confirmed with RSI not overbought"
	} else if emaFast < emaSlow && rsi > s.params["rsi_oversold"] {
		action = "SELL"
		side = "SHORT"
		reason = "Downtrend confirmed with RSI not oversold"
	} else {
		// 没有明确信号
		return nil, nil
	}

	// 获取最新价格作为入场价格
	latestPrice := prices[len(prices)-1]

	signal := map[string]interface{}{
		"strategy_id":  fmt.Sprintf("trend-following-%s", s.symbol),
		"symbol":       s.symbol,
		"action":       action,
		"side":         side,
		"quantity":     0.01, // 固定数量，可根据账户资金调整
		"entry_price":  latestPrice,
		"stop_loss":    s.calculateStopLoss(latestPrice, action),
		"take_profits": s.calculateTakeProfits(latestPrice, action),
		"reason":       reason,
		"timestamp":    time.Now().UnixMilli(),
	}

	return signal, nil
}

func (s *TrendFollowingStrategy) calculateEMA(prices []float64, period int) float64 {
	if len(prices) < period {
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
	if len(prices) <= period {
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
	riskPercentage := 0.02 // 2% 风险
	if action == "BUY" {
		return price * (1 - riskPercentage)
	} else {
		return price * (1 + riskPercentage)
	}
}

func (s *TrendFollowingStrategy) calculateTakeProfits(price float64, action string) []float64 {
	if action == "BUY" {
		return []float64{
			price * 1.01, // 1% profit
			price * 1.02, // 2% profit
			price * 1.03, // 3% profit
		}
	} else {
		return []float64{
			price * 0.99, // 1% profit
			price * 0.98, // 2% profit
			price * 0.97, // 3% profit
		}
	}
}

func (s *TrendFollowingStrategy) sendSignal(ctx context.Context, signal map[string]interface{}) error {
	signalJSON, err := json.Marshal(signal)
	if err != nil {
		return err
	}

	log.Printf("Generated signal: %s", string(signalJSON))
	return s.producer.SendMarketData(ctx, signal)
}
