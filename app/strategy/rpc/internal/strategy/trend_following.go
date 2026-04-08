package strategy

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"time"

	"exchange-system/app/strategy/rpc/internal/kafka"
	marketpb "exchange-system/common/pb/market"
)

type TrendFollowingStrategy struct {
	symbol         string
	params         map[string]float64
	producer       *kafka.Producer
	historicalData []map[string]interface{}
}

func NewTrendFollowingStrategy(symbol string, params map[string]float64, producer *kafka.Producer) *TrendFollowingStrategy {
	if params == nil {
		params = map[string]float64{}
	}
	return &TrendFollowingStrategy{
		symbol:         symbol,
		params:         params,
		producer:       producer,
		historicalData: make([]map[string]interface{}, 0),
	}
}

func (s *TrendFollowingStrategy) OnKline(ctx context.Context, k *marketpb.Kline) error {
	if k == nil || k.Symbol != s.symbol || !k.IsClosed {
		return nil
	}
	data := map[string]interface{}{
		"symbol":   k.Symbol,
		"type":     "kline",
		"interval": k.Interval,
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

	emaFast := s.calculateEMA(prices, int(s.params["ema_fast"]))
	emaSlow := s.calculateEMA(prices, int(s.params["ema_slow"]))
	rsi := s.calculateRSI(prices, int(s.params["rsi_period"]))

	var action, side, reason string
	if emaFast > emaSlow && rsi < s.params["rsi_overbought"] {
		action = "BUY"
		side = "LONG"
		reason = "Uptrend confirmed with RSI not overbought"
	} else if emaFast < emaSlow && rsi > s.params["rsi_oversold"] {
		action = "SELL"
		side = "SHORT"
		reason = "Downtrend confirmed with RSI not oversold"
	} else {
		return nil, nil
	}

	latestPrice := prices[len(prices)-1]

	signal := map[string]interface{}{
		"strategy_id":  fmt.Sprintf("trend-following-%s", s.symbol),
		"symbol":       s.symbol,
		"action":       action,
		"side":         side,
		"quantity":     0.01,
		"entry_price":  latestPrice,
		"stop_loss":    s.calculateStopLoss(latestPrice, action),
		"take_profits": s.calculateTakeProfits(latestPrice, action),
		"reason":       reason,
		"timestamp":    time.Now().UnixMilli(),
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
	signalJSON, err := json.Marshal(signal)
	if err != nil {
		return err
	}
	log.Printf("Generated signal: %s", string(signalJSON))
	return s.producer.SendMarketData(ctx, signal)
}
