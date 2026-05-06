package strategy

import (
	"strings"
	"testing"
)

// buildTestKlineSnapshots 按给定收盘价构造一组简化K线，便于在单测中复用趋势/动量计算。
func buildTestKlineSnapshots(closes []float64) []klineSnapshot {
	snaps := make([]klineSnapshot, 0, len(closes))
	prevClose := 0.0
	for i, closePrice := range closes {
		openPrice := closePrice
		if i > 0 {
			openPrice = prevClose
		}
		snaps = append(snaps, klineSnapshot{
			OpenTime: int64(i + 1),
			Open:     openPrice,
			High:     closePrice + 1,
			Low:      closePrice - 1,
			Close:    closePrice,
			Volume:   100 + float64(i),
			IsFinal:  true,
			Atr:      2,
		})
		prevClose = closePrice
	}
	return snaps
}

// buildBullishCrossoverKlines 动态生成一组同时满足 MACD 走强与 ADX 趋势性的样本，降低单测对手工参数的脆弱性。
func buildBullishCrossoverKlines(t *testing.T) []klineSnapshot {
	t.Helper()
	base := []float64{
		120, 119.5, 119, 118.5, 118, 117.5, 117, 116.5, 116, 115.5,
		115, 114.5, 114, 113.5, 113, 112.5, 112, 111.5, 111, 110.5,
	}
	for step := 0.8; step <= 2.0; step += 0.1 {
		closes := append([]float64(nil), base...)
		last := base[len(base)-1]
		for i := 0; i < 20; i++ {
			last += step + float64(i)*0.08
			closes = append(closes, last)
		}
		snaps := buildTestKlineSnapshots(closes)
		hist, prev, macdOK := calculateLatestMACDHistogram(snaps, 12, 26, 9)
		if macdOK && hist > 0 && hist > prev {
			return snaps
		}
	}
	t.Fatal("failed to build bullish crossover klines with rising MACD histogram")
	return nil
}

// TestEvaluate1HEntryDecisionReturnsPullbackDeepScale 验证标准回调模式会在深回调时返回 0.9 的仓位缩放。
func TestEvaluate1HEntryDecisionReturnsPullbackDeepScale(t *testing.T) {
	s := NewTrendFollowingStrategy("ETHUSDT", map[string]float64{
		paramDeepPullbackScale: 0.9,
		paramPullbackDeepBand:  0.003,
	}, nil, nil, "", nil)
	s.klines1h = []klineSnapshot{
		{OpenTime: 1, Close: 103.6, Low: 102.8, Ema21: 102.0, Ema55: 100.5, Rsi: 48, Atr: 1.2},
		{OpenTime: 2, Close: 104.0, Low: 100.6, Ema21: 103.0, Ema55: 101.0, Rsi: 49, Atr: 1.2},
		{OpenTime: 3, Close: 101.2, Low: 100.8, Ema21: 103.4, Ema55: 101.0, Rsi: 50, Atr: 1.0},
	}
	s.latest1h = s.klines1h[len(s.klines1h)-1]

	got := s.evaluate1HEntryDecision(trendLong)
	if got.Pullback != pullbackLong {
		t.Fatalf("Pullback = %v, want %v", got.Pullback, pullbackLong)
	}
	if got.Mode != entryModePullback {
		t.Fatalf("Mode = %q, want %q", got.Mode, entryModePullback)
	}
	if got.Scale != 0.9 {
		t.Fatalf("Scale = %.2f, want 0.90", got.Scale)
	}
}

// TestEvaluate1HEntryDecisionReturnsCrossoverLong 验证趋势交叉模式会在 ADX 与 MACD 同时确认时放行做多。
func TestEvaluate1HEntryDecisionReturnsCrossoverLong(t *testing.T) {
	s := NewTrendFollowingStrategy("ETHUSDT", map[string]float64{
		paramCrossoverATRDist:  0.5,
		paramCrossoverPosScale: 0.9,
		paramCrossoverAdxMin:   25,
		paramH4AdxPeriod:       14,
		paramH1MacdFast:        12,
		paramH1MacdSlow:        26,
		paramH1MacdSignal:      9,
	}, nil, nil, "", nil)

	s.klines4h = buildTestKlineSnapshots([]float64{
		100, 101, 102, 103, 104, 105, 106, 107, 108, 109,
		110, 111, 112, 113, 114, 115, 116, 117, 118, 119,
		120, 121, 122, 123, 124, 125, 126, 127, 128, 129,
		130, 131, 132, 133, 134, 135, 136, 137, 138, 139,
	})
	s.latest4h = s.klines4h[len(s.klines4h)-1]

	s.klines1h = buildBullishCrossoverKlines(t)
	last := len(s.klines1h) - 1
	prev := last - 1
	s.klines1h[prev].Ema21 = s.klines1h[prev].Close - 0.4
	s.klines1h[prev].Ema55 = s.klines1h[prev].Close - 0.3
	s.klines1h[prev].Low = s.klines1h[prev].Close - 1.2
	s.klines1h[prev].Rsi = 52
	s.klines1h[prev].Atr = 2.0
	s.klines1h[last].High = s.klines1h[last].Close + 0.6
	s.klines1h[last].Low = s.klines1h[last].Close - 0.8
	s.klines1h[last].Ema21 = s.klines1h[last].Close - 0.2
	s.klines1h[last].Ema55 = s.klines1h[last].Close - 0.5
	s.klines1h[last].Rsi = 55
	s.klines1h[last].Atr = 2.0
	s.latest1h = s.klines1h[last]

	got := s.evaluate1HEntryDecision(trendLong)
	if got.Pullback != pullbackLong {
		t.Fatalf("Pullback = %v, want %v", got.Pullback, pullbackLong)
	}
	if got.Mode != entryModeCrossover {
		t.Fatalf("Mode = %q, want %q", got.Mode, entryModeCrossover)
	}
	if got.Scale != 0.9 {
		t.Fatalf("Scale = %.2f, want 0.90", got.Scale)
	}
	if ready, _ := got.Extras["h4_adx_ready"].(bool); !ready {
		t.Fatalf("h4_adx_ready = %v, want true", got.Extras["h4_adx_ready"])
	}
	if ok, _ := got.Extras["crossover_macd_ok"].(bool); !ok {
		t.Fatalf("crossover_macd_ok = %v, want true", got.Extras["crossover_macd_ok"])
	}
}

// TestEvaluateTrendExitDecisionReturnsPartialCloseAtPlus3R 验证 +3R 命中后会产生平半仓信号，而不是直接全平。
func TestEvaluateTrendExitDecisionReturnsPartialCloseAtPlus3R(t *testing.T) {
	s := NewTrendFollowingStrategy("BTCUSDT", map[string]float64{}, nil, nil, "", nil)
	s.pos = position{
		side:        sideLong,
		entryMode:   entryModeCrossover,
		entryPrice:  100,
		quantity:    2,
		stopLoss:    98.5,
		takeProfit1: 101.5,
		takeProfit2: 104.5,
		atr:         1.0,
	}

	decision := s.evaluateTrendExitDecision(klineSnapshot{
		Close: 104.6,
		Ema21: 103.9,
		Atr:   1.0,
	}, 6, 5, 2, 0.3, "15m")

	if decision.SignalType != "PARTIAL_CLOSE" {
		t.Fatalf("SignalType = %q, want PARTIAL_CLOSE", decision.SignalType)
	}
	if !decision.Partial {
		t.Fatalf("Partial = %v, want true", decision.Partial)
	}
	if decision.Quantity != 1 {
		t.Fatalf("Quantity = %.2f, want 1.00", decision.Quantity)
	}
	if decision.ExitReasonKind != "partial_take_profit" {
		t.Fatalf("ExitReasonKind = %q, want partial_take_profit", decision.ExitReasonKind)
	}
	if !s.pos.hitTP1 {
		t.Fatal("hitTP1 = false, want true after price reaches +1R before +3R")
	}
	if s.pos.stopLoss != s.pos.entryPrice {
		t.Fatalf("stopLoss = %.2f, want %.2f after breakeven move", s.pos.stopLoss, s.pos.entryPrice)
	}
}

// TestEvaluateTrendExitDecisionFormatsEmaBreakReasonReadable 验证 EMA 破位平仓文案里价格与 EMA 指标值之间会保留空格，便于日志直接阅读。
func TestEvaluateTrendExitDecisionFormatsEmaBreakReasonReadable(t *testing.T) {
	s := NewTrendFollowingStrategy("ETHUSDT", map[string]float64{}, nil, nil, "", nil)
	s.pos = position{
		side:          sideLong,
		entryMode:     entryModePullback,
		entryPrice:    2400,
		quantity:      1.16,
		stopLoss:      2369.33,
		takeProfit1:   2380.26,
		takeProfit2:   2402.13,
		atr:           7.3,
		breakBelowCnt: 1,
	}

	decision := s.evaluateTrendExitDecision(klineSnapshot{
		Close: 2382.23,
		Ema21: 2397.40,
		Atr:   10.18,
	}, 6, 2, 2, 0.3, "15m")

	if decision.SignalType != "CLOSE" {
		t.Fatalf("SignalType = %q, want CLOSE", decision.SignalType)
	}
	if !strings.Contains(decision.Reason, "价格 2382.23 < EMA21 2397.40") {
		t.Fatalf("Reason = %q, want readable EMA spacing", decision.Reason)
	}
}
