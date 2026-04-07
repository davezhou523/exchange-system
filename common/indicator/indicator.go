package indicator

import (
	"errors"
	"math"
)

func EMA(values []float64, period int) ([]float64, error) {
	if period <= 0 {
		return nil, errors.New("ema period must be positive")
	}
	if len(values) < period {
		return nil, errors.New("insufficient data for ema")
	}
	result := make([]float64, len(values))
	for i := range result {
		result[i] = math.NaN()
	}
	sum := 0.0
	for i := 0; i < period; i++ {
		sum += values[i]
	}
	result[period-1] = sum / float64(period)
	multiplier := 2.0 / float64(period+1)
	for i := period; i < len(values); i++ {
		result[i] = (values[i]-result[i-1])*multiplier + result[i-1]
	}
	return result, nil
}

func RSI(values []float64, period int) ([]float64, error) {
	if period <= 0 {
		return nil, errors.New("rsi period must be positive")
	}
	if len(values) <= period {
		return nil, errors.New("insufficient data for rsi")
	}
	result := make([]float64, len(values))
	for i := range result {
		result[i] = math.NaN()
	}
	gains := 0.0
	losses := 0.0
	for i := 1; i <= period; i++ {
		change := values[i] - values[i-1]
		if change > 0 {
			gains += change
		} else {
			losses -= change
		}
	}
	avgGain := gains / float64(period)
	avgLoss := losses / float64(period)
	result[period] = rsiFromAverage(avgGain, avgLoss)
	for i := period + 1; i < len(values); i++ {
		change := values[i] - values[i-1]
		gain := 0.0
		loss := 0.0
		if change > 0 {
			gain = change
		} else {
			loss = -change
		}
		avgGain = ((avgGain * float64(period-1)) + gain) / float64(period)
		avgLoss = ((avgLoss * float64(period-1)) + loss) / float64(period)
		result[i] = rsiFromAverage(avgGain, avgLoss)
	}
	return result, nil
}

func ATR(highs, lows, closes []float64, period int) ([]float64, error) {
	if period <= 0 {
		return nil, errors.New("atr period must be positive")
	}
	if len(highs) != len(lows) || len(lows) != len(closes) {
		return nil, errors.New("atr series length mismatch")
	}
	if len(highs) <= period {
		return nil, errors.New("insufficient data for atr")
	}
	tr := make([]float64, len(highs))
	tr[0] = highs[0] - lows[0]
	for i := 1; i < len(highs); i++ {
		rangeHL := highs[i] - lows[i]
		rangeHC := math.Abs(highs[i] - closes[i-1])
		rangeLC := math.Abs(lows[i] - closes[i-1])
		tr[i] = maxFloat(rangeHL, maxFloat(rangeHC, rangeLC))
	}
	result := make([]float64, len(highs))
	for i := range result {
		result[i] = math.NaN()
	}
	sum := 0.0
	for i := 1; i <= period; i++ {
		sum += tr[i]
	}
	result[period] = sum / float64(period)
	for i := period + 1; i < len(tr); i++ {
		result[i] = ((result[i-1] * float64(period-1)) + tr[i]) / float64(period)
	}
	return result, nil
}

func LastFinite(values []float64) (float64, bool) {
	for i := len(values) - 1; i >= 0; i-- {
		if !math.IsNaN(values[i]) && !math.IsInf(values[i], 0) {
			return values[i], true
		}
	}
	return 0, false
}

func rsiFromAverage(avgGain, avgLoss float64) float64 {
	if avgLoss == 0 {
		return 100
	}
	rs := avgGain / avgLoss
	return 100 - 100/(1+rs)
}

func maxFloat(a, b float64) float64 {
	if a > b {
		return a
	}
	return b
}
