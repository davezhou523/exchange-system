package universepool

import (
	"math"
	"time"

	"exchange-system/app/market/rpc/internal/marketstate"
	"exchange-system/common/pb/market"
)

const defaultRangeGateADXPeriod = 14

// h4GateKline 保存 universepool 评估 4H 震荡门禁所需的最小 K 线字段。
type h4GateKline struct {
	High       float64
	Low        float64
	Close      float64
	Ema21      float64
	Ema55      float64
	Atr        float64
	UpdatedAt  time.Time
	IsDirty    bool
	IsTradable bool
	IsFinal    bool
}

// buildH4GateKline 把 4H K 线裁剪成门禁计算所需的轻量结构，避免 manager 长期持有整条 protobuf。
func buildH4GateKline(k *market.Kline) h4GateKline {
	if k == nil {
		return h4GateKline{}
	}
	return h4GateKline{
		High:       k.High,
		Low:        k.Low,
		Close:      k.Close,
		Ema21:      k.Ema21,
		Ema55:      k.Ema55,
		Atr:        k.Atr,
		UpdatedAt:  time.UnixMilli(k.EventTime).UTC(),
		IsDirty:    k.IsDirty,
		IsTradable: k.IsTradable,
		IsFinal:    k.IsFinal,
	}
}

// appendH4GateHistory 把最新 4H K 线接入历史窗口，并裁剪到计算 ADX 所需的最大长度。
func appendH4GateHistory(history []h4GateKline, item h4GateKline) []h4GateKline {
	if item.UpdatedAt.IsZero() {
		return history
	}
	if n := len(history); n > 0 && history[n-1].UpdatedAt.Equal(item.UpdatedAt) {
		history[n-1] = item
		return history
	}
	history = append(history, item)
	maxLen := defaultRangeGateADXPeriod*3 + 1
	if len(history) > maxLen {
		history = append([]h4GateKline(nil), history[len(history)-maxLen:]...)
	}
	return history
}

// evaluateRangeGateFromHistory 基于当前 4H 历史窗口计算动态币池的 4H 震荡门禁。
func evaluateRangeGateFromHistory(now time.Time, history []h4GateKline, cfg Config) marketstate.RangeGate {
	if len(history) == 0 {
		return marketstate.RangeGate{Reason: "range_gate_h4_missing"}
	}
	current := history[len(history)-1]
	previous := h4GateKline{}
	if len(history) >= 2 {
		previous = history[len(history)-2]
	}
	features := marketstate.Features{
		Timeframe:  "4h",
		Close:      current.Close,
		Ema21:      current.Ema21,
		Ema55:      current.Ema55,
		Atr:        current.Atr,
		UpdatedAt:  current.UpdatedAt,
		Healthy:    !current.IsDirty,
		IsTradable: current.IsTradable,
		IsFinal:    current.IsFinal,
	}
	if adx, ok := calculateH4ADX(history, defaultRangeGateADXPeriod); ok {
		features.Adx = adx
	}
	prevFeatures := marketstate.Features{
		Timeframe:  "4h",
		Atr:        previous.Atr,
		UpdatedAt:  previous.UpdatedAt,
		Healthy:    !previous.IsDirty,
		IsTradable: previous.IsTradable,
		IsFinal:    previous.IsFinal,
	}
	return marketstate.EvaluateRangeGate(now, features, prevFeatures, rangeGateMarketStateConfig(cfg))
}

// rangeGateMarketStateConfig 把 universepool 配置转换成统一的 marketstate 门禁配置，确保两条链路阈值一致。
func rangeGateMarketStateConfig(cfg Config) marketstate.Config {
	return marketstate.Config{
		FreshnessWindow:        4*time.Hour + cfg.EvaluateInterval,
		RangeGateH4AdxMax:      cfg.RangeGateH4AdxMax,
		RangeGateH4EmaCloseMax: cfg.RangeGateH4EmaCloseMax,
		RangeGateH4ScoreMin:    cfg.RangeGateH4ScoreMin,
	}
}

// calculateH4ADX 基于 4H 历史窗口计算最新 ADX，用于给 universepool 的 4H range gate 复用同一套趋势强弱约束。
func calculateH4ADX(history []h4GateKline, period int) (float64, bool) {
	if period <= 0 || len(history) < 2*period {
		return 0, false
	}
	tr := make([]float64, len(history))
	plusDM := make([]float64, len(history))
	minusDM := make([]float64, len(history))
	for i := 1; i < len(history); i++ {
		cur := history[i]
		prev := history[i-1]
		highDiff := cur.High - prev.High
		lowDiff := prev.Low - cur.Low
		if highDiff > lowDiff && highDiff > 0 {
			plusDM[i] = highDiff
		}
		if lowDiff > highDiff && lowDiff > 0 {
			minusDM[i] = lowDiff
		}
		tr1 := cur.High - cur.Low
		tr2 := math.Abs(cur.High - prev.Close)
		tr3 := math.Abs(cur.Low - prev.Close)
		tr[i] = maxFloat(tr1, tr2, tr3)
	}
	smoothedTR := sumFloat(tr[1 : period+1])
	smoothedPlus := sumFloat(plusDM[1 : period+1])
	smoothedMinus := sumFloat(minusDM[1 : period+1])
	if smoothedTR <= 0 {
		return 0, false
	}
	dxValues := make([]float64, 0, len(history)-period)
	for i := period + 1; i < len(history); i++ {
		smoothedTR = smoothedTR - smoothedTR/float64(period) + tr[i]
		smoothedPlus = smoothedPlus - smoothedPlus/float64(period) + plusDM[i]
		smoothedMinus = smoothedMinus - smoothedMinus/float64(period) + minusDM[i]
		if smoothedTR <= 0 {
			continue
		}
		plusDI := 100 * smoothedPlus / smoothedTR
		minusDI := 100 * smoothedMinus / smoothedTR
		denom := plusDI + minusDI
		if denom <= 0 {
			continue
		}
		dxValues = append(dxValues, 100*math.Abs(plusDI-minusDI)/denom)
	}
	if len(dxValues) < period {
		return 0, false
	}
	adx := averageFloat(dxValues[:period])
	for i := period; i < len(dxValues); i++ {
		adx = ((adx * float64(period-1)) + dxValues[i]) / float64(period)
	}
	return adx, true
}

// sumFloat 计算浮点切片求和，供 ADX 平滑过程复用。
func sumFloat(values []float64) float64 {
	sum := 0.0
	for _, value := range values {
		sum += value
	}
	return sum
}

// averageFloat 计算浮点切片平均值，避免在 ADX 初始化阶段重复写样板代码。
func averageFloat(values []float64) float64 {
	if len(values) == 0 {
		return 0
	}
	return sumFloat(values) / float64(len(values))
}

// maxFloat 返回若干浮点数中的最大值，供 TR 计算复用。
func maxFloat(values ...float64) float64 {
	if len(values) == 0 {
		return 0
	}
	maxValue := values[0]
	for _, value := range values[1:] {
		if value > maxValue {
			maxValue = value
		}
	}
	return maxValue
}
