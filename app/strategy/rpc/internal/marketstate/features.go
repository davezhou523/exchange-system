package marketstate

import "time"

// BuildFeaturesFromSnapshotValues 把上游快照字段转换为 Market State Engine 可直接消费的特征。
func BuildFeaturesFromSnapshotValues(symbol, timeframe string, close, ema21, ema55, atr float64, isDirty, isTradable, isFinal bool, updatedAt time.Time) Features {
	features := Features{
		Symbol:    symbol,
		Timeframe: timeframe,
		Close:     close,
		Ema21:     ema21,
		Ema55:     ema55,
		Atr:       atr,
		Rsi:       0,
		UpdatedAt: updatedAt,
	}
	if features.Timeframe == "" {
		features.Timeframe = "1m"
	}
	if close > 0 && atr > 0 {
		features.AtrPct = atr / close
	}

	switch {
	case symbol == "":
		features.Healthy = false
		features.LastReason = "empty_symbol"
	case updatedAt.IsZero():
		features.Healthy = false
		features.LastReason = "no_snapshot"
	case isDirty:
		features.Healthy = false
		features.LastReason = "dirty_data"
	case !isTradable:
		features.Healthy = false
		features.LastReason = "not_tradable"
	case !isFinal:
		features.Healthy = false
		features.LastReason = "not_final"
	default:
		features.Healthy = true
		features.LastReason = "healthy_data"
	}

	return features
}

// NormalizeFeatures 补齐可从基础价格特征推导出的字段，避免上游未显式提供时结果不可用。
func NormalizeFeatures(in Features) Features {
	if in.Timeframe == "" {
		in.Timeframe = "1m"
	}
	if in.AtrPct <= 0 && in.Close > 0 && in.Atr > 0 {
		in.AtrPct = in.Atr / in.Close
	}
	return in
}
