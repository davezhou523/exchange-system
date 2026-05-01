package marketstate

import (
	"time"

	"exchange-system/common/featureengine"
)

// BuildFeaturesFromSnapshotValues 把上游快照字段转换为 Market State Engine 可直接消费的特征。
func BuildFeaturesFromSnapshotValues(symbol, timeframe string, close, ema21, ema55, atr float64, isDirty, isTradable, isFinal bool, updatedAt time.Time) Features {
	return featureengine.BuildFromSnapshotValues(
		symbol,
		timeframe,
		close,
		ema21,
		ema55,
		atr,
		0,
		0,
		isDirty,
		isTradable,
		isFinal,
		updatedAt,
	)
}

// NormalizeFeatures 补齐可从基础价格特征推导出的字段，避免上游未显式提供时结果不可用。
func NormalizeFeatures(in Features) Features {
	return featureengine.Normalize(in)
}
