package marketstate

import "time"

// Detector 定义市场状态识别器接口，便于后续替换成更复杂的实现。
type Detector interface {
	// Detect 根据输入特征输出当前市场状态。
	Detect(now time.Time, features Features) Result
}

// DefaultDetector 是 Phase 4 第一版的最小状态识别器实现。
type DefaultDetector struct {
	cfg Config
}

// NewDetector 创建一个带保守默认阈值的状态识别器。
func NewDetector(cfg Config) *DefaultDetector {
	if cfg.FreshnessWindow <= 0 {
		cfg.FreshnessWindow = 3 * time.Minute
	}
	if cfg.RangeAtrPctMax <= 0 {
		cfg.RangeAtrPctMax = 0.0015
	}
	if cfg.BreakoutAtrPctMin <= 0 {
		cfg.BreakoutAtrPctMin = 0.006
	}
	return &DefaultDetector{cfg: cfg}
}

// Detect 根据最小趋势和波动规则识别当前市场状态。
func (d *DefaultDetector) Detect(now time.Time, features Features) Result {
	features = NormalizeFeatures(features)
	result := Result{
		Symbol:    features.Symbol,
		State:     MarketStateUnknown,
		Reason:    "insufficient_features",
		UpdatedAt: now.UTC(),
	}
	if d == nil {
		return result
	}
	if !features.Healthy {
		if features.LastReason != "" {
			result.Reason = features.LastReason
		} else {
			result.Reason = "unhealthy_data"
		}
		return result
	}
	if !d.isFresh(now, features) {
		result.Reason = "stale_features"
		return result
	}
	if features.Close <= 0 || features.Ema21 <= 0 || features.Ema55 <= 0 {
		result.Reason = "missing_trend_features"
		return result
	}
	if features.AtrPct >= d.cfg.BreakoutAtrPctMin {
		result.State = MarketStateBreakout
		result.Confidence = 0.8
		result.Reason = "atr_pct_high"
		return result
	}
	if features.AtrPct > 0 && features.AtrPct <= d.cfg.RangeAtrPctMax {
		result.State = MarketStateRange
		result.Confidence = 0.7
		result.Reason = "atr_pct_low"
		return result
	}
	if features.Close > features.Ema21 && features.Ema21 > features.Ema55 {
		result.State = MarketStateTrendUp
		result.Confidence = 0.75
		result.Reason = "ema_bull_alignment"
		return result
	}
	if features.Close < features.Ema21 && features.Ema21 < features.Ema55 {
		result.State = MarketStateTrendDown
		result.Confidence = 0.75
		result.Reason = "ema_bear_alignment"
		return result
	}
	result.State = MarketStateRange
	result.Confidence = 0.5
	result.Reason = "fallback_range"
	return result
}

// isFresh 判断输入特征是否仍在状态识别允许的新鲜度窗口内。
func (d *DefaultDetector) isFresh(now time.Time, features Features) bool {
	if d == nil || features.UpdatedAt.IsZero() {
		return false
	}
	return now.Sub(features.UpdatedAt) <= d.cfg.FreshnessWindow
}
