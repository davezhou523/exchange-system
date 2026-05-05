package marketstate

import (
	"time"

	"exchange-system/common/regimejudge"
)

// Evaluation 表示一次市场判态同时产出的底层 Analysis 和对外 Result。
type Evaluation struct {
	Analysis regimejudge.Analysis
	Result   Result
}

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
	cfg = normalizeConfig(cfg)
	return &DefaultDetector{cfg: cfg}
}

// normalizeConfig 为判态器补齐最小默认阈值，保证不同入口的判态口径一致。
func normalizeConfig(cfg Config) Config {
	if cfg.FreshnessWindow <= 0 {
		cfg.FreshnessWindow = 3 * time.Minute
	}
	if cfg.RangeAtrPctMax <= 0 {
		cfg.RangeAtrPctMax = 0.0015
	}
	if cfg.BreakoutAtrPctMin <= 0 {
		cfg.BreakoutAtrPctMin = 0.006
	}
	return cfg
}

// Evaluate 基于统一 Regime Judge 产出底层 Analysis 和对外市场状态结果。
func Evaluate(now time.Time, features Features, cfg Config) Evaluation {
	cfg = normalizeConfig(cfg)
	features = NormalizeFeatures(features)
	result := Result{
		Symbol:    features.Symbol,
		State:     MarketStateUnknown,
		Reason:    "insufficient_features",
		UpdatedAt: now.UTC(),
	}
	analysis := regimejudge.Analyze(now, features, regimejudge.Config{
		FreshnessWindow:   cfg.FreshnessWindow,
		RangeAtrPctMax:    cfg.RangeAtrPctMax,
		BreakoutAtrPctMin: cfg.BreakoutAtrPctMin,
	})
	features = analysis.Features
	if !analysis.Healthy {
		if features.LastReason != "" {
			result.Reason = features.LastReason
		} else {
			result.Reason = "unhealthy_data"
		}
		return Evaluation{Analysis: analysis, Result: result}
	}
	if !analysis.Fresh {
		result.Reason = "stale_features"
		return Evaluation{Analysis: analysis, Result: result}
	}
	if !analysis.HasTrendFeatures {
		result.Reason = "missing_trend_features"
		return Evaluation{Analysis: analysis, Result: result}
	}
	if analysis.BreakoutMatch {
		result.State = MarketStateBreakout
		result.Confidence = 0.8
		result.Reason = "atr_pct_high"
		return Evaluation{Analysis: analysis, Result: result}
	}
	if analysis.RangeMatch {
		result.State = MarketStateRange
		result.Confidence = 0.7
		result.Reason = "atr_pct_low"
		return Evaluation{Analysis: analysis, Result: result}
	}
	if analysis.BullTrendStrict {
		result.State = MarketStateTrendUp
		result.Confidence = 0.75
		result.Reason = "ema_bull_alignment"
		return Evaluation{Analysis: analysis, Result: result}
	}
	if analysis.BearTrendStrict {
		result.State = MarketStateTrendDown
		result.Confidence = 0.75
		result.Reason = "ema_bear_alignment"
		return Evaluation{Analysis: analysis, Result: result}
	}
	result.State = MarketStateRange
	result.Confidence = 0.5
	result.Reason = "fallback_range"
	return Evaluation{Analysis: analysis, Result: result}
}

// Detect 根据最小趋势和波动规则识别当前市场状态。
func (d *DefaultDetector) Detect(now time.Time, features Features) Result {
	if d == nil {
		return Result{
			Symbol:    features.Symbol,
			State:     MarketStateUnknown,
			Reason:    "insufficient_features",
			UpdatedAt: now.UTC(),
		}
	}
	return Evaluate(now, features, d.cfg).Result
}
