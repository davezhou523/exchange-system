package regimejudge

import (
	"time"

	"exchange-system/common/featureengine"
)

const (
	// RouteReasonRange 表示统一判态结果支持走震荡桶。
	RouteReasonRange = "market_state_range"
	// RouteReasonBreakout 表示统一判态结果支持走突破桶。
	RouteReasonBreakout = "market_state_breakout"
	// RouteReasonTrend 表示统一判态结果支持走趋势桶。
	RouteReasonTrend = "market_state_trend"
)

// Config 定义统一 Regime Judge 需要的最小阈值配置。
type Config struct {
	FreshnessWindow   time.Duration
	RangeAtrPctMax    float64
	BreakoutAtrPctMin float64
}

// Analysis 表示一次统一判态分析的结构化结果，供上层决定如何分类和解释原因。
type Analysis struct {
	Features           featureengine.Features
	Healthy            bool
	Fresh              bool
	HasTrendFeatures   bool
	RangeMatch         bool
	BreakoutMatch      bool
	BullTrendStrict    bool
	BullTrendInclusive bool
	BullTrendAligned   bool
	BearTrendStrict    bool
	BearTrendAligned   bool
}

// HasSignal 判断当前 Analysis 是否已经携带可供上层消费的判态信息。
func (a Analysis) HasSignal() bool {
	return a.Healthy || a.Fresh || a.HasTrendFeatures ||
		a.RangeMatch || a.BreakoutMatch ||
		a.BullTrendStrict || a.BullTrendInclusive || a.BullTrendAligned ||
		a.BearTrendStrict || a.BearTrendAligned ||
		a.Features.Close > 0 || a.Features.Ema21 > 0 || a.Features.Ema55 > 0 ||
		a.Features.Atr > 0 || a.Features.AtrPct > 0 || a.Features.Volume > 0
}

// PrefersRange 判断当前 Analysis 是否明确支持走震荡路由。
func (a Analysis) PrefersRange() bool {
	return a.RangeMatch
}

// RangeReason 返回当前震荡命中的统一解释原因，供上层日志和路由复用。
func (a Analysis) RangeReason() string {
	if a.PrefersRange() {
		return RouteReasonRange
	}
	return ""
}

// PrefersBreakout 判断当前 Analysis 是否明确支持走突破路由。
func (a Analysis) PrefersBreakout() bool {
	return a.BreakoutMatch
}

// BreakoutReason 返回当前突破命中的统一解释原因，供上层日志和路由复用。
func (a Analysis) BreakoutReason() string {
	if a.PrefersBreakout() {
		return RouteReasonBreakout
	}
	return ""
}

// PrefersTrend 判断当前 Analysis 是否明确支持走趋势路由。
func (a Analysis) PrefersTrend() bool {
	return a.BullTrendStrict || a.BearTrendStrict
}

// TrendReason 返回当前趋势命中的统一解释原因，供上层日志和路由复用。
func (a Analysis) TrendReason() string {
	if a.PrefersTrend() {
		return RouteReasonTrend
	}
	return ""
}

// Analyze 基于统一特征计算 breakout/range/trend 命中情况，收拢 market 和 strategy 的底层判态规则。
func Analyze(now time.Time, in featureengine.Features, cfg Config) Analysis {
	features := featureengine.Normalize(in)
	if cfg.FreshnessWindow <= 0 {
		cfg.FreshnessWindow = 3 * time.Minute
	}

	out := Analysis{
		Features:         features,
		Healthy:          features.Healthy,
		Fresh:            isFresh(now, features, cfg.FreshnessWindow),
		HasTrendFeatures: hasTrendFeatures(features),
	}
	if !out.Healthy || !out.Fresh {
		return out
	}

	out.RangeMatch = features.AtrPct > 0 && features.AtrPct <= cfg.RangeAtrPctMax
	out.BreakoutMatch = features.AtrPct >= cfg.BreakoutAtrPctMin
	if !out.HasTrendFeatures {
		return out
	}

	out.BullTrendStrict = features.Close > features.Ema21 && features.Ema21 > features.Ema55
	out.BullTrendInclusive = features.Close >= features.Ema21 && features.Ema21 > features.Ema55
	out.BullTrendAligned = features.Ema21 > features.Ema55
	out.BearTrendStrict = features.Close < features.Ema21 && features.Ema21 < features.Ema55
	out.BearTrendAligned = features.Ema21 < features.Ema55
	return out
}

// isFresh 判断当前特征是否仍处于允许参与判态的新鲜度窗口内。
func isFresh(now time.Time, features featureengine.Features, window time.Duration) bool {
	if features.UpdatedAt.IsZero() || window <= 0 {
		return false
	}
	return now.Sub(features.UpdatedAt) <= window
}

// hasTrendFeatures 判断当前特征是否具备最小趋势识别所需字段。
func hasTrendFeatures(features featureengine.Features) bool {
	return features.Close > 0 && features.Ema21 > 0 && features.Ema55 > 0
}
