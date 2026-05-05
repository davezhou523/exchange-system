package marketstate

import (
	"time"

	"exchange-system/common/featureengine"
)

// MarketState 表示当前市场 regime 的最小分类结果。
type MarketState string

const (
	// MarketStateTrendUp 表示市场处于相对稳定的上行趋势状态。
	MarketStateTrendUp MarketState = "trend_up"
	// MarketStateTrendDown 表示市场处于相对稳定的下行趋势状态。
	MarketStateTrendDown MarketState = "trend_down"
	// MarketStateRange 表示市场处于低波动或窄幅震荡状态。
	MarketStateRange MarketState = "range"
	// MarketStateBreakout 表示市场处于高波动或突破阶段。
	MarketStateBreakout MarketState = "breakout"
	// MarketStateUnknown 表示当前数据不足或不健康，无法安全识别状态。
	MarketStateUnknown MarketState = "unknown"
)

// Features 复用统一 Feature Engine 的标准特征结构，避免 strategy 自己再维护一套字段定义。
type Features = featureengine.Features

// Result 表示一次市场状态识别的结构化输出。
type Result struct {
	Symbol                   string
	State                    MarketState
	Confidence               float64
	Reason                   string
	UpdatedAt                time.Time
	FallbackSource           string
	FallbackMissingIntervals []string
	FallbackIntervalReady    map[string]bool
	FallbackIntervalReasons  map[string]string
	FallbackIntervalAgeSec   map[string]int64
}

// AggregateResult 表示一轮全市场状态聚合后的结构化输出。
type AggregateResult struct {
	State           MarketState
	Confidence      float64
	Reason          string
	UpdatedAt       time.Time
	StateCounts     map[string]int
	MatchCounts     map[string]int
	HealthyCount    int
	UnknownCount    int
	DominantSymbols []string
}

// Config 定义最小版状态识别器的阈值参数。
type Config struct {
	FreshnessWindow   time.Duration
	RangeAtrPctMax    float64
	BreakoutAtrPctMin float64
}
