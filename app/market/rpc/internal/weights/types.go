package weights

import (
	"time"

	"exchange-system/app/market/rpc/internal/marketstate"
	"exchange-system/common/regimejudge"
)

// Config 定义最小版 Strategy Weight Engine 的阈值参数。
type Config struct {
	DefaultTrendWeight    float64
	DefaultRangeWeight    float64
	DefaultBreakoutWeight float64
	DefaultRiskScale      float64
	LossStreakThreshold   int
	DailyLossSoftLimit    float64
	DrawdownSoftLimit     float64
	CoolingPauseDuration  time.Duration
	AtrSpikeRatioMin      float64
	VolumeSpikeRatioMin   float64
	CoolingMinSamples     int
	TrendStrategyMix      map[string]float64
	BreakoutStrategyMix   map[string]float64
	RangeStrategyMix      map[string]float64
	TrendSymbolWeights    map[string]float64
	BreakoutSymbolWeights map[string]float64
	RangeSymbolWeights    map[string]float64
}

// Inputs 表示一次权重评估所需的最小输入。
type Inputs struct {
	MarketState        marketstate.AggregateResult
	RegimeAnalyses     map[string]regimejudge.Analysis
	Symbols            []string
	Templates          map[string]string
	StrategyBuckets    map[string]string
	RouteReasons       map[string]string
	SymbolScores       map[string]float64
	LossStreak         int
	DailyLossPct       float64
	DrawdownPct        float64
	AvgAtrPct          float64
	AvgVolume          float64
	HealthySymbolCount int
	UpdatedAt          time.Time
}

// Recommendation 表示某个 symbol 当前的权重与风险预算建议。
type Recommendation struct {
	Symbol         string  `json:"symbol"`
	Template       string  `json:"template,omitempty"`
	Bucket         string  `json:"route_bucket,omitempty"`
	RouteReason    string  `json:"route_reason,omitempty"`
	Score          float64 `json:"score,omitempty"`
	ScoreSource    string  `json:"score_source,omitempty"`
	BucketBudget   float64 `json:"bucket_budget,omitempty"`
	StrategyWeight float64 `json:"strategy_weight"`
	SymbolWeight   float64 `json:"symbol_weight"`
	RiskScale      float64 `json:"risk_scale"`
	PositionBudget float64 `json:"position_budget"`
	TradingPaused  bool    `json:"trading_paused"`
	PauseReason    string  `json:"pause_reason,omitempty"`
}

// Output 表示一次权重评估的结构化输出。
type Output struct {
	Recommendations   []Recommendation   `json:"recommendations"`
	MarketPaused      bool               `json:"market_paused"`
	MarketPauseReason string             `json:"market_pause_reason,omitempty"`
	CoolingUntil      time.Time          `json:"cooling_until,omitempty"`
	AtrSpikeRatio     float64            `json:"atr_spike_ratio,omitempty"`
	VolumeSpikeRatio  float64            `json:"volume_spike_ratio,omitempty"`
	MatchCounts       map[string]int     `json:"match_counts,omitempty"`
	StrategyMix       map[string]float64 `json:"strategy_mix,omitempty"`
	BucketBudgets     map[string]float64 `json:"bucket_budgets,omitempty"`
	BucketSymbolCount map[string]int     `json:"bucket_symbol_count,omitempty"`
	UpdatedAt         time.Time          `json:"updated_at"`
}

// Engine 定义权重引擎接口，便于后续替换成更复杂的实现。
type Engine interface {
	// Evaluate 根据 MarketState 和风险输入生成当前轮权重建议。
	Evaluate(now time.Time, in Inputs) Output
}
