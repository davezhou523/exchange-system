package universepool

import (
	"encoding/json"
	"fmt"
	"time"
)

// SymbolLifecycleState 表示某个交易对在动态币池中的生命周期状态。
type SymbolLifecycleState string

const (
	// SymbolInactive 表示当前未订阅、未激活。
	SymbolInactive SymbolLifecycleState = "inactive"
	// SymbolPendingAdd 表示已进入候选集，等待真正加入订阅。
	SymbolPendingAdd SymbolLifecycleState = "pending_add"
	// SymbolWarming 表示已经订阅，但仍处于 warmup 阶段。
	SymbolWarming SymbolLifecycleState = "warming"
	// SymbolActive 表示 warmup 完成，已进入可正常使用状态。
	SymbolActive SymbolLifecycleState = "active"
	// SymbolPendingRemove 表示准备移除，但仍处于最小启用时长保护中。
	SymbolPendingRemove SymbolLifecycleState = "pending_remove"
	// SymbolCooldown 表示刚移除后的冷却期，暂不允许立即重新加入。
	SymbolCooldown SymbolLifecycleState = "cooldown"
)

// Config 定义动态币池管理器的最小配置集合。
type Config struct {
	Enabled                  bool
	CandidateSymbols         []string
	AllowList                []string
	BlockList                []string
	ValidationMode           string
	TrendPreferredSymbols    []string
	RangePreferredSymbols    []string
	BreakoutPreferredSymbols []string
	BreakoutAtrPctMin        float64
	BreakoutAtrPctExitMin    float64
	EvaluateInterval         time.Duration
	MinActiveDuration        time.Duration
	MinInactiveDuration      time.Duration
	CooldownDuration         time.Duration
	AddScoreThreshold        float64
	RemoveScoreThreshold     float64
	Warmup                   WarmupConfig
}

// WarmupConfig 定义交易对从订阅到可用之间的最小预热要求。
type WarmupConfig struct {
	Enabled                bool
	Min1mBars              int
	Require15mReady        bool
	Require1hReady         bool
	Require4hReady         bool
	RequireIndicatorsReady bool
}

// Snapshot 表示 selector 评估动态币池时使用的轻量市场快照。
type Snapshot struct {
	Symbol     string
	UpdatedAt  time.Time
	LastPrice  float64
	Ema21      float64
	Ema55      float64
	Rsi        float64
	Atr        float64
	AtrPct     float64
	Volume24h  float64
	SpreadBps  float64
	Healthy    bool
	LastReason string
}

// RankDetail 表示 Symbol Ranker 对某个候选币产出的基础分与分量拆解。
type RankDetail struct {
	BaseScore       float64 `json:"base_score,omitempty"`
	TrendScore      float64 `json:"trend_score,omitempty"`
	VolatilityScore float64 `json:"volatility_score,omitempty"`
	VolumeScore     float64 `json:"volume_score,omitempty"`
	RawTrendScore   float64 `json:"raw_trend_score,omitempty"`
	RawVolatility   float64 `json:"raw_volatility,omitempty"`
	RawVolume       float64 `json:"raw_volume,omitempty"`
}

// MarshalJSON 自定义序列化，统一保留4位小数。
func (r *RankDetail) MarshalJSON() ([]byte, error) {
	type alias RankDetail
	return json.Marshal(&struct {
		*alias
		BaseScore       string `json:"base_score"`
		TrendScore      string `json:"trend_score"`
		VolatilityScore string `json:"volatility_score"`
		VolumeScore     string `json:"volume_score"`
		RawTrendScore   string `json:"raw_trend_score"`
		RawVolatility   string `json:"raw_volatility"`
		RawVolume       string `json:"raw_volume"`
	}{
		alias:           (*alias)(r),
		BaseScore:       fmt.Sprintf("%.4f", r.BaseScore),
		TrendScore:      fmt.Sprintf("%.4f", r.TrendScore),
		VolatilityScore: fmt.Sprintf("%.4f", r.VolatilityScore),
		VolumeScore:     fmt.Sprintf("%.4f", r.VolumeScore),
		RawTrendScore:   fmt.Sprintf("%.4f", r.RawTrendScore),
		RawVolatility:   fmt.Sprintf("%.4f", r.RawVolatility),
		RawVolume:       fmt.Sprintf("%.4f", r.RawVolume),
	})
}

// StateVoteDetail 表示某个候选币在本轮全局状态投票中的分类结果与论证依据。
type StateVoteDetail struct {
	ClassifiedState    string      `json:"classified_state,omitempty"`
	ClassifiedReason   string      `json:"classified_reason,omitempty"`
	ClassifiedReasonZh string      `json:"classified_reason_zh,omitempty"`
	Fresh              bool        `json:"fresh"`
	Healthy            bool        `json:"healthy"`
	LastPrice          float64     `json:"last_price,omitempty"`
	Ema21              float64     `json:"ema21,omitempty"`
	Ema55              float64     `json:"ema55,omitempty"`
	AtrPct             float64     `json:"atr_pct,omitempty"`
	RangeAtrPctMax     float64     `json:"range_atr_pct_max,omitempty"`
	BreakoutAtrPctMin  float64     `json:"breakout_atr_pct_min,omitempty"`
	TrendAligned       bool        `json:"trend_aligned"`
	RangeMatch         bool        `json:"range_match"`
	BreakoutMatch      bool        `json:"breakout_match"`
	TrendMatch         bool        `json:"trend_match"`
	RankDetail         *RankDetail `json:"rank_detail,omitempty"`
}

// MarshalJSON 自定义序列化，统一保留4位小数。
func (s *StateVoteDetail) MarshalJSON() ([]byte, error) {
	type alias StateVoteDetail
	return json.Marshal(&struct {
		*alias
		LastPrice         string `json:"last_price"`
		Ema21             string `json:"ema21"`
		Ema55             string `json:"ema55"`
		AtrPct            string `json:"atr_pct"`
		RangeAtrPctMax    string `json:"range_atr_pct_max"`
		BreakoutAtrPctMin string `json:"breakout_atr_pct_min"`
	}{
		alias:             (*alias)(s),
		LastPrice:         fmt.Sprintf("%.4f", s.LastPrice),
		Ema21:             fmt.Sprintf("%.4f", s.Ema21),
		Ema55:             fmt.Sprintf("%.4f", s.Ema55),
		AtrPct:            fmt.Sprintf("%.4f", s.AtrPct),
		RangeAtrPctMax:    fmt.Sprintf("%.4f", s.RangeAtrPctMax),
		BreakoutAtrPctMin: fmt.Sprintf("%.4f", s.BreakoutAtrPctMin),
	})
}

// DesiredUniverseSymbol 表示某个交易对在当前轮评估中的目标状态。
type DesiredUniverseSymbol struct {
	Symbol    string
	Reason    string
	Score     float64
	Desired   bool
	Template  string
	StateVote StateVoteDetail
}

// DesiredUniverse 表示当前轮评估希望纳入动态币池的交易对集合。
type DesiredUniverse struct {
	GlobalState   string
	TrendCount    int
	RangeCount    int
	BreakoutCount int
	Symbols       map[string]DesiredUniverseSymbol
	StateVotes    map[string]StateVoteDetail
}

// SymbolRuntimeState 表示某个交易对在 market 侧的运行时状态。
type SymbolRuntimeState struct {
	Symbol          string
	State           SymbolLifecycleState
	Subscribed      bool
	Desired         bool
	LastStateChange time.Time
	WarmupStartedAt time.Time
	ActiveAt        time.Time
	CooldownUntil   time.Time
	Reason          string
	Template        string
}

// WarmupStatus 表示某个交易对当前的 warmup 完成度。
type WarmupStatus struct {
	Symbol               string
	HasEnough1mBars      bool
	Has15mReady          bool
	Has1hReady           bool
	Has4hReady           bool
	IndicatorsReady      bool
	Ready                bool
	LastUpdatedAt        time.Time
	LastIncompleteReason string
}

// Selector 定义动态币池选择器接口，负责产出目标交易宇宙。
type Selector interface {
	// Evaluate 根据当前候选快照生成目标交易宇宙。
	Evaluate(now time.Time, snapshots map[string]Snapshot) DesiredUniverse
}

// WarmupChecker 定义 warmup 查询接口，供状态机判断是否可以从 warming 进入 active。
type WarmupChecker interface {
	// GetWarmupStatus 返回指定交易对当前的 warmup 状态。
	GetWarmupStatus(symbol string) WarmupStatus
}

// SubscriptionController 定义订阅控制接口，供动态币池更新真实 WebSocket 订阅集合。
type SubscriptionController interface {
	// UpdateSymbols 用新的 symbol 集合替换当前订阅集合。
	UpdateSymbols(symbols []string) error
	// CurrentSymbols 返回当前正在订阅的 symbol 集合快照。
	CurrentSymbols() []string
}

// Logger 定义动态币池的结构化日志接口。
type Logger interface {
	// WriteSymbolEvent 写入单个交易对的状态变化日志。
	WriteSymbolEvent(now time.Time, state SymbolRuntimeState, warmup WarmupStatus, action string)
	// WriteSelectorDecision 写入 selector 本轮对单个交易对的决策快照。
	WriteSelectorDecision(now time.Time, state SymbolRuntimeState, warmup WarmupStatus, globalState string, decision DesiredUniverseSymbol)
	// WriteMeta 写入当前轮整体评估的聚合日志。
	WriteMeta(now time.Time, summary stateSummary)
}
