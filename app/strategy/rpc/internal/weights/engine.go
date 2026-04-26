package weights

import (
	"sort"
	"time"

	"exchange-system/app/strategy/rpc/internal/marketstate"
)

// DefaultEngine 是 Phase 5 第一版的最小权重引擎实现。
type DefaultEngine struct {
	cfg Config
}

// NewEngine 创建一个带保守默认值的权重引擎。
func NewEngine(cfg Config) *DefaultEngine {
	if cfg.DefaultTrendWeight <= 0 {
		cfg.DefaultTrendWeight = 0.7
	}
	if cfg.DefaultRangeWeight <= 0 {
		cfg.DefaultRangeWeight = 0.7
	}
	if cfg.DefaultBreakoutWeight <= 0 {
		cfg.DefaultBreakoutWeight = 0.7
	}
	if cfg.DefaultRiskScale <= 0 {
		cfg.DefaultRiskScale = 1
	}
	if cfg.LossStreakThreshold <= 0 {
		cfg.LossStreakThreshold = 3
	}
	if cfg.DailyLossSoftLimit <= 0 {
		cfg.DailyLossSoftLimit = 0.03
	}
	if cfg.DrawdownSoftLimit <= 0 {
		cfg.DrawdownSoftLimit = 0.10
	}
	return &DefaultEngine{cfg: cfg}
}

// Evaluate 根据当前 MarketState、symbol score 和风险输入生成权重建议。
func (e *DefaultEngine) Evaluate(now time.Time, in Inputs) Output {
	if e == nil {
		return Output{UpdatedAt: now.UTC()}
	}
	if in.UpdatedAt.IsZero() {
		in.UpdatedAt = now.UTC()
	}
	riskScale, paused, pauseReason := e.computeRiskScale(in)
	recommendations := e.allocateBudgets(in, riskScale, paused, pauseReason)
	return Output{
		Recommendations:   recommendations,
		MarketPaused:      paused,
		MarketPauseReason: pauseReason,
		UpdatedAt:         in.UpdatedAt.UTC(),
	}
}

// computeRiskScale 根据连亏、单日亏损和回撤情况生成当前轮风险缩放建议。
func (e *DefaultEngine) computeRiskScale(in Inputs) (float64, bool, string) {
	riskScale := e.cfg.DefaultRiskScale
	if in.LossStreak >= e.cfg.LossStreakThreshold {
		riskScale *= 0.5
	}
	if in.DailyLossPct >= e.cfg.DailyLossSoftLimit {
		riskScale *= 0.5
	}
	if in.DrawdownPct >= e.cfg.DrawdownSoftLimit {
		riskScale *= 0.5
	}
	if riskScale <= 0.125 {
		return 0, true, "risk_limit_triggered"
	}
	return riskScale, false, ""
}

// allocateBudgets 按当前状态和 symbol score 生成每个交易对的最小仓位预算建议。
func (e *DefaultEngine) allocateBudgets(in Inputs, riskScale float64, paused bool, pauseReason string) []Recommendation {
	symbols := append([]string(nil), in.Symbols...)
	sort.Strings(symbols)

	baseStrategyWeight := e.strategyWeightForState(in.MarketState.State)
	totalScore := 0.0
	scoreBySymbol := make(map[string]float64, len(symbols))
	for _, symbol := range symbols {
		score := in.SymbolScores[symbol]
		if score <= 0 {
			score = 1
		}
		scoreBySymbol[symbol] = score
		totalScore += score
	}
	if totalScore <= 0 {
		totalScore = float64(len(symbols))
		if totalScore <= 0 {
			totalScore = 1
		}
	}

	out := make([]Recommendation, 0, len(symbols))
	for _, symbol := range symbols {
		symbolWeight := scoreBySymbol[symbol] / totalScore
		rec := Recommendation{
			Symbol:         symbol,
			Template:       in.Templates[symbol],
			StrategyWeight: baseStrategyWeight,
			SymbolWeight:   symbolWeight,
			RiskScale:      riskScale,
			PositionBudget: baseStrategyWeight * symbolWeight * riskScale,
			TradingPaused:  paused,
			PauseReason:    pauseReason,
		}
		out = append(out, rec)
	}
	return out
}

// strategyWeightForState 根据市场状态返回当前轮的基础策略权重。
func (e *DefaultEngine) strategyWeightForState(state marketstate.MarketState) float64 {
	switch state {
	case marketstate.MarketStateTrendUp, marketstate.MarketStateTrendDown:
		return e.cfg.DefaultTrendWeight
	case marketstate.MarketStateBreakout:
		return e.cfg.DefaultBreakoutWeight
	case marketstate.MarketStateRange:
		return e.cfg.DefaultRangeWeight
	default:
		return 0.5
	}
}
