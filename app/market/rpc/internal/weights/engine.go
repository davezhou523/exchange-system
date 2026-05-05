package weights

import (
	"sort"
	"time"

	"exchange-system/app/market/rpc/internal/marketstate"
	"exchange-system/common/regimejudge"
)

type strategyBucket string

const (
	strategyBucketTrend    strategyBucket = "trend"
	strategyBucketBreakout strategyBucket = "breakout"
	strategyBucketRange    strategyBucket = "range"
)

var trendPreferredSymbolWeights = map[string]float64{
	"ETHUSDT": 0.4,
	"SOLUSDT": 0.3,
	"BNBUSDT": 0.3,
}

var breakoutPreferredSymbolWeights = map[string]float64{
	"BTCUSDT":  0.5,
	"DOGEUSDT": 0.25,
	"PEPEUSDT": 0.25,
}

var defaultTrendStrategyMix = map[string]float64{
	"trend":    0.7,
	"breakout": 0.3,
}

var defaultBreakoutStrategyMix = map[string]float64{
	"breakout": 0.7,
	"trend":    0.3,
}

var defaultRangeStrategyMix = map[string]float64{
	"range": 0.7,
	"trend": 0.3,
}

// DefaultEngine 是 Phase 5 第一版的最小权重引擎实现。
type DefaultEngine struct {
	cfg          Config
	lastPulse    marketPulse
	coolingUntil time.Time
}

type marketPulse struct {
	AvgAtrPct          float64
	AvgVolume          float64
	HealthySymbolCount int
}

type scoreDecision struct {
	Value  float64
	Source string
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
	if cfg.CoolingPauseDuration <= 0 {
		cfg.CoolingPauseDuration = 30 * time.Minute
	}
	if cfg.AtrSpikeRatioMin <= 0 {
		cfg.AtrSpikeRatioMin = 1.8
	}
	if cfg.VolumeSpikeRatioMin <= 0 {
		cfg.VolumeSpikeRatioMin = 2.0
	}
	if cfg.CoolingMinSamples <= 0 {
		cfg.CoolingMinSamples = 2
	}
	if len(cfg.TrendStrategyMix) == 0 {
		cfg.TrendStrategyMix = cloneSymbolWeights(defaultTrendStrategyMix)
	}
	if len(cfg.BreakoutStrategyMix) == 0 {
		cfg.BreakoutStrategyMix = cloneSymbolWeights(defaultBreakoutStrategyMix)
	}
	if len(cfg.RangeStrategyMix) == 0 {
		cfg.RangeStrategyMix = cloneSymbolWeights(defaultRangeStrategyMix)
	}
	if len(cfg.TrendSymbolWeights) == 0 {
		cfg.TrendSymbolWeights = cloneSymbolWeights(trendPreferredSymbolWeights)
	}
	if len(cfg.BreakoutSymbolWeights) == 0 {
		cfg.BreakoutSymbolWeights = cloneSymbolWeights(breakoutPreferredSymbolWeights)
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
	riskScale, paused, pauseReason, coolingUntil, atrSpikeRatio, volumeSpikeRatio := e.computeRiskScale(now, in)
	strategyWeights := e.strategyWeightsForAggregate(in.MarketState)
	recommendations := e.allocateBudgets(in, strategyWeights, riskScale, paused, pauseReason)
	return Output{
		Recommendations:   recommendations,
		MarketPaused:      paused,
		MarketPauseReason: pauseReason,
		CoolingUntil:      coolingUntil,
		AtrSpikeRatio:     atrSpikeRatio,
		VolumeSpikeRatio:  volumeSpikeRatio,
		MatchCounts:       cloneMatchCounts(in.MarketState.MatchCounts),
		StrategyMix:       strategyMixForLog(strategyWeights),
		BucketBudgets:     bucketBudgetsForLog(strategyWeights, riskScale),
		BucketSymbolCount: bucketSymbolCountForLog(recommendations),
		UpdatedAt:         in.UpdatedAt.UTC(),
	}
}

// computeRiskScale 根据连亏、回撤和市场降温状态生成当前轮风险缩放建议。
func (e *DefaultEngine) computeRiskScale(now time.Time, in Inputs) (float64, bool, string, time.Time, float64, float64) {
	if e == nil {
		return 0, false, "", time.Time{}, 0, 0
	}
	atrSpikeRatio, volumeSpikeRatio, triggerCooling := e.evaluateCoolingTrigger(in)
	e.updateCoolingState(now, triggerCooling)
	if !e.coolingUntil.IsZero() && now.Before(e.coolingUntil) {
		return 0, true, "market_cooling_pause", e.coolingUntil.UTC(), atrSpikeRatio, volumeSpikeRatio
	}

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
		return 0, true, "risk_limit_triggered", time.Time{}, atrSpikeRatio, volumeSpikeRatio
	}
	return riskScale, false, "", time.Time{}, atrSpikeRatio, volumeSpikeRatio
}

// allocateBudgets 按“策略权重 * symbol 权重 * 风险因子”生成每个交易对的仓位预算。
func (e *DefaultEngine) allocateBudgets(in Inputs, strategyWeights map[strategyBucket]float64, riskScale float64, paused bool, pauseReason string) []Recommendation {
	symbols := append([]string(nil), in.Symbols...)
	sort.Strings(symbols)

	scoreBySymbol := make(map[string]scoreDecision, len(symbols))
	symbolsByBucket := make(map[strategyBucket][]string)
	for _, symbol := range symbols {
		scoreBySymbol[symbol] = resolveScoreDecision(in, symbol)
		bucket := bucketForSymbol(in, symbol)
		symbolsByBucket[bucket] = append(symbolsByBucket[bucket], symbol)
	}

	symbolWeights := make(map[string]float64, len(symbols))
	for bucket, items := range symbolsByBucket {
		weights := e.symbolWeightsForBucket(in.MarketState.State, bucket, items, scoreBySymbol)
		for symbol, weight := range weights {
			symbolWeights[symbol] = weight
		}
	}

	out := make([]Recommendation, 0, len(symbols))
	for _, symbol := range symbols {
		bucket := bucketForSymbol(in, symbol)
		strategyWeight := strategyWeights[bucket]
		symbolWeight := symbolWeights[symbol]
		score := scoreBySymbol[symbol]
		if symbolWeight <= 0 {
			symbolWeight = 1
		}
		rec := Recommendation{
			Symbol:         symbol,
			Template:       in.Templates[symbol],
			Bucket:         string(bucket),
			RouteReason:    in.RouteReasons[symbol],
			Score:          score.Value,
			ScoreSource:    score.Source,
			BucketBudget:   strategyWeight * riskScale,
			StrategyWeight: strategyWeight,
			SymbolWeight:   symbolWeight,
			RiskScale:      riskScale,
			PositionBudget: strategyWeight * symbolWeight * riskScale,
			TradingPaused:  paused,
			PauseReason:    pauseReason,
		}
		out = append(out, rec)
	}
	return out
}

// bucketForSymbol 优先使用上游路由器显式给出的策略桶，避免 weights 再次从模板名反推。
func bucketForSymbol(in Inputs, symbol string) strategyBucket {
	if bucket, ok := parseStrategyBucket(in.StrategyBuckets[symbol]); ok {
		return bucket
	}
	return classifyStrategyBucket(in.Templates[symbol])
}

// scoreFromAnalysis 把统一 Regime Judge 结果转换成权重层可复用的最小 symbol score。
func scoreFromAnalysis(analysis regimejudge.Analysis) float64 {
	if !analysis.Healthy || !analysis.Fresh {
		return 0
	}
	switch {
	case analysis.BreakoutMatch:
		return 1.15
	case analysis.BullTrendStrict || analysis.BearTrendStrict:
		return 1.10
	case analysis.RangeMatch:
		return 1.05
	default:
		return 1
	}
}

// resolveScoreDecision 为某个 symbol 生成当前轮预算分配使用的 score 与其来源。
func resolveScoreDecision(in Inputs, symbol string) scoreDecision {
	if score := in.SymbolScores[symbol]; score > 0 {
		return scoreDecision{Value: score, Source: "symbol_score"}
	}
	if score := scoreFromAnalysis(in.RegimeAnalyses[symbol]); score > 0 {
		return scoreDecision{Value: score, Source: "regime_analysis"}
	}
	return scoreDecision{Value: 1, Source: "default"}
}

// strategyWeightsForAggregate 根据命中面强弱返回当前轮的策略桶资金配比；若无命中面则回退到单一市场状态。
func (e *DefaultEngine) strategyWeightsForAggregate(aggregate marketstate.AggregateResult) map[strategyBucket]float64 {
	if weights := strategyWeightsFromMatchCounts(aggregate.MatchCounts); len(weights) > 0 {
		return weights
	}
	return e.strategyWeightsForState(aggregate.State)
}

// strategyWeightsForState 根据市场状态返回当前轮的策略桶资金配比。
func (e *DefaultEngine) strategyWeightsForState(state marketstate.MarketState) map[strategyBucket]float64 {
	switch state {
	case marketstate.MarketStateTrendUp, marketstate.MarketStateTrendDown:
		return normalizeStrategyMix(e.cfg.TrendStrategyMix)
	case marketstate.MarketStateBreakout:
		return normalizeStrategyMix(e.cfg.BreakoutStrategyMix)
	case marketstate.MarketStateRange:
		return normalizeStrategyMix(e.cfg.RangeStrategyMix)
	default:
		return map[strategyBucket]float64{
			strategyBucketTrend: 0.5,
		}
	}
}

// strategyWeightsFromMatchCounts 把聚合后的命中面强度直接映射为策略桶配比。
func strategyWeightsFromMatchCounts(matchCounts map[string]int) map[strategyBucket]float64 {
	if len(matchCounts) == 0 {
		return nil
	}
	trendMatches := matchCounts[string(marketstate.MarketStateTrendUp)] + matchCounts[string(marketstate.MarketStateTrendDown)]
	breakoutMatches := matchCounts[string(marketstate.MarketStateBreakout)]
	rangeMatches := matchCounts[string(marketstate.MarketStateRange)]
	total := trendMatches + breakoutMatches + rangeMatches
	if total <= 0 {
		return nil
	}
	out := make(map[strategyBucket]float64, 3)
	if trendMatches > 0 {
		out[strategyBucketTrend] = float64(trendMatches) / float64(total)
	}
	if breakoutMatches > 0 {
		out[strategyBucketBreakout] = float64(breakoutMatches) / float64(total)
	}
	if rangeMatches > 0 {
		out[strategyBucketRange] = float64(rangeMatches) / float64(total)
	}
	return out
}

// strategyMixForLog 把内部策略桶配比转换成日志友好的字符串键结构。
func strategyMixForLog(in map[strategyBucket]float64) map[string]float64 {
	if len(in) == 0 {
		return nil
	}
	out := make(map[string]float64, len(in))
	for bucket, weight := range in {
		if weight <= 0 {
			continue
		}
		out[string(bucket)] = weight
	}
	if len(out) == 0 {
		return nil
	}
	return out
}

// cloneMatchCounts 复制聚合命中面统计，避免输出侧意外共享上游 map。
func cloneMatchCounts(in map[string]int) map[string]int {
	if len(in) == 0 {
		return nil
	}
	out := make(map[string]int, len(in))
	for state, count := range in {
		out[state] = count
	}
	return out
}

// symbolWeightsForBucket 返回某个策略桶内部各交易对的资金占比。
func (e *DefaultEngine) symbolWeightsForBucket(state marketstate.MarketState, bucket strategyBucket, symbols []string, scores map[string]scoreDecision) map[string]float64 {
	if len(symbols) == 0 {
		return nil
	}
	if state == marketstate.MarketStateTrendUp || state == marketstate.MarketStateTrendDown {
		if bucket == strategyBucketTrend {
			if weights := normalizeFixedSymbolWeights(symbols, e.cfg.TrendSymbolWeights); len(weights) > 0 {
				return weights
			}
		}
	}
	if state == marketstate.MarketStateBreakout && bucket == strategyBucketBreakout {
		if weights := normalizeFixedSymbolWeights(symbols, e.cfg.BreakoutSymbolWeights); len(weights) > 0 {
			return weights
		}
	}
	if state == marketstate.MarketStateRange && bucket == strategyBucketRange {
		if weights := normalizeFixedSymbolWeights(symbols, e.cfg.RangeSymbolWeights); len(weights) > 0 {
			return weights
		}
	}
	return normalizeScoreWeights(symbols, scores)
}

// classifyStrategyBucket 根据模板名把策略实例归到 trend/breakout/range 桶。
func classifyStrategyBucket(template string) strategyBucket {
	switch template {
	case "breakout-core":
		return strategyBucketBreakout
	case "range-core":
		return strategyBucketRange
	default:
		return strategyBucketTrend
	}
}

// normalizeFixedSymbolWeights 把固定配比裁剪到当前可交易的 symbol 集合并归一化。
func normalizeFixedSymbolWeights(symbols []string, fixed map[string]float64) map[string]float64 {
	total := 0.0
	out := make(map[string]float64)
	for _, symbol := range symbols {
		weight := fixed[symbol]
		if weight <= 0 {
			continue
		}
		out[symbol] = weight
		total += weight
	}
	if total <= 0 {
		return nil
	}
	for symbol, weight := range out {
		out[symbol] = weight / total
	}
	return out
}

// normalizeScoreWeights 按 score 在同一个策略桶内归一化资金占比。
func normalizeScoreWeights(symbols []string, scores map[string]scoreDecision) map[string]float64 {
	total := 0.0
	out := make(map[string]float64, len(symbols))
	for _, symbol := range symbols {
		score := scores[symbol].Value
		if score <= 0 {
			score = 1
		}
		out[symbol] = score
		total += score
	}
	if total <= 0 {
		total = float64(len(symbols))
		if total <= 0 {
			total = 1
		}
	}
	for symbol, score := range out {
		out[symbol] = score / total
	}
	return out
}

// bucketBudgetsForLog 返回每个策略桶当前轮的总预算，便于从 Allocator 视角直接观察桶级资金分配。
func bucketBudgetsForLog(strategyWeights map[strategyBucket]float64, riskScale float64) map[string]float64 {
	if len(strategyWeights) == 0 || riskScale <= 0 {
		return nil
	}
	out := make(map[string]float64, len(strategyWeights))
	for bucket, weight := range strategyWeights {
		budget := weight * riskScale
		if budget <= 0 {
			continue
		}
		out[string(bucket)] = budget
	}
	if len(out) == 0 {
		return nil
	}
	return out
}

// bucketSymbolCountForLog 返回每个策略桶当前轮覆盖的 symbol 数量。
func bucketSymbolCountForLog(recs []Recommendation) map[string]int {
	if len(recs) == 0 {
		return nil
	}
	out := make(map[string]int)
	for _, rec := range recs {
		if rec.Bucket == "" {
			continue
		}
		out[rec.Bucket]++
	}
	if len(out) == 0 {
		return nil
	}
	return out
}

// cloneSymbolWeights 复制一份 symbol->weight 映射，避免外部修改影响运行时配置。
func cloneSymbolWeights(in map[string]float64) map[string]float64 {
	if len(in) == 0 {
		return nil
	}
	out := make(map[string]float64, len(in))
	for symbol, weight := range in {
		out[symbol] = weight
	}
	return out
}

// normalizeStrategyMix 把 YAML 中的策略桶配比转成运行时可直接使用的归一化结果。
func normalizeStrategyMix(in map[string]float64) map[strategyBucket]float64 {
	total := 0.0
	out := make(map[strategyBucket]float64)
	for name, weight := range in {
		bucket, ok := parseStrategyBucket(name)
		if !ok || weight <= 0 {
			continue
		}
		out[bucket] += weight
		total += weight
	}
	if total <= 0 {
		return map[strategyBucket]float64{
			strategyBucketTrend: 0.5,
		}
	}
	for bucket, weight := range out {
		out[bucket] = weight / total
	}
	return out
}

// parseStrategyBucket 把配置键名解析为内部策略桶枚举。
func parseStrategyBucket(name string) (strategyBucket, bool) {
	switch name {
	case string(strategyBucketTrend):
		return strategyBucketTrend, true
	case string(strategyBucketBreakout):
		return strategyBucketBreakout, true
	case string(strategyBucketRange):
		return strategyBucketRange, true
	default:
		return "", false
	}
}

// evaluateCoolingTrigger 根据当前市场脉冲与上一轮基线判断是否需要进入降温暂停。
func (e *DefaultEngine) evaluateCoolingTrigger(in Inputs) (float64, float64, bool) {
	if e == nil {
		return 0, 0, false
	}
	current := marketPulse{
		AvgAtrPct:          in.AvgAtrPct,
		AvgVolume:          in.AvgVolume,
		HealthySymbolCount: in.HealthySymbolCount,
	}
	defer func() {
		e.lastPulse = current
	}()
	if current.HealthySymbolCount < e.cfg.CoolingMinSamples {
		return 0, 0, false
	}
	if e.lastPulse.HealthySymbolCount < e.cfg.CoolingMinSamples || e.lastPulse.AvgAtrPct <= 0 || e.lastPulse.AvgVolume <= 0 {
		return 0, 0, false
	}
	atrSpikeRatio := current.AvgAtrPct / e.lastPulse.AvgAtrPct
	volumeSpikeRatio := current.AvgVolume / e.lastPulse.AvgVolume
	triggerCooling := atrSpikeRatio >= e.cfg.AtrSpikeRatioMin && volumeSpikeRatio >= e.cfg.VolumeSpikeRatioMin
	return atrSpikeRatio, volumeSpikeRatio, triggerCooling
}

// updateCoolingState 在触发降温时刷新暂停截止时间，冷却结束后自动清空。
func (e *DefaultEngine) updateCoolingState(now time.Time, triggerCooling bool) {
	if e == nil {
		return
	}
	if triggerCooling {
		e.coolingUntil = now.UTC().Add(e.cfg.CoolingPauseDuration)
		return
	}
	if !e.coolingUntil.IsZero() && !now.Before(e.coolingUntil) {
		e.coolingUntil = time.Time{}
	}
}
