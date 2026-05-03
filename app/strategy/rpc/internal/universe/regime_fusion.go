package universe

import (
	"time"

	"exchange-system/app/strategy/rpc/internal/marketstate"
	"exchange-system/common/featureengine"
	"exchange-system/common/regimejudge"
)

const (
	// PrimaryRegimeWeight 表示 1H 主状态在融合中的默认权重。
	PrimaryRegimeWeight = 0.7
	// ConfirmRegimeWeight 表示 15M 辅助状态在融合中的默认权重。
	ConfirmRegimeWeight = 0.3
)

// KlineFrame 保存某个周期最近一根 K 线的轻量快照，供 selector 评估和状态输出复用。
type KlineFrame struct {
	Symbol      string
	Interval    string
	UpdatedAt   time.Time
	LastEventMs int64
	IsDirty     bool
	IsTradable  bool
	IsFinal     bool
	Close       float64
	Atr         float64
	Volume      float64
	Ema21       float64
	Ema55       float64
	Rsi         float64
}

// RegimeFrame 表示单个周期的结构化市场状态结果，便于 status/query 直接透出。
type RegimeFrame struct {
	Interval    string
	State       marketstate.MarketState
	Reason      string
	RouteReason string
	Confidence  float64
	UpdatedAt   time.Time
	Healthy     bool
	Fresh       bool
}

// RegimeFusion 表示 1H 主状态和 15M 辅助状态融合后的统一 regime 结果。
type RegimeFusion struct {
	PrimaryWeight float64
	ConfirmWeight float64
	H1            RegimeFrame
	M15           RegimeFrame
	FusedState    marketstate.MarketState
	FusedReason   string
	FusedScore    float64
	UpdatedAt     time.Time
}

// BuildRegimeFrame 把单周期 Evaluation 压缩成便于 selector/status 复用的轻量视图。
func BuildRegimeFrame(interval string, evaluation marketstate.Evaluation) RegimeFrame {
	frame := RegimeFrame{
		Interval:   interval,
		State:      evaluation.Result.State,
		Reason:     evaluation.Result.Reason,
		Confidence: evaluation.Result.Confidence,
		UpdatedAt:  evaluation.Result.UpdatedAt.UTC(),
		Healthy:    evaluation.Analysis.Healthy,
		Fresh:      evaluation.Analysis.Fresh,
	}
	switch {
	case evaluation.Analysis.RangeReason() != "":
		frame.RouteReason = evaluation.Analysis.RangeReason()
	case evaluation.Analysis.BreakoutReason() != "":
		frame.RouteReason = evaluation.Analysis.BreakoutReason()
	case evaluation.Analysis.TrendReason() != "":
		frame.RouteReason = evaluation.Analysis.TrendReason()
	default:
		frame.RouteReason = routeReasonForState(frame.State)
	}
	return frame
}

// FuseRegimes 按 1H 主状态 70% + 15M 辅助状态 30% 的口径生成统一融合状态和兼容 Analysis。
func FuseRegimes(h1 RegimeFrame, h1Analysis regimejudge.Analysis, m15 RegimeFrame, m15Analysis regimejudge.Analysis) (marketstate.MarketState, regimejudge.Analysis, RegimeFusion) {
	fusion := RegimeFusion{
		PrimaryWeight: PrimaryRegimeWeight,
		ConfirmWeight: ConfirmRegimeWeight,
		H1:            h1,
		M15:           m15,
	}
	scores := make(map[marketstate.MarketState]float64, 4)
	addRegimeScore(scores, h1, fusion.PrimaryWeight)
	addRegimeScore(scores, m15, fusion.ConfirmWeight)

	fusion.FusedState, fusion.FusedScore = selectFusedState(scores, h1, m15)
	fusion.FusedReason = fusedReason(h1, m15, fusion.FusedState)
	fusion.UpdatedAt = latestRegimeUpdateAt(h1, m15)

	analysis := mergedAnalysis(h1Analysis, m15Analysis, fusion)
	return fusion.FusedState, analysis, fusion
}

// routeReasonForState 把统一状态映射成 router 可复用的原因码，避免状态输出与路由原因分叉。
func routeReasonForState(state marketstate.MarketState) string {
	switch state {
	case marketstate.MarketStateRange:
		return regimejudge.RouteReasonRange
	case marketstate.MarketStateBreakout:
		return regimejudge.RouteReasonBreakout
	case marketstate.MarketStateTrendUp, marketstate.MarketStateTrendDown:
		return regimejudge.RouteReasonTrend
	default:
		return ""
	}
}

// addRegimeScore 把单周期状态按权重累积到候选分数，供融合阶段选择主导 regime。
func addRegimeScore(scores map[marketstate.MarketState]float64, frame RegimeFrame, weight float64) {
	if scores == nil || weight <= 0 {
		return
	}
	if frame.State == marketstate.MarketStateUnknown || frame.State == "" {
		return
	}
	score := frame.Confidence
	if score <= 0 {
		score = 0.5
	}
	scores[frame.State] += score * weight
}

// selectFusedState 从累计分数中挑出最终状态，并在分数接近时优先保留 1H 主状态。
func selectFusedState(scores map[marketstate.MarketState]float64, h1, m15 RegimeFrame) (marketstate.MarketState, float64) {
	if len(scores) == 0 {
		return marketstate.MarketStateUnknown, 0
	}
	bestState := marketstate.MarketStateUnknown
	bestScore := 0.0
	for _, candidate := range []marketstate.MarketState{
		h1.State,
		m15.State,
		marketstate.MarketStateBreakout,
		marketstate.MarketStateRange,
		marketstate.MarketStateTrendUp,
		marketstate.MarketStateTrendDown,
	} {
		if candidate == "" || candidate == marketstate.MarketStateUnknown {
			continue
		}
		if score := scores[candidate]; score > bestScore {
			bestState = candidate
			bestScore = score
		}
	}
	if bestState == marketstate.MarketStateUnknown && h1.State != marketstate.MarketStateUnknown {
		return h1.State, scores[h1.State]
	}
	if bestState == marketstate.MarketStateUnknown && m15.State != marketstate.MarketStateUnknown {
		return m15.State, scores[m15.State]
	}
	return bestState, bestScore
}

// fusedReason 产出融合层的解释原因，方便 status/query 判断当前是共振还是主周期压制。
func fusedReason(h1, m15 RegimeFrame, fused marketstate.MarketState) string {
	switch {
	case fused == marketstate.MarketStateUnknown:
		return "fusion_unavailable"
	case h1.State == fused && m15.State == fused:
		return "timeframes_aligned"
	case h1.State == fused && m15.State == marketstate.MarketStateUnknown:
		return "h1_only"
	case m15.State == fused && h1.State == marketstate.MarketStateUnknown:
		return "m15_only"
	case h1.State == fused:
		return "h1_primary_dominant"
	case m15.State == fused:
		return "m15_confirm_override"
	default:
		return "weighted_fusion"
	}
}

// latestRegimeUpdateAt 返回两个周期里最新的更新时间，便于外部快速判断融合结果是否新鲜。
func latestRegimeUpdateAt(h1, m15 RegimeFrame) time.Time {
	switch {
	case h1.UpdatedAt.After(m15.UpdatedAt):
		return h1.UpdatedAt.UTC()
	case !m15.UpdatedAt.IsZero():
		return m15.UpdatedAt.UTC()
	default:
		return h1.UpdatedAt.UTC()
	}
}

// mergedAnalysis 生成兼容旧 router 输入的统一 Analysis，让现有路由规则直接消费 fused regime。
func mergedAnalysis(h1Analysis, m15Analysis regimejudge.Analysis, fusion RegimeFusion) regimejudge.Analysis {
	base := h1Analysis
	if base.Features.UpdatedAt.IsZero() {
		base = m15Analysis
	}
	base.Features = mergeFeatures(h1Analysis.Features, m15Analysis.Features, fusion)
	base.Healthy = h1Analysis.Healthy || m15Analysis.Healthy
	base.Fresh = h1Analysis.Fresh || m15Analysis.Fresh
	base.HasTrendFeatures = h1Analysis.HasTrendFeatures || m15Analysis.HasTrendFeatures
	base.RangeMatch = fusion.FusedState == marketstate.MarketStateRange
	base.BreakoutMatch = fusion.FusedState == marketstate.MarketStateBreakout
	base.BullTrendStrict = fusion.FusedState == marketstate.MarketStateTrendUp
	base.BullTrendInclusive = base.BullTrendStrict
	base.BullTrendAligned = base.BullTrendStrict
	base.BearTrendStrict = fusion.FusedState == marketstate.MarketStateTrendDown
	base.BearTrendAligned = base.BearTrendStrict
	return base
}

// mergeFeatures 以 1H 为主、15M 为辅合并关键特征，避免现有 router 和 weights 失去可用的数值上下文。
func mergeFeatures(h1, m15 featureengine.Features, fusion RegimeFusion) featureengine.Features {
	out := h1
	if out.UpdatedAt.IsZero() {
		out = m15
	}
	out.Timeframe = "fusion_1h_15m"
	out.UpdatedAt = fusion.UpdatedAt.UTC()
	out.Close = weightedValue(h1.Close, m15.Close, fusion.PrimaryWeight, fusion.ConfirmWeight)
	out.Price = weightedValue(h1.Price, m15.Price, fusion.PrimaryWeight, fusion.ConfirmWeight)
	out.Ema21 = weightedValue(h1.Ema21, m15.Ema21, fusion.PrimaryWeight, fusion.ConfirmWeight)
	out.Ema55 = weightedValue(h1.Ema55, m15.Ema55, fusion.PrimaryWeight, fusion.ConfirmWeight)
	out.Atr = weightedValue(h1.Atr, m15.Atr, fusion.PrimaryWeight, fusion.ConfirmWeight)
	out.AtrPct = weightedValue(h1.AtrPct, m15.AtrPct, fusion.PrimaryWeight, fusion.ConfirmWeight)
	out.Adx = weightedValue(h1.Adx, m15.Adx, fusion.PrimaryWeight, fusion.ConfirmWeight)
	out.Rsi = weightedValue(h1.Rsi, m15.Rsi, fusion.PrimaryWeight, fusion.ConfirmWeight)
	out.Volume = weightedValue(h1.Volume, m15.Volume, fusion.PrimaryWeight, fusion.ConfirmWeight)
	out.TrendScore = weightedValue(h1.TrendScore, m15.TrendScore, fusion.PrimaryWeight, fusion.ConfirmWeight)
	out.Volatility = weightedValue(h1.Volatility, m15.Volatility, fusion.PrimaryWeight, fusion.ConfirmWeight)
	out.Healthy = h1.Healthy || m15.Healthy
	out.LastReason = fusion.FusedReason
	return featureengine.Normalize(out)
}

// weightedValue 只对有效数值做加权平均，避免缺失周期把结果错误拉低为零。
func weightedValue(primary, confirm, primaryWeight, confirmWeight float64) float64 {
	totalWeight := 0.0
	totalValue := 0.0
	if primary > 0 {
		totalValue += primary * primaryWeight
		totalWeight += primaryWeight
	}
	if confirm > 0 {
		totalValue += confirm * confirmWeight
		totalWeight += confirmWeight
	}
	if totalWeight <= 0 {
		return 0
	}
	return totalValue / totalWeight
}
