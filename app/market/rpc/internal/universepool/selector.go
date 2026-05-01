package universepool

import (
	"strings"
	"sync"
	"time"

	"exchange-system/common/featureengine"
	"exchange-system/common/symbolranker"
)

type selectorMarketState string

const (
	selectorMarketStateUnknown  selectorMarketState = "unknown"
	selectorMarketStateTrend    selectorMarketState = "trend"
	selectorMarketStateRange    selectorMarketState = "range"
	selectorMarketStateBreakout selectorMarketState = "breakout"
)

// BasicSelector 是动态币池的最小默认选择器实现。
type BasicSelector struct {
	mu                sync.Mutex
	cfg               Config
	ranker            *symbolranker.Ranker
	lastStableState   selectorMarketState
	lastStableStateAt time.Time
}

// NewBasicSelector 创建一个基于静态候选池和健康度的最小 selector。
func NewBasicSelector(cfg Config) *BasicSelector {
	cfg = normalizeConfig(cfg)
	cfg = applyPreferredDefaults(cfg)
	return &BasicSelector{
		cfg: cfg,
		ranker: symbolranker.New(symbolranker.Weights{
			TrendScore: 0.4,
			Volatility: 0.3,
			Volume:     0.3,
		}),
	}
}

// Evaluate 根据候选快照输出当前轮希望纳入动态币池的 symbol 集合。
func (s *BasicSelector) Evaluate(now time.Time, snapshots map[string]Snapshot) DesiredUniverse {
	out := DesiredUniverse{
		Symbols:    make(map[string]DesiredUniverseSymbol),
		StateVotes: make(map[string]StateVoteDetail),
	}
	if s == nil {
		return out
	}
	stableHint := s.currentStableState()
	globalState, counts, stateVotes := s.detectGlobalState(now, snapshots, stableHint)
	globalState = s.stabilizeGlobalState(now, globalState, snapshots)
	out.GlobalState = string(globalState)
	out.TrendCount = counts[selectorMarketStateTrend]
	out.RangeCount = counts[selectorMarketStateRange]
	out.BreakoutCount = counts[selectorMarketStateBreakout]
	out.StateVotes = stateVotes
	rankScores := s.buildRankScores(snapshots)
	allowSet := makeStringSet(s.cfg.AllowList)
	blockSet := makeStringSet(s.cfg.BlockList)
	for _, symbol := range s.cfg.CandidateSymbols {
		if symbol == "" {
			continue
		}
		if _, blocked := blockSet[symbol]; blocked {
			out.Symbols[symbol] = DesiredUniverseSymbol{
				Symbol:  symbol,
				Reason:  "block_list",
				Score:   0,
				Desired: false,
			}
			continue
		}
		if _, allowed := allowSet[symbol]; allowed {
			// AllowList 由 manager 直接维持 active，这里不重复输出 desired。
			continue
		}

		snap, ok := snapshots[symbol]
		if !ok {
			out.Symbols[symbol] = DesiredUniverseSymbol{
				Symbol:  symbol,
				Reason:  "no_snapshot",
				Score:   0,
				Desired: false,
			}
			continue
		}

		vote := stateVotes[symbol]
		if detail, ok := rankScores[symbol]; ok {
			vote.RankDetail = rankDetailFromSymbolScore(detail)
			stateVotes[symbol] = vote
			out.StateVotes[symbol] = vote
		}
		score := s.scoreSnapshot(symbol, snap, globalState, rankScores)
		item := DesiredUniverseSymbol{
			Symbol:    symbol,
			Score:     score,
			StateVote: vote,
		}
		if !s.isFresh(now, snap) {
			item.Desired = false
			item.Reason = "stale_snapshot"
			item.Score = 0
			out.Symbols[symbol] = item
			continue
		}
		if !snap.Healthy {
			item.Desired = false
			if snap.LastReason != "" {
				item.Reason = snap.LastReason
			} else {
				item.Reason = "unhealthy"
			}
			out.Symbols[symbol] = item
			continue
		}
		if score >= s.cfg.AddScoreThreshold {
			item.Desired = true
			if s.isPreferredSymbol(symbol, globalState) {
				item.Reason = "state_preferred_score_pass"
			} else {
				item.Reason = "score_pass"
			}
		} else {
			item.Desired = false
			if s.hasPreferredSymbols(globalState) {
				item.Reason = "state_filtered"
			} else {
				item.Reason = "score_below_add_threshold"
			}
		}
		out.Symbols[symbol] = item
	}
	return out
}

// isFresh 复用 manager 侧的新鲜度判定，确保 _meta 和 selector 对 freshness 的理解一致。
func (s *BasicSelector) isFresh(now time.Time, snap Snapshot) bool {
	if s == nil {
		return false
	}
	return isSnapshotFresh(s.cfg, now, snap)
}

// scoreSnapshot 对单个快照做最小打分，并在配置偏好时按全局状态优先放行特定币组。
func (s *BasicSelector) scoreSnapshot(symbol string, snap Snapshot, globalState selectorMarketState, rankScores map[string]symbolranker.SymbolScore) float64 {
	if !snap.Healthy {
		return 0
	}
	baseScore := rankScores[symbol].Score
	if !s.hasPreferredSymbols(globalState) {
		// 如果当前状态没有配置偏好币列表，则维持原先“健康样本可直接通过”的最小骨架行为。
		// 同时当 ranker 产出有效分时，也允许把该分数直接作为基础分透传给上层。
		if baseScore > 0 {
			return baseScore
		}
		return 1
	}
	// 基础分改由 Symbol Ranker 提供，再叠加原有状态与偏好加分，尽量保持旧策略语义稳定。
	score := 0.55 + baseScore*0.05
	switch globalState {
	case selectorMarketStateTrend:
		if s.isTrendAligned(snap) {
			score += 0.10
		}
	case selectorMarketStateRange:
		if snap.AtrPct > 0 && snap.AtrPct <= s.rangeAtrPctMax() {
			score += 0.10
		}
	case selectorMarketStateBreakout:
		if snap.AtrPct >= s.breakoutAtrPctMin() {
			score += 0.10
		}
	}
	if s.isPreferredSymbol(symbol, globalState) {
		score += 0.25
	}
	if score > 1 {
		score = 1
	}
	return score
}

// buildRankScores 把当前候选快照转换为统一特征后交给 Symbol Ranker，产出每个 symbol 的基础分。
func (s *BasicSelector) buildRankScores(snapshots map[string]Snapshot) map[string]symbolranker.SymbolScore {
	out := make(map[string]symbolranker.SymbolScore, len(s.cfg.CandidateSymbols))
	if s == nil || s.ranker == nil || len(s.cfg.CandidateSymbols) == 0 {
		return out
	}
	timeframe := validationSnapshotIntervalName(s.cfg)
	features := make([]featureengine.Features, 0, len(s.cfg.CandidateSymbols))
	for _, symbol := range s.cfg.CandidateSymbols {
		snap, ok := snapshots[symbol]
		if !ok {
			continue
		}
		item := featureengine.BuildFromSnapshotValues(
			symbol,
			timeframe,
			snap.LastPrice,
			snap.Ema21,
			snap.Ema55,
			snap.Atr,
			snap.Rsi,
			snap.Volume24h,
			false,
			true,
			true,
			snap.UpdatedAt,
		)
		item.Healthy = snap.Healthy
		if snap.LastReason != "" {
			item.LastReason = snap.LastReason
		}
		features = append(features, item)
	}
	scores := s.ranker.RankSymbols(features)
	for _, item := range scores {
		out[item.Symbol] = item
	}
	return out
}

// rankDetailFromSymbolScore 把公共排名结果转换成日志层稳定使用的明细结构。
func rankDetailFromSymbolScore(score symbolranker.SymbolScore) *RankDetail {
	return &RankDetail{
		BaseScore:       score.Score,
		TrendScore:      score.TrendScore,
		VolatilityScore: score.VolatilityScore,
		VolumeScore:     score.VolumeScore,
		RawTrendScore:   score.RawTrendScore,
		RawVolatility:   score.RawVolatility,
		RawVolume:       score.RawVolume,
	}
}

// detectGlobalState 基于当前候选快照的多数状态，推断 market 侧动态币池应跟随的全局状态，并保留每个候选币的投票证据。
func (s *BasicSelector) detectGlobalState(now time.Time, snapshots map[string]Snapshot, stableHint selectorMarketState) (selectorMarketState, map[selectorMarketState]int, map[string]StateVoteDetail) {
	if s == nil {
		return selectorMarketStateUnknown, nil, nil
	}
	counts := map[selectorMarketState]int{
		selectorMarketStateTrend:    0,
		selectorMarketStateRange:    0,
		selectorMarketStateBreakout: 0,
	}
	stateVotes := make(map[string]StateVoteDetail, len(s.cfg.CandidateSymbols))
	for _, symbol := range s.cfg.CandidateSymbols {
		snap, ok := snapshots[symbol]
		if !ok {
			continue
		}
		vote := s.buildStateVoteDetail(now, snap, stableHint)
		stateVotes[symbol] = vote
		if !vote.Healthy || !vote.Fresh {
			continue
		}
		switch selectorMarketState(vote.ClassifiedState) {
		case selectorMarketStateBreakout:
			counts[selectorMarketStateBreakout]++
		case selectorMarketStateRange:
			counts[selectorMarketStateRange]++
		case selectorMarketStateTrend:
			counts[selectorMarketStateTrend]++
		}
	}
	bestState := selectorMarketStateUnknown
	bestCount := 0
	for _, candidate := range []selectorMarketState{
		selectorMarketStateTrend,
		selectorMarketStateBreakout,
		selectorMarketStateRange,
	} {
		if counts[candidate] > bestCount {
			bestState = candidate
			bestCount = counts[candidate]
		}
	}
	return bestState, counts, stateVotes
}

// currentStableState 返回当前记住的稳定全局状态，供本轮判定决定是否放宽退出阈值。
func (s *BasicSelector) currentStableState() selectorMarketState {
	if s == nil {
		return selectorMarketStateUnknown
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.lastStableState
}

// classifySnapshot 按当前验证模式配置的优先级分类单个快照，并在已有稳定状态时启用退出阈值。
func (s *BasicSelector) classifySnapshot(snap Snapshot, stableHint selectorMarketState) selectorMarketState {
	breakoutMatch := s.isBreakoutMatch(snap, stableHint == selectorMarketStateBreakout)
	rangeMatch := s.isRangeMatch(snap, stableHint == selectorMarketStateRange)
	trendMatch := s.isTrendMatch(snap, stableHint == selectorMarketStateTrend)
	for _, state := range s.classificationPriority() {
		switch state {
		case selectorMarketStateBreakout:
			if breakoutMatch {
				return selectorMarketStateBreakout
			}
		case selectorMarketStateTrend:
			if trendMatch {
				return selectorMarketStateTrend
			}
		case selectorMarketStateRange:
			if rangeMatch {
				return selectorMarketStateRange
			}
		}
	}
	return selectorMarketStateUnknown
}

// buildStateVoteDetail 生成单个候选币在本轮全局状态投票中的证据，方便日志直接解释“为什么判成某个状态”。
func (s *BasicSelector) buildStateVoteDetail(now time.Time, snap Snapshot, stableHint selectorMarketState) StateVoteDetail {
	detail := StateVoteDetail{
		Fresh:     s.isFresh(now, snap),
		Healthy:   snap.Healthy,
		LastPrice: snap.LastPrice,
		Ema21:     snap.Ema21,
		Ema55:     snap.Ema55,
		AtrPct:    snap.AtrPct,
	}
	rangeMax := s.rangeAtrPctMax()
	if stableHint == selectorMarketStateRange {
		rangeMax = s.rangeAtrPctExitMax()
	}
	breakoutMin := s.breakoutAtrPctMin()
	if stableHint == selectorMarketStateBreakout {
		breakoutMin = s.breakoutAtrPctExitMin()
	}
	detail.RangeAtrPctMax = rangeMax
	detail.BreakoutAtrPctMin = breakoutMin
	detail.TrendAligned = s.isTrendAligned(snap)
	detail.BreakoutMatch = s.isBreakoutMatch(snap, stableHint == selectorMarketStateBreakout)
	detail.RangeMatch = s.isRangeMatch(snap, stableHint == selectorMarketStateRange)
	detail.TrendMatch = s.isTrendMatch(snap, stableHint == selectorMarketStateTrend)
	switch {
	case !detail.Healthy:
		detail.ClassifiedReason = "unhealthy_snapshot"
		detail.ClassifiedReasonZh = "快照不健康，未参与全局状态投票"
	case !detail.Fresh:
		detail.ClassifiedReason = "stale_snapshot"
		detail.ClassifiedReasonZh = "快照不新鲜，未参与全局状态投票"
	default:
		detail.ClassifiedState, detail.ClassifiedReason, detail.ClassifiedReasonZh = s.resolveClassification(detail)
	}
	return detail
}

// classificationPriority 返回当前验证模式下的状态分类优先级。
func (s *BasicSelector) classificationPriority() []selectorMarketState {
	if s != nil && strings.EqualFold(s.cfg.ValidationMode, "1m") {
		// 1m 更强调快速暴露趋势切换，因此在 breakout 之后优先判 trend，再回落到 range。
		return []selectorMarketState{
			selectorMarketStateBreakout,
			selectorMarketStateTrend,
			selectorMarketStateRange,
		}
	}
	return []selectorMarketState{
		selectorMarketStateBreakout,
		selectorMarketStateRange,
		selectorMarketStateTrend,
	}
}

// resolveClassification 根据当前验证模式的优先级，输出单个快照的最终分类及原因。
func (s *BasicSelector) resolveClassification(detail StateVoteDetail) (string, string, string) {
	for _, state := range s.classificationPriority() {
		switch state {
		case selectorMarketStateBreakout:
			if detail.BreakoutMatch {
				return string(selectorMarketStateBreakout), "breakout_match", "ATR 达到 breakout 阈值，优先判为 breakout"
			}
		case selectorMarketStateTrend:
			if detail.TrendMatch {
				if detail.RangeMatch {
					return string(selectorMarketStateTrend), "trend_match_precedes_range", "均线满足趋势条件，按 breakout>trend>range 顺序优先判为 trend"
				}
				return string(selectorMarketStateTrend), "trend_match", "未命中 breakout/range，且均线满足趋势条件，判为 trend"
			}
		case selectorMarketStateRange:
			if detail.RangeMatch {
				if detail.TrendMatch {
					return string(selectorMarketStateRange), "range_match_precedes_trend", "ATR 命中 range 阈值，按 breakout>range>trend 顺序优先判为 range"
				}
				return string(selectorMarketStateRange), "range_match", "ATR 落在 range 阈值内，判为 range"
			}
		}
	}
	return "", "no_state_match", "未命中 breakout/range/trend 规则"
}

// stabilizeGlobalState 在本轮无法明确分类时，短暂沿用上一次稳定状态，减少 fresh snapshot 之间的抖动。
func (s *BasicSelector) stabilizeGlobalState(now time.Time, detected selectorMarketState, snapshots map[string]Snapshot) selectorMarketState {
	if s == nil {
		return detected
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if detected != selectorMarketStateUnknown {
		s.lastStableState = detected
		s.lastStableStateAt = now
		return detected
	}
	if !s.hasFreshSnapshots(now, snapshots) {
		return detected
	}
	if s.lastStableState == "" || s.lastStableState == selectorMarketStateUnknown {
		return detected
	}
	if now.Sub(s.lastStableStateAt) > s.globalStateHoldWindow() {
		return detected
	}
	return s.lastStableState
}

// hasFreshSnapshots 判断当前是否仍有可参与决策的新鲜快照，避免在数据断流时盲目保持旧状态。
func (s *BasicSelector) hasFreshSnapshots(now time.Time, snapshots map[string]Snapshot) bool {
	for _, symbol := range s.cfg.CandidateSymbols {
		snap, ok := snapshots[symbol]
		if !ok || !snap.Healthy || !s.isFresh(now, snap) {
			continue
		}
		return true
	}
	return false
}

// globalStateHoldWindow 依据验证周期生成一个最小保持窗口，让 5m 验证时比 1m 更稳。
func (s *BasicSelector) globalStateHoldWindow() time.Duration {
	base := validationSnapshotIntervalDuration(s.cfg)
	if base <= 0 {
		base = s.cfg.EvaluateInterval
	}
	if base <= 0 {
		base = time.Minute
	}
	extra := s.cfg.EvaluateInterval
	if extra <= 0 {
		extra = 30 * time.Second
	}
	return base + extra
}

// hasPreferredSymbols 判断当前全局状态是否配置了偏好币列表。
func (s *BasicSelector) hasPreferredSymbols(globalState selectorMarketState) bool {
	if s == nil {
		return false
	}
	switch globalState {
	case selectorMarketStateTrend:
		return len(s.cfg.TrendPreferredSymbols) > 0
	case selectorMarketStateRange:
		return len(s.cfg.RangePreferredSymbols) > 0
	case selectorMarketStateBreakout:
		return len(s.cfg.BreakoutPreferredSymbols) > 0
	default:
		return false
	}
}

// isPreferredSymbol 判断某个 symbol 是否属于当前全局状态的偏好币列表。
func (s *BasicSelector) isPreferredSymbol(symbol string, globalState selectorMarketState) bool {
	if s == nil || symbol == "" {
		return false
	}
	_, ok := s.preferredSet(globalState)[symbol]
	return ok
}

// preferredSet 返回当前全局状态对应的偏好币集合。
func (s *BasicSelector) preferredSet(globalState selectorMarketState) map[string]struct{} {
	if s == nil {
		return nil
	}
	switch globalState {
	case selectorMarketStateTrend:
		return makeStringSet(s.cfg.TrendPreferredSymbols)
	case selectorMarketStateRange:
		return makeStringSet(s.cfg.RangePreferredSymbols)
	case selectorMarketStateBreakout:
		return makeStringSet(s.cfg.BreakoutPreferredSymbols)
	default:
		return nil
	}
}

// applyPreferredDefaults 为状态驱动选币补齐最小默认偏好，保持 demo 即开即有辨识度。
func applyPreferredDefaults(cfg Config) Config {
	if len(cfg.TrendPreferredSymbols) == 0 {
		cfg.TrendPreferredSymbols = []string{"ETHUSDT", "SOLUSDT", "BNBUSDT"}
	}
	if len(cfg.RangePreferredSymbols) == 0 {
		cfg.RangePreferredSymbols = []string{"BTCUSDT", "ETHUSDT"}
	}
	if len(cfg.BreakoutPreferredSymbols) == 0 {
		cfg.BreakoutPreferredSymbols = []string{"BTCUSDT", "DOGEUSDT", "PEPEUSDT"}
	}
	cfg = applyValidationModeDefaults(cfg)
	return cfg
}

// applyValidationModeDefaults 为 1m/5m 验证模式注入更合适的评估频率和预热要求。
func applyValidationModeDefaults(cfg Config) Config {
	switch strings.ToLower(strings.TrimSpace(cfg.ValidationMode)) {
	case "1m":
		if cfg.EvaluateInterval <= 0 || cfg.EvaluateInterval > 10*time.Second {
			cfg.EvaluateInterval = 10 * time.Second
		}
		cfg.Warmup.Enabled = true
		if cfg.Warmup.Min1mBars <= 0 {
			cfg.Warmup.Min1mBars = 1
		}
		cfg.Warmup.Require15mReady = false
		cfg.Warmup.Require1hReady = false
		cfg.Warmup.Require4hReady = false
		cfg.Warmup.RequireIndicatorsReady = true
	case "5m":
		if cfg.EvaluateInterval <= 0 || cfg.EvaluateInterval > 30*time.Second {
			cfg.EvaluateInterval = 30 * time.Second
		}
		cfg.Warmup.Enabled = true
		if cfg.Warmup.Min1mBars <= 0 || cfg.Warmup.Min1mBars < 5 {
			cfg.Warmup.Min1mBars = 5
		}
		cfg.Warmup.Require15mReady = false
		cfg.Warmup.Require1hReady = false
		cfg.Warmup.Require4hReady = false
		cfg.Warmup.RequireIndicatorsReady = true
	}
	return cfg
}

// rangeAtrPctMax 返回进入 range 状态时使用的 ATR 百分比上限。
func (s *BasicSelector) rangeAtrPctMax() float64 {
	if s != nil && strings.EqualFold(s.cfg.ValidationMode, "1m") {
		// 1m 验证模式更关注尽快看到状态切换，因此把震荡阈值收紧到更接近实盘分界的位置。
		return 0.0004
	}
	if s != nil && strings.EqualFold(s.cfg.ValidationMode, "5m") {
		return 0.0022
	}
	return 0.0015
}

// rangeAtrPctExitMax 返回退出 range 状态前允许的更宽 ATR 百分比上限，用于抑制边界抖动。
func (s *BasicSelector) rangeAtrPctExitMax() float64 {
	if s != nil && strings.EqualFold(s.cfg.ValidationMode, "1m") {
		// 1m 验证模式把退出阈值收得更近一些，减少 trend 边界样本被 range 迟滞重新吞回去。
		return 0.0005
	}
	if s != nil && strings.EqualFold(s.cfg.ValidationMode, "5m") {
		return 0.0028
	}
	return 0.002
}

// breakoutAtrPctMin 返回进入 breakout 状态时使用的 ATR 百分比下限。
func (s *BasicSelector) breakoutAtrPctMin() float64 {
	if s != nil && s.cfg.BreakoutAtrPctMin > 0 {
		return s.cfg.BreakoutAtrPctMin
	}
	if s != nil && strings.EqualFold(s.cfg.ValidationMode, "1m") {
		return 0.0045
	}
	if s != nil && strings.EqualFold(s.cfg.ValidationMode, "5m") {
		return 0.005
	}
	return 0.006
}

// breakoutAtrPctExitMin 返回退出 breakout 状态前允许的更低 ATR 百分比下限，用于形成迟滞。
func (s *BasicSelector) breakoutAtrPctExitMin() float64 {
	if s != nil && s.cfg.BreakoutAtrPctExitMin > 0 {
		return s.cfg.BreakoutAtrPctExitMin
	}
	if s != nil && strings.EqualFold(s.cfg.ValidationMode, "1m") {
		return 0.004
	}
	if s != nil && strings.EqualFold(s.cfg.ValidationMode, "5m") {
		return 0.0045
	}
	return 0.0055
}

// isTrendAligned 判断快照是否满足不同验证模式下的趋势入场条件。
func (s *BasicSelector) isTrendAligned(snap Snapshot) bool {
	if strings.EqualFold(s.cfg.ValidationMode, "1m") {
		return snap.Ema21 > snap.Ema55
	}
	if strings.EqualFold(s.cfg.ValidationMode, "5m") {
		return snap.LastPrice >= snap.Ema21 && snap.Ema21 > snap.Ema55
	}
	return snap.LastPrice > snap.Ema21 && snap.Ema21 > snap.Ema55
}

// isRangeMatch 判断快照是否命中 range，并在保持旧状态时切换到更宽的退出阈值。
func (s *BasicSelector) isRangeMatch(snap Snapshot, allowExitThreshold bool) bool {
	if snap.AtrPct <= 0 {
		return false
	}
	max := s.rangeAtrPctMax()
	if allowExitThreshold {
		max = s.rangeAtrPctExitMax()
	}
	return snap.AtrPct <= max
}

// isBreakoutMatch 判断快照是否命中 breakout，并在保持旧状态时切换到更宽的退出阈值。
func (s *BasicSelector) isBreakoutMatch(snap Snapshot, allowExitThreshold bool) bool {
	min := s.breakoutAtrPctMin()
	if allowExitThreshold {
		min = s.breakoutAtrPctExitMin()
	}
	return snap.AtrPct >= min
}

// isTrendMatch 判断快照是否命中 trend，并在退出阶段放宽为均线仍保持多头排列即可。
func (s *BasicSelector) isTrendMatch(snap Snapshot, allowExitThreshold bool) bool {
	if allowExitThreshold {
		return snap.Ema21 > snap.Ema55
	}
	return s.isTrendAligned(snap)
}

// makeStringSet 把字符串切片转成集合，方便做白名单和黑名单判断。
func makeStringSet(items []string) map[string]struct{} {
	out := make(map[string]struct{}, len(items))
	for _, item := range items {
		if item == "" {
			continue
		}
		out[item] = struct{}{}
	}
	return out
}
