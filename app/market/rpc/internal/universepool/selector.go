package universepool

import (
	"strings"
	"time"
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
	cfg Config
}

// NewBasicSelector 创建一个基于静态候选池和健康度的最小 selector。
func NewBasicSelector(cfg Config) *BasicSelector {
	cfg = normalizeConfig(cfg)
	cfg = applyPreferredDefaults(cfg)
	return &BasicSelector{cfg: cfg}
}

// Evaluate 根据候选快照输出当前轮希望纳入动态币池的 symbol 集合。
func (s *BasicSelector) Evaluate(now time.Time, snapshots map[string]Snapshot) DesiredUniverse {
	out := DesiredUniverse{
		Symbols: make(map[string]DesiredUniverseSymbol),
	}
	if s == nil {
		return out
	}
	globalState, counts := s.detectGlobalState(now, snapshots)
	out.GlobalState = string(globalState)
	out.TrendCount = counts[selectorMarketStateTrend]
	out.RangeCount = counts[selectorMarketStateRange]
	out.BreakoutCount = counts[selectorMarketStateBreakout]
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

		score := s.scoreSnapshot(symbol, snap, globalState)
		item := DesiredUniverseSymbol{
			Symbol: symbol,
			Score:  score,
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

// isFresh 判断快照是否仍在可接受的新鲜度窗口内。
func (s *BasicSelector) isFresh(now time.Time, snap Snapshot) bool {
	if s == nil {
		return false
	}
	if snap.UpdatedAt.IsZero() {
		return false
	}
	window := s.cfg.EvaluateInterval * 3
	if window <= 0 {
		window = 90 * time.Second
	}
	return now.Sub(snap.UpdatedAt) <= window
}

// scoreSnapshot 对单个快照做最小打分，并在配置偏好时按全局状态优先放行特定币组。
func (s *BasicSelector) scoreSnapshot(symbol string, snap Snapshot, globalState selectorMarketState) float64 {
	if !snap.Healthy {
		return 0
	}
	if !s.hasPreferredSymbols(globalState) {
		// 如果当前状态没有配置偏好币列表，则维持原先的最小骨架行为。
		return 1
	}
	score := 0.55
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

// detectGlobalState 基于当前候选快照的多数状态，推断 market 侧动态币池应跟随的全局状态。
func (s *BasicSelector) detectGlobalState(now time.Time, snapshots map[string]Snapshot) (selectorMarketState, map[selectorMarketState]int) {
	if s == nil {
		return selectorMarketStateUnknown, nil
	}
	counts := map[selectorMarketState]int{
		selectorMarketStateTrend:    0,
		selectorMarketStateRange:    0,
		selectorMarketStateBreakout: 0,
	}
	for _, symbol := range s.cfg.CandidateSymbols {
		snap, ok := snapshots[symbol]
		if !ok || !snap.Healthy || !s.isFresh(now, snap) {
			continue
		}
		switch {
		case snap.AtrPct >= s.breakoutAtrPctMin():
			counts[selectorMarketStateBreakout]++
		case snap.AtrPct > 0 && snap.AtrPct <= s.rangeAtrPctMax():
			counts[selectorMarketStateRange]++
		case s.isTrendAligned(snap):
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
	return bestState, counts
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
		cfg.BreakoutPreferredSymbols = []string{"BTCUSDT", "SOLUSDT", "XRPUSDT"}
	}
	cfg = applyValidationModeDefaults(cfg)
	return cfg
}

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

func (s *BasicSelector) rangeAtrPctMax() float64 {
	if s != nil && strings.EqualFold(s.cfg.ValidationMode, "1m") {
		return 0.003
	}
	if s != nil && strings.EqualFold(s.cfg.ValidationMode, "5m") {
		return 0.0022
	}
	return 0.0015
}

func (s *BasicSelector) breakoutAtrPctMin() float64 {
	if s != nil && strings.EqualFold(s.cfg.ValidationMode, "1m") {
		return 0.0045
	}
	if s != nil && strings.EqualFold(s.cfg.ValidationMode, "5m") {
		return 0.005
	}
	return 0.006
}

func (s *BasicSelector) isTrendAligned(snap Snapshot) bool {
	if strings.EqualFold(s.cfg.ValidationMode, "1m") {
		return snap.Ema21 > snap.Ema55
	}
	if strings.EqualFold(s.cfg.ValidationMode, "5m") {
		return snap.LastPrice >= snap.Ema21 && snap.Ema21 > snap.Ema55
	}
	return snap.LastPrice > snap.Ema21 && snap.Ema21 > snap.Ema55
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
