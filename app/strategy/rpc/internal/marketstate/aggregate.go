package marketstate

import (
	"time"

	"exchange-system/common/regimejudge"
)

// Aggregate 基于统一 Analysis 命中面和对外 Result 共同聚合一轮全局市场状态。
func Aggregate(now time.Time, analyses map[string]regimejudge.Analysis, results map[string]Result) AggregateResult {
	out := AggregateResult{
		State:       MarketStateUnknown,
		Reason:      "no_results",
		UpdatedAt:   now.UTC(),
		StateCounts: make(map[string]int),
		MatchCounts: make(map[string]int),
	}
	if len(results) == 0 && len(analyses) == 0 {
		return out
	}

	stateCounts := make(map[MarketState]int)
	matchCounts := make(map[MarketState]int)
	confidenceSums := make(map[MarketState]float64)
	stateSymbols := make(map[MarketState][]string)
	matchSymbols := make(map[MarketState][]string)

	for symbol, result := range results {
		state := result.State
		if state == "" {
			state = MarketStateUnknown
		}
		out.StateCounts[string(state)]++
		if state == MarketStateUnknown {
			out.UnknownCount++
			continue
		}
		stateCounts[state]++
		confidenceSums[state] += result.Confidence
		stateSymbols[state] = append(stateSymbols[state], symbol)
	}

	for symbol, analysis := range analyses {
		if !analysis.Healthy || !analysis.Fresh {
			continue
		}
		out.HealthyCount++
		if analysis.BreakoutMatch {
			matchCounts[MarketStateBreakout]++
			matchSymbols[MarketStateBreakout] = append(matchSymbols[MarketStateBreakout], symbol)
		}
		if analysis.RangeMatch {
			matchCounts[MarketStateRange]++
			matchSymbols[MarketStateRange] = append(matchSymbols[MarketStateRange], symbol)
		}
		if analysis.BullTrendStrict {
			matchCounts[MarketStateTrendUp]++
			matchSymbols[MarketStateTrendUp] = append(matchSymbols[MarketStateTrendUp], symbol)
		}
		if analysis.BearTrendStrict {
			matchCounts[MarketStateTrendDown]++
			matchSymbols[MarketStateTrendDown] = append(matchSymbols[MarketStateTrendDown], symbol)
		}
	}
	for state, count := range matchCounts {
		out.MatchCounts[string(state)] = count
	}

	bestState, bestMatchCount, bestConfidenceSum := selectDominantState(matchCounts, stateCounts, confidenceSums)
	if bestState == MarketStateUnknown || (bestMatchCount == 0 && len(results) == 0) {
		out.Reason = "all_unknown"
		return out
	}

	out.State = bestState
	if stateCounts[bestState] > 0 {
		out.Confidence = bestConfidenceSum / float64(stateCounts[bestState])
	}
	if bestMatchCount > 0 {
		out.Reason = "dominant_match_surface"
		out.DominantSymbols = matchSymbols[bestState]
		return out
	}
	out.Reason = "dominant_state"
	out.DominantSymbols = stateSymbols[bestState]
	return out
}

// selectDominantState 按命中面数量、对外状态数量和置信度和依次选择本轮主导状态。
func selectDominantState(matchCounts, stateCounts map[MarketState]int, confidenceSums map[MarketState]float64) (MarketState, int, float64) {
	bestState := MarketStateUnknown
	bestMatchCount := 0
	bestStateCount := 0
	bestConfidenceSum := 0.0
	for _, candidate := range []MarketState{
		MarketStateTrendUp,
		MarketStateTrendDown,
		MarketStateBreakout,
		MarketStateRange,
	} {
		matchCount := matchCounts[candidate]
		stateCount := stateCounts[candidate]
		confidenceSum := confidenceSums[candidate]
		if matchCount > bestMatchCount ||
			(matchCount == bestMatchCount && stateCount > bestStateCount) ||
			(matchCount == bestMatchCount && stateCount == bestStateCount && confidenceSum > bestConfidenceSum) {
			bestState = candidate
			bestMatchCount = matchCount
			bestStateCount = stateCount
			bestConfidenceSum = confidenceSum
		}
	}
	return bestState, bestMatchCount, bestConfidenceSum
}
