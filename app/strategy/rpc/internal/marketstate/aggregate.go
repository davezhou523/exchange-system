package marketstate

import "time"

// Aggregate 把多个 symbol 的状态识别结果聚合成一轮全局市场状态。
func Aggregate(now time.Time, results map[string]Result) AggregateResult {
	out := AggregateResult{
		State:       MarketStateUnknown,
		Reason:      "no_results",
		UpdatedAt:   now.UTC(),
		StateCounts: make(map[string]int),
	}
	if len(results) == 0 {
		return out
	}

	counts := make(map[MarketState]int)
	confidenceSums := make(map[MarketState]float64)
	dominantSymbols := make(map[MarketState][]string)

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
		out.HealthyCount++
		counts[state]++
		confidenceSums[state] += result.Confidence
		dominantSymbols[state] = append(dominantSymbols[state], symbol)
	}

	bestState := MarketStateUnknown
	bestCount := 0
	bestConfidenceSum := 0.0
	for _, candidate := range []MarketState{
		MarketStateTrendUp,
		MarketStateTrendDown,
		MarketStateBreakout,
		MarketStateRange,
	} {
		count := counts[candidate]
		confidenceSum := confidenceSums[candidate]
		if count > bestCount || (count == bestCount && confidenceSum > bestConfidenceSum) {
			bestState = candidate
			bestCount = count
			bestConfidenceSum = confidenceSum
		}
	}
	if bestState == MarketStateUnknown || bestCount == 0 {
		out.Reason = "all_unknown"
		return out
	}

	out.State = bestState
	out.Confidence = bestConfidenceSum / float64(bestCount)
	out.Reason = "dominant_state"
	out.DominantSymbols = dominantSymbols[bestState]
	return out
}
