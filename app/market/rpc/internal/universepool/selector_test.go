package universepool

import (
	"testing"
	"time"
)

func TestBasicSelectorEvaluatePrefersTrendSymbols(t *testing.T) {
	now := time.Date(2026, 4, 26, 12, 0, 0, 0, time.UTC)
	selector := NewBasicSelector(Config{
		CandidateSymbols:         []string{"ETHUSDT", "SOLUSDT", "XRPUSDT"},
		TrendPreferredSymbols:    []string{"ETHUSDT", "SOLUSDT"},
		RangePreferredSymbols:    []string{"BTCUSDT", "ETHUSDT"},
		BreakoutPreferredSymbols: []string{"BTCUSDT", "SOLUSDT"},
		EvaluateInterval:         30 * time.Second,
		AddScoreThreshold:        0.75,
	})

	snapshots := map[string]Snapshot{
		"ETHUSDT": {Symbol: "ETHUSDT", UpdatedAt: now.Add(-10 * time.Second), LastPrice: 100, Ema21: 99, Ema55: 98, Atr: 0.2, AtrPct: 0.002, Healthy: true},
		"SOLUSDT": {Symbol: "SOLUSDT", UpdatedAt: now.Add(-10 * time.Second), LastPrice: 100, Ema21: 99, Ema55: 98, Atr: 0.2, AtrPct: 0.002, Healthy: true},
		"XRPUSDT": {Symbol: "XRPUSDT", UpdatedAt: now.Add(-10 * time.Second), LastPrice: 100, Ema21: 99, Ema55: 98, Atr: 0.2, AtrPct: 0.002, Healthy: true},
	}

	got := selector.Evaluate(now, snapshots)
	if got.GlobalState != "trend" || got.TrendCount != 3 || got.RangeCount != 0 || got.BreakoutCount != 0 {
		t.Fatalf("global state = %s trend=%d range=%d breakout=%d, want trend/3/0/0", got.GlobalState, got.TrendCount, got.RangeCount, got.BreakoutCount)
	}
	if !got.Symbols["ETHUSDT"].Desired || got.Symbols["ETHUSDT"].Reason != "state_preferred_score_pass" {
		t.Fatalf("ETHUSDT = %+v, want desired state_preferred_score_pass", got.Symbols["ETHUSDT"])
	}
	if !got.Symbols["SOLUSDT"].Desired || got.Symbols["SOLUSDT"].Reason != "state_preferred_score_pass" {
		t.Fatalf("SOLUSDT = %+v, want desired state_preferred_score_pass", got.Symbols["SOLUSDT"])
	}
	if got.Symbols["XRPUSDT"].Desired || got.Symbols["XRPUSDT"].Reason != "state_filtered" {
		t.Fatalf("XRPUSDT = %+v, want filtered", got.Symbols["XRPUSDT"])
	}
}

func TestBasicSelectorEvaluateFallsBackWhenNoPreferredConfig(t *testing.T) {
	now := time.Date(2026, 4, 26, 12, 0, 0, 0, time.UTC)
	selector := &BasicSelector{cfg: normalizeConfig(Config{
		CandidateSymbols:  []string{"ETHUSDT"},
		EvaluateInterval:  30 * time.Second,
		AddScoreThreshold: 0.75,
	})}

	got := selector.Evaluate(now, map[string]Snapshot{
		"ETHUSDT": {
			Symbol: "ETHUSDT", UpdatedAt: now.Add(-10 * time.Second),
			LastPrice: 100, Healthy: true,
		},
	})
	if !got.Symbols["ETHUSDT"].Desired || got.Symbols["ETHUSDT"].Reason != "score_pass" {
		t.Fatalf("ETHUSDT = %+v, want desired score_pass", got.Symbols["ETHUSDT"])
	}
}

func TestBasicSelectorEvaluateValidationMode1mUsesTighterRangeThreshold(t *testing.T) {
	now := time.Date(2026, 4, 26, 12, 0, 0, 0, time.UTC)
	selector := NewBasicSelector(Config{
		ValidationMode:        "1m",
		CandidateSymbols:      []string{"ETHUSDT", "SOLUSDT", "XRPUSDT"},
		TrendPreferredSymbols: []string{"ETHUSDT", "SOLUSDT"},
		RangePreferredSymbols: []string{"BTCUSDT", "ETHUSDT"},
		EvaluateInterval:      30 * time.Second,
		AddScoreThreshold:     0.75,
	})

	got := selector.Evaluate(now, map[string]Snapshot{
		"ETHUSDT": {Symbol: "ETHUSDT", UpdatedAt: now.Add(-5 * time.Second), LastPrice: 100, Ema21: 100.3, Ema55: 100.0, AtrPct: 0.00045, Healthy: true},
		"SOLUSDT": {Symbol: "SOLUSDT", UpdatedAt: now.Add(-5 * time.Second), LastPrice: 100, Ema21: 100.4, Ema55: 100.0, AtrPct: 0.00055, Healthy: true},
		"XRPUSDT": {Symbol: "XRPUSDT", UpdatedAt: now.Add(-5 * time.Second), LastPrice: 100, Ema21: 100.0, Ema55: 100.0, AtrPct: 0, Healthy: true},
	})
	if got.GlobalState != "trend" || got.TrendCount != 2 || got.RangeCount != 0 {
		t.Fatalf("global state = %s trend=%d range=%d, want trend/2/0", got.GlobalState, got.TrendCount, got.RangeCount)
	}
	if got.Symbols["ETHUSDT"].Reason != "state_preferred_score_pass" || !got.Symbols["ETHUSDT"].Desired {
		t.Fatalf("ETHUSDT = %+v, want desired state_preferred_score_pass", got.Symbols["ETHUSDT"])
	}
	if got.Symbols["SOLUSDT"].Reason != "state_preferred_score_pass" || !got.Symbols["SOLUSDT"].Desired {
		t.Fatalf("SOLUSDT = %+v, want desired state_preferred_score_pass", got.Symbols["SOLUSDT"])
	}
	if got.Symbols["XRPUSDT"].Reason != "state_filtered" || got.Symbols["XRPUSDT"].Desired {
		t.Fatalf("XRPUSDT = %+v, want filtered by trend preference", got.Symbols["XRPUSDT"])
	}
}

// TestBasicSelectorEvaluateRecordsTrendEvidenceInStateVote 验证 1m 模式下 trend 优先于 range 时，日志证据会保留双命中但最终归 trend 的原因。
func TestBasicSelectorEvaluateRecordsTrendEvidenceInStateVote(t *testing.T) {
	now := time.Date(2026, 5, 1, 6, 16, 0, 0, time.UTC)
	selector := NewBasicSelector(Config{
		ValidationMode:        "1m",
		CandidateSymbols:      []string{"BTCUSDT", "ETHUSDT", "BNBUSDT", "SOLUSDT", "XRPUSDT"},
		RangePreferredSymbols: []string{"BTCUSDT", "ETHUSDT"},
		EvaluateInterval:      10 * time.Second,
		AddScoreThreshold:     0.75,
	})

	got := selector.Evaluate(now, map[string]Snapshot{
		"BTCUSDT": {Symbol: "BTCUSDT", UpdatedAt: now.Add(-5 * time.Second), LastPrice: 77134, Ema21: 77087.9, Ema55: 77063.11, AtrPct: 0.000339, Healthy: true},
		"ETHUSDT": {Symbol: "ETHUSDT", UpdatedAt: now.Add(-5 * time.Second), LastPrice: 2284.56, Ema21: 2282.68, Ema55: 2281.29, AtrPct: 0.000390, Healthy: true},
		"BNBUSDT": {Symbol: "BNBUSDT", UpdatedAt: now.Add(-5 * time.Second), LastPrice: 618.5, Ema21: 618.12, Ema55: 618.05, AtrPct: 0.000275, Healthy: true},
		"SOLUSDT": {Symbol: "SOLUSDT", UpdatedAt: now.Add(-5 * time.Second), LastPrice: 84.12, Ema21: 84.01, Ema55: 83.93, AtrPct: 0.000390, Healthy: true},
		"XRPUSDT": {Symbol: "XRPUSDT", UpdatedAt: now.Add(-5 * time.Second), LastPrice: 1.38, Ema21: 1.38, Ema55: 1.38, AtrPct: 0, Healthy: true},
	})
	if got.GlobalState != "trend" || got.TrendCount != 4 || got.RangeCount != 0 {
		t.Fatalf("global state = %s trend=%d range=%d, want trend with 4 trend votes", got.GlobalState, got.TrendCount, got.RangeCount)
	}

	vote := got.StateVotes["SOLUSDT"]
	if vote.ClassifiedState != "trend" {
		t.Fatalf("SOLUSDT classified_state = %s, want trend", vote.ClassifiedState)
	}
	if vote.ClassifiedReason != "trend_match_precedes_range" {
		t.Fatalf("SOLUSDT classified_reason = %s, want trend_match_precedes_range", vote.ClassifiedReason)
	}
	if !vote.RangeMatch || !vote.TrendMatch {
		t.Fatalf("SOLUSDT vote = %+v, want both range_match and trend_match true", vote)
	}
	if got.Symbols["SOLUSDT"].StateVote.ClassifiedState != "trend" {
		t.Fatalf("SOLUSDT decision vote = %+v, want trend evidence attached", got.Symbols["SOLUSDT"].StateVote)
	}
}

// TestBasicSelectorEvaluateValidationMode1mPrefersTrendOverRange 验证 1m 模式会把同时命中 trend/range 的样本优先判成 trend。
func TestBasicSelectorEvaluateValidationMode1mPrefersTrendOverRange(t *testing.T) {
	now := time.Date(2026, 5, 1, 11, 22, 0, 0, time.UTC)
	selector := NewBasicSelector(Config{
		ValidationMode:        "1m",
		CandidateSymbols:      []string{"BNBUSDT", "BTCUSDT", "SOLUSDT"},
		TrendPreferredSymbols: []string{"BNBUSDT", "BTCUSDT", "SOLUSDT"},
		RangePreferredSymbols: []string{"BTCUSDT"},
		EvaluateInterval:      10 * time.Second,
		AddScoreThreshold:     0.75,
	})

	got := selector.Evaluate(now, map[string]Snapshot{
		"BNBUSDT": {Symbol: "BNBUSDT", UpdatedAt: now.Add(-5 * time.Second), LastPrice: 618.95, Ema21: 618.23, Ema55: 618.13, AtrPct: 0.00032312787785766214, Healthy: true},
		"BTCUSDT": {Symbol: "BTCUSDT", UpdatedAt: now.Add(-5 * time.Second), LastPrice: 77489.4, Ema21: 77319.48, Ema55: 77290.04, AtrPct: 0.0004949064001011752, Healthy: true},
		"SOLUSDT": {Symbol: "SOLUSDT", UpdatedAt: now.Add(-5 * time.Second), LastPrice: 84.07, Ema21: 83.95, Ema55: 83.96, AtrPct: 0.0005947424765076723, Healthy: true},
	})

	if got.GlobalState != "trend" || got.TrendCount != 2 || got.RangeCount != 0 {
		t.Fatalf("global state = %s trend=%d range=%d, want trend/2/0", got.GlobalState, got.TrendCount, got.RangeCount)
	}
	if got.StateVotes["BNBUSDT"].ClassifiedState != "trend" || got.StateVotes["BNBUSDT"].ClassifiedReason != "trend_match_precedes_range" {
		t.Fatalf("BNBUSDT vote = %+v, want trend precedence over range", got.StateVotes["BNBUSDT"])
	}
	if got.StateVotes["BTCUSDT"].ClassifiedState != "trend" || got.StateVotes["BTCUSDT"].ClassifiedReason != "trend_match" {
		t.Fatalf("BTCUSDT vote = %+v, want pure trend_match without range overlap", got.StateVotes["BTCUSDT"])
	}
	if got.StateVotes["SOLUSDT"].ClassifiedState != "" || got.StateVotes["SOLUSDT"].ClassifiedReason != "no_state_match" {
		t.Fatalf("SOLUSDT vote = %+v, want no_state_match", got.StateVotes["SOLUSDT"])
	}
	if got.StateVotes["BNBUSDT"].RankDetail == nil {
		t.Fatalf("BNBUSDT rank_detail = nil, want populated")
	}
	if got.StateVotes["BNBUSDT"].RankDetail.RawTrendScore <= 0 {
		t.Fatalf("BNBUSDT rank_detail = %+v, want raw_trend_score > 0", got.StateVotes["BNBUSDT"].RankDetail)
	}
	if got.Symbols["BNBUSDT"].StateVote.RankDetail == nil {
		t.Fatalf("BNBUSDT decision vote rank_detail = nil, want populated")
	}
}

func TestBasicSelectorEvaluateValidationMode5mUsesSteadierThresholds(t *testing.T) {
	now := time.Date(2026, 4, 26, 12, 0, 0, 0, time.UTC)
	selector := NewBasicSelector(Config{
		ValidationMode:        "5m",
		CandidateSymbols:      []string{"BTCUSDT", "ETHUSDT", "XRPUSDT"},
		RangePreferredSymbols: []string{"BTCUSDT", "ETHUSDT"},
		EvaluateInterval:      30 * time.Second,
		AddScoreThreshold:     0.75,
	})

	got := selector.Evaluate(now, map[string]Snapshot{
		"BTCUSDT": {Symbol: "BTCUSDT", UpdatedAt: now.Add(-5 * time.Second), LastPrice: 100, Ema21: 100.1, Ema55: 100.0, AtrPct: 0.0021, Healthy: true},
		"ETHUSDT": {Symbol: "ETHUSDT", UpdatedAt: now.Add(-5 * time.Second), LastPrice: 100, Ema21: 100.1, Ema55: 100.0, AtrPct: 0.0020, Healthy: true},
		"XRPUSDT": {Symbol: "XRPUSDT", UpdatedAt: now.Add(-5 * time.Second), LastPrice: 100, Ema21: 100.1, Ema55: 100.0, AtrPct: 0.0021, Healthy: true},
	})
	if got.GlobalState != "range" || got.RangeCount != 3 {
		t.Fatalf("global state = %s range=%d, want range/3", got.GlobalState, got.RangeCount)
	}
}

func TestBasicSelectorEvaluateUsesConfiguredBreakoutThreshold(t *testing.T) {
	now := time.Date(2026, 5, 1, 14, 0, 0, 0, time.UTC)
	selector := NewBasicSelector(Config{
		ValidationMode:        "1m",
		CandidateSymbols:      []string{"BTCUSDT", "SOLUSDT"},
		BreakoutAtrPctMin:     0.0010,
		BreakoutAtrPctExitMin: 0.0009,
	})

	got := selector.Evaluate(now, map[string]Snapshot{
		"BTCUSDT": {
			Symbol:     "BTCUSDT",
			UpdatedAt:  now,
			LastPrice:  78000,
			Ema21:      77950,
			Ema55:      77800,
			Atr:        95,
			AtrPct:     95 / 78000.0,
			Healthy:    true,
			LastReason: "fresh_1m",
		},
		"SOLUSDT": {
			Symbol:     "SOLUSDT",
			UpdatedAt:  now,
			LastPrice:  84,
			Ema21:      83.8,
			Ema55:      83.6,
			Atr:        0.07,
			AtrPct:     0.07 / 84.0,
			Healthy:    true,
			LastReason: "fresh_1m",
		},
	})

	if got.StateVotes["BTCUSDT"].ClassifiedState != "breakout" {
		t.Fatalf("BTCUSDT classified_state = %s, want breakout", got.StateVotes["BTCUSDT"].ClassifiedState)
	}
	if got.BreakoutCount != 1 {
		t.Fatalf("breakout_count = %d, want 1", got.BreakoutCount)
	}
	if got.StateVotes["BTCUSDT"].BreakoutAtrPctMin != 0.0010 {
		t.Fatalf("BTCUSDT breakout_atr_pct_min = %v, want 0.0010", got.StateVotes["BTCUSDT"].BreakoutAtrPctMin)
	}
}

func TestBasicSelectorEvaluateHoldsPreviousStateWhenFreshInputsRemain(t *testing.T) {
	now := time.Date(2026, 5, 1, 1, 1, 0, 0, time.UTC)
	selector := NewBasicSelector(Config{
		ValidationMode:        "5m",
		CandidateSymbols:      []string{"BTCUSDT", "ETHUSDT", "XRPUSDT"},
		RangePreferredSymbols: []string{"BTCUSDT", "ETHUSDT"},
		EvaluateInterval:      30 * time.Second,
		AddScoreThreshold:     0.75,
	})

	first := selector.Evaluate(now, map[string]Snapshot{
		"BTCUSDT": {Symbol: "BTCUSDT", UpdatedAt: now.Add(-5 * time.Second), LastPrice: 100, Ema21: 100.1, Ema55: 100.0, AtrPct: 0.0021, Healthy: true},
		"ETHUSDT": {Symbol: "ETHUSDT", UpdatedAt: now.Add(-5 * time.Second), LastPrice: 100, Ema21: 100.1, Ema55: 100.0, AtrPct: 0.0020, Healthy: true},
		"XRPUSDT": {Symbol: "XRPUSDT", UpdatedAt: now.Add(-5 * time.Second), LastPrice: 100, Ema21: 100.1, Ema55: 100.0, AtrPct: 0.0021, Healthy: true},
	})
	if first.GlobalState != "range" {
		t.Fatalf("first global state = %s, want range", first.GlobalState)
	}

	second := selector.Evaluate(now.Add(30*time.Second), map[string]Snapshot{
		"BTCUSDT": {Symbol: "BTCUSDT", UpdatedAt: now.Add(-5 * time.Second), LastPrice: 100, Ema21: 100.1, Ema55: 100.0, AtrPct: 0, Healthy: true},
		"ETHUSDT": {Symbol: "ETHUSDT", UpdatedAt: now.Add(-5 * time.Second), LastPrice: 100, Ema21: 100.1, Ema55: 100.0, AtrPct: 0, Healthy: true},
		"XRPUSDT": {Symbol: "XRPUSDT", UpdatedAt: now.Add(-5 * time.Second), LastPrice: 100, Ema21: 100.1, Ema55: 100.0, AtrPct: 0, Healthy: true},
	})
	if second.GlobalState != "range" {
		t.Fatalf("second global state = %s, want held range", second.GlobalState)
	}
	if second.Symbols["XRPUSDT"].Reason != "state_filtered" {
		t.Fatalf("XRPUSDT = %+v, want state_filtered during hold", second.Symbols["XRPUSDT"])
	}
}

func TestBasicSelectorEvaluateValidationMode5mUsesExitThresholdToHoldRange(t *testing.T) {
	now := time.Date(2026, 5, 1, 2, 31, 0, 0, time.UTC)
	selector := NewBasicSelector(Config{
		ValidationMode:        "5m",
		CandidateSymbols:      []string{"BTCUSDT", "ETHUSDT", "BNBUSDT", "XRPUSDT"},
		RangePreferredSymbols: []string{"BTCUSDT", "ETHUSDT"},
		EvaluateInterval:      30 * time.Second,
		AddScoreThreshold:     0.75,
	})

	first := selector.Evaluate(now, map[string]Snapshot{
		"BTCUSDT": {Symbol: "BTCUSDT", UpdatedAt: now.Add(-5 * time.Second), LastPrice: 100, Ema21: 100.1, Ema55: 100.0, AtrPct: 0.0021, Healthy: true},
		"ETHUSDT": {Symbol: "ETHUSDT", UpdatedAt: now.Add(-5 * time.Second), LastPrice: 100, Ema21: 100.1, Ema55: 100.0, AtrPct: 0.0020, Healthy: true},
		"BNBUSDT": {Symbol: "BNBUSDT", UpdatedAt: now.Add(-5 * time.Second), LastPrice: 100, Ema21: 100.1, Ema55: 100.0, AtrPct: 0.0021, Healthy: true},
		"XRPUSDT": {Symbol: "XRPUSDT", UpdatedAt: now.Add(-5 * time.Second), LastPrice: 100, Ema21: 100.1, Ema55: 100.0, AtrPct: 0.0021, Healthy: true},
	})
	if first.GlobalState != "range" || first.RangeCount != 4 {
		t.Fatalf("first global state = %s range=%d, want range/4", first.GlobalState, first.RangeCount)
	}

	second := selector.Evaluate(now.Add(2*time.Minute), map[string]Snapshot{
		"BTCUSDT": {Symbol: "BTCUSDT", UpdatedAt: now.Add(115 * time.Second), LastPrice: 100, Ema21: 100.1, Ema55: 100.0, AtrPct: 0.0026, Healthy: true},
		"ETHUSDT": {Symbol: "ETHUSDT", UpdatedAt: now.Add(115 * time.Second), LastPrice: 100, Ema21: 100.1, Ema55: 100.0, AtrPct: 0.0025, Healthy: true},
		"BNBUSDT": {Symbol: "BNBUSDT", UpdatedAt: now.Add(115 * time.Second), LastPrice: 100, Ema21: 100.1, Ema55: 100.0, AtrPct: 0.0026, Healthy: true},
		"XRPUSDT": {Symbol: "XRPUSDT", UpdatedAt: now.Add(115 * time.Second), LastPrice: 100, Ema21: 100.1, Ema55: 100.0, AtrPct: 0.0026, Healthy: true},
	})
	if second.GlobalState != "range" || second.RangeCount != 4 {
		t.Fatalf("second global state = %s range=%d, want held range/4 via exit threshold", second.GlobalState, second.RangeCount)
	}
	if second.Symbols["BNBUSDT"].Reason != "state_filtered" {
		t.Fatalf("BNBUSDT = %+v, want state_filtered during hysteresis hold", second.Symbols["BNBUSDT"])
	}
}

func TestBasicSelectorEvaluateValidationMode5mKeepsSnapshotsFreshPastNinetySeconds(t *testing.T) {
	now := time.Date(2026, 5, 1, 2, 33, 40, 0, time.UTC)
	selector := NewBasicSelector(Config{
		ValidationMode:        "5m",
		CandidateSymbols:      []string{"BTCUSDT", "ETHUSDT", "BNBUSDT", "XRPUSDT"},
		RangePreferredSymbols: []string{"BTCUSDT", "ETHUSDT"},
		EvaluateInterval:      30 * time.Second,
		AddScoreThreshold:     0.75,
	})

	got := selector.Evaluate(now, map[string]Snapshot{
		"BTCUSDT": {Symbol: "BTCUSDT", UpdatedAt: now.Add(-160 * time.Second), LastPrice: 100, Ema21: 100.1, Ema55: 100.0, AtrPct: 0.0021, Healthy: true},
		"ETHUSDT": {Symbol: "ETHUSDT", UpdatedAt: now.Add(-160 * time.Second), LastPrice: 100, Ema21: 100.1, Ema55: 100.0, AtrPct: 0.0020, Healthy: true},
		"BNBUSDT": {Symbol: "BNBUSDT", UpdatedAt: now.Add(-160 * time.Second), LastPrice: 100, Ema21: 100.1, Ema55: 100.0, AtrPct: 0.0021, Healthy: true},
		"XRPUSDT": {Symbol: "XRPUSDT", UpdatedAt: now.Add(-160 * time.Second), LastPrice: 100, Ema21: 100.1, Ema55: 100.0, AtrPct: 0.0021, Healthy: true},
	})
	if got.GlobalState != "range" || got.RangeCount != 4 {
		t.Fatalf("global state = %s range=%d, want range/4 with 5m freshness window", got.GlobalState, got.RangeCount)
	}
	if got.Symbols["BNBUSDT"].Reason != "state_filtered" {
		t.Fatalf("BNBUSDT = %+v, want state_filtered instead of stale_snapshot", got.Symbols["BNBUSDT"])
	}
}
