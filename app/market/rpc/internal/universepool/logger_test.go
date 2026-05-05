package universepool

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestJSONLLoggerWriteSelectorDecisionIncludesReasonZh(t *testing.T) {
	dir := t.TempDir()
	logger := NewJSONLLogger(dir)
	now := time.Date(2026, 5, 1, 1, 2, 3, 0, time.UTC)

	logger.WriteSelectorDecision(
		now,
		SymbolRuntimeState{Symbol: "XRPUSDT", State: SymbolInactive},
		WarmupStatus{Symbol: "XRPUSDT", Ready: true, LastIncompleteReason: "warmup_pending"},
		"range",
		DesiredUniverseSymbol{
			Symbol:  "XRPUSDT",
			Reason:  "state_filtered",
			Score:   0.55,
			Desired: false,
			StateVote: StateVoteDetail{
				ClassifiedState:    "range",
				ClassifiedReason:   "range_match_precedes_trend",
				ClassifiedReasonZh: "ATR 命中 range 阈值，按 breakout>range>trend 顺序优先判为 range",
				Fresh:              true,
				Healthy:            true,
				LastPrice:          100,
				Ema21:              100.1,
				Ema55:              100.0,
				AtrPct:             0.00039,
				RangeAtrPctMax:     0.0004,
				BreakoutAtrPctMin:  0.0045,
				RangeMatch:         true,
				TrendMatch:         true,
				RangeGateReady:     true,
				RangeGatePassed:    false,
				RangeGateReason:    "range_gate_h4_failed",
				RangeGateScore:     1,
				RangeGateUpdatedAt: now.Add(-4 * time.Hour),
				RangeGateSource:    "warmup",
				RankDetail: &RankDetail{
					BaseScore:       0.82,
					TrendScore:      0.70,
					VolatilityScore: 0.90,
					VolumeScore:     0.60,
					RawTrendScore:   0.002,
					RawVolatility:   0.00039,
					RawVolume:       12345,
				},
			},
		},
	)

	path := filepath.Join(dir, "XRPUSDT", "2026-05-01.jsonl")
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("ReadFile(%s) error = %v", path, err)
	}
	lines := strings.Split(strings.TrimSpace(string(data)), "\n")
	if len(lines) != 1 {
		t.Fatalf("log lines = %d, want 1", len(lines))
	}
	if strings.Contains(lines[0], `\u003e`) {
		t.Fatalf("raw log line still contains escaped >: %s", lines[0])
	}
	if !strings.Contains(lines[0], `"score":0.550000`) {
		t.Fatalf("raw log line score = %s, want fixed 6 decimal places", lines[0])
	}
	if !strings.Contains(lines[0], `"atr_pct":"0.000390"`) {
		t.Fatalf("raw log line atr_pct = %s, want fixed 6 decimal places", lines[0])
	}
	if !strings.Contains(lines[0], `ATR 命中 range 阈值，按 breakout>range>trend 顺序优先判为 range`) {
		t.Fatalf("raw log line still contains escaped classified_reason_zh: %s", lines[0])
	}

	var entry map[string]any
	if err := json.Unmarshal([]byte(lines[0]), &entry); err != nil {
		t.Fatalf("Unmarshal() error = %v", err)
	}
	if got := entry["reason"]; got != "state_filtered" {
		t.Fatalf("reason = %v, want state_filtered", got)
	}
	if got := entry["timestamp_bj"]; got != "2026-05-01 09:02:03 CST" {
		t.Fatalf("timestamp_bj = %v, want 2026-05-01 09:02:03 CST", got)
	}
	if got := entry["reason_zh"]; got != "状态过滤" {
		t.Fatalf("reason_zh = %v, want 状态过滤", got)
	}
	if got := entry["last_incomplete_reason"]; got != "warmup_pending" {
		t.Fatalf("last_incomplete_reason = %v, want warmup_pending", got)
	}
	if got := entry["last_incomplete_reason_zh"]; got != "预热未完成" {
		t.Fatalf("last_incomplete_reason_zh = %v, want 预热未完成", got)
	}
	if got := entry["range_gate_ready"]; got != true {
		t.Fatalf("range_gate_ready = %v, want true", got)
	}
	if got := entry["range_gate_passed"]; got != false {
		t.Fatalf("range_gate_passed = %v, want false", got)
	}
	if got := entry["range_gate_reason"]; got != "range_gate_h4_failed" {
		t.Fatalf("range_gate_reason = %v, want range_gate_h4_failed", got)
	}
	if got := entry["range_gate_reason_zh"]; got != "4H 震荡门禁未通过" {
		t.Fatalf("range_gate_reason_zh = %v, want 4H 震荡门禁未通过", got)
	}
	if got := entry["range_gate_score"]; got != float64(1) {
		t.Fatalf("range_gate_score = %v, want 1", got)
	}
	if got := entry["range_gate_source"]; got != "warmup" {
		t.Fatalf("range_gate_source = %v, want warmup", got)
	}
	if got := entry["range_gate_updated_at"]; got != "2026-04-30 21:02:03 UTC" {
		t.Fatalf("range_gate_updated_at = %v, want 2026-04-30 21:02:03 UTC", got)
	}
	if got := entry["range_gate_updated_at_bj"]; got != "2026-05-01 05:02:03 CST" {
		t.Fatalf("range_gate_updated_at_bj = %v, want 2026-05-01 05:02:03 CST", got)
	}
	stateVote, ok := entry["state_vote"].(map[string]any)
	if !ok {
		t.Fatalf("state_vote = %T, want map", entry["state_vote"])
	}
	if got := stateVote["classified_state"]; got != "range" {
		t.Fatalf("state_vote.classified_state = %v, want range", got)
	}
	if got := stateVote["classified_reason"]; got != "range_match_precedes_trend" {
		t.Fatalf("state_vote.classified_reason = %v, want range_match_precedes_trend", got)
	}
	if got := stateVote["range_match"]; got != true {
		t.Fatalf("state_vote.range_match = %v, want true", got)
	}
	if got := stateVote["trend_match"]; got != true {
		t.Fatalf("state_vote.trend_match = %v, want true", got)
	}
	if got := stateVote["atr_pct"]; got != "0.000390" {
		t.Fatalf("state_vote.atr_pct = %v, want 0.000390", got)
	}
	rankDetail, ok := stateVote["rank_detail"].(map[string]any)
	if !ok {
		t.Fatalf("state_vote.rank_detail = %T, want map", stateVote["rank_detail"])
	}
	if got := rankDetail["base_score"]; got != "0.8200" {
		t.Fatalf("state_vote.rank_detail.base_score = %v, want 0.8200", got)
	}
	if got := rankDetail["volatility_score"]; got != "0.9000" {
		t.Fatalf("state_vote.rank_detail.volatility_score = %v, want 0.9000", got)
	}
}

// TestJSONLLoggerWriteMetaIncludesRankDetail 验证 _meta 的 state_votes 也会写出 ranker 分量明细。
func TestJSONLLoggerWriteMetaIncludesRankDetail(t *testing.T) {
	dir := t.TempDir()
	logger := NewJSONLLogger(dir)
	now := time.Date(2026, 5, 1, 1, 2, 3, 0, time.UTC)

	logger.WriteMeta(now, stateSummary{
		GlobalState:      "trend",
		SnapshotInterval: "1m",
		LastSnapshotAt:   now,
		TrendCount:       2,
		StateVotes: map[string]StateVoteDetail{
			"BNBUSDT": {
				ClassifiedState:    "trend",
				ClassifiedReason:   "trend_match",
				Fresh:              true,
				Healthy:            true,
				AtrPct:             0.001234567,
				RangeGateReady:     true,
				RangeGatePassed:    true,
				RangeGateReason:    "range_gate_h4_passed",
				RangeGateScore:     3,
				RangeGateSource:    "live",
				RangeGateUpdatedAt: now.Add(-4 * time.Hour),
				RankDetail: &RankDetail{
					BaseScore:       0.88,
					TrendScore:      1,
					VolatilityScore: 0.72,
					VolumeScore:     0.55,
				},
			},
		},
	})

	path := filepath.Join(dir, "_meta", "2026-05-01.jsonl")
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("ReadFile(%s) error = %v", path, err)
	}
	lines := strings.Split(strings.TrimSpace(string(data)), "\n")
	if len(lines) != 1 {
		t.Fatalf("log lines = %d, want 1", len(lines))
	}
	if !strings.Contains(lines[0], `"atr_pct":"0.001235"`) {
		t.Fatalf("raw meta log line atr_pct = %s, want fixed 6 decimal places", lines[0])
	}

	var entry map[string]any
	if err := json.Unmarshal([]byte(lines[0]), &entry); err != nil {
		t.Fatalf("Unmarshal() error = %v", err)
	}
	if got := entry["timestamp_bj"]; got != "2026-05-01 09:02:03 CST" {
		t.Fatalf("timestamp_bj = %v, want 2026-05-01 09:02:03 CST", got)
	}
	if got := entry["last_snapshot_at_bj"]; got != "2026-05-01 09:02:03 CST" {
		t.Fatalf("last_snapshot_at_bj = %v, want 2026-05-01 09:02:03 CST", got)
	}
	stateVotes, ok := entry["state_votes"].(map[string]any)
	if !ok {
		t.Fatalf("state_votes = %T, want map", entry["state_votes"])
	}
	bnbVote, ok := stateVotes["BNBUSDT"].(map[string]any)
	if !ok {
		t.Fatalf("state_votes.BNBUSDT = %T, want map", stateVotes["BNBUSDT"])
	}
	rankDetail, ok := bnbVote["rank_detail"].(map[string]any)
	if !ok {
		t.Fatalf("state_votes.BNBUSDT.rank_detail = %T, want map", bnbVote["rank_detail"])
	}
	if got := bnbVote["atr_pct"]; got != "0.001235" {
		t.Fatalf("state_votes.BNBUSDT.atr_pct = %v, want 0.001235", got)
	}
	if got := rankDetail["base_score"]; got != "0.8800" {
		t.Fatalf("state_votes.BNBUSDT.rank_detail.base_score = %v, want 0.8800", got)
	}
	if got := entry["range_gate_ready_count"]; got != float64(1) {
		t.Fatalf("range_gate_ready_count = %v, want 1", got)
	}
	if got := entry["range_gate_passed_count"]; got != float64(1) {
		t.Fatalf("range_gate_passed_count = %v, want 1", got)
	}
	if got := entry["range_gate_live_count"]; got != float64(1) {
		t.Fatalf("range_gate_live_count = %v, want 1", got)
	}
	rangeGates, ok := entry["range_gates"].(map[string]any)
	if !ok {
		t.Fatalf("range_gates = %T, want map", entry["range_gates"])
	}
	bnbGate, ok := rangeGates["BNBUSDT"].(map[string]any)
	if !ok {
		t.Fatalf("range_gates.BNBUSDT = %T, want map", rangeGates["BNBUSDT"])
	}
	if got := bnbGate["reason"]; got != "range_gate_h4_passed" {
		t.Fatalf("range_gates.BNBUSDT.reason = %v, want range_gate_h4_passed", got)
	}
	if got := bnbGate["reason_zh"]; got != "4H 震荡门禁通过" {
		t.Fatalf("range_gates.BNBUSDT.reason_zh = %v, want 4H 震荡门禁通过", got)
	}
	if got := bnbGate["source"]; got != "live" {
		t.Fatalf("range_gates.BNBUSDT.source = %v, want live", got)
	}
	if got := bnbGate["updated_at"]; got != "2026-04-30 21:02:03 UTC" {
		t.Fatalf("range_gates.BNBUSDT.updated_at = %v, want 2026-04-30 21:02:03 UTC", got)
	}
	if got := bnbGate["updated_at_bj"]; got != "2026-05-01 05:02:03 CST" {
		t.Fatalf("range_gates.BNBUSDT.updated_at_bj = %v, want 2026-05-01 05:02:03 CST", got)
	}
}
