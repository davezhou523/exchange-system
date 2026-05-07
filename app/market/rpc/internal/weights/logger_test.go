package weights

import (
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
	"time"
)

// TestBuildMetaLogEntryIncludesMatchCountsAndStrategyMix 验证 weights/_meta 会写出命中面统计和最终策略桶配比。
func TestBuildMetaLogEntryIncludesMatchCountsAndStrategyMix(t *testing.T) {
	now := time.Date(2026, 5, 2, 12, 0, 0, 0, time.UTC)
	entry := BuildMetaLogEntry(Output{
		Recommendations: []Recommendation{
			{Symbol: "BTCUSDT", Template: "breakout-core", Bucket: "breakout"},
			{Symbol: "ETHUSDT", Template: "eth-core", Bucket: "trend"},
		},
		MatchCounts: map[string]int{
			"trend_up": 2,
			"breakout": 1,
			"range":    1,
		},
		StrategyMix: map[string]float64{
			"trend":    0.5,
			"breakout": 0.25,
			"range":    0.25,
		},
		BucketBudgets: map[string]float64{
			"trend":    0.5,
			"breakout": 0.25,
		},
		BucketSymbolCount: map[string]int{
			"trend":    1,
			"breakout": 1,
		},
	}, 2, "global:trend_up", "marketstate.aggregate", now)

	if got := entry.MatchCounts["trend_up"]; got != 2 {
		t.Fatalf("match_counts[trend_up] = %d, want 2", got)
	}
	if entry.TimestampBJ == "" {
		t.Fatalf("timestamp_bj = %q, want non-empty beijing time", entry.TimestampBJ)
	}
	if got := entry.MatchCounts["breakout"]; got != 1 {
		t.Fatalf("match_counts[breakout] = %d, want 1", got)
	}
	if got := entry.StrategyMix["trend"]; got != 0.5 {
		t.Fatalf("strategy_mix[trend] = %.4f, want 0.5000", got)
	}
	if got := entry.StrategyMix["breakout"]; got != 0.25 {
		t.Fatalf("strategy_mix[breakout] = %.4f, want 0.2500", got)
	}
	if got := entry.TemplateCounts["breakout-core"]; got != 1 {
		t.Fatalf("template_counts[breakout-core] = %d, want 1", got)
	}
	if got := entry.BucketBudgets["trend"]; got != 0.5 {
		t.Fatalf("bucket_budgets[trend] = %.4f, want 0.5000", got)
	}
	if got := entry.BucketSymbolCount["breakout"]; got != 1 {
		t.Fatalf("bucket_symbol_count[breakout] = %d, want 1", got)
	}
}

// TestWriteIncludesRouteReason 验证 symbol 级 weights 日志会写出统一路由原因。
func TestWriteIncludesRouteReason(t *testing.T) {
	dir := t.TempDir()
	now := time.Date(2026, 5, 2, 12, 0, 0, 0, time.UTC)
	logs := NewLogState()
	defer func() {
		_ = logs.Close()
	}()

	logs.Write(dir, Recommendation{
		Symbol:         "BTCUSDT",
		Template:       "breakout-core",
		Bucket:         "breakout",
		RouteReason:    "market_state_breakout",
		Score:          1.15,
		ScoreSource:    "regime_analysis",
		BucketBudget:   0.7,
		StrategyWeight: 0.7,
		SymbolWeight:   0.5,
		RiskScale:      1,
		PositionBudget: 0.35,
	}, now)

	data, err := os.ReadFile(filepath.Join(dir, "weights", "BTCUSDT", now.Format("2006-01-02")+".jsonl"))
	if err != nil {
		t.Fatalf("ReadFile failed: %v", err)
	}
	var entry map[string]interface{}
	if err := json.Unmarshal(data, &entry); err != nil {
		t.Fatalf("Unmarshal failed: %v", err)
	}
	if got := entry["route_reason"]; got != "market_state_breakout" {
		t.Fatalf("route_reason = %#v, want market_state_breakout", got)
	}
	if got, ok := entry["timestamp_bj"].(string); !ok || got == "" {
		t.Fatalf("timestamp_bj = %#v, want non-empty beijing time", entry["timestamp_bj"])
	}
	if got := entry["score_source"]; got != "regime_analysis" {
		t.Fatalf("score_source = %#v, want regime_analysis", got)
	}
	if got := entry["template_desc"]; got != "突破核心模板" {
		t.Fatalf("template_desc = %#v, want 突破核心模板", got)
	}
	if got := entry["route_bucket_desc"]; got != "突破策略桶" {
		t.Fatalf("route_bucket_desc = %#v, want 突破策略桶", got)
	}
	if got := entry["route_reason_desc"]; got != "统一判态支持走突破策略桶" {
		t.Fatalf("route_reason_desc = %#v, want route reason desc", got)
	}
	if got := entry["score_source_desc"]; got != "使用 regime analysis 推导的分数作为权重来源" {
		t.Fatalf("score_source_desc = %#v, want score source desc", got)
	}
}
