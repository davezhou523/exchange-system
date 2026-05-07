package marketstate

import (
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
	"time"

	"exchange-system/common/regimejudge"
)

// TestBuildMetaLogEntryIncludesChineseDescriptions 验证 _meta 日志会把关键状态码和原因码同步输出为中文说明。
func TestBuildMetaLogEntryIncludesChineseDescriptions(t *testing.T) {
	now := time.Date(2026, 5, 4, 14, 31, 48, 299*int(time.Millisecond), time.UTC)

	entry := BuildMetaLogEntry(
		now,
		map[string]Features{
			"ETHUSDT": {Symbol: "ETHUSDT"},
		},
		map[string]regimejudge.Analysis{
			"ETHUSDT": {
				Healthy:    true,
				Fresh:      true,
				RangeMatch: true,
			},
		},
		map[string]Result{
			"ETHUSDT": {
				Symbol:     "ETHUSDT",
				State:      MarketStateRange,
				Confidence: 0.7,
				Reason:     "fallback_1m_only",
			},
		},
	)

	if entry.GlobalStateDesc != "震荡" {
		t.Fatalf("global_state_desc = %q, want %q", entry.GlobalStateDesc, "震荡")
	}
	if entry.TimestampBJ == "" {
		t.Fatalf("timestamp_bj = %q, want non-empty beijing time", entry.TimestampBJ)
	}
	if entry.GlobalReasonDesc != "全局状态由命中面最强的形态主导" {
		t.Fatalf("global_reason_desc = %q, want dominant_match_surface description", entry.GlobalReasonDesc)
	}
	if entry.StateCountsDesc["range"] != "震荡" {
		t.Fatalf("state_counts_desc[range] = %q, want %q", entry.StateCountsDesc["range"], "震荡")
	}
	if entry.MatchCountsDesc["range"] != "震荡" {
		t.Fatalf("match_counts_desc[range] = %q, want %q", entry.MatchCountsDesc["range"], "震荡")
	}
	if entry.ReasonCountsDesc["fallback_1m_only"] != "高周期不可用，当前仅使用 1m 判态结果兜底" {
		t.Fatalf("reason_counts_desc[fallback_1m_only] = %q, want fallback description", entry.ReasonCountsDesc["fallback_1m_only"])
	}
}

// TestWriteIncludesFallbackIntervalDetails 验证 fallback_1m_only 日志会输出缺失周期和逐周期原因，避免排查时再去猜 15m/1h。
func TestWriteIncludesFallbackIntervalDetails(t *testing.T) {
	dir := t.TempDir()
	now := time.Date(2026, 5, 4, 14, 57, 34, 985*int(time.Millisecond), time.UTC)
	logs := NewLogState()
	defer func() {
		_ = logs.Close()
	}()

	logs.Write(dir, Features{
		Symbol:    "ETHUSDT",
		Timeframe: "1m",
		Healthy:   true,
	}, Result{
		Symbol:                   "ETHUSDT",
		State:                    MarketStateTrendUp,
		Confidence:               0.75,
		Reason:                   "fallback_1m_only",
		FallbackSource:           "1m",
		FallbackMissingIntervals: []string{"1h"},
		FallbackIntervalReady: map[string]bool{
			"1m":  true,
			"1h":  false,
			"15m": true,
		},
		FallbackIntervalReasons: map[string]string{
			"1m":  "ema_bull_alignment",
			"1h":  "missing_trend_features",
			"15m": "atr_pct_low",
		},
		FallbackIntervalAgeSec: map[string]int64{
			"15m": 123,
		},
	}, now)

	data, err := os.ReadFile(filepath.Join(dir, "marketstate", "ETHUSDT", now.Format("2006-01-02")+".jsonl"))
	if err != nil {
		t.Fatalf("ReadFile failed: %v", err)
	}
	var entry map[string]interface{}
	if err := json.Unmarshal(data, &entry); err != nil {
		t.Fatalf("Unmarshal failed: %v", err)
	}
	if got := entry["fallback_source"]; got != "1m" {
		t.Fatalf("fallback_source = %#v, want 1m", got)
	}
	if got := entry["fallback_source_desc"]; got != "1m" {
		t.Fatalf("fallback_source_desc = %#v, want 1m", got)
	}
	if got, ok := entry["timestamp_bj"].(string); !ok || got == "" {
		t.Fatalf("timestamp_bj = %#v, want non-empty beijing time", entry["timestamp_bj"])
	}
	missing, ok := entry["fallback_missing_intervals"].([]interface{})
	if !ok || len(missing) != 1 || missing[0] != "1h" {
		t.Fatalf("fallback_missing_intervals = %#v, want [1h]", entry["fallback_missing_intervals"])
	}
	missingDescs, ok := entry["fallback_missing_interval_descs"].([]interface{})
	if !ok || len(missingDescs) != 1 || missingDescs[0] != "1h 周期不可用" {
		t.Fatalf("fallback_missing_interval_descs = %#v, want [1h 周期不可用]", entry["fallback_missing_interval_descs"])
	}
	reasonDescs, ok := entry["fallback_interval_reason_descs"].(map[string]interface{})
	if !ok {
		t.Fatalf("fallback_interval_reason_descs type = %T, want map", entry["fallback_interval_reason_descs"])
	}
	if reasonDescs["1h"] != "趋势判定所需特征不足" {
		t.Fatalf("fallback_interval_reason_descs[1h] = %#v, want missing trend desc", reasonDescs["1h"])
	}
	ages, ok := entry["fallback_interval_age_sec"].(map[string]interface{})
	if !ok {
		t.Fatalf("fallback_interval_age_sec type = %T, want map", entry["fallback_interval_age_sec"])
	}
	if ages["15m"] != float64(123) {
		t.Fatalf("fallback_interval_age_sec[15m] = %#v, want 123", ages["15m"])
	}
}

// TestWriteIncludesHealthGateReasonDescriptions 验证 fallback_interval_reason_descs 会把健康门禁原因也翻成中文，避免日志里残留英文码。
func TestWriteIncludesHealthGateReasonDescriptions(t *testing.T) {
	dir := t.TempDir()
	now := time.Date(2026, 5, 5, 1, 15, 15, 556*int(time.Millisecond), time.UTC)
	logs := NewLogState()
	defer func() {
		_ = logs.Close()
	}()

	logs.Write(dir, Features{
		Symbol:    "ETHUSDT",
		Timeframe: "1m",
		Healthy:   true,
	}, Result{
		Symbol:                   "ETHUSDT",
		State:                    MarketStateUnknown,
		Reason:                   "fallback_1m_only",
		FallbackSource:           "1m",
		FallbackMissingIntervals: []string{"1h", "15m"},
		FallbackIntervalReady: map[string]bool{
			"1m":  false,
			"15m": false,
			"1h":  false,
			"4h":  false,
		},
		FallbackIntervalReasons: map[string]string{
			"1m":  "stale_data",
			"15m": "dirty_data",
			"1h":  "not_final",
			"4h":  "not_tradable",
		},
	}, now)

	data, err := os.ReadFile(filepath.Join(dir, "marketstate", "ETHUSDT", now.Format("2006-01-02")+".jsonl"))
	if err != nil {
		t.Fatalf("ReadFile failed: %v", err)
	}
	var entry map[string]interface{}
	if err := json.Unmarshal(data, &entry); err != nil {
		t.Fatalf("Unmarshal failed: %v", err)
	}
	reasonDescs, ok := entry["fallback_interval_reason_descs"].(map[string]interface{})
	if !ok {
		t.Fatalf("fallback_interval_reason_descs type = %T, want map", entry["fallback_interval_reason_descs"])
	}
	if got := reasonDescs["1m"]; got != "快照已过期，当前周期未就绪" {
		t.Fatalf("fallback_interval_reason_descs[1m] = %#v, want stale_data desc", got)
	}
	if got := reasonDescs["15m"]; got != "快照仍为脏数据，当前周期未就绪" {
		t.Fatalf("fallback_interval_reason_descs[15m] = %#v, want dirty_data desc", got)
	}
	if got := reasonDescs["1h"]; got != "快照尚未最终确认，当前周期未就绪" {
		t.Fatalf("fallback_interval_reason_descs[1h] = %#v, want not_final desc", got)
	}
	if got := reasonDescs["4h"]; got != "快照当前不可交易，当前周期未就绪" {
		t.Fatalf("fallback_interval_reason_descs[4h] = %#v, want not_tradable desc", got)
	}
}

// TestWriteIncludesFusionReasonDescription 验证融合层 reason 也会输出中文说明，避免日志里再次出现英文码。
func TestWriteIncludesFusionReasonDescription(t *testing.T) {
	dir := t.TempDir()
	now := time.Date(2026, 5, 4, 15, 47, 20, 398*int(time.Millisecond), time.UTC)
	logs := NewLogState()
	defer func() {
		_ = logs.Close()
	}()

	logs.Write(dir, Features{
		Symbol:    "ETHUSDT",
		Timeframe: "fusion_1h_15m",
		Healthy:   true,
	}, Result{
		Symbol:     "ETHUSDT",
		State:      MarketStateTrendUp,
		Confidence: 0.225,
		Reason:     "m15_confirm_override",
	}, now)

	data, err := os.ReadFile(filepath.Join(dir, "marketstate", "ETHUSDT", now.Format("2006-01-02")+".jsonl"))
	if err != nil {
		t.Fatalf("ReadFile failed: %v", err)
	}
	var entry map[string]interface{}
	if err := json.Unmarshal(data, &entry); err != nil {
		t.Fatalf("Unmarshal failed: %v", err)
	}
	if got := entry["reason_desc"]; got != "15M 确认信号更强，覆盖主周期判断" {
		t.Fatalf("reason_desc = %#v, want fusion reason desc", got)
	}
}
