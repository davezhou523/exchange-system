package strategy

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
	"time"

	"exchange-system/app/strategy/rpc/internal/weights"
	marketpb "exchange-system/common/pb/market"
)

// TestWriteSignalLogIncludesRouteContext 验证 signal 日志中的 weights 快照会带上统一 route 视角。
func TestWriteSignalLogIncludesRouteContext(t *testing.T) {
	dir := t.TempDir()
	s := NewTrendFollowingStrategy("BTCUSDT", map[string]float64{}, nil, nil, dir, &RuntimeOptions{
		WeightProvider: func(symbol string) (weights.Recommendation, bool) {
			return weights.Recommendation{
				Symbol:         symbol,
				Template:       "breakout-core",
				Bucket:         "breakout",
				RouteReason:    "market_state_breakout",
				Score:          1.15,
				ScoreSource:    "symbol_score",
				BucketBudget:   0.7,
				StrategyWeight: 0.7,
				SymbolWeight:   0.5,
				RiskScale:      1,
				PositionBudget: 0.35,
			}, true
		},
	})
	now := time.Now().UTC()
	k := &marketpb.Kline{
		Symbol:     "BTCUSDT",
		Interval:   "15m",
		OpenTime:   now.Add(-15 * time.Minute).UnixMilli(),
		CloseTime:  now.UnixMilli(),
		IsTradable: true,
		IsFinal:    true,
	}
	s.writeSignalLog(map[string]interface{}{
		"strategy_id":   "breakout-BTCUSDT",
		"action":        "BUY",
		"side":          "LONG",
		"entry_price":   100000.0,
		"quantity":      0.01,
		"stop_loss":     99000.0,
		"take_profits":  []float64{101000.0, 102000.0},
		"reason":        "test",
		"signal_reason": map[string]interface{}{"summary": "test"},
		"interval":      "15m",
	}, k)

	entry := readSingleJSONLine(t, filepath.Join(dir, "BTCUSDT", now.Format("2006-01-02")+".jsonl"))
	weightsValue, ok := entry["weights"].(map[string]interface{})
	if !ok {
		t.Fatalf("weights = %#v, want object", entry["weights"])
	}
	if got := weightsValue["route_bucket"]; got != "breakout" {
		t.Fatalf("weights.route_bucket = %#v, want breakout", got)
	}
	if got := weightsValue["route_reason"]; got != "market_state_breakout" {
		t.Fatalf("weights.route_reason = %#v, want market_state_breakout", got)
	}
	if got := weightsValue["score_source"]; got != "symbol_score" {
		t.Fatalf("weights.score_source = %#v, want symbol_score", got)
	}
	if got := weightsValue["bucket_budget"]; got != float64(0.7) {
		t.Fatalf("weights.bucket_budget = %#v, want 0.7", got)
	}
}

// TestEnrichSignalWithRouteContext 验证实际发出的 signal payload 顶层会补齐 route 解释字段。
func TestEnrichSignalWithRouteContext(t *testing.T) {
	s := NewTrendFollowingStrategy("BTCUSDT", map[string]float64{}, nil, nil, "", &RuntimeOptions{
		WeightProvider: func(symbol string) (weights.Recommendation, bool) {
			return weights.Recommendation{
				Symbol:      symbol,
				Template:    "breakout-core",
				Bucket:      "breakout",
				RouteReason: "market_state_breakout",
			}, true
		},
	})
	payload := s.enrichSignalWithRouteContext(map[string]interface{}{
		"strategy_id": "breakout-BTCUSDT",
	})
	if got := payload["route_bucket"]; got != "breakout" {
		t.Fatalf("route_bucket = %#v, want breakout", got)
	}
	if got := payload["route_reason"]; got != "market_state_breakout" {
		t.Fatalf("route_reason = %#v, want market_state_breakout", got)
	}
	if got := payload["route_template"]; got != "breakout-core" {
		t.Fatalf("route_template = %#v, want breakout-core", got)
	}
}

// TestNewSignalReasonIncludesAllocator 验证 signal_reason 会携带统一 allocator 快照，供下游订单查询复用。
func TestNewSignalReasonIncludesAllocator(t *testing.T) {
	s := NewTrendFollowingStrategy("BTCUSDT", map[string]float64{}, nil, nil, "", &RuntimeOptions{
		WeightProvider: func(symbol string) (weights.Recommendation, bool) {
			return weights.Recommendation{
				Symbol:         symbol,
				Template:       "breakout-core",
				Bucket:         "breakout",
				RouteReason:    "market_state_breakout",
				Score:          1.15,
				ScoreSource:    "symbol_score",
				BucketBudget:   0.7,
				StrategyWeight: 0.7,
				SymbolWeight:   0.5,
				RiskScale:      1,
				PositionBudget: 0.35,
			}, true
		},
	})
	got := s.newSignalReason("test", "OPEN_ENTRY", "", "", "")
	if got.Allocator == nil {
		t.Fatal("allocator = nil, want snapshot")
	}
	if got.Allocator.ScoreSource != "symbol_score" || got.Allocator.PositionBudget != 0.35 {
		t.Fatalf("allocator = %+v, want symbol_score/0.35", got.Allocator)
	}
}

// TestNewExitSignalReasonIncludesExitReason 验证策略侧会在 signal_reason 中直接下发结构化出场原因。
func TestNewExitSignalReasonIncludesExitReason(t *testing.T) {
	s := NewTrendFollowingStrategy("BTCUSDT", map[string]float64{}, nil, nil, "", nil)
	got := s.newExitSignalReason(
		"[15m range] 分批止盈：价格2460.00 命中中轨目标2458.00，减仓0.1000",
		"持仓方向=LONG | 周期=15m",
		"[15m range] 分批止盈：价格2460.00 命中中轨目标2458.00，减仓0.1000",
		"平仓价=2460.00 | 预计盈亏=10.00 | 止损=2450.00 | 数量=0.1000",
		"partial_take_profit",
		"分批止盈",
		"15m", "range", "close", "LONG",
	)
	if got.Phase != "CLOSE_EXIT" {
		t.Fatalf("phase = %q, want CLOSE_EXIT", got.Phase)
	}
	if got.ExitReasonKind != "partial_take_profit" {
		t.Fatalf("exit_reason_kind = %q, want partial_take_profit", got.ExitReasonKind)
	}
	if got.ExitReasonLabel != "分批止盈" {
		t.Fatalf("exit_reason_label = %q, want 分批止盈", got.ExitReasonLabel)
	}
}

// TestSignalReasonRangePayloadFromStats 验证震荡策略会生成前端友好的 range 布尔摘要。
func TestSignalReasonRangePayloadFromStats(t *testing.T) {
	got := signalReasonRangePayloadFromStats(rangeStats{
		H1RangeOK:      true,
		H1AdxOk:        true,
		H1BollWidthOk:  true,
		NearRangeLow:   true,
		LongRSITurnOk:  true,
		NearRangeHigh:  false,
		ShortRSITurnOk: false,
	})
	if got == nil {
		t.Fatal("signalReasonRangePayloadFromStats() = nil, want value")
	}
	if !got.H1RangeOK || !got.M15TouchLower || !got.M15RsiTurnUp {
		t.Fatalf("unexpected range payload: %+v", got)
	}
	if got.M15TouchUpper || got.M15RsiTurnDown {
		t.Fatalf("unexpected short flags in range payload: %+v", got)
	}
}

// TestWriteDecisionLogIncludesRouteContext 验证 decision 日志会自动补齐统一 route 视角。
func TestWriteDecisionLogIncludesRouteContext(t *testing.T) {
	dir := t.TempDir()
	s := NewTrendFollowingStrategy("BTCUSDT", map[string]float64{
		paramDecisionLogEnabled: 1,
	}, nil, nil, dir, &RuntimeOptions{
		WeightProvider: func(symbol string) (weights.Recommendation, bool) {
			return weights.Recommendation{
				Symbol:      symbol,
				Template:    "breakout-core",
				Bucket:      "breakout",
				RouteReason: "market_state_breakout",
			}, true
		},
	})
	now := time.Now().UTC()
	k := &marketpb.Kline{
		Symbol:     "BTCUSDT",
		Interval:   "15m",
		OpenTime:   now.Add(-15 * time.Minute).UnixMilli(),
		CloseTime:  now.UnixMilli(),
		IsTradable: true,
		IsFinal:    true,
	}
	s.writeDecisionLogIfEnabled("entry", "skip", "m15_no_entry", k, map[string]interface{}{
		"trend": "LONG",
	})

	entry := readSingleJSONLine(t, filepath.Join(dir, "decision", "BTCUSDT", now.Format("2006-01-02")+".jsonl"))
	extras, ok := entry["extras"].(map[string]interface{})
	if !ok {
		t.Fatalf("extras = %#v, want object", entry["extras"])
	}
	if got := extras["route_bucket"]; got != "breakout" {
		t.Fatalf("extras.route_bucket = %#v, want breakout", got)
	}
	if got := extras["route_reason"]; got != "market_state_breakout" {
		t.Fatalf("extras.route_reason = %#v, want market_state_breakout", got)
	}
	if got := extras["route_template"]; got != "breakout-core" {
		t.Fatalf("extras.route_template = %#v, want breakout-core", got)
	}
}

// TestWriteDecisionLogSummaryIncludesRejectReasons 验证决策日志 summary 会把拒绝原因压缩成一行，方便直接扫日志。
func TestWriteDecisionLogSummaryIncludesRejectReasons(t *testing.T) {
	dir := t.TempDir()
	s := NewTrendFollowingStrategy("BTCUSDT", map[string]float64{
		paramDecisionLogEnabled: 1,
	}, nil, nil, dir, nil)
	now := time.Now().UTC()
	k := &marketpb.Kline{
		Symbol:     "BTCUSDT",
		Interval:   "15m",
		OpenTime:   now.Add(-15 * time.Minute).UnixMilli(),
		CloseTime:  now.UnixMilli(),
		IsTradable: true,
		IsFinal:    true,
	}
	s.writeDecisionLogIfEnabled("entry", "skip", "breakout_no_price_break", k, map[string]interface{}{
		"m15_open_time":         "2026-05-04 16:30:00 UTC",
		"m15_is_dirty":          false,
		"m15_dirty_reason":      "clean",
		"m15_is_tradable":       true,
		"m15_is_final":          true,
		"breakout_reject_descs": []string{"价格未突破前高/前低", "量能不足，未达到放量确认条件"},
		"breakout_reject_codes": []string{"breakout_no_price_break", "breakout_volume_low"},
	})

	entry := readSingleJSONLine(t, filepath.Join(dir, "decision", "BTCUSDT", now.Format("2006-01-02")+".jsonl"))
	extras, ok := entry["extras"].(map[string]interface{})
	if !ok {
		t.Fatalf("extras = %#v, want object", entry["extras"])
	}
	if got := extras["summary"]; got != "m15=final -> reject=价格未突破前高/前低+量能不足，未达到放量确认条件" {
		t.Fatalf("extras.summary = %#v, want compact reject summary", got)
	}
	if got, ok := entry["timestamp_bj"].(string); !ok || got == "" {
		t.Fatalf("timestamp_bj = %#v, want non-empty beijing time", entry["timestamp_bj"])
	}
}

// TestWriteDecisionLogSummaryIncludesGenericRejectReasons 验证通用 reject_descs 也会生成统一的一行 reject 摘要。
func TestWriteDecisionLogSummaryIncludesGenericRejectReasons(t *testing.T) {
	dir := t.TempDir()
	s := NewTrendFollowingStrategy("BTCUSDT", map[string]float64{
		paramDecisionLogEnabled: 1,
	}, nil, nil, dir, nil)
	now := time.Now().UTC()
	k := &marketpb.Kline{
		Symbol:     "BTCUSDT",
		Interval:   "15m",
		OpenTime:   now.Add(-15 * time.Minute).UnixMilli(),
		CloseTime:  now.UnixMilli(),
		IsTradable: true,
		IsFinal:    true,
	}
	s.writeDecisionLogIfEnabled("entry", "skip", "m15_long_structure_missing", k, map[string]interface{}{
		"m15_open_time":   "2026-05-04 16:30:00 UTC",
		"m15_is_dirty":    false,
		"m15_is_tradable": true,
		"m15_is_final":    true,
		"reject_descs":    []string{"多头入场时价格未突破近期高点", "多头入场时RSI未上穿50且未达到偏强阈值"},
		"reject_codes":    []string{"m15_long_structure_missing", "m15_long_rsi_signal_missing"},
	})

	entry := readSingleJSONLine(t, filepath.Join(dir, "decision", "BTCUSDT", now.Format("2006-01-02")+".jsonl"))
	extras, ok := entry["extras"].(map[string]interface{})
	if !ok {
		t.Fatalf("extras = %#v, want object", entry["extras"])
	}
	if got := extras["summary"]; got != "m15=final -> reject=多头入场时价格未突破近期高点+多头入场时RSI未上穿50且未达到偏强阈值" {
		t.Fatalf("extras.summary = %#v, want generic reject summary", got)
	}
}

// TestDescribeTrendM15EntryReject 验证普通趋势 15M 拒绝原因会拆成结构与 RSI 两类细分码。
func TestDescribeTrendM15EntryReject(t *testing.T) {
	s := NewTrendFollowingStrategy("BTCUSDT", map[string]float64{
		paramM15BreakoutLookback: 1,
		paramM15RsiBiasLong:      52,
	}, nil, nil, "", nil)
	s.klines15m = []klineSnapshot{
		{High: 101, Low: 99, Close: 100, Atr: 1, Rsi: 49},
		{High: 100.8, Low: 99.2, Close: 100.3, Atr: 1, Rsi: 49.5},
	}
	s.latest15m = s.klines15m[len(s.klines15m)-1]

	reject := s.describeTrendM15EntryReject(pullbackLong)
	if reject.PrimaryCode != "m15_long_structure_missing" {
		t.Fatalf("PrimaryCode = %q, want m15_long_structure_missing (reject=%+v)", reject.PrimaryCode, reject)
	}
	if len(reject.Codes) != 2 {
		t.Fatalf("Codes = %#v, want 2 reasons", reject.Codes)
	}
	if reject.Codes[0] != "m15_long_structure_missing" || reject.Codes[1] != "m15_long_rsi_signal_missing" {
		t.Fatalf("Codes = %#v, want structure+rsi reasons", reject.Codes)
	}
}

// TestUpdateRuntimeConfigPreservesCaches 验证模板切换时原地更新配置不会丢掉已积累的多周期缓存。
func TestUpdateRuntimeConfigPreservesCaches(t *testing.T) {
	s := NewTrendFollowingStrategy("BTCUSDT", map[string]float64{
		paramStrategyVariant: 0,
	}, nil, nil, "", nil)
	s.latest15m = klineSnapshot{OpenTime: 1, Close: 100, Atr: 1.2, Rsi: 55}
	s.klines1h = []klineSnapshot{{OpenTime: 2, Close: 101}}

	s.UpdateRuntimeConfig(map[string]float64{
		paramStrategyVariant: 2,
	}, "/tmp/strategy-log", nil)

	if !s.isRangeVariant() {
		t.Fatal("isRangeVariant() = false, want true after runtime update")
	}
	if s.latest15m.Atr != 1.2 || s.latest15m.Rsi != 55 {
		t.Fatalf("latest15m = %+v, want preserved snapshot", s.latest15m)
	}
	if len(s.klines1h) != 1 || s.klines1h[0].Close != 101 {
		t.Fatalf("klines1h = %+v, want preserved history", s.klines1h)
	}
	if s.signalLogDir != "/tmp/strategy-log" {
		t.Fatalf("signalLogDir = %q, want /tmp/strategy-log", s.signalLogDir)
	}
}

// TestCheckRangeEntryConditionsLogsMissingReasons 验证震荡策略未就绪时会把具体缺项写入决策日志。
func TestCheckRangeEntryConditionsLogsMissingReasons(t *testing.T) {
	dir := t.TempDir()
	s := NewTrendFollowingStrategy("BTCUSDT", map[string]float64{
		paramDecisionLogEnabled: 1,
		paramStrategyVariant:    2,
	}, nil, nil, dir, nil)
	s.latest15m = klineSnapshot{
		OpenTime:   time.Date(2026, 5, 3, 8, 45, 0, 0, time.UTC).UnixMilli(),
		Close:      100,
		Rsi:        52,
		Atr:        0,
		IsTradable: true,
		IsFinal:    true,
	}
	k := &marketpb.Kline{
		Symbol:     "BTCUSDT",
		Interval:   "15m",
		OpenTime:   time.Date(2026, 5, 3, 8, 45, 0, 0, time.UTC).UnixMilli(),
		CloseTime:  time.Date(2026, 5, 3, 8, 59, 59, 0, time.UTC).UnixMilli(),
		IsTradable: true,
		IsFinal:    true,
	}

	if err := s.checkRangeEntryConditions(context.Background(), k); err != nil {
		t.Fatalf("checkRangeEntryConditions() error = %v", err)
	}

	entry := readSingleJSONLine(t, filepath.Join(dir, "decision", "BTCUSDT", time.Now().UTC().Format("2006-01-02")+".jsonl"))
	extras, ok := entry["extras"].(map[string]interface{})
	if !ok {
		t.Fatalf("extras = %#v, want object", entry["extras"])
	}
	if got := extras["missing_h1_history"]; got != true {
		t.Fatalf("extras.missing_h1_history = %#v, want true", got)
	}
	if got := extras["missing_m15_atr"]; got != true {
		t.Fatalf("extras.missing_m15_atr = %#v, want true", got)
	}
	if got := extras["missing_m15_rsi"]; got != false {
		t.Fatalf("extras.missing_m15_rsi = %#v, want false", got)
	}
	if got := extras["h1_history_len"]; got != float64(0) {
		t.Fatalf("extras.h1_history_len = %#v, want 0", got)
	}
}

// readSingleJSONLine 读取测试产生的单行 JSONL 文件并解析为 map，便于断言日志字段。
func readSingleJSONLine(t *testing.T, path string) map[string]interface{} {
	t.Helper()
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("ReadFile(%s) failed: %v", path, err)
	}
	var entry map[string]interface{}
	if err := json.Unmarshal(data, &entry); err != nil {
		t.Fatalf("Unmarshal(%s) failed: %v", path, err)
	}
	return entry
}
