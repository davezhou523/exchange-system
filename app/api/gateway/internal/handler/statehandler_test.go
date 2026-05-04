package handler

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	gatewaysvc "exchange-system/app/api/gateway/internal/svc"
	strategypb "exchange-system/common/pb/strategy"

	"google.golang.org/grpc"
)

type fakeStrategyServiceClient struct {
	status *strategypb.StrategyStatus
	err    error
}

// StartStrategy 为测试桩实现必需接口；当前用例不覆盖该路径。
func (f *fakeStrategyServiceClient) StartStrategy(ctx context.Context, in *strategypb.StrategyConfig, opts ...grpc.CallOption) (*strategypb.StrategyStatus, error) {
	return nil, nil
}

// StopStrategy 为测试桩实现必需接口；当前用例不覆盖该路径。
func (f *fakeStrategyServiceClient) StopStrategy(ctx context.Context, in *strategypb.StrategyRequest, opts ...grpc.CallOption) (*strategypb.StrategyStatus, error) {
	return nil, nil
}

// GetStrategyStatus 返回预置状态，验证 gateway 会把结构化字段透传到 HTTP JSON。
func (f *fakeStrategyServiceClient) GetStrategyStatus(ctx context.Context, in *strategypb.StrategyRequest, opts ...grpc.CallOption) (*strategypb.StrategyStatus, error) {
	return f.status, f.err
}

// StreamSignals 为测试桩实现必需接口；当前用例不覆盖该路径。
func (f *fakeStrategyServiceClient) StreamSignals(ctx context.Context, in *strategypb.StrategyRequest, opts ...grpc.CallOption) (grpc.ServerStreamingClient[strategypb.Signal], error) {
	return nil, nil
}

func TestStrategyStatusHandlerIncludesStructuredRouterAndAllocator(t *testing.T) {
	serviceContext := &gatewaysvc.ServiceContext{
		Strategy: &fakeStrategyServiceClient{
			status: &strategypb.StrategyStatus{
				StrategyId:  "BTCUSDT",
				Status:      "RUNNING",
				Message:     "allocator ready",
				MessageCode: "allocator_ready",
				MessageDesc: "策略运行中，且已生成最新仓位分配建议",
				Allocator: &strategypb.PositionAllocatorStatus{
					RouteReason:    "market_state_trend",
					BucketBudget:   0.7,
					Score:          1.15,
					ScoreSource:    "symbol_score",
					PositionBudget: 0.28,
				},
				Router: &strategypb.StrategyRouteRuntimeStatus{
					Enabled:         true,
					Template:        "btc-trend",
					RouteBucket:     "trend",
					TargetReason:    "market_state_trend",
					RuntimeEnabled:  true,
					RuntimeTemplate: "btc-core",
					ApplyAction:     "defer_switch",
					ApplyGateReason: "open_position",
					Warmup: &strategypb.StrategyWarmupStatus{
						HistoryLen_4H:     6,
						HistoryLen_1H:     24,
						HistoryLen_15M:    80,
						HistoryLen_1M:     120,
						Source:            "runtime",
						Status:            "warmup_incomplete",
						IncompleteReasons: []string{"insufficient_4h_history", "insufficient_1h_history", "insufficient_1m_history"},
					},
					RegimeFusion: &strategypb.RegimeFusionStatus{
						H1: &strategypb.RegimeFrameStatus{
							Interval:        "1h",
							State:           "trend_up",
							Reason:          "ema_bull_alignment",
							ReasonDesc:      "EMA 多头排列，更偏向上升趋势",
							RouteReason:     "market_state_trend",
							RouteReasonDesc: "统一判态支持走趋势策略桶",
							Confidence:      0.75,
							LastUpdate:      1714636800000,
							Healthy:         true,
							Fresh:           true,
						},
						M15: &strategypb.RegimeFrameStatus{
							Interval:        "15m",
							State:           "breakout",
							Reason:          "atr_pct_high",
							ReasonDesc:      "波动率偏高，更偏向突破态",
							RouteReason:     "market_state_breakout",
							RouteReasonDesc: "统一判态支持走突破策略桶",
							Confidence:      0.8,
							LastUpdate:      1714637700000,
							Healthy:         true,
							Fresh:           true,
						},
						FusedState:      "trend_up",
						FusedReason:     "h1_primary_dominant",
						FusedReasonDesc: "1H 主周期占优，压过 15M 辅助周期",
						FusedScore:      0.53,
					},
					LatestSecondBar: &strategypb.LatestSecondBarStatus{
						Source:      "depth_fallback",
						Synthetic:   true,
						IsFinal:     true,
						OpenTimeMs:  1700000000000,
						CloseTimeMs: 1700000000999,
						Open:        100.5,
						High:        100.7,
						Low:         100.4,
						Close:       100.6,
						Volume:      0,
					},
				},
			},
		},
	}
	req := httptest.NewRequest(http.MethodGet, "/strategy/status?strategy_id=BTCUSDT", nil)
	rec := httptest.NewRecorder()

	StrategyStatusHandler(serviceContext).ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status code = %d, want 200", rec.Code)
	}
	var body map[string]interface{}
	if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
		t.Fatalf("Unmarshal() error = %v", err)
	}
	router, ok := body["router"].(map[string]interface{})
	if !ok {
		t.Fatalf("router = %#v, want object", body["router"])
	}
	if got := router["target_reason"]; got != "market_state_trend" {
		t.Fatalf("router.target_reason = %#v, want market_state_trend", got)
	}
	if got := router["apply_gate_reason"]; got != "open_position" {
		t.Fatalf("router.apply_gate_reason = %#v, want open_position", got)
	}
	allocator, ok := body["allocator"].(map[string]interface{})
	if !ok {
		t.Fatalf("allocator = %#v, want object", body["allocator"])
	}
	if got := allocator["route_reason"]; got != "market_state_trend" {
		t.Fatalf("allocator.route_reason = %#v, want market_state_trend", got)
	}
	if got := body["message_code"]; got != "allocator_ready" {
		t.Fatalf("message_code = %#v, want allocator_ready", got)
	}
	if got := body["message_desc"]; got != "策略运行中，且已生成最新仓位分配建议" {
		t.Fatalf("message_desc = %#v, want Chinese message description", got)
	}
	summary, ok := body["summary"].(map[string]interface{})
	if !ok {
		t.Fatalf("summary = %#v, want object", body["summary"])
	}
	target, ok := summary["target"].(map[string]interface{})
	if !ok {
		t.Fatalf("summary.target = %#v, want object", summary["target"])
	}
	if got := target["enabled"]; got != true {
		t.Fatalf("summary.target.enabled = %#v, want true", got)
	}
	if got := target["template"]; got != "btc-trend" {
		t.Fatalf("summary.target.template = %#v, want btc-trend", got)
	}
	if got := target["bucket"]; got != "trend" {
		t.Fatalf("summary.target.bucket = %#v, want trend", got)
	}
	if got := target["reason"]; got != "market_state_trend" {
		t.Fatalf("summary.target.reason = %#v, want market_state_trend", got)
	}
	regime, ok := summary["regime"].(map[string]interface{})
	if !ok {
		t.Fatalf("summary.regime = %#v, want object", summary["regime"])
	}
	if got := regime["fused_state"]; got != "trend_up" {
		t.Fatalf("summary.regime.fused_state = %#v, want trend_up", got)
	}
	if got := regime["fused_reason"]; got != "h1_primary_dominant" {
		t.Fatalf("summary.regime.fused_reason = %#v, want h1_primary_dominant", got)
	}
	if got := regime["fused_reason_desc"]; got != "1H 主周期占优，压过 15M 辅助周期" {
		t.Fatalf("summary.regime.fused_reason_desc = %#v, want Chinese desc", got)
	}
	if got := regime["fused_score"]; got != 0.53 {
		t.Fatalf("summary.regime.fused_score = %#v, want 0.53", got)
	}
	h1, ok := regime["h1"].(map[string]interface{})
	if !ok {
		t.Fatalf("summary.regime.h1 = %#v, want object", regime["h1"])
	}
	if got := h1["state"]; got != "trend_up" {
		t.Fatalf("summary.regime.h1.state = %#v, want trend_up", got)
	}
	if got := h1["route_reason"]; got != "market_state_trend" {
		t.Fatalf("summary.regime.h1.route_reason = %#v, want market_state_trend", got)
	}
	m15, ok := regime["m15"].(map[string]interface{})
	if !ok {
		t.Fatalf("summary.regime.m15 = %#v, want object", regime["m15"])
	}
	if got := m15["state"]; got != "breakout" {
		t.Fatalf("summary.regime.m15.state = %#v, want breakout", got)
	}
	if got := m15["route_reason"]; got != "market_state_breakout" {
		t.Fatalf("summary.regime.m15.route_reason = %#v, want market_state_breakout", got)
	}
	warmup, ok := summary["warmup"].(map[string]interface{})
	if !ok {
		t.Fatalf("summary.warmup = %#v, want object", summary["warmup"])
	}
	if got := warmup["status"]; got != "warmup_incomplete" {
		t.Fatalf("summary.warmup.status = %#v, want warmup_incomplete", got)
	}
	if got := warmup["source"]; got != "runtime" {
		t.Fatalf("summary.warmup.source = %#v, want runtime", got)
	}
	if got := warmup["history_len_4h"]; got != float64(6) {
		t.Fatalf("summary.warmup.history_len_4h = %#v, want 6", got)
	}
	if got := warmup["history_len_1m"]; got != float64(120) {
		t.Fatalf("summary.warmup.history_len_1m = %#v, want 120", got)
	}
	incompleteReasons, ok := warmup["incomplete_reasons"].([]interface{})
	if !ok || len(incompleteReasons) != 3 {
		t.Fatalf("summary.warmup.incomplete_reasons = %#v, want 3 reasons", warmup["incomplete_reasons"])
	}
	blockers, ok := summary["blockers"].([]interface{})
	if !ok || len(blockers) < 4 {
		t.Fatalf("summary.blockers = %#v, want gate reason plus warmup blockers", summary["blockers"])
	}
	runtime, ok := summary["runtime"].(map[string]interface{})
	if !ok {
		t.Fatalf("summary.runtime = %#v, want object", summary["runtime"])
	}
	if got := runtime["enabled"]; got != true {
		t.Fatalf("summary.runtime.enabled = %#v, want true", got)
	}
	if got := runtime["template"]; got != "btc-core" {
		t.Fatalf("summary.runtime.template = %#v, want btc-core", got)
	}
	if got := runtime["action"]; got != "defer_switch" {
		t.Fatalf("summary.runtime.action = %#v, want defer_switch", got)
	}
	if got := runtime["gate"]; got != "open_position" {
		t.Fatalf("summary.runtime.gate = %#v, want open_position", got)
	}
	latestSecondBar, ok := summary["latest_second_bar"].(map[string]interface{})
	if !ok {
		t.Fatalf("summary.latest_second_bar = %#v, want object", summary["latest_second_bar"])
	}
	if got := latestSecondBar["source"]; got != "depth_fallback" {
		t.Fatalf("summary.latest_second_bar.source = %#v, want depth_fallback", got)
	}
	if got := latestSecondBar["synthetic"]; got != true {
		t.Fatalf("summary.latest_second_bar.synthetic = %#v, want true", got)
	}
	if got := latestSecondBar["close"]; got != 100.6 {
		t.Fatalf("summary.latest_second_bar.close = %#v, want 100.6", got)
	}
	if got := latestSecondBar["volume"]; got != 0.0 {
		t.Fatalf("summary.latest_second_bar.volume = %#v, want 0", got)
	}
	allocatorSummary, ok := summary["allocator"].(map[string]interface{})
	if !ok {
		t.Fatalf("summary.allocator = %#v, want object", summary["allocator"])
	}
	if got := allocatorSummary["budget"]; got != 0.28 {
		t.Fatalf("summary.allocator.budget = %#v, want 0.28", got)
	}
	if got := allocatorSummary["bucket_budget"]; got != 0.7 {
		t.Fatalf("summary.allocator.bucket_budget = %#v, want 0.7", got)
	}
	if got := allocatorSummary["score"]; got != 1.15 {
		t.Fatalf("summary.allocator.score = %#v, want 1.15", got)
	}
	if got := allocatorSummary["source"]; got != "symbol_score" {
		t.Fatalf("summary.allocator.source = %#v, want symbol_score", got)
	}
	if got := allocatorSummary["paused"]; got != false {
		t.Fatalf("summary.allocator.paused = %#v, want false", got)
	}
}
