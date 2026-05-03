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
				StrategyId: "BTCUSDT",
				Status:     "RUNNING",
				Message:    "ok",
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
							Interval:    "1h",
							State:       "trend_up",
							RouteReason: "market_state_trend",
						},
						M15: &strategypb.RegimeFrameStatus{
							Interval:    "15m",
							State:       "breakout",
							RouteReason: "market_state_breakout",
						},
						FusedState:  "trend_up",
						FusedReason: "h1_primary_dominant",
						FusedScore:  0.53,
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
	summary, ok := body["summary"].(map[string]interface{})
	if !ok {
		t.Fatalf("summary = %#v, want object", body["summary"])
	}
	if got := summary["target"]; got != "target enabled=true template=btc-trend bucket=trend reason=market_state_trend" {
		t.Fatalf("summary.target = %#v, want target summary", got)
	}
	if got := summary["regime"]; got != "fused=trend_up score=0.53 1h=trend_up/market_state_trend 15m=breakout/market_state_breakout reason=h1_primary_dominant" {
		t.Fatalf("summary.regime = %#v, want regime summary", got)
	}
	if got := summary["warmup"]; got != "warmup_incomplete source=runtime 4h=6 1h=24 15m=80 1m=120 reasons=insufficient_4h_history,insufficient_1h_history,insufficient_1m_history" {
		t.Fatalf("summary.warmup = %#v, want warmup summary", got)
	}
	blockers, ok := summary["blockers"].([]interface{})
	if !ok || len(blockers) < 4 {
		t.Fatalf("summary.blockers = %#v, want gate reason plus warmup blockers", summary["blockers"])
	}
	if got := summary["runtime"]; got != "runtime enabled=true template=btc-core action=defer_switch gate=open_position" {
		t.Fatalf("summary.runtime = %#v, want runtime summary", got)
	}
	if got := summary["allocator"]; got != "budget=0.2800 bucket=0.7000 score=1.1500 source=symbol_score paused=false" {
		t.Fatalf("summary.allocator = %#v, want allocator summary", got)
	}
}
