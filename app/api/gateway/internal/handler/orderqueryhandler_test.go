package handler

import (
	"strings"
	"testing"

	orderpb "exchange-system/common/pb/order"
)

// TestToSignalReasonViewIncludesRouterAndAllocator 验证 query 侧会把 signal_reason 整理成统一 router + allocator 视图。
func TestToSignalReasonViewIncludesRouterAndAllocator(t *testing.T) {
	got := toSignalReasonView(&orderpb.SignalReason{
		Summary:       "open long",
		RouteBucket:   "trend",
		RouteReason:   "market_state_trend",
		RouteTemplate: "eth-trend",
		Range: &orderpb.RangeSignalReason{
			H1RangeOk:      true,
			H1AdxOk:        true,
			H1BollWidthOk:  true,
			M15TouchLower:  true,
			M15RsiTurnUp:   true,
			M15TouchUpper:  false,
			M15RsiTurnDown: false,
		},
		Allocator: &orderpb.PositionAllocatorStatus{
			Template:       "eth-trend",
			RouteBucket:    "trend",
			RouteReason:    "market_state_trend",
			Score:          1.1,
			ScoreSource:    "symbol_score",
			BucketBudget:   0.6,
			StrategyWeight: 0.6,
			SymbolWeight:   0.5,
			RiskScale:      1,
			PositionBudget: 0.3,
		},
	})
	if got == nil {
		t.Fatal("toSignalReasonView() = nil, want value")
	}
	if got.Router == nil || got.Router.TargetReason != "market_state_trend" || got.Router.Template != "eth-trend" {
		t.Fatalf("router = %+v, want unified router view", got.Router)
	}
	if got.Allocator == nil || got.Allocator.ScoreSource != "symbol_score" || got.Allocator.PositionBudget != 0.3 {
		t.Fatalf("allocator = %+v, want unified allocator view", got.Allocator)
	}
	if got.RouteReason != "market_state_trend" {
		t.Fatalf("route_reason = %s, want market_state_trend", got.RouteReason)
	}
	if got.Range == nil || !got.Range.H1RangeOK || !got.Range.M15TouchLower || !got.Range.M15RsiTurnUp {
		t.Fatalf("range = %+v, want range summary flags", got.Range)
	}
	if got.Range.RegimeSummary != "1H震荡成立" {
		t.Fatalf("range.regime_summary = %q, want 1H震荡成立", got.Range.RegimeSummary)
	}
	if got.Range.EntrySummary != "15M触下轨 | RSI拐头向上" {
		t.Fatalf("range.entry_summary = %q, want long entry summary", got.Range.EntrySummary)
	}
	if got.Range.FullSummary != "1H震荡成立 | 15M触下轨 | RSI拐头向上" {
		t.Fatalf("range.full_summary = %q, want long full summary", got.Range.FullSummary)
	}
	if got.Range.Summary != got.Range.FullSummary {
		t.Fatalf("range.summary = %q, want alias of full_summary %q", got.Range.Summary, got.Range.FullSummary)
	}
}

// TestToSignalReasonViewIncludesRangeShortSummary 验证 query 侧会为震荡做空信号生成可直接展示的摘要。
func TestToSignalReasonViewIncludesRangeShortSummary(t *testing.T) {
	got := toSignalReasonView(&orderpb.SignalReason{
		Summary: "open short",
		Range: &orderpb.RangeSignalReason{
			H1RangeOk:      true,
			H1AdxOk:        true,
			H1BollWidthOk:  true,
			M15TouchUpper:  true,
			M15RsiTurnDown: true,
		},
	})
	if got == nil || got.Range == nil {
		t.Fatalf("toSignalReasonView() = %+v, want range view", got)
	}
	if got.Range.RegimeSummary != "1H震荡成立" {
		t.Fatalf("range.regime_summary = %q, want 1H震荡成立", got.Range.RegimeSummary)
	}
	if got.Range.EntrySummary != "15M触上轨 | RSI拐头向下" {
		t.Fatalf("range.entry_summary = %q, want short entry summary", got.Range.EntrySummary)
	}
	if got.Range.FullSummary != "1H震荡成立 | 15M触上轨 | RSI拐头向下" {
		t.Fatalf("range.full_summary = %q, want short full summary", got.Range.FullSummary)
	}
}

// TestToSignalReasonViewIncludesPartialRangeRegimeSummary 验证 1H 只部分满足时会输出更保守的 regime 摘要。
func TestToSignalReasonViewIncludesPartialRangeRegimeSummary(t *testing.T) {
	got := toSignalReasonView(&orderpb.SignalReason{
		Summary: "watch",
		Range: &orderpb.RangeSignalReason{
			H1RangeOk:     false,
			H1AdxOk:       true,
			M15TouchLower: true,
		},
	})
	if got == nil || got.Range == nil {
		t.Fatalf("toSignalReasonView() = %+v, want range view", got)
	}
	if got.Range.RegimeSummary != "1H震荡未完全成立" {
		t.Fatalf("range.regime_summary = %q, want partial regime summary", got.Range.RegimeSummary)
	}
	if got.Range.EntrySummary != "15M触下轨" {
		t.Fatalf("range.entry_summary = %q, want touch-only summary", got.Range.EntrySummary)
	}
	if got.Range.FullSummary != "1H震荡未完全成立 | 15M触下轨" {
		t.Fatalf("range.full_summary = %q, want combined partial summary", got.Range.FullSummary)
	}
}

// TestSignalReasonDisplaySummaries 验证列表与详情页摘要会优先使用新的 range 分层字段。
func TestSignalReasonDisplaySummaries(t *testing.T) {
	view := toSignalReasonView(&orderpb.SignalReason{
		Summary: "legacy summary",
		Phase:   "OPEN_ENTRY",
		Tags:    []string{"15m", "range", "long"},
		Range: &orderpb.RangeSignalReason{
			H1RangeOk:      true,
			H1AdxOk:        true,
			H1BollWidthOk:  true,
			M15TouchLower:  true,
			M15RsiTurnUp:   true,
			M15TouchUpper:  false,
			M15RsiTurnDown: false,
		},
	})
	if got := signalReasonListSummary(view); got != "1H震荡成立" {
		t.Fatalf("signalReasonListSummary() = %q, want regime summary", got)
	}
	if got := signalReasonEntrySummary(view); got != "15M触下轨 | RSI拐头向上" {
		t.Fatalf("signalReasonEntrySummary() = %q, want entry summary", got)
	}
	if got := signalReasonFullSummary(view); got != "1H震荡成立 | 15M触下轨 | RSI拐头向上" {
		t.Fatalf("signalReasonFullSummary() = %q, want full summary", got)
	}
	if got := cycleReasonBrief(view, nil); !strings.Contains(got, "1H震荡成立 | 15M触下轨 | RSI拐头向上") {
		t.Fatalf("cycleReasonBrief() = %q, want full range summary", got)
	}
}

// TestToProtectionStatusViewIncludesLegResults 验证 query 侧会把保护单结果整理成前端可直接展示的结构。
func TestToProtectionStatusViewIncludesLegResults(t *testing.T) {
	got := toProtectionStatusView(&orderpb.ProtectionStatus{
		Requested: true,
		Status:    "partial_success",
		Reason:    "1/2 protection leg(s) created; set take profit failed",
		StopLoss: &orderpb.ProtectionLegStatus{
			Requested:     true,
			Status:        "success",
			TriggerPrice:  83.6,
			OrderId:       "9001",
			ClientOrderId: "algo-sl-9001",
		},
		TakeProfit: &orderpb.ProtectionLegStatus{
			Requested:    true,
			Status:       "failed",
			TriggerPrice: 83.86,
			Reason:       "set take profit failed",
		},
	})
	if got == nil {
		t.Fatal("toProtectionStatusView() = nil, want value")
	}
	if got.Status != "partial_success" {
		t.Fatalf("status = %s, want partial_success", got.Status)
	}
	if got.StopLoss == nil || got.StopLoss.OrderID != "9001" {
		t.Fatalf("stop_loss = %+v, want order id 9001", got.StopLoss)
	}
	if got.TakeProfit == nil || got.TakeProfit.Reason != "set take profit failed" {
		t.Fatalf("take_profit = %+v, want failure reason", got.TakeProfit)
	}
	if got.Summary == "" {
		t.Fatal("summary = empty, want readable summary")
	}
}

// TestBuildPositionCycleViewsIncludesPartialClose 验证持仓周期聚合会把 PARTIAL_CLOSE 计入平仓侧数量与状态。
func TestBuildPositionCycleViewsIncludesPartialClose(t *testing.T) {
	items := []*orderpb.AllOrderItem{
		{
			OrderId:         20003,
			Symbol:          "ETHUSDT",
			PositionSide:    "LONG",
			ActionType:      "CLOSE_LONG",
			PositionCycleId: "ETHUSDT-LONG-20260503T120000-0001",
			ExecutedQty:     "0.15",
			AvgPrice:        "2465.0",
			UpdateTime:      3000,
			ActualFee:       "0.08",
			EstimatedFee:    "0.08",
			Reason:          "[15m range] 多头止损：价格2450.00 ≤ 止损2450.00",
			SignalReason:    &orderpb.SignalReason{Summary: "[15m range] 多头止损：价格2450.00 ≤ 止损2450.00", Phase: "EXIT", ExecutionContext: "平仓价=2450.00 | 预计盈亏=0.01 | 止损=2450.00 | 数量=0.1500"},
		},
		{
			OrderId:         20002,
			Symbol:          "ETHUSDT",
			PositionSide:    "LONG",
			ActionType:      "PARTIAL_CLOSE_LONG",
			PositionCycleId: "ETHUSDT-LONG-20260503T120000-0001",
			ExecutedQty:     "0.10",
			AvgPrice:        "2460.0",
			UpdateTime:      2000,
			ActualFee:       "0.05",
			EstimatedFee:    "0.05",
			Reason:          "[15m range] 分批止盈：价格2460.00 命中中轨目标2458.00，减仓0.1000",
			SignalReason:    &orderpb.SignalReason{Summary: "[15m range] 分批止盈：价格2460.00 命中中轨目标2458.00，减仓0.1000", Phase: "EXIT"},
		},
		{
			OrderId:         20001,
			Symbol:          "ETHUSDT",
			PositionSide:    "LONG",
			ActionType:      "OPEN_LONG",
			PositionCycleId: "ETHUSDT-LONG-20260503T120000-0001",
			ExecutedQty:     "0.25",
			AvgPrice:        "2450.0",
			UpdateTime:      1000,
			ActualFee:       "0.12",
			EstimatedFee:    "0.12",
			SignalReason:    &orderpb.SignalReason{Summary: "open long", Phase: "OPEN_ENTRY"},
		},
	}

	views, orderToCycle := buildPositionCycleViews(items)
	if len(views) != 1 {
		t.Fatalf("expected 1 cycle view, got %d", len(views))
	}
	view := views[0]
	if view.CycleStatus != "CLOSED" {
		t.Fatalf("cycle_status = %s, want CLOSED", view.CycleStatus)
	}
	if view.CycleStatusLabel != "已平仓" {
		t.Fatalf("cycle_status_label = %s, want 已平仓", view.CycleStatusLabel)
	}
	if view.PositionSideLabel != "多头" {
		t.Fatalf("position_side_label = %s, want 多头", view.PositionSideLabel)
	}
	if view.OpenedQty != "0.25" {
		t.Fatalf("opened_qty = %s, want 0.25", view.OpenedQty)
	}
	if view.ClosedQty != "0.25" {
		t.Fatalf("closed_qty = %s, want 0.25", view.ClosedQty)
	}
	if view.RemainingQty != "0" {
		t.Fatalf("remaining_qty = %s, want 0", view.RemainingQty)
	}
	if view.CloseProgress != "已平0.25 / 0.25，剩余0" {
		t.Fatalf("close_progress = %q, want 已平0.25 / 0.25，剩余0", view.CloseProgress)
	}
	if view.CloseOrderCount != 2 {
		t.Fatalf("close_order_count = %d, want 2", view.CloseOrderCount)
	}
	if view.PartialCloseOrderCount != 1 {
		t.Fatalf("partial_close_order_count = %d, want 1", view.PartialCloseOrderCount)
	}
	if view.FinalCloseOrderCount != 1 {
		t.Fatalf("final_close_order_count = %d, want 1", view.FinalCloseOrderCount)
	}
	if view.ExitActionSummary != "部分止盈1次后最终平仓" {
		t.Fatalf("exit_action_summary = %q, want 部分止盈1次后最终平仓", view.ExitActionSummary)
	}
	if view.ExitReasonKind != "break_even_stop" {
		t.Fatalf("exit_reason_kind = %q, want break_even_stop", view.ExitReasonKind)
	}
	if view.ExitReasonLabel != "保本止损" {
		t.Fatalf("exit_reason_label = %q, want 保本止损", view.ExitReasonLabel)
	}
	if !strings.Contains(view.ExitReasonDetail, "多头止损") {
		t.Fatalf("exit_reason_detail = %q, want raw stop-loss detail", view.ExitReasonDetail)
	}
	if len(view.CloseOrderIDs) != 2 {
		t.Fatalf("close_order_ids len = %d, want 2", len(view.CloseOrderIDs))
	}
	if orderToCycle[20002] != view.PositionCycleID {
		t.Fatalf("partial close order should map to cycle %s, got %s", view.PositionCycleID, orderToCycle[20002])
	}
}

// TestActionTypeLabelIncludesPartialClose 验证查询层会把部分平仓动作映射为可读中文标签。
func TestActionTypeLabelIncludesPartialClose(t *testing.T) {
	if got := actionTypeLabel("PARTIAL_CLOSE_LONG"); got != "部分平多" {
		t.Fatalf("actionTypeLabel(PARTIAL_CLOSE_LONG) = %q, want 部分平多", got)
	}
	if got := actionTypeLabel("PARTIAL_CLOSE_SHORT"); got != "部分平空" {
		t.Fatalf("actionTypeLabel(PARTIAL_CLOSE_SHORT) = %q, want 部分平空", got)
	}
}

// TestPositionCycleLabelsAndSummaryHelpers 验证持仓周期新增的中文标签和退出摘要辅助方法。
func TestPositionCycleLabelsAndSummaryHelpers(t *testing.T) {
	if got := positionSideLabel("SHORT"); got != "空头" {
		t.Fatalf("positionSideLabel(SHORT) = %q, want 空头", got)
	}
	if got := cycleStatusLabel("PARTIALLY_CLOSED"); got != "部分平仓" {
		t.Fatalf("cycleStatusLabel(PARTIALLY_CLOSED) = %q, want 部分平仓", got)
	}
	if got := closeProgressSummary(0.25, 0.10, 0.15); got != "已平0.1 / 0.25，剩余0.15" {
		t.Fatalf("closeProgressSummary() = %q, want 已平0.1 / 0.25，剩余0.15", got)
	}
	if got := exitActionSummary("PARTIALLY_CLOSED", 2, 0); got != "已部分平仓2次" {
		t.Fatalf("exitActionSummary(partial) = %q, want 已部分平仓2次", got)
	}
	if got := exitActionSummary("CLOSED", 0, 1); got != "直接平仓" {
		t.Fatalf("exitActionSummary(final) = %q, want 直接平仓", got)
	}
}

// TestNormalizeExitReason 验证 query 侧会把不同出场文案统一归类为稳定的 kind/label。
func TestNormalizeExitReason(t *testing.T) {
	tests := []struct {
		name         string
		actionType   string
		rawReason    string
		signalReason *signalReasonView
		wantKind     string
		wantLabel    string
	}{
		{
			name:       "partial close",
			actionType: "PARTIAL_CLOSE_LONG",
			rawReason:  "[15m range] 分批止盈：价格2460.00 命中中轨目标2458.00，减仓0.1000",
			signalReason: &signalReasonView{
				ExitReasonKind:  "partial_take_profit",
				ExitReasonLabel: "分批止盈",
			},
			wantKind:  "partial_take_profit",
			wantLabel: "分批止盈",
		},
		{
			name:       "rsi take profit",
			actionType: "CLOSE_LONG",
			rawReason:  "[15m range] RSI止盈：RSI=72.00 ≥ 70.00",
			signalReason: &signalReasonView{
				ExitReasonKind:  "rsi_take_profit",
				ExitReasonLabel: "RSI止盈",
			},
			wantKind:  "rsi_take_profit",
			wantLabel: "RSI止盈",
		},
		{
			name:       "breakeven stop",
			actionType: "CLOSE_LONG",
			rawReason:  "[15m range] 多头止损：价格2450.00 ≤ 止损2450.00",
			signalReason: &signalReasonView{
				Summary:          "[15m range] 多头止损：价格2450.00 ≤ 止损2450.00",
				ExecutionContext: "平仓价=2450.00 | 预计盈亏=0.01 | 止损=2450.00 | 数量=0.1500",
			},
			wantKind:  "break_even_stop",
			wantLabel: "保本止损",
		},
		{
			name:       "generic final close",
			actionType: "CLOSE_LONG",
			rawReason:  "close long",
			signalReason: &signalReasonView{
				ExitReasonKind:  "final_close",
				ExitReasonLabel: "最终平仓",
			},
			wantKind:  "final_close",
			wantLabel: "最终平仓",
		},
		{
			name:       "structured fields win over raw reason",
			actionType: "CLOSE_LONG",
			rawReason:  "close long",
			signalReason: &signalReasonView{
				ExitReasonKind:  "break_even_stop",
				ExitReasonLabel: "保本止损",
			},
			wantKind:  "break_even_stop",
			wantLabel: "保本止损",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := normalizeExitReason(tt.actionType, tt.rawReason, tt.signalReason)
			if got.Kind != tt.wantKind {
				t.Fatalf("kind = %q, want %q (got=%+v)", got.Kind, tt.wantKind, got)
			}
			if got.Label != tt.wantLabel {
				t.Fatalf("label = %q, want %q (got=%+v)", got.Label, tt.wantLabel, got)
			}
			if strings.TrimSpace(got.Detail) == "" {
				t.Fatalf("detail = empty, want raw reason preserved (got=%+v)", got)
			}
		})
	}
}

// TestExtractPNLFromExecutionContext 验证保本止损识别会从 execution_context 中提取盈亏字段。
func TestExtractPNLFromExecutionContext(t *testing.T) {
	got, ok := extractPNLFromExecutionContext(&signalReasonView{
		ExecutionContext: "平仓价=2450.00 | 已实现盈亏=-0.03 | 止损=2450.00 | 数量=0.1500",
	})
	if !ok {
		t.Fatal("extractPNLFromExecutionContext() ok = false, want true")
	}
	if got != -0.03 {
		t.Fatalf("extractPNLFromExecutionContext() = %.4f, want -0.03", got)
	}
}
