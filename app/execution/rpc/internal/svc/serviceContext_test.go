package svc

import (
	"strings"
	"testing"
	"time"

	"exchange-system/app/execution/rpc/internal/exchange"
	strategypb "exchange-system/common/pb/strategy"
)

func TestBuildOrderEventIncludesSignalReasonAndHarvestPathMeta(t *testing.T) {
	t.Parallel()

	svcCtx := &ServiceContext{
		harvestPathSignals: map[string]harvestPathRiskSnapshot{
			"ETHUSDT": {
				Symbol:                 "ETHUSDT",
				EventTime:              time.Now().UnixMilli(),
				HarvestPathProbability: 0.82,
				RuleProbability:        0.73,
				LSTMProbability:        0.64,
				PathAction:             "WAIT_FOR_RECLAIM",
				RiskLevel:              "PATH_ALERT",
				TargetSide:             "UP",
				ReferencePrice:         2450.5,
				MarketPrice:            2448.3,
				ReceivedAt:             time.Now().UTC(),
			},
		},
	}

	sig := &strategypb.Signal{
		StrategyId: "trend-following-ETHUSDT",
		Symbol:     "ETHUSDT",
		Action:     "BUY",
		Side:       "LONG",
		SignalType: "OPEN",
		StopLoss:   2400.5,
		TakeProfits: []float64{
			2480.5,
			2510.5,
		},
		Reason:     "trend aligned",
		Atr:        18.6,
		RiskReward: 2.0,
		Indicators: map[string]float64{
			"m15_rsi": 53.2,
		},
		SignalReason: &strategypb.SignalReason{
			Summary:          "open long",
			Phase:            "OPEN_ENTRY",
			TrendContext:     "4H uptrend",
			SetupContext:     "1H pullback + 15m breakout",
			PathContext:      "harvest_path_guard=enabled",
			ExecutionContext: "atr stop",
			ExitReasonKind:   "take_profit",
			ExitReasonLabel:  "目标止盈",
			Tags:             []string{"15m", "trend_following", "long"},
			RouteBucket:      "trend",
			RouteReason:      "market_state_trend",
			RouteTemplate:    "btc-trend",
			Allocator: &strategypb.PositionAllocatorStatus{
				Template:       "btc-trend",
				RouteBucket:    "trend",
				RouteReason:    "market_state_trend",
				Score:          1.15,
				ScoreSource:    "symbol_score",
				BucketBudget:   0.7,
				StrategyWeight: 0.7,
				SymbolWeight:   0.4,
				RiskScale:      1,
				PositionBudget: 0.28,
			},
		},
	}

	result := &exchange.OrderResult{
		OrderID:          "10001",
		ClientOrderID:    "cid-10001",
		Symbol:           "ETHUSDT",
		Status:           exchange.StatusFilled,
		Side:             exchange.SideBuy,
		PositionSide:     exchange.PosLong,
		ExecutedQuantity: 0.25,
		AvgPrice:         2449.8,
		Commission:       0.51,
		CommissionAsset:  "USDT",
		TransactTime:     1710000000000,
		Slippage:         0.12,
	}

	event := svcCtx.buildOrderEvent(sig, result)

	if got := event["order_id"]; got != "10001" {
		t.Fatalf("unexpected order_id: %v", got)
	}
	if got := event["signal_type"]; got != "OPEN" {
		t.Fatalf("unexpected signal_type: %v", got)
	}
	if got := event["position_side"]; got != "LONG" {
		t.Fatalf("unexpected position_side: %v", got)
	}
	if got := event["quantity"]; got != 0.25 {
		t.Fatalf("unexpected quantity: %v", got)
	}
	if got := event["harvest_path_probability"]; got != 0.82 {
		t.Fatalf("unexpected harvest_path_probability: %v", got)
	}
	if got := event["harvest_path_action"]; got != "WAIT_FOR_RECLAIM" {
		t.Fatalf("unexpected harvest_path_action: %v", got)
	}
	if got := event["harvest_path_target_side"]; got != "UP" {
		t.Fatalf("unexpected harvest_path_target_side: %v", got)
	}

	reason, _ := event["reason"].(string)
	if !strings.Contains(reason, "trend aligned") {
		t.Fatalf("reason should include base reason, got %q", reason)
	}
	if !strings.Contains(reason, "WAIT_FOR_RECLAIM") {
		t.Fatalf("reason should include harvest-path context, got %q", reason)
	}

	indicators, ok := event["indicators"].(map[string]float64)
	if !ok {
		t.Fatalf("indicators type mismatch: %T", event["indicators"])
	}
	if indicators["m15_rsi"] != 53.2 {
		t.Fatalf("unexpected indicators content: %+v", indicators)
	}

	signalReason, ok := event["signal_reason"].(map[string]interface{})
	if !ok {
		t.Fatalf("signal_reason type mismatch: %T", event["signal_reason"])
	}
	if signalReason["summary"] != "open long" {
		t.Fatalf("unexpected signal_reason summary: %v", signalReason["summary"])
	}
	if signalReason["phase"] != "OPEN_ENTRY" {
		t.Fatalf("unexpected signal_reason phase: %v", signalReason["phase"])
	}
	if signalReason["route_reason"] != "market_state_trend" {
		t.Fatalf("unexpected signal_reason route_reason: %v", signalReason["route_reason"])
	}
	if signalReason["exit_reason_kind"] != "take_profit" {
		t.Fatalf("unexpected signal_reason exit_reason_kind: %v", signalReason["exit_reason_kind"])
	}
	if signalReason["exit_reason_label"] != "目标止盈" {
		t.Fatalf("unexpected signal_reason exit_reason_label: %v", signalReason["exit_reason_label"])
	}
	allocator, ok := signalReason["allocator"].(map[string]interface{})
	if !ok {
		t.Fatalf("signal_reason allocator type mismatch: %T", signalReason["allocator"])
	}
	if allocator["score_source"] != "symbol_score" {
		t.Fatalf("unexpected signal_reason allocator score_source: %v", allocator["score_source"])
	}
	tags, ok := signalReason["tags"].([]string)
	if !ok {
		t.Fatalf("signal_reason tags type mismatch: %T", signalReason["tags"])
	}
	if len(tags) != 3 || tags[0] != "15m" {
		t.Fatalf("unexpected signal_reason tags: %#v", tags)
	}
}

func TestResolveProtectionQuantityPrefersExecutedQuantity(t *testing.T) {
	t.Parallel()

	got := resolveProtectionQuantity(&exchange.OrderResult{ExecutedQuantity: 0.1234}, 0.2)
	if got != 0.1234 {
		t.Fatalf("resolveProtectionQuantity() = %.4f, want 0.1234", got)
	}
}

func TestResolveProtectionQuantityFallsBackToRequestedQuantity(t *testing.T) {
	t.Parallel()

	got := resolveProtectionQuantity(&exchange.OrderResult{ExecutedQuantity: 0}, 0.2)
	if got != 0.2 {
		t.Fatalf("resolveProtectionQuantity() = %.4f, want 0.2000", got)
	}
}
