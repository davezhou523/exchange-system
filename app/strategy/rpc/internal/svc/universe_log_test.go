package svc

import (
	"testing"
	"time"

	"exchange-system/app/strategy/rpc/internal/universe"
)

// TestBuildUniverseLogEntryIncludesRouteDetail 验证 universe 日志会显式写出策略路由原因和策略桶明细。
func TestBuildUniverseLogEntryIncludesRouteDetail(t *testing.T) {
	now := time.Date(2026, 5, 2, 12, 0, 0, 0, time.UTC)
	entry := buildUniverseLogEntry(now, universe.DesiredStrategy{
		Symbol:       "BTCUSDT",
		BaseTemplate: "btc-core",
		Template:     "btc-trend",
		Bucket:       "trend",
		Reason:       "market_state_trend",
		Enabled:      true,
	}, universe.Snapshot{
		Symbol:       "BTCUSDT",
		UpdatedAt:    now.Add(-30 * time.Second),
		LastEventMs:  now.UnixMilli(),
		IsTradable:   true,
		IsFinal:      true,
		LastInterval: "1m",
	}, universeApplyResult{
		Action:  "switch",
		Reason:  "market_state_trend",
		Enabled: true,
	})

	if entry.RouteBucket != "trend" {
		t.Fatalf("route_bucket = %s, want trend", entry.RouteBucket)
	}
	if entry.RouteReason != "market_state_trend" {
		t.Fatalf("route_reason = %s, want market_state_trend", entry.RouteReason)
	}
	if entry.Reason != "market_state_trend" {
		t.Fatalf("reason = %s, want market_state_trend", entry.Reason)
	}
}
