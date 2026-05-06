package position

import (
	"testing"

	"exchange-system/app/execution/rpc/internal/exchange"
)

// TestUpdateFromOrderPartialCloseKeepsRemainingPosition 验证 PARTIAL_CLOSE 成交后会保留剩余仓位，而不是误判为清仓。
func TestUpdateFromOrderPartialCloseKeepsRemainingPosition(t *testing.T) {
	t.Parallel()

	manager := NewManager()
	manager.positions["ETHUSDT"] = &PositionState{
		Symbol:         "ETHUSDT",
		PositionAmount: 1.1607,
		EntryPrice:     2416.48,
		StrategyID:     "trend-following-ETHUSDT",
	}

	manager.UpdateFromOrder(&exchange.OrderResult{
		Symbol:           "ETHUSDT",
		Side:             exchange.SideSell,
		PositionSide:     exchange.PosLong,
		ExecutedQuantity: 0.4010,
		AvgPrice:         2390.85,
	}, "trend-following-ETHUSDT", "PARTIAL_CLOSE", 0, nil)

	pos, ok := manager.GetPosition("ETHUSDT")
	if !ok {
		t.Fatal("GetPosition() ok = false, want true")
	}
	if pos.PositionAmount != 0.7597 {
		t.Fatalf("position amount = %.4f, want 0.7597", pos.PositionAmount)
	}
}

// TestUpdateFromOrderPartialCloseWithoutLocalPositionSkipsDelete 验证本地无仓时收到 PARTIAL_CLOSE 不会生成负仓位或误删仓位。
func TestUpdateFromOrderPartialCloseWithoutLocalPositionSkipsDelete(t *testing.T) {
	t.Parallel()

	manager := NewManager()
	manager.UpdateFromOrder(&exchange.OrderResult{
		Symbol:           "ETHUSDT",
		Side:             exchange.SideSell,
		PositionSide:     exchange.PosLong,
		ExecutedQuantity: 0.4010,
		AvgPrice:         2390.85,
	}, "trend-following-ETHUSDT", "PARTIAL_CLOSE", 0, nil)

	if _, ok := manager.GetPosition("ETHUSDT"); ok {
		t.Fatal("GetPosition() ok = true, want false")
	}
}
