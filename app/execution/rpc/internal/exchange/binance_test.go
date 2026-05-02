package exchange

import (
	"context"
	"testing"
)

// TestNormalizeOrderQuantityUsesExchangePrecision 验证开仓单也会按交易所真实数量精度向下取整。
func TestNormalizeOrderQuantityUsesExchangePrecision(t *testing.T) {
	client := NewBinanceClient("", "", "", "", 1)
	client.quantityPrecisionMap["BTCUSDT"] = 3

	got, precision, err := client.normalizeOrderQuantity(context.Background(), CreateOrderParam{
		Symbol:   "BTCUSDT",
		Quantity: 0.00538128,
	})
	if err != nil {
		t.Fatalf("normalizeOrderQuantity() error = %v", err)
	}
	if precision != 3 {
		t.Fatalf("precision = %d, want 3", precision)
	}
	if got != 0.005 {
		t.Fatalf("quantity = %.8f, want 0.00500000", got)
	}
}
