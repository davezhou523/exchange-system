package svc

import (
	"testing"

	marketpb "exchange-system/common/pb/market"
)

// TestSecondBarAssemblerOnTradeFinalizesPreviousSecond 验证成交跨秒时会正确封口上一秒真实成交桶。
func TestSecondBarAssemblerOnTradeFinalizesPreviousSecond(t *testing.T) {
	assembler := NewSecondBarAssembler("BTCUSDT")

	if bars := assembler.OnTrade(&marketpb.Trade{
		Symbol:    "BTCUSDT",
		TradeId:   1,
		Price:     100,
		Quantity:  2,
		Timestamp: 1_700_000_000_100,
	}); len(bars) != 0 {
		t.Fatalf("first trade returned %d bars, want 0", len(bars))
	}

	if bars := assembler.OnTrade(&marketpb.Trade{
		Symbol:    "BTCUSDT",
		TradeId:   2,
		Price:     101,
		Quantity:  3,
		Timestamp: 1_700_000_000_800,
	}); len(bars) != 0 {
		t.Fatalf("same-second trade returned %d bars, want 0", len(bars))
	}

	bars := assembler.OnTrade(&marketpb.Trade{
		Symbol:    "BTCUSDT",
		TradeId:   3,
		Price:     99,
		Quantity:  1,
		Timestamp: 1_700_000_001_050,
	})
	if len(bars) != 1 {
		t.Fatalf("cross-second trade returned %d bars, want 1", len(bars))
	}

	got := bars[0]
	if !got.IsFinal {
		t.Fatalf("IsFinal = false, want true")
	}
	if got.Synthetic {
		t.Fatalf("Synthetic = true, want false")
	}
	if got.Open != 100 || got.High != 101 || got.Low != 100 || got.Close != 101 {
		t.Fatalf("OHLC = %.2f/%.2f/%.2f/%.2f, want 100/101/100/101", got.Open, got.High, got.Low, got.Close)
	}
	if got.Volume != 5 {
		t.Fatalf("Volume = %.2f, want 5", got.Volume)
	}
}

// TestSecondBarAssemblerOnDepthFallbackFinalizesSyntheticBar 验证开启 depth fallback 后，无成交秒也能按 mid-price 封口 synthetic bar。
func TestSecondBarAssemblerOnDepthFallbackFinalizesSyntheticBar(t *testing.T) {
	assembler := NewSecondBarAssembler("BTCUSDT", SecondBarAssemblerConfig{
		EnableDepthMidFallback: true,
	})

	if bars := assembler.OnDepth(buildDepth("BTCUSDT", 1_700_000_000_100, 100, 101)); len(bars) != 0 {
		t.Fatalf("first depth returned %d bars, want 0", len(bars))
	}

	bars := assembler.OnDepth(buildDepth("BTCUSDT", 1_700_000_001_100, 102, 104))
	if len(bars) != 1 {
		t.Fatalf("second depth returned %d bars, want 1", len(bars))
	}

	got := bars[0]
	if !got.IsFinal {
		t.Fatalf("IsFinal = false, want true")
	}
	if !got.Synthetic {
		t.Fatalf("Synthetic = false, want true")
	}
	if got.Open != 100.5 || got.High != 100.5 || got.Low != 100.5 || got.Close != 100.5 {
		t.Fatalf("OHLC = %.2f/%.2f/%.2f/%.2f, want 100.5/100.5/100.5/100.5", got.Open, got.High, got.Low, got.Close)
	}
	if got.Volume != 0 {
		t.Fatalf("Volume = %.2f, want 0", got.Volume)
	}
}

// TestSecondBarAssemblerTradeOverridesSyntheticSameSecond 验证同秒 synthetic 桶遇到首笔真实成交时，会切回真实成交开高低收。
func TestSecondBarAssemblerTradeOverridesSyntheticSameSecond(t *testing.T) {
	assembler := NewSecondBarAssembler("BTCUSDT", SecondBarAssemblerConfig{
		EnableDepthMidFallback: true,
	})

	_ = assembler.OnDepth(buildDepth("BTCUSDT", 1_700_000_000_100, 100, 102))

	if bars := assembler.OnTrade(&marketpb.Trade{
		Symbol:    "BTCUSDT",
		TradeId:   1,
		Price:     105,
		Quantity:  1.5,
		Timestamp: 1_700_000_000_700,
	}); len(bars) != 0 {
		t.Fatalf("same-second trade returned %d bars, want 0", len(bars))
	}

	bars := assembler.OnDepth(buildDepth("BTCUSDT", 1_700_000_001_100, 106, 108))
	if len(bars) != 1 {
		t.Fatalf("next-second depth returned %d bars, want 1", len(bars))
	}

	got := bars[0]
	if got.Open != 105 || got.High != 105 || got.Low != 105 || got.Close != 105 {
		t.Fatalf("OHLC = %.2f/%.2f/%.2f/%.2f, want 105/105/105/105", got.Open, got.High, got.Low, got.Close)
	}
	if got.Synthetic {
		t.Fatalf("Synthetic = true, want false")
	}
	if got.Volume != 1.5 {
		t.Fatalf("Volume = %.2f, want 1.5", got.Volume)
	}
}

// buildDepth 构造测试用最小盘口，避免每个用例重复拼装 bids/asks。
func buildDepth(symbol string, ts int64, bid, ask float64) *marketpb.Depth {
	return &marketpb.Depth{
		Symbol:    symbol,
		Timestamp: ts,
		Bids: []*marketpb.DepthItem{
			{Price: bid, Quantity: 1},
		},
		Asks: []*marketpb.DepthItem{
			{Price: ask, Quantity: 1},
		},
	}
}
