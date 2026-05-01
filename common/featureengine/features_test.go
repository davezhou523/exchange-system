package featureengine

import (
	"testing"
	"time"

	"exchange-system/common/pb/market"
)

// TestBuildFromKlineBuildsDerivedFields 验证 Kline 转特征时会补齐 ATR 百分比、趋势强度和波动率。
func TestBuildFromKlineBuildsDerivedFields(t *testing.T) {
	got := BuildFromKline(&market.Kline{
		Symbol:     "BTCUSDT",
		Interval:   "5m",
		Close:      80000,
		Ema21:      79850,
		Ema55:      79500,
		Atr:        160,
		Rsi:        61,
		Volume:     1234,
		EventTime:  time.Date(2026, 5, 1, 12, 0, 0, 0, time.UTC).UnixMilli(),
		IsDirty:    false,
		IsTradable: true,
		IsFinal:    true,
	})

	if got.Symbol != "BTCUSDT" || got.Timeframe != "5m" {
		t.Fatalf("unexpected identity fields: %+v", got)
	}
	if got.AtrPct <= 0 {
		t.Fatalf("atr_pct = %v, want > 0", got.AtrPct)
	}
	if got.Volatility != got.AtrPct {
		t.Fatalf("volatility = %v, want atr_pct %v", got.Volatility, got.AtrPct)
	}
	if got.TrendScore <= 0 {
		t.Fatalf("trend_score = %v, want > 0", got.TrendScore)
	}
	if !got.Healthy || got.LastReason != "healthy_data" {
		t.Fatalf("health = %v/%s, want true/healthy_data", got.Healthy, got.LastReason)
	}
}

// TestNormalizeMarksDirtyFeatureUnhealthy 验证脏数据特征会被统一标记为不健康。
func TestNormalizeMarksDirtyFeatureUnhealthy(t *testing.T) {
	got := Normalize(Features{
		Symbol:     "ETHUSDT",
		Timeframe:  "1m",
		Close:      2000,
		Ema21:      1995,
		Ema55:      1980,
		Atr:        20,
		UpdatedAt:  time.Date(2026, 5, 1, 12, 0, 0, 0, time.UTC),
		IsDirty:    true,
		IsTradable: true,
		IsFinal:    true,
	})

	if got.Healthy {
		t.Fatalf("healthy = true, want false")
	}
	if got.LastReason != "dirty_data" {
		t.Fatalf("last_reason = %s, want dirty_data", got.LastReason)
	}
}
