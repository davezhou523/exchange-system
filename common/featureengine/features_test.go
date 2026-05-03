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

// TestBuildFromSnapshotPreservesExplicitHealth 验证显式健康状态会覆盖默认推导结果，便于上游复用现成健康判定。
func TestBuildFromSnapshotPreservesExplicitHealth(t *testing.T) {
	got := BuildFromSnapshot(SnapshotValues{
		Symbol:     "DOGEUSDT",
		Timeframe:  "1m",
		Close:      0.11,
		Ema21:      0.109,
		Ema55:      0.108,
		Atr:        0.001,
		Volume:     123,
		UpdatedAt:  time.Date(2026, 5, 2, 12, 0, 0, 0, time.UTC),
		IsTradable: true,
		IsFinal:    true,
		Healthy:    false,
		HasHealth:  true,
		LastReason: "bootstrap_no_snapshot",
	})

	if got.Healthy {
		t.Fatalf("healthy = true, want false")
	}
	if got.LastReason != "bootstrap_no_snapshot" {
		t.Fatalf("last_reason = %s, want bootstrap_no_snapshot", got.LastReason)
	}
}

// TestBuildFeatureMapBuildsBatch 验证 Feature Engine 可以批量产出统一特征，供上层模块共享。
func TestBuildFeatureMapBuildsBatch(t *testing.T) {
	got := BuildFeatureMap(map[string]SnapshotValues{
		"BTCUSDT": {
			Timeframe:  "1m",
			Close:      80000,
			Ema21:      79900,
			Ema55:      79800,
			Atr:        120,
			Volume:     888,
			UpdatedAt:  time.Date(2026, 5, 2, 12, 0, 0, 0, time.UTC),
			IsTradable: true,
			IsFinal:    true,
		},
		"ETHUSDT": {
			Symbol:     "ETHUSDT",
			Timeframe:  "5m",
			Close:      2000,
			Ema21:      1995,
			Ema55:      1990,
			Atr:        15,
			Volume:     456,
			UpdatedAt:  time.Date(2026, 5, 2, 12, 0, 0, 0, time.UTC),
			IsTradable: true,
			IsFinal:    true,
		},
	})

	if len(got) != 2 {
		t.Fatalf("len(features) = %d, want 2", len(got))
	}
	if got["BTCUSDT"].Symbol != "BTCUSDT" {
		t.Fatalf("BTC symbol = %s, want BTCUSDT", got["BTCUSDT"].Symbol)
	}
	if got["ETHUSDT"].Timeframe != "5m" {
		t.Fatalf("ETH timeframe = %s, want 5m", got["ETHUSDT"].Timeframe)
	}
	if got["BTCUSDT"].AtrPct <= 0 || got["ETHUSDT"].TrendScore <= 0 {
		t.Fatalf("derived fields not populated: BTC=%+v ETH=%+v", got["BTCUSDT"], got["ETHUSDT"])
	}
}
