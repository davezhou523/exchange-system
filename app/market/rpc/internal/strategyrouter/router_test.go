package strategyrouter

import (
	"testing"

	"exchange-system/common/featureengine"
	"exchange-system/common/regimejudge"
)

// TestRouteUsesAnalysisToSwitchRange 验证路由器会直接基于 Analysis 切到 range 模板和 range 桶。
func TestRouteUsesAnalysisToSwitchRange(t *testing.T) {
	router := New(Config{
		StaticTemplateMap: map[string]string{
			"ETHUSDT": "eth-core",
		},
		RangeTemplate: "range-core",
	})

	got := router.Route(Input{
		Symbol: "ETHUSDT",
		MarketAnalysis: regimejudge.Analysis{
			RangeMatch: true,
			Features: featureengine.Features{
				AtrPct: 0.002,
			},
		},
	})
	if got.Template != "range-core" || got.Bucket != BucketRange || got.Reason != "market_state_range" {
		t.Fatalf("route = %+v, want range-core/range/market_state_range", got)
	}
}

// TestRouteUsesAnalysisToSwitchBTCTrend 验证路由器会直接基于 Analysis 切到 BTC 趋势模板。
func TestRouteUsesAnalysisToSwitchBTCTrend(t *testing.T) {
	router := New(Config{
		StaticTemplateMap: map[string]string{
			"BTCUSDT": "btc-core",
		},
		BTCTrendTemplate:  "btc-trend",
		BTCTrendAtrPctMax: 0.003,
	})

	got := router.Route(Input{
		Symbol: "BTCUSDT",
		Close:  100,
		Atr:    0.2,
		MarketAnalysis: regimejudge.Analysis{
			BullTrendStrict: true,
			Features: featureengine.Features{
				AtrPct: 0.002,
			},
		},
	})
	if got.Template != "btc-trend" || got.Bucket != BucketTrend || got.Reason != "market_state_trend" {
		t.Fatalf("route = %+v, want btc-trend/trend/market_state_trend", got)
	}
}

// TestRouteDoesNotFallbackToRangeWhenAnalysisAlreadyDisagrees 验证当 Analysis 已存在但未命中 range 时，不再继续被旧 MarketState 抢走。
func TestRouteDoesNotFallbackToRangeWhenAnalysisAlreadyDisagrees(t *testing.T) {
	router := New(Config{
		StaticTemplateMap: map[string]string{
			"ETHUSDT": "eth-core",
		},
		RangeTemplate: "range-core",
	})

	got := router.Route(Input{
		Symbol:      "ETHUSDT",
		MarketState: "range",
		MarketAnalysis: regimejudge.Analysis{
			Healthy: true,
			Fresh:   true,
			Features: featureengine.Features{
				AtrPct: 0.01,
			},
		},
	})
	if got.Template != "eth-core" || got.Bucket != BucketTrend || got.Reason != ReasonHealthyData {
		t.Fatalf("route = %+v, want eth-core/trend/healthy_data", got)
	}
}

// TestRouteDoesNotFallbackToTrendWhenAnalysisAlreadyDisagrees 验证当 Analysis 已存在但未命中趋势时，不再继续回退到 EMA 兼容路径。
func TestRouteDoesNotFallbackToTrendWhenAnalysisAlreadyDisagrees(t *testing.T) {
	router := New(Config{
		StaticTemplateMap: map[string]string{
			"BTCUSDT": "btc-core",
		},
		BTCTrendTemplate:  "btc-trend",
		BTCTrendAtrPctMax: 0.003,
	})

	got := router.Route(Input{
		Symbol:      "BTCUSDT",
		Close:       100,
		Atr:         0.2,
		Ema21:       99,
		Ema55:       98,
		MarketState: "trend_up",
		MarketAnalysis: regimejudge.Analysis{
			Healthy: true,
			Fresh:   true,
			Features: featureengine.Features{
				AtrPct: 0.002,
			},
		},
	})
	if got.Template != "btc-core" || got.Bucket != BucketTrend || got.Reason != ReasonHealthyData {
		t.Fatalf("route = %+v, want btc-core/trend/healthy_data", got)
	}
}
