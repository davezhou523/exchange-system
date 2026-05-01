package symbolranker

import (
	"testing"
	"time"

	"exchange-system/common/featureengine"
)

// TestRankSymbolsOrdersByWeightedScore 验证选币引擎会按归一化后的综合得分降序输出。
func TestRankSymbolsOrdersByWeightedScore(t *testing.T) {
	ranker := New(Weights{
		TrendScore: 0.4,
		Volatility: 0.3,
		Volume:     0.3,
	})

	now := time.Date(2026, 5, 1, 12, 0, 0, 0, time.UTC)
	got := ranker.RankSymbols([]featureengine.Features{
		featureengine.Normalize(featureengine.Features{
			Symbol:     "SOLUSDT",
			TrendScore: 0.95,
			Volatility: 0.75,
			Volume:     0.80,
			UpdatedAt:  now,
			IsTradable: true,
			IsFinal:    true,
		}),
		featureengine.Normalize(featureengine.Features{
			Symbol:     "ETHUSDT",
			TrendScore: 0.80,
			Volatility: 0.60,
			Volume:     0.70,
			UpdatedAt:  now,
			IsTradable: true,
			IsFinal:    true,
		}),
		featureengine.Normalize(featureengine.Features{
			Symbol:     "BNBUSDT",
			TrendScore: 0.70,
			Volatility: 0.55,
			Volume:     0.65,
			UpdatedAt:  now,
			IsTradable: true,
			IsFinal:    true,
		}),
	})

	if len(got) != 3 {
		t.Fatalf("len(scores) = %d, want 3", len(got))
	}
	if got[0].Symbol != "SOLUSDT" || got[1].Symbol != "ETHUSDT" || got[2].Symbol != "BNBUSDT" {
		t.Fatalf("ranking order = %#v, want SOLUSDT -> ETHUSDT -> BNBUSDT", got)
	}
}

// TestRankSymbolsFiltersUnhealthyFeatures 验证不健康样本不会进入最终候选排序结果。
func TestRankSymbolsFiltersUnhealthyFeatures(t *testing.T) {
	ranker := New(Weights{})
	now := time.Date(2026, 5, 1, 12, 0, 0, 0, time.UTC)

	got := ranker.RankSymbols([]featureengine.Features{
		featureengine.Normalize(featureengine.Features{
			Symbol:     "BTCUSDT",
			TrendScore: 0.8,
			Volatility: 0.6,
			Volume:     0.9,
			UpdatedAt:  now,
			IsTradable: true,
			IsFinal:    true,
		}),
		featureengine.Normalize(featureengine.Features{
			Symbol:     "BADUSDT",
			TrendScore: 1,
			Volatility: 1,
			Volume:     1,
			UpdatedAt:  now,
			IsDirty:    true,
			IsTradable: true,
			IsFinal:    true,
		}),
	})

	if len(got) != 1 {
		t.Fatalf("len(scores) = %d, want 1", len(got))
	}
	if got[0].Symbol != "BTCUSDT" {
		t.Fatalf("top symbol = %s, want BTCUSDT", got[0].Symbol)
	}
}

// TestTopNReturnsLeadingScores 验证 TopN 会安全截取前 N 个候选币。
func TestTopNReturnsLeadingScores(t *testing.T) {
	ranker := New(Weights{})
	scores := []SymbolScore{
		{Symbol: "SOLUSDT", Score: 0.95},
		{Symbol: "ETHUSDT", Score: 0.82},
		{Symbol: "BNBUSDT", Score: 0.74},
	}

	got := ranker.TopN(scores, 2)
	if len(got) != 2 {
		t.Fatalf("len(topn) = %d, want 2", len(got))
	}
	if got[0].Symbol != "SOLUSDT" || got[1].Symbol != "ETHUSDT" {
		t.Fatalf("topn = %#v, want SOLUSDT / ETHUSDT", got)
	}
}
