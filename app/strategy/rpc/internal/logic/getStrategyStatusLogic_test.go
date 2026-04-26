package logic

import (
	"context"
	"strings"
	"testing"

	"exchange-system/app/strategy/rpc/internal/svc"
	"exchange-system/app/strategy/rpc/internal/weights"
	strategypb "exchange-system/common/pb/strategy"
)

func TestGetStrategyStatusIncludesWeightRecommendation(t *testing.T) {
	svcCtx := &svc.ServiceContext{}
	svcCtxWeight := weights.Recommendation{
		Symbol:         "BTCUSDT",
		Template:       "btc-trend",
		StrategyWeight: 0.7,
		SymbolWeight:   0.4,
		RiskScale:      1.0,
		PositionBudget: 0.28,
	}
	svcCtx.RecordLatestWeightRecommendation(svcCtxWeight)

	logic := NewGetStrategyStatusLogic(context.Background(), svcCtx)
	got, err := logic.GetStrategyStatus(&strategypb.StrategyRequest{StrategyId: "BTCUSDT"})
	if err != nil {
		t.Fatalf("GetStrategyStatus() error = %v", err)
	}
	if got.Status != "STOPPED" {
		t.Fatalf("status = %s, want STOPPED", got.Status)
	}
	if !strings.Contains(got.Message, "budget=0.2800") || !strings.Contains(got.Message, "risk=1.0000") {
		t.Fatalf("message = %q, want weight summary", got.Message)
	}
}
