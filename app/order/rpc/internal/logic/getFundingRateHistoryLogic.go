package logic

import (
	"context"

	"exchange-system/app/order/rpc/internal/svc"
	"exchange-system/common/binance"
	pb "exchange-system/common/pb/order"

	"github.com/zeromicro/go-zero/core/logx"
)

// ---------------------------------------------------------------------------
// GetFundingRateHistory 查询资金费率历史
// ---------------------------------------------------------------------------

// GetFundingRateHistoryLogic 资金费率查询逻辑
type GetFundingRateHistoryLogic struct {
	ctx    context.Context
	svcCtx *svc.ServiceContext
	logx.Logger
}

// NewGetFundingRateHistoryLogic 创建资金费率查询逻辑
func NewGetFundingRateHistoryLogic(ctx context.Context, svcCtx *svc.ServiceContext) *GetFundingRateHistoryLogic {
	return &GetFundingRateHistoryLogic{
		ctx:    ctx,
		svcCtx: svcCtx,
		Logger: logx.WithContext(ctx),
	}
}

// GetFundingRateHistory 查询资金费率历史
func (l *GetFundingRateHistoryLogic) GetFundingRateHistory(in *pb.OrderQueryRequest) (*pb.FundingRateResponse, error) {
	symbol := normalizeSymbol(in.GetSymbol())

	limit := int(in.GetLimit())
	if limit <= 0 {
		limit = 100
	}

	rates, err := l.svcCtx.GetFundingRateHistory(l.ctx, symbol, in.GetStartTime(), in.GetEndTime(), limit)
	if err != nil {
		l.Errorf("GetFundingRateHistory failed: %v", err)
		return nil, err
	}

	items := make([]*pb.FundingRateItem, 0, len(rates))
	for _, r := range rates {
		items = append(items, convertFundingRate(&r))
	}

	return &pb.FundingRateResponse{Items: items}, nil
}

// convertFundingRate 将 binance.FundingRate 转换为 protobuf FundingRateItem
func convertFundingRate(r *binance.FundingRate) *pb.FundingRateItem {
	if r == nil {
		return nil
	}
	return &pb.FundingRateItem{
		Symbol:      r.Symbol,
		FundingRate: r.FundingRate,
		FundingTime: r.FundingTime,
		MarkPrice:   r.MarkPrice,
	}
}
