package logic

import (
	"context"

	"exchange-system/app/order/rpc/internal/svc"
	"exchange-system/common/binance"
	pb "exchange-system/common/pb/order"

	"github.com/zeromicro/go-zero/core/logx"
)

// ---------------------------------------------------------------------------
// GetFundingFees 查询资金费用
// ---------------------------------------------------------------------------

// GetFundingFeesLogic 资金费用查询逻辑
type GetFundingFeesLogic struct {
	ctx    context.Context
	svcCtx *svc.ServiceContext
	logx.Logger
}

// NewGetFundingFeesLogic 创建资金费用查询逻辑
func NewGetFundingFeesLogic(ctx context.Context, svcCtx *svc.ServiceContext) *GetFundingFeesLogic {
	return &GetFundingFeesLogic{
		ctx:    ctx,
		svcCtx: svcCtx,
		Logger: logx.WithContext(ctx),
	}
}

// GetFundingFees 查询资金费用
func (l *GetFundingFeesLogic) GetFundingFees(in *pb.OrderQueryRequest) (*pb.FundingFeeResponse, error) {
	symbol := in.GetSymbol()

	limit := int(in.GetLimit())
	if limit <= 0 {
		limit = 100
	}

	fees, err := l.svcCtx.GetFundingFees(l.ctx, symbol, in.GetStartTime(), in.GetEndTime(), limit)
	if err != nil {
		l.Errorf("GetFundingFees failed: %v", err)
		return nil, err
	}

	items := make([]*pb.FundingFeeItem, 0, len(fees))
	for _, f := range fees {
		items = append(items, convertFundingFee(&f))
	}

	return &pb.FundingFeeResponse{Items: items}, nil
}

// convertFundingFee 将 binance.FundingFee 转换为 protobuf FundingFeeItem
func convertFundingFee(f *binance.FundingFee) *pb.FundingFeeItem {
	if f == nil {
		return nil
	}
	return &pb.FundingFeeItem{
		Symbol:     f.Symbol,
		IncomeType: f.IncomeType,
		Income:     f.Income,
		Asset:      f.Asset,
		Time:       f.Time,
		TranId:     f.TranID,
		TradeId:    f.TradeID,
	}
}
