package logic

import (
	"context"

	"exchange-system/app/order/rpc/internal/svc"
	"exchange-system/common/binance"
	pb "exchange-system/common/pb/order"

	"github.com/zeromicro/go-zero/core/logx"
)

// ---------------------------------------------------------------------------
// GetIncomeHistory 查询资金流水
// ---------------------------------------------------------------------------

// GetIncomeHistoryLogic 资金流水查询逻辑
type GetIncomeHistoryLogic struct {
	ctx    context.Context
	svcCtx *svc.ServiceContext
	logx.Logger
}

// NewGetIncomeHistoryLogic 创建资金流水查询逻辑
func NewGetIncomeHistoryLogic(ctx context.Context, svcCtx *svc.ServiceContext) *GetIncomeHistoryLogic {
	return &GetIncomeHistoryLogic{
		ctx:    ctx,
		svcCtx: svcCtx,
		Logger: logx.WithContext(ctx),
	}
}

// GetIncomeHistory 查询资金流水
func (l *GetIncomeHistoryLogic) GetIncomeHistory(in *pb.OrderQueryRequest) (*pb.IncomeResponse, error) {
	symbol := in.GetSymbol()
	incomeType := in.GetIncomeType()

	limit := int(in.GetLimit())
	if limit <= 0 {
		limit = 100
	}

	incomes, err := l.svcCtx.GetIncomeHistory(l.ctx, symbol, incomeType, in.GetStartTime(), in.GetEndTime(), limit)
	if err != nil {
		l.Errorf("GetIncomeHistory failed: %v", err)
		return nil, err
	}

	items := make([]*pb.IncomeItem, 0, len(incomes))
	for _, inc := range incomes {
		items = append(items, convertIncome(&inc))
	}

	return &pb.IncomeResponse{Items: items}, nil
}

// convertIncome 将 binance.Income 转换为 protobuf IncomeItem
func convertIncome(inc *binance.Income) *pb.IncomeItem {
	if inc == nil {
		return nil
	}
	return &pb.IncomeItem{
		Symbol:     inc.Symbol,
		IncomeType: inc.IncomeType,
		Income:     inc.Income,
		Asset:      inc.Asset,
		Time:       inc.Time,
		TranId:     inc.TranID,
		TradeId:    inc.TradeID,
	}
}
