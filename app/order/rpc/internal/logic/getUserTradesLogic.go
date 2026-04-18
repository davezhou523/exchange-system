package logic

import (
	"context"

	"exchange-system/app/order/rpc/internal/svc"
	"exchange-system/common/binance"
	pb "exchange-system/common/pb/order"

	"github.com/zeromicro/go-zero/core/logx"
)

// ---------------------------------------------------------------------------
// GetUserTrades 查询历史成交
// ---------------------------------------------------------------------------

// GetUserTradesLogic 历史成交查询逻辑
type GetUserTradesLogic struct {
	ctx    context.Context
	svcCtx *svc.ServiceContext
	logx.Logger
}

// NewGetUserTradesLogic 创建历史成交查询逻辑
func NewGetUserTradesLogic(ctx context.Context, svcCtx *svc.ServiceContext) *GetUserTradesLogic {
	return &GetUserTradesLogic{
		ctx:    ctx,
		svcCtx: svcCtx,
		Logger: logx.WithContext(ctx),
	}
}

// GetUserTrades 查询历史成交
func (l *GetUserTradesLogic) GetUserTrades(in *pb.OrderQueryRequest) (*pb.UserTradeResponse, error) {
	symbol := normalizeSymbol(in.GetSymbol())

	limit := int(in.GetLimit())
	if limit <= 0 {
		limit = 500
	}

	trades, err := l.svcCtx.GetUserTrades(l.ctx, symbol, in.GetStartTime(), in.GetEndTime(), limit)
	if err != nil {
		l.Errorf("GetUserTrades failed: %v", err)
		return nil, err
	}

	items := make([]*pb.UserTradeItem, 0, len(trades))
	for _, t := range trades {
		items = append(items, convertUserTrade(&t))
	}

	return &pb.UserTradeResponse{Items: items}, nil
}

// convertUserTrade 将 binance.UserTrade 转换为 protobuf UserTradeItem
func convertUserTrade(t *binance.UserTrade) *pb.UserTradeItem {
	if t == nil {
		return nil
	}
	return &pb.UserTradeItem{
		Id:              t.ID,
		Symbol:          t.Symbol,
		OrderId:         t.OrderID,
		Side:            t.Side,
		PositionSide:    t.PositionSide,
		Price:           t.Price,
		Qty:             t.Qty,
		RealizedPnl:     t.RealizedPnl,
		MarginAsset:     t.MarginAsset,
		QuoteQty:        t.QuoteQty,
		Commission:      t.Commission,
		CommissionAsset: t.CommissionAsset,
		Time:            t.Time,
		Buyer:           t.Buyer,
		Maker:           t.Maker,
	}
}
