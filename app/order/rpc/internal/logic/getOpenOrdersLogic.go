package logic

import (
	"context"

	"exchange-system/app/order/rpc/internal/svc"
	"exchange-system/common/binance"
	pb "exchange-system/common/pb/order"

	"github.com/zeromicro/go-zero/core/logx"
)

// ---------------------------------------------------------------------------
// GetOpenOrders 查询当前委托（未成交订单）
// ---------------------------------------------------------------------------

// GetOpenOrdersLogic 当前委托查询逻辑
type GetOpenOrdersLogic struct {
	ctx    context.Context
	svcCtx *svc.ServiceContext
	logx.Logger
}

// NewGetOpenOrdersLogic 创建当前委托查询逻辑
func NewGetOpenOrdersLogic(ctx context.Context, svcCtx *svc.ServiceContext) *GetOpenOrdersLogic {
	return &GetOpenOrdersLogic{
		ctx:    ctx,
		svcCtx: svcCtx,
		Logger: logx.WithContext(ctx),
	}
}

// GetOpenOrders 查询当前委托（未成交订单）
func (l *GetOpenOrdersLogic) GetOpenOrders(in *pb.OrderQueryRequest) (*pb.OpenOrderResponse, error) {
	symbol := normalizeSymbol(in.GetSymbol())

	orders, err := l.svcCtx.GetOpenOrders(l.ctx, symbol)
	if err != nil {
		l.Errorf("GetOpenOrders failed: %v", err)
		return nil, err
	}

	items := make([]*pb.OpenOrderItem, 0, len(orders))
	for _, o := range orders {
		items = append(items, convertOpenOrder(&o))
	}

	return &pb.OpenOrderResponse{Items: items}, nil
}

// convertOpenOrder 将 binance.OpenOrder 转换为 protobuf OpenOrderItem
func convertOpenOrder(o *binance.OpenOrder) *pb.OpenOrderItem {
	if o == nil {
		return nil
	}
	return &pb.OpenOrderItem{
		OrderId:       o.OrderID,
		Symbol:        o.Symbol,
		Status:        o.Status,
		Side:          o.Side,
		PositionSide:  o.PositionSide,
		Type:          o.Type,
		OrigQty:       o.OrigQty,
		ExecutedQty:   o.ExecutedQty,
		AvgPrice:      o.AvgPrice,
		Price:         o.Price,
		StopPrice:     o.StopPrice,
		ClientOrderId: o.ClientOrderID,
		Time:          o.Time,
		UpdateTime:    o.UpdateTime,
		ReduceOnly:    o.ReduceOnly,
		ClosePosition: o.ClosePosition,
		TimeInForce:   o.TimeInForce,
	}
}
