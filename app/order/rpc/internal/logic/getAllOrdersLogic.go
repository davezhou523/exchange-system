package logic

import (
	"context"

	"exchange-system/app/order/rpc/internal/svc"
	"exchange-system/common/binance"
	pb "exchange-system/common/pb/order"

	"github.com/zeromicro/go-zero/core/logx"
)

// ---------------------------------------------------------------------------
// GetAllOrders 查询历史委托
// ---------------------------------------------------------------------------

// GetAllOrdersLogic 历史委托查询逻辑
type GetAllOrdersLogic struct {
	ctx    context.Context
	svcCtx *svc.ServiceContext
	logx.Logger
}

// NewGetAllOrdersLogic 创建历史委托查询逻辑
func NewGetAllOrdersLogic(ctx context.Context, svcCtx *svc.ServiceContext) *GetAllOrdersLogic {
	return &GetAllOrdersLogic{
		ctx:    ctx,
		svcCtx: svcCtx,
		Logger: logx.WithContext(ctx),
	}
}

// GetAllOrders 查询历史委托
func (l *GetAllOrdersLogic) GetAllOrders(in *pb.OrderQueryRequest) (*pb.AllOrderResponse, error) {
	symbol := in.GetSymbol()
	if symbol == "" {
		return nil, errSymbolRequired
	}

	limit := int(in.GetLimit())
	if limit <= 0 {
		limit = 500
	}

	orders, err := l.svcCtx.GetAllOrders(l.ctx, symbol, in.GetStartTime(), in.GetEndTime(), limit)
	if err != nil {
		l.Errorf("GetAllOrders failed: %v", err)
		return nil, err
	}

	items := make([]*pb.AllOrderItem, 0, len(orders))
	for _, o := range orders {
		items = append(items, convertAllOrder(&o))
	}

	return &pb.AllOrderResponse{Items: items}, nil
}

// convertAllOrder 将 binance.AllOrder 转换为 protobuf AllOrderItem
func convertAllOrder(o *binance.AllOrder) *pb.AllOrderItem {
	if o == nil {
		return nil
	}
	return &pb.AllOrderItem{
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
