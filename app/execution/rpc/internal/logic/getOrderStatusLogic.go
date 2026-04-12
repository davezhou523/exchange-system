package logic

import (
	"context"
	"strings"

	"exchange-system/app/execution/rpc/internal/svc"
	"exchange-system/common/pb/execution"

	"github.com/zeromicro/go-zero/core/logx"
)

type GetOrderStatusLogic struct {
	ctx    context.Context
	svcCtx *svc.ServiceContext
	logx.Logger
}

func NewGetOrderStatusLogic(ctx context.Context, svcCtx *svc.ServiceContext) *GetOrderStatusLogic {
	return &GetOrderStatusLogic{
		ctx:    ctx,
		svcCtx: svcCtx,
		Logger: logx.WithContext(ctx),
	}
}

// GetOrderStatus 获取订单状态
func (l *GetOrderStatusLogic) GetOrderStatus(in *execution.OrderQuery) (*execution.OrderStatus, error) {
	symbol := strings.TrimSpace(in.GetSymbol())
	orderID := strings.TrimSpace(in.GetOrderId())
	clientOrderID := strings.TrimSpace(in.GetClientOrderId())

	if symbol == "" {
		return &execution.OrderStatus{Status: "REJECTED", ErrorMessage: "symbol is required"}, nil
	}
	if orderID == "" && clientOrderID == "" {
		return &execution.OrderStatus{Status: "REJECTED", ErrorMessage: "order_id or client_order_id is required"}, nil
	}

	result, err := l.svcCtx.QueryOrderViaGRPC(l.ctx, symbol, orderID, clientOrderID)
	if err != nil {
		return &execution.OrderStatus{Status: "REJECTED", ErrorMessage: err.Error()}, nil
	}

	return convertOrderResult(result), nil
}
