package logic

import (
	"context"
	"strings"

	"exchange-system/app/execution/rpc/internal/svc"
	"exchange-system/common/pb/execution"

	"github.com/zeromicro/go-zero/core/logx"
)

type CancelOrderLogic struct {
	ctx    context.Context
	svcCtx *svc.ServiceContext
	logx.Logger
}

func NewCancelOrderLogic(ctx context.Context, svcCtx *svc.ServiceContext) *CancelOrderLogic {
	return &CancelOrderLogic{
		ctx:    ctx,
		svcCtx: svcCtx,
		Logger: logx.WithContext(ctx),
	}
}

// CancelOrder 取消订单
func (l *CancelOrderLogic) CancelOrder(in *execution.OrderCancelRequest) (*execution.OrderStatus, error) {
	symbol := strings.TrimSpace(in.GetSymbol())
	orderID := strings.TrimSpace(in.GetOrderId())
	clientOrderID := strings.TrimSpace(in.GetClientOrderId())

	if symbol == "" {
		return &execution.OrderStatus{Status: "REJECTED", ErrorMessage: "symbol is required"}, nil
	}
	if orderID == "" && clientOrderID == "" {
		return &execution.OrderStatus{Status: "REJECTED", ErrorMessage: "order_id or client_order_id is required"}, nil
	}

	result, err := l.svcCtx.CancelOrderViaGRPC(l.ctx, symbol, orderID, clientOrderID)
	if err != nil {
		return &execution.OrderStatus{Status: "REJECTED", ErrorMessage: err.Error()}, nil
	}

	return convertOrderResult(result), nil
}
