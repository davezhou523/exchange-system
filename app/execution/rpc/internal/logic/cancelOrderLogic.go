package logic

import (
	"context"

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

// 取消订单
func (l *CancelOrderLogic) CancelOrder(in *execution.OrderCancelRequest) (*execution.OrderStatus, error) {
	// todo: add your logic here and delete this line

	return &execution.OrderStatus{}, nil
}
