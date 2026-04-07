package logic

import (
	"context"

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

// 获取订单状态
func (l *GetOrderStatusLogic) GetOrderStatus(in *execution.OrderQuery) (*execution.OrderStatus, error) {
	// todo: add your logic here and delete this line

	return &execution.OrderStatus{}, nil
}
