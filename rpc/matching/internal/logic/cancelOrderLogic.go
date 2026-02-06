package logic

import (
	"context"

	"exchange-system/rpc/matching/exchange/rpc/matching/matching"
	"exchange-system/rpc/matching/internal/svc"

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

func (l *CancelOrderLogic) CancelOrder(in *matching.CancelOrderRequest) (*matching.CancelOrderResponse, error) {
	// todo: add your logic here and delete this line

	return &matching.CancelOrderResponse{}, nil
}
