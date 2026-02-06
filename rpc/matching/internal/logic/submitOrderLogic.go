package logic

import (
	"context"

	"exchange-system/rpc/matching/exchange/rpc/matching/matching"
	"exchange-system/rpc/matching/internal/svc"

	"github.com/zeromicro/go-zero/core/logx"
)

type SubmitOrderLogic struct {
	ctx    context.Context
	svcCtx *svc.ServiceContext
	logx.Logger
}

func NewSubmitOrderLogic(ctx context.Context, svcCtx *svc.ServiceContext) *SubmitOrderLogic {
	return &SubmitOrderLogic{
		ctx:    ctx,
		svcCtx: svcCtx,
		Logger: logx.WithContext(ctx),
	}
}

func (l *SubmitOrderLogic) SubmitOrder(in *matching.OrderRequest) (*matching.OrderResponse, error) {
	// todo: add your logic here and delete this line

	return &matching.OrderResponse{}, nil
}
