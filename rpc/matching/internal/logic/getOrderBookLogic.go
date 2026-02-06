package logic

import (
	"context"

	"exchange-system/rpc/matching/exchange/rpc/matching/matching"
	"exchange-system/rpc/matching/internal/svc"

	"github.com/zeromicro/go-zero/core/logx"
)

type GetOrderBookLogic struct {
	ctx    context.Context
	svcCtx *svc.ServiceContext
	logx.Logger
}

func NewGetOrderBookLogic(ctx context.Context, svcCtx *svc.ServiceContext) *GetOrderBookLogic {
	return &GetOrderBookLogic{
		ctx:    ctx,
		svcCtx: svcCtx,
		Logger: logx.WithContext(ctx),
	}
}

func (l *GetOrderBookLogic) GetOrderBook(in *matching.GetOrderBookRequest) (*matching.GetOrderBookResponse, error) {
	// todo: add your logic here and delete this line

	return &matching.GetOrderBookResponse{}, nil
}
