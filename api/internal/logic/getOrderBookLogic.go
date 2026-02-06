// Code scaffolded by goctl. Safe to edit.
// goctl 1.9.2

package logic

import (
	"context"

	"exchange-system/api/internal/svc"
	"exchange-system/api/internal/types"

	"github.com/zeromicro/go-zero/core/logx"
)

type GetOrderBookLogic struct {
	logx.Logger
	ctx    context.Context
	svcCtx *svc.ServiceContext
}

func NewGetOrderBookLogic(ctx context.Context, svcCtx *svc.ServiceContext) *GetOrderBookLogic {
	return &GetOrderBookLogic{
		Logger: logx.WithContext(ctx),
		ctx:    ctx,
		svcCtx: svcCtx,
	}
}

func (l *GetOrderBookLogic) GetOrderBook(req *types.GetOrderBookReq) (resp *types.GetOrderBookResp, err error) {
	// todo: add your logic here and delete this line

	return
}
