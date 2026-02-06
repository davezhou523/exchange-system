package logic

import (
	"context"

	"exchange-system/rpc/account/account"
	"exchange-system/rpc/account/internal/svc"

	"github.com/zeromicro/go-zero/core/logx"
)

type UnfreezeLogic struct {
	ctx    context.Context
	svcCtx *svc.ServiceContext
	logx.Logger
}

func NewUnfreezeLogic(ctx context.Context, svcCtx *svc.ServiceContext) *UnfreezeLogic {
	return &UnfreezeLogic{
		ctx:    ctx,
		svcCtx: svcCtx,
		Logger: logx.WithContext(ctx),
	}
}

func (l *UnfreezeLogic) Unfreeze(in *account.FreezeRequest) (*account.FreezeResponse, error) {
	// todo: add your logic here and delete this line

	return &account.FreezeResponse{}, nil
}
