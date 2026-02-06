package logic

import (
	"context"

	"exchange-system/rpc/account/account"
	"exchange-system/rpc/account/internal/svc"

	"github.com/zeromicro/go-zero/core/logx"
)

type SettleLogic struct {
	ctx    context.Context
	svcCtx *svc.ServiceContext
	logx.Logger
}

func NewSettleLogic(ctx context.Context, svcCtx *svc.ServiceContext) *SettleLogic {
	return &SettleLogic{
		ctx:    ctx,
		svcCtx: svcCtx,
		Logger: logx.WithContext(ctx),
	}
}

func (l *SettleLogic) Settle(in *account.SettleRequest) (*account.SettleResponse, error) {
	// todo: add your logic here and delete this line

	return &account.SettleResponse{}, nil
}
