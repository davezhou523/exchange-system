package logic

import (
	"context"

	"exchange-system/rpc/account/account"
	"exchange-system/rpc/account/internal/svc"

	"github.com/zeromicro/go-zero/core/logx"
)

type GetBalanceLogic struct {
	ctx    context.Context
	svcCtx *svc.ServiceContext
	logx.Logger
}

func NewGetBalanceLogic(ctx context.Context, svcCtx *svc.ServiceContext) *GetBalanceLogic {
	return &GetBalanceLogic{
		ctx:    ctx,
		svcCtx: svcCtx,
		Logger: logx.WithContext(ctx),
	}
}

func (l *GetBalanceLogic) GetBalance(in *account.GetBalanceRequest) (*account.GetBalanceResponse, error) {
	// todo: add your logic here and delete this line

	return &account.GetBalanceResponse{}, nil
}
