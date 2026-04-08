package logic

import (
	"context"
	"strconv"

	"exchange-system/app/execution/rpc/internal/svc"
	"exchange-system/common/pb/execution"

	"github.com/zeromicro/go-zero/core/logx"
)

type GetAccountInfoLogic struct {
	ctx    context.Context
	svcCtx *svc.ServiceContext
	logx.Logger
}

func NewGetAccountInfoLogic(ctx context.Context, svcCtx *svc.ServiceContext) *GetAccountInfoLogic {
	return &GetAccountInfoLogic{
		ctx:    ctx,
		svcCtx: svcCtx,
		Logger: logx.WithContext(ctx),
	}
}

// 获取账户信息
func (l *GetAccountInfoLogic) GetAccountInfo(in *execution.AccountQuery) (*execution.AccountInfo, error) {
	info, err := l.svcCtx.GetAccount(l.ctx)
	if err != nil {
		return nil, err
	}
	bal, err := strconv.ParseFloat(info.TotalWalletBalance, 64)
	if err != nil {
		return nil, err
	}
	return &execution.AccountInfo{
		TotalWalletBalance: bal,
	}, nil
}
