package logic

import (
	"context"

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
	// todo: add your logic here and delete this line

	return &execution.AccountInfo{}, nil
}
