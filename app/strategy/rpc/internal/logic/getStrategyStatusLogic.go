package logic

import (
	"context"

	"exchange-system/app/strategy/rpc/internal/svc"
	"exchange-system/common/pb/strategy"

	"github.com/zeromicro/go-zero/core/logx"
)

type GetStrategyStatusLogic struct {
	ctx    context.Context
	svcCtx *svc.ServiceContext
	logx.Logger
}

func NewGetStrategyStatusLogic(ctx context.Context, svcCtx *svc.ServiceContext) *GetStrategyStatusLogic {
	return &GetStrategyStatusLogic{
		ctx:    ctx,
		svcCtx: svcCtx,
		Logger: logx.WithContext(ctx),
	}
}

// 获取策略状态
func (l *GetStrategyStatusLogic) GetStrategyStatus(in *strategy.StrategyRequest) (*strategy.StrategyStatus, error) {
	// todo: add your logic here and delete this line

	return &strategy.StrategyStatus{}, nil
}
