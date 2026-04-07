package logic

import (
	"context"

	"exchange-system/app/strategy/rpc/internal/svc"
	"exchange-system/common/pb/strategy"

	"github.com/zeromicro/go-zero/core/logx"
)

type StartStrategyLogic struct {
	ctx    context.Context
	svcCtx *svc.ServiceContext
	logx.Logger
}

func NewStartStrategyLogic(ctx context.Context, svcCtx *svc.ServiceContext) *StartStrategyLogic {
	return &StartStrategyLogic{
		ctx:    ctx,
		svcCtx: svcCtx,
		Logger: logx.WithContext(ctx),
	}
}

// 启动策略
func (l *StartStrategyLogic) StartStrategy(in *strategy.StrategyConfig) (*strategy.StrategyStatus, error) {
	// todo: add your logic here and delete this line

	return &strategy.StrategyStatus{}, nil
}
