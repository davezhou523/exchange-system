package logic

import (
	"context"

	"exchange-system/app/strategy/rpc/internal/svc"
	"exchange-system/common/pb/strategy"

	"github.com/zeromicro/go-zero/core/logx"
)

type StopStrategyLogic struct {
	ctx    context.Context
	svcCtx *svc.ServiceContext
	logx.Logger
}

func NewStopStrategyLogic(ctx context.Context, svcCtx *svc.ServiceContext) *StopStrategyLogic {
	return &StopStrategyLogic{
		ctx:    ctx,
		svcCtx: svcCtx,
		Logger: logx.WithContext(ctx),
	}
}

// 停止策略
func (l *StopStrategyLogic) StopStrategy(in *strategy.StrategyRequest) (*strategy.StrategyStatus, error) {
	// todo: add your logic here and delete this line

	return &strategy.StrategyStatus{}, nil
}
