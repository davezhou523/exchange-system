package logic

import (
	"context"
	"time"

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
	strategyId := in.GetSymbol()
	if strategyId == "" {
		strategyId = in.GetName()
	}
	in.Enabled = true
	l.svcCtx.UpsertStrategy(in)
	return &strategy.StrategyStatus{
		StrategyId: strategyId,
		Status:     "RUNNING",
		Message:    "started",
		LastUpdate: time.Now().UnixMilli(),
	}, nil
}
