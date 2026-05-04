package logic

import (
	"context"
	"time"

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
	strategyId := in.GetStrategyId()
	l.svcCtx.StopStrategy(strategyId)
	return &strategy.StrategyStatus{
		StrategyId:  strategyId,
		Status:      "STOPPED",
		StatusDesc:  describeServiceStatus("STOPPED"),
		Message:     statusMessageSummary("stopped"),
		MessageCode: "stopped",
		MessageDesc: describeStatusMessage("stopped"),
		LastUpdate:  time.Now().UnixMilli(),
	}, nil
}
