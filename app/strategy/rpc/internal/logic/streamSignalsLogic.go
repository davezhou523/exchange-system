package logic

import (
	"context"

	"exchange-system/app/strategy/rpc/internal/svc"
	"exchange-system/common/pb/strategy"

	"github.com/zeromicro/go-zero/core/logx"
)

type StreamSignalsLogic struct {
	ctx    context.Context
	svcCtx *svc.ServiceContext
	logx.Logger
}

func NewStreamSignalsLogic(ctx context.Context, svcCtx *svc.ServiceContext) *StreamSignalsLogic {
	return &StreamSignalsLogic{
		ctx:    ctx,
		svcCtx: svcCtx,
		Logger: logx.WithContext(ctx),
	}
}

// 获取策略信号流
func (l *StreamSignalsLogic) StreamSignals(in *strategy.StrategyRequest, stream strategy.StrategyService_StreamSignalsServer) error {
	// todo: add your logic here and delete this line

	return nil
}
