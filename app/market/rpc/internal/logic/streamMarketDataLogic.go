package logic

import (
	"context"

	"exchange-system/app/market/rpc/internal/svc"
	"exchange-system/common/pb/market"

	"github.com/zeromicro/go-zero/core/logx"
)

type StreamMarketDataLogic struct {
	ctx    context.Context
	svcCtx *svc.ServiceContext
	logx.Logger
}

func NewStreamMarketDataLogic(ctx context.Context, svcCtx *svc.ServiceContext) *StreamMarketDataLogic {
	return &StreamMarketDataLogic{
		ctx:    ctx,
		svcCtx: svcCtx,
		Logger: logx.WithContext(ctx),
	}
}

// 订阅市场数据流
func (l *StreamMarketDataLogic) StreamMarketData(in *market.StreamRequest, stream market.MarketService_StreamMarketDataServer) error {
	// todo: add your logic here and delete this line

	return nil
}
