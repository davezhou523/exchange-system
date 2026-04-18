package logic

import (
	"context"
	"exchange-system/app/order/rpc/internal/svc"
	pb "exchange-system/common/pb/order"

	"github.com/zeromicro/go-zero/core/logx"
)

// ---------------------------------------------------------------------------
// FetchAllData 拉取全部合约数据并保存到本地 JSONL
// ---------------------------------------------------------------------------

// FetchAllDataLogic 全量数据拉取逻辑
type FetchAllDataLogic struct {
	ctx    context.Context
	svcCtx *svc.ServiceContext
	logx.Logger
}

// NewFetchAllDataLogic 创建全量数据拉取逻辑
func NewFetchAllDataLogic(ctx context.Context, svcCtx *svc.ServiceContext) *FetchAllDataLogic {
	return &FetchAllDataLogic{
		ctx:    ctx,
		svcCtx: svcCtx,
		Logger: logx.WithContext(ctx),
	}
}

// FetchAllData 拉取全部数据并保存到本地 JSONL
func (l *FetchAllDataLogic) FetchAllData(in *pb.OrderQueryRequest) (*pb.EmptyRequest, error) {
	symbol := normalizeSymbol(in.GetSymbol())

	if err := l.svcCtx.FetchAllData(l.ctx, symbol, in.GetStartTime(), in.GetEndTime()); err != nil {
		l.Errorf("FetchAllData failed: %v", err)
		return nil, err
	}

	return &pb.EmptyRequest{}, nil
}
