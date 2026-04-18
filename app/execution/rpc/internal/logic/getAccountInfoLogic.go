package logic

import (
	"context"
	"strings"

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

// GetAccountInfo 获取账户信息（含持仓）
func (l *GetAccountInfoLogic) GetAccountInfo(in *execution.AccountQuery) (*execution.AccountInfo, error) {
	symbol := ""
	if in != nil {
		symbol = strings.ToUpper(strings.TrimSpace(in.GetSymbol()))
	}

	result, err := l.svcCtx.GetAccountInfoViaGRPC(l.ctx, symbol)
	if err != nil {
		return nil, err
	}

	resp := convertAccountResult(result)
	if in == nil {
		return resp, nil
	}
	if !in.GetIncludePositions() {
		resp.Positions = nil
		return resp, nil
	}

	if symbol == "" {
		return resp, nil
	}

	filtered := make([]*execution.Position, 0, len(resp.Positions))
	for _, p := range resp.Positions {
		if p.GetSymbol() == symbol {
			if localPos, ok := l.svcCtx.GetLocalPosition(symbol); ok {
				if p.GetStopLossPrice() == 0 {
					p.StopLossPrice = localPos.StopLoss
				}
				if p.GetTakeProfitPrice() == 0 && len(localPos.TakeProfits) > 0 {
					p.TakeProfitPrice = localPos.TakeProfits[0]
				}
			}
			if (p.GetStopLossPrice() == 0 || p.GetTakeProfitPrice() == 0) && p.GetPositionSide() != "" {
				if stopLoss, takeProfit, ok := l.svcCtx.GetRiskTargetsFromSignalLog(symbol, p.GetPositionSide()); ok {
					if p.GetStopLossPrice() == 0 {
						p.StopLossPrice = stopLoss
					}
					if p.GetTakeProfitPrice() == 0 {
						p.TakeProfitPrice = takeProfit
					}
				}
			}
			filtered = append(filtered, p)
		}
	}
	resp.Positions = filtered
	return resp, nil
}
