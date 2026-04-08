package logic

import (
	"context"
	"strconv"
	"strings"

	"exchange-system/app/execution/rpc/internal/svc"
	"exchange-system/common/pb/execution"

	"github.com/zeromicro/go-zero/core/logx"
)

type CreateOrderLogic struct {
	ctx    context.Context
	svcCtx *svc.ServiceContext
	logx.Logger
}

func NewCreateOrderLogic(ctx context.Context, svcCtx *svc.ServiceContext) *CreateOrderLogic {
	return &CreateOrderLogic{
		ctx:    ctx,
		svcCtx: svcCtx,
		Logger: logx.WithContext(ctx),
	}
}

// 创建订单
func (l *CreateOrderLogic) CreateOrder(in *execution.OrderRequest) (*execution.OrderStatus, error) {
	symbol := strings.TrimSpace(in.GetSymbol())
	side := strings.ToUpper(strings.TrimSpace(in.GetSide()))
	positionSide := strings.ToUpper(strings.TrimSpace(in.GetPositionSide()))
	qty := in.GetQuantity()

	if symbol == "" {
		return &execution.OrderStatus{Status: "REJECTED", ErrorMessage: "symbol is required"}, nil
	}
	if side != "BUY" && side != "SELL" {
		return &execution.OrderStatus{Status: "REJECTED", ErrorMessage: "side must be BUY or SELL"}, nil
	}
	if qty <= 0 {
		return &execution.OrderStatus{Status: "REJECTED", ErrorMessage: "quantity must be positive"}, nil
	}
	if positionSide == "" {
		if side == "BUY" {
			positionSide = "LONG"
		} else {
			positionSide = "SHORT"
		}
	}

	resp, err := l.svcCtx.CreateOrder(l.ctx, symbol, side, positionSide, qty)
	if err != nil {
		return &execution.OrderStatus{Status: "REJECTED", ErrorMessage: err.Error()}, nil
	}
	return &execution.OrderStatus{
		OrderId:          strconv.FormatInt(resp.OrderID, 10),
		Symbol:           resp.Symbol,
		Status:           resp.Status,
		Side:             side,
		AvgPrice:         parseFloat(resp.AvgPrice),
		TransactTime:     resp.Time,
		ErrorMessage:     "",
		ClientOrderId:    "",
		ExecutedQuantity: qty,
	}, nil
}

func parseFloat(v string) float64 {
	f, _ := strconv.ParseFloat(v, 64)
	return f
}
