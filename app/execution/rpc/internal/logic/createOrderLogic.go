package logic

import (
	"context"
	"fmt"
	"strings"

	"exchange-system/app/execution/rpc/internal/exchange"
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

// CreateOrder 创建订单
func (l *CreateOrderLogic) CreateOrder(in *execution.OrderRequest) (*execution.OrderStatus, error) {
	symbol := strings.TrimSpace(in.GetSymbol())
	side := strings.ToUpper(strings.TrimSpace(in.GetSide()))
	positionSide := strings.ToUpper(strings.TrimSpace(in.GetPositionSide()))
	qty := in.GetQuantity()
	price := in.GetPrice()
	orderType := strings.ToUpper(strings.TrimSpace(in.GetType()))
	clientID := strings.TrimSpace(in.GetClientOrderId())

	// 参数验证
	if symbol == "" {
		return &execution.OrderStatus{Status: "REJECTED", ErrorMessage: "symbol is required"}, nil
	}
	if side != "BUY" && side != "SELL" {
		return &execution.OrderStatus{Status: "REJECTED", ErrorMessage: "side must be BUY or SELL"}, nil
	}
	if qty <= 0 && !in.ClosePosition {
		return &execution.OrderStatus{Status: "REJECTED", ErrorMessage: "quantity must be positive"}, nil
	}

	// 默认值填充
	if positionSide == "" {
		if side == "BUY" {
			positionSide = "LONG"
		} else {
			positionSide = "SHORT"
		}
	}
	if orderType == "" {
		orderType = "MARKET"
	}

	// 调用 ServiceContext 下单
	result, err := l.svcCtx.CreateOrderViaGRPC(l.ctx, symbol, side, positionSide, qty, price, orderType, clientID)
	if err != nil {
		return &execution.OrderStatus{Status: "REJECTED", ErrorMessage: err.Error()}, nil
	}

	return convertOrderResult(result), nil
}

// convertOrderResult 将统一 OrderResult 转换为 protobuf OrderStatus
func convertOrderResult(r *exchange.OrderResult) *execution.OrderStatus {
	if r == nil {
		return &execution.OrderStatus{Status: "REJECTED", ErrorMessage: "nil result"}
	}
	return &execution.OrderStatus{
		OrderId:          r.OrderID,
		ClientOrderId:    r.ClientOrderID,
		Symbol:           r.Symbol,
		Status:           string(r.Status),
		Side:             string(r.Side),
		ExecutedQuantity: r.ExecutedQuantity,
		AvgPrice:         r.AvgPrice,
		Commission:       r.Commission,
		CommissionAsset:  r.CommissionAsset,
		TransactTime:     r.TransactTime,
		ErrorMessage:     r.ErrorMessage,
	}
}

// convertAccountResult 将统一 AccountResult 转换为 protobuf AccountInfo
func convertAccountResult(r *exchange.AccountResult) *execution.AccountInfo {
	if r == nil {
		return &execution.AccountInfo{}
	}

	positions := make([]*execution.Position, 0, len(r.Positions))
	for _, p := range r.Positions {
		positions = append(positions, &execution.Position{
			Symbol:                 p.Symbol,
			PositionAmount:         p.PositionAmount,
			EntryPrice:             p.EntryPrice,
			MarkPrice:              p.MarkPrice,
			UnrealizedPnl:          p.UnrealizedPnl,
			LiquidationPrice:       p.LiquidationPrice,
			Leverage:               p.Leverage,
			MarginType:             p.MarginType,
			PositionSide:           p.PositionSide,
			BreakEvenPrice:         p.BreakEvenPrice,
			Notional:               p.Notional,
			InitialMargin:          p.InitialMargin,
			MaintMargin:            p.MaintMargin,
			PositionInitialMargin:  p.PositionInitialMargin,
			OpenOrderInitialMargin: p.OpenOrderInitialMargin,
			IsolatedMargin:         p.IsolatedMargin,
			BidNotional:            p.BidNotional,
			AskNotional:            p.AskNotional,
			UpdateTime:             p.UpdateTime,
			Adl:                    p.Adl,
			PnlPercent:             p.PnlPercent,
			StopLossPrice:          p.StopLossPrice,
			TakeProfitPrice:        p.TakeProfitPrice,
			FundingRate:            p.FundingRate,
			EstimatedFundingFee:    p.EstimatedFundingFee,
		})
	}

	return &execution.AccountInfo{
		TotalWalletBalance: r.TotalWalletBalance,
		TotalUnrealizedPnl: r.TotalUnrealizedPnl,
		TotalMarginBalance: r.TotalMarginBalance,
		AvailableBalance:   r.AvailableBalance,
		MaxWithdrawAmount:  r.MaxWithdrawAmount,
		Positions:          positions,
	}
}

// formatFloat 格式化浮点数为字符串
func formatFloat(v float64) string {
	if v == 0 {
		return "0"
	}
	return fmt.Sprintf("%.8f", v)
}
