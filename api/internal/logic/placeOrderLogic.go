package logic

import (
	"context"
	"errors"
	"exchange-system/rpc/account/account"
	"fmt"
	"math/big"
	"time"

	"exchange-system/api/internal/svc"
	"exchange-system/api/internal/types"
	"exchange-system/common/types"
	"exchange-system/rpc/matching/matching"

	"github.com/zeromicro/go-zero/core/jsonx"
	"github.com/zeromicro/go-zero/core/logx"
	"github.com/zeromicro/go-zero/core/stores/redis"
)

type PlaceOrderLogic struct {
	logx.Logger
	ctx    context.Context
	svcCtx *svc.ServiceContext
}

func NewPlaceOrderLogic(ctx context.Context, svcCtx *svc.ServiceContext) *PlaceOrderLogic {
	return &PlaceOrderLogic{
		Logger: logx.WithContext(ctx),
		ctx:    ctx,
		svcCtx: svcCtx,
	}
}

func (l *PlaceOrderLogic) PlaceOrder(req *types.PlaceOrderReq) (*types.PlaceOrderResp, error) {
	// 1. 风控检查
	riskResp, err := l.svcCtx.RiskRpc.CheckOrder(l.ctx, &risk.CheckOrderRequest{
		UserId: req.UserID,
		Symbol: req.Symbol,
		Side:   req.Side,
		Type:   req.Type,
		Price:  req.Price,
		Amount: req.Amount,
	})

	if err != nil || !riskResp.Allowed {
		return &types.PlaceOrderResp{
			ErrorMsg: "Risk check failed: " + riskResp.Reason,
		}, nil
	}

	// 2. 资金冻结
	currency := "USDT"
	if req.Side == "sell" {
		currency = getBaseCurrency(req.Symbol)
	}

	freezeResp, err := l.svcCtx.AccountRpc.Freeze(l.ctx, &account.FreezeRequest{
		UserId:   req.UserID,
		OrderId:  generateOrderID(),
		Currency: currency,
		Amount:   l.calculateFreezeAmount(req),
	})

	if err != nil || !freezeResp.Success {
		return &types.PlaceOrderResp{
			ErrorMsg: "Freeze balance failed: " + freezeResp.Error,
		}, nil
	}

	// 3. 创建订单
	order := &exchange.Order{
		OrderId:       generateOrderID(),
		UserId:        req.UserID,
		Symbol:        req.Symbol,
		Side:          l.mapOrderSide(req.Side),
		Type:          l.mapOrderType(req.Type),
		Price:         req.Price,
		Amount:        req.Amount,
		FilledAmount:  "0",
		Status:        exchange.OrderStatus_PENDING,
		CreateTime:    time.Now().Unix(),
		ClientOrderId: req.ClientOrderID,
	}

	// 4. 发送到撮合引擎
	matchResp, err := l.svcCtx.MatchingRpc.SubmitOrder(l.ctx, &matching.OrderRequest{
		Order: order,
	})

	if err != nil {
		// 解冻资金
		l.svcCtx.AccountRpc.Unfreeze(l.ctx, &account.FreezeRequest{
			UserId:   req.UserID,
			OrderId:  order.OrderId,
			Currency: currency,
			Amount:   l.calculateFreezeAmount(req),
		})

		return &types.PlaceOrderResp{
			ErrorMsg: "Submit order failed: " + err.Error(),
		}, nil
	}

	// 5. 存储订单
	orderJson, _ := jsonx.MarshalToString(order)
	err = l.svcCtx.Redis.Setex(fmt.Sprintf("order:%s", order.OrderId), orderJson, 3600*24)
	if err != nil {
		l.Errorf("Failed to cache order: %v", err)
	}

	return &types.PlaceOrderResp{
		OrderID: order.OrderId,
		Status:  matchResp.Status,
	}, nil
}

func (l *PlaceOrderLogic) calculateFreezeAmount(req *types.PlaceOrderReq) string {
	if req.Side == "buy" && req.Type == "limit" {
		// 限价买单：冻结 price * amount
		price := new(big.Float)
		amount := new(big.Float)

		price.SetString(req.Price)
		amount.SetString(req.Amount)

		result := new(big.Float).Mul(price, amount)
		return result.String()
	} else if req.Side == "buy" && req.Type == "market" {
		// 市价买单：冻结最大金额（简化处理）
		return req.Amount
	} else {
		// 卖单：冻结数量
		return req.Amount
	}
}
