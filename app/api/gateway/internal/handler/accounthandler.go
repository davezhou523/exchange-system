package handler

import (
	"net/http"
	"strings"
	"time"

	"exchange-system/app/api/gateway/internal/svc"
	"exchange-system/common/pb/execution"
)

type accountView struct {
	TotalWalletBalance float64        `json:"total_wallet_balance"`
	TotalUnrealizedPnl float64        `json:"total_unrealized_pnl"`
	TotalMarginBalance float64        `json:"total_margin_balance"`
	AvailableBalance   float64        `json:"available_balance"`
	MaxWithdrawAmount  float64        `json:"max_withdraw_amount"`
	WalletBalance      float64        `json:"wallet_balance"`
	UnrealizedPnl      float64        `json:"unrealized_pnl"`
	MarginBalance      float64        `json:"margin_balance"`
	Positions          []positionView `json:"positions"`
}

type positionView struct {
	Symbol                 string  `json:"symbol"`
	PositionSide           string  `json:"position_side"`
	PositionAmount         float64 `json:"position_amount"`
	EntryPrice             float64 `json:"entry_price"`
	BreakEvenPrice         float64 `json:"break_even_price"`
	MarkPrice              float64 `json:"mark_price"`
	LiquidationPrice       float64 `json:"liquidation_price"`
	StopLossPrice          float64 `json:"stop_loss_price"`
	TakeProfitPrice        float64 `json:"take_profit_price"`
	UnrealizedPnl          float64 `json:"unrealized_pnl"`
	PnlPercent             float64 `json:"pnl_percent"`
	Leverage               float64 `json:"leverage"`
	MarginType             string  `json:"margin_type"`
	Notional               float64 `json:"notional"`
	InitialMargin          float64 `json:"initial_margin"`
	MaintMargin            float64 `json:"maint_margin"`
	PositionInitialMargin  float64 `json:"position_initial_margin"`
	OpenOrderInitialMargin float64 `json:"open_order_initial_margin"`
	IsolatedMargin         float64 `json:"isolated_margin"`
	FundingRate            float64 `json:"funding_rate"`
	EstimatedFundingFee    float64 `json:"estimated_funding_fee"`
	Adl                    int32   `json:"adl"`
	UpdateTime             int64   `json:"update_time"`
	UpdateTimeText         string  `json:"update_time_text"`

	// UI aliases that are closer to the Binance position page wording.
	Side            string  `json:"side"`
	Size            float64 `json:"size"`
	OpenPrice       float64 `json:"open_price"`
	BreakevenPrice  float64 `json:"breakeven_price"`
	LiqPrice        float64 `json:"liq_price"`
	Pnl             float64 `json:"pnl"`
	Roe             float64 `json:"roe"`
	Margin          float64 `json:"margin"`
	SlPrice         float64 `json:"sl_price"`
	TpPrice         float64 `json:"tp_price"`
	EstimatedFee    float64 `json:"estimated_fee"`
	MarginMode      string  `json:"margin_mode"`
	UpdateTimeLabel string  `json:"update_time_label"`
}

func AccountHandler(serviceContext *svc.ServiceContext) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		symbol := strings.TrimSpace(strings.ToUpper(r.URL.Query().Get("symbol")))
		resp, err := serviceContext.Execution.GetAccountInfo(r.Context(), &execution.AccountQuery{
			IncludePositions: true,
			Symbol:           symbol,
		})
		if err != nil {
			writeError(w, http.StatusBadGateway, err.Error())
			return
		}
		writeJSON(w, http.StatusOK, formatAccountView(resp))
	}
}

func formatAccountView(resp *execution.AccountInfo) accountView {
	if resp == nil {
		return accountView{}
	}

	positions := make([]positionView, 0, len(resp.GetPositions()))
	for _, p := range resp.GetPositions() {
		positions = append(positions, positionView{
			Symbol:                 p.GetSymbol(),
			PositionSide:           p.GetPositionSide(),
			PositionAmount:         p.GetPositionAmount(),
			EntryPrice:             p.GetEntryPrice(),
			BreakEvenPrice:         p.GetBreakEvenPrice(),
			MarkPrice:              p.GetMarkPrice(),
			LiquidationPrice:       p.GetLiquidationPrice(),
			StopLossPrice:          p.GetStopLossPrice(),
			TakeProfitPrice:        p.GetTakeProfitPrice(),
			UnrealizedPnl:          p.GetUnrealizedPnl(),
			PnlPercent:             p.GetPnlPercent(),
			Leverage:               p.GetLeverage(),
			MarginType:             p.GetMarginType(),
			Notional:               p.GetNotional(),
			InitialMargin:          p.GetInitialMargin(),
			MaintMargin:            p.GetMaintMargin(),
			PositionInitialMargin:  p.GetPositionInitialMargin(),
			OpenOrderInitialMargin: p.GetOpenOrderInitialMargin(),
			IsolatedMargin:         p.GetIsolatedMargin(),
			FundingRate:            p.GetFundingRate(),
			EstimatedFundingFee:    p.GetEstimatedFundingFee(),
			Adl:                    p.GetAdl(),
			UpdateTime:             p.GetUpdateTime(),
			UpdateTimeText:         formatMillis(p.GetUpdateTime()),
			Side:                   p.GetPositionSide(),
			Size:                   p.GetPositionAmount(),
			OpenPrice:              p.GetEntryPrice(),
			BreakevenPrice:         p.GetBreakEvenPrice(),
			LiqPrice:               p.GetLiquidationPrice(),
			Pnl:                    p.GetUnrealizedPnl(),
			Roe:                    p.GetPnlPercent(),
			Margin:                 p.GetInitialMargin(),
			SlPrice:                p.GetStopLossPrice(),
			TpPrice:                p.GetTakeProfitPrice(),
			EstimatedFee:           p.GetEstimatedFundingFee(),
			MarginMode:             p.GetMarginType(),
			UpdateTimeLabel:        formatMillis(p.GetUpdateTime()),
		})
	}

	return accountView{
		TotalWalletBalance: resp.GetTotalWalletBalance(),
		TotalUnrealizedPnl: resp.GetTotalUnrealizedPnl(),
		TotalMarginBalance: resp.GetTotalMarginBalance(),
		AvailableBalance:   resp.GetAvailableBalance(),
		MaxWithdrawAmount:  resp.GetMaxWithdrawAmount(),
		WalletBalance:      resp.GetTotalWalletBalance(),
		UnrealizedPnl:      resp.GetTotalUnrealizedPnl(),
		MarginBalance:      resp.GetTotalMarginBalance(),
		Positions:          positions,
	}
}

func formatMillis(ms int64) string {
	if ms <= 0 {
		return ""
	}
	return time.UnixMilli(ms).UTC().Format("2006-01-02 15:04:05")
}

func OrderStatusHandler(serviceContext *svc.ServiceContext) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		symbol := strings.TrimSpace(strings.ToUpper(r.URL.Query().Get("symbol")))
		orderID := strings.TrimSpace(r.URL.Query().Get("order_id"))
		clientOrderID := strings.TrimSpace(r.URL.Query().Get("client_order_id"))
		if symbol == "" || (orderID == "" && clientOrderID == "") {
			writeError(w, http.StatusBadRequest, "symbol and (order_id or client_order_id) are required")
			return
		}
		resp, err := serviceContext.Execution.GetOrderStatus(r.Context(), &execution.OrderQuery{
			Symbol:        symbol,
			OrderId:       orderID,
			ClientOrderId: clientOrderID,
		})
		if err != nil {
			writeError(w, http.StatusBadGateway, err.Error())
			return
		}
		writeJSON(w, http.StatusOK, resp)
	}
}
