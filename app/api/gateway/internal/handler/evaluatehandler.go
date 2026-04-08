package handler

import (
	"encoding/json"
	"net/http"
	"strings"

	"exchange-system/app/api/gateway/internal/svc"
	"exchange-system/common/pb/execution"
)

type placeOrderReq struct {
	Symbol       string  `json:"symbol"`
	Side         string  `json:"side"`
	PositionSide string  `json:"position_side"`
	Quantity     float64 `json:"quantity"`
}

type cancelOrderReq struct {
	Symbol        string `json:"symbol"`
	OrderId       string `json:"order_id"`
	ClientOrderId string `json:"client_order_id"`
}

func PlaceOrderHandler(serviceContext *svc.ServiceContext) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req placeOrderReq
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			writeError(w, http.StatusBadRequest, "invalid json")
			return
		}
		req.Symbol = strings.TrimSpace(strings.ToUpper(req.Symbol))
		req.Side = strings.TrimSpace(strings.ToUpper(req.Side))
		req.PositionSide = strings.TrimSpace(strings.ToUpper(req.PositionSide))
		if req.Symbol == "" || (req.Side != "BUY" && req.Side != "SELL") || req.Quantity <= 0 {
			writeError(w, http.StatusBadRequest, "invalid order")
			return
		}
		resp, err := serviceContext.Execution.CreateOrder(r.Context(), &execution.OrderRequest{
			Symbol:       req.Symbol,
			Side:         req.Side,
			PositionSide: req.PositionSide,
			Quantity:     req.Quantity,
		})
		if err != nil {
			writeError(w, http.StatusBadGateway, err.Error())
			return
		}
		writeJSON(w, http.StatusOK, resp)
	}
}

func CancelOrderHandler(serviceContext *svc.ServiceContext) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req cancelOrderReq
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			writeError(w, http.StatusBadRequest, "invalid json")
			return
		}
		req.Symbol = strings.TrimSpace(strings.ToUpper(req.Symbol))
		if req.Symbol == "" || (req.OrderId == "" && req.ClientOrderId == "") {
			writeError(w, http.StatusBadRequest, "symbol and (order_id or client_order_id) are required")
			return
		}
		resp, err := serviceContext.Execution.CancelOrder(r.Context(), &execution.OrderCancelRequest{
			Symbol:        req.Symbol,
			OrderId:       req.OrderId,
			ClientOrderId: req.ClientOrderId,
		})
		if err != nil {
			writeError(w, http.StatusBadGateway, err.Error())
			return
		}
		writeJSON(w, http.StatusOK, resp)
	}
}

func writeJSON(w http.ResponseWriter, statusCode int, value any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(statusCode)
	_ = json.NewEncoder(w).Encode(value)
}

func writeError(w http.ResponseWriter, statusCode int, message string) {
	writeJSON(w, statusCode, map[string]any{
		"error": message,
	})
}
