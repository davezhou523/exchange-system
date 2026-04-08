package handler

import (
	"net/http"
	"strings"

	"exchange-system/app/api/gateway/internal/svc"
	"exchange-system/common/pb/execution"
)

func AccountHandler(serviceContext *svc.ServiceContext) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		resp, err := serviceContext.Execution.GetAccountInfo(r.Context(), &execution.AccountQuery{})
		if err != nil {
			writeError(w, http.StatusBadGateway, err.Error())
			return
		}
		writeJSON(w, http.StatusOK, resp)
	}
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
