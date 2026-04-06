package handler

import (
	"net/http"

	"exchange-system/apps/strategy3api/internal/svc"
)

func AccountHandler(serviceContext *svc.ServiceContext) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		writeJSON(w, http.StatusOK, map[string]any{
			"account": serviceContext.Runtime.Account(),
			"orders":  serviceContext.Runtime.Orders(),
		})
	}
}
