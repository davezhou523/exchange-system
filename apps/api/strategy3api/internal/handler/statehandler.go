package handler

import (
	"net/http"

	"exchange-system/apps/api/strategy3api/internal/svc"
)

func StateHandler(serviceContext *svc.ServiceContext) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		writeJSON(w, http.StatusOK, map[string]any{
			"state":        serviceContext.Runtime.State(),
			"lastDecision": serviceContext.Runtime.LastDecision(),
		})
	}
}
