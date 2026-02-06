// Code scaffolded by goctl. Safe to edit.
// goctl 1.9.2

package handler

import (
	"net/http"

	"exchange-system/api/internal/logic"
	"exchange-system/api/internal/svc"
	"github.com/zeromicro/go-zero/rest/httpx"
)

func CancelOrderHandler(svcCtx *svc.ServiceContext) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		l := logic.NewCancelOrderLogic(r.Context(), svcCtx)
		resp, err := l.CancelOrder()
		if err != nil {
			httpx.ErrorCtx(r.Context(), w, err)
		} else {
			httpx.OkJsonCtx(r.Context(), w, resp)
		}
	}
}
