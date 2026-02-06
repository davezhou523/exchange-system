// Code scaffolded by goctl. Safe to edit.
// goctl 1.9.2

package handler

import (
	"net/http"

	"exchange-system/api/internal/logic"
	"exchange-system/api/internal/svc"
	"exchange-system/api/internal/types"
	"github.com/zeromicro/go-zero/rest/httpx"
)

func GetOrderBookHandler(svcCtx *svc.ServiceContext) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req types.GetOrderBookReq
		if err := httpx.Parse(r, &req); err != nil {
			httpx.ErrorCtx(r.Context(), w, err)
			return
		}

		l := logic.NewGetOrderBookLogic(r.Context(), svcCtx)
		resp, err := l.GetOrderBook(&req)
		if err != nil {
			httpx.ErrorCtx(r.Context(), w, err)
		} else {
			httpx.OkJsonCtx(r.Context(), w, resp)
		}
	}
}
