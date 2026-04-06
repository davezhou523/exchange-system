package handler

import (
	"net/http"

	"exchange-system/apps/api/strategy3api/internal/svc"

	"github.com/zeromicro/go-zero/rest"
)

func RegisterHandlers(server *rest.Server, serviceContext *svc.ServiceContext) {
	server.AddRoutes([]rest.Route{
		{
			Method:  http.MethodPost,
			Path:    "/strategy3/evaluate",
			Handler: EvaluateHandler(serviceContext),
		},
		{
			Method:  http.MethodGet,
			Path:    "/strategy3/account",
			Handler: AccountHandler(serviceContext),
		},
		{
			Method:  http.MethodGet,
			Path:    "/strategy3/state",
			Handler: StateHandler(serviceContext),
		},
	})
}
