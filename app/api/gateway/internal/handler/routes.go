package handler

import (
	"net/http"

	"exchange-system/app/api/gateway/internal/svc"

	"github.com/zeromicro/go-zero/rest"
)

func RegisterHandlers(server *rest.Server, serviceContext *svc.ServiceContext) {
	server.AddRoutes([]rest.Route{
		{Method: http.MethodPost, Path: "/order", Handler: PlaceOrderHandler(serviceContext)},
		{Method: http.MethodPost, Path: "/order/cancel", Handler: CancelOrderHandler(serviceContext)},
		{Method: http.MethodGet, Path: "/order/status", Handler: OrderStatusHandler(serviceContext)},
		{Method: http.MethodGet, Path: "/orders", Handler: AllOrdersHandler(serviceContext)},
		{Method: http.MethodGet, Path: "/trades", Handler: UserTradesHandler(serviceContext)},
		{Method: http.MethodGet, Path: "/position-cycles", Handler: PositionCyclesHandler(serviceContext)},
		{Method: http.MethodGet, Path: "/position-cycles.csv", Handler: PositionCyclesCSVHandler(serviceContext)},
		{Method: http.MethodGet, Path: "/account", Handler: AccountHandler(serviceContext)},
		{Method: http.MethodPost, Path: "/strategy/start", Handler: StartStrategyHandler(serviceContext)},
		{Method: http.MethodPost, Path: "/strategy/stop", Handler: StopStrategyHandler(serviceContext)},
		{Method: http.MethodGet, Path: "/strategy/status", Handler: StrategyStatusHandler(serviceContext)},
	})
}
