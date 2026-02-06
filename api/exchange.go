// api/exchange.go
package main

import (
	"flag"
	"fmt"
	"net/http"

	"exchange-system/api/internal/config"
	"exchange-system/api/internal/handler"
	"exchange-system/api/internal/svc"

	"github.com/zeromicro/go-zero/core/conf"
	"github.com/zeromicro/go-zero/core/service"
	"github.com/zeromicro/go-zero/rest"
)

var configFile = flag.String("f", "etc/exchange-api.yaml", "the config file")

func main() {
	flag.Parse()

	var c config.Config
	conf.MustLoad(*configFile, &c)

	ctx := svc.NewServiceContext(c)
	server := rest.MustNewServer(c.RestConf)
	defer server.Stop()

	handler.RegisterHandlers(server, ctx)

	fmt.Printf("Starting server at %s:%d...\n", c.Host, c.Port)

	// 启动 WebSocket 服务
	wsHandler := handler.NewWebSocketHandler()
	server.AddRoute(rest.Route{
		Method:  http.MethodGet,
		Path:    "/ws",
		Handler: wsHandler.Handle,
	})

	// 启动后台任务
	go startBackgroundTasks(ctx, wsHandler)

	server.Start()
}

func startBackgroundTasks(ctx *svc.ServiceContext, wsHandler *handler.WebSocketHandler) {
	// 订阅撮合引擎的交易流
	go func() {
		for {
			select {
			case trade := <-ctx.MatchingRpc.TradeStream():
				// 广播交易信息
				wsHandler.BroadcastTrade(trade)

				// 更新最新价格
				ctx.Redis.Setex(fmt.Sprintf("last_price:%s", trade.Symbol), trade.Price, 60)
			}
		}
	}()
}
