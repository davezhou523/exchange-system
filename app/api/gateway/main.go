package main

import (
	"flag"
	"fmt"
	"os"

	"exchange-system/app/api/gateway/internal/config"
	"exchange-system/app/api/gateway/internal/handler"
	"exchange-system/app/api/gateway/internal/svc"

	"github.com/zeromicro/go-zero/core/conf"
	"github.com/zeromicro/go-zero/rest"
)

func main() {
	configFile := flag.String("f", "app/api/gateway/etc/gateway.yaml", "配置文件路径")
	flag.Parse()

	if _, err := os.Stat(*configFile); err != nil {
		if _, fallbackErr := os.Stat("etc/gateway.yaml"); fallbackErr == nil {
			*configFile = "etc/gateway.yaml"
		}
	}

	var c config.Config
	conf.MustLoad(*configFile, &c)

	server := rest.MustNewServer(c.RestConf)
	defer server.Stop()

	serviceContext := svc.NewServiceContext(c)
	handler.RegisterHandlers(server, serviceContext)

	fmt.Printf("strategy3 api listening on %s:%d\n", c.Host, c.Port)
	server.Start()
}
