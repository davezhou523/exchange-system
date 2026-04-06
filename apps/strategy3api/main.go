package main

import (
	"flag"
	"fmt"

	"exchange-system/apps/strategy3api/internal/config"
	"exchange-system/apps/strategy3api/internal/handler"
	"exchange-system/apps/strategy3api/internal/svc"

	"github.com/zeromicro/go-zero/core/conf"
	"github.com/zeromicro/go-zero/rest"
)

func main() {
	configFile := flag.String("f", "apps/strategy3api/etc/strategy3-api.yaml", "配置文件路径")
	flag.Parse()

	var c config.Config
	conf.MustLoad(*configFile, &c)

	server := rest.MustNewServer(c.RestConf)
	defer server.Stop()

	serviceContext := svc.NewServiceContext(c)
	handler.RegisterHandlers(server, serviceContext)

	fmt.Printf("strategy3 api listening on %s:%d\n", c.Host, c.Port)
	server.Start()
}
