package main

import (
	"flag"
	"fmt"
	"log"
	"os"
	"path/filepath"

	"exchange-system/app/order/rpc/internal/config"
	"exchange-system/app/order/rpc/internal/server"
	"exchange-system/app/order/rpc/internal/svc"
	pb "exchange-system/common/pb/order"

	"github.com/zeromicro/go-zero/core/conf"
	"github.com/zeromicro/go-zero/core/service"
	"github.com/zeromicro/go-zero/zrpc"
	"google.golang.org/grpc"
	"google.golang.org/grpc/reflection"
)

// ---------------------------------------------------------------------------
// Order 微服务主入口
// 提供合约订单查询能力：当前委托、历史委托、历史成交、资金流水、资金费用
// 数据保存为 JSONL 到 data 目录
// ---------------------------------------------------------------------------

var configFile = flag.String("f", "etc/order.yaml", "the config file")

func main() {
	flag.Parse()

	var c config.Config
	conf.MustLoad(*configFile, &c)
	if err := clearAllOrdersDir(c.DataDir); err != nil {
		log.Fatalf("failed to clear all_orders dir: %v", err)
	}

	ctx, err := svc.NewServiceContext(c)
	if err != nil {
		log.Fatalf("failed to init service context: %v", err)
	}
	defer func() {
		if err := ctx.Close(); err != nil {
			log.Printf("failed to close order service context: %v", err)
		}
	}()

	s := zrpc.MustNewServer(c.RpcServerConf, func(grpcServer *grpc.Server) {
		pb.RegisterOrderServiceServer(grpcServer, server.NewOrderServiceServer(ctx))

		if c.Mode == service.DevMode || c.Mode == service.TestMode {
			reflection.Register(grpcServer)
		}
	})
	defer s.Stop()

	fmt.Printf("Starting order rpc server at %s...\n", c.ListenOn)
	s.Start()
}

func clearAllOrdersDir(dataDir string) error {
	if dataDir == "" {
		dataDir = "data/futures"
	}
	dirs := []string{
		filepath.Join(dataDir, "all_orders"),
		filepath.Join(dataDir, "positions"),
	}
	for _, dir := range dirs {
		if err := os.RemoveAll(dir); err != nil {
			return err
		}
		log.Printf("[Order服务] 启动清理目录: %s", dir)
	}
	return nil
}
