package svc

import (
	"exchange-system/app/api/gateway/internal/config"
	"exchange-system/common/pb/execution"
	"exchange-system/common/pb/market"
	"exchange-system/common/pb/order"
	"exchange-system/common/pb/strategy"

	"github.com/zeromicro/go-zero/zrpc"
)

type ServiceContext struct {
	Config config.Config

	Execution execution.ExecutionServiceClient
	Order     order.OrderServiceClient
	Strategy  strategy.StrategyServiceClient
	Market    market.MarketServiceClient
}

func NewServiceContext(c config.Config) *ServiceContext {
	execCli := zrpc.MustNewClient(c.Execution)
	orderCli := zrpc.MustNewClient(c.Order)
	strategyCli := zrpc.MustNewClient(c.Strategy)
	marketCli := zrpc.MustNewClient(c.Market)

	return &ServiceContext{
		Config:    c,
		Execution: execution.NewExecutionServiceClient(execCli.Conn()),
		Order:     order.NewOrderServiceClient(orderCli.Conn()),
		Strategy:  strategy.NewStrategyServiceClient(strategyCli.Conn()),
		Market:    market.NewMarketServiceClient(marketCli.Conn()),
	}
}
