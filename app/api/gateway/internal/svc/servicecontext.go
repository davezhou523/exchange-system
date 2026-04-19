package svc

import (
	"context"
	"exchange-system/app/api/gateway/internal/config"
	"exchange-system/common/binance"
	"exchange-system/common/pb/execution"
	"exchange-system/common/pb/market"
	"exchange-system/common/pb/order"
	"exchange-system/common/pb/strategy"
	"strings"
	"time"

	"github.com/zeromicro/go-zero/zrpc"
)

type ServiceContext struct {
	Config config.Config

	Execution     execution.ExecutionServiceClient
	Order         order.OrderServiceClient
	Strategy      strategy.StrategyServiceClient
	Market        market.MarketServiceClient
	binanceClient *binance.Client
}

func NewServiceContext(c config.Config) *ServiceContext {
	execCli := zrpc.MustNewClient(c.Execution)
	orderCli := zrpc.MustNewClient(c.Order)
	strategyCli := zrpc.MustNewClient(c.Strategy)
	marketCli := zrpc.MustNewClient(c.Market)

	return &ServiceContext{
		Config:        c,
		Execution:     execution.NewExecutionServiceClient(execCli.Conn()),
		Order:         order.NewOrderServiceClient(orderCli.Conn()),
		Strategy:      strategy.NewStrategyServiceClient(strategyCli.Conn()),
		Market:        market.NewMarketServiceClient(marketCli.Conn()),
		binanceClient: binance.NewClient("", "", "", ""),
	}
}

func (s *ServiceContext) SymbolPrecisions(symbol string) (int, int) {
	if s == nil || s.binanceClient == nil || strings.TrimSpace(symbol) == "" {
		return 2, 3
	}

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	pricePrecision, quantityPrecision, err := s.binanceClient.GetSymbolPrecisions(ctx, symbol)
	if err != nil {
		return 2, 3
	}
	if pricePrecision < 0 {
		pricePrecision = 2
	}
	if quantityPrecision < 0 {
		quantityPrecision = 3
	}
	if pricePrecision > 8 {
		pricePrecision = 8
	}
	if quantityPrecision > 8 {
		quantityPrecision = 8
	}
	return pricePrecision, quantityPrecision
}
