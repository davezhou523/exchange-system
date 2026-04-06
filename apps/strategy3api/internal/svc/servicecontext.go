package svc

import (
	"exchange-system/apps/strategy3api/internal/config"
	"exchange-system/strategy3"
)

type ServiceContext struct {
	Config  config.Config
	Runtime *strategy3.RuntimeService
}

func NewServiceContext(c config.Config) *ServiceContext {
	symbol := c.Symbol
	if symbol == "" {
		symbol = "BTCUSDT"
	}
	initialBalance := c.InitialBalance
	if initialBalance <= 0 {
		initialBalance = 10000
	}

	return &ServiceContext{
		Config:  c,
		Runtime: strategy3.NewRuntimeService(symbol, strategy3.DefaultParams(), initialBalance),
	}
}
