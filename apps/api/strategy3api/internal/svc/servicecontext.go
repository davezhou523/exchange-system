package svc

import (
	"exchange-system/apps/api/strategy3api/internal/config"
	strategyengine "exchange-system/internal/strategy3/engine"
	strategyruntime "exchange-system/internal/strategy3/runtime"
)

type ServiceContext struct {
	Config  config.Config
	Runtime *strategyruntime.Service
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
		Runtime: strategyruntime.NewService(symbol, strategyengine.DefaultParams(), initialBalance),
	}
}
