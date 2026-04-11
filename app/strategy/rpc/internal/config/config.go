package config

import "github.com/zeromicro/go-zero/zrpc"

type StrategyConfig struct {
	Name       string
	Symbol     string
	Enabled    bool
	Parameters map[string]float64
}

type Config struct {
	zrpc.RpcServerConf
	Kafka struct {
		Addrs  []string
		Group  string
		Topics struct {
			Kline  string
			Signal string
		}
	}

	KlineLogDir  string
	SignalLogDir string

	Strategies []StrategyConfig

	Strategy struct {
		Symbol      string
		Name        string
		RuntimeMode string
	}
}
