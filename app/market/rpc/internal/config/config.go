package config

import "github.com/zeromicro/go-zero/zrpc"

type Config struct {
	zrpc.RpcServerConf
	Kafka struct {
		Addrs []string
		Topic struct {
			MarketData string
		}
	}
	Binance struct {
		WebSocketURL string
		Symbols      []string
		Intervals    []string
	}
}