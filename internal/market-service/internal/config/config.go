package config

import (
	"github.com/zeromicro/go-zero/rest"
)

type Config struct {
	rest.RestConf
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
