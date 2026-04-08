package config

import "github.com/zeromicro/go-zero/zrpc"

type Config struct {
	zrpc.RpcServerConf
	Kafka struct {
		Addrs  []string
		Group  string
		Topics struct {
			Signal string
			Orders string
		}
	}
	Binance struct {
		Testnet   bool
		APIKey    string
		SecretKey string
		BaseURL   string
	}
	Risk struct {
		MaxPositionSize float64
		MaxLeverage     float64
		StopLossPercent float64
	}
}
