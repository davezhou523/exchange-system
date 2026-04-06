package config

import (
	"github.com/zeromicro/go-zero/rest"
)

type Config struct {
	rest.RestConf
	Kafka struct {
		Addrs  []string
		Topics struct {
			Signals string
			Orders  string
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
