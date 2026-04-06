package config

import (
	"github.com/zeromicro/go-zero/rest"
)

type StrategyConfig struct {
	Name       string             `json:"name"`
	Symbol     string             `json:"symbol"`
	Enabled    bool               `json:"enabled"`
	Parameters map[string]float64 `json:"parameters"`
}

type Config struct {
	rest.RestConf
	Kafka struct {
		Addrs  []string
		Topics struct {
			MarketData string
			Signals    string
		}
	}
	Strategies []StrategyConfig
}
