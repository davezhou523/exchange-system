package config

import "github.com/zeromicro/go-zero/rest"

type Config struct {
	rest.RestConf
	Symbol         string
	InitialBalance float64
}
