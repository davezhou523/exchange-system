package config

import (
	"github.com/zeromicro/go-zero/rest"
	"github.com/zeromicro/go-zero/zrpc"
)

type Config struct {
	rest.RestConf

	Execution zrpc.RpcClientConf
	Order     zrpc.RpcClientConf
	Strategy  zrpc.RpcClientConf
	Market    zrpc.RpcClientConf
}
