package config

import "github.com/zeromicro/go-zero/zrpc"

// ---------------------------------------------------------------------------
// Order 微服务配置
// 提供合约订单查询能力：当前委托、历史委托、历史成交、资金流水、资金费用
// ---------------------------------------------------------------------------

// Config Order 服务配置
type Config struct {
	zrpc.RpcServerConf

	// 币安合约 API 配置
	Binance BinanceConfig

	// 数据保存目录（JSONL 格式）
	DataDir string

	// execution 服务订单日志目录（simulated 模式下的成交来源）
	ExecutionOrderLogDir string
}

// BinanceConfig 币安合约 API 配置
type BinanceConfig struct {
	BaseURL   string // API 基础端点（如 https://demo-fapi.binance.com）
	APIKey    string // API Key
	SecretKey string // Secret Key
	Proxy     string // 可选代理地址（如 socks5://127.0.0.1:1080）
}
