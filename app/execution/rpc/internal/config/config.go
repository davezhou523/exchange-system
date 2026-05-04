package config

import (
	"time"

	"github.com/zeromicro/go-zero/zrpc"
)

// Config 执行服务配置
type Config struct {
	zrpc.RpcServerConf
	Kafka struct {
		Addrs  []string
		Group  string
		Topics struct {
			Signal            string // 消费的策略信号 topic
			HarvestPathSignal string // 消费的收割路径风险 topic
			Order             string // 生产的订单结果 topic
			Kline             string // 生产的历史K线数据 topic
		}
	}

	// 交易所路由配置
	Exchange ExchangeConfig

	// 风控配置
	Risk RiskConfig

	// 主动降仓监控配置
	PositionMonitor PositionMonitorConfig

	// 幂等配置
	Idempotent IdempotentConfig

	// 日志目录
	SignalLogDir string // 接收信号的日志目录（如 data/signal）
	OrderLogDir  string // 订单结果的日志目录（如 data/order）

	// ClickHouse 配置执行分析数据落库。
	ClickHouse ClickHouseConfig

	// Postgres 配置执行业务数据扩展落库。
	Postgres PostgresConfig
}

// ExchangeConfig 交易所路由配置
type ExchangeConfig struct {
	// 路由策略: "direct" | "by_symbol"
	RouteStrategy string

	// 默认交易所名称: "binance" | "okx"
	DefaultExchange string

	// 交易对路由映射（by_symbol 策略用）
	SymbolRoutes map[string]string

	// Binance 配置
	Binance BinanceConfig

	// OKX 配置（可选，不使用时YAML中留空即可）
	OKX OKXConfig `json:",optional"`
}

// BinanceConfig 币安交易所配置
type BinanceConfig struct {
	Testnet   bool
	APIKey    string
	SecretKey string
	BaseURL   string
	Proxy     string
}

// OKXConfig OKX 交易所配置
type OKXConfig struct {
	APIKey     string
	SecretKey  string
	Passphrase string
	BaseURL    string
}

// RiskConfig 风控配置
type RiskConfig struct {
	MaxPositionSize     float64 // 单笔最大仓位占比（默认 0.55）
	MaxLeverage         float64 // 最大杠杆（默认 7.0）
	MaxDailyLossPct     float64 // 最大日亏损占比（默认 0.07）
	MaxOpenPositions    int     // 最大同时持仓数（默认 3）
	MaxPositionExposure float64 // 总持仓敞口占比（默认 2.0）
	StopLossPercent     float64 // 默认止损百分比（默认 0.02）
	MinOrderNotional    float64 // 最小下单金额（默认 5.0）
}

// IdempotentConfig 幂等配置
type IdempotentConfig struct {
	TTLSeconds int // 信号去重记录保留时间（秒，默认 600 = 10分钟）
}

// PositionMonitorConfig 主动降仓监控配置
type PositionMonitorConfig struct {
	Enabled           bool          `json:",default=false"`
	CheckInterval     time.Duration `json:",default=30s"`
	DrawdownThreshold float64       `json:",default=0.15"` // 回撤触发阈值（默认 15%）
	ReduceRatio       float64       `json:",default=0.5"`  // 触发后缩减比例（默认 50%）
	MinReduceNotional float64       `json:",default=10"`   // 最小降仓金额，低于此跳过
}

// ClickHouseConfig 定义 ClickHouse 连接与执行分析写入参数。
type ClickHouseConfig struct {
	Enabled       bool          `json:",default=false"`
	Endpoint      string        `json:",optional"`
	Database      string        `json:",default=exchange_analytics"`
	Username      string        `json:",default=default"`
	Password      string        `json:",optional"`
	AccountID     string        `json:",optional"`
	Source        string        `json:",default=execution-rpc"`
	Timeout       time.Duration `json:",default=3s"`
	QueueSize     int           `json:",default=2048"`
	FlushInterval time.Duration `json:",default=1s"`
}

// PostgresConfig 定义 PostgreSQL 连接与写入参数。
type PostgresConfig struct {
	Enabled         bool          `json:",default=false"`
	DSN             string        `json:",optional"`
	AccountID       string        `json:",optional"`
	MaxOpenConns    int           `json:",default=10"`
	MaxIdleConns    int           `json:",default=5"`
	ConnMaxLifetime time.Duration `json:",default=30m"`
}
