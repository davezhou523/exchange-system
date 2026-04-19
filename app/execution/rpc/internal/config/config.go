package config

import "github.com/zeromicro/go-zero/zrpc"

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
			Kline             string // 消费的1m K线 topic（1m撮合模式下使用）
		}
	}

	// 交易所路由配置
	Exchange ExchangeConfig

	// 风控配置
	Risk RiskConfig

	// 幂等配置
	Idempotent IdempotentConfig

	// 日志目录
	SignalLogDir string // 接收信号的日志目录（如 data/signal）
	OrderLogDir  string // 订单结果的日志目录（如 data/order）
}

// ExchangeConfig 交易所路由配置
type ExchangeConfig struct {
	// 路由策略: "direct" | "simulated" | "by_symbol"
	RouteStrategy string

	// 默认交易所名称: "binance" | "okx" | "simulated"
	DefaultExchange string

	// 交易对路由映射（by_symbol 策略用）
	SymbolRoutes map[string]string

	// Binance 配置
	Binance BinanceConfig

	// OKX 配置（可选，不使用时YAML中留空即可）
	OKX OKXConfig `json:",optional"`

	// 模拟撮合配置
	Simulated SimulatedConfig
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
	Simulated  bool
}

// SimulatedConfig 模拟撮合配置
type SimulatedConfig struct {
	Enabled         bool    // 是否启用模拟撮合
	InitialBalance  float64 // 初始余额
	SlippageBPS     float64 // 滑点基点
	SlippageModel   string  // 滑点模型: "fixed" | "random" | "volume"
	CommissionRate  float64 // 手续费率
	CommissionAsset string  // 手续费资产
	FillDelayMs     int64   // 成交延迟毫秒
	MatchMode       string  // 撮合模式: "instant"=即时成交 | "1m"=1m K线驱动撮合
}

// RiskConfig 风控配置
type RiskConfig struct {
	MaxPositionSize     float64 // 单笔最大仓位占比（默认 0.55）
	MaxLeverage         float64 // 最大杠杆（默认 7.0）
	MaxDailyLossPct     float64 // 最大日亏损占比（默认 0.07）
	MaxDrawdownPct      float64 // 最大回撤占比（默认 0.15）
	MaxOpenPositions    int     // 最大同时持仓数（默认 3）
	MaxPositionExposure float64 // 总持仓敞口占比（默认 2.0）
	StopLossPercent     float64 // 默认止损百分比（默认 0.02）
	MinOrderNotional    float64 // 最小下单金额（默认 5.0）
}

// IdempotentConfig 幂等配置
type IdempotentConfig struct {
	TTLSeconds int // 信号去重记录保留时间（秒，默认 600 = 10分钟）
}
