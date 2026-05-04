package config

import (
	"time"

	"exchange-system/app/market/rpc/internal/universepool"

	"github.com/zeromicro/go-zero/zrpc"
)

type Config struct {
	zrpc.RpcServerConf
	Kafka struct {
		Addrs  []string
		Topics struct {
			Kline string
			Depth string
		}
	}
	Binance struct {
		WebSocketURL string
		Symbols      []string
		Intervals    []string
		Proxy        string
	}
	KlineLogDir    string        `json:",default=data/kline"`
	WatermarkDelay time.Duration `json:",default=2s"`

	// 指标默认参数（向后兼容，如果某周期未单独配置则使用此默认值）
	Indicators struct {
		Ema21Period int `json:",default=21"`
		Ema55Period int `json:",default=55"`
		RsiPeriod   int `json:",default=14"`
		AtrPeriod   int `json:",default=14"`
	}

	// IntervalIndicators 为每个周期独立配置指标参数，实现"指标按周期计算"
	// 例如：15m K线用 15m 收盘价序列算 EMA(12)，而非用 1m 数据
	// 如果某周期未配置，则回退到 Indicators 默认值
	IntervalIndicators map[string]IndicatorConfig `json:",optional"`

	// IndicatorMode 指标计算模式，控制当前K线是否参与指标计算
	// "closed"（默认）：当前K线参与计算，适合回测和收盘后下单
	// "previous"：只用历史K线，适合实盘未收盘提前信号
	// ⚠️ 安全约束：EmitMode=immediate 时必须使用 IndicatorPrevious
	IndicatorMode string `json:",default=closed,options=closed|previous"`

	// EmitMode K线发射模式，控制 bucket 完成后的发射时机
	// "watermark"（默认）：watermark 确认后才发射，保证数据完整性，适合 Kafka 存储
	// "immediate"：bucket 完成后立即发射，低延迟适合实盘交易信号
	// ⚠️ 安全约束：EmitMode=immediate 时必须搭配 IndicatorMode=previous
	EmitMode string `json:",default=watermark,options=watermark|immediate"`

	// AllowedLateness 延迟容忍窗口（Flink watermark + lateness 模型）
	// 超过 watermark + allowedLateness 的迟到数据将被丢弃，不参与聚合
	// 例如：watermarkDelay=2s, allowedLateness=2m → 数据超过当前 watermark 2 分钟后丢弃
	// 默认 0 表示不丢弃任何迟到数据（兼容现有行为）
	AllowedLateness time.Duration `json:",default=0"`

	// MaxWatermarkBufferAge watermark buffer 中 bucket 的最大存活时间
	// 超过此时间仍未被 watermark 确认的 bucket 将被强制 flush（标记 dirty）
	// 解决：长时间无新数据 / symbol 停止推送 → buffer 内存泄漏
	MaxWatermarkBufferAge time.Duration `json:",default=5m"`

	// WorkerGC worker 空闲清理配置
	// 适用于币对数量多、部分币对可能下架/停止交易的场景，避免内存和 goroutine 泄漏
	WorkerGC struct {
		// GCInterval GC 检查间隔（0 表示禁用 GC）
		GCInterval time.Duration `json:",default=5m"`
		// MaxIdleTime worker 最大空闲时间，超过此时间没有新数据的 worker 将被清理
		MaxIdleTime time.Duration `json:",default=30m"`
	}

	// WarmupPages 预热分页数量，每页最多1500根K线（Binance API 限制）。
	// 更多页 = 更多历史数据 = 递推指标更接近交易所真实值。
	// 默认 3 页（3×1500=4500根），例如：
	//   - 15m: 4500 × 15min ≈ 46.9 天
	//   - 3m:  4500 × 3min  ≈ 9.4 天
	//   - 1h:  4500 × 1h    ≈ 187.5 天
	WarmupPages int `json:",default=3"`

	// UniversePool 为 Phase 3 动态币池预留配置。
	// 第一版先支持 inactive -> warming -> active，不启用删除逻辑。
	UniversePool struct {
		Enabled                  bool          `json:",default=false"`
		LogDir                   string        `json:",default=data/universepool"`
		CandidateSymbols         []string      `json:",optional"`
		AllowList                []string      `json:",optional"`
		BlockList                []string      `json:",optional"`
		ValidationMode           string        `json:",optional"`
		TrendPreferredSymbols    []string      `json:",optional"`
		RangePreferredSymbols    []string      `json:",optional"`
		BreakoutPreferredSymbols []string      `json:",optional"`
		BreakoutAtrPctMin        float64       `json:",optional"`
		BreakoutAtrPctExitMin    float64       `json:",optional"`
		EvaluateInterval         time.Duration `json:",default=30s"`
		MinActiveDuration        time.Duration `json:",default=1h"`
		MinInactiveDuration      time.Duration `json:",default=1h"`
		CooldownDuration         time.Duration `json:",default=1h"`
		AddScoreThreshold        float64       `json:",default=0.75"`
		RemoveScoreThreshold     float64       `json:",default=0.55"`
		Warmup                   universepool.WarmupConfig
	}

	// ClickHouse 配置聚合后的 K 线分析库。
	ClickHouse ClickHouseConfig

	// Postgres 配置聚合数据扩展落库。
	Postgres PostgresConfig
}

// IndicatorConfig 每个周期的指标配置
type IndicatorConfig struct {
	Ema21Period int `json:",default=21"`
	Ema55Period int `json:",default=55"`
	RsiPeriod   int `json:",default=14"`
	AtrPeriod   int `json:",default=14"`
}

// ClickHouseConfig 定义 ClickHouse 连接与写入参数。
type ClickHouseConfig struct {
	Enabled       bool          `json:",default=false"`
	Endpoint      string        `json:",optional"`
	Database      string        `json:",default=exchange_analytics"`
	Username      string        `json:",default=default"`
	Password      string        `json:",optional"`
	Source        string        `json:",default=market-rpc"`
	Timeout       time.Duration `json:",default=3s"`
	QueueSize     int           `json:",default=2048"`
	FlushInterval time.Duration `json:",default=1s"`
	Recovery      RecoveryConfig
}

// RecoveryConfig 定义 market 启动时基于 ClickHouse 的断点补齐参数。
type RecoveryConfig struct {
	Enabled           bool          `json:",default=true"`
	QueryTimeout      time.Duration `json:",default=15s"`
	WarmupWaitTimeout time.Duration `json:",default=2m"`
	PageLimit         int           `json:",default=1500"`
	DeleteOverlap     bool          `json:",default=true"`
	OverlapWindow     time.Duration `json:",default=3m"`
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
