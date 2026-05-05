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
			Kline  string
			Depth  string
			Signal string
		}
	}
	Binance struct {
		WebSocketURL string
		Symbols      []string
		Intervals    []string
		Proxy        string
	}
	KlineLogDir     string `json:",default=data/kline"`
	SharedWarmupDir string `json:",default=runtime/shared/kline/warmup"`
	// WarmupCleanupOnStartup 控制服务启动时是否先清空共享 warmup 目录。
	// demo 环境建议开启，确保每次排查都基于本轮新快照；prod 环境建议关闭，避免 warmup 失败时失去上一轮可用快照。
	WarmupCleanupOnStartup bool          `json:",default=false"`
	WatermarkDelay         time.Duration `json:",default=2s"`

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

	// StrategyEngine 策略引擎配置，启动策略实例并接收聚合器 K 线。
	StrategyEngine StrategyEngineConfig `json:",optional"`

	// ClickHouse 配置聚合后的 K 线分析库。
	ClickHouse ClickHouseConfig

	// Postgres 配置聚合数据扩展落库。
	Postgres PostgresConfig
}

// StrategyEngineConfig 定义策略引擎参数。
type StrategyEngineConfig struct {
	// 配置列表
	Strategies []StrategyConfig `json:",optional"`
	// 模板参数映射
	Templates map[string]map[string]float64 `json:",optional"`
	// 信号日志目录
	SignalLogDir string `json:",default=data/signal"`
	// HarvestPath LSTM 配置
	HarvestPathLSTM struct {
		Enabled      bool   `json:",default=false"`
		PythonBin    string `json:",optional"`
		ScriptPath   string `json:",optional"`
		DataDir      string `json:",optional"`
		ArtifactsDir string `json:",optional"`
		TimeoutMs    int64  `json:",default=30000"`
	}
	// 市场状态配置
	MarketState struct {
		FreshnessWindow        time.Duration `json:",default=3m"`
		RangeAtrPctMax         float64       `json:",default=0.006"`
		BreakoutAtrPctMin      float64       `json:",default=0.0045"`
		RangeGateH4AdxMax      float64       `json:",default=20"`
		RangeGateH4EmaCloseMax float64       `json:",default=0.005"`
		RangeGateH4ScoreMin    int           `json:",default=2"`
	}
	// 权重配置
	Weights struct {
		DefaultTrendWeight    float64            `json:",default=0.7"`
		DefaultRangeWeight    float64            `json:",default=0.7"`
		DefaultBreakoutWeight float64            `json:",default=0.7"`
		DefaultRiskScale      float64            `json:",default=1.0"`
		LossStreakThreshold   int                `json:",default=3"`
		DailyLossSoftLimit    float64            `json:",default=0.05"`
		DrawdownSoftLimit     float64            `json:",default=0.1"`
		CoolingPauseDuration  time.Duration      `json:",default=30m"`
		AtrSpikeRatioMin      float64            `json:",default=1.5"`
		VolumeSpikeRatioMin   float64            `json:",default=2.0"`
		CoolingMinSamples     int                `json:",default=3"`
		TrendStrategyMix      map[string]float64 `json:",optional"`
		BreakoutStrategyMix   map[string]float64 `json:",optional"`
		RangeStrategyMix      map[string]float64 `json:",optional"`
		TrendSymbolWeights    map[string]float64 `json:",optional"`
		BreakoutSymbolWeights map[string]float64 `json:",optional"`
		RangeSymbolWeights    map[string]float64 `json:",optional"`
	}
	// Universe 选择器配置
	Universe struct {
		Enabled            bool          `json:",default=false"`
		BootstrapDuration  time.Duration `json:",default=5m"`
		EvaluateInterval   time.Duration `json:",default=30s"`
		FreshnessWindow    time.Duration `json:",default=3m"`
		MinEnabledDuration time.Duration `json:",default=5m"`
		CooldownDuration   time.Duration `json:",default=5m"`
		RequireFinal       bool          `json:",default=true"`
		RequireTradable    bool          `json:",default=true"`
		RequireClean       bool          `json:",default=true"`
		CandidateSymbols   []string      `json:",optional"`
		RouterConfig       struct {
			StaticTemplateMap     map[string]string `json:",optional"`
			RangeTemplate         string            `json:",optional"`
			BreakoutTemplate      string            `json:",optional"`
			BTCTrendTemplate      string            `json:",optional"`
			BTCTrendAtrPctMax     float64           `json:",optional"`
			HighBetaSafeTemplate  string            `json:",optional"`
			HighBetaSafeSymbols   []string          `json:",optional"`
			HighBetaSafeAtrPct    float64           `json:",optional"`
			HighBetaDisableAtrPct float64           `json:",optional"`
		}
	}
}

// StrategyConfig 策略实例配置
type StrategyConfig struct {
	Name       string `json:",optional"`
	Symbol     string
	Enabled    bool
	Template   string             `json:",optional"`
	Parameters map[string]float64 `json:",optional"`
	Overrides  map[string]float64 `json:",optional"`
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
