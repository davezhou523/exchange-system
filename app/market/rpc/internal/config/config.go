package config

import (
	"time"

	"github.com/zeromicro/go-zero/zrpc"
)

type Config struct {
	zrpc.RpcServerConf
	Kafka struct {
		Addrs  []string
		Topics struct {
			Kline string
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
		EmaFastPeriod int `json:",default=12"`
		EmaSlowPeriod int `json:",default=26"`
		RsiPeriod     int `json:",default=14"`
		AtrPeriod     int `json:",default=14"`
	}

	// IntervalIndicators 为每个周期独立配置指标参数，实现"指标按周期计算"
	// 例如：15m K线用 15m 收盘价序列算 EMA(12)，而非用 1m 数据
	// 如果某周期未配置，则回退到 Indicators 默认值
	IntervalIndicators map[string]IndicatorConfig `json:",optional"`

	// IndicatorMode 指标计算模式，控制当前K线是否参与指标计算
	// "closed"（默认）：当前K线参与计算，适合回测和收盘后下单
	// "previous"：只用历史K线，适合实盘未收盘提前信号
	IndicatorMode string `json:",default=closed,options=closed|previous"`

	// EmitMode K线发射模式，控制 bucket 完成后的发射时机
	// "watermark"（默认）：watermark 确认后才发射，保证数据完整性，适合 Kafka 存储
	// "immediate"：bucket 完成后立即发射，低延迟适合实盘交易信号
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
}

// IndicatorConfig 每个周期的指标配置
type IndicatorConfig struct {
	EmaFastPeriod int `json:",default=12"`
	EmaSlowPeriod int `json:",default=26"`
	RsiPeriod     int `json:",default=14"`
	AtrPeriod     int `json:",default=14"`
}
