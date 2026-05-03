package featureengine

import (
	"math"
	"time"

	"exchange-system/common/pb/market"
)

// Features 定义统一的市场特征载体，供选币、判态、路由和分仓复用。
type Features struct {
	Symbol     string
	Timeframe  string
	Price      float64
	Close      float64
	Ema21      float64
	Ema55      float64
	Atr        float64
	AtrPct     float64
	Adx        float64
	Rsi        float64
	Volume     float64
	TrendScore float64
	Volatility float64
	Healthy    bool
	LastReason string
	UpdatedAt  time.Time
	IsDirty    bool
	IsTradable bool
	IsFinal    bool
}

// SnapshotValues 表示特征引擎从任意上游输入收拢出的统一原始字段。
type SnapshotValues struct {
	Symbol     string
	Timeframe  string
	Close      float64
	Ema21      float64
	Ema55      float64
	Atr        float64
	AtrPct     float64
	Adx        float64
	Rsi        float64
	Volume     float64
	TrendScore float64
	Volatility float64
	UpdatedAt  time.Time
	IsDirty    bool
	IsTradable bool
	IsFinal    bool
	Healthy    bool
	HasHealth  bool
	LastReason string
}

// Engine 定义统一 Feature Engine 的最小实现，负责把分散输入转换成标准化特征。
type Engine struct{}

// New 创建一个最小版 Feature Engine，供选币、判态、路由和分仓统一复用。
func New() *Engine {
	return &Engine{}
}

// BuildFromKline 把统一的 Kline 消息转换为标准化特征，避免下游重复手工拆字段。
func BuildFromKline(k *market.Kline) Features {
	return New().BuildFromKline(k)
}

// BuildFromKline 把统一的 Kline 消息转换为标准化特征，避免下游重复手工拆字段。
func (e *Engine) BuildFromKline(k *market.Kline) Features {
	if k == nil {
		return Features{Healthy: false, LastReason: "nil_kline"}
	}
	return e.BuildFromSnapshot(SnapshotValues{
		Symbol:     k.Symbol,
		Timeframe:  k.Interval,
		Close:      k.Close,
		Ema21:      k.Ema21,
		Ema55:      k.Ema55,
		Atr:        k.Atr,
		Rsi:        k.Rsi,
		Volume:     k.Volume,
		IsDirty:    k.IsDirty,
		IsTradable: k.IsTradable,
		IsFinal:    k.IsFinal,
		UpdatedAt:  resolveUpdatedAt(k.EventTime),
	})
}

// BuildFromSnapshotValues 把分散字段收拢成统一特征结构，并补齐最小衍生特征。
func BuildFromSnapshotValues(symbol, timeframe string, close, ema21, ema55, atr, rsi, volume float64, isDirty, isTradable, isFinal bool, updatedAt time.Time) Features {
	return New().BuildFromSnapshotValues(symbol, timeframe, close, ema21, ema55, atr, rsi, volume, isDirty, isTradable, isFinal, updatedAt)
}

// BuildFromSnapshotValues 把分散字段收拢成统一特征结构，并补齐最小衍生特征。
func (e *Engine) BuildFromSnapshotValues(symbol, timeframe string, close, ema21, ema55, atr, rsi, volume float64, isDirty, isTradable, isFinal bool, updatedAt time.Time) Features {
	return e.BuildFromSnapshot(SnapshotValues{
		Symbol:     symbol,
		Timeframe:  timeframe,
		Close:      close,
		Ema21:      ema21,
		Ema55:      ema55,
		Atr:        atr,
		Rsi:        rsi,
		Volume:     volume,
		IsDirty:    isDirty,
		IsTradable: isTradable,
		IsFinal:    isFinal,
		UpdatedAt:  updatedAt,
	})
}

// BuildFromSnapshot 把统一原始字段转换为标准化特征，是 Feature Engine 的核心收口入口。
func BuildFromSnapshot(in SnapshotValues) Features {
	return New().BuildFromSnapshot(in)
}

// BuildFromSnapshot 把统一原始字段转换为标准化特征，是 Feature Engine 的核心收口入口。
func (e *Engine) BuildFromSnapshot(in SnapshotValues) Features {
	features := Features{
		Symbol:     in.Symbol,
		Timeframe:  in.Timeframe,
		Price:      in.Close,
		Close:      in.Close,
		Ema21:      in.Ema21,
		Ema55:      in.Ema55,
		Atr:        in.Atr,
		AtrPct:     in.AtrPct,
		Adx:        in.Adx,
		Rsi:        in.Rsi,
		Volume:     in.Volume,
		TrendScore: in.TrendScore,
		Volatility: in.Volatility,
		UpdatedAt:  in.UpdatedAt.UTC(),
		IsDirty:    in.IsDirty,
		IsTradable: in.IsTradable,
		IsFinal:    in.IsFinal,
	}
	features = Normalize(features)
	if in.HasHealth {
		features.Healthy = in.Healthy
		if in.LastReason != "" {
			features.LastReason = in.LastReason
		}
	}
	return features
}

// BuildFeatureMap 批量构建标准化特征，统一供选币、判态和路由层复用。
func BuildFeatureMap(inputs map[string]SnapshotValues) map[string]Features {
	return New().BuildFeatureMap(inputs)
}

// BuildFeatureMap 批量构建标准化特征，统一供选币、判态和路由层复用。
func (e *Engine) BuildFeatureMap(inputs map[string]SnapshotValues) map[string]Features {
	out := make(map[string]Features, len(inputs))
	for symbol, item := range inputs {
		if item.Symbol == "" {
			item.Symbol = symbol
		}
		out[symbol] = e.BuildFromSnapshot(item)
	}
	return out
}

// Normalize 补齐可从基础字段推导的特征，保证不同上游入口产出的结果一致。
func Normalize(in Features) Features {
	if in.Timeframe == "" {
		in.Timeframe = "1m"
	}
	if in.Price <= 0 && in.Close > 0 {
		in.Price = in.Close
	}
	if in.Close <= 0 && in.Price > 0 {
		in.Close = in.Price
	}
	if in.AtrPct <= 0 && in.Close > 0 && in.Atr > 0 {
		in.AtrPct = in.Atr / in.Close
	}
	if in.Volatility <= 0 {
		in.Volatility = deriveVolatility(in)
	}
	if in.TrendScore == 0 {
		in.TrendScore = deriveTrendScore(in)
	}
	in.Healthy, in.LastReason = deriveHealth(in)
	return in
}

// resolveUpdatedAt 优先使用事件时间生成特征时间戳，缺失时回退到当前 UTC。
func resolveUpdatedAt(eventTimeMs int64) time.Time {
	if eventTimeMs > 0 {
		return time.UnixMilli(eventTimeMs).UTC()
	}
	return time.Now().UTC()
}

// deriveVolatility 用 ATR 百分比统一表达最小版波动率特征。
func deriveVolatility(in Features) float64 {
	if in.AtrPct > 0 {
		return in.AtrPct
	}
	if in.Close > 0 && in.Atr > 0 {
		return in.Atr / in.Close
	}
	return 0
}

// deriveTrendScore 用均线间距和价格相对快线的位置近似表达趋势强度。
func deriveTrendScore(in Features) float64 {
	if in.Close <= 0 || in.Ema21 <= 0 || in.Ema55 <= 0 {
		return 0
	}
	emaSpread := math.Abs(in.Ema21-in.Ema55) / in.Close
	priceBias := math.Abs(in.Close-in.Ema21) / in.Close
	score := emaSpread*200 + priceBias*50
	if score > 1 {
		score = 1
	}
	return score
}

// deriveHealth 用统一规则判断当前特征是否健康，并产出可观测原因码。
func deriveHealth(in Features) (bool, string) {
	switch {
	case in.Symbol == "":
		return false, "empty_symbol"
	case in.UpdatedAt.IsZero():
		return false, "no_snapshot"
	case in.IsDirty:
		return false, "dirty_data"
	case !in.IsTradable:
		return false, "not_tradable"
	case !in.IsFinal:
		return false, "not_final"
	default:
		return true, "healthy_data"
	}
}
