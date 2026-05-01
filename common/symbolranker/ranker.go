package symbolranker

import (
	"sort"

	"exchange-system/common/featureengine"
)

const (
	defaultTrendWeight      = 0.4
	defaultVolatilityWeight = 0.3
	defaultVolumeWeight     = 0.3
)

// Weights 定义选币引擎对各类特征分量的权重。
type Weights struct {
	TrendScore float64
	Volatility float64
	Volume     float64
}

// SymbolScore 表示单个交易对在本轮排序中的最终分数和分量细节。
type SymbolScore struct {
	Symbol          string
	Score           float64
	TrendScore      float64
	VolatilityScore float64
	VolumeScore     float64
	RawTrendScore   float64
	RawVolatility   float64
	RawVolume       float64
}

// Ranker 负责把统一特征转换成可排序的 symbol 分数列表。
type Ranker struct {
	weights Weights
}

// New 创建一个最小可用的 Symbol Ranker，并为缺省权重补齐保守默认值。
func New(weights Weights) *Ranker {
	weights = normalizeWeights(weights)
	return &Ranker{weights: weights}
}

// RankSymbols 对输入特征做过滤、归一化和加权，输出按分数降序排列的结果。
func (r *Ranker) RankSymbols(list []featureengine.Features) []SymbolScore {
	if len(list) == 0 {
		return nil
	}
	rankable := make([]featureengine.Features, 0, len(list))
	for _, item := range list {
		if !isRankable(item) {
			continue
		}
		rankable = append(rankable, featureengine.Normalize(item))
	}
	if len(rankable) == 0 {
		return nil
	}

	trendRange := buildRange(rankable, func(item featureengine.Features) float64 { return item.TrendScore })
	volatilityRange := buildRange(rankable, func(item featureengine.Features) float64 { return item.Volatility })
	volumeRange := buildRange(rankable, func(item featureengine.Features) float64 { return item.Volume })

	scores := make([]SymbolScore, 0, len(rankable))
	for _, item := range rankable {
		trendScore := normalizeValue(item.TrendScore, trendRange.min, trendRange.max)
		volatilityScore := normalizeValue(item.Volatility, volatilityRange.min, volatilityRange.max)
		volumeScore := normalizeValue(item.Volume, volumeRange.min, volumeRange.max)
		score := trendScore*r.weights.TrendScore +
			volatilityScore*r.weights.Volatility +
			volumeScore*r.weights.Volume

		scores = append(scores, SymbolScore{
			Symbol:          item.Symbol,
			Score:           score,
			TrendScore:      trendScore,
			VolatilityScore: volatilityScore,
			VolumeScore:     volumeScore,
			RawTrendScore:   item.TrendScore,
			RawVolatility:   item.Volatility,
			RawVolume:       item.Volume,
		})
	}

	sort.Slice(scores, func(i, j int) bool {
		if scores[i].Score == scores[j].Score {
			return scores[i].Symbol < scores[j].Symbol
		}
		return scores[i].Score > scores[j].Score
	})
	return scores
}

// TopN 从排序结果中截取前 N 个交易对，方便上游直接拿到最小候选池。
func (r *Ranker) TopN(scores []SymbolScore, n int) []SymbolScore {
	if len(scores) == 0 || n <= 0 {
		return nil
	}
	if n >= len(scores) {
		out := make([]SymbolScore, len(scores))
		copy(out, scores)
		return out
	}
	out := make([]SymbolScore, n)
	copy(out, scores[:n])
	return out
}

// normalizeWeights 统一补齐并归一化权重，避免调用方传入零值时无法排序。
func normalizeWeights(weights Weights) Weights {
	if weights.TrendScore == 0 && weights.Volatility == 0 && weights.Volume == 0 {
		return Weights{
			TrendScore: defaultTrendWeight,
			Volatility: defaultVolatilityWeight,
			Volume:     defaultVolumeWeight,
		}
	}
	total := weights.TrendScore + weights.Volatility + weights.Volume
	if total <= 0 {
		return Weights{
			TrendScore: defaultTrendWeight,
			Volatility: defaultVolatilityWeight,
			Volume:     defaultVolumeWeight,
		}
	}
	return Weights{
		TrendScore: weights.TrendScore / total,
		Volatility: weights.Volatility / total,
		Volume:     weights.Volume / total,
	}
}

// isRankable 判断一个特征是否适合参与选币排序，先挡掉不健康或空 symbol 样本。
func isRankable(item featureengine.Features) bool {
	return item.Symbol != "" && item.Healthy
}

type metricRange struct {
	min float64
	max float64
}

// buildRange 扫描同一分量的最小值和最大值，为后续归一化准备边界。
func buildRange(list []featureengine.Features, getter func(item featureengine.Features) float64) metricRange {
	if len(list) == 0 {
		return metricRange{}
	}
	out := metricRange{
		min: getter(list[0]),
		max: getter(list[0]),
	}
	for _, item := range list[1:] {
		value := getter(item)
		if value < out.min {
			out.min = value
		}
		if value > out.max {
			out.max = value
		}
	}
	return out
}

// normalizeValue 用最小最大归一化把不同量纲压缩到 0~1，便于线性加权。
func normalizeValue(value, minValue, maxValue float64) float64 {
	if maxValue <= minValue {
		if value <= 0 {
			return 0
		}
		return 1
	}
	return (value - minValue) / (maxValue - minValue)
}
