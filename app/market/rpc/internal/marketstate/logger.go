package marketstate

import (
	"encoding/json"
	"log"
	"os"
	"path/filepath"
	"sync"
	"time"

	"exchange-system/common/regimejudge"
)

// LogState 管理 Market State 结果的 jsonl 文件句柄缓存。
type LogState struct {
	mu    sync.Mutex
	files map[string]*os.File
}

type logEntry struct {
	Timestamp                    string            `json:"timestamp"`
	TimestampBJ                  string            `json:"timestamp_bj"`
	Symbol                       string            `json:"symbol"`
	State                        MarketState       `json:"state"`
	StateDesc                    string            `json:"state_desc,omitempty"`
	Confidence                   float64           `json:"confidence,omitempty"`
	Reason                       string            `json:"reason"`
	ReasonDesc                   string            `json:"reason_desc,omitempty"`
	FallbackSource               string            `json:"fallback_source,omitempty"`
	FallbackSourceDesc           string            `json:"fallback_source_desc,omitempty"`
	FallbackMissingIntervals     []string          `json:"fallback_missing_intervals,omitempty"`
	FallbackMissingIntervalDescs []string          `json:"fallback_missing_interval_descs,omitempty"`
	FallbackIntervalReady        map[string]bool   `json:"fallback_interval_ready,omitempty"`
	FallbackIntervalReasons      map[string]string `json:"fallback_interval_reasons,omitempty"`
	FallbackIntervalReasonDescs  map[string]string `json:"fallback_interval_reason_descs,omitempty"`
	FallbackIntervalAgeSec       map[string]int64  `json:"fallback_interval_age_sec,omitempty"`
	Timeframe                    string            `json:"timeframe,omitempty"`
	Close                        float64           `json:"close,omitempty"`
	Ema21                        float64           `json:"ema21,omitempty"`
	Ema55                        float64           `json:"ema55,omitempty"`
	Atr                          float64           `json:"atr,omitempty"`
	AtrPct                       float64           `json:"atr_pct,omitempty"`
	Healthy                      bool              `json:"healthy"`
	LastReason                   string            `json:"last_reason,omitempty"`
	UpdatedAt                    string            `json:"updated_at,omitempty"`
}

type metaLogEntry struct {
	Timestamp        string            `json:"timestamp"`
	TimestampBJ      string            `json:"timestamp_bj"`
	CandidateCount   int               `json:"candidate_count"`
	ResultCount      int               `json:"result_count"`
	HealthyCount     int               `json:"healthy_count"`
	UnknownCount     int               `json:"unknown_count"`
	GlobalState      string            `json:"global_state,omitempty"`
	GlobalStateDesc  string            `json:"global_state_desc,omitempty"`
	GlobalConfidence float64           `json:"global_confidence,omitempty"`
	GlobalReason     string            `json:"global_reason,omitempty"`
	GlobalReasonDesc string            `json:"global_reason_desc,omitempty"`
	DominantSymbols  []string          `json:"dominant_symbols,omitempty"`
	StateCounts      map[string]int    `json:"state_counts,omitempty"`
	StateCountsDesc  map[string]string `json:"state_counts_desc,omitempty"`
	MatchCounts      map[string]int    `json:"match_counts,omitempty"`
	MatchCountsDesc  map[string]string `json:"match_counts_desc,omitempty"`
	ReasonCounts     map[string]int    `json:"reason_counts,omitempty"`
	ReasonCountsDesc map[string]string `json:"reason_counts_desc,omitempty"`
}

// NewLogState 创建 marketstate 日志文件句柄缓存。
func NewLogState() *LogState {
	return &LogState{files: make(map[string]*os.File)}
}

// Write 追加一条 market state 日志到按 symbol 分目录的 jsonl 文件。
func (l *LogState) Write(baseDir string, features Features, result Result, now time.Time) {
	if l == nil || baseDir == "" || result.Symbol == "" {
		return
	}
	dateStr := now.UTC().Format("2006-01-02")
	dir := filepath.Join(baseDir, "marketstate", result.Symbol)

	l.mu.Lock()
	defer l.mu.Unlock()

	key := result.Symbol + "/" + dateStr
	f, ok := l.files[key]
	if !ok {
		if err := os.MkdirAll(dir, 0o755); err != nil {
			log.Printf("[marketstate-log] failed to create dir %s: %v", dir, err)
			return
		}
		path := filepath.Join(dir, dateStr+".jsonl")
		var err error
		f, err = os.OpenFile(path, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o644)
		if err != nil {
			log.Printf("[marketstate-log] failed to open %s: %v", path, err)
			return
		}
		l.files[key] = f
	}

	entry := logEntry{
		Timestamp:                    formatLogTime(now),
		TimestampBJ:                  formatLogTimeBJ(now),
		Symbol:                       result.Symbol,
		State:                        result.State,
		StateDesc:                    describeMarketState(result.State),
		Confidence:                   roundFloat(result.Confidence),
		Reason:                       result.Reason,
		ReasonDesc:                   describeReasonCode(result.Reason),
		FallbackSource:               result.FallbackSource,
		FallbackSourceDesc:           describeIntervalLabel(result.FallbackSource),
		FallbackMissingIntervals:     cloneStringSlice(result.FallbackMissingIntervals),
		FallbackMissingIntervalDescs: describeIntervalList(result.FallbackMissingIntervals),
		FallbackIntervalReady:        cloneBoolMap(result.FallbackIntervalReady),
		FallbackIntervalReasons:      cloneStringMap(result.FallbackIntervalReasons),
		FallbackIntervalReasonDescs:  describeReasonMap(result.FallbackIntervalReasons),
		FallbackIntervalAgeSec:       cloneInt64Map(result.FallbackIntervalAgeSec),
		Timeframe:                    features.Timeframe,
		Close:                        roundFloat(features.Close),
		Ema21:                        roundFloat(features.Ema21),
		Ema55:                        roundFloat(features.Ema55),
		Atr:                          roundFloat(features.Atr),
		AtrPct:                       roundFloat(features.AtrPct),
		Healthy:                      features.Healthy,
		LastReason:                   features.LastReason,
	}
	if !features.UpdatedAt.IsZero() {
		entry.UpdatedAt = formatLogTime(features.UpdatedAt)
	}

	b, err := json.Marshal(entry)
	if err != nil {
		log.Printf("[marketstate-log] failed to marshal symbol=%s: %v", result.Symbol, err)
		return
	}
	if _, err := f.Write(append(b, '\n')); err != nil {
		log.Printf("[marketstate-log] failed to write symbol=%s: %v", result.Symbol, err)
	}
}

// WriteMeta 追加一条 market state 总览日志到 _meta 目录。
func (l *LogState) WriteMeta(baseDir string, entry metaLogEntry, now time.Time) {
	if l == nil || baseDir == "" {
		return
	}
	entry.Timestamp = formatLogTime(now)
	entry.TimestampBJ = formatLogTimeBJ(now)
	dateStr := now.UTC().Format("2006-01-02")
	dir := filepath.Join(baseDir, "marketstate", "_meta")

	l.mu.Lock()
	defer l.mu.Unlock()

	key := "_meta/" + dateStr
	f, ok := l.files[key]
	if !ok {
		if err := os.MkdirAll(dir, 0o755); err != nil {
			log.Printf("[marketstate-log] failed to create dir %s: %v", dir, err)
			return
		}
		path := filepath.Join(dir, dateStr+".jsonl")
		var err error
		f, err = os.OpenFile(path, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o644)
		if err != nil {
			log.Printf("[marketstate-log] failed to open %s: %v", path, err)
			return
		}
		l.files[key] = f
	}

	b, err := json.Marshal(entry)
	if err != nil {
		log.Printf("[marketstate-log] failed to marshal meta: %v", err)
		return
	}
	if _, err := f.Write(append(b, '\n')); err != nil {
		log.Printf("[marketstate-log] failed to write meta: %v", err)
	}
}

// Close 关闭所有 marketstate 日志文件句柄。
func (l *LogState) Close() error {
	if l == nil {
		return nil
	}
	l.mu.Lock()
	defer l.mu.Unlock()

	var firstErr error
	for key, f := range l.files {
		if f == nil {
			delete(l.files, key)
			continue
		}
		if err := f.Close(); err != nil && firstErr == nil {
			firstErr = err
		}
		delete(l.files, key)
	}
	return firstErr
}

// BuildMetaLogEntry 汇总一轮 market state 结果和统一 Analysis，生成总览 jsonl 记录。
func BuildMetaLogEntry(now time.Time, features map[string]Features, analyses map[string]regimejudge.Analysis, results map[string]Result) metaLogEntry {
	aggregate := Aggregate(now, analyses, results)
	entry := metaLogEntry{
		Timestamp:        formatLogTime(now),
		TimestampBJ:      formatLogTimeBJ(now),
		CandidateCount:   len(results),
		ResultCount:      len(results),
		HealthyCount:     aggregate.HealthyCount,
		UnknownCount:     aggregate.UnknownCount,
		GlobalState:      string(aggregate.State),
		GlobalStateDesc:  describeMarketState(aggregate.State),
		GlobalConfidence: roundFloat(aggregate.Confidence),
		GlobalReason:     aggregate.Reason,
		GlobalReasonDesc: describeReasonCode(aggregate.Reason),
		DominantSymbols:  aggregate.DominantSymbols,
		StateCounts:      make(map[string]int),
		StateCountsDesc:  make(map[string]string),
		MatchCounts:      aggregate.MatchCounts,
		MatchCountsDesc:  describeCountStateMap(aggregate.MatchCounts),
		ReasonCounts:     make(map[string]int),
		ReasonCountsDesc: make(map[string]string),
	}
	for symbol, result := range results {
		entry.StateCounts[string(result.State)]++
		entry.StateCountsDesc[string(result.State)] = describeMarketState(result.State)
		entry.ReasonCounts[result.Reason]++
		entry.ReasonCountsDesc[result.Reason] = describeReasonCode(result.Reason)
		_, _ = features[symbol]
	}
	if len(entry.StateCounts) == 0 {
		entry.StateCounts = nil
	}
	if len(entry.StateCountsDesc) == 0 {
		entry.StateCountsDesc = nil
	}
	if len(entry.MatchCounts) == 0 {
		entry.MatchCounts = nil
	}
	if len(entry.MatchCountsDesc) == 0 {
		entry.MatchCountsDesc = nil
	}
	if len(entry.ReasonCounts) == 0 {
		entry.ReasonCounts = nil
	}
	if len(entry.ReasonCountsDesc) == 0 {
		entry.ReasonCountsDesc = nil
	}
	return entry
}

// describeMarketState 把 marketstate 的英文状态码转换成中文说明，便于直接查看日志。
func describeMarketState(state MarketState) string {
	switch state {
	case MarketStateTrendUp:
		return "上升趋势"
	case MarketStateTrendDown:
		return "下降趋势"
	case MarketStateRange:
		return "震荡"
	case MarketStateBreakout:
		return "突破"
	case MarketStateUnknown:
		return "未知"
	default:
		if state == "" {
			return ""
		}
		return string(state)
	}
}

// describeReasonCode 把 marketstate 相关英文原因码转换成中文说明，减少排查时来回查文档。
func describeReasonCode(code string) string {
	switch code {
	case "":
		return ""
	case "no_results":
		return "当前轮没有可汇总的 marketstate 结果"
	case "all_unknown":
		return "当前轮结果全部为未知状态"
	case "dominant_match_surface":
		return "全局状态由命中面最强的形态主导"
	case "dominant_state":
		return "全局状态按最终状态分布选出主导态"
	case "insufficient_features":
		return "输入特征不足，暂时无法判态"
	case "no_frame":
		return "当前周期暂无可用快照"
	case "missing_kline_frame":
		return "当前周期未收到 K 线快照"
	case "missing_updated_at":
		return "当前周期快照缺少更新时间"
	case "unhealthy_data":
		return "输入数据不健康，暂时无法判态"
	case "stale_data":
		return "快照已过期，当前周期未就绪"
	case "dirty_data":
		return "快照仍为脏数据，当前周期未就绪"
	case "not_tradable":
		return "快照当前不可交易，当前周期未就绪"
	case "not_final":
		return "快照尚未最终确认，当前周期未就绪"
	case "stale_features":
		return "输入特征已过期"
	case "missing_trend_features":
		return "趋势判定所需特征不足"
	case "atr_pct_high":
		return "波动率偏高，更偏向突破态"
	case "atr_pct_low":
		return "波动率偏低，更偏向震荡态"
	case "ema_bull_alignment":
		return "EMA 多头排列，更偏向上升趋势"
	case "ema_bear_alignment":
		return "EMA 空头排列，更偏向下降趋势"
	case "fallback_range":
		return "未命中突破或严格趋势，回退归类为震荡"
	case "fallback_1m_only":
		return "高周期不可用，当前仅使用 1m 判态结果兜底"
	case "fusion_unavailable":
		return "多周期融合结果暂不可用"
	case "timeframes_aligned":
		return "1H 与 15M 周期状态一致"
	case "h1_only":
		return "仅 1H 周期可用，由 1H 主导"
	case "m15_only":
		return "仅 15M 周期可用，由 15M 主导"
	case "h1_primary_dominant":
		return "1H 主周期占优，压过 15M 辅助周期"
	case "m15_confirm_override":
		return "15M 确认信号更强，覆盖主周期判断"
	case "weighted_fusion":
		return "由 1H/15M 加权融合后得出"
	default:
		return code
	}
}

// describeCountStateMap 为 state_counts 和 match_counts 生成中文映射，便于直接理解各键含义。
func describeCountStateMap(counts map[string]int) map[string]string {
	if len(counts) == 0 {
		return nil
	}
	descs := make(map[string]string, len(counts))
	for state := range counts {
		descs[state] = describeMarketState(MarketState(state))
	}
	return descs
}

// describeIntervalList 把周期列表转换成中文说明，便于直接看出哪些周期当前不可用。
func describeIntervalList(intervals []string) []string {
	if len(intervals) == 0 {
		return nil
	}
	descs := make([]string, 0, len(intervals))
	for _, interval := range intervals {
		label := describeIntervalLabel(interval)
		if label == "" {
			continue
		}
		descs = append(descs, label+" 周期不可用")
	}
	if len(descs) == 0 {
		return nil
	}
	return descs
}

// describeIntervalLabel 把周期键转换成可直接展示的中文标签。
func describeIntervalLabel(interval string) string {
	switch interval {
	case "":
		return ""
	case "1m":
		return "1m"
	case "15m":
		return "15m"
	case "1h":
		return "1h"
	default:
		return interval
	}
}

// describeReasonMap 把周期原因码 map 转成中文说明 map，便于逐周期查看未就绪原因。
func describeReasonMap(reasons map[string]string) map[string]string {
	if len(reasons) == 0 {
		return nil
	}
	descs := make(map[string]string, len(reasons))
	for interval, reason := range reasons {
		descs[interval] = describeReasonCode(reason)
	}
	return descs
}

// cloneStringSlice 复制字符串切片，避免日志对象与运行态共享底层数组。
func cloneStringSlice(items []string) []string {
	if len(items) == 0 {
		return nil
	}
	out := make([]string, len(items))
	copy(out, items)
	return out
}

// cloneBoolMap 复制 bool map，避免日志对象与运行态共享底层状态。
func cloneBoolMap(in map[string]bool) map[string]bool {
	if len(in) == 0 {
		return nil
	}
	out := make(map[string]bool, len(in))
	for key, value := range in {
		out[key] = value
	}
	return out
}

// cloneStringMap 复制字符串 map，避免日志对象与运行态共享底层状态。
func cloneStringMap(in map[string]string) map[string]string {
	if len(in) == 0 {
		return nil
	}
	out := make(map[string]string, len(in))
	for key, value := range in {
		out[key] = value
	}
	return out
}

// cloneInt64Map 复制 int64 map，避免日志对象与运行态共享底层状态。
func cloneInt64Map(in map[string]int64) map[string]int64 {
	if len(in) == 0 {
		return nil
	}
	out := make(map[string]int64, len(in))
	for key, value := range in {
		out[key] = value
	}
	return out
}

func formatLogTime(t time.Time) string {
	t = t.UTC()
	if t.Nanosecond()/int(time.Millisecond) == 0 {
		return t.Format("2006-01-02 15:04:05 UTC")
	}
	return t.Format("2006-01-02 15:04:05.000 UTC")
}

// formatLogTimeBJ 把日志时间统一格式化为北京时间字符串，便于直接对照东八区的运行窗口。
func formatLogTimeBJ(t time.Time) string {
	bj := t.UTC().In(time.FixedZone("CST", 8*3600))
	if bj.Nanosecond()/int(time.Millisecond) == 0 {
		return bj.Format("2006-01-02 15:04:05 CST")
	}
	return bj.Format("2006-01-02 15:04:05.000 CST")
}

func roundFloat(v float64) float64 {
	return float64(int(v*10000)) / 10000
}
