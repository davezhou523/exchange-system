package marketstate

import (
	"encoding/json"
	"log"
	"os"
	"path/filepath"
	"sync"
	"time"
)

// LogState 管理 Market State 结果的 jsonl 文件句柄缓存。
type LogState struct {
	mu    sync.Mutex
	files map[string]*os.File
}

type logEntry struct {
	Timestamp  string      `json:"timestamp"`
	Symbol     string      `json:"symbol"`
	State      MarketState `json:"state"`
	Confidence float64     `json:"confidence,omitempty"`
	Reason     string      `json:"reason"`
	Timeframe  string      `json:"timeframe,omitempty"`
	Close      float64     `json:"close,omitempty"`
	Ema21      float64     `json:"ema21,omitempty"`
	Ema55      float64     `json:"ema55,omitempty"`
	Atr        float64     `json:"atr,omitempty"`
	AtrPct     float64     `json:"atr_pct,omitempty"`
	Healthy    bool        `json:"healthy"`
	LastReason string      `json:"last_reason,omitempty"`
	UpdatedAt  string      `json:"updated_at,omitempty"`
}

type metaLogEntry struct {
	Timestamp        string         `json:"timestamp"`
	CandidateCount   int            `json:"candidate_count"`
	ResultCount      int            `json:"result_count"`
	HealthyCount     int            `json:"healthy_count"`
	UnknownCount     int            `json:"unknown_count"`
	GlobalState      string         `json:"global_state,omitempty"`
	GlobalConfidence float64        `json:"global_confidence,omitempty"`
	GlobalReason     string         `json:"global_reason,omitempty"`
	DominantSymbols  []string       `json:"dominant_symbols,omitempty"`
	StateCounts      map[string]int `json:"state_counts,omitempty"`
	ReasonCounts     map[string]int `json:"reason_counts,omitempty"`
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
		Timestamp:  formatLogTime(now),
		Symbol:     result.Symbol,
		State:      result.State,
		Confidence: roundFloat(result.Confidence),
		Reason:     result.Reason,
		Timeframe:  features.Timeframe,
		Close:      roundFloat(features.Close),
		Ema21:      roundFloat(features.Ema21),
		Ema55:      roundFloat(features.Ema55),
		Atr:        roundFloat(features.Atr),
		AtrPct:     roundFloat(features.AtrPct),
		Healthy:    features.Healthy,
		LastReason: features.LastReason,
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

// BuildMetaLogEntry 汇总一轮 market state 结果，生成总览 jsonl 记录。
func BuildMetaLogEntry(now time.Time, features map[string]Features, results map[string]Result) metaLogEntry {
	aggregate := Aggregate(now, results)
	entry := metaLogEntry{
		Timestamp:        formatLogTime(now),
		CandidateCount:   len(results),
		ResultCount:      len(results),
		GlobalState:      string(aggregate.State),
		GlobalConfidence: roundFloat(aggregate.Confidence),
		GlobalReason:     aggregate.Reason,
		DominantSymbols:  aggregate.DominantSymbols,
		StateCounts:      make(map[string]int),
		ReasonCounts:     make(map[string]int),
	}
	for symbol, result := range results {
		entry.StateCounts[string(result.State)]++
		entry.ReasonCounts[result.Reason]++
		if result.State == MarketStateUnknown {
			entry.UnknownCount++
		}
		if feature, ok := features[symbol]; ok && feature.Healthy {
			entry.HealthyCount++
		}
	}
	if len(entry.StateCounts) == 0 {
		entry.StateCounts = nil
	}
	if len(entry.ReasonCounts) == 0 {
		entry.ReasonCounts = nil
	}
	return entry
}

func formatLogTime(t time.Time) string {
	t = t.UTC()
	if t.Nanosecond()/int(time.Millisecond) == 0 {
		return t.Format("2006-01-02 15:04:05 UTC")
	}
	return t.Format("2006-01-02 15:04:05.000 UTC")
}

func roundFloat(v float64) float64 {
	return float64(int(v*10000)) / 10000
}
