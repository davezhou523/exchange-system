package weights

import (
	"encoding/json"
	"log"
	"os"
	"path/filepath"
	"sync"
	"time"
)

// LogState 管理 weights 结果的 jsonl 文件句柄缓存。
type LogState struct {
	mu    sync.Mutex
	files map[string]*os.File
}

type logEntry struct {
	Timestamp      string  `json:"timestamp"`
	Symbol         string  `json:"symbol"`
	Template       string  `json:"template,omitempty"`
	StrategyWeight float64 `json:"strategy_weight"`
	SymbolWeight   float64 `json:"symbol_weight"`
	RiskScale      float64 `json:"risk_scale"`
	PositionBudget float64 `json:"position_budget"`
	TradingPaused  bool    `json:"trading_paused"`
	PauseReason    string  `json:"pause_reason,omitempty"`
}

type metaLogEntry struct {
	Timestamp           string         `json:"timestamp"`
	SymbolCount         int            `json:"symbol_count"`
	RecommendationCount int            `json:"recommendation_count"`
	PausedCount         int            `json:"paused_count"`
	MarketPaused        bool           `json:"market_paused"`
	MarketPauseReason   string         `json:"market_pause_reason,omitempty"`
	CoolingUntil        string         `json:"cooling_until,omitempty"`
	AtrSpikeRatio       float64        `json:"atr_spike_ratio,omitempty"`
	VolumeSpikeRatio    float64        `json:"volume_spike_ratio,omitempty"`
	MarketState         string         `json:"market_state,omitempty"`
	MarketStateSource   string         `json:"market_state_source,omitempty"`
	TemplateCounts      map[string]int `json:"template_counts,omitempty"`
}

// NewLogState 创建 weights 日志文件句柄缓存。
func NewLogState() *LogState {
	return &LogState{files: make(map[string]*os.File)}
}

// Write 追加一条 symbol 级权重建议日志。
func (l *LogState) Write(baseDir string, rec Recommendation, now time.Time) {
	if l == nil || baseDir == "" || rec.Symbol == "" {
		return
	}
	dateStr := now.UTC().Format("2006-01-02")
	dir := filepath.Join(baseDir, "weights", rec.Symbol)

	l.mu.Lock()
	defer l.mu.Unlock()

	key := rec.Symbol + "/" + dateStr
	f, ok := l.files[key]
	if !ok {
		if err := os.MkdirAll(dir, 0o755); err != nil {
			log.Printf("[weights-log] failed to create dir %s: %v", dir, err)
			return
		}
		path := filepath.Join(dir, dateStr+".jsonl")
		var err error
		f, err = os.OpenFile(path, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o644)
		if err != nil {
			log.Printf("[weights-log] failed to open %s: %v", path, err)
			return
		}
		l.files[key] = f
	}

	entry := logEntry{
		Timestamp:      formatLogTime(now),
		Symbol:         rec.Symbol,
		Template:       rec.Template,
		StrategyWeight: roundFloat(rec.StrategyWeight),
		SymbolWeight:   roundFloat(rec.SymbolWeight),
		RiskScale:      roundFloat(rec.RiskScale),
		PositionBudget: roundFloat(rec.PositionBudget),
		TradingPaused:  rec.TradingPaused,
		PauseReason:    rec.PauseReason,
	}
	b, err := json.Marshal(entry)
	if err != nil {
		log.Printf("[weights-log] failed to marshal symbol=%s: %v", rec.Symbol, err)
		return
	}
	if _, err := f.Write(append(b, '\n')); err != nil {
		log.Printf("[weights-log] failed to write symbol=%s: %v", rec.Symbol, err)
	}
}

// WriteMeta 追加一条权重建议总览日志到 _meta 目录。
func (l *LogState) WriteMeta(baseDir string, entry metaLogEntry, now time.Time) {
	if l == nil || baseDir == "" {
		return
	}
	dateStr := now.UTC().Format("2006-01-02")
	dir := filepath.Join(baseDir, "weights", "_meta")

	l.mu.Lock()
	defer l.mu.Unlock()

	key := "_meta/" + dateStr
	f, ok := l.files[key]
	if !ok {
		if err := os.MkdirAll(dir, 0o755); err != nil {
			log.Printf("[weights-log] failed to create dir %s: %v", dir, err)
			return
		}
		path := filepath.Join(dir, dateStr+".jsonl")
		var err error
		f, err = os.OpenFile(path, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o644)
		if err != nil {
			log.Printf("[weights-log] failed to open %s: %v", path, err)
			return
		}
		l.files[key] = f
	}

	b, err := json.Marshal(entry)
	if err != nil {
		log.Printf("[weights-log] failed to marshal meta: %v", err)
		return
	}
	if _, err := f.Write(append(b, '\n')); err != nil {
		log.Printf("[weights-log] failed to write meta: %v", err)
	}
}

// Close 关闭所有 weights 日志文件句柄。
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

// BuildMetaLogEntry 汇总一轮权重建议，生成总览 jsonl 记录。
func BuildMetaLogEntry(out Output, symbolCount int, marketState string, marketStateSource string, now time.Time) metaLogEntry {
	entry := metaLogEntry{
		Timestamp:           formatLogTime(now),
		SymbolCount:         symbolCount,
		RecommendationCount: len(out.Recommendations),
		MarketPaused:        out.MarketPaused,
		MarketPauseReason:   out.MarketPauseReason,
		AtrSpikeRatio:       roundFloat(out.AtrSpikeRatio),
		VolumeSpikeRatio:    roundFloat(out.VolumeSpikeRatio),
		MarketState:         marketState,
		MarketStateSource:   marketStateSource,
		TemplateCounts:      make(map[string]int),
	}
	if !out.CoolingUntil.IsZero() {
		entry.CoolingUntil = formatLogTime(out.CoolingUntil)
	}
	for _, rec := range out.Recommendations {
		if rec.TradingPaused {
			entry.PausedCount++
		}
		if rec.Template != "" {
			entry.TemplateCounts[rec.Template]++
		}
	}
	if len(entry.TemplateCounts) == 0 {
		entry.TemplateCounts = nil
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
