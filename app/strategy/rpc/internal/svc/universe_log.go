package svc

import (
	"encoding/json"
	"log"
	"os"
	"path/filepath"
	"sync"
	"time"

	"exchange-system/app/strategy/rpc/internal/universe"
)

type universeLogState struct {
	mu    sync.Mutex
	files map[string]*os.File
}

type universeLogEntry struct {
	Timestamp       string  `json:"timestamp"`
	Symbol          string  `json:"symbol"`
	BaseTemplate    string  `json:"base_template,omitempty"`
	Template        string  `json:"template"`
	RouteBucket     string  `json:"route_bucket,omitempty"`
	RouteReason     string  `json:"route_reason,omitempty"`
	Action          string  `json:"action"`
	Reason          string  `json:"reason"`
	Enabled         bool    `json:"enabled"`
	HasStrategy     bool    `json:"has_strategy"`
	HasOpenPosition bool    `json:"has_open_position"`
	CurrentTemplate string  `json:"current_template,omitempty"`
	LastEventTimeMs int64   `json:"last_event_time_ms,omitempty"`
	LastEventTime   string  `json:"last_event_time,omitempty"`
	SnapshotAgeSec  float64 `json:"snapshot_age_sec,omitempty"`
	IsDirty         bool    `json:"is_dirty"`
	IsTradable      bool    `json:"is_tradable"`
	IsFinal         bool    `json:"is_final"`
	LastInterval    string  `json:"last_interval,omitempty"`
}

type universeMetaLogEntry struct {
	Timestamp             string         `json:"timestamp"`
	CandidateCount        int            `json:"candidate_count"`
	EnabledCount          int            `json:"enabled_count"`
	BootstrapObserveCount int            `json:"bootstrap_observe_count,omitempty"`
	HealthyCount          int            `json:"healthy_count,omitempty"`
	StaleCount            int            `json:"stale_count,omitempty"`
	SwitchCount           int            `json:"switch_count,omitempty"`
	ActionCounts          map[string]int `json:"action_counts,omitempty"`
	ReasonCounts          map[string]int `json:"reason_counts,omitempty"`
}

// newUniverseLogState 创建 universe 日志文件句柄缓存。
func newUniverseLogState() *universeLogState {
	return &universeLogState{files: make(map[string]*os.File)}
}

// write 追加一条 universe 调度日志到按 symbol 分目录的 jsonl 文件。
func (u *universeLogState) write(baseDir string, entry universeLogEntry, now time.Time) {
	if u == nil || baseDir == "" || entry.Symbol == "" {
		return
	}
	dateStr := now.UTC().Format("2006-01-02")
	dir := filepath.Join(baseDir, "universe", entry.Symbol)

	u.mu.Lock()
	defer u.mu.Unlock()

	key := entry.Symbol + "/" + dateStr
	f, ok := u.files[key]
	if !ok {
		if err := os.MkdirAll(dir, 0o755); err != nil {
			log.Printf("[universe-log] failed to create dir %s: %v", dir, err)
			return
		}
		path := filepath.Join(dir, dateStr+".jsonl")
		var err error
		f, err = os.OpenFile(path, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o644)
		if err != nil {
			log.Printf("[universe-log] failed to open %s: %v", path, err)
			return
		}
		u.files[key] = f
	}

	b, err := json.Marshal(entry)
	if err != nil {
		log.Printf("[universe-log] failed to marshal symbol=%s: %v", entry.Symbol, err)
		return
	}
	if _, err := f.Write(append(b, '\n')); err != nil {
		log.Printf("[universe-log] failed to write symbol=%s: %v", entry.Symbol, err)
	}
}

// writeMeta 追加一条 universe 总览日志到 _meta 目录，便于快速观察每轮评估结果。
func (u *universeLogState) writeMeta(baseDir string, entry universeMetaLogEntry, now time.Time) {
	if u == nil || baseDir == "" {
		return
	}
	dateStr := now.UTC().Format("2006-01-02")
	dir := filepath.Join(baseDir, "universe", "_meta")

	u.mu.Lock()
	defer u.mu.Unlock()

	key := "_meta/" + dateStr
	f, ok := u.files[key]
	if !ok {
		if err := os.MkdirAll(dir, 0o755); err != nil {
			log.Printf("[universe-log] failed to create dir %s: %v", dir, err)
			return
		}
		path := filepath.Join(dir, dateStr+".jsonl")
		var err error
		f, err = os.OpenFile(path, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o644)
		if err != nil {
			log.Printf("[universe-log] failed to open %s: %v", path, err)
			return
		}
		u.files[key] = f
	}

	b, err := json.Marshal(entry)
	if err != nil {
		log.Printf("[universe-log] failed to marshal meta: %v", err)
		return
	}
	if _, err := f.Write(append(b, '\n')); err != nil {
		log.Printf("[universe-log] failed to write meta: %v", err)
	}
}

// close 关闭所有 universe 日志文件句柄。
func (u *universeLogState) close() error {
	if u == nil {
		return nil
	}
	u.mu.Lock()
	defer u.mu.Unlock()

	var firstErr error
	for key, f := range u.files {
		if f == nil {
			delete(u.files, key)
			continue
		}
		if err := f.Close(); err != nil && firstErr == nil {
			firstErr = err
		}
		delete(u.files, key)
	}
	return firstErr
}

// buildUniverseLogEntry 把一次 universe 调度结果转换成统一的结构化日志。
func buildUniverseLogEntry(now time.Time, decision universe.DesiredStrategy, snap universe.Snapshot, result universeApplyResult) universeLogEntry {
	entry := universeLogEntry{
		Timestamp:       formatUniverseLogTime(now),
		Symbol:          decision.Symbol,
		BaseTemplate:    decision.BaseTemplate,
		Template:        decision.Template,
		RouteBucket:     decision.Bucket,
		RouteReason:     decision.Reason,
		Action:          result.Action,
		Reason:          result.Reason,
		Enabled:         result.Enabled,
		HasStrategy:     result.HasStrategy,
		HasOpenPosition: result.HasOpenPosition,
		CurrentTemplate: result.CurrentTemplate,
		IsDirty:         snap.IsDirty,
		IsTradable:      snap.IsTradable,
		IsFinal:         snap.IsFinal,
		LastInterval:    snap.LastInterval,
	}
	if snap.LastEventMs > 0 {
		entry.LastEventTimeMs = snap.LastEventMs
		entry.LastEventTime = formatUniverseLogTime(time.UnixMilli(snap.LastEventMs).UTC())
	}
	if !snap.UpdatedAt.IsZero() {
		entry.SnapshotAgeSec = roundUniverseLogFloat(now.Sub(snap.UpdatedAt).Seconds())
	}
	return entry
}

// buildUniverseMetaLogEntry 汇总一轮 universe 调度结果，生成总览 jsonl 记录。
func buildUniverseMetaLogEntry(now time.Time, desired []universe.DesiredStrategy, results map[string]universeApplyResult) universeMetaLogEntry {
	entry := universeMetaLogEntry{
		Timestamp:      formatUniverseLogTime(now),
		CandidateCount: len(desired),
		ActionCounts:   make(map[string]int),
		ReasonCounts:   make(map[string]int),
	}
	for _, d := range desired {
		if d.Enabled {
			entry.EnabledCount++
		}
		result, ok := results[d.Symbol]
		if !ok {
			continue
		}
		entry.ActionCounts[result.Action]++
		if result.Action == "bootstrap_observe" {
			entry.BootstrapObserveCount++
		}
		if result.Action == "switch" {
			entry.SwitchCount++
		}
		entry.ReasonCounts[result.Reason]++
		if result.Reason == "healthy_data" {
			entry.HealthyCount++
		}
		if result.Reason == "stale_data" || result.Reason == "no_snapshot" || result.Reason == "bootstrap_no_snapshot" {
			entry.StaleCount++
		}
	}
	if len(entry.ActionCounts) == 0 {
		entry.ActionCounts = nil
	}
	if len(entry.ReasonCounts) == 0 {
		entry.ReasonCounts = nil
	}
	return entry
}

// formatUniverseLogTime 把时间统一格式化成易读的 UTC 字符串。
func formatUniverseLogTime(t time.Time) string {
	t = t.UTC()
	if t.Nanosecond()/int(time.Millisecond) == 0 {
		return t.Format("2006-01-02 15:04:05 UTC")
	}
	return t.Format("2006-01-02 15:04:05.000 UTC")
}

// roundUniverseLogFloat 用于压缩日志中的小数位，提升可读性。
func roundUniverseLogFloat(v float64) float64 {
	return float64(int(v*100)) / 100
}
