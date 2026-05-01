package universepool

import (
	"bytes"
	"encoding/json"
	"log"
	"os"
	"path/filepath"
	"sync"
	"time"
)

type jsonlLogger struct {
	baseDir string
	mu      sync.Mutex
	files   map[string]*os.File
}

type symbolLogEntry struct {
	Timestamp              string               `json:"timestamp"`
	Symbol                 string               `json:"symbol"`
	State                  SymbolLifecycleState `json:"state"`
	Action                 string               `json:"action"`
	GlobalState            string               `json:"global_state,omitempty"`
	Reason                 string               `json:"reason,omitempty"`
	ReasonZh               string               `json:"reason_zh,omitempty"`
	Score                  float64              `json:"score,omitempty"`
	Subscribed             bool                 `json:"subscribed"`
	Desired                bool                 `json:"desired"`
	Template               string               `json:"template,omitempty"`
	WarmupReady            bool                 `json:"warmup_ready"`
	HasEnough1mBars        bool                 `json:"has_enough_1m_bars"`
	Has15mReady            bool                 `json:"has_15m_ready"`
	Has1hReady             bool                 `json:"has_1h_ready"`
	Has4hReady             bool                 `json:"has_4h_ready"`
	IndicatorsReady        bool                 `json:"indicators_ready"`
	LastIncompleteReason   string               `json:"last_incomplete_reason,omitempty"`
	LastIncompleteReasonZh string               `json:"last_incomplete_reason_zh,omitempty"`
	StateVote              *StateVoteDetail     `json:"state_vote,omitempty"`
}

type metaLogEntry struct {
	Timestamp        string                     `json:"timestamp"`
	GlobalState      string                     `json:"global_state,omitempty"`
	SnapshotInterval string                     `json:"snapshot_interval,omitempty"`
	LastSnapshotAt   string                     `json:"last_snapshot_at,omitempty"`
	TrendCount       int                        `json:"trend_count"`
	RangeCount       int                        `json:"range_count"`
	BreakoutCount    int                        `json:"breakout_count"`
	CandidateCount   int                        `json:"candidate_count"`
	SnapshotCount    int                        `json:"snapshot_count"`
	FreshCount       int                        `json:"fresh_count"`
	StaleCount       int                        `json:"stale_count"`
	Inactive         int                        `json:"inactive_count"`
	PendingAdd       int                        `json:"pending_add_count,omitempty"`
	Warming          int                        `json:"warming_count,omitempty"`
	Active           int                        `json:"active_count,omitempty"`
	PendingRemove    int                        `json:"pending_remove_count,omitempty"`
	Cooldown         int                        `json:"cooldown_count,omitempty"`
	StateCounts      map[string]int             `json:"state_counts,omitempty"`
	StateVotes       map[string]StateVoteDetail `json:"state_votes,omitempty"`
}

// NewJSONLLogger 创建动态币池的 jsonl 日志器。
func NewJSONLLogger(baseDir string) Logger {
	return &jsonlLogger{
		baseDir: baseDir,
		files:   make(map[string]*os.File),
	}
}

// WriteSymbolEvent 写入单个交易对的状态变化日志。
func (l *jsonlLogger) WriteSymbolEvent(now time.Time, state SymbolRuntimeState, warmup WarmupStatus, action string) {
	if l == nil || l.baseDir == "" || state.Symbol == "" {
		return
	}
	entry := symbolLogEntry{
		Timestamp:              formatLogTime(now),
		Symbol:                 state.Symbol,
		State:                  state.State,
		Action:                 action,
		Reason:                 state.Reason,
		ReasonZh:               reasonTextZH(state.Reason),
		Subscribed:             state.Subscribed,
		Desired:                state.Desired,
		Template:               state.Template,
		WarmupReady:            warmup.Ready,
		HasEnough1mBars:        warmup.HasEnough1mBars,
		Has15mReady:            warmup.Has15mReady,
		Has1hReady:             warmup.Has1hReady,
		Has4hReady:             warmup.Has4hReady,
		IndicatorsReady:        warmup.IndicatorsReady,
		LastIncompleteReason:   warmup.LastIncompleteReason,
		LastIncompleteReasonZh: incompleteReasonTextZH(warmup.LastIncompleteReason),
	}
	l.writeJSONL(filepath.Join(l.baseDir, state.Symbol), now, entry)
}

// WriteSelectorDecision 写入 selector 对单个交易对的本轮决策快照。
func (l *jsonlLogger) WriteSelectorDecision(now time.Time, state SymbolRuntimeState, warmup WarmupStatus, globalState string, decision DesiredUniverseSymbol) {
	if l == nil || l.baseDir == "" || decision.Symbol == "" {
		return
	}
	entry := symbolLogEntry{
		Timestamp:              formatLogTime(now),
		Symbol:                 decision.Symbol,
		State:                  state.State,
		Action:                 "selector_decision",
		GlobalState:            globalState,
		Reason:                 decision.Reason,
		ReasonZh:               reasonTextZH(decision.Reason),
		Score:                  decision.Score,
		Subscribed:             state.Subscribed,
		Desired:                decision.Desired,
		Template:               decision.Template,
		WarmupReady:            warmup.Ready,
		HasEnough1mBars:        warmup.HasEnough1mBars,
		Has15mReady:            warmup.Has15mReady,
		Has1hReady:             warmup.Has1hReady,
		Has4hReady:             warmup.Has4hReady,
		IndicatorsReady:        warmup.IndicatorsReady,
		LastIncompleteReason:   warmup.LastIncompleteReason,
		LastIncompleteReasonZh: incompleteReasonTextZH(warmup.LastIncompleteReason),
	}
	if decision.StateVote.ClassifiedState != "" || decision.StateVote.ClassifiedReason != "" {
		vote := decision.StateVote
		entry.StateVote = &vote
	}
	l.writeJSONL(filepath.Join(l.baseDir, decision.Symbol), now, entry)
}

// WriteMeta 写入当前轮整体状态的聚合日志。
func (l *jsonlLogger) WriteMeta(now time.Time, summary stateSummary) {
	if l == nil || l.baseDir == "" {
		return
	}
	entry := metaLogEntry{
		Timestamp:        formatLogTime(now),
		GlobalState:      summary.GlobalState,
		SnapshotInterval: summary.SnapshotInterval,
		TrendCount:       summary.TrendCount,
		RangeCount:       summary.RangeCount,
		BreakoutCount:    summary.BreakoutCount,
		CandidateCount:   summary.Candidates,
		SnapshotCount:    summary.Snapshots,
		FreshCount:       summary.Fresh,
		StaleCount:       summary.Stale,
		Inactive:         summary.Inactive,
		PendingAdd:       summary.PendingAdd,
		Warming:          summary.Warming,
		Active:           summary.Active,
		PendingRemove:    summary.PendingRemove,
		Cooldown:         summary.Cooldown,
		StateCounts: map[string]int{
			string(SymbolInactive):      summary.Inactive,
			string(SymbolPendingAdd):    summary.PendingAdd,
			string(SymbolWarming):       summary.Warming,
			string(SymbolActive):        summary.Active,
			string(SymbolPendingRemove): summary.PendingRemove,
			string(SymbolCooldown):      summary.Cooldown,
		},
		StateVotes: summary.StateVotes,
	}
	if !summary.LastSnapshotAt.IsZero() {
		entry.LastSnapshotAt = formatLogTime(summary.LastSnapshotAt)
	}
	l.writeJSONL(filepath.Join(l.baseDir, "_meta"), now, entry)
}

// writeJSONL 将一条结构化日志追加到指定目录下的当天 jsonl 文件中。
func (l *jsonlLogger) writeJSONL(dir string, now time.Time, entry interface{}) {
	dateStr := now.UTC().Format("2006-01-02")
	l.mu.Lock()
	defer l.mu.Unlock()

	if err := os.MkdirAll(dir, 0o755); err != nil {
		log.Printf("[universepool-log] create dir failed %s: %v", dir, err)
		return
	}
	path := filepath.Join(dir, dateStr+".jsonl")
	f, ok := l.files[path]
	if !ok {
		var err error
		f, err = os.OpenFile(path, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o644)
		if err != nil {
			log.Printf("[universepool-log] open file failed %s: %v", path, err)
			return
		}
		l.files[path] = f
	}
	var buf bytes.Buffer
	encoder := json.NewEncoder(&buf)
	// 关闭 HTML 转义，避免日志里的 > 被写成 \u003e，便于值班直接阅读论证文案。
	encoder.SetEscapeHTML(false)
	if err := encoder.Encode(entry); err != nil {
		log.Printf("[universepool-log] marshal failed: %v", err)
		return
	}
	if _, err := f.Write(buf.Bytes()); err != nil {
		log.Printf("[universepool-log] write failed %s: %v", path, err)
	}
}

// formatLogTime 把日志时间统一格式化为 UTC 可读字符串。
func formatLogTime(t time.Time) string {
	t = t.UTC()
	if t.Nanosecond()/int(time.Millisecond) == 0 {
		return t.Format("2006-01-02 15:04:05 UTC")
	}
	return t.Format("2006-01-02 15:04:05.000 UTC")
}

// reasonTextZH 把英文原因码映射成中文文案，便于直接阅读 jsonl 日志。
func reasonTextZH(reason string) string {
	switch reason {
	case "allow_list":
		return "白名单保留"
	case "allow_list_activate":
		return "白名单激活"
	case "block_list":
		return "黑名单过滤"
	case "no_snapshot":
		return "没有快照"
	case "stale_snapshot":
		return "快照过期"
	case "unhealthy":
		return "状态不健康"
	case "score_pass":
		return "评分通过"
	case "score_below_add_threshold":
		return "评分低于加入阈值"
	case "state_filtered":
		return "状态过滤"
	case "state_preferred_score_pass":
		return "状态偏好通过"
	case "move_to_warming":
		return "进入预热"
	case "move_to_active":
		return "进入激活"
	default:
		return ""
	}
}

// incompleteReasonTextZH 把预热未完成原因映射成中文文案，方便排查 warmup 卡点。
func incompleteReasonTextZH(reason string) string {
	switch reason {
	case "invalid_symbol":
		return "交易对无效"
	case "no_worker":
		return "没有预热工作器"
	case "warmup_pending":
		return "预热未完成"
	case "missing_1m_history":
		return "缺少 1m 历史"
	case "missing_15m_history":
		return "缺少 15m 历史"
	case "missing_1h_history":
		return "缺少 1h 历史"
	case "missing_4h_history":
		return "缺少 4h 历史"
	case "indicators_not_ready":
		return "指标未就绪"
	default:
		return ""
	}
}
