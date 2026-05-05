package universepool

import (
	"bytes"
	"encoding/json"
	"fmt"
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
	TimestampBJ            string               `json:"timestamp_bj"`
	Symbol                 string               `json:"symbol"`
	State                  SymbolLifecycleState `json:"state"`
	Action                 string               `json:"action"`
	GlobalState            string               `json:"global_state,omitempty"`
	Reason                 string               `json:"reason,omitempty"`
	ReasonZh               string               `json:"reason_zh,omitempty"`
	Score                  fixedDecimal6        `json:"score,omitempty"`
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
	RangeGateReady         bool                 `json:"range_gate_ready"`
	RangeGatePassed        bool                 `json:"range_gate_passed"`
	RangeGateReason        string               `json:"range_gate_reason,omitempty"`
	RangeGateReasonZh      string               `json:"range_gate_reason_zh,omitempty"`
	RangeGateScore         int                  `json:"range_gate_score,omitempty"`
	RangeGateUpdatedAt     string               `json:"range_gate_updated_at,omitempty"`
	RangeGateUpdatedAtBJ   string               `json:"range_gate_updated_at_bj,omitempty"`
	RangeGateSource        string               `json:"range_gate_source,omitempty"`
	StateVote              *StateVoteDetail     `json:"state_vote,omitempty"`
}

type rangeGateLogEntry struct {
	Ready       bool   `json:"ready"`
	Passed      bool   `json:"passed"`
	Reason      string `json:"reason,omitempty"`
	ReasonZh    string `json:"reason_zh,omitempty"`
	Score       int    `json:"score,omitempty"`
	UpdatedAt   string `json:"updated_at,omitempty"`
	UpdatedAtBJ string `json:"updated_at_bj,omitempty"`
	Source      string `json:"source,omitempty"`
}

// fixedDecimal6 以 6 位小数输出数值，便于日志直接观察 score 的细微变化。
type fixedDecimal6 float64

// MarshalJSON 自定义数值序列化，保持 JSON number 形态同时固定保留 6 位小数。
func (v fixedDecimal6) MarshalJSON() ([]byte, error) {
	return []byte(fmt.Sprintf("%.6f", float64(v))), nil
}

type metaLogEntry struct {
	Timestamp        string                       `json:"timestamp"`
	TimestampBJ      string                       `json:"timestamp_bj"`
	GlobalState      string                       `json:"global_state,omitempty"`
	SnapshotInterval string                       `json:"snapshot_interval,omitempty"`
	LastSnapshotAt   string                       `json:"last_snapshot_at,omitempty"`
	LastSnapshotAtBJ string                       `json:"last_snapshot_at_bj,omitempty"`
	TrendCount       int                          `json:"trend_count"`
	RangeCount       int                          `json:"range_count"`
	BreakoutCount    int                          `json:"breakout_count"`
	CandidateCount   int                          `json:"candidate_count"`
	SnapshotCount    int                          `json:"snapshot_count"`
	FreshCount       int                          `json:"fresh_count"`
	StaleCount       int                          `json:"stale_count"`
	Inactive         int                          `json:"inactive_count"`
	PendingAdd       int                          `json:"pending_add_count,omitempty"`
	Warming          int                          `json:"warming_count,omitempty"`
	Active           int                          `json:"active_count,omitempty"`
	PendingRemove    int                          `json:"pending_remove_count,omitempty"`
	Cooldown         int                          `json:"cooldown_count,omitempty"`
	RangeGateReady   int                          `json:"range_gate_ready_count,omitempty"`
	RangeGatePassed  int                          `json:"range_gate_passed_count,omitempty"`
	RangeGateWarmup  int                          `json:"range_gate_warmup_count,omitempty"`
	RangeGateLive    int                          `json:"range_gate_live_count,omitempty"`
	StateCounts      map[string]int               `json:"state_counts,omitempty"`
	StateVotes       map[string]StateVoteDetail   `json:"state_votes,omitempty"`
	RangeGates       map[string]rangeGateLogEntry `json:"range_gates,omitempty"`
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
		TimestampBJ:            formatLogTimeBJ(now),
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
		TimestampBJ:            formatLogTimeBJ(now),
		Symbol:                 decision.Symbol,
		State:                  state.State,
		Action:                 "selector_decision",
		GlobalState:            globalState,
		Reason:                 decision.Reason,
		ReasonZh:               reasonTextZH(decision.Reason),
		Score:                  fixedDecimal6(decision.Score),
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
	applyRangeGateToSymbolLogEntry(&entry, decision.StateVote)
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
		TimestampBJ:      formatLogTimeBJ(now),
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
	entry.RangeGates, entry.RangeGateReady, entry.RangeGatePassed, entry.RangeGateWarmup, entry.RangeGateLive = buildMetaRangeGates(summary.StateVotes)
	if !summary.LastSnapshotAt.IsZero() {
		entry.LastSnapshotAt = formatLogTime(summary.LastSnapshotAt)
		entry.LastSnapshotAtBJ = formatLogTimeBJ(summary.LastSnapshotAt)
	}
	l.writeJSONL(filepath.Join(l.baseDir, "_meta"), now, entry)
}

// applyRangeGateToSymbolLogEntry 把 state vote 里的 4H gate 结果提升到 selector 日志顶层，方便值班时直接检索。
func applyRangeGateToSymbolLogEntry(entry *symbolLogEntry, vote StateVoteDetail) {
	if entry == nil {
		return
	}
	entry.RangeGateReady = vote.RangeGateReady
	entry.RangeGatePassed = vote.RangeGatePassed
	entry.RangeGateReason = vote.RangeGateReason
	entry.RangeGateReasonZh = rangeGateReasonTextZH(vote.RangeGateReason)
	entry.RangeGateScore = vote.RangeGateScore
	entry.RangeGateSource = vote.RangeGateSource
	if !vote.RangeGateUpdatedAt.IsZero() {
		entry.RangeGateUpdatedAt = formatLogTime(vote.RangeGateUpdatedAt)
		entry.RangeGateUpdatedAtBJ = formatLogTimeBJ(vote.RangeGateUpdatedAt)
	}
}

// buildMetaRangeGates 生成 _meta 级别的 range gate 全量视图与计数，便于对比冷启动回灌和实时接管结果。
func buildMetaRangeGates(votes map[string]StateVoteDetail) (map[string]rangeGateLogEntry, int, int, int, int) {
	if len(votes) == 0 {
		return nil, 0, 0, 0, 0
	}
	rangeGates := make(map[string]rangeGateLogEntry, len(votes))
	readyCount := 0
	passedCount := 0
	warmupCount := 0
	liveCount := 0
	for symbol, vote := range votes {
		item := rangeGateLogEntry{
			Ready:    vote.RangeGateReady,
			Passed:   vote.RangeGatePassed,
			Reason:   vote.RangeGateReason,
			ReasonZh: rangeGateReasonTextZH(vote.RangeGateReason),
			Score:    vote.RangeGateScore,
			Source:   vote.RangeGateSource,
		}
		if !vote.RangeGateUpdatedAt.IsZero() {
			item.UpdatedAt = formatLogTime(vote.RangeGateUpdatedAt)
			item.UpdatedAtBJ = formatLogTimeBJ(vote.RangeGateUpdatedAt)
		}
		rangeGates[symbol] = item
		if vote.RangeGateReady {
			readyCount++
		}
		if vote.RangeGatePassed {
			passedCount++
		}
		switch vote.RangeGateSource {
		case "warmup":
			warmupCount++
		case "live":
			liveCount++
		}
	}
	return rangeGates, readyCount, passedCount, warmupCount, liveCount
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

// formatLogTimeBJ 把日志时间统一格式化为北京时间字符串，便于直接排查启动窗口。
func formatLogTimeBJ(t time.Time) string {
	bj := t.UTC().In(time.FixedZone("CST", 8*3600))
	if bj.Nanosecond()/int(time.Millisecond) == 0 {
		return bj.Format("2006-01-02 15:04:05 CST")
	}
	return bj.Format("2006-01-02 15:04:05.000 CST")
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

// rangeGateReasonTextZH 把 4H range gate 的原因码映射成中文文案，方便直接判断是 warmup 未恢复还是门禁未通过。
func rangeGateReasonTextZH(reason string) string {
	switch reason {
	case "range_gate_h4_passed":
		return "4H 震荡门禁通过"
	case "range_gate_h4_failed":
		return "4H 震荡门禁未通过"
	case "range_gate_h4_missing":
		return "缺少 4H 震荡门禁数据"
	default:
		return ""
	}
}
