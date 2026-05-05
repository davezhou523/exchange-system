package universepool

import (
	"context"
	"log"
	"sort"
	"strings"
	"sync"
	"time"

	"exchange-system/common/featureengine"
	"exchange-system/common/pb/market"
)

// Manager 负责驱动动态币池的状态机、订阅更新和 warmup 过程。
type Manager struct {
	mu        sync.RWMutex
	cfg       Config
	states    map[string]*SymbolRuntimeState
	snapshots map[string]Snapshot
	h4History map[string][]h4GateKline
	selector  Selector
	warmup    WarmupChecker
	subCtrl   SubscriptionController
	logger    Logger
}

type stateSummary struct {
	GlobalState      string
	SnapshotInterval string
	LastSnapshotAt   time.Time
	TrendCount       int
	RangeCount       int
	BreakoutCount    int
	StateVotes       map[string]StateVoteDetail
	Candidates       int
	Snapshots        int
	Fresh            int
	Stale            int
	Inactive         int
	PendingAdd       int
	Warming          int
	Active           int
	PendingRemove    int
	Cooldown         int
}

// NewManager 创建一个动态币池管理器骨架。
func NewManager(cfg Config, selector Selector, warmup WarmupChecker, subCtrl SubscriptionController, logger Logger) *Manager {
	cfg = normalizeConfig(cfg)
	if selector == nil {
		selector = NewBasicSelector(cfg)
	}
	return &Manager{
		cfg:       cfg,
		states:    make(map[string]*SymbolRuntimeState),
		snapshots: make(map[string]Snapshot),
		h4History: make(map[string][]h4GateKline),
		selector:  selector,
		warmup:    warmup,
		subCtrl:   subCtrl,
		logger:    logger,
	}
}

// Start 启动动态币池评估循环。
func (m *Manager) Start(ctx context.Context) {
	if m == nil || !m.cfg.Enabled {
		return
	}
	m.syncObservationSubscriptions()
	ticker := time.NewTicker(m.cfg.EvaluateInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case now := <-ticker.C:
			m.Tick(now.UTC())
		}
	}
}

// Tick 执行一轮动态币池评估并推进状态机。
func (m *Manager) Tick(now time.Time) {
	if m == nil {
		return
	}
	m.syncObservationSubscriptions()
	snapshots := m.snapshotInputs(now)
	desired := DesiredUniverse{}
	if m.selector != nil {
		desired = m.selector.Evaluate(now, snapshots)
	}
	m.applyDesiredUniverse(now, desired)
	states := m.snapshotStates()
	summary := summarizeStates(m.cfg, now, desired, snapshots, states)
	if m.logger != nil {
		m.logger.WriteMeta(now, summary)
	}
	m.logEvaluate(summary)
}

// syncObservationSubscriptions 确保候选币至少维持最小观察订阅，避免 inactive 候选永远没有 snapshot。
func (m *Manager) syncObservationSubscriptions() {
	if m == nil || m.subCtrl == nil {
		return
	}
	desired := m.buildDesiredSubscriptions()
	current := append([]string(nil), m.subCtrl.CurrentSymbols()...)
	sort.Strings(current)
	if stringSlicesEqual(current, desired) {
		return
	}
	if err := m.subCtrl.UpdateSymbols(desired); err != nil {
		log.Printf("[universepool] sync_observation_subscriptions err=%v", err)
	}
}

// SnapshotInputs 返回 selector 本轮评估所需的输入快照。
func (m *Manager) snapshotInputs(_ time.Time) map[string]Snapshot {
	out := make(map[string]Snapshot)
	if m == nil {
		return out
	}
	m.mu.RLock()
	defer m.mu.RUnlock()
	for symbol, snap := range m.snapshots {
		out[symbol] = snap
	}
	return out
}

// applyDesiredUniverse 把目标交易宇宙映射到内部状态机并推进状态。
func (m *Manager) applyDesiredUniverse(now time.Time, desired DesiredUniverse) {
	if m == nil {
		return
	}
	for _, symbol := range m.cfg.AllowList {
		state := m.ensureState(symbol)
		state.Desired = true
		if state.State == "" || state.State == SymbolInactive {
			from := state.State
			state.State = SymbolActive
			state.Subscribed = true
			state.ActiveAt = now
			state.LastStateChange = now
			state.Reason = "allow_list"
			if m.logger != nil {
				m.logger.WriteSymbolEvent(now, *state, m.getWarmupStatus(symbol), "allow_list_activate")
			}
			m.logStateTransition(now, state.Symbol, from, state.State, state.Reason, WarmupStatus{Symbol: state.Symbol})
		}
	}
	for symbol, item := range desired.Symbols {
		state := m.ensureState(symbol)
		if m.logger != nil {
			m.logger.WriteSelectorDecision(now, *state, m.getWarmupStatus(symbol), desired.GlobalState, item)
		}
		state.Desired = item.Desired
		state.Template = item.Template
		if !item.Desired {
			continue
		}
		switch state.State {
		case "", SymbolInactive:
			if err := m.moveToWarming(now, symbol, item.Reason); err != nil {
				log.Printf("[universepool] move_to_warming symbol=%s err=%v", symbol, err)
			}
		case SymbolWarming:
			if m.isWarmupReady(symbol) {
				if err := m.moveToActive(now, symbol, item.Reason); err != nil {
					log.Printf("[universepool] move_to_active symbol=%s err=%v", symbol, err)
				}
			}
		}
	}
}

// moveToWarming 让一个交易对进入 warming 状态，并尝试更新底层订阅集合。
func (m *Manager) moveToWarming(now time.Time, symbol, reason string) error {
	state := m.ensureState(symbol)
	from := state.State
	state.State = SymbolWarming
	state.Reason = reason
	state.Desired = true
	state.WarmupStartedAt = now
	state.LastStateChange = now

	if m.subCtrl != nil {
		symbols := m.buildDesiredSubscriptions(symbol)
		if err := m.subCtrl.UpdateSymbols(symbols); err != nil {
			return err
		}
		state.Subscribed = true
	}
	if m.logger != nil {
		m.logger.WriteSymbolEvent(now, *state, m.getWarmupStatus(symbol), "move_to_warming")
	}
	m.logStateTransition(now, state.Symbol, from, state.State, state.Reason, m.getWarmupStatus(symbol))
	return nil
}

// moveToActive 在 warmup 完成后把交易对切换到 active 状态。
func (m *Manager) moveToActive(now time.Time, symbol, reason string) error {
	state := m.ensureState(symbol)
	from := state.State
	state.State = SymbolActive
	state.Reason = reason
	state.ActiveAt = now
	state.LastStateChange = now
	state.Subscribed = true

	warmup := m.getWarmupStatus(symbol)
	if m.logger != nil {
		m.logger.WriteSymbolEvent(now, *state, warmup, "move_to_active")
	}
	m.logStateTransition(now, state.Symbol, from, state.State, state.Reason, warmup)
	return nil
}

// buildDesiredSubscriptions 生成本轮希望 market 实际订阅的 symbol 集合。
func (m *Manager) buildDesiredSubscriptions(extraSymbols ...string) []string {
	set := make(map[string]struct{})
	for _, symbol := range m.cfg.AllowList {
		if symbol != "" {
			set[symbol] = struct{}{}
		}
	}
	for _, symbol := range m.cfg.CandidateSymbols {
		if symbol != "" {
			set[symbol] = struct{}{}
		}
	}
	m.mu.RLock()
	for symbol, state := range m.states {
		if state == nil {
			continue
		}
		if state.State == SymbolWarming || state.State == SymbolActive {
			set[symbol] = struct{}{}
		}
	}
	m.mu.RUnlock()
	for _, symbol := range extraSymbols {
		if symbol != "" {
			set[symbol] = struct{}{}
		}
	}
	out := make([]string, 0, len(set))
	for symbol := range set {
		out = append(out, symbol)
	}
	sort.Strings(out)
	return out
}

func stringSlicesEqual(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

// snapshotStates 返回当前状态表的只读拷贝，供日志和观测使用。
func (m *Manager) snapshotStates() map[string]SymbolRuntimeState {
	out := make(map[string]SymbolRuntimeState)
	if m == nil {
		return out
	}
	m.mu.RLock()
	defer m.mu.RUnlock()
	for symbol, state := range m.states {
		if state == nil {
			continue
		}
		out[symbol] = *state
	}
	return out
}

// ensureState 获取或初始化某个交易对的运行时状态。
func (m *Manager) ensureState(symbol string) *SymbolRuntimeState {
	m.mu.Lock()
	defer m.mu.Unlock()
	state, ok := m.states[symbol]
	if ok && state != nil {
		return state
	}
	state = &SymbolRuntimeState{
		Symbol: symbol,
		State:  SymbolInactive,
	}
	m.states[symbol] = state
	return state
}

// isWarmupReady 判断某个交易对是否满足从 warming 进入 active 的条件。
func (m *Manager) isWarmupReady(symbol string) bool {
	return m.isWarmupReadyStatus(m.getWarmupStatus(symbol))
}

func (m *Manager) isWarmupReadyStatus(status WarmupStatus) bool {
	if m == nil {
		return false
	}
	cfg := m.cfg.Warmup
	if !cfg.Enabled {
		return true
	}
	if cfg.Min1mBars > 0 && !status.HasEnough1mBars {
		return false
	}
	if cfg.Require15mReady && !status.Has15mReady {
		return false
	}
	if cfg.Require1hReady && !status.Has1hReady {
		return false
	}
	if cfg.Require4hReady && !status.Has4hReady {
		return false
	}
	if cfg.RequireIndicatorsReady && !status.IndicatorsReady {
		return false
	}
	return true
}

// getWarmupStatus 查询某个交易对当前的 warmup 状态。
func (m *Manager) getWarmupStatus(symbol string) WarmupStatus {
	if m == nil || m.warmup == nil {
		return WarmupStatus{Symbol: symbol}
	}
	return m.warmup.GetWarmupStatus(symbol)
}

// logEvaluate 输出当前轮动态币池状态机的聚合观测日志。
func (m *Manager) logEvaluate(summary stateSummary) {
	log.Printf(
		"[universepool] evaluate global_state=%s candidates=%d snapshots=%d fresh=%d stale=%d inactive=%d pending_add=%d warming=%d active=%d pending_remove=%d cooldown=%d",
		summary.GlobalState,
		summary.Candidates,
		summary.Snapshots,
		summary.Fresh,
		summary.Stale,
		summary.Inactive,
		summary.PendingAdd,
		summary.Warming,
		summary.Active,
		summary.PendingRemove,
		summary.Cooldown,
	)
}

// logStateTransition 输出单个交易对的状态迁移日志，便于观察状态机流转。
func (m *Manager) logStateTransition(_ time.Time, symbol string, from, to SymbolLifecycleState, reason string, warmup WarmupStatus) {
	if m == nil || symbol == "" {
		return
	}
	log.Printf(
		"[universepool] state symbol=%s from=%s to=%s reason=%s warmup_ready=%v incomplete=%s",
		symbol,
		string(from),
		string(to),
		reason,
		warmup.Ready,
		warmup.LastIncompleteReason,
	)
}

// summarizeStates 汇总当前状态表和快照新鲜度，生成控制台和 _meta 可复用的观测摘要。
func summarizeStates(cfg Config, now time.Time, desired DesiredUniverse, snapshots map[string]Snapshot, states map[string]SymbolRuntimeState) stateSummary {
	summary := stateSummary{
		GlobalState:      desired.GlobalState,
		SnapshotInterval: validationSnapshotIntervalName(cfg),
		TrendCount:       desired.TrendCount,
		RangeCount:       desired.RangeCount,
		BreakoutCount:    desired.BreakoutCount,
		StateVotes:       desired.StateVotes,
		Candidates:       len(cfg.CandidateSymbols),
		Snapshots:        len(snapshots),
	}
	for _, snap := range snapshots {
		if summary.LastSnapshotAt.IsZero() || snap.UpdatedAt.After(summary.LastSnapshotAt) {
			summary.LastSnapshotAt = snap.UpdatedAt
		}
		if isSnapshotFresh(cfg, now, snap) {
			summary.Fresh++
		} else {
			summary.Stale++
		}
	}
	for _, state := range states {
		switch state.State {
		case SymbolInactive, "":
			summary.Inactive++
		case SymbolPendingAdd:
			summary.PendingAdd++
		case SymbolWarming:
			summary.Warming++
		case SymbolActive:
			summary.Active++
		case SymbolPendingRemove:
			summary.PendingRemove++
		case SymbolCooldown:
			summary.Cooldown++
		default:
			summary.Inactive++
		}
	}
	return summary
}

// isSnapshotFresh 判断动态币池快照是否仍处于可接受的新鲜度窗口内。
func isSnapshotFresh(cfg Config, now time.Time, snap Snapshot) bool {
	if snap.UpdatedAt.IsZero() {
		return false
	}
	return now.Sub(snap.UpdatedAt) <= snapshotFreshnessWindow(cfg)
}

// snapshotFreshnessWindow 统一计算动态币池快照的新鲜度窗口，供 manager 与 selector 共用。
func snapshotFreshnessWindow(cfg Config) time.Duration {
	window := cfg.EvaluateInterval * 3
	if iv := validationSnapshotIntervalDuration(cfg); iv > 0 {
		candidate := iv*2 + cfg.EvaluateInterval
		if candidate > window {
			window = candidate
		}
	}
	if window <= 0 {
		window = 90 * time.Second
	}
	return window
}

// UpdateSnapshotFromKline 使用验证模式对应周期的闭合 K 线更新动态币池评估快照。
func (m *Manager) UpdateSnapshotFromKline(k *market.Kline) {
	if m == nil || k == nil || k.Symbol == "" || !k.IsClosed {
		return
	}
	expectedInterval := validationSnapshotIntervalName(m.cfg)
	m.mu.Lock()
	defer m.mu.Unlock()
	if k.Interval == "4h" {
		m.updateRangeGateSnapshotLocked(k, "live")
	}
	if k.Interval != expectedInterval {
		return
	}
	features := featureengine.BuildFromKline(k)
	prev := m.snapshots[k.Symbol]
	prev.Symbol = features.Symbol
	prev.UpdatedAt = features.UpdatedAt
	prev.LastPrice = features.Price
	prev.Ema21 = features.Ema21
	prev.Ema55 = features.Ema55
	prev.Rsi = features.Rsi
	prev.Atr = features.Atr
	prev.AtrPct = features.AtrPct
	prev.Volume24h = features.Volume
	prev.Healthy = true
	prev.LastReason = "fresh_" + expectedInterval
	m.snapshots[k.Symbol] = prev
}

// HydrateRangeGateWarmup 把启动阶段恢复出的 4H 历史 K 线回灌到动态币池，用于冷启动时立即恢复 range gate。
func (m *Manager) HydrateRangeGateWarmup(symbol string, klines []*market.Kline) int {
	if m == nil || len(klines) == 0 {
		return 0
	}
	normalizedSymbol := strings.ToUpper(strings.TrimSpace(symbol))
	if normalizedSymbol == "" {
		return 0
	}
	items := make([]*market.Kline, 0, len(klines))
	for _, kline := range klines {
		if kline == nil || !kline.IsClosed || !strings.EqualFold(strings.TrimSpace(kline.Interval), "4h") {
			continue
		}
		items = append(items, kline)
	}
	if len(items) == 0 {
		return 0
	}
	sort.Slice(items, func(i, j int) bool {
		return items[i].GetEventTime() < items[j].GetEventTime()
	})
	m.mu.Lock()
	defer m.mu.Unlock()
	hydrated := 0
	for _, kline := range items {
		kline.Symbol = normalizedSymbol
		m.updateRangeGateSnapshotLocked(kline, "warmup")
		hydrated++
	}
	return hydrated
}

// RangeGateSnapshot 返回指定交易对当前缓存的 4H 震荡门禁快照，便于启动诊断和测试直接校验。
func (m *Manager) RangeGateSnapshot(symbol string) (Snapshot, bool) {
	if m == nil {
		return Snapshot{}, false
	}
	normalizedSymbol := strings.ToUpper(strings.TrimSpace(symbol))
	if normalizedSymbol == "" {
		return Snapshot{}, false
	}
	m.mu.RLock()
	defer m.mu.RUnlock()
	snap, ok := m.snapshots[normalizedSymbol]
	if !ok {
		return Snapshot{}, false
	}
	return snap, true
}

// updateRangeGateSnapshotLocked 在收到 4H 闭合 K 线时更新对应 symbol 的 4H 震荡门禁结果。
func (m *Manager) updateRangeGateSnapshotLocked(k *market.Kline, source string) {
	if m == nil || k == nil || k.Symbol == "" {
		return
	}
	history := appendH4GateHistory(m.h4History[k.Symbol], buildH4GateKline(k))
	m.h4History[k.Symbol] = history
	snap := m.snapshots[k.Symbol]
	snap.Symbol = k.Symbol
	snap.RangeGate4H = evaluateRangeGateFromHistory(time.UnixMilli(k.EventTime).UTC(), history, m.cfg)
	snap.RangeGate4HSource = strings.TrimSpace(source)
	m.snapshots[k.Symbol] = snap
}

// normalizeConfig 为骨架阶段补齐最小默认值，避免上层传入空配置时 loop 失效。
func normalizeConfig(cfg Config) Config {
	cfg = applyValidationModeDefaults(cfg)
	if cfg.EvaluateInterval <= 0 {
		cfg.EvaluateInterval = 30 * time.Second
	}
	if cfg.RangeGateH4AdxMax <= 0 {
		cfg.RangeGateH4AdxMax = 20
	}
	if cfg.RangeGateH4EmaCloseMax <= 0 {
		cfg.RangeGateH4EmaCloseMax = 0.005
	}
	if cfg.RangeGateH4ScoreMin <= 0 {
		cfg.RangeGateH4ScoreMin = 2
	}
	return cfg
}

func validationSnapshotIntervalName(cfg Config) string {
	switch cfg.ValidationMode {
	case "5m", "5M":
		return "5m"
	default:
		return "1m"
	}
}

func validationSnapshotIntervalDuration(cfg Config) time.Duration {
	switch validationSnapshotIntervalName(cfg) {
	case "5m":
		return 5 * time.Minute
	default:
		return time.Minute
	}
}
