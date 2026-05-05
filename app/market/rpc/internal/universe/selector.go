package universe

import (
	"time"

	"exchange-system/app/market/rpc/internal/marketstate"
	"exchange-system/app/market/rpc/internal/strategyrouter"
	"exchange-system/common/regimejudge"
)

// Snapshot 保存某个交易对最近一次用于 Universe 评估的轻量健康快照。
type Snapshot struct {
	Symbol         string
	UpdatedAt      time.Time
	LastEventMs    int64
	IsDirty        bool
	IsTradable     bool
	IsFinal        bool
	LastInterval   string
	Close          float64
	Atr            float64
	Volume         float64
	Ema21          float64
	Ema55          float64
	Kline1m        KlineFrame
	Kline15m       KlineFrame
	Kline1h        KlineFrame
	Kline4h        KlineFrame
	Regime15m      RegimeFrame
	Regime1h       RegimeFrame
	Fusion         RegimeFusion
	RangeGate4H    marketstate.RangeGate
	MarketState    marketstate.MarketState
	MarketAnalysis regimejudge.Analysis
}

// DesiredStrategy 表示 selector 输出的目标策略状态。
type DesiredStrategy struct {
	Symbol       string
	BaseTemplate string
	Template     string
	Bucket       string
	Enabled      bool
	Reason       string
	Overrides    map[string]float64
}

// Config 定义 Phase 1 UniverseSelector 所需的最小配置项。
type Config struct {
	CandidateSymbols []string
	FreshnessWindow  time.Duration
	RequireFinal     bool
	RequireTradable  bool
	RequireClean     bool
	RouterConfig     strategyrouter.Config
}

// Selector 负责做健康门禁，并把健康样本交给显式 Strategy Router 产出模板与策略桶。
type Selector struct {
	cfg    Config
	router *strategyrouter.Router
}

// NewSelector 创建一个带最小健康门禁与显式 Strategy Router 的 UniverseSelector。
func NewSelector(cfg Config) *Selector {
	if cfg.FreshnessWindow <= 0 {
		cfg.FreshnessWindow = 3 * time.Minute
	}
	return &Selector{
		cfg:    cfg,
		router: strategyrouter.New(cfg.RouterConfig.Clone()),
	}
}

// Evaluate 根据当前快照生成每个候选交易对的目标策略状态。
func (s *Selector) Evaluate(now time.Time, snapshots map[string]Snapshot) []DesiredStrategy {
	if s == nil {
		return nil
	}
	desired := make([]DesiredStrategy, 0, len(s.cfg.CandidateSymbols))
	for _, symbol := range s.cfg.CandidateSymbols {
		baseTemplate := s.baseTemplateFor(symbol)
		snap, ok := snapshots[symbol]
		if !ok {
			desired = append(desired, DesiredStrategy{
				Symbol:       symbol,
				BaseTemplate: baseTemplate,
				Template:     baseTemplate,
				Bucket:       s.defaultBucket(baseTemplate),
				Enabled:      false,
				Reason:       "no_snapshot",
			})
			continue
		}
		if healthy, reason := s.isHealthy(now, snap); healthy {
			decision := s.routeDecision(now, snap)
			if decision.Reason == "" {
				decision.Reason = reason
			}
			desired = append(desired, DesiredStrategy{
				Symbol:       symbol,
				BaseTemplate: decision.BaseTemplate,
				Template:     decision.Template,
				Bucket:       string(decision.Bucket),
				Enabled:      decision.Enabled,
				Reason:       decision.Reason,
			})
			continue
		}
		desired = append(desired, DesiredStrategy{
			Symbol:       symbol,
			BaseTemplate: baseTemplate,
			Template:     baseTemplate,
			Bucket:       s.defaultBucket(baseTemplate),
			Enabled:      false,
			Reason:       s.unhealthyReason(now, snap),
		})
	}
	return desired
}

// baseTemplateFor 返回某个交易对当前的基础模板名，供健康门禁前后统一复用。
func (s *Selector) baseTemplateFor(symbol string) string {
	if s == nil || s.router == nil {
		return ""
	}
	return s.router.BaseTemplate(symbol)
}

// routeDecision 把健康快照交给显式 Strategy Router，避免模板与策略桶映射散落在 UniverseSelector 中。
func (s *Selector) routeDecision(now time.Time, snap Snapshot) strategyrouter.Decision {
	if s == nil || s.router == nil {
		return strategyrouter.Decision{
			BaseTemplate: "",
			Template:     "",
			Bucket:       strategyrouter.BucketTrend,
			Enabled:      true,
			Reason:       "healthy_data",
		}
	}
	rangeGate := snap.RangeGate4H
	if !rangeGate.Ready && !snap.Kline4h.UpdatedAt.IsZero() {
		rangeGate = marketstate.EvaluateRangeGate(now, marketstate.BuildFeaturesFromSnapshotValues(
			snap.Symbol,
			"4h",
			snap.Kline4h.Close,
			snap.Kline4h.Ema21,
			snap.Kline4h.Ema55,
			snap.Kline4h.Atr,
			snap.Kline4h.IsDirty,
			snap.Kline4h.IsTradable,
			snap.Kline4h.IsFinal,
			snap.Kline4h.UpdatedAt,
		), marketstate.Features{}, marketstate.Config{})
	}
	return s.router.Route(strategyrouter.Input{
		Symbol:         snap.Symbol,
		Close:          snap.Close,
		Atr:            snap.Atr,
		Ema21:          snap.Ema21,
		Ema55:          snap.Ema55,
		MarketState:    snap.MarketState,
		MarketAnalysis: snap.MarketAnalysis,
		RangeGate4H:    rangeGate,
	})
}

// defaultBucket 根据基础模板返回默认策略桶，用于 no_snapshot 或健康门禁失败场景。
func (s *Selector) defaultBucket(template string) string {
	return string(strategyrouter.RouteBucketForTemplate(template))
}

// isHealthy 对最新快照应用最小健康度校验，判断该交易对是否允许启用策略。
func (s *Selector) isHealthy(now time.Time, snap Snapshot) (bool, string) {
	if s == nil {
		return false, "selector_nil"
	}
	if snap.UpdatedAt.IsZero() {
		return false, "stale_data"
	}
	if s.cfg.FreshnessWindow > 0 && now.Sub(snap.UpdatedAt) > s.cfg.FreshnessWindow {
		return false, "stale_data"
	}
	if s.cfg.RequireClean && snap.IsDirty {
		return false, "dirty_data"
	}
	if s.cfg.RequireTradable && !snap.IsTradable {
		return false, "not_tradable"
	}
	if s.cfg.RequireFinal && !snap.IsFinal {
		return false, "not_final"
	}
	return true, "healthy_data"
}

// unhealthyReason 按与 isHealthy 相同的顺序返回不健康原因，便于日志排查。
func (s *Selector) unhealthyReason(now time.Time, snap Snapshot) string {
	if s == nil {
		return "selector_nil"
	}
	if snap.UpdatedAt.IsZero() {
		return "stale_data"
	}
	if s.cfg.FreshnessWindow > 0 && now.Sub(snap.UpdatedAt) > s.cfg.FreshnessWindow {
		return "stale_data"
	}
	if s.cfg.RequireClean && snap.IsDirty {
		return "dirty_data"
	}
	if s.cfg.RequireTradable && !snap.IsTradable {
		return "not_tradable"
	}
	if s.cfg.RequireFinal && !snap.IsFinal {
		return "not_final"
	}
	return "healthy_data"
}
