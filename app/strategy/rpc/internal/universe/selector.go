package universe

import (
	"time"

	"exchange-system/app/strategy/rpc/internal/marketstate"
)

// Snapshot 保存某个交易对最近一次用于 Universe 评估的轻量健康快照。
type Snapshot struct {
	Symbol       string
	UpdatedAt    time.Time
	LastEventMs  int64
	IsDirty      bool
	IsTradable   bool
	IsFinal      bool
	LastInterval string
	Close        float64
	Atr          float64
	Ema21        float64
	Ema55        float64
	MarketState  marketstate.MarketState
}

// DesiredStrategy 表示 selector 输出的目标策略状态。
type DesiredStrategy struct {
	Symbol       string
	BaseTemplate string
	Template     string
	Enabled      bool
	Reason       string
	Overrides    map[string]float64
}

// Config 定义 Phase 1 UniverseSelector 所需的最小配置项。
type Config struct {
	CandidateSymbols      []string
	StaticTemplateMap     map[string]string
	FreshnessWindow       time.Duration
	RequireFinal          bool
	RequireTradable       bool
	RequireClean          bool
	BTCTrendTemplate      string
	BTCTrendAtrPctMax     float64
	HighBetaSafeTemplate  string
	HighBetaSafeSymbols   []string
	HighBetaSafeAtrPct    float64
	HighBetaDisableAtrPct float64
}

// Selector 负责根据最新快照判断哪些交易对应该启用策略实例。
type Selector struct {
	cfg Config
}

// NewSelector 创建一个带默认安全参数的 UniverseSelector。
func NewSelector(cfg Config) *Selector {
	if cfg.FreshnessWindow <= 0 {
		cfg.FreshnessWindow = 3 * time.Minute
	}
	if cfg.BTCTrendAtrPctMax <= 0 {
		cfg.BTCTrendAtrPctMax = 0.003
	}
	if cfg.HighBetaSafeAtrPct <= 0 {
		cfg.HighBetaSafeAtrPct = 0.006
	}
	if cfg.HighBetaDisableAtrPct <= 0 {
		cfg.HighBetaDisableAtrPct = 0.012
	}
	return &Selector{cfg: cfg}
}

// Evaluate 根据当前快照生成每个候选交易对的目标策略状态。
func (s *Selector) Evaluate(now time.Time, snapshots map[string]Snapshot) []DesiredStrategy {
	if s == nil {
		return nil
	}
	desired := make([]DesiredStrategy, 0, len(s.cfg.CandidateSymbols))
	for _, symbol := range s.cfg.CandidateSymbols {
		template := s.templateFor(symbol)
		snap, ok := snapshots[symbol]
		if !ok {
			desired = append(desired, DesiredStrategy{
				Symbol:       symbol,
				BaseTemplate: template,
				Template:     template,
				Enabled:      false,
				Reason:       "no_snapshot",
			})
			continue
		}
		if healthy, reason := s.isHealthy(now, snap); healthy {
			template, enabled, dynamicReason := s.dynamicTemplateDecision(symbol, template, snap)
			if dynamicReason == "" {
				dynamicReason = reason
			}
			desired = append(desired, DesiredStrategy{
				Symbol:       symbol,
				BaseTemplate: s.templateFor(symbol),
				Template:     template,
				Enabled:      enabled,
				Reason:       dynamicReason,
			})
			continue
		}
		desired = append(desired, DesiredStrategy{
			Symbol:       symbol,
			BaseTemplate: template,
			Template:     template,
			Enabled:      false,
			Reason:       s.unhealthyReason(now, snap),
		})
	}
	return desired
}

// templateFor 返回某个交易对当前应使用的策略模板名。
func (s *Selector) templateFor(symbol string) string {
	if s == nil || s.cfg.StaticTemplateMap == nil {
		return ""
	}
	return s.cfg.StaticTemplateMap[symbol]
}

// dynamicTemplateDecision 在基础模板之上，为特定交易对应用最小版动态模板切换规则。
func (s *Selector) dynamicTemplateDecision(symbol, baseTemplate string, snap Snapshot) (string, bool, string) {
	if s == nil {
		return baseTemplate, true, "healthy_data"
	}
	switch symbol {
	case "BTCUSDT":
		if s.cfg.BTCTrendTemplate != "" && s.shouldUseBTCTrend(snap) {
			if snap.MarketState == marketstate.MarketStateTrendUp || snap.MarketState == marketstate.MarketStateTrendDown {
				return s.cfg.BTCTrendTemplate, true, "market_state_trend"
			}
			return s.cfg.BTCTrendTemplate, true, "trend_strong"
		}
	case "SOLUSDT":
		if s.isHighBetaSafeSymbol(symbol) && s.cfg.HighBetaSafeTemplate != "" {
			atrPct := s.atrPct(snap)
			if atrPct >= s.cfg.HighBetaDisableAtrPct {
				return s.cfg.HighBetaSafeTemplate, false, "volatility_extreme"
			}
			if atrPct >= s.cfg.HighBetaSafeAtrPct {
				return s.cfg.HighBetaSafeTemplate, true, "volatility_high"
			}
		}
	}
	return baseTemplate, true, "healthy_data"
}

// shouldUseBTCTrend 判断 BTC 是否应切换到更积极的趋势模板。
func (s *Selector) shouldUseBTCTrend(snap Snapshot) bool {
	if snap.Close <= 0 || snap.Atr <= 0 || snap.Ema21 <= 0 || snap.Ema55 <= 0 {
		return false
	}
	atrPct := s.atrPct(snap)
	if atrPct <= 0 || atrPct > s.cfg.BTCTrendAtrPctMax {
		return false
	}
	return (snap.Close > snap.Ema21 && snap.Ema21 > snap.Ema55) ||
		(snap.Close < snap.Ema21 && snap.Ema21 < snap.Ema55)
}

// isHighBetaSafeSymbol 判断某个交易对是否启用了 high-beta-safe 规则。
func (s *Selector) isHighBetaSafeSymbol(symbol string) bool {
	if s == nil {
		return false
	}
	for _, item := range s.cfg.HighBetaSafeSymbols {
		if item == symbol {
			return true
		}
	}
	return false
}

// atrPct 返回 ATR 相对价格的百分比，用于做波动率 regime 判定。
func (s *Selector) atrPct(snap Snapshot) float64 {
	if snap.Close <= 0 || snap.Atr <= 0 {
		return 0
	}
	return snap.Atr / snap.Close
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
