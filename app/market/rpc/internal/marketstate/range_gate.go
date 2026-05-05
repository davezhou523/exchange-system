package marketstate

import (
	"math"
	"time"
)

// EvaluateRangeGate 基于 4H ADX、EMA 收敛度和 ATR 回落情况判断是否允许进入震荡路由。
func EvaluateRangeGate(now time.Time, current, previous Features, cfg Config) RangeGate {
	cfg = normalizeConfig(cfg)
	gate := RangeGate{
		UpdatedAt: latestRangeGateUpdatedAt(current, previous, now),
	}
	if current.UpdatedAt.IsZero() {
		gate.Reason = "range_gate_h4_missing"
		return gate
	}
	if !current.Healthy {
		gate.Reason = "range_gate_h4_unhealthy"
		return gate
	}
	if !current.IsTradable {
		gate.Reason = "range_gate_h4_not_tradable"
		return gate
	}
	if !current.IsFinal {
		gate.Reason = "range_gate_h4_not_final"
		return gate
	}
	if cfg.FreshnessWindow > 0 && now.Sub(current.UpdatedAt) > cfg.FreshnessWindow {
		gate.Reason = "range_gate_h4_stale"
		return gate
	}
	if current.Ema55 <= 0 || current.Atr <= 0 {
		gate.Reason = "range_gate_h4_missing_features"
		return gate
	}

	gate.Ready = true
	gate.Adx = current.Adx
	if current.Adx > 0 && current.Adx < cfg.RangeGateH4AdxMax {
		gate.AdxOK = true
		gate.Score++
	}
	gate.EmaCloseness = math.Abs(current.Ema21-current.Ema55) / current.Ema55
	if gate.EmaCloseness <= cfg.RangeGateH4EmaCloseMax {
		gate.EmaClosenessOK = true
		gate.Score++
	}
	if previous.Atr > 0 && current.Atr < previous.Atr {
		gate.AtrFalling = true
		gate.Score++
	}
	gate.Passed = gate.Score >= cfg.RangeGateH4ScoreMin
	if gate.Passed {
		gate.Reason = "range_gate_h4_passed"
		return gate
	}
	gate.Reason = "range_gate_h4_failed"
	return gate
}

// latestRangeGateUpdatedAt 返回 4H gate 相关输入里最新的更新时间，便于外层判断门禁是否新鲜。
func latestRangeGateUpdatedAt(current, previous Features, fallback time.Time) time.Time {
	switch {
	case current.UpdatedAt.After(previous.UpdatedAt):
		return current.UpdatedAt.UTC()
	case !previous.UpdatedAt.IsZero():
		return previous.UpdatedAt.UTC()
	default:
		return fallback.UTC()
	}
}
