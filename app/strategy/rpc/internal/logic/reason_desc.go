package logic

import "strings"

// describeStatusReason 把状态接口里常见的内部英文原因码转换成中文说明，便于前端直接展示。
func describeStatusReason(code string) string {
	switch strings.TrimSpace(code) {
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
	case "unhealthy_data":
		return "输入数据不健康，暂时无法判态"
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
	case "market_state_range":
		return "统一判态支持走震荡策略桶"
	case "market_state_breakout":
		return "统一判态支持走突破策略桶"
	case "market_state_trend":
		return "统一判态支持走趋势策略桶"
	case "trend_strong":
		return "趋势强度满足趋势模板切换条件"
	case "healthy_data":
		return "样本健康，允许沿用当前模板"
	case "volatility_high":
		return "波动偏高，切入高波动保护模板"
	case "volatility_extreme":
		return "波动过高，触发高波动保护并暂停启用"
	case "no_snapshot":
		return "当前轮没有可用快照"
	case "bootstrap_no_snapshot":
		return "启动观察期内暂无快照，先保持观察"
	case "stale_data":
		return "快照已过期，当前不允许启用"
	case "dirty_data":
		return "快照仍为脏数据，当前不允许启用"
	case "not_tradable":
		return "快照当前不可交易"
	case "not_final":
		return "快照尚未最终确认"
	case "open_position":
		return "当前仍有未平仓位，暂不切换/停用"
	case "min_enabled_duration":
		return "仍处于最小启用时长窗口，暂不关闭"
	case "cooldown":
		return "仍处于冷却期，暂不重新启用"
	case "market_cooling_pause":
		return "市场进入冷静期，暂时暂停交易"
	case "risk_limit_triggered":
		return "触发风险限制，暂时暂停交易"
	default:
		return code
	}
}

// describeStatusAction 把状态接口里的运行时动作码转换成中文说明，便于前端直接展示当前执行结果。
func describeStatusAction(action string) string {
	switch strings.TrimSpace(action) {
	case "":
		return ""
	case "bootstrap_observe":
		return "启动观察期内暂无快照，先保持观察"
	case "noop_absent":
		return "当前无运行实例，保持无操作"
	case "keep_open_position":
		return "因仍有持仓，继续保留当前实例"
	case "defer_disable":
		return "暂缓停用，等待满足最小启用时长"
	case "disable_error":
		return "停用策略实例失败"
	case "disable":
		return "已停用当前策略实例"
	case "defer_enable":
		return "暂缓启用，等待冷却期结束"
	case "keep":
		return "当前模板未变化，继续沿用"
	case "defer_switch":
		return "因仍有持仓，暂缓切换模板"
	case "enable_error":
		return "启用策略实例失败"
	case "enable":
		return "已启用目标模板"
	case "switch":
		return "已切换到目标模板"
	default:
		return action
	}
}

// describeServiceStatus 把策略服务状态码转换成中文说明，便于前端直接展示运行状态。
func describeServiceStatus(status string) string {
	switch strings.TrimSpace(status) {
	case "":
		return ""
	case "RUNNING":
		return "运行中"
	case "STOPPED":
		return "已停止"
	case "ERROR":
		return "异常"
	default:
		return status
	}
}

// describeStatusMessage 把策略状态短消息码转换成中文说明，避免前端继续解析 message 文本。
func describeStatusMessage(code string) string {
	switch strings.TrimSpace(code) {
	case "":
		return ""
	case "started":
		return "策略已启动"
	case "stopped":
		return "策略已停止"
	case "not_running":
		return "策略当前未运行"
	case "running":
		return "策略运行中"
	case "allocator_ready":
		return "策略运行中，且已生成最新仓位分配建议"
	case "allocator_paused":
		return "策略运行中，已生成仓位分配建议，但当前处于暂停交易"
	default:
		return code
	}
}
