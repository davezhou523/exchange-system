package risk

import (
	"fmt"
	"log"

	"exchange-system/app/execution/rpc/internal/exchange"
	"exchange-system/app/execution/rpc/internal/position"
)

// ---------------------------------------------------------------------------
// 风控管理器
// 多维度风控检查：仓位大小 / 杠杆限制 / 日亏损 / 最大持仓数 / 关联风险
// ---------------------------------------------------------------------------

// RiskConfig 风控配置
type RiskConfig struct {
	MaxPositionSize     float64 // 单笔最大仓位占比（相对余额），默认 0.55（55%）
	MaxLeverage         float64 // 最大允许杠杆，默认 7.0
	MaxDailyLossPct     float64 // 最大日亏损占比，默认 0.07（7%）
	MaxDrawdownPct      float64 // 最大回撤占比，默认 0.15（15%）
	MaxOpenPositions    int     // 最大同时持仓数，默认 3
	MaxPositionExposure float64 // 总持仓敞口占比（所有仓位总价值 / 余额），默认 2.0（200%）
	StopLossPercent     float64 // 默认止损百分比，默认 0.02（2%）
	MinOrderNotional    float64 // 最小下单金额（USDT），默认 5.0
}

// DefaultRiskConfig 默认风控配置
func DefaultRiskConfig() RiskConfig {
	return RiskConfig{
		MaxPositionSize:     0.55,
		MaxLeverage:         7.0,
		MaxDailyLossPct:     0.07,
		MaxDrawdownPct:      0.15,
		MaxOpenPositions:    3,
		MaxPositionExposure: 2.0,
		StopLossPercent:     0.02,
		MinOrderNotional:    5.0,
	}
}

// DailyStats 日内风控统计
type DailyStats struct {
	Date        string  // 日期（UTC）
	TotalPnl    float64 // 当日累计盈亏
	TotalTrades int     // 当日交易次数
	WinTrades   int     // 当日盈利次数
	LossTrades  int     // 当日亏损次数
	PeakEquity  float64 // 当日峰值权益
	MaxDrawdown float64 // 当日最大回撤
}

// Manager 风控管理器
type Manager struct {
	config     RiskConfig
	posManager *position.Manager
	dailyStats DailyStats
}

// NewManager 创建风控管理器
func NewManager(config RiskConfig, posManager *position.Manager) *Manager {
	return &Manager{
		config:     config,
		posManager: posManager,
	}
}

// CheckResult 风控检查结果
type CheckResult struct {
	Passed  bool     // 是否通过
	Reasons []string // 拒绝原因列表
}

// Pass 风控通过
func (r *CheckResult) Pass() *CheckResult {
	r.Passed = true
	return r
}

// Fail 添加拒绝原因
func (r *CheckResult) Fail(reason string) *CheckResult {
	r.Passed = false
	r.Reasons = append(r.Reasons, reason)
	return r
}

// CheckPreOrder 下单前风控检查（核心风控逻辑）
// account: 当前账户信息（余额等）
// symbol: 交易对
// side: 买卖方向
// quantity: 下单数量
// price: 预估成交价格
func (m *Manager) CheckPreOrder(account *exchange.AccountResult, symbol, side string, quantity, price float64) *CheckResult {
	result := &CheckResult{Passed: true}

	if account == nil {
		result.Fail("无法获取账户信息")
		return result
	}

	balance := account.TotalWalletBalance
	if balance <= 0 {
		result.Fail("账户余额为零或负数")
		return result
	}

	// 1. 仓位大小检查：下单金额 ≤ MaxPositionSize × 余额（容差0.1%，避免浮点精度边界拒绝）
	notional := quantity * price
	positionLimit := balance * m.config.MaxPositionSize
	tolerance := positionLimit * 0.001 // 0.1% 容差
	if notional > positionLimit+tolerance {
		result.Fail(fmt.Sprintf("仓位超限: %.2f > %.2f (余额%.2f × %.0f%%)",
			notional, positionLimit, balance, m.config.MaxPositionSize*100))
	}

	// 2. 杠杆检查：下单金额 / 余额 ≤ MaxLeverage
	effectiveLeverage := notional / balance
	if effectiveLeverage > m.config.MaxLeverage {
		result.Fail(fmt.Sprintf("杠杆超限: %.1fx > %.1fx",
			effectiveLeverage, m.config.MaxLeverage))
	}

	// 3. 最大持仓数检查
	if m.posManager != nil {
		positions := m.posManager.GetAllPositions()
		// 如果当前没有该交易对的仓位，新开仓需检查持仓数
		if !m.posManager.HasPosition(symbol) {
			if len(positions) >= m.config.MaxOpenPositions {
				result.Fail(fmt.Sprintf("持仓数超限: %d ≥ %d",
					len(positions), m.config.MaxOpenPositions))
			}
		}

		// 4. 总敞口检查：所有仓位总价值 + 新下单金额 ≤ MaxPositionExposure × 余额
		currentExposure := m.posManager.TotalExposure()
		totalExposure := currentExposure + notional
		exposureLimit := balance * m.config.MaxPositionExposure
		if totalExposure > exposureLimit {
			result.Fail(fmt.Sprintf("总敞口超限: %.2f + %.2f = %.2f > %.2f",
				currentExposure, notional, totalExposure, exposureLimit))
		}
	}

	// 5. 最小下单金额检查
	if notional < m.config.MinOrderNotional {
		result.Fail(fmt.Sprintf("下单金额过小: %.2f < %.2f",
			notional, m.config.MinOrderNotional))
	}

	// 6. 日亏损检查
	if m.dailyStats.TotalPnl < 0 && balance > 0 {
		dailyLossPct := -m.dailyStats.TotalPnl / balance
		if dailyLossPct >= m.config.MaxDailyLossPct {
			result.Fail(fmt.Sprintf("日亏损超限: %.2f%% ≥ %.0f%%",
				dailyLossPct*100, m.config.MaxDailyLossPct*100))
		}
	}

	if !result.Passed {
		log.Printf("[风控] 拒绝下单 | symbol=%s side=%s quantity=%.4f price=%.2f | 原因: %v",
			symbol, side, quantity, price, result.Reasons)
	}

	return result
}

// RecordTrade 记录交易结果（用于更新日内风控统计）
func (m *Manager) RecordTrade(pnl float64) {
	m.dailyStats.TotalPnl += pnl
	m.dailyStats.TotalTrades++
	if pnl >= 0 {
		m.dailyStats.WinTrades++
	} else {
		m.dailyStats.LossTrades++
	}

	// 更新峰值权益和回撤
	equity := m.dailyStats.PeakEquity + pnl
	if equity > m.dailyStats.PeakEquity {
		m.dailyStats.PeakEquity = equity
	}
	drawdown := m.dailyStats.PeakEquity - equity
	if drawdown > m.dailyStats.MaxDrawdown {
		m.dailyStats.MaxDrawdown = drawdown
	}
}

// GetDailyStats 获取日内风控统计
func (m *Manager) GetDailyStats() DailyStats {
	return m.dailyStats
}

// ResetDailyStats 重置日内统计（新一天开始时调用）
func (m *Manager) ResetDailyStats(date string) {
	m.dailyStats = DailyStats{
		Date:       date,
		PeakEquity: 0,
	}
}

// GetConfig 获取风控配置
func (m *Manager) GetConfig() RiskConfig {
	return m.config
}
