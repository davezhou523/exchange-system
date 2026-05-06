package position

import (
	"fmt"
	"log"
	"strings"
	"sync"

	"exchange-system/app/execution/rpc/internal/exchange"
)

// ---------------------------------------------------------------------------
// 仓位管理器
// 维护内存中的仓位状态，与交易所仓位保持同步
// 用途：风控检查、仓位展示、策略状态反馈
// ---------------------------------------------------------------------------

// PositionState 仓位状态（扩展，包含管理字段）
type PositionState struct {
	Symbol           string  // 交易对
	PositionAmount   float64 // 持仓数量（正=多头, 负=空头）
	EntryPrice       float64 // 开仓均价
	MarkPrice        float64 // 标记价格
	UnrealizedPnl    float64 // 未实现盈亏
	LiquidationPrice float64 // 强平价格
	Leverage         float64 // 杠杆倍数
	MarginType       string  // 逐仓/全仓

	// 管理字段
	StrategyID  string    // 开仓策略ID
	EntryTime   int64     // 开仓时间（毫秒时间戳）
	StopLoss    float64   // 止损价
	TakeProfits []float64 // 止盈价列表
	MaxDrawdown float64   // 最大回撤
}

// Manager 仓位管理器
type Manager struct {
	mu        sync.RWMutex
	positions map[string]*PositionState // symbol -> PositionState
}

// NewManager 创建仓位管理器
func NewManager() *Manager {
	return &Manager{
		positions: make(map[string]*PositionState),
	}
}

// UpdateFromExchange 从交易所账户信息同步仓位
func (m *Manager) UpdateFromExchange(account *exchange.AccountResult) {
	if account == nil {
		return
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	// 标记所有仓位为待验证
	pending := make(map[string]bool)
	for sym := range m.positions {
		pending[sym] = true
	}

	// 更新或添加交易所仓位
	for _, pos := range account.Positions {
		if pos.PositionAmount == 0 {
			continue
		}

		if existing, ok := m.positions[pos.Symbol]; ok {
			// 更新已有仓位
			existing.PositionAmount = pos.PositionAmount
			existing.MarkPrice = pos.MarkPrice
			existing.UnrealizedPnl = pos.UnrealizedPnl
			existing.LiquidationPrice = pos.LiquidationPrice
			existing.Leverage = pos.Leverage
			existing.MarginType = pos.MarginType

			// 更新开仓均价（交易所数据为准）
			if pos.EntryPrice > 0 {
				existing.EntryPrice = pos.EntryPrice
			}
			delete(pending, pos.Symbol)
		} else {
			// 新仓位
			m.positions[pos.Symbol] = &PositionState{
				Symbol:           pos.Symbol,
				PositionAmount:   pos.PositionAmount,
				EntryPrice:       pos.EntryPrice,
				MarkPrice:        pos.MarkPrice,
				UnrealizedPnl:    pos.UnrealizedPnl,
				LiquidationPrice: pos.LiquidationPrice,
				Leverage:         pos.Leverage,
				MarginType:       pos.MarginType,
			}
			delete(pending, pos.Symbol)
		}
	}

	// 清除交易所已无记录的仓位
	for sym := range pending {
		delete(m.positions, sym)
		log.Printf("[仓位管理] 清除仓位 | symbol=%s", sym)
	}
}

// UpdateFromOrder 根据订单成交结果更新仓位，reduce-only 场景在本地无持仓时会等待交易所同步，避免误删仓位。
func (m *Manager) UpdateFromOrder(result *exchange.OrderResult, strategyID, signalType string, stopLoss float64, takeProfits []float64) {
	if result == nil || result.Symbol == "" {
		return
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	symbol := result.Symbol
	pos, ok := m.positions[symbol]
	normalizedSignalType := strings.ToUpper(strings.TrimSpace(signalType))
	if !ok {
		if normalizedSignalType == "PARTIAL_CLOSE" || normalizedSignalType == "CLOSE" {
			log.Printf("[仓位管理] 忽略减仓成交 | symbol=%s strategy=%s signal_type=%s 本地无持仓基数，等待交易所同步", symbol, strategyID, normalizedSignalType)
			return
		}
		pos = &PositionState{
			Symbol:      symbol,
			StrategyID:  strategyID,
			StopLoss:    stopLoss,
			TakeProfits: takeProfits,
		}
		m.positions[symbol] = pos
	}

	// 根据订单方向更新仓位数量
	qtyChange := result.ExecutedQuantity
	if result.Side == exchange.SideSell {
		qtyChange = -qtyChange
	}

	newAmt := pos.PositionAmount + qtyChange

	// 更新开仓均价
	if pos.PositionAmount == 0 && newAmt != 0 {
		// 新开仓
		pos.EntryPrice = result.AvgPrice
		pos.StrategyID = strategyID
		pos.StopLoss = stopLoss
		pos.TakeProfits = takeProfits
		pos.EntryTime = result.TransactTime
	}

	// 清仓
	if abs64(newAmt) < 1e-9 {
		delete(m.positions, symbol)
		log.Printf("[仓位管理] 清仓 | symbol=%s strategy=%s", symbol, strategyID)
		return
	}

	pos.PositionAmount = newAmt
	pos.MarkPrice = result.AvgPrice

	log.Printf("[仓位管理] 更新 | symbol=%s 仓位=%.4f 均价=%.2f 策略=%s",
		symbol, newAmt, pos.EntryPrice, pos.StrategyID)
}

// GetPosition 获取指定交易对的仓位
func (m *Manager) GetPosition(symbol string) (*PositionState, bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	pos, ok := m.positions[symbol]
	if !ok {
		return nil, false
	}
	copy := *pos
	return &copy, true
}

// GetAllPositions 获取所有仓位
func (m *Manager) GetAllPositions() []*PositionState {
	m.mu.RLock()
	defer m.mu.RUnlock()

	result := make([]*PositionState, 0, len(m.positions))
	for _, pos := range m.positions {
		copy := *pos
		result = append(result, &copy)
	}
	return result
}

// HasPosition 判断是否有指定交易对的仓位
func (m *Manager) HasPosition(symbol string) bool {
	m.mu.RLock()
	defer m.mu.RUnlock()
	pos, ok := m.positions[symbol]
	return ok && pos.PositionAmount != 0
}

// GetPositionSide 获取仓位方向（多头/空头/无仓位）
func (m *Manager) GetPositionSide(symbol string) string {
	m.mu.RLock()
	defer m.mu.RUnlock()
	pos, ok := m.positions[symbol]
	if !ok || pos.PositionAmount == 0 {
		return "NONE"
	}
	if pos.PositionAmount > 0 {
		return "LONG"
	}
	return "SHORT"
}

// TotalExposure 计算总持仓价值（所有仓位的绝对值之和）
func (m *Manager) TotalExposure() float64 {
	m.mu.RLock()
	defer m.mu.RUnlock()

	total := 0.0
	for _, pos := range m.positions {
		total += abs64(pos.PositionAmount) * pos.MarkPrice
	}
	return total
}

// TotalUnrealizedPnl 计算总未实现盈亏
func (m *Manager) TotalUnrealizedPnl() float64 {
	m.mu.RLock()
	defer m.mu.RUnlock()

	total := 0.0
	for _, pos := range m.positions {
		total += pos.UnrealizedPnl
	}
	return total
}

// FormatPosition 格式化仓位为可读字符串
func FormatPosition(pos *PositionState) string {
	if pos == nil {
		return "<nil>"
	}
	side := "多头"
	if pos.PositionAmount < 0 {
		side = "空头"
	}
	return fmt.Sprintf("%s %s | 数量=%.4f 均价=%.2f 标记=%.2f 盈亏=%.2f | 策略=%s",
		pos.Symbol, side, pos.PositionAmount, pos.EntryPrice, pos.MarkPrice,
		pos.UnrealizedPnl, pos.StrategyID)
}

func abs64(x float64) float64 {
	if x < 0 {
		return -x
	}
	return x
}
