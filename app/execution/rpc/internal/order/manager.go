package order

import (
	"fmt"
	"log"
	"sync"
	"time"

	"exchange-system/app/execution/rpc/internal/exchange"
)

// ---------------------------------------------------------------------------
// 订单管理器
// 跟踪所有订单的生命周期：创建 → 挂单 → 部分成交 → 全部成交 / 取消 / 拒绝
// ---------------------------------------------------------------------------

// OrderState 订单状态（扩展，包含内部管理字段）
type OrderState struct {
	// 订单基本信息
	OrderID      string                // 交易所订单ID
	ClientID     string                // 客户端自定义订单ID
	Symbol       string                // 交易对
	Side         exchange.OrderSide    // 买卖方向
	PositionSide exchange.PositionSide // 持仓方向
	Type         exchange.OrderType    // 订单类型
	Status       exchange.OrderStatus  // 订单状态

	// 数量与价格
	Quantity    float64 // 原始下单数量
	ExecutedQty float64 // 已成交数量
	AvgPrice    float64 // 成交均价
	Price       float64 // 委托价格（LIMIT 单）
	StopPrice   float64 // 止损触发价（STOP 单）

	// 手续费
	Commission      float64 // 手续费
	CommissionAsset string  // 手续费资产

	// 来源信息
	StrategyID string // 策略ID（来源信号）
	SignalKey  string // 信号去重键
	SignalType string // 信号类型：OPEN/CLOSE

	// 策略信号关联
	StopLoss    float64   // 止损价
	TakeProfits []float64 // 止盈价列表
	Atr         float64   // 入场时ATR
	RiskReward  float64   // 风险收益比

	// 时间
	CreateTime   time.Time // 创建时间
	TransactTime time.Time // 最后成交时间

	// 滑点
	Slippage float64 // 滑点金额

	// 错误信息
	ErrorMessage string
}

// Manager 订单管理器
type Manager struct {
	mu         sync.RWMutex
	orders     map[string]*OrderState // orderID -> OrderState
	byClientID map[string]string      // clientOrderID -> orderID 索引
}

// NewManager 创建订单管理器
func NewManager() *Manager {
	return &Manager{
		orders:     make(map[string]*OrderState),
		byClientID: make(map[string]string),
	}
}

// AddOrder 添加订单到管理器
func (m *Manager) AddOrder(state *OrderState) {
	if state == nil || state.OrderID == "" {
		return
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	m.orders[state.OrderID] = state
	if state.ClientID != "" {
		m.byClientID[state.ClientID] = state.OrderID
	}

	log.Printf("[订单管理] 添加订单 | ID=%s 客户端ID=%s %s %s %s | 数量=%.4f 价格=%.2f",
		state.OrderID, state.ClientID, state.Side, state.PositionSide, state.Symbol,
		state.Quantity, state.Price)
}

// UpdateOrder 更新订单状态
func (m *Manager) UpdateOrder(orderID string, result *exchange.OrderResult) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	state, ok := m.orders[orderID]
	if !ok {
		// 新订单（可能是交易所主动推送的），创建新记录
		state = &OrderState{
			OrderID:      orderID,
			ClientID:     result.ClientOrderID,
			Symbol:       result.Symbol,
			Side:         result.Side,
			PositionSide: result.PositionSide,
			Status:       result.Status,
			CreateTime:   time.Now(),
		}
		m.orders[orderID] = state
		if result.ClientOrderID != "" {
			m.byClientID[result.ClientOrderID] = orderID
		}
	}

	// 更新字段
	state.Status = result.Status
	state.ExecutedQty = result.ExecutedQuantity
	state.AvgPrice = result.AvgPrice
	state.Commission = result.Commission
	state.CommissionAsset = result.CommissionAsset
	state.Slippage = result.Slippage
	state.ErrorMessage = result.ErrorMessage
	if result.TransactTime > 0 {
		state.TransactTime = time.UnixMilli(result.TransactTime)
	}

	return nil
}

// GetOrder 按 orderID 查询订单
func (m *Manager) GetOrder(orderID string) (*OrderState, bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	state, ok := m.orders[orderID]
	if !ok {
		return nil, false
	}
	// 返回副本避免外部修改
	copy := *state
	return &copy, true
}

// GetOrderByClientID 按客户端自定义ID查询订单
func (m *Manager) GetOrderByClientID(clientID string) (*OrderState, bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	orderID, ok := m.byClientID[clientID]
	if !ok {
		return nil, false
	}
	state, ok := m.orders[orderID]
	if !ok {
		return nil, false
	}
	copy := *state
	return &copy, true
}

// GetActiveOrders 获取所有活跃订单（未完成）
func (m *Manager) GetActiveOrders() []*OrderState {
	m.mu.RLock()
	defer m.mu.RUnlock()

	var active []*OrderState
	for _, state := range m.orders {
		if state.Status == exchange.StatusNew || state.Status == exchange.StatusPartiallyFilled {
			copy := *state
			active = append(active, &copy)
		}
	}
	return active
}

// GetOrdersBySymbol 获取指定交易对的所有订单
func (m *Manager) GetOrdersBySymbol(symbol string) []*OrderState {
	m.mu.RLock()
	defer m.mu.RUnlock()

	var result []*OrderState
	for _, state := range m.orders {
		if state.Symbol == symbol {
			copy := *state
			result = append(result, &copy)
		}
	}
	return result
}

// GetOrdersByStrategy 获取指定策略的所有订单
func (m *Manager) GetOrdersByStrategy(strategyID string) []*OrderState {
	m.mu.RLock()
	defer m.mu.RUnlock()

	var result []*OrderState
	for _, state := range m.orders {
		if state.StrategyID == strategyID {
			copy := *state
			result = append(result, &copy)
		}
	}
	return result
}

// Stats 获取订单统计
func (m *Manager) Stats() (total, active, filled, canceled, rejected int) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	for _, state := range m.orders {
		total++
		switch state.Status {
		case exchange.StatusNew, exchange.StatusPartiallyFilled:
			active++
		case exchange.StatusFilled:
			filled++
		case exchange.StatusCanceled:
			canceled++
		case exchange.StatusRejected:
			rejected++
		}
	}
	return
}

// FormatOrderState 格式化订单状态为可读字符串
func FormatOrderState(state *OrderState) string {
	if state == nil {
		return "<nil>"
	}
	return fmt.Sprintf("ID=%s %s %s %s | 状态=%s | 数量=%.4f 成交=%.4f 均价=%.2f | 策略=%s",
		state.OrderID, state.Side, state.PositionSide, state.Symbol,
		state.Status, state.Quantity, state.ExecutedQty, state.AvgPrice, state.StrategyID)
}
