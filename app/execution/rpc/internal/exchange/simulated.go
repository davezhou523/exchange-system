package exchange

import (
	"context"
	"fmt"
	"log"
	"math"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"
)

// ---------------------------------------------------------------------------
// 模拟撮合引擎
// 用于回测和仿真环境，不发送真实订单到交易所
// 支持：滑点模拟、手续费模拟、订单簿管理
// ---------------------------------------------------------------------------

// SimulatedExchange 模拟交易所
type SimulatedExchange struct {
	mu sync.RWMutex

	// 订单簿：orderID -> OrderResult
	orders map[string]*OrderResult

	// 仓位簿：symbol -> PositionInfo
	positions map[string]*PositionInfo

	// 账户余额
	balance float64
	equity  float64

	// 序列号生成器
	nextOrderID atomic.Int64

	// 配置
	config SimConfig
}

// SimConfig 模拟撮合配置
type SimConfig struct {
	InitialBalance float64 // 初始余额（默认 10000 USDT）

	// 滑点模拟
	SlippageBPS   float64 // 基点滑点（1 bps = 0.01%），默认 5（即 0.05%）
	SlippageModel string  // 滑点模型: "fixed" | "random" | "volume"

	// 手续费模拟
	CommissionRate  float64 // 手续费率，默认 0.0004（0.04%，币安默认 maker）
	CommissionAsset string  // 手续费资产，默认 "USDT"

	// 成交延迟模拟
	FillDelayMs int64 // 模拟成交延迟（毫秒），0=立即成交
}

// DefaultSimConfig 默认模拟配置
func DefaultSimConfig() SimConfig {
	return SimConfig{
		InitialBalance:  10000,
		SlippageBPS:     5,
		SlippageModel:   "fixed",
		CommissionRate:  0.0004,
		CommissionAsset: "USDT",
		FillDelayMs:     0,
	}
}

// simPriceProvider 模拟价格提供者接口
// 外部可注入实时价格源（如从 market 服务获取最新价格）
type simPriceProvider interface {
	getPrice(symbol string) float64
}

// NewSimulatedExchange 创建模拟交易所
func NewSimulatedExchange(config SimConfig) *SimulatedExchange {
	if config.InitialBalance <= 0 {
		config.InitialBalance = 10000
	}
	if config.CommissionRate <= 0 {
		config.CommissionRate = 0.0004
	}
	if config.SlippageBPS <= 0 {
		config.SlippageBPS = 5
	}
	if config.CommissionAsset == "" {
		config.CommissionAsset = "USDT"
	}
	return &SimulatedExchange{
		orders:    make(map[string]*OrderResult),
		positions: make(map[string]*PositionInfo),
		balance:   config.InitialBalance,
		equity:    config.InitialBalance,
		config:    config,
	}
}

// Name 返回交易所名称
func (s *SimulatedExchange) Name() string { return "simulated" }

// SetPriceProvider 设置外部价格提供者（用于获取当前市场价格）
// 此方法可选，默认使用 entry_price 作为成交价
func (s *SimulatedExchange) SetPriceProvider(provider simPriceProvider) {
	// 预留给未来扩展，外部可注入实时价格源
}

// CreateOrder 模拟创建订单
func (s *SimulatedExchange) CreateOrder(ctx context.Context, param CreateOrderParam) (*OrderResult, error) {
	if param.Symbol == "" {
		return nil, fmt.Errorf("symbol is required")
	}
	if param.Quantity <= 0 && !param.ClosePosition {
		return nil, fmt.Errorf("quantity must be positive")
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	orderID := fmt.Sprintf("sim-%d", s.nextOrderID.Add(1))

	// 确定成交价格
	var basePrice float64
	if param.Type == OrderTypeLimit && param.Price > 0 {
		basePrice = param.Price
	} else if param.Price > 0 {
		basePrice = param.Price
	} else {
		// 尝试从已有仓位的标记价格获取
		if pos, ok := s.positions[param.Symbol]; ok && pos.MarkPrice > 0 {
			basePrice = pos.MarkPrice
		} else {
			basePrice = 0 // 无价格时使用 quantity * entry_price 推算
		}
	}

	if basePrice <= 0 {
		return nil, fmt.Errorf("cannot determine execution price for %s", param.Symbol)
	}

	// 计算滑点
	fillPrice := s.applySlippage(basePrice, param.Side)

	// 确定成交量
	quantity := param.Quantity
	if param.ClosePosition {
		if pos, ok := s.positions[param.Symbol]; ok {
			quantity = math.Abs(pos.PositionAmount)
		} else {
			return nil, fmt.Errorf("no position to close for %s", param.Symbol)
		}
	}

	// 计算手续费
	commission := quantity * fillPrice * s.config.CommissionRate

	// 计算滑点金额
	slippage := fillPrice - basePrice
	if param.Side == SideSell {
		slippage = basePrice - fillPrice // 卖出时滑点为负
	}

	// 模拟成交延迟
	if s.config.FillDelayMs > 0 {
		time.Sleep(time.Duration(s.config.FillDelayMs) * time.Millisecond)
	}

	now := time.Now().UnixMilli()

	result := &OrderResult{
		OrderID:          orderID,
		ClientOrderID:    param.ClientID,
		Symbol:           param.Symbol,
		Status:           StatusFilled,
		Side:             param.Side,
		PositionSide:     param.PositionSide,
		ExecutedQuantity: quantity,
		AvgPrice:         fillPrice,
		Commission:       commission,
		CommissionAsset:  s.config.CommissionAsset,
		TransactTime:     now,
		Slippage:         slippage,
	}

	// 记录订单到订单簿
	s.orders[orderID] = result

	// 更新仓位
	s.updatePosition(result)

	log.Printf("[模拟撮合] 订单成交 | ID=%s %s %s %s | 价格=%.2f(滑点%.4f) | 数量=%.4f | 手续费=%.4f %s",
		orderID, param.Side, param.PositionSide, param.Symbol,
		fillPrice, slippage, quantity, commission, s.config.CommissionAsset)

	return result, nil
}

// CancelOrder 模拟取消订单
func (s *SimulatedExchange) CancelOrder(ctx context.Context, param CancelOrderParam) (*OrderResult, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	// 查找订单
	var order *OrderResult
	if param.OrderID != "" {
		if o, ok := s.orders[param.OrderID]; ok {
			order = o
		}
	}
	if order == nil && param.ClientOrderID != "" {
		for _, o := range s.orders {
			if o.ClientOrderID == param.ClientOrderID {
				order = o
				break
			}
		}
	}

	if order == nil {
		return nil, fmt.Errorf("order not found: order_id=%s client_id=%s", param.OrderID, param.ClientOrderID)
	}

	// 模拟撮合中市价单已立即成交，LIMIT 单才能取消
	if order.Status == StatusFilled {
		return nil, fmt.Errorf("order %s already filled, cannot cancel", order.OrderID)
	}

	order.Status = StatusCanceled
	order.TransactTime = time.Now().UnixMilli()

	return &OrderResult{
		OrderID:       order.OrderID,
		ClientOrderID: order.ClientOrderID,
		Symbol:        order.Symbol,
		Status:        StatusCanceled,
		Side:          order.Side,
		TransactTime:  order.TransactTime,
	}, nil
}

// QueryOrder 查询订单状态
func (s *SimulatedExchange) QueryOrder(ctx context.Context, param QueryOrderParam) (*OrderResult, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	if param.OrderID != "" {
		if o, ok := s.orders[param.OrderID]; ok {
			return o, nil
		}
	}
	if param.ClientOrderID != "" {
		for _, o := range s.orders {
			if o.ClientOrderID == param.ClientOrderID {
				return o, nil
			}
		}
	}
	return nil, fmt.Errorf("order not found: order_id=%s client_id=%s", param.OrderID, param.ClientOrderID)
}

// GetAccountInfo 获取模拟账户信息
func (s *SimulatedExchange) GetAccountInfo(ctx context.Context) (*AccountResult, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	// 计算总未实现盈亏
	totalUnrealizedPnl := 0.0
	positions := make([]PositionInfo, 0)
	for _, pos := range s.positions {
		if pos.PositionAmount != 0 {
			totalUnrealizedPnl += pos.UnrealizedPnl
			positions = append(positions, *pos)
		}
	}

	return &AccountResult{
		TotalWalletBalance: s.balance,
		TotalUnrealizedPnl: totalUnrealizedPnl,
		TotalMarginBalance: s.balance + totalUnrealizedPnl,
		AvailableBalance:   s.balance,
		MaxWithdrawAmount:  s.balance,
		Positions:          positions,
	}, nil
}

// applySlippage 应用滑点到价格
func (s *SimulatedExchange) applySlippage(price float64, side OrderSide) float64 {
	if s.config.SlippageBPS <= 0 {
		return price
	}

	// 基点滑点转换为价格偏移
	slipFactor := s.config.SlippageBPS / 10000.0 // bps -> 小数

	switch s.config.SlippageModel {
	case "random":
		// 随机滑点：0 ~ SlippageBPS
		slipFactor *= rand.Float64()
	case "volume":
		// 成交量滑点模型：简化为固定滑点的 1.5 倍
		slipFactor *= 1.5
	default: // "fixed"
		// 固定滑点
	}

	// 买入时滑点不利方向（价格更高），卖出时不利方向（价格更低）
	if side == SideBuy {
		return price * (1 + slipFactor)
	}
	return price * (1 - slipFactor)
}

// updatePosition 根据成交结果更新仓位（需持有锁）
func (s *SimulatedExchange) updatePosition(result *OrderResult) {
	symbol := result.Symbol
	pos, ok := s.positions[symbol]
	if !ok {
		pos = &PositionInfo{Symbol: symbol}
		s.positions[symbol] = pos
	}

	// 计算仓位变化
	qty := result.ExecutedQuantity
	if result.Side == SideSell {
		qty = -qty
	}

	// 更新持仓数量
	oldAmt := pos.PositionAmount
	newAmt := oldAmt + qty

	// 更新开仓均价
	if oldAmt == 0 {
		pos.EntryPrice = result.AvgPrice
	} else if (oldAmt > 0 && qty > 0) || (oldAmt < 0 && qty < 0) {
		// 加仓：重新计算均价
		totalCost := math.Abs(oldAmt)*pos.EntryPrice + math.Abs(qty)*result.AvgPrice
		totalQty := math.Abs(newAmt)
		if totalQty > 0 {
			pos.EntryPrice = totalCost / totalQty
		}
	}
	// 减仓时不改变均价

	pos.PositionAmount = newAmt
	pos.MarkPrice = result.AvgPrice

	// 更新余额：减去手续费
	s.balance -= result.Commission

	// 清除零仓位
	if math.Abs(newAmt) < 1e-10 {
		delete(s.positions, symbol)
	}
}
