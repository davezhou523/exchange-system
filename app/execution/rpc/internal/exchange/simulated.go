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
//
// 两种成交模式：
//   - 即时成交模式（默认）：市价单立即以信号价+滑点成交
//   - 1m K线撮合模式：市价单等待下一根1m K线开盘价成交，
//     限价/止损/止盈用1m K线OHLC判断是否触发
// ---------------------------------------------------------------------------

// SimMatchMode 撮合模式
type SimMatchMode string

const (
	// MatchModeInstant 即时成交模式（默认）：市价单立即成交
	MatchModeInstant SimMatchMode = "instant"
	// MatchModeKline1m 1m K线撮合模式：等待1m K线驱动成交
	MatchModeKline1m SimMatchMode = "1m"
)

// SimulatedExchange 模拟交易所
type SimulatedExchange struct {
	mu sync.RWMutex

	// 订单簿：orderID -> OrderResult
	orders map[string]*OrderResult

	// 仓位簿：symbol -> PositionInfo
	positions map[string]*PositionInfo

	// 待撮合订单（1m K线模式下使用）
	pendingOrders map[string]*pendingOrder // orderID -> pendingOrder

	// 持仓止损止盈记录（1m K线模式下使用）
	positionSLTP map[string]*positionSLTP // symbol -> 止损止盈

	// 账户余额
	balance float64
	equity  float64

	// 序列号生成器
	nextOrderID atomic.Int64

	// 配置
	config SimConfig

	// 成交回调：撮合完成后通知上层
	onFillCallback func(result *OrderResult)
}

// pendingOrder 待撮合订单（1m K线模式下，市价单和限价单等待K线驱动撮合）
type pendingOrder struct {
	orderID      string
	symbol       string
	side         OrderSide
	positionSide PositionSide
	orderType    OrderType
	quantity     float64
	price        float64 // 限价单委托价 / 市价单信号价
	stopPrice    float64 // 止损触发价
	reduceOnly   bool
	closePos     bool
	clientID     string
	createTime   int64 // 创建时间（毫秒）
}

// positionSLTP 持仓止损止盈记录（用于1m K线驱动检查）
type positionSLTP struct {
	symbol      string
	side        PositionSide // 仓位方向
	quantity    float64      // 持仓数量
	entryPrice  float64      // 开仓均价
	stopLoss    float64      // 止损价（0=无）
	takeProfits []float64    // 止盈价列表
	strategyID  string       // 策略ID
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

	// 撮合模式
	MatchMode SimMatchMode // 撮合模式: "instant" | "1m"
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
		MatchMode:       MatchModeInstant,
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
	if config.MatchMode == "" {
		config.MatchMode = MatchModeInstant
	}
	return &SimulatedExchange{
		orders:        make(map[string]*OrderResult),
		positions:     make(map[string]*PositionInfo),
		pendingOrders: make(map[string]*pendingOrder),
		positionSLTP:  make(map[string]*positionSLTP),
		balance:       config.InitialBalance,
		equity:        config.InitialBalance,
		config:        config,
	}
}

// Name 返回交易所名称
func (s *SimulatedExchange) Name() string {
	return "simulated"
}

// SetStopLossTakeProfit 设置止损止盈（模拟交易所）
func (s *SimulatedExchange) SetStopLossTakeProfit(ctx context.Context, symbol string, positionSide string, quantity float64, stopLossPrice float64, takeProfitPrices []float64) error {
	// 模拟交易所的止损止盈设置逻辑
	posSide := PositionSide(positionSide)
	s.SetPositionSLTP(symbol, posSide, quantity, stopLossPrice, takeProfitPrices, "")
	return nil
}

// SetPriceProvider 设置外部价格提供者（用于获取当前市场价格）
// 此方法可选，默认使用 entry_price 作为成交价
func (s *SimulatedExchange) SetPriceProvider(provider simPriceProvider) {
	// 预留给未来扩展，外部可注入实时价格源
}

// SetOnFillCallback 设置成交回调（撮合完成后通知上层更新订单/仓位管理器）
func (s *SimulatedExchange) SetOnFillCallback(callback func(result *OrderResult)) {
	s.onFillCallback = callback
}

// CreateOrder 模拟创建订单
//
// 即时模式：市价单立即成交
// 1m K线模式：市价单进入待撮合队列，等待下一根1m K线撮合
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

	// ========== 1m K线撮合模式 ==========
	if s.config.MatchMode == MatchModeKline1m {
		return s.createOrderKlineMode(orderID, param)
	}

	// ========== 即时成交模式 ==========
	return s.createOrderInstantMode(orderID, param)
}

// createOrderKlineMode 1m K线模式下创建订单（订单挂起，等待K线撮合）
func (s *SimulatedExchange) createOrderKlineMode(orderID string, param CreateOrderParam) (*OrderResult, error) {
	quantity := param.Quantity
	if param.ClosePosition {
		if pos, ok := s.positions[param.Symbol]; ok {
			quantity = math.Abs(pos.PositionAmount)
		} else {
			return nil, fmt.Errorf("no position to close for %s", param.Symbol)
		}
	}

	// 确定委托价格
	price := param.Price
	if param.Type == OrderTypeLimit && param.Price > 0 {
		price = param.Price
	}

	// 创建待撮合订单
	pending := &pendingOrder{
		orderID:      orderID,
		symbol:       param.Symbol,
		side:         param.Side,
		positionSide: param.PositionSide,
		orderType:    param.Type,
		quantity:     quantity,
		price:        price,
		stopPrice:    param.StopPrice,
		reduceOnly:   param.ReduceOnly,
		closePos:     param.ClosePosition,
		clientID:     param.ClientID,
		createTime:   time.Now().UnixMilli(),
	}
	s.pendingOrders[orderID] = pending

	// 返回 NEW 状态（等待撮合）
	result := &OrderResult{
		OrderID:       orderID,
		ClientOrderID: param.ClientID,
		Symbol:        param.Symbol,
		Status:        StatusNew,
		Side:          param.Side,
		PositionSide:  param.PositionSide,
		TransactTime:  pending.createTime,
	}
	s.orders[orderID] = result

	log.Printf("[模拟撮合-1m] 订单挂起 | ID=%s %s %s %s %s | 价格=%.2f 数量=%.4f | 等待1m K线撮合",
		orderID, param.Side, param.PositionSide, param.Type, param.Symbol,
		price, quantity)

	return result, nil
}

// createOrderInstantMode 即时成交模式下创建订单（市价单立即成交）
func (s *SimulatedExchange) createOrderInstantMode(orderID string, param CreateOrderParam) (*OrderResult, error) {
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

	log.Printf("[模拟撮合-即时] 订单成交 | ID=%s %s %s %s | 价格=%.2f(滑点%.4f) | 数量=%.4f | 手续费=%.4f %s",
		orderID, param.Side, param.PositionSide, param.Symbol,
		fillPrice, slippage, quantity, commission, s.config.CommissionAsset)

	return result, nil
}

// OnKline1m 处理1m K线数据，驱动撮合判断
//
// 撮合逻辑：
//  1. 遍历所有待撮合订单：
//     - 市价单：用K线开盘价+滑点成交（模拟"下一根K线开盘时执行"）
//     - 限价买单：K线最低价 ≤ 委托价 → 成交
//     - 限价卖单：K线最高价 ≥ 委托价 → 成交
//     - 止损买单：K线最高价 ≥ 止损价 → 成交（止损价或更高）
//     - 止损卖单：K线最低价 ≤ 止损价 → 成交（止损价或更低）
//  2. 检查持仓止损止盈：
//     - 多头止损：K线最低价 ≤ 止损价 → 触发平仓
//     - 多头止盈：K线最高价 ≥ 止盈价 → 触发平仓
//     - 空头止损：K线最高价 ≥ 止损价 → 触发平仓
//     - 空头止盈：K线最低价 ≤ 止盈价 → 触发平仓
func (s *SimulatedExchange) OnKline1m(symbol string, open, high, low, close float64, openTime int64) {
	s.mu.Lock()
	defer s.mu.Unlock()

	// 1. 撮合待成交订单
	s.matchPendingOrders(symbol, open, high, low, close, openTime)

	// 2. 检查持仓止损止盈
	s.checkPositionSLTP(symbol, high, low, close, openTime)
}

// matchPendingOrders 用1m K线撮合待成交订单
func (s *SimulatedExchange) matchPendingOrders(symbol string, open, high, low, close float64, openTime int64) {
	var filledIDs []string

	for orderID, pending := range s.pendingOrders {
		// 只撮合同symbol的订单
		if pending.symbol != symbol {
			continue
		}

		var fillPrice float64
		var matched bool

		switch pending.orderType {
		case OrderTypeMarket:
			// 市价单：用开盘价+滑点成交（模拟"收到信号后下一根K线开盘执行"）
			fillPrice = s.applySlippage(open, pending.side)
			matched = true

		case OrderTypeLimit:
			// 限价单：K线价格穿过委托价时成交
			if pending.side == SideBuy {
				// 买单：K线最低价 ≤ 委托价 → 委托价或更好成交
				if low <= pending.price {
					fillPrice = math.Min(pending.price, open) // 以开盘价和委托价中更优者成交
					if fillPrice > pending.price {
						fillPrice = pending.price
					}
					fillPrice = s.applySlippage(fillPrice, pending.side)
					matched = true
				}
			} else {
				// 卖单：K线最高价 ≥ 委托价 → 委托价或更好成交
				if high >= pending.price {
					fillPrice = math.Max(pending.price, open)
					if fillPrice < pending.price {
						fillPrice = pending.price
					}
					fillPrice = s.applySlippage(fillPrice, pending.side)
					matched = true
				}
			}

		case OrderTypeStop:
			// 止损单：K线价格穿越止损价时触发
			if pending.side == SideBuy {
				// 止损买单：K线最高价 ≥ 止损价 → 触发
				if high >= pending.stopPrice {
					fillPrice = s.applySlippage(pending.stopPrice, pending.side)
					matched = true
				}
			} else {
				// 止损卖单：K线最低价 ≤ 止损价 → 触发
				if low <= pending.stopPrice {
					fillPrice = s.applySlippage(pending.stopPrice, pending.side)
					matched = true
				}
			}
		}

		if !matched {
			continue
		}

		// 撮合成功
		quantity := pending.quantity
		commission := quantity * fillPrice * s.config.CommissionRate

		// 滑点金额
		slippage := fillPrice - open
		if pending.side == SideSell {
			slippage = open - fillPrice
		}

		now := time.Now().UnixMilli()
		result := &OrderResult{
			OrderID:          orderID,
			ClientOrderID:    pending.clientID,
			Symbol:           symbol,
			Status:           StatusFilled,
			Side:             pending.side,
			PositionSide:     pending.positionSide,
			ExecutedQuantity: quantity,
			AvgPrice:         fillPrice,
			Commission:       commission,
			CommissionAsset:  s.config.CommissionAsset,
			TransactTime:     now,
			Slippage:         slippage,
		}

		// 更新订单簿
		s.orders[orderID] = result

		// 更新仓位
		s.updatePosition(result)

		// 如果是开仓订单，记录持仓止损止盈（如果有的话）
		// 注意：止损止盈由 SetPositionSLTP 设置

		filledIDs = append(filledIDs, orderID)

		log.Printf("[模拟撮合-1m] 订单成交 | ID=%s %s %s %s | 成交价=%.2f 开盘=%.2f 高=%.2f 低=%.2f | 数量=%.4f | 手续费=%.4f",
			orderID, pending.side, pending.positionSide, symbol,
			fillPrice, open, high, low, quantity, commission)

		// 回调通知上层
		if s.onFillCallback != nil {
			s.onFillCallback(result)
		}
	}

	// 从待撮合队列中移除已成交订单
	for _, id := range filledIDs {
		delete(s.pendingOrders, id)
	}
}

// checkPositionSLTP 检查持仓止损止盈（1m K线驱动）
func (s *SimulatedExchange) checkPositionSLTP(symbol string, high, low, close float64, openTime int64) {
	sltp, ok := s.positionSLTP[symbol]
	if !ok {
		return
	}

	var triggerPrice float64
	var triggerReason string
	var closeSide OrderSide

	if sltp.side == PosLong || sltp.quantity > 0 {
		// 多头持仓
		// 止损：K线最低价 ≤ 止损价
		if sltp.stopLoss > 0 && low <= sltp.stopLoss {
			triggerPrice = sltp.stopLoss
			triggerReason = "多头止损触发"
			closeSide = SideSell
		}
		// 止盈：K线最高价 ≥ 止盈价（优先检查止损，止损更紧急）
		if triggerPrice == 0 && len(sltp.takeProfits) > 0 {
			for _, tp := range sltp.takeProfits {
				if tp > 0 && high >= tp {
					triggerPrice = tp
					triggerReason = fmt.Sprintf("多头止盈触发(%.2f)", tp)
					closeSide = SideSell
					break
				}
			}
		}
	} else if sltp.side == PosShort || sltp.quantity < 0 {
		// 空头持仓
		// 止损：K线最高价 ≥ 止损价
		if sltp.stopLoss > 0 && high >= sltp.stopLoss {
			triggerPrice = sltp.stopLoss
			triggerReason = "空头止损触发"
			closeSide = SideBuy
		}
		// 止盈：K线最低价 ≤ 止盈价
		if triggerPrice == 0 && len(sltp.takeProfits) > 0 {
			for _, tp := range sltp.takeProfits {
				if tp > 0 && low <= tp {
					triggerPrice = tp
					triggerReason = fmt.Sprintf("空头止盈触发(%.2f)", tp)
					closeSide = SideBuy
					break
				}
			}
		}
	}

	if triggerPrice == 0 {
		return
	}

	// 生成止损止盈平仓订单
	orderID := fmt.Sprintf("sim-sltp-%d", s.nextOrderID.Add(1))
	quantity := math.Abs(sltp.quantity)
	fillPrice := s.applySlippage(triggerPrice, closeSide)
	commission := quantity * fillPrice * s.config.CommissionRate
	slippage := fillPrice - triggerPrice
	if closeSide == SideSell {
		slippage = triggerPrice - fillPrice
	}

	now := time.Now().UnixMilli()
	result := &OrderResult{
		OrderID:          orderID,
		ClientOrderID:    fmt.Sprintf("sltp-%s-%d", symbol, openTime),
		Symbol:           symbol,
		Status:           StatusFilled,
		Side:             closeSide,
		PositionSide:     sltp.side,
		ExecutedQuantity: quantity,
		AvgPrice:         fillPrice,
		Commission:       commission,
		CommissionAsset:  s.config.CommissionAsset,
		TransactTime:     now,
		Slippage:         slippage,
	}

	s.orders[orderID] = result
	s.updatePosition(result)
	delete(s.positionSLTP, symbol)

	log.Printf("[模拟撮合-1m] %s | %s | 成交价=%.2f 触发价=%.2f 高=%.2f 低=%.2f | 数量=%.4f | 策略=%s",
		triggerReason, symbol, fillPrice, triggerPrice, high, low, quantity, sltp.strategyID)

	// 回调通知上层
	if s.onFillCallback != nil {
		s.onFillCallback(result)
	}
}

// SetPositionSLTP 设置持仓止损止盈（1m K线模式下由上层在开仓成交后调用）
func (s *SimulatedExchange) SetPositionSLTP(symbol string, side PositionSide, quantity float64, stopLoss float64, takeProfits []float64, strategyID string) {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.positionSLTP[symbol] = &positionSLTP{
		symbol:      symbol,
		side:        side,
		quantity:    quantity,
		stopLoss:    stopLoss,
		takeProfits: takeProfits,
		strategyID:  strategyID,
	}

	tpStr := "[]"
	if len(takeProfits) > 0 {
		tpStr = fmt.Sprintf("%v", takeProfits)
	}
	log.Printf("[模拟撮合-1m] 设置止损止盈 | %s %s | 止损=%.2f 止盈=%s | 策略=%s",
		symbol, side, stopLoss, tpStr, strategyID)
}

// GetPendingOrderCount 获取待撮合订单数量
func (s *SimulatedExchange) GetPendingOrderCount() int {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return len(s.pendingOrders)
}

// GetMatchMode 获取当前撮合模式
func (s *SimulatedExchange) GetMatchMode() SimMatchMode {
	return s.config.MatchMode
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

	// 已成交的订单不能取消
	if order.Status == StatusFilled {
		return nil, fmt.Errorf("order %s already filled, cannot cancel", order.OrderID)
	}

	order.Status = StatusCanceled
	order.TransactTime = time.Now().UnixMilli()

	// 从待撮合队列中移除
	delete(s.pendingOrders, order.OrderID)

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
