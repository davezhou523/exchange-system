package exchange

import (
	"context"
	"fmt"
	"log"
)

// ---------------------------------------------------------------------------
// 下单路由器
// 根据配置/规则将订单路由到不同的交易所
// 支持路由策略：指定交易所 / 按交易对分流
// ---------------------------------------------------------------------------

// RouteStrategy 路由策略
type RouteStrategy string

const (
	// RouteDirect 直接路由到指定交易所
	RouteDirect RouteStrategy = "direct"
	// RouteBySymbol 按交易对路由（不同交易对走不同交易所）
	RouteBySymbol RouteStrategy = "by_symbol"
)

// RouterConfig 路由器配置
type RouterConfig struct {
	Strategy        RouteStrategy     // 路由策略
	DefaultExchange string            // 默认交易所名称（direct 策略下使用）
	SymbolRoutes    map[string]string // 交易对路由映射（by_symbol 策略下使用）
}

// Router 下单路由器
type Router struct {
	exchanges map[string]Exchange // 已注册的交易所实例
	config    RouterConfig
}

// NewRouter 创建下单路由器
func NewRouter(config RouterConfig) *Router {
	return &Router{
		exchanges: make(map[string]Exchange),
		config:    config,
	}
}

// Register 注册交易所实例
func (r *Router) Register(name string, ex Exchange) {
	r.exchanges[name] = ex
	log.Printf("[路由] 注册交易所: %s (%s)", name, ex.Name())
}

// Route 根据路由策略选择交易所
func (r *Router) Route(symbol string) (Exchange, error) {
	switch r.config.Strategy {
	case RouteBySymbol:
		// 按交易对路由
		if target, ok := r.config.SymbolRoutes[symbol]; ok {
			if ex, ok := r.exchanges[target]; ok {
				return ex, nil
			}
			return nil, fmt.Errorf("exchange %s not registered for symbol %s", target, symbol)
		}
		// 未配置的交易对走默认
		if r.config.DefaultExchange != "" {
			if ex, ok := r.exchanges[r.config.DefaultExchange]; ok {
				return ex, nil
			}
		}

	case RouteDirect:
		fallthrough
	default:
		// 直接路由到指定交易所
		if r.config.DefaultExchange != "" {
			if ex, ok := r.exchanges[r.config.DefaultExchange]; ok {
				return ex, nil
			}
			return nil, fmt.Errorf("default exchange %s not registered", r.config.DefaultExchange)
		}
	}

	// 兜底：使用第一个注册的交易所
	for _, ex := range r.exchanges {
		return ex, nil
	}
	return nil, fmt.Errorf("no exchange available for routing")
}

// CreateOrder 路由下单
func (r *Router) CreateOrder(ctx context.Context, param CreateOrderParam) (*OrderResult, error) {
	ex, err := r.Route(param.Symbol)
	if err != nil {
		return nil, fmt.Errorf("route order failed: %v", err)
	}
	log.Printf("[路由] %s → %s | %s %s %s | 数量=%.4f",
		param.Symbol, ex.Name(), param.Side, param.PositionSide, param.Type, param.Quantity)
	return ex.CreateOrder(ctx, param)
}

// CancelOrder 路由取消订单
func (r *Router) CancelOrder(ctx context.Context, param CancelOrderParam) (*OrderResult, error) {
	// 取消订单需要路由到下单时的交易所，这里简单处理：
	// 1. 如果知道是哪个交易所的订单，直接路由
	// 2. 否则广播到所有交易所（简化实现）
	for name, ex := range r.exchanges {
		result, err := ex.CancelOrder(ctx, param)
		if err == nil {
			log.Printf("[路由] 取消订单 %s → %s", param.OrderID, name)
			return result, nil
		}
		// 某些交易所可能找不到订单，跳过
	}
	return nil, fmt.Errorf("order %s not found in any exchange", param.OrderID)
}

// QueryOrder 路由查询订单
func (r *Router) QueryOrder(ctx context.Context, param QueryOrderParam) (*OrderResult, error) {
	for _, ex := range r.exchanges {
		result, err := ex.QueryOrder(ctx, param)
		if err == nil {
			return result, nil
		}
	}
	return nil, fmt.Errorf("order not found in any exchange")
}

// GetAccountInfo 获取默认交易所的账户信息
func (r *Router) GetAccountInfo(ctx context.Context) (*AccountResult, error) {
	ex, err := r.Route("")
	if err != nil {
		return nil, err
	}
	return ex.GetAccountInfo(ctx)
}

// SetStopLossTakeProfit 路由设置止损止盈，并返回结构化保护单结果。
func (r *Router) SetStopLossTakeProfit(ctx context.Context, symbol string, positionSide string, quantity float64, stopLossPrice float64, takeProfitPrices []float64) (*ProtectionSetupResult, error) {
	ex, err := r.Route(symbol)
	if err != nil {
		return nil, fmt.Errorf("route set stop loss take profit failed: %v", err)
	}
	log.Printf("[路由] %s → %s | 设置止损止盈 | 止损=%.2f 止盈=%v",
		symbol, ex.Name(), stopLossPrice, takeProfitPrices)
	return ex.SetStopLossTakeProfit(ctx, symbol, positionSide, quantity, stopLossPrice, takeProfitPrices)
}
