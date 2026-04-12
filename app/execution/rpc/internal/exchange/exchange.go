package exchange

import "context"

// ---------------------------------------------------------------------------
// 交易所统一接口
// 支持多交易所路由：Binance / OKX / 模拟撮合
// ---------------------------------------------------------------------------

// OrderSide 订单方向
type OrderSide string

const (
	SideBuy  OrderSide = "BUY"
	SideSell OrderSide = "SELL"
)

// PositionSide 持仓方向
type PositionSide string

const (
	PosLong  PositionSide = "LONG"
	PosShort PositionSide = "SHORT"
	PosBoth  PositionSide = "BOTH"
)

// OrderType 订单类型
type OrderType string

const (
	OrderTypeMarket OrderType = "MARKET"
	OrderTypeLimit  OrderType = "LIMIT"
	OrderTypeStop   OrderType = "STOP"
)

// OrderStatus 订单状态
type OrderStatus string

const (
	StatusNew             OrderStatus = "NEW"
	StatusPartiallyFilled OrderStatus = "PARTIALLY_FILLED"
	StatusFilled          OrderStatus = "FILLED"
	StatusCanceled        OrderStatus = "CANCELED"
	StatusRejected        OrderStatus = "REJECTED"
	StatusExpired         OrderStatus = "EXPIRED"
)

// CreateOrderParam 创建订单参数
type CreateOrderParam struct {
	Symbol        string       // 交易对（如 ETHUSDT）
	Side          OrderSide    // 买卖方向
	PositionSide  PositionSide // 持仓方向
	Type          OrderType    // 订单类型
	Quantity      float64      // 数量
	Price         float64      // 价格（LIMIT 单必填）
	StopPrice     float64      // 止损触发价（STOP 单必填）
	ReduceOnly    bool         // 仅减仓
	ClosePosition bool         // 全部平仓
	ClientID      string       // 客户端自定义订单ID（用于幂等）
}

// OrderResult 订单执行结果
type OrderResult struct {
	OrderID          string      // 交易所订单ID
	ClientOrderID    string      // 客户端自定义订单ID
	Symbol           string      // 交易对
	Status           OrderStatus // 订单状态
	Side             OrderSide   // 买卖方向
	PositionSide     PositionSide
	ExecutedQuantity float64 // 实际成交量
	AvgPrice         float64 // 成交均价
	Commission       float64 // 手续费
	CommissionAsset  string  // 手续费资产
	TransactTime     int64   // 交易时间戳（毫秒）
	ErrorMessage     string  // 错误信息
	Slippage         float64 // 滑点（模拟撮合用）
}

// CancelOrderParam 取消订单参数
type CancelOrderParam struct {
	Symbol        string // 交易对
	OrderID       string // 交易所订单ID
	ClientOrderID string // 客户端自定义订单ID
}

// QueryOrderParam 查询订单参数
type QueryOrderParam struct {
	Symbol        string // 交易对
	OrderID       string // 交易所订单ID
	ClientOrderID string // 客户端自定义订单ID
}

// PositionInfo 仓位信息
type PositionInfo struct {
	Symbol           string  // 交易对
	PositionAmount   float64 // 持仓数量（正=多头, 负=空头）
	EntryPrice       float64 // 开仓均价
	MarkPrice        float64 // 标记价格
	UnrealizedPnl    float64 // 未实现盈亏
	LiquidationPrice float64 // 强平价格
	Leverage         float64 // 杠杆倍数
	MarginType       string  // 逐仓/全仓
}

// AccountResult 账户信息
type AccountResult struct {
	TotalWalletBalance float64        // 账户总余额
	TotalUnrealizedPnl float64        // 总未实现盈亏
	TotalMarginBalance float64        // 总保证金余额
	AvailableBalance   float64        // 可用余额
	MaxWithdrawAmount  float64        // 最大可转出余额
	Positions          []PositionInfo // 持仓列表
}

// Exchange 交易所统一接口
// 所有交易所客户端（Binance/OKX/模拟撮合）均实现此接口
type Exchange interface {
	// CreateOrder 创建订单
	CreateOrder(ctx context.Context, param CreateOrderParam) (*OrderResult, error)

	// CancelOrder 取消订单
	CancelOrder(ctx context.Context, param CancelOrderParam) (*OrderResult, error)

	// QueryOrder 查询订单状态
	QueryOrder(ctx context.Context, param QueryOrderParam) (*OrderResult, error)

	// GetAccountInfo 获取账户信息（含持仓）
	GetAccountInfo(ctx context.Context) (*AccountResult, error)

	// Name 返回交易所名称
	Name() string
}
