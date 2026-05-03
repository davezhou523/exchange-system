package exchange

import (
	"context"
	"fmt"
)

// ---------------------------------------------------------------------------
// OKX 交易所客户端（占位实现）
// 实现 Exchange 接口
// 后续接入 OKX API 时补充具体逻辑
// ---------------------------------------------------------------------------

// OKXClient OKX 交易所客户端
type OKXClient struct {
	apiKey     string
	secretKey  string
	passphrase string
	baseURL    string
}

// OKXConfig OKX 客户端配置
type OKXConfig struct {
	APIKey     string
	SecretKey  string
	Passphrase string // OKX 独有的 passphrase
	BaseURL    string // 默认 https://www.okx.com
}

// NewOKXClient 创建 OKX 交易所客户端
func NewOKXClient(cfg OKXConfig) *OKXClient {
	if cfg.BaseURL == "" {
		cfg.BaseURL = "https://www.okx.com"
	}
	return &OKXClient{
		apiKey:     cfg.APIKey,
		secretKey:  cfg.SecretKey,
		passphrase: cfg.Passphrase,
		baseURL:    cfg.BaseURL,
	}
}

// Name 返回交易所名称
func (c *OKXClient) Name() string { return "okx" }

// CreateOrder 创建订单（未实现）
func (c *OKXClient) CreateOrder(ctx context.Context, param CreateOrderParam) (*OrderResult, error) {
	return nil, fmt.Errorf("okx CreateOrder not implemented yet")
}

// CancelOrder 取消订单（未实现）
func (c *OKXClient) CancelOrder(ctx context.Context, param CancelOrderParam) (*OrderResult, error) {
	return nil, fmt.Errorf("okx CancelOrder not implemented yet")
}

// QueryOrder 查询订单状态（未实现）
func (c *OKXClient) QueryOrder(ctx context.Context, param QueryOrderParam) (*OrderResult, error) {
	return nil, fmt.Errorf("okx QueryOrder not implemented yet")
}

// GetAccountInfo 获取账户信息（未实现）
func (c *OKXClient) GetAccountInfo(ctx context.Context) (*AccountResult, error) {
	return nil, fmt.Errorf("okx GetAccountInfo not implemented yet")
}

// SetStopLossTakeProfit 设置止损止盈（未实现）。
func (c *OKXClient) SetStopLossTakeProfit(ctx context.Context, symbol string, positionSide string, quantity float64, stopLossPrice float64, takeProfitPrices []float64) (*ProtectionSetupResult, error) {
	return nil, fmt.Errorf("okx SetStopLossTakeProfit not implemented yet")
}
