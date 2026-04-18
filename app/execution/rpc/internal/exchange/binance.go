package exchange

import (
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"math"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"time"
)

// ---------------------------------------------------------------------------
// Binance 期货交易所客户端
// 实现 Exchange 接口
// API 文档: https://binance-docs.github.io/apidocs/futures/cn/
// ---------------------------------------------------------------------------

// BinanceClient 币安期货 API 客户端
type BinanceClient struct {
	baseURL   string
	apiKey    string
	secretKey string
	client    *http.Client
	leverage  int

	precisionMu          sync.RWMutex
	quantityPrecisionMap map[string]int
	leverageMu           sync.RWMutex
	leverageSet          map[string]int
}

// NewBinanceClient 创建币安期货客户端
// baseURL 为空时默认使用正式环境 https://fapi.binance.com
func NewBinanceClient(baseURL, apiKey, secretKey, proxyURL string, leverage float64) *BinanceClient {
	if baseURL == "" {
		baseURL = "https://fapi.binance.com"
	}
	transport := &http.Transport{}
	if proxyURL != "" {
		if proxy, err := url.Parse(proxyURL); err == nil {
			transport.Proxy = http.ProxyURL(proxy)
		}
	}
	return &BinanceClient{
		baseURL:   strings.TrimRight(baseURL, "/"),
		apiKey:    apiKey,
		secretKey: secretKey,
		client: &http.Client{
			Transport: transport,
			Timeout:   30 * time.Second,
		},
		leverage:             int(math.Round(leverage)),
		quantityPrecisionMap: make(map[string]int),
		leverageSet:          make(map[string]int),
	}
}

// Name 返回交易所名称
func (c *BinanceClient) Name() string { return "binance" }

// ---------------------------------------------------------------------------
// HTTP 请求基础设施
// ---------------------------------------------------------------------------

// signedQuery 生成带 HMAC-SHA256 签名的查询字符串
func (c *BinanceClient) signedQuery(query url.Values) (string, error) {
	query.Set("timestamp", strconv.FormatInt(time.Now().UnixMilli(), 10))
	encoded := query.Encode()
	mac := hmac.New(sha256.New, []byte(c.secretKey))
	_, _ = mac.Write([]byte(encoded))
	signature := hex.EncodeToString(mac.Sum(nil))
	return encoded + "&signature=" + url.QueryEscape(signature), nil
}

// doSignedRequest 执行带签名的 HTTP 请求
func (c *BinanceClient) doSignedRequest(ctx context.Context, method, path string, query url.Values, out any) error {
	return c.doSignedRequestBase(ctx, c.baseURL, method, path, query, out)
}

func (c *BinanceClient) doSignedRequestBase(ctx context.Context, baseURL, method, path string, query url.Values, out any) error {
	sq, err := c.signedQuery(query)
	if err != nil {
		return err
	}
	endpoint := strings.TrimRight(baseURL, "/") + path + "?" + sq

	req, err := http.NewRequestWithContext(ctx, method, endpoint, nil)
	if err != nil {
		return err
	}
	if c.apiKey != "" {
		req.Header.Set("X-MBX-APIKEY", c.apiKey)
	}

	resp, err := c.client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
		return fmt.Errorf("binance request failed: status=%d body=%s", resp.StatusCode, string(body))
	}
	return json.NewDecoder(resp.Body).Decode(out)
}

// ---------------------------------------------------------------------------
// 订单相关 API
// ---------------------------------------------------------------------------

// binanceOrderResponse 币安期货下单 API 响应结构
type binanceOrderResponse struct {
	OrderID       int64  `json:"orderId"`
	Symbol        string `json:"symbol"`
	Status        string `json:"status"` // NEW, PARTIALLY_FILLED, FILLED, CANCELED, REJECTED, EXPIRED
	Side          string `json:"side"`
	PositionSide  string `json:"positionSide"`
	Type          string `json:"type"`
	OrigQty       string `json:"origQty"`
	ExecutedQty   string `json:"executedQty"`
	AvgPrice      string `json:"avgPrice"`
	Price         string `json:"price"`
	StopPrice     string `json:"stopPrice"`
	ClientOrderID string `json:"clientOrderId"`
	Time          int64  `json:"updateTime"`
}

// binanceCancelResponse 币安取消订单 API 响应结构
type binanceCancelResponse struct {
	OrderID       int64  `json:"orderId"`
	Symbol        string `json:"symbol"`
	Status        string `json:"status"`
	Side          string `json:"side"`
	OrigQty       string `json:"origQty"`
	ExecutedQty   string `json:"executedQty"`
	AvgPrice      string `json:"avgPrice"`
	ClientOrderID string `json:"clientOrderId"`
	Time          int64  `json:"updateTime"`
}

// CreateOrder 创建订单
func (c *BinanceClient) CreateOrder(ctx context.Context, param CreateOrderParam) (*OrderResult, error) {
	if !param.ReduceOnly && !param.ClosePosition {
		if err := c.ensureSymbolLeverage(ctx, param.Symbol); err != nil {
			return nil, err
		}
	}

	normalizedQty, qtyPrecision, err := c.normalizeOrderQuantity(ctx, param)
	if err != nil {
		return nil, err
	}

	q := url.Values{}
	q.Set("symbol", strings.ToUpper(param.Symbol))
	q.Set("side", string(param.Side))
	q.Set("positionSide", string(param.PositionSide))

	// 订单类型，默认市价
	orderType := string(param.Type)
	if orderType == "" {
		orderType = string(OrderTypeMarket)
	}
	q.Set("type", orderType)
	if orderType == string(OrderTypeMarket) {
		// 让市价单直接返回成交结果，避免被误判为 NEW 而跳过订单日志和止损止盈设置。
		q.Set("newOrderRespType", "RESULT")
	}

	q.Set("quantity", fmt.Sprintf("%.*f", qtyPrecision, normalizedQty))

	// LIMIT 单必须设置价格和 TimeInForce
	if orderType == string(OrderTypeLimit) {
		if param.Price <= 0 {
			return nil, fmt.Errorf("limit order requires price > 0")
		}
		q.Set("price", fmt.Sprintf("%.2f", param.Price))
		q.Set("timeInForce", "GTC")
	}

	// STOP 单需要止损触发价
	if orderType == string(OrderTypeStop) {
		if param.StopPrice <= 0 {
			return nil, fmt.Errorf("stop order requires stop_price > 0")
		}
		q.Set("stopPrice", fmt.Sprintf("%.2f", param.StopPrice))
		q.Set("timeInForce", "GTC")
		if param.Price > 0 {
			q.Set("price", fmt.Sprintf("%.2f", param.Price))
		}
	}

	// 仅减仓
	if param.ReduceOnly && (param.PositionSide == "" || param.PositionSide == PosBoth) {
		q.Set("reduceOnly", "true")
	}

	// 全部平仓（与 quantity 互斥）
	if param.ClosePosition {
		q.Set("closePosition", "true")
		q.Del("quantity")
	}

	// 客户端自定义订单ID（用于幂等防重）
	if param.ClientID != "" {
		q.Set("newClientOrderId", param.ClientID)
	}

	q.Set("recvWindow", "5000")

	var resp binanceOrderResponse
	if err := c.doSignedRequest(ctx, http.MethodPost, "/fapi/v1/order", q, &resp); err != nil {
		return nil, err
	}

	return c.convertOrderResponse(&resp), nil
}

type binanceLeverageResponse struct {
	Leverage         int    `json:"leverage"`
	MaxNotionalValue string `json:"maxNotionalValue"`
	Symbol           string `json:"symbol"`
}

func (c *BinanceClient) ensureSymbolLeverage(ctx context.Context, symbol string) error {
	if c.leverage <= 0 {
		return nil
	}

	symbol = strings.ToUpper(strings.TrimSpace(symbol))
	if symbol == "" {
		return fmt.Errorf("symbol is required")
	}

	c.leverageMu.RLock()
	current, ok := c.leverageSet[symbol]
	c.leverageMu.RUnlock()
	if ok && current == c.leverage {
		return nil
	}

	q := url.Values{}
	q.Set("symbol", symbol)
	q.Set("leverage", strconv.Itoa(c.leverage))
	q.Set("recvWindow", "5000")

	var resp binanceLeverageResponse
	if err := c.doSignedRequest(ctx, http.MethodPost, "/fapi/v1/leverage", q, &resp); err != nil {
		return fmt.Errorf("set leverage failed for %s: %w", symbol, err)
	}

	c.leverageMu.Lock()
	c.leverageSet[symbol] = resp.Leverage
	c.leverageMu.Unlock()
	return nil
}

// SetStopLossTakeProfit 设置止损止盈
func (c *BinanceClient) SetStopLossTakeProfit(ctx context.Context, symbol string, positionSide string, quantity float64, stopLossPrice float64, takeProfitPrices []float64) error {
	// 设置止损单
	if stopLossPrice > 0 {
		side := "SELL"
		if positionSide == string(PosShort) {
			side = "BUY"
		}

		if err := c.createAlgoConditionalOrder(ctx, strings.ToUpper(symbol), side, positionSide, "STOP_MARKET", stopLossPrice, true); err != nil {
			return fmt.Errorf("set stop loss failed: %v", err)
		}
	}

	// Binance 持仓级 TP/SL 更适合单组全平条件单。
	// 多段止盈需要拆分仓位分别挂单，当前先使用第一档止盈，确保官网可见且语义明确。
	takeProfitPrice := 0.0
	for _, price := range takeProfitPrices {
		if price > 0 {
			takeProfitPrice = price
			break
		}
	}
	if takeProfitPrice > 0 {
		side := "SELL"
		if positionSide == string(PosShort) {
			side = "BUY"
		}

		if err := c.createAlgoConditionalOrder(ctx, strings.ToUpper(symbol), side, positionSide, "TAKE_PROFIT_MARKET", takeProfitPrice, true); err != nil {
			return fmt.Errorf("set take profit failed: %v", err)
		}
	}

	return nil
}

type binanceAlgoOrderResponse struct {
	Symbol       string `json:"symbol"`
	AlgoId       int64  `json:"algoId"`
	ClientAlgoId string `json:"clientAlgoId"`
}

// createAlgoConditionalOrder creates a conditional order via the Futures Algo Order API.
// Binance migrated conditional order types (STOP_MARKET/TAKE_PROFIT_MARKET, etc.) away from /fapi/v1/order,
// which now returns -4120 ("use Algo Order API endpoints instead").
func (c *BinanceClient) createAlgoConditionalOrder(ctx context.Context, symbol, side, positionSide, orderType string, triggerPrice float64, closePosition bool) error {
	if symbol == "" {
		return fmt.Errorf("symbol is required")
	}
	if side == "" {
		return fmt.Errorf("side is required")
	}
	if orderType == "" {
		return fmt.Errorf("type is required")
	}
	if triggerPrice <= 0 {
		return fmt.Errorf("trigger price must be positive")
	}

	q := url.Values{}
	q.Set("symbol", symbol)
	q.Set("side", side)
	if positionSide != "" {
		q.Set("positionSide", positionSide)
	}
	// Binance futures algo orders require the conditional order type and trigger price.
	q.Set("algoType", "CONDITIONAL")
	q.Set("type", orderType)
	q.Set("triggerPrice", fmt.Sprintf("%.2f", triggerPrice))
	q.Set("workingType", "MARK_PRICE")
	q.Set("priceProtect", "TRUE")
	if closePosition {
		q.Set("closePosition", "true")
	}
	q.Set("recvWindow", "5000")

	// Try both known paths to tolerate minor upstream naming differences.
	var resp binanceAlgoOrderResponse
	if err := c.doSignedRequest(ctx, http.MethodPost, "/fapi/v1/algo/order", q, &resp); err == nil {
		return nil
	} else {
		var resp2 binanceAlgoOrderResponse
		err2 := c.doSignedRequest(ctx, http.MethodPost, "/fapi/v1/algoOrder", q, &resp2)
		if err2 == nil {
			return nil
		}
		return fmt.Errorf("algo endpoint failed: path1=%v; path2=%v", err, err2)
	}
}

const fallbackQuantityPrecision = 3

type binanceExchangeInfoResponse struct {
	Symbols []struct {
		Symbol            string `json:"symbol"`
		QuantityPrecision int    `json:"quantityPrecision"`
	} `json:"symbols"`
}

func (c *BinanceClient) normalizeOrderQuantity(ctx context.Context, param CreateOrderParam) (float64, int, error) {
	if param.Quantity <= 0 {
		return 0, 2, fmt.Errorf("quantity must be positive")
	}

	precision := 2
	if param.ReduceOnly || param.ClosePosition {
		exchangePrecision, err := c.getQuantityPrecision(ctx, param.Symbol)
		if err == nil && exchangePrecision >= 0 {
			precision = exchangePrecision
		}
	}
	if precision > 8 {
		precision = 8
	}

	scale := math.Pow10(precision)
	normalized := math.Floor(param.Quantity*scale+1e-9) / scale
	if normalized <= 0 {
		return 0, precision, fmt.Errorf("quantity %.8f is below minimum precision for %s", param.Quantity, param.Symbol)
	}
	return normalized, precision, nil
}

func (c *BinanceClient) getQuantityPrecision(ctx context.Context, symbol string) (int, error) {
	symbol = strings.ToUpper(strings.TrimSpace(symbol))
	if symbol == "" {
		return fallbackQuantityPrecision, fmt.Errorf("symbol is required")
	}

	c.precisionMu.RLock()
	if precision, ok := c.quantityPrecisionMap[symbol]; ok {
		c.precisionMu.RUnlock()
		return precision, nil
	}
	c.precisionMu.RUnlock()

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, c.baseURL+"/fapi/v1/exchangeInfo", nil)
	if err != nil {
		return fallbackQuantityPrecision, err
	}

	resp, err := c.client.Do(req)
	if err != nil {
		return fallbackQuantityPrecision, err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
		return fallbackQuantityPrecision, fmt.Errorf("binance exchangeInfo failed: status=%d body=%s", resp.StatusCode, string(body))
	}

	var info binanceExchangeInfoResponse
	if err := json.NewDecoder(resp.Body).Decode(&info); err != nil {
		return fallbackQuantityPrecision, err
	}
	for _, item := range info.Symbols {
		if strings.EqualFold(item.Symbol, symbol) {
			c.precisionMu.Lock()
			c.quantityPrecisionMap[symbol] = item.QuantityPrecision
			c.precisionMu.Unlock()
			return item.QuantityPrecision, nil
		}
	}
	return fallbackQuantityPrecision, fmt.Errorf("symbol %s not found in exchangeInfo", symbol)
}

// CancelOrder 取消订单
func (c *BinanceClient) CancelOrder(ctx context.Context, param CancelOrderParam) (*OrderResult, error) {
	if param.Symbol == "" {
		return nil, fmt.Errorf("symbol is required")
	}
	if param.OrderID == "" && param.ClientOrderID == "" {
		return nil, fmt.Errorf("order_id or client_order_id is required")
	}

	q := url.Values{}
	q.Set("symbol", strings.ToUpper(param.Symbol))
	if param.OrderID != "" {
		q.Set("orderId", param.OrderID)
	}
	if param.ClientOrderID != "" {
		q.Set("origClientOrderId", param.ClientOrderID)
	}
	q.Set("recvWindow", "5000")

	var resp binanceCancelResponse
	if err := c.doSignedRequest(ctx, http.MethodDelete, "/fapi/v1/order", q, &resp); err != nil {
		return nil, err
	}

	return &OrderResult{
		OrderID:          strconv.FormatInt(resp.OrderID, 10),
		ClientOrderID:    resp.ClientOrderID,
		Symbol:           resp.Symbol,
		Status:           OrderStatus(resp.Status),
		Side:             OrderSide(resp.Side),
		ExecutedQuantity: parseFloat(resp.ExecutedQty),
		AvgPrice:         parseFloat(resp.AvgPrice),
		TransactTime:     resp.Time,
	}, nil
}

// QueryOrder 查询订单状态
func (c *BinanceClient) QueryOrder(ctx context.Context, param QueryOrderParam) (*OrderResult, error) {
	if param.Symbol == "" {
		return nil, fmt.Errorf("symbol is required")
	}
	if param.OrderID == "" && param.ClientOrderID == "" {
		return nil, fmt.Errorf("order_id or client_order_id is required")
	}

	q := url.Values{}
	q.Set("symbol", strings.ToUpper(param.Symbol))
	if param.OrderID != "" {
		q.Set("orderId", param.OrderID)
	}
	if param.ClientOrderID != "" {
		q.Set("origClientOrderId", param.ClientOrderID)
	}
	q.Set("recvWindow", "5000")

	var resp binanceOrderResponse
	if err := c.doSignedRequest(ctx, http.MethodGet, "/fapi/v1/order", q, &resp); err != nil {
		return nil, err
	}

	return c.convertOrderResponse(&resp), nil
}

// convertOrderResponse 将币安 API 响应转换为统一的 OrderResult
func (c *BinanceClient) convertOrderResponse(resp *binanceOrderResponse) *OrderResult {
	return &OrderResult{
		OrderID:          strconv.FormatInt(resp.OrderID, 10),
		ClientOrderID:    resp.ClientOrderID,
		Symbol:           resp.Symbol,
		Status:           OrderStatus(resp.Status),
		Side:             OrderSide(resp.Side),
		PositionSide:     PositionSide(resp.PositionSide),
		ExecutedQuantity: parseFloat(resp.ExecutedQty),
		AvgPrice:         parseFloat(resp.AvgPrice),
		TransactTime:     resp.Time,
	}
}

// ---------------------------------------------------------------------------
// 账户信息 API
// ---------------------------------------------------------------------------

// binanceAccountResponse 币安期货账户 API 响应结构
type binanceAccountResponse struct {
	TotalWalletBalance string                `json:"totalWalletBalance"`
	TotalUnrealizedPNL string                `json:"totalUnrealizedProfit"`
	TotalMarginBalance string                `json:"totalMarginBalance"`
	AvailableBalance   string                `json:"availableBalance"`
	MaxWithdrawAmount  string                `json:"maxWithdrawAmount"`
	Positions          []binancePositionResp `json:"positions"`
}

// binancePositionResp 币安期货仓位信息
type binancePositionResp struct {
	Symbol                 string `json:"symbol"`
	PositionAmt            string `json:"positionAmt"`
	EntryPrice             string `json:"entryPrice"`
	BreakEvenPrice         string `json:"breakEvenPrice"`
	MarkPrice              string `json:"markPrice"`
	UnrealizedProfit       string `json:"unrealizedProfit"`
	LiquidationPrice       string `json:"liquidationPrice"`
	Leverage               string `json:"leverage"`
	MarginType             string `json:"marginType"`
	PositionSide           string `json:"positionSide"`
	Notional               string `json:"notional"`
	InitialMargin          string `json:"initialMargin"`
	MaintMargin            string `json:"maintMargin"`
	PositionInitialMargin  string `json:"positionInitialMargin"`
	OpenOrderInitialMargin string `json:"openOrderInitialMargin"`
	IsolatedMargin         string `json:"isolatedMargin"`
	Isolated               bool   `json:"isolated"`
	BidNotional            string `json:"bidNotional"`
	AskNotional            string `json:"askNotional"`
	UpdateTime             int64  `json:"updateTime"`
	Adl                    int32  `json:"adl"`
}

type binancePositionRiskResp struct {
	Symbol                 string `json:"symbol"`
	PositionAmt            string `json:"positionAmt"`
	EntryPrice             string `json:"entryPrice"`
	BreakEvenPrice         string `json:"breakEvenPrice"`
	MarkPrice              string `json:"markPrice"`
	UnRealizedProfit       string `json:"unRealizedProfit"`
	LiquidationPrice       string `json:"liquidationPrice"`
	Leverage               string `json:"leverage"`
	MarginType             string `json:"marginType"`
	PositionSide           string `json:"positionSide"`
	Notional               string `json:"notional"`
	IsolatedMargin         string `json:"isolatedMargin"`
	IsolatedWallet         string `json:"isolatedWallet"`
	InitialMargin          string `json:"initialMargin"`
	MaintMargin            string `json:"maintMargin"`
	PositionInitialMargin  string `json:"positionInitialMargin"`
	OpenOrderInitialMargin string `json:"openOrderInitialMargin"`
	Adl                    int32  `json:"adl"`
	BidNotional            string `json:"bidNotional"`
	AskNotional            string `json:"askNotional"`
	UpdateTime             int64  `json:"updateTime"`
}

// GetAccountInfo 获取账户信息（含全部持仓）
func (c *BinanceClient) GetAccountInfo(ctx context.Context) (*AccountResult, error) {
	return c.getAccountInfo(ctx, "")
}

// GetAccountInfoBySymbol 获取指定交易对的账户仓位详情。
func (c *BinanceClient) GetAccountInfoBySymbol(ctx context.Context, symbol string) (*AccountResult, error) {
	return c.getAccountInfo(ctx, strings.ToUpper(strings.TrimSpace(symbol)))
}

func (c *BinanceClient) getAccountInfo(ctx context.Context, symbol string) (*AccountResult, error) {
	q := url.Values{}
	q.Set("recvWindow", "5000")

	var resp binanceAccountResponse
	if err := c.doSignedRequest(ctx, http.MethodGet, "/fapi/v2/account", q, &resp); err != nil {
		return nil, err
	}

	result := &AccountResult{
		TotalWalletBalance: parseFloat(resp.TotalWalletBalance),
		TotalUnrealizedPnl: parseFloat(resp.TotalUnrealizedPNL),
		TotalMarginBalance: parseFloat(resp.TotalMarginBalance),
		AvailableBalance:   parseFloat(resp.AvailableBalance),
		MaxWithdrawAmount:  parseFloat(resp.MaxWithdrawAmount),
		Positions:          make([]PositionInfo, 0, len(resp.Positions)),
	}

	openOrders, err := c.GetOpenOrders(ctx, symbol)
	if err != nil {
		return nil, err
	}
	sltpMap := buildSLTPMap(openOrders)
	algoSLTPMap, err := c.getAlgoSLTPMapBestEffort(ctx, symbol)
	if err == nil {
		for k, v := range algoSLTPMap {
			current := sltpMap[k]
			if current.stopLossPrice == 0 {
				current.stopLossPrice = v.stopLossPrice
			}
			if current.takeProfitPrice == 0 {
				current.takeProfitPrice = v.takeProfitPrice
			}
			sltpMap[k] = current
		}
	}
	accountPositionMap := buildAccountPositionMap(resp.Positions)
	symbolConfigMap, _ := c.getSymbolConfigBestEffort(ctx, symbol)
	positionRisks, err := c.getPositionRisk(ctx, symbol)
	if err != nil {
		return nil, err
	}

	// positionRisk 是当前仓位详情的更准确来源，尤其在 Demo/新版合约环境下更稳定。
	for _, p := range positionRisks {
		amt := parseFloat(p.PositionAmt)
		if amt == 0 {
			continue
		}
		positionSide := strings.ToUpper(strings.TrimSpace(p.PositionSide))
		if positionSide == "" {
			if amt > 0 {
				positionSide = string(PosLong)
			} else {
				positionSide = string(PosShort)
			}
		}
		notional := parseFloat(p.Notional)
		unrealizedPnl := parseFloat(p.UnRealizedProfit)
		accountPos := accountPositionMap[positionKey(strings.ToUpper(p.Symbol), positionSide)]
		initialMargin := parseFloat(p.InitialMargin)
		if initialMargin == 0 {
			initialMargin = accountPos.InitialMargin
		}
		pnlPercent := 0.0
		if initialMargin != 0 {
			pnlPercent = unrealizedPnl / math.Abs(initialMargin) * 100
		}
		key := positionKey(strings.ToUpper(p.Symbol), positionSide)
		fundingRate := c.getLatestFundingRateValueBestEffort(ctx, p.Symbol)
		symbolCfg := symbolConfigMap[strings.ToUpper(p.Symbol)]
		result.Positions = append(result.Positions, PositionInfo{
			Symbol:                 p.Symbol,
			PositionAmount:         amt,
			EntryPrice:             parseFloat(p.EntryPrice),
			MarkPrice:              parseFloat(p.MarkPrice),
			UnrealizedPnl:          unrealizedPnl,
			LiquidationPrice:       firstNonZero(parseFloat(p.LiquidationPrice), accountPos.LiquidationPrice),
			Leverage:               firstNonZero(parseFloat(p.Leverage), symbolCfg.Leverage, accountPos.Leverage),
			MarginType:             firstNonEmpty(p.MarginType, symbolCfg.MarginType, accountPos.MarginType),
			PositionSide:           positionSide,
			BreakEvenPrice:         parseFloat(p.BreakEvenPrice),
			Notional:               notional,
			InitialMargin:          initialMargin,
			MaintMargin:            firstNonZero(parseFloat(p.MaintMargin), accountPos.MaintMargin),
			PositionInitialMargin:  firstNonZero(parseFloat(p.PositionInitialMargin), accountPos.PositionInitialMargin),
			OpenOrderInitialMargin: firstNonZero(parseFloat(p.OpenOrderInitialMargin), accountPos.OpenOrderInitialMargin),
			IsolatedMargin:         firstNonZero(parseFloat(p.IsolatedMargin), parseFloat(p.IsolatedWallet), accountPos.IsolatedMargin),
			BidNotional:            parseFloat(p.BidNotional),
			AskNotional:            parseFloat(p.AskNotional),
			UpdateTime:             p.UpdateTime,
			Adl:                    p.Adl,
			PnlPercent:             pnlPercent,
			StopLossPrice:          sltpMap[key].stopLossPrice,
			TakeProfitPrice:        sltpMap[key].takeProfitPrice,
			FundingRate:            fundingRate,
			EstimatedFundingFee:    math.Abs(notional) * fundingRate,
		})
	}

	return result, nil
}

func (c *BinanceClient) getSymbolConfigBestEffort(ctx context.Context, symbol string) (map[string]binanceSymbolConfig, error) {
	queryCtx, cancel := context.WithTimeout(detachContext(ctx), 250*time.Millisecond)
	defer cancel()

	q := url.Values{}
	q.Set("recvWindow", "5000")
	if symbol != "" {
		q.Set("symbol", strings.ToUpper(strings.TrimSpace(symbol)))
	}

	var resp []binanceSymbolConfig
	if err := c.doSignedRequest(queryCtx, http.MethodGet, "/fapi/v1/symbolConfig", q, &resp); err != nil {
		return map[string]binanceSymbolConfig{}, err
	}

	result := make(map[string]binanceSymbolConfig, len(resp))
	for _, item := range resp {
		result[strings.ToUpper(item.Symbol)] = item
	}
	return result, nil
}

func (c *BinanceClient) getPositionRisk(ctx context.Context, symbol string) ([]binancePositionRiskResp, error) {
	q := url.Values{}
	q.Set("recvWindow", "5000")
	if symbol != "" {
		q.Set("symbol", strings.ToUpper(strings.TrimSpace(symbol)))
	}

	var resp []binancePositionRiskResp
	if err := c.doSignedRequest(ctx, http.MethodGet, "/fapi/v3/positionRisk", q, &resp); err == nil {
		return resp, nil
	}

	var fallback []binancePositionRiskResp
	if err := c.doSignedRequest(ctx, http.MethodGet, "/fapi/v2/positionRisk", q, &fallback); err != nil {
		return nil, err
	}
	return fallback, nil
}

func buildAccountPositionMap(positions []binancePositionResp) map[string]PositionInfo {
	result := make(map[string]PositionInfo, len(positions))
	for _, p := range positions {
		amt := parseFloat(p.PositionAmt)
		if amt == 0 {
			continue
		}
		positionSide := strings.ToUpper(strings.TrimSpace(p.PositionSide))
		if positionSide == "" {
			if amt > 0 {
				positionSide = string(PosLong)
			} else {
				positionSide = string(PosShort)
			}
		}
		result[positionKey(strings.ToUpper(p.Symbol), positionSide)] = PositionInfo{
			InitialMargin:          parseFloat(p.InitialMargin),
			MaintMargin:            parseFloat(p.MaintMargin),
			PositionInitialMargin:  parseFloat(p.PositionInitialMargin),
			OpenOrderInitialMargin: parseFloat(p.OpenOrderInitialMargin),
			IsolatedMargin:         parseFloat(p.IsolatedMargin),
			Leverage:               parseFloat(p.Leverage),
			MarginType:             normalizeMarginType(p.MarginType, p.Isolated),
			LiquidationPrice:       parseFloat(p.LiquidationPrice),
		}
	}
	return result
}

func firstNonZero(values ...float64) float64 {
	for _, v := range values {
		if v != 0 {
			return v
		}
	}
	return 0
}

func firstNonEmpty(values ...string) string {
	for _, v := range values {
		if strings.TrimSpace(v) != "" {
			return v
		}
	}
	return ""
}

func normalizeMarginType(marginType string, isolated bool) string {
	if strings.TrimSpace(marginType) != "" {
		return strings.ToUpper(strings.TrimSpace(marginType))
	}
	if isolated {
		return "ISOLATED"
	}
	return "CROSSED"
}

type sltpInfo struct {
	stopLossPrice   float64
	takeProfitPrice float64
}

type binanceAlgoOpenOrdersResponse struct {
	Total  int                    `json:"total"`
	Orders []binanceAlgoOpenOrder `json:"orders"`
}

type binanceAlgoOpenOrder struct {
	AlgoID        int64  `json:"algoId"`
	Symbol        string `json:"symbol"`
	Side          string `json:"side"`
	PositionSide  string `json:"positionSide"`
	AlgoStatus    string `json:"algoStatus"`
	AlgoType      string `json:"algoType"`
	OrderType     string `json:"orderType"`
	StopPrice     string `json:"stopPrice"`
	TriggerPrice  string `json:"triggerPrice"`
	ActivatePrice string `json:"activatePrice"`
	ClientAlgoID  string `json:"clientAlgoId"`
	ReduceOnly    bool   `json:"reduceOnly"`
	ClosePosition bool   `json:"closePosition"`
}

type binanceConditionalAlgoOrder struct {
	AlgoID         int64  `json:"algoId"`
	ClientAlgoID   string `json:"clientAlgoId"`
	AlgoType       string `json:"algoType"`
	OrderType      string `json:"orderType"`
	Symbol         string `json:"symbol"`
	Side           string `json:"side"`
	PositionSide   string `json:"positionSide"`
	TimeInForce    string `json:"timeInForce"`
	Quantity       string `json:"quantity"`
	AlgoStatus     string `json:"algoStatus"`
	TriggerPrice   string `json:"triggerPrice"`
	Price          string `json:"price"`
	TPTriggerPrice string `json:"tpTriggerPrice"`
	TPPrice        string `json:"tpPrice"`
	SLTriggerPrice string `json:"slTriggerPrice"`
	SLPrice        string `json:"slPrice"`
	WorkingType    string `json:"workingType"`
	ClosePosition  bool   `json:"closePosition"`
	PriceProtect   bool   `json:"priceProtect"`
	ReduceOnly     bool   `json:"reduceOnly"`
	CreateTime     int64  `json:"createTime"`
	UpdateTime     int64  `json:"updateTime"`
	TriggerTime    int64  `json:"triggerTime"`
}

type binanceSymbolConfig struct {
	Symbol           string  `json:"symbol"`
	MarginType       string  `json:"marginType"`
	IsAutoAddMargin  bool    `json:"isAutoAddMargin"`
	Leverage         float64 `json:"leverage"`
	MaxNotionalValue string  `json:"maxNotionalValue"`
}

func positionKey(symbol, positionSide string) string {
	return strings.ToUpper(strings.TrimSpace(symbol)) + "|" + strings.ToUpper(strings.TrimSpace(positionSide))
}

func (c *BinanceClient) getAlgoSLTPMap(ctx context.Context, symbol string) (map[string]sltpInfo, error) {
	if result, err := c.getConditionalAlgoSLTPMap(ctx, symbol); err == nil && len(result) > 0 {
		return result, nil
	}

	q := url.Values{}
	q.Set("recvWindow", "5000")

	var lastErr error
	for _, baseURL := range c.algoBaseURLs() {
		var resp binanceAlgoOpenOrdersResponse
		err := c.doSignedRequestBase(ctx, baseURL, http.MethodGet, "/sapi/v1/algo/futures/openOrders", q, &resp)
		if err != nil {
			lastErr = err
			continue
		}

		result := make(map[string]sltpInfo, len(resp.Orders))
		for _, order := range resp.Orders {
			key := positionKey(order.Symbol, order.PositionSide)
			info := result[key]
			price := firstNonZero(
				parseFloat(order.TriggerPrice),
				parseFloat(order.StopPrice),
				parseFloat(order.ActivatePrice),
			)
			typeName := strings.ToUpper(strings.TrimSpace(order.OrderType))
			if typeName == "" {
				typeName = strings.ToUpper(strings.TrimSpace(order.AlgoType))
			}

			switch {
			case strings.Contains(typeName, "STOP"):
				info.stopLossPrice = price
			case strings.Contains(typeName, "TAKE_PROFIT"), strings.Contains(typeName, "TP"):
				info.takeProfitPrice = price
			}
			result[key] = info
		}
		return result, nil
	}

	if lastErr == nil {
		lastErr = fmt.Errorf("algo open orders request failed")
	}
	return nil, lastErr
}

func (c *BinanceClient) getAlgoSLTPMapBestEffort(ctx context.Context, symbol string) (map[string]sltpInfo, error) {
	queryCtx, cancel := context.WithTimeout(detachContext(ctx), 350*time.Millisecond)
	defer cancel()

	result, err := c.getAlgoSLTPMap(queryCtx, symbol)
	if err != nil {
		return map[string]sltpInfo{}, err
	}
	return result, nil
}

func (c *BinanceClient) getConditionalAlgoSLTPMap(ctx context.Context, symbol string) (map[string]sltpInfo, error) {
	q := url.Values{}
	q.Set("recvWindow", "5000")
	if symbol != "" {
		q.Set("symbol", strings.ToUpper(strings.TrimSpace(symbol)))
	}

	var orders []binanceConditionalAlgoOrder
	if err := c.doSignedRequest(ctx, http.MethodGet, "/fapi/v1/algoOrders", q, &orders); err != nil {
		return nil, err
	}

	result := make(map[string]sltpInfo, len(orders))
	for _, order := range orders {
		if strings.ToUpper(strings.TrimSpace(order.AlgoType)) != "CONDITIONAL" {
			continue
		}
		key := positionKey(order.Symbol, order.PositionSide)
		info := result[key]

		orderType := strings.ToUpper(strings.TrimSpace(order.OrderType))
		price := firstNonZero(
			parseFloat(order.TriggerPrice),
			parseFloat(order.Price),
		)
		switch {
		case strings.Contains(orderType, "STOP"):
			info.stopLossPrice = price
		case strings.Contains(orderType, "TAKE_PROFIT"), strings.Contains(orderType, "TP"):
			info.takeProfitPrice = price
		}

		// Some conditional order payloads also expose combined TP/SL trigger fields.
		if info.stopLossPrice == 0 {
			info.stopLossPrice = firstNonZero(parseFloat(order.SLTriggerPrice), parseFloat(order.SLPrice))
		}
		if info.takeProfitPrice == 0 {
			info.takeProfitPrice = firstNonZero(parseFloat(order.TPTriggerPrice), parseFloat(order.TPPrice))
		}
		result[key] = info
	}
	return result, nil
}

func (c *BinanceClient) algoBaseURLs() []string {
	candidates := []string{
		c.baseURL,
		strings.Replace(c.baseURL, "://demo-fapi.", "://api.", 1),
		strings.Replace(c.baseURL, "://fapi.", "://api.", 1),
		"https://api.binance.com",
	}

	seen := make(map[string]struct{}, len(candidates))
	result := make([]string, 0, len(candidates))
	for _, candidate := range candidates {
		candidate = strings.TrimRight(strings.TrimSpace(candidate), "/")
		if candidate == "" {
			continue
		}
		if _, ok := seen[candidate]; ok {
			continue
		}
		seen[candidate] = struct{}{}
		result = append(result, candidate)
	}
	return result
}

func buildSLTPMap(orders []BinanceOpenOrder) map[string]sltpInfo {
	result := make(map[string]sltpInfo, len(orders))
	for _, order := range orders {
		if !order.ClosePosition {
			continue
		}
		key := positionKey(order.Symbol, order.PositionSide)
		info := result[key]
		switch strings.ToUpper(order.Type) {
		case "STOP_MARKET", "STOP":
			info.stopLossPrice = parseFloat(order.StopPrice)
		case "TAKE_PROFIT_MARKET", "TAKE_PROFIT":
			info.takeProfitPrice = parseFloat(order.StopPrice)
		}
		result[key] = info
	}
	return result
}

func (c *BinanceClient) getLatestFundingRateValue(ctx context.Context, symbol string) float64 {
	if strings.TrimSpace(symbol) == "" {
		return 0
	}
	rates, err := c.GetFundingRate(ctx, symbol, 0, 0, 1)
	if err != nil || len(rates) == 0 {
		return 0
	}
	return parseFloat(rates[len(rates)-1].FundingRate)
}

func (c *BinanceClient) getLatestFundingRateValueBestEffort(ctx context.Context, symbol string) float64 {
	queryCtx, cancel := context.WithTimeout(detachContext(ctx), 250*time.Millisecond)
	defer cancel()
	return c.getLatestFundingRateValue(queryCtx, symbol)
}

func detachContext(parent context.Context) context.Context {
	if parent == nil {
		return context.Background()
	}
	return context.WithoutCancel(parent)
}

// parseFloat 安全解析浮点数字符串
func parseFloat(v string) float64 {
	f, _ := strconv.ParseFloat(v, 64)
	return f
}

// ---------------------------------------------------------------------------
// 合约查询 API（当前委托、历史委托、历史成交、资金流水、资金费用）
// ---------------------------------------------------------------------------

// BinanceOpenOrder 币安合约当前委托响应结构
type BinanceOpenOrder struct {
	OrderID       int64  `json:"orderId"`
	Symbol        string `json:"symbol"`
	Status        string `json:"status"`
	Side          string `json:"side"`
	PositionSide  string `json:"positionSide"`
	Type          string `json:"type"`
	OrigQty       string `json:"origQty"`
	ExecutedQty   string `json:"executedQty"`
	AvgPrice      string `json:"avgPrice"`
	Price         string `json:"price"`
	StopPrice     string `json:"stopPrice"`
	ClientOrderID string `json:"clientOrderId"`
	Time          int64  `json:"time"`
	UpdateTime    int64  `json:"updateTime"`
	ReduceOnly    bool   `json:"reduceOnly"`
	ClosePosition bool   `json:"closePosition"`
	TimeInForce   string `json:"timeInForce"`
}

// GetOpenOrders 查询当前委托（未成交订单）
// symbol 为空时查询所有交易对的当前委托
func (c *BinanceClient) GetOpenOrders(ctx context.Context, symbol string) ([]BinanceOpenOrder, error) {
	q := url.Values{}
	if symbol != "" {
		q.Set("symbol", strings.ToUpper(symbol))
	}
	q.Set("recvWindow", "5000")

	var resp []BinanceOpenOrder
	if err := c.doSignedRequest(ctx, http.MethodGet, "/fapi/v1/openOrders", q, &resp); err != nil {
		return nil, err
	}
	return resp, nil
}

// BinanceAllOrder 币安合约历史委托响应结构
type BinanceAllOrder struct {
	OrderID       int64  `json:"orderId"`
	Symbol        string `json:"symbol"`
	Status        string `json:"status"`
	Side          string `json:"side"`
	PositionSide  string `json:"positionSide"`
	Type          string `json:"type"`
	OrigQty       string `json:"origQty"`
	ExecutedQty   string `json:"executedQty"`
	AvgPrice      string `json:"avgPrice"`
	Price         string `json:"price"`
	StopPrice     string `json:"stopPrice"`
	ClientOrderID string `json:"clientOrderId"`
	Time          int64  `json:"time"`
	UpdateTime    int64  `json:"updateTime"`
	ReduceOnly    bool   `json:"reduceOnly"`
	ClosePosition bool   `json:"closePosition"`
	TimeInForce   string `json:"timeInForce"`
}

// GetAllOrders 查询历史委托（所有订单）
// startTime/endTime 为毫秒时间戳，0 表示不限制
// limit 最大1000，默认500
func (c *BinanceClient) GetAllOrders(ctx context.Context, symbol string, startTime, endTime int64, limit int) ([]BinanceAllOrder, error) {
	if symbol == "" {
		return nil, fmt.Errorf("symbol is required for allOrders")
	}

	q := url.Values{}
	q.Set("symbol", strings.ToUpper(symbol))
	if startTime > 0 {
		q.Set("startTime", strconv.FormatInt(startTime, 10))
	}
	if endTime > 0 {
		q.Set("endTime", strconv.FormatInt(endTime, 10))
	}
	if limit > 0 {
		q.Set("limit", strconv.Itoa(limit))
	}
	q.Set("recvWindow", "5000")

	var resp []BinanceAllOrder
	if err := c.doSignedRequest(ctx, http.MethodGet, "/fapi/v1/allOrders", q, &resp); err != nil {
		return nil, err
	}
	return resp, nil
}

// BinanceUserTrade 币安合约历史成交响应结构
type BinanceUserTrade struct {
	ID              int64  `json:"id"`
	Symbol          string `json:"symbol"`
	OrderID         int64  `json:"orderId"`
	Side            string `json:"side"`
	PositionSide    string `json:"positionSide"`
	Price           string `json:"price"`
	Qty             string `json:"qty"`
	RealizedPnl     string `json:"realizedPnl"`
	MarginAsset     string `json:"marginAsset"`
	QuoteQty        string `json:"quoteQty"`
	Commission      string `json:"commission"`
	CommissionAsset string `json:"commissionAsset"`
	Time            int64  `json:"time"`
	Buyer           bool   `json:"buyer"`
	Maker           bool   `json:"maker"`
}

// GetUserTrades 查询历史成交
// startTime/endTime 为毫秒时间戳，0 表示不限制
// limit 最大1000，默认500
func (c *BinanceClient) GetUserTrades(ctx context.Context, symbol string, startTime, endTime int64, limit int) ([]BinanceUserTrade, error) {
	if symbol == "" {
		return nil, fmt.Errorf("symbol is required for userTrades")
	}

	q := url.Values{}
	q.Set("symbol", strings.ToUpper(symbol))
	if startTime > 0 {
		q.Set("startTime", strconv.FormatInt(startTime, 10))
	}
	if endTime > 0 {
		q.Set("endTime", strconv.FormatInt(endTime, 10))
	}
	if limit > 0 {
		q.Set("limit", strconv.Itoa(limit))
	}
	q.Set("recvWindow", "5000")

	var resp []BinanceUserTrade
	if err := c.doSignedRequest(ctx, http.MethodGet, "/fapi/v1/userTrades", q, &resp); err != nil {
		return nil, err
	}
	return resp, nil
}

// BinanceIncome 币安合约资金流水响应结构
type BinanceIncome struct {
	Symbol     string `json:"symbol"`
	IncomeType string `json:"incomeType"` // TRANSFER, WELCOME_BONUS, REALIZED_PNL, FUNDING_FEE, COMMISSION, etc.
	Income     string `json:"income"`
	Asset      string `json:"asset"`
	Time       int64  `json:"time"`
	TranID     int64  `json:"tranId"`
	TradeID    string `json:"tradeId"`
}

// GetIncomeHistory 查询资金流水
// incomeType 过滤收入类型，空字符串表示所有类型
// startTime/endTime 为毫秒时间戳，0 表示不限制
// limit 最大1000，默认100
func (c *BinanceClient) GetIncomeHistory(ctx context.Context, symbol, incomeType string, startTime, endTime int64, limit int) ([]BinanceIncome, error) {
	q := url.Values{}
	if symbol != "" {
		q.Set("symbol", strings.ToUpper(symbol))
	}
	if incomeType != "" {
		q.Set("incomeType", incomeType)
	}
	if startTime > 0 {
		q.Set("startTime", strconv.FormatInt(startTime, 10))
	}
	if endTime > 0 {
		q.Set("endTime", strconv.FormatInt(endTime, 10))
	}
	if limit > 0 {
		q.Set("limit", strconv.Itoa(limit))
	}
	q.Set("recvWindow", "5000")

	var resp []BinanceIncome
	if err := c.doSignedRequest(ctx, http.MethodGet, "/fapi/v1/income", q, &resp); err != nil {
		return nil, err
	}
	return resp, nil
}

// BinanceFundingRate 币安合约资金费率响应结构
type BinanceFundingRate struct {
	Symbol      string `json:"symbol"`
	FundingRate string `json:"fundingRate"`
	FundingTime int64  `json:"fundingTime"`
	MarkPrice   string `json:"markPrice"`
}

// BinanceFundingFee 资金费用条目（从资金流水中提取 FUNDING_FEE 类型）
type BinanceFundingFee struct {
	Symbol     string `json:"symbol"`
	IncomeType string `json:"incomeType"`
	Income     string `json:"income"`
	Asset      string `json:"asset"`
	Time       int64  `json:"time"`
	TranID     int64  `json:"tranId"`
	TradeID    string `json:"tradeId"`
}

// GetFundingRate 查询资金费率历史
// startTime/endTime 为毫秒时间戳，0 表示不限制
// limit 最大1000，默认100
func (c *BinanceClient) GetFundingRate(ctx context.Context, symbol string, startTime, endTime int64, limit int) ([]BinanceFundingRate, error) {
	q := url.Values{}
	if symbol != "" {
		q.Set("symbol", strings.ToUpper(symbol))
	}
	if startTime > 0 {
		q.Set("startTime", strconv.FormatInt(startTime, 10))
	}
	if endTime > 0 {
		q.Set("endTime", strconv.FormatInt(endTime, 10))
	}
	if limit > 0 {
		q.Set("limit", strconv.Itoa(limit))
	}

	var resp []BinanceFundingRate
	if err := c.doSignedRequest(ctx, http.MethodGet, "/fapi/v1/fundingRate", q, &resp); err != nil {
		return nil, err
	}
	return resp, nil
}

// GetFundingFees 查询资金费用（从资金流水中筛选 FUNDING_FEE 类型）
// startTime/endTime 为毫秒时间戳，0 表示不限制
// limit 最大1000，默认100
func (c *BinanceClient) GetFundingFees(ctx context.Context, symbol string, startTime, endTime int64, limit int) ([]BinanceFundingFee, error) {
	// 资金费用本质是 incomeType=FUNDING_FEE 的资金流水
	incomes, err := c.GetIncomeHistory(ctx, symbol, "FUNDING_FEE", startTime, endTime, limit)
	if err != nil {
		return nil, err
	}

	fees := make([]BinanceFundingFee, 0, len(incomes))
	for _, inc := range incomes {
		fees = append(fees, BinanceFundingFee{
			Symbol:     inc.Symbol,
			IncomeType: inc.IncomeType,
			Income:     inc.Income,
			Asset:      inc.Asset,
			Time:       inc.Time,
			TranID:     inc.TranID,
			TradeID:    inc.TradeID,
		})
	}
	return fees, nil
}
