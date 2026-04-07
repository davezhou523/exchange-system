package binance

import (
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"
)

type BinanceClient struct {
	baseURL   string
	apiKey    string
	secretKey string
	client    *http.Client
}

type OrderResponse struct {
	OrderID  int64  `json:"orderId"`
	Symbol   string `json:"symbol"`
	Status   string `json:"status"`
	AvgPrice string `json:"avgPrice"`
	Time     int64  `json:"time"`
}

type AccountInfo struct {
	TotalWalletBalance string `json:"totalWalletBalance"`
}

func NewBinanceClient(baseURL, apiKey, secretKey string) *BinanceClient {
	if baseURL == "" {
		baseURL = "https://fapi.binance.com"
	}
	return &BinanceClient{
		baseURL:   strings.TrimRight(baseURL, "/"),
		apiKey:    apiKey,
		secretKey: secretKey,
		client:    &http.Client{Timeout: 30 * time.Second},
	}
}

func (c *BinanceClient) signedQuery(query url.Values) (string, error) {
	query.Set("timestamp", strconv.FormatInt(time.Now().UnixMilli(), 10))
	encoded := query.Encode()
	mac := hmac.New(sha256.New, []byte(c.secretKey))
	_, _ = mac.Write([]byte(encoded))
	signature := hex.EncodeToString(mac.Sum(nil))
	return encoded + "&signature=" + url.QueryEscape(signature), nil
}

func (c *BinanceClient) doSignedRequest(ctx context.Context, method, path string, query url.Values, out any) error {
	sq, err := c.signedQuery(query)
	if err != nil {
		return err
	}
	endpoint := c.baseURL + path + "?" + sq

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
		body, _ := io.ReadAll(io.LimitReader(resp.Body, 2048))
		return fmt.Errorf("binance request failed: status=%d body=%s", resp.StatusCode, string(body))
	}
	return json.NewDecoder(resp.Body).Decode(out)
}

func (c *BinanceClient) CreateOrder(ctx context.Context, symbol, side, positionSide string, quantity float64) (*OrderResponse, error) {
	q := url.Values{}
	q.Set("symbol", strings.ToUpper(symbol))
	q.Set("side", strings.ToUpper(side))
	q.Set("positionSide", strings.ToUpper(positionSide))
	q.Set("type", "MARKET")
	q.Set("quantity", fmt.Sprintf("%.6f", quantity))
	q.Set("recvWindow", "5000")

	var resp OrderResponse
	if err := c.doSignedRequest(ctx, http.MethodPost, "/fapi/v1/order", q, &resp); err != nil {
		return nil, err
	}
	return &resp, nil
}

func (c *BinanceClient) GetAccountInfo(ctx context.Context) (*AccountInfo, error) {
	q := url.Values{}
	q.Set("recvWindow", "5000")

	var resp AccountInfo
	if err := c.doSignedRequest(ctx, http.MethodGet, "/fapi/v2/account", q, &resp); err != nil {
		return nil, err
	}
	return &resp, nil
}
