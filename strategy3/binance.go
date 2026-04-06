package strategy3

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"time"
)

type MarketDataSource interface {
	FetchCandles(ctx context.Context, symbol, interval string, limit int) ([]Candle, error)
}

type BinanceClient struct {
	BaseURL    string
	HTTPClient *http.Client
}

func NewBinanceClient() *BinanceClient {
	return &BinanceClient{
		BaseURL: "https://api.binance.com",
		HTTPClient: &http.Client{
			Timeout: 30 * time.Second,
		},
	}
}

func (c *BinanceClient) FetchCandles(ctx context.Context, symbol, interval string, limit int) ([]Candle, error) {
	if limit <= 0 {
		limit = 100
	}
	baseURL := c.BaseURL
	if baseURL == "" {
		baseURL = "https://api.binance.com"
	}
	httpClient := c.HTTPClient
	if httpClient == nil {
		httpClient = &http.Client{Timeout: 30 * time.Second}
	}

	endpoint, err := url.Parse(baseURL + "/api/v3/klines")
	if err != nil {
		return nil, err
	}
	query := endpoint.Query()
	query.Set("symbol", symbol)
	query.Set("interval", interval)
	query.Set("limit", strconv.Itoa(limit))
	endpoint.RawQuery = query.Encode()

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint.String(), nil)
	if err != nil {
		return nil, err
	}
	var resp *http.Response
	maxRetries := 3
	retryDelay := 2 * time.Second

	for i := 0; i < maxRetries; i++ {
		if i > 0 {
			select {
			case <-time.After(retryDelay):
			case <-ctx.Done():
				return nil, ctx.Err()
			}
		}
		resp, err = httpClient.Do(req)
		if err == nil {
			break
		}
		// 如果是临时性网络错误且还有重试机会，则继续重试
		if isTemporaryError(err) && i < maxRetries-1 {
			continue
		}
		return nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(io.LimitReader(resp.Body, 2048))
		return nil, fmt.Errorf("binance klines request failed: status=%d body=%s", resp.StatusCode, string(body))
	}

	var payload [][]any
	if err := json.NewDecoder(resp.Body).Decode(&payload); err != nil {
		return nil, err
	}

	candles := make([]Candle, 0, len(payload))
	for _, row := range payload {
		if len(row) < 7 {
			return nil, fmt.Errorf("unexpected kline payload length: %d", len(row))
		}
		openTime, err := toInt64(row[0])
		if err != nil {
			return nil, err
		}
		open, err := toFloat(row[1])
		if err != nil {
			return nil, err
		}
		high, err := toFloat(row[2])
		if err != nil {
			return nil, err
		}
		low, err := toFloat(row[3])
		if err != nil {
			return nil, err
		}
		closePrice, err := toFloat(row[4])
		if err != nil {
			return nil, err
		}
		volume, err := toFloat(row[5])
		if err != nil {
			return nil, err
		}
		closeTime, err := toInt64(row[6])
		if err != nil {
			return nil, err
		}

		closed := true
		if len(row) > 11 {
			if value, ok := row[11].(bool); ok {
				closed = value
			}
		}

		candles = append(candles, Candle{
			OpenTime:  time.UnixMilli(openTime),
			CloseTime: time.UnixMilli(closeTime),
			Open:      open,
			High:      high,
			Low:       low,
			Close:     closePrice,
			Volume:    volume,
			Closed:    closed,
		})
	}

	return ClosedCandles(candles), nil
}

func ClosedCandles(candles []Candle) []Candle {
	filtered := make([]Candle, 0, len(candles))
	for _, candle := range candles {
		if candle.Closed {
			filtered = append(filtered, candle)
		}
	}
	if len(filtered) == 0 {
		return candles
	}
	return filtered
}

type LiveService struct {
	Engine       *Engine
	DataSource   MarketDataSource
	Symbol       string
	Account      Account
	State        State
	PollInterval time.Duration
}

func NewLiveService(symbol string, source MarketDataSource, account Account) *LiveService {
	return &LiveService{
		Engine:       NewEngine(DefaultParams()),
		DataSource:   source,
		Symbol:       symbol,
		Account:      account,
		PollInterval: 15 * time.Minute,
	}
}

func (s *LiveService) EvaluateOnce(ctx context.Context) (Decision, error) {
	if s.Engine == nil {
		s.Engine = NewEngine(DefaultParams())
	}
	if s.DataSource == nil {
		s.DataSource = NewBinanceClient()
	}
	snapshot, err := s.loadSnapshot(ctx)
	if err != nil {
		return Decision{}, err
	}
	decision, err := s.Engine.Evaluate(snapshot, s.State, s.Account)
	if err != nil {
		return Decision{}, err
	}
	s.State = decision.UpdatedState
	s.Account.Equity += decision.RealizedPnL
	if s.Account.Equity < 0 {
		s.Account.Equity = 0
	}
	if s.Account.AvailableCash == 0 {
		s.Account.AvailableCash = s.Account.Equity
	}
	return decision, nil
}

func (s *LiveService) Run(ctx context.Context, handler func(Decision)) error {
	interval := s.PollInterval
	if interval <= 0 {
		interval = 15 * time.Minute
	}

	decision, err := s.EvaluateOnce(ctx)
	if err != nil {
		return err
	}
	if handler != nil {
		handler(decision)
	}

	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
			decision, err := s.EvaluateOnce(ctx)
			if err != nil {
				return err
			}
			if handler != nil {
				handler(decision)
			}
		}
	}
}

func (s *LiveService) loadSnapshot(ctx context.Context) (Snapshot, error) {
	h4, err := s.DataSource.FetchCandles(ctx, s.Symbol, "4h", maxInt(100, s.Engine.params.H4EmaSlow+5))
	if err != nil {
		return Snapshot{}, err
	}
	h1, err := s.DataSource.FetchCandles(ctx, s.Symbol, "1h", maxInt(100, s.Engine.params.H1EmaSlow+10))
	if err != nil {
		return Snapshot{}, err
	}
	m15, err := s.DataSource.FetchCandles(ctx, s.Symbol, "15m", maxInt(100, s.Engine.params.M15BreakoutLookback+20))
	if err != nil {
		return Snapshot{}, err
	}
	if len(m15) == 0 {
		return Snapshot{}, fmt.Errorf("no m15 candles returned for %s", s.Symbol)
	}

	return Snapshot{
		Symbol:    s.Symbol,
		H4:        h4,
		H1:        h1,
		M15:       m15,
		Timestamp: m15[len(m15)-1].CloseTime,
	}, nil
}

func toFloat(value any) (float64, error) {
	switch v := value.(type) {
	case string:
		return strconv.ParseFloat(v, 64)
	case float64:
		return v, nil
	case json.Number:
		return v.Float64()
	default:
		return 0, fmt.Errorf("unsupported float value type: %T", value)
	}
}

func toInt64(value any) (int64, error) {
	switch v := value.(type) {
	case float64:
		return int64(v), nil
	case int64:
		return v, nil
	case json.Number:
		return v.Int64()
	default:
		return 0, fmt.Errorf("unsupported integer value type: %T", value)
	}
}
