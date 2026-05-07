package svc

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	"exchange-system/app/market/rpc/internal/aggregator"
	"exchange-system/app/market/rpc/internal/config"
	"exchange-system/common/pb/market"
)

const oneMinuteMillis = int64(time.Minute / time.Millisecond)

// clickHouseRecoveryClient 封装 market 启动补数所需的 ClickHouse 查询与删除能力。
type clickHouseRecoveryClient struct {
	endpoint string
	database string
	username string
	password string
	client   *http.Client
}

type clickHouseLatestOpenTime struct {
	Symbol         string          `json:"symbol"`
	LastOpenTimeMs json.RawMessage `json:"last_open_time_ms"`
}

// recoverKlineFactGap 在 market 启动时按 ClickHouse 最新 1m 断点回补缺失 K 线。
func recoverKlineFactGap(ctx context.Context, cfg config.Config, agg *aggregator.KlineAggregator, chWriter *clickHouseWriter, warmupper aggregator.HistoryWarmupper) error {
	if agg == nil || chWriter == nil || warmupper == nil {
		return nil
	}
	if !cfg.ClickHouse.Enabled || !cfg.ClickHouse.Recovery.Enabled {
		return nil
	}

	symbols := normalizeRecoverySymbols(cfg.Binance.Symbols)
	if len(symbols) == 0 {
		return nil
	}
	log.Printf("[market recovery] start symbols=%d deleteOverlap=%v overlapWindow=%s pageLimit=%d warmupWaitTimeout=%s",
		len(symbols), cfg.ClickHouse.Recovery.DeleteOverlap, cfg.ClickHouse.Recovery.OverlapWindow,
		cfg.ClickHouse.Recovery.PageLimit, cfg.ClickHouse.Recovery.WarmupWaitTimeout)

	recoveryClient, err := newClickHouseRecoveryClient(cfg.ClickHouse)
	if err != nil {
		return err
	}

	queryCtx, cancel := context.WithTimeout(ctx, cfg.ClickHouse.Recovery.QueryTimeout)
	defer cancel()

	queryStartedAt := time.Now()
	log.Printf("[market recovery] step=query_checkpoints status=begin")
	checkpoints, err := recoveryClient.queryLatest1mOpenTimes(queryCtx, symbols)
	if err != nil {
		return err
	}
	log.Printf("[market recovery] step=query_checkpoints status=done checkpoints=%d elapsed=%s",
		len(checkpoints), time.Since(queryStartedAt).Round(time.Millisecond))

	latestClosedOpenTime := calcLatestClosed1mOpenTime(time.Now().UTC())
	if latestClosedOpenTime <= 0 {
		return nil
	}
	log.Printf("[market recovery] latest_closed_1m=%s", formatRecoveryTime(latestClosedOpenTime))

	agg.SetKafkaSendEnabled(false)
	defer agg.SetKafkaSendEnabled(true)

	for _, symbol := range symbols {
		lastOpenTime, ok := checkpoints[symbol]
		if !ok || lastOpenTime <= 0 {
			log.Printf("[market recovery] skip symbol=%s: no ClickHouse checkpoint", symbol)
			continue
		}
		log.Printf("[market recovery] symbol=%s checkpoint=%s", symbol, formatRecoveryTime(lastOpenTime))

		startOpenTime := lastOpenTime + oneMinuteMillis
		if cfg.ClickHouse.Recovery.DeleteOverlap {
			startOpenTime = calcRecoveryStartOpenTime(lastOpenTime, cfg.ClickHouse.Recovery.OverlapWindow)
			deleteStartedAt := time.Now()
			log.Printf("[market recovery] step=delete_overlap status=begin symbol=%s from=%s",
				symbol, formatRecoveryTime(startOpenTime))
			deleteCtx, deleteCancel := context.WithTimeout(ctx, cfg.ClickHouse.Recovery.QueryTimeout)
			err = recoveryClient.deleteKlineFactFromOpenTime(deleteCtx, symbol, startOpenTime)
			deleteCancel()
			if err != nil {
				return fmt.Errorf("delete overlap for %s: %w", symbol, err)
			}
			log.Printf("[market recovery] step=delete_overlap status=done symbol=%s from=%s elapsed=%s",
				symbol, formatRecoveryTime(startOpenTime), time.Since(deleteStartedAt).Round(time.Millisecond))
		}

		if startOpenTime > latestClosedOpenTime {
			log.Printf("[market recovery] symbol=%s already up-to-date lastOpen=%s",
				symbol, formatRecoveryTime(lastOpenTime))
			continue
		}

		fetchStartedAt := time.Now()
		log.Printf("[market recovery] step=fetch_gap status=begin symbol=%s from=%s to=%s",
			symbol, formatRecoveryTime(startOpenTime), formatRecoveryTime(latestClosedOpenTime))
		klines, err := fetchRecoveryKlines(ctx, warmupper, symbol, startOpenTime, latestClosedOpenTime, cfg.ClickHouse.Recovery.PageLimit)
		if err != nil {
			return fmt.Errorf("fetch missing klines for %s: %w", symbol, err)
		}
		log.Printf("[market recovery] step=fetch_gap status=done symbol=%s count=%d elapsed=%s",
			symbol, len(klines), time.Since(fetchStartedAt).Round(time.Millisecond))
		if len(klines) == 0 {
			log.Printf("[market recovery] symbol=%s no missing klines between %s and %s",
				symbol, formatRecoveryTime(startOpenTime), formatRecoveryTime(latestClosedOpenTime))
			continue
		}

		log.Printf("[market recovery] step=replay_gap status=begin symbol=%s count=%d from=%s to=%s deleteOverlap=%v",
			symbol, len(klines), formatRecoveryTime(klines[0].OpenTime),
			formatRecoveryTime(klines[len(klines)-1].OpenTime), cfg.ClickHouse.Recovery.DeleteOverlap)
		replayStartedAt := time.Now()
		if err := replayRecoveryKlines(ctx, agg, symbol, klines, cfg.ClickHouse.Recovery.WarmupWaitTimeout); err != nil {
			return fmt.Errorf("replay missing klines for %s: %w", symbol, err)
		}
		log.Printf("[market recovery] step=replay_gap status=done symbol=%s count=%d elapsed=%s",
			symbol, len(klines), time.Since(replayStartedAt).Round(time.Millisecond))
	}

	log.Printf("[market recovery] done symbols=%d", len(symbols))
	return nil
}

// newClickHouseRecoveryClient 创建启动补数使用的 ClickHouse HTTP 客户端。
func newClickHouseRecoveryClient(cfg config.ClickHouseConfig) (*clickHouseRecoveryClient, error) {
	endpoint := strings.TrimSpace(cfg.Endpoint)
	if endpoint == "" {
		return nil, fmt.Errorf("clickhouse endpoint is empty")
	}
	timeout := cfg.Recovery.QueryTimeout
	if timeout <= 0 {
		timeout = cfg.Timeout
	}
	if timeout <= 0 {
		timeout = 15 * time.Second
	}
	return &clickHouseRecoveryClient{
		endpoint: endpoint,
		database: strings.TrimSpace(cfg.Database),
		username: strings.TrimSpace(cfg.Username),
		password: cfg.Password,
		client: &http.Client{
			Timeout: timeout,
		},
	}, nil
}

// queryLatest1mOpenTimes 查询每个交易对在 ClickHouse 中最新已落库的 1m 开盘时间。
func (c *clickHouseRecoveryClient) queryLatest1mOpenTimes(ctx context.Context, symbols []string) (map[string]int64, error) {
	result := make(map[string]int64, len(symbols))
	if c == nil || len(symbols) == 0 {
		return result, nil
	}

	query := fmt.Sprintf(
		"SELECT symbol, toInt64(max(toUnixTimestamp64Milli(open_time))) AS last_open_time_ms "+
			"FROM %s.kline_fact WHERE interval = '1m' AND is_closed = 1 AND symbol IN (%s) GROUP BY symbol FORMAT JSONEachRow",
		c.database,
		buildClickHouseStringList(symbols),
	)

	body, err := c.executeQuery(ctx, query, nil)
	if err != nil {
		return nil, err
	}
	if len(bytes.TrimSpace(body)) == 0 {
		return result, nil
	}

	scanner := bufio.NewScanner(bytes.NewReader(body))
	scanner.Buffer(make([]byte, 0, 1024), 1024*1024)
	for scanner.Scan() {
		line := bytes.TrimSpace(scanner.Bytes())
		if len(line) == 0 {
			continue
		}
		var row clickHouseLatestOpenTime
		if err := json.Unmarshal(line, &row); err != nil {
			return nil, fmt.Errorf("decode checkpoint row: %w", err)
		}
		lastOpenTime, err := parseJSONInt64(row.LastOpenTimeMs)
		if err != nil {
			return nil, fmt.Errorf("parse checkpoint last_open_time_ms: %w", err)
		}
		if row.Symbol == "" || lastOpenTime <= 0 {
			continue
		}
		result[strings.ToUpper(strings.TrimSpace(row.Symbol))] = lastOpenTime
	}
	if err := scanner.Err(); err != nil {
		return nil, fmt.Errorf("scan checkpoint rows: %w", err)
	}
	return result, nil
}

// deleteKlineFactFromOpenTime 删除指定交易对自某个开盘时间起的重叠分析数据。
func (c *clickHouseRecoveryClient) deleteKlineFactFromOpenTime(ctx context.Context, symbol string, startOpenTime int64) error {
	if c == nil || symbol == "" || startOpenTime <= 0 {
		return nil
	}
	query := fmt.Sprintf(
		"ALTER TABLE %s.kline_fact DELETE WHERE symbol = %s AND open_time >= fromUnixTimestamp64Milli(%d)",
		c.database,
		quoteClickHouseString(symbol),
		startOpenTime,
	)
	params := url.Values{}
	params.Set("mutations_sync", "1")
	_, err := c.executeQuery(ctx, query, params)
	return err
}

// executeQuery 通过 ClickHouse HTTP 接口执行单条 SQL。
func (c *clickHouseRecoveryClient) executeQuery(ctx context.Context, query string, params url.Values) ([]byte, error) {
	if c == nil {
		return nil, fmt.Errorf("clickhouse client is nil")
	}
	endpoint, err := url.Parse(c.endpoint)
	if err != nil {
		return nil, fmt.Errorf("parse clickhouse endpoint: %w", err)
	}
	queryParams := endpoint.Query()
	for key, values := range params {
		for _, value := range values {
			queryParams.Add(key, value)
		}
	}
	if c.database != "" {
		queryParams.Set("database", c.database)
	}
	endpoint.RawQuery = queryParams.Encode()

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint.String(), strings.NewReader(query))
	if err != nil {
		return nil, fmt.Errorf("build clickhouse request: %w", err)
	}
	req.Header.Set("Content-Type", "text/plain; charset=utf-8")
	if c.username != "" {
		req.SetBasicAuth(c.username, c.password)
	}

	resp, err := c.client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("execute clickhouse request: %w", err)
	}
	defer resp.Body.Close()

	body, readErr := io.ReadAll(resp.Body)
	if readErr != nil {
		return nil, fmt.Errorf("read clickhouse response: %w", readErr)
	}
	if resp.StatusCode >= http.StatusBadRequest {
		return nil, fmt.Errorf("clickhouse status=%d body=%s", resp.StatusCode, strings.TrimSpace(string(body)))
	}
	return body, nil
}

// fetchRecoveryKlines 按分页方式从 Binance 拉取指定交易对缺失的 1m K 线。
func fetchRecoveryKlines(ctx context.Context, warmupper aggregator.HistoryWarmupper, symbol string, startOpenTime, endOpenTime int64, pageLimit int) ([]*market.Kline, error) {
	if warmupper == nil || symbol == "" || startOpenTime <= 0 || endOpenTime < startOpenTime {
		return nil, nil
	}
	if pageLimit <= 0 || pageLimit > 1500 {
		pageLimit = 1500
	}

	result := make([]*market.Kline, 0, 512)
	nextStart := startOpenTime
	totalMissingCount := recoveryWindowCount(startOpenTime, endOpenTime)
	page := 0
	for nextStart <= endOpenTime {
		page++
		pageStartedAt := time.Now()
		log.Printf("[market recovery] step=fetch_gap_page status=begin symbol=%s expected_open=%s current_open=%s missing_count=%d page=%d from=%s to=%s limit=%d",
			symbol,
			formatRecoveryTime(startOpenTime),
			formatRecoveryTime(endOpenTime),
			totalMissingCount,
			page,
			formatRecoveryTime(nextStart),
			formatRecoveryTime(endOpenTime),
			pageLimit)
		rows, err := warmupper.FetchKlinesRange(ctx, symbol, "1m", nextStart, endOpenTime, pageLimit)
		if err != nil {
			return nil, err
		}
		if len(rows) == 0 {
			log.Printf("[market recovery] step=fetch_gap_page status=done symbol=%s expected_open=%s current_open=%s missing_count=%d page=%d rows=0 elapsed=%s",
				symbol,
				formatRecoveryTime(startOpenTime),
				formatRecoveryTime(endOpenTime),
				totalMissingCount,
				page,
				time.Since(pageStartedAt).Round(time.Millisecond))
			break
		}

		lastOpenTime := int64(0)
		pageCount := 0
		for _, row := range rows {
			kline, err := buildRecoveryKline(symbol, row)
			if err != nil {
				return nil, err
			}
			if kline.OpenTime < nextStart || kline.OpenTime > endOpenTime {
				continue
			}
			if !kline.IsClosed {
				continue
			}
			result = append(result, kline)
			pageCount++
			lastOpenTime = kline.OpenTime
		}
		log.Printf("[market recovery] step=fetch_gap_page status=done symbol=%s expected_open=%s current_open=%s missing_count=%d page=%d rows=%d accepted=%d last_open=%s elapsed=%s",
			symbol,
			formatRecoveryTime(startOpenTime),
			formatRecoveryTime(endOpenTime),
			totalMissingCount,
			page,
			len(rows),
			pageCount,
			formatRecoveryTime(lastOpenTime),
			time.Since(pageStartedAt).Round(time.Millisecond))

		if lastOpenTime == 0 {
			break
		}
		if lastOpenTime >= endOpenTime {
			break
		}
		nextStart = lastOpenTime + oneMinuteMillis
	}
	return result, nil
}

// buildRecoveryKline 把 Binance REST K 线数组转换为聚合器可消费的闭合 1m K 线。
func buildRecoveryKline(symbol string, row []interface{}) (*market.Kline, error) {
	if len(row) < 11 {
		return nil, fmt.Errorf("invalid binance kline row length: %d", len(row))
	}

	openTime, err := parseBinanceInt64(row[0])
	if err != nil {
		return nil, fmt.Errorf("parse open time: %w", err)
	}
	open, err := parseBinanceFloat64(row[1])
	if err != nil {
		return nil, fmt.Errorf("parse open: %w", err)
	}
	high, err := parseBinanceFloat64(row[2])
	if err != nil {
		return nil, fmt.Errorf("parse high: %w", err)
	}
	low, err := parseBinanceFloat64(row[3])
	if err != nil {
		return nil, fmt.Errorf("parse low: %w", err)
	}
	closePrice, err := parseBinanceFloat64(row[4])
	if err != nil {
		return nil, fmt.Errorf("parse close: %w", err)
	}
	volume, err := parseBinanceFloat64(row[5])
	if err != nil {
		return nil, fmt.Errorf("parse volume: %w", err)
	}
	closeTime, err := parseBinanceInt64(row[6])
	if err != nil {
		return nil, fmt.Errorf("parse close time: %w", err)
	}
	quoteVolume, err := parseBinanceFloat64(row[7])
	if err != nil {
		return nil, fmt.Errorf("parse quote volume: %w", err)
	}
	numTrades, err := parseBinanceInt32(row[8])
	if err != nil {
		return nil, fmt.Errorf("parse num trades: %w", err)
	}
	takerBuyVolume, err := parseBinanceFloat64(row[9])
	if err != nil {
		return nil, fmt.Errorf("parse taker buy volume: %w", err)
	}
	takerBuyQuote, err := parseBinanceFloat64(row[10])
	if err != nil {
		return nil, fmt.Errorf("parse taker buy quote: %w", err)
	}

	return &market.Kline{
		Symbol:         strings.ToUpper(strings.TrimSpace(symbol)),
		Interval:       "1m",
		OpenTime:       openTime,
		CloseTime:      closeTime,
		Open:           open,
		High:           high,
		Low:            low,
		Close:          closePrice,
		Volume:         volume,
		QuoteVolume:    quoteVolume,
		TakerBuyVolume: takerBuyVolume,
		TakerBuyQuote:  takerBuyQuote,
		NumTrades:      numTrades,
		IsClosed:       true,
	}, nil
}

// replayRecoveryKlines 按 symbol 顺序把缺失 K 线阻塞回放进主聚合器。
func replayRecoveryKlines(ctx context.Context, agg *aggregator.KlineAggregator, symbol string, klines []*market.Kline, warmupWaitTimeout time.Duration) error {
	if agg == nil || len(klines) == 0 {
		return nil
	}
	expectedOpenTime := klines[0].OpenTime
	currentOpenTime := klines[len(klines)-1].OpenTime
	missingCount := len(klines)

	firstCtx := ctx
	if firstCtx == nil {
		firstCtx = context.Background()
	}
	log.Printf("[market recovery] step=replay_first_kline status=begin symbol=%s expected_open=%s current_open=%s missing_count=%d open=%s",
		symbol,
		formatRecoveryTime(expectedOpenTime),
		formatRecoveryTime(currentOpenTime),
		missingCount,
		formatRecoveryTime(klines[0].OpenTime))
	agg.ReplayKline(firstCtx, klines[0])
	log.Printf("[market recovery] step=replay_first_kline status=done symbol=%s expected_open=%s current_open=%s missing_count=%d open=%s",
		symbol,
		formatRecoveryTime(expectedOpenTime),
		formatRecoveryTime(currentOpenTime),
		missingCount,
		formatRecoveryTime(klines[0].OpenTime))
	log.Printf("[market recovery] step=wait_warmup status=begin symbol=%s timeout=%s",
		symbol, warmupWaitTimeout)
	waitStartedAt := time.Now()
	if err := waitRecoveryWarmupReady(ctx, agg, symbol, warmupWaitTimeout); err != nil {
		return err
	}
	log.Printf("[market recovery] step=wait_warmup status=done symbol=%s elapsed=%s",
		symbol, time.Since(waitStartedAt).Round(time.Millisecond))

	for index, kline := range klines[1:] {
		agg.ReplayKline(firstCtx, kline)
		if (index+2)%500 == 0 || index == len(klines)-2 {
			log.Printf("[market recovery] step=replay_progress status=running symbol=%s expected_open=%s current_open=%s missing_count=%d replayed=%d total=%d progress_open=%s",
				symbol,
				formatRecoveryTime(expectedOpenTime),
				formatRecoveryTime(currentOpenTime),
				missingCount,
				index+2,
				len(klines),
				formatRecoveryTime(kline.OpenTime))
		}
	}
	return nil
}

// recoveryWindowCount 返回指定 1m 补数窗口理论上包含的 K 线根数，便于启动补数日志与运行时 gap 日志统一口径。
func recoveryWindowCount(startOpenTime, endOpenTime int64) int {
	if startOpenTime <= 0 || endOpenTime < startOpenTime {
		return 0
	}
	return int((endOpenTime-startOpenTime)/oneMinuteMillis) + 1
}

// waitRecoveryWarmupReady 等待指定交易对完成 warmup，避免补数期间 pending 队列溢出。
func waitRecoveryWarmupReady(ctx context.Context, agg *aggregator.KlineAggregator, symbol string, timeout time.Duration) error {
	if agg == nil || symbol == "" {
		return nil
	}
	if timeout <= 0 {
		timeout = 2 * time.Minute
	}

	waitCtx := ctx
	if waitCtx == nil {
		waitCtx = context.Background()
	}
	waitCtx, cancel := context.WithTimeout(waitCtx, timeout)
	defer cancel()

	ticker := time.NewTicker(200 * time.Millisecond)
	defer ticker.Stop()
	progressTicker := time.NewTicker(5 * time.Second)
	defer progressTicker.Stop()

	for {
		status := agg.GetWarmupStatus(symbol)
		if status.Ready {
			return nil
		}
		select {
		case <-waitCtx.Done():
			return fmt.Errorf("wait warmup ready timeout symbol=%s reason=%s", symbol, status.LastIncompleteReason)
		case <-ticker.C:
		case <-progressTicker.C:
			log.Printf("[market recovery] step=wait_warmup status=pending symbol=%s reason=%s has1m=%v has15m=%v has1h=%v has4h=%v indicatorsReady=%v",
				symbol, status.LastIncompleteReason, status.HasEnough1mBars, status.Has15mReady,
				status.Has1hReady, status.Has4hReady, status.IndicatorsReady)
		}
	}
}

// normalizeRecoverySymbols 归一化配置里的交易对列表，便于断点查询与补数复用。
func normalizeRecoverySymbols(symbols []string) []string {
	result := make([]string, 0, len(symbols))
	seen := make(map[string]struct{}, len(symbols))
	for _, symbol := range symbols {
		normalized := strings.ToUpper(strings.TrimSpace(symbol))
		if normalized == "" {
			continue
		}
		if _, ok := seen[normalized]; ok {
			continue
		}
		seen[normalized] = struct{}{}
		result = append(result, normalized)
	}
	return result
}

// calcLatestClosed1mOpenTime 计算当前时刻之前最近一根已闭合 1m K 线的开盘时间。
func calcLatestClosed1mOpenTime(now time.Time) int64 {
	utcNow := now.UTC()
	currentMinuteOpen := utcNow.Truncate(time.Minute)
	latestClosedOpen := currentMinuteOpen.Add(-time.Minute)
	if latestClosedOpen.Before(time.Unix(0, 0).UTC()) {
		return 0
	}
	return latestClosedOpen.UnixMilli()
}

// calcRecoveryStartOpenTime 根据重叠窗口回退恢复起点，便于先删后补。
func calcRecoveryStartOpenTime(lastOpenTime int64, overlapWindow time.Duration) int64 {
	if lastOpenTime <= 0 {
		return 0
	}
	if overlapWindow <= 0 {
		return lastOpenTime
	}
	start := time.UnixMilli(lastOpenTime).UTC().Add(-overlapWindow).Truncate(time.Minute).UnixMilli()
	if start <= 0 {
		return 0
	}
	return start
}

// buildClickHouseStringList 构造 ClickHouse IN 查询所需的字符串字面量列表。
func buildClickHouseStringList(values []string) string {
	quoted := make([]string, 0, len(values))
	for _, value := range values {
		trimmed := strings.TrimSpace(value)
		if trimmed == "" {
			continue
		}
		quoted = append(quoted, quoteClickHouseString(trimmed))
	}
	return strings.Join(quoted, ", ")
}

// quoteClickHouseString 按 ClickHouse 规则转义单引号字符串字面量。
func quoteClickHouseString(value string) string {
	return "'" + strings.ReplaceAll(value, "'", "\\'") + "'"
}

// parseBinanceFloat64 解析 Binance 返回的字符串或数字浮点值。
func parseBinanceFloat64(value interface{}) (float64, error) {
	switch v := value.(type) {
	case string:
		return strconv.ParseFloat(v, 64)
	case json.Number:
		return v.Float64()
	case float64:
		return v, nil
	case int64:
		return float64(v), nil
	case int:
		return float64(v), nil
	default:
		return 0, fmt.Errorf("unsupported float type %T", value)
	}
}

// parseBinanceInt64 解析 Binance 返回的字符串或数字整型值。
func parseBinanceInt64(value interface{}) (int64, error) {
	switch v := value.(type) {
	case string:
		return strconv.ParseInt(v, 10, 64)
	case json.Number:
		return v.Int64()
	case float64:
		return int64(v), nil
	case int64:
		return v, nil
	case int:
		return int64(v), nil
	default:
		return 0, fmt.Errorf("unsupported int type %T", value)
	}
}

// parseBinanceInt32 解析 Binance 返回的整型值并转换为 int32。
func parseBinanceInt32(value interface{}) (int32, error) {
	parsed, err := parseBinanceInt64(value)
	if err != nil {
		return 0, err
	}
	return int32(parsed), nil
}

// parseJSONInt64 解析 JSON 字段中的 int64，兼容字符串和数字两种编码形式。
func parseJSONInt64(value json.RawMessage) (int64, error) {
	trimmed := bytes.TrimSpace(value)
	if len(trimmed) == 0 || bytes.Equal(trimmed, []byte("null")) {
		return 0, nil
	}

	var asInt int64
	if err := json.Unmarshal(trimmed, &asInt); err == nil {
		return asInt, nil
	}

	var asString string
	if err := json.Unmarshal(trimmed, &asString); err == nil {
		return strconv.ParseInt(strings.TrimSpace(asString), 10, 64)
	}

	return 0, fmt.Errorf("unsupported json int64 payload: %s", string(trimmed))
}

// formatRecoveryTime 把毫秒时间戳转换为 UTC 字符串，便于恢复日志排查。
func formatRecoveryTime(ms int64) string {
	if ms <= 0 {
		return "n/a"
	}
	return time.UnixMilli(ms).UTC().Format("2006-01-02 15:04:05")
}
