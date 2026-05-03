package svc

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"hash/fnv"
	"log"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"exchange-system/app/order/rpc/internal/config"
	orderkafka "exchange-system/app/order/rpc/internal/kafka"
	"exchange-system/common/binance"
	commonkafka "exchange-system/common/kafka"
	orderpb "exchange-system/common/pb/order"

	_ "github.com/lib/pq"
)

const defaultAllOrdersLookback = 7 * 24 * time.Hour

// ---------------------------------------------------------------------------
// ServiceContext — Order 微服务核心
//
// 职责：
//   1. 封装币安合约 API 客户端
//   2. 提供6种合约数据查询能力
//   3. 数据保存为 JSONL 到 data 目录
// ---------------------------------------------------------------------------

// ServiceContext Order 服务上下文
type ServiceContext struct {
	Config        config.Config
	client        *binance.Client
	dataDir       string
	postgresDB    *sql.DB
	orderConsumer *orderkafka.Consumer
	cancel        context.CancelFunc
	feeSummaryMu  sync.RWMutex
	feeSummaries  map[string]map[int64]tradeFeeSummary
}

type refreshSummary struct {
	Symbol         string
	PositionsPath  string
	PositionsCount int
	AllOrdersPath  string
	AllOrdersCount int
	NoOpenPosition bool
}

type tradeFeeSummary struct {
	ActualFee      float64
	EstimatedFee   float64
	ActualFeeAsset string
	MixedAsset     bool
}

type OrderLifecycleInfo struct {
	ActionType      string
	PositionCycleID string
}

type executionOrderMeta struct {
	SignalType                  string
	HarvestPathProbability      float64
	HarvestPathRuleProbability  float64
	HarvestPathLSTMProbability  float64
	HarvestPathBookProbability  float64
	HarvestPathBookSummary      string
	HarvestPathVolatilityRegime string
	HarvestPathThresholdSource  string
	HarvestPathAppliedThreshold float64
	HarvestPathAction           string
	HarvestPathRiskLevel        string
	HarvestPathTargetSide       string
	HarvestPathReferencePrice   float64
	HarvestPathMarketPrice      float64
	Reason                      string
	SignalReason                *orderpb.SignalReason
	Protection                  *orderpb.ProtectionStatus
}

// NewServiceContext 创建 Order 服务上下文
func NewServiceContext(c config.Config) (*ServiceContext, error) {
	ctx, cancel := context.WithCancel(context.Background())
	client := binance.NewClient(
		c.Binance.BaseURL,
		c.Binance.APIKey,
		c.Binance.SecretKey,
		c.Binance.Proxy,
	)

	dataDir := c.DataDir
	if dataDir == "" {
		dataDir = "data/futures"
	}

	// 确保数据目录存在
	if err := os.MkdirAll(dataDir, 0o755); err != nil {
		cancel()
		return nil, fmt.Errorf("create data dir %s: %w", dataDir, err)
	}

	log.Printf("[Order服务] 初始化完成 | 端点=%s 数据目录=%s", c.Binance.BaseURL, dataDir)
	defaultStartTime := defaultAllOrdersStartTime()
	log.Printf("[Order服务] GetAllOrders 查询上下文 | baseURL=%s defaultSymbol=%s startTime=%d(%s) endTime=0(no-limit)",
		c.Binance.BaseURL, "ETHUSDT", defaultStartTime, formatMillis(defaultStartTime))

	postgresDB, err := openPostgresDB(ctx, c.Postgres)
	if err != nil {
		cancel()
		return nil, err
	}

	svcCtx := &ServiceContext{
		Config:       c,
		client:       client,
		dataDir:      dataDir,
		postgresDB:   postgresDB,
		cancel:       cancel,
		feeSummaries: make(map[string]map[int64]tradeFeeSummary),
	}

	orderTopic := c.Kafka.Topics.Order
	if len(c.Kafka.Addrs) > 0 && strings.TrimSpace(orderTopic) != "" {
		groupID := c.Kafka.Group
		if groupID == "" {
			if c.Name != "" {
				groupID = c.Name + "-order"
			} else {
				groupID = "order-rpc-order"
			}
		}

		consumer, err := orderkafka.NewConsumer(c.Kafka.Addrs, groupID, orderTopic)
		if err != nil {
			if postgresDB != nil {
				_ = postgresDB.Close()
			}
			cancel()
			return nil, fmt.Errorf("init order consumer: %w", err)
		}
		svcCtx.orderConsumer = consumer
		if err := consumer.StartConsuming(ctx, svcCtx.handleOrderEvent); err != nil {
			_ = consumer.Close()
			if postgresDB != nil {
				_ = postgresDB.Close()
			}
			cancel()
			return nil, fmt.Errorf("start order consumer: %w", err)
		}
		commonkafka.StartConsumerGroupLagReporter(ctx, c.Kafka.Addrs, groupID, orderTopic, 30*time.Second)
		log.Printf("[Order服务] Kafka order consumer started | group=%s topic=%s brokers=%v", groupID, orderTopic, c.Kafka.Addrs)
	}

	return svcCtx, nil
}

// GetClient 获取币安合约 API 客户端
func (s *ServiceContext) GetClient() *binance.Client {
	return s.client
}

func (s *ServiceContext) handleOrderEvent(event *orderkafka.OrderEvent) error {
	if s == nil || event == nil {
		return nil
	}
	if !event.HasExecution() {
		return nil
	}
	symbol := strings.ToUpper(strings.TrimSpace(event.Symbol))
	if symbol == "" {
		return nil
	}

	log.Printf("[Order服务] 收到成交订单事件 -> 开始刷新 | symbol=%s order_id=%s client_id=%s signal_type=%s side=%s position_side=%s status=%s qty=%.6f avg_price=%.6f",
		symbol, event.OrderID, event.ClientID, event.SignalType, event.Side, event.PositionSide, event.Status, event.Quantity, event.AvgPrice)

	refreshCtx, cancel := context.WithTimeout(context.Background(), 8*time.Second)
	defer cancel()

	summary, err := s.refreshSymbolDataDetailed(refreshCtx, symbol)
	if err != nil {
		return err
	}
	positionMsg := fmt.Sprintf("positions=%s(%d)", summary.PositionsPath, summary.PositionsCount)
	if summary.NoOpenPosition {
		positionMsg = fmt.Sprintf("positions=no-open-position(%s)", summary.PositionsPath)
	}
	log.Printf("[Order服务] 成交订单事件刷新完成 | symbol=%s order_id=%s -> %s, all_orders=%s(%d)",
		symbol, event.OrderID, positionMsg, summary.AllOrdersPath, summary.AllOrdersCount)
	return nil
}

func (s *ServiceContext) RefreshSymbolData(ctx context.Context, symbol string) error {
	_, err := s.refreshSymbolDataDetailed(ctx, symbol)
	return err
}

func (s *ServiceContext) refreshSymbolDataDetailed(ctx context.Context, symbol string) (refreshSummary, error) {
	if s == nil || strings.TrimSpace(symbol) == "" {
		return refreshSummary{}, nil
	}
	symbol = strings.ToUpper(strings.TrimSpace(symbol))
	summary := refreshSummary{
		Symbol:        symbol,
		PositionsPath: s.categoryJSONLPath("positions", symbol),
		AllOrdersPath: s.categoryJSONLPath("all_orders", symbol),
	}

	posCount, posErr := s.refreshPositions(ctx, symbol)
	summary.PositionsCount = posCount
	summary.NoOpenPosition = posCount == 0
	if posErr != nil {
		log.Printf("[Order服务] 刷新当前仓位失败 | symbol=%s err=%v", symbol, posErr)
	} else {
		log.Printf("[Order服务] 当前仓位已刷新 | symbol=%s file=%s rows=%d", symbol, summary.PositionsPath, posCount)
	}

	orders, err := s.GetAllOrders(ctx, symbol, 0, 0, 1000)
	if err != nil {
		return summary, fmt.Errorf("refresh all orders for %s: %w", symbol, err)
	}
	summary.AllOrdersCount = len(orders)
	if posErr != nil {
		return summary, posErr
	}
	return summary, nil
}

func (s *ServiceContext) refreshPositions(ctx context.Context, symbol string) (int, error) {
	account, err := s.client.GetAccountInfo(ctx)
	if err != nil {
		return 0, err
	}

	items := make([]positionJSONLEntry, 0, len(account.Positions))
	symbol = strings.ToUpper(strings.TrimSpace(symbol))
	for _, p := range account.Positions {
		if strings.ToUpper(strings.TrimSpace(p.Symbol)) != symbol {
			continue
		}
		if parseFloatString(p.PositionAmt) == 0 {
			continue
		}
		items = append(items, s.toPositionJSONLEntry(p))
	}

	if len(items) == 0 {
		if err := s.removeCategoryJSONL("positions", symbol); err != nil && !os.IsNotExist(err) {
			return 0, err
		}
		return 0, nil
	}
	if err := s.writeJSONL("positions", symbol, items); err != nil {
		return 0, err
	}
	return len(items), nil
}

// ---------------------------------------------------------------------------
// 合约数据查询方法
// ---------------------------------------------------------------------------

// GetOpenOrders 查询当前委托（未成交订单）
func (s *ServiceContext) GetOpenOrders(ctx context.Context, symbol string) ([]binance.OpenOrder, error) {
	return s.client.GetOpenOrders(ctx, symbol)
}

// GetAllOrders 查询历史委托
func (s *ServiceContext) GetAllOrders(ctx context.Context, symbol string, startTime, endTime int64, limit int) ([]binance.AllOrder, error) {
	startTime = normalizeAllOrdersStartTime(startTime)
	log.Printf("[Order服务] GetAllOrders 请求 | baseURL=%s symbol=%s startTime=%d(%s) endTime=%d(%s) limit=%d",
		s.Config.Binance.BaseURL, symbol, startTime, formatMillis(startTime), endTime, formatMillis(endTime), limit)
	orders, apiErr := s.client.GetAllOrders(ctx, symbol, startTime, endTime, limit)
	if trades, tradeErr := s.GetUserTrades(ctx, symbol, startTime, endTime, maxInt(limit, 1000)); tradeErr == nil {
		s.cacheTradeFeeSummaries(symbol, trades)
	} else {
		log.Printf("[Order服务] 预取手续费汇总失败 | symbol=%s err=%v", symbol, tradeErr)
	}
	if apiErr != nil {
		return nil, apiErr
	}
	sort.Slice(orders, func(i, j int) bool {
		if orders[i].UpdateTime == orders[j].UpdateTime {
			return orders[i].OrderID > orders[j].OrderID
		}
		return orders[i].UpdateTime > orders[j].UpdateTime
	})
	if limit > 0 && len(orders) > limit {
		orders = orders[:limit]
	}
	if err := s.writeJSONL("all_orders", symbol, orders); err != nil {
		log.Printf("[Order服务] 写入历史委托失败: %v", err)
	}
	return orders, nil
}

// GetUserTrades 查询历史成交
func (s *ServiceContext) GetUserTrades(ctx context.Context, symbol string, startTime, endTime int64, limit int) ([]binance.UserTrade, error) {
	trades, apiErr := s.client.GetUserTrades(ctx, symbol, startTime, endTime, limit)
	if apiErr != nil {
		return nil, apiErr
	}
	sort.Slice(trades, func(i, j int) bool {
		if trades[i].Time == trades[j].Time {
			return trades[i].ID > trades[j].ID
		}
		return trades[i].Time > trades[j].Time
	})
	if limit > 0 && len(trades) > limit {
		trades = trades[:limit]
	}
	return trades, nil
}

// GetIncomeHistory 查询资金流水
func (s *ServiceContext) GetIncomeHistory(ctx context.Context, symbol, incomeType string, startTime, endTime int64, limit int) ([]binance.Income, error) {
	return s.client.GetIncomeHistory(ctx, symbol, incomeType, startTime, endTime, limit)
}

// GetFundingRateHistory 查询资金费率历史
func (s *ServiceContext) GetFundingRateHistory(ctx context.Context, symbol string, startTime, endTime int64, limit int) ([]binance.FundingRate, error) {
	return s.client.GetFundingRate(ctx, symbol, startTime, endTime, limit)
}

// GetFundingFees 查询资金费用
func (s *ServiceContext) GetFundingFees(ctx context.Context, symbol string, startTime, endTime int64, limit int) ([]binance.FundingFee, error) {
	return s.client.GetFundingFees(ctx, symbol, startTime, endTime, limit)
}

// ---------------------------------------------------------------------------
// JSONL 数据保存
// ---------------------------------------------------------------------------

// writeJSONL 将订单查询结果按“每日快照”写入 JSONL 文件。
// 目录结构：{dataDir}/{category}/{symbol}/2026-04-17.jsonl
// 同一 category/symbol/date 会被覆盖，以避免查询或刷新时不断追加重复数据。
func (s *ServiceContext) writeJSONL(category, symbol string, items interface{}) error {
	dateStr := time.Now().UTC().Format("2006-01-02")
	dir := filepath.Join(s.dataDir, category, symbol)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return fmt.Errorf("mkdir %s: %w", dir, err)
	}

	filePath := filepath.Join(dir, dateStr+".jsonl")
	file, err := os.OpenFile(filePath, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0o644)
	if err != nil {
		return fmt.Errorf("open file %s: %w", filePath, err)
	}
	defer file.Close()

	enc := json.NewEncoder(file)
	enc.SetEscapeHTML(false)

	switch v := items.(type) {
	case []binance.OpenOrder:
		for _, item := range v {
			if err := enc.Encode(item); err != nil {
				return err
			}
		}
	case []binance.AllOrder:
		lifecycleMap := s.BuildOrderLifecycleMap(v)
		for _, item := range v {
			entry := s.toAllOrderJSONLEntry(item, lifecycleMap[item.OrderID])
			if err := enc.Encode(entry); err != nil {
				return err
			}
		}
	case []positionJSONLEntry:
		for _, item := range v {
			if err := enc.Encode(item); err != nil {
				return err
			}
		}
	case []binance.UserTrade:
		for _, item := range v {
			if err := enc.Encode(s.toUserTradeJSONLEntry(item)); err != nil {
				return err
			}
		}
	case []binance.Income:
		for _, item := range v {
			if err := enc.Encode(item); err != nil {
				return err
			}
		}
	case []binance.FundingRate:
		for _, item := range v {
			if err := enc.Encode(item); err != nil {
				return err
			}
		}
	case []binance.FundingFee:
		for _, item := range v {
			if err := enc.Encode(item); err != nil {
				return err
			}
		}
	}

	if err := s.persistCategorySnapshot(category, symbol, items); err != nil {
		log.Printf("[Order服务] PostgreSQL落库失败 | category=%s symbol=%s err=%v", category, symbol, err)
	}

	return nil
}

// openPostgresDB 按配置初始化 PostgreSQL 连接池。
func openPostgresDB(ctx context.Context, cfg config.PostgresConfig) (*sql.DB, error) {
	if !cfg.Enabled {
		return nil, nil
	}
	if strings.TrimSpace(cfg.DSN) == "" {
		return nil, fmt.Errorf("postgres dsn is required when postgres is enabled")
	}
	if strings.TrimSpace(cfg.AccountID) == "" {
		return nil, fmt.Errorf("postgres account_id is required when postgres is enabled")
	}
	db, err := sql.Open("postgres", cfg.DSN)
	if err != nil {
		return nil, fmt.Errorf("open postgres: %w", err)
	}
	if cfg.MaxOpenConns > 0 {
		db.SetMaxOpenConns(cfg.MaxOpenConns)
	}
	if cfg.MaxIdleConns > 0 {
		db.SetMaxIdleConns(cfg.MaxIdleConns)
	}
	if cfg.ConnMaxLifetime > 0 {
		db.SetConnMaxLifetime(cfg.ConnMaxLifetime)
	}
	pingCtx, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer cancel()
	if err := db.PingContext(pingCtx); err != nil {
		_ = db.Close()
		return nil, fmt.Errorf("ping postgres: %w", err)
	}
	log.Printf("[Order服务] PostgreSQL连接成功 | account_id=%s", strings.TrimSpace(cfg.AccountID))
	return db, nil
}

// persistCategorySnapshot 根据分类把快照同步写入 PostgreSQL。
func (s *ServiceContext) persistCategorySnapshot(category, symbol string, items interface{}) error {
	if s == nil || s.postgresDB == nil {
		return nil
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	switch v := items.(type) {
	case []binance.AllOrder:
		return s.persistAllOrdersSnapshot(ctx, v)
	case []positionJSONLEntry:
		return s.persistPositionsSnapshot(ctx, symbol, v)
	case []binance.UserTrade:
		return s.persistTradesSnapshot(ctx, v)
	default:
		return nil
	}
}

// persistAllOrdersSnapshot 把历史委托快照 upsert 到 exchange_core.orders。
func (s *ServiceContext) persistAllOrdersSnapshot(ctx context.Context, orders []binance.AllOrder) error {
	if len(orders) == 0 {
		return nil
	}
	tx, err := s.postgresDB.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer func() {
		_ = tx.Rollback()
	}()

	lifecycleMap := s.BuildOrderLifecycleMap(orders)
	const query = `
INSERT INTO exchange_core.orders
	(account_id, symbol, order_id, client_order_id, strategy_id, position_cycle_id, side, type, price, quantity, executed_quantity, status, reduce_only, created_at, updated_at)
VALUES
	($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15)
ON CONFLICT (account_id, order_id) DO UPDATE SET
	symbol = EXCLUDED.symbol,
	client_order_id = EXCLUDED.client_order_id,
	strategy_id = EXCLUDED.strategy_id,
	position_cycle_id = EXCLUDED.position_cycle_id,
	side = EXCLUDED.side,
	type = EXCLUDED.type,
	price = EXCLUDED.price,
	quantity = EXCLUDED.quantity,
	executed_quantity = EXCLUDED.executed_quantity,
	status = EXCLUDED.status,
	reduce_only = EXCLUDED.reduce_only,
	updated_at = EXCLUDED.updated_at`
	accountID := strings.TrimSpace(s.Config.Postgres.AccountID)
	for _, item := range orders {
		lifecycle := lifecycleMap[item.OrderID]
		if _, err := tx.ExecContext(
			ctx,
			query,
			accountID,
			strings.ToUpper(strings.TrimSpace(item.Symbol)),
			strconv.FormatInt(item.OrderID, 10),
			normalizeClientOrderID(item.ClientOrderID, item.OrderID),
			"",
			lifecycle.PositionCycleID,
			strings.ToUpper(strings.TrimSpace(item.Side)),
			strings.ToUpper(strings.TrimSpace(item.Type)),
			normalizeNumericString(item.Price),
			normalizeNumericString(item.OrigQty),
			normalizeNumericString(item.ExecutedQty),
			strings.ToUpper(strings.TrimSpace(item.Status)),
			item.ReduceOnly,
			millisToTime(item.Time),
			millisToTime(maxInt64(item.UpdateTime, item.Time)),
		); err != nil {
			return err
		}
	}
	return tx.Commit()
}

// persistPositionsSnapshot 用最新持仓快照覆盖指定 symbol 的 PostgreSQL 记录。
func (s *ServiceContext) persistPositionsSnapshot(ctx context.Context, symbol string, items []positionJSONLEntry) error {
	symbol = strings.ToUpper(strings.TrimSpace(symbol))
	if symbol == "" {
		return nil
	}
	tx, err := s.postgresDB.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer func() {
		_ = tx.Rollback()
	}()

	accountID := strings.TrimSpace(s.Config.Postgres.AccountID)
	if _, err := tx.ExecContext(ctx, `DELETE FROM exchange_core.positions WHERE account_id = $1 AND symbol = $2`, accountID, symbol); err != nil {
		return err
	}
	if len(items) == 0 {
		return tx.Commit()
	}
	const query = `
INSERT INTO exchange_core.positions
	(account_id, symbol, position_side, position_amt, entry_price, mark_price, unrealized_pnl, leverage, updated_at)
VALUES
	($1, $2, $3, $4, $5, $6, $7, $8, $9)
ON CONFLICT (account_id, symbol, position_side) DO UPDATE SET
	position_amt = EXCLUDED.position_amt,
	entry_price = EXCLUDED.entry_price,
	mark_price = EXCLUDED.mark_price,
	unrealized_pnl = EXCLUDED.unrealized_pnl,
	leverage = EXCLUDED.leverage,
	updated_at = EXCLUDED.updated_at`
	for _, item := range items {
		if _, err := tx.ExecContext(
			ctx,
			query,
			accountID,
			symbol,
			strings.ToUpper(strings.TrimSpace(item.PositionSide)),
			normalizeNumericString(item.PositionAmt),
			normalizeNumericString(item.EntryPrice),
			normalizeNumericString(item.MarkPrice),
			normalizeNumericString(item.UnrealizedProfit),
			normalizeNumericString(item.Leverage),
			millisToTime(item.SnapshotTimestamp),
		); err != nil {
			return err
		}
	}
	return tx.Commit()
}

// persistTradesSnapshot 把历史成交快照 upsert 到 exchange_core.trades。
func (s *ServiceContext) persistTradesSnapshot(ctx context.Context, trades []binance.UserTrade) error {
	if len(trades) == 0 {
		return nil
	}
	tx, err := s.postgresDB.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer func() {
		_ = tx.Rollback()
	}()

	const query = `
INSERT INTO exchange_core.trades
	(account_id, symbol, trade_id, order_id, price, qty, fee, fee_asset, realized_pnl, trade_time)
VALUES
	($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
ON CONFLICT (account_id, trade_id) DO UPDATE SET
	symbol = EXCLUDED.symbol,
	order_id = EXCLUDED.order_id,
	price = EXCLUDED.price,
	qty = EXCLUDED.qty,
	fee = EXCLUDED.fee,
	fee_asset = EXCLUDED.fee_asset,
	realized_pnl = EXCLUDED.realized_pnl,
	trade_time = EXCLUDED.trade_time`
	accountID := strings.TrimSpace(s.Config.Postgres.AccountID)
	for _, item := range trades {
		if _, err := tx.ExecContext(
			ctx,
			query,
			accountID,
			strings.ToUpper(strings.TrimSpace(item.Symbol)),
			strconv.FormatInt(item.ID, 10),
			strconv.FormatInt(item.OrderID, 10),
			normalizeNumericString(item.Price),
			normalizeNumericString(item.Qty),
			normalizeNumericString(item.Commission),
			strings.TrimSpace(item.CommissionAsset),
			normalizeNumericString(item.RealizedPnl),
			millisToTime(item.Time),
		); err != nil {
			return err
		}
	}
	return tx.Commit()
}

// normalizeClientOrderID 为缺失的 client_order_id 生成稳定兜底值，避免唯一键冲突。
func normalizeClientOrderID(clientOrderID string, orderID int64) string {
	clientOrderID = strings.TrimSpace(clientOrderID)
	if clientOrderID != "" {
		return clientOrderID
	}
	if orderID <= 0 {
		return "unknown-client-order-id"
	}
	return "order-" + strconv.FormatInt(orderID, 10)
}

// normalizeNumericString 把空数字段转换成 0，便于写入 numeric 列。
func normalizeNumericString(v string) string {
	v = strings.TrimSpace(v)
	if v == "" {
		return "0"
	}
	return v
}

// millisToTime 把毫秒时间戳转换为 UTC 时间。
func millisToTime(ms int64) time.Time {
	if ms <= 0 {
		return time.UnixMilli(0).UTC()
	}
	return time.UnixMilli(ms).UTC()
}

// maxInt64 返回两个 int64 中的较大值。
func maxInt64(a, b int64) int64 {
	if a >= b {
		return a
	}
	return b
}

func (s *ServiceContext) removeCategoryJSONL(category, symbol string) error {
	filePath := s.categoryJSONLPath(category, symbol)
	if err := os.Remove(filePath); err != nil { // ignore_security_alert
		return err
	}
	return nil
}

func (s *ServiceContext) categoryJSONLPath(category, symbol string) string {
	dateStr := time.Now().UTC().Format("2006-01-02")
	return filepath.Join(s.dataDir, category, symbol, dateStr+".jsonl")
}

type executionOrderLogEntry struct {
	Timestamp                   string                `json:"timestamp"`
	SignalType                  string                `json:"signal_type"`
	StrategyID                  string                `json:"strategy_id"`
	Symbol                      string                `json:"symbol"`
	OrderID                     string                `json:"order_id"`
	ClientID                    string                `json:"client_id"`
	Side                        string                `json:"side"`
	PositionSide                string                `json:"position_side"`
	Type                        string                `json:"type"`
	Status                      string                `json:"status"`
	Quantity                    float64               `json:"quantity"`
	ExecutedQty                 float64               `json:"executed_qty"`
	AvgPrice                    float64               `json:"avg_price"`
	Commission                  float64               `json:"commission"`
	CommissionAsset             string                `json:"commission_asset"`
	Slippage                    float64               `json:"slippage"`
	StopLoss                    float64               `json:"stop_loss"`
	Atr                         float64               `json:"atr"`
	RiskReward                  float64               `json:"risk_reward"`
	Reason                      string                `json:"reason"`
	SignalReason                *signalReasonJSON     `json:"signal_reason,omitempty"`
	Protection                  *protectionStatusJSON `json:"protection,omitempty"`
	TransactTime                int64                 `json:"-"`
	HarvestPathProbability      float64               `json:"harvest_path_probability"`
	HarvestPathRuleProbability  float64               `json:"harvest_path_rule_probability"`
	HarvestPathLSTMProbability  float64               `json:"harvest_path_lstm_probability"`
	HarvestPathBookProbability  float64               `json:"harvest_path_book_probability,omitempty"`
	HarvestPathBookSummary      string                `json:"harvest_path_book_summary,omitempty"`
	HarvestPathVolatilityRegime string                `json:"harvest_path_volatility_regime,omitempty"`
	HarvestPathThresholdSource  string                `json:"harvest_path_threshold_source,omitempty"`
	HarvestPathAppliedThreshold float64               `json:"harvest_path_applied_threshold,omitempty"`
	HarvestPathAction           string                `json:"harvest_path_action"`
	HarvestPathRiskLevel        string                `json:"harvest_path_risk_level"`
	HarvestPathTargetSide       string                `json:"harvest_path_target_side"`
	HarvestPathReferencePrice   float64               `json:"harvest_path_reference_price"`
	HarvestPathMarketPrice      float64               `json:"harvest_path_market_price"`
}

type allOrderJSONLEntry struct {
	Time                        string                `json:"time"`
	TimeLocal                   string                `json:"time_local"`
	OrderID                     int64                 `json:"order_id"`
	Symbol                      string                `json:"symbol"`
	Status                      string                `json:"status"`
	Side                        string                `json:"side"`
	PositionSide                string                `json:"position_side"`
	Type                        string                `json:"type"`
	OrigQty                     string                `json:"orig_qty"`
	ExecutedQty                 string                `json:"executed_qty"`
	BaseQty                     string                `json:"base_qty"`
	QuoteQty                    string                `json:"quote_qty"`
	AvgPrice                    string                `json:"avg_price"`
	Price                       string                `json:"price"`
	StopPrice                   string                `json:"stop_price"`
	ClientOrderID               string                `json:"client_order_id"`
	EstimatedFee                string                `json:"estimated_fee"`
	ActualFee                   string                `json:"actual_fee"`
	ActualFeeAsset              string                `json:"actual_fee_asset"`
	ActionType                  string                `json:"action_type"`
	PositionCycleID             string                `json:"position_cycle_id"`
	HarvestPathProbability      string                `json:"harvest_path_probability,omitempty"`
	HarvestPathRuleProbability  string                `json:"harvest_path_rule_probability,omitempty"`
	HarvestPathLSTMProbability  string                `json:"harvest_path_lstm_probability,omitempty"`
	HarvestPathBookProbability  string                `json:"harvest_path_book_probability,omitempty"`
	HarvestPathBookSummary      string                `json:"harvest_path_book_summary,omitempty"`
	HarvestPathVolatilityRegime string                `json:"harvest_path_volatility_regime,omitempty"`
	HarvestPathThresholdSource  string                `json:"harvest_path_threshold_source,omitempty"`
	HarvestPathAppliedThreshold string                `json:"harvest_path_applied_threshold,omitempty"`
	HarvestPathAction           string                `json:"harvest_path_action,omitempty"`
	HarvestPathRiskLevel        string                `json:"harvest_path_risk_level,omitempty"`
	HarvestPathTargetSide       string                `json:"harvest_path_target_side,omitempty"`
	HarvestPathReferencePrice   string                `json:"harvest_path_reference_price,omitempty"`
	HarvestPathMarketPrice      string                `json:"harvest_path_market_price,omitempty"`
	Reason                      string                `json:"reason,omitempty"`
	SignalReason                *signalReasonJSON     `json:"signal_reason,omitempty"`
	Protection                  *protectionStatusJSON `json:"protection,omitempty"`
	ReduceOnly                  bool                  `json:"reduce_only"`
	ClosePosition               bool                  `json:"close_position"`
	UpdateTime                  string                `json:"update_time"`
	TimeInForce                 string                `json:"time_in_force"`
}

type positionJSONLEntry struct {
	TimeLocal         string `json:"time_local"`
	Symbol            string `json:"symbol"`
	PositionSide      string `json:"position_side"`
	PositionAmt       string `json:"position_amt"`
	BaseQty           string `json:"base_qty"`
	EntryPrice        string `json:"entry_price"`
	MarkPrice         string `json:"mark_price"`
	UnrealizedProfit  string `json:"unrealized_profit"`
	LiquidationPrice  string `json:"liquidation_price"`
	Leverage          string `json:"leverage"`
	MarginType        string `json:"margin_type"`
	TimeUTC           string `json:"time_utc"`
	SnapshotTimestamp int64  `json:"snapshot_timestamp"`
}

type signalReasonJSON struct {
	Summary          string                       `json:"summary,omitempty"`
	Phase            string                       `json:"phase,omitempty"`
	TrendContext     string                       `json:"trend_context,omitempty"`
	SetupContext     string                       `json:"setup_context,omitempty"`
	PathContext      string                       `json:"path_context,omitempty"`
	ExecutionContext string                       `json:"execution_context,omitempty"`
	ExitReasonKind   string                       `json:"exit_reason_kind,omitempty"`
	ExitReasonLabel  string                       `json:"exit_reason_label,omitempty"`
	Tags             []string                     `json:"tags,omitempty"`
	RouteBucket      string                       `json:"route_bucket,omitempty"`
	RouteReason      string                       `json:"route_reason,omitempty"`
	RouteTemplate    string                       `json:"route_template,omitempty"`
	Allocator        *positionAllocatorStatusJSON `json:"allocator,omitempty"`
	Range            *rangeSignalReasonJSON       `json:"range,omitempty"`
}

type rangeSignalReasonJSON struct {
	H1RangeOK      bool `json:"h1_range_ok"`
	H1AdxOK        bool `json:"h1_adx_ok"`
	H1BollWidthOK  bool `json:"h1_boll_width_ok"`
	M15TouchLower  bool `json:"m15_touch_lower"`
	M15RsiTurnUp   bool `json:"m15_rsi_turn_up"`
	M15TouchUpper  bool `json:"m15_touch_upper"`
	M15RsiTurnDown bool `json:"m15_rsi_turn_down"`
}

type positionAllocatorStatusJSON struct {
	Template       string  `json:"template,omitempty"`
	RouteBucket    string  `json:"route_bucket,omitempty"`
	RouteReason    string  `json:"route_reason,omitempty"`
	Score          float64 `json:"score,omitempty"`
	ScoreSource    string  `json:"score_source,omitempty"`
	BucketBudget   float64 `json:"bucket_budget,omitempty"`
	StrategyWeight float64 `json:"strategy_weight"`
	SymbolWeight   float64 `json:"symbol_weight"`
	RiskScale      float64 `json:"risk_scale"`
	PositionBudget float64 `json:"position_budget"`
	TradingPaused  bool    `json:"trading_paused"`
	PauseReason    string  `json:"pause_reason,omitempty"`
}

type protectionStatusJSON struct {
	Requested  bool                     `json:"requested"`
	Status     string                   `json:"status,omitempty"`
	Reason     string                   `json:"reason,omitempty"`
	StopLoss   *protectionLegStatusJSON `json:"stop_loss,omitempty"`
	TakeProfit *protectionLegStatusJSON `json:"take_profit,omitempty"`
}

type protectionLegStatusJSON struct {
	Requested     bool    `json:"requested"`
	Status        string  `json:"status,omitempty"`
	TriggerPrice  float64 `json:"trigger_price,omitempty"`
	Reason        string  `json:"reason,omitempty"`
	OrderID       string  `json:"order_id,omitempty"`
	ClientOrderID string  `json:"client_order_id,omitempty"`
}

type userTradeJSONLEntry struct {
	ID              int64  `json:"id"`
	Symbol          string `json:"symbol"`
	OrderID         int64  `json:"order_id"`
	Side            string `json:"side"`
	PositionSide    string `json:"position_side"`
	Price           string `json:"price"`
	Qty             string `json:"qty"`
	RealizedPnl     string `json:"realized_pnl"`
	MarginAsset     string `json:"margin_asset"`
	QuoteQty        string `json:"quote_qty"`
	Commission      string `json:"commission"`
	CommissionAsset string `json:"commission_asset"`
	EstimatedFee    string `json:"estimated_fee"`
	ActualFee       string `json:"actual_fee"`
	Time            string `json:"time"`
	TimeLocal       string `json:"time_local"`
	Buyer           bool   `json:"buyer"`
	Maker           bool   `json:"maker"`
}

// UnmarshalJSON 兼容 execution 订单日志里字符串和毫秒时间戳两种 transact_time 表达。
func (e *executionOrderLogEntry) UnmarshalJSON(data []byte) error {
	type alias executionOrderLogEntry
	aux := &struct {
		TransactTime any `json:"transact_time"`
		*alias
	}{
		alias: (*alias)(e),
	}
	if err := json.Unmarshal(data, aux); err != nil {
		return err
	}
	e.TransactTime = parseExecutionTransactTime(aux.TransactTime, e.Timestamp)
	return nil
}

// parseExecutionTransactTime 将 execution 订单日志里的成交时间统一转换为毫秒时间戳。
func parseExecutionTransactTime(raw any, fallbackTimestamp string) int64 {
	switch v := raw.(type) {
	case float64:
		return int64(v)
	case string:
		return parseExecutionTransactTimeString(v, fallbackTimestamp)
	default:
		return parseExecutionTransactTimeString("", fallbackTimestamp)
	}
}

// parseExecutionTransactTimeString 解析 execution 日志中的字符串成交时间。
func parseExecutionTransactTimeString(raw string, fallbackTimestamp string) int64 {
	raw = strings.TrimSpace(raw)
	if raw != "" {
		if millis, err := strconv.ParseInt(raw, 10, 64); err == nil {
			return millis
		}
		layouts := []string{
			"2006-01-02 15:04:05",
			time.RFC3339,
			"2006-01-02T15:04:05.000Z",
		}
		for _, layout := range layouts {
			if ts, err := time.Parse(layout, raw); err == nil {
				return ts.UTC().UnixMilli()
			}
		}
	}
	if ts, err := time.Parse(time.RFC3339, strings.TrimSpace(fallbackTimestamp)); err == nil {
		return ts.UTC().UnixMilli()
	}
	if ts, err := time.Parse("2006-01-02T15:04:05.000Z", strings.TrimSpace(fallbackTimestamp)); err == nil {
		return ts.UTC().UnixMilli()
	}
	return 0
}

func (s *ServiceContext) allOrderHarvestPathMeta(order binance.AllOrder) executionOrderMeta {
	_ = s
	_ = order
	return executionOrderMeta{}
}

func stableID(s string) int64 {
	if s == "" {
		return 0
	}
	if n, err := strconv.ParseInt(s, 10, 64); err == nil {
		return n
	}
	var digits strings.Builder
	for _, r := range s {
		if r >= '0' && r <= '9' {
			digits.WriteRune(r)
		}
	}
	if digits.Len() > 0 {
		if n, err := strconv.ParseInt(digits.String(), 10, 64); err == nil {
			return n
		}
	}
	h := fnv.New64a()
	_, _ = h.Write([]byte(s))
	return int64(h.Sum64() & 0x7fffffffffffffff)
}

func formatFloat(v float64) string {
	return strconv.FormatFloat(v, 'f', -1, 64)
}

func formatMillis(ms int64) string {
	if ms <= 0 {
		return "no-limit"
	}
	return time.UnixMilli(ms).UTC().Format(time.RFC3339)
}

func formatOrderTime(ms int64) string {
	if ms <= 0 {
		return ""
	}
	return time.UnixMilli(ms).UTC().Format("2006-01-02 15:04:05")
}

func formatOrderTimeLocal(ms int64) string {
	if ms <= 0 {
		return ""
	}
	loc := orderDisplayLocation()
	return time.UnixMilli(ms).In(loc).Format("2006-01-02 15:04:05")
}

func orderDisplayLocation() *time.Location {
	loc, err := time.LoadLocation("Asia/Shanghai")
	if err == nil {
		return loc
	}
	return time.FixedZone("UTC+8", 8*60*60)
}

func parseFloatString(v string) float64 {
	if strings.TrimSpace(v) == "" {
		return 0
	}
	f, err := strconv.ParseFloat(v, 64)
	if err != nil {
		return 0
	}
	return f
}

func (s *ServiceContext) toAllOrderJSONLEntry(item binance.AllOrder, lifecycle OrderLifecycleInfo) allOrderJSONLEntry {
	pricePrecision, quantityPrecision := s.symbolPrecisions(item.Symbol)
	feeSummary := s.tradeFeeSummary(item.Symbol, item.OrderID)
	harvestPathMeta := s.allOrderHarvestPathMeta(item)
	baseQty := parseFloatString(item.ExecutedQty)
	if baseQty == 0 {
		baseQty = parseFloatString(item.OrigQty)
	}
	quoteQty := parseFloatString(item.AvgPrice) * baseQty
	estimatedFee := feeSummary.EstimatedFee
	if estimatedFee == 0 {
		estimatedFee = quoteQty * estimateFeeRate(item.Type, false)
	}
	actualFeeAsset := feeSummary.ActualFeeAsset
	if feeSummary.MixedAsset {
		actualFeeAsset = "MIXED"
	}

	return allOrderJSONLEntry{
		Time:                        formatOrderTime(item.Time),
		TimeLocal:                   formatOrderTimeLocal(item.Time),
		OrderID:                     item.OrderID,
		Symbol:                      item.Symbol,
		Status:                      item.Status,
		Side:                        item.Side,
		PositionSide:                item.PositionSide,
		Type:                        item.Type,
		OrigQty:                     formatDecimalString(item.OrigQty, quantityPrecision, "0"),
		ExecutedQty:                 formatDecimalString(item.ExecutedQty, quantityPrecision, "0"),
		BaseQty:                     formatFloatWithPrecision(baseQty, quantityPrecision),
		QuoteQty:                    formatFloatWithPrecision(quoteQty, 2),
		AvgPrice:                    formatDecimalString(item.AvgPrice, pricePrecision, "0"),
		Price:                       formatDecimalString(item.Price, pricePrecision, "0"),
		StopPrice:                   formatDecimalString(item.StopPrice, pricePrecision, "0"),
		ClientOrderID:               item.ClientOrderID,
		EstimatedFee:                formatFloatWithPrecision(estimatedFee, 8),
		ActualFee:                   formatFloatWithPrecision(feeSummary.ActualFee, 8),
		ActualFeeAsset:              actualFeeAsset,
		ActionType:                  lifecycle.ActionType,
		PositionCycleID:             lifecycle.PositionCycleID,
		HarvestPathProbability:      formatOptionalFloatWithPrecision(harvestPathMeta.HarvestPathProbability, 4),
		HarvestPathRuleProbability:  formatOptionalFloatWithPrecision(harvestPathMeta.HarvestPathRuleProbability, 4),
		HarvestPathLSTMProbability:  formatOptionalFloatWithPrecision(harvestPathMeta.HarvestPathLSTMProbability, 4),
		HarvestPathBookProbability:  formatOptionalFloatWithPrecision(harvestPathMeta.HarvestPathBookProbability, 4),
		HarvestPathBookSummary:      strings.TrimSpace(harvestPathMeta.HarvestPathBookSummary),
		HarvestPathVolatilityRegime: strings.TrimSpace(harvestPathMeta.HarvestPathVolatilityRegime),
		HarvestPathThresholdSource:  strings.TrimSpace(harvestPathMeta.HarvestPathThresholdSource),
		HarvestPathAppliedThreshold: formatOptionalFloatWithPrecision(harvestPathMeta.HarvestPathAppliedThreshold, 4),
		HarvestPathAction:           harvestPathMeta.HarvestPathAction,
		HarvestPathRiskLevel:        harvestPathMeta.HarvestPathRiskLevel,
		HarvestPathTargetSide:       harvestPathMeta.HarvestPathTargetSide,
		HarvestPathReferencePrice:   formatOptionalFloatWithPrecision(harvestPathMeta.HarvestPathReferencePrice, pricePrecision),
		HarvestPathMarketPrice:      formatOptionalFloatWithPrecision(harvestPathMeta.HarvestPathMarketPrice, pricePrecision),
		Reason:                      harvestPathMeta.Reason,
		SignalReason:                signalReasonPBToJSON(harvestPathMeta.SignalReason),
		Protection:                  protectionStatusPBToJSON(harvestPathMeta.Protection),
		ReduceOnly:                  item.ReduceOnly,
		ClosePosition:               item.ClosePosition,
		UpdateTime:                  formatOrderTime(item.UpdateTime),
		TimeInForce:                 item.TimeInForce,
	}
}

func (s *ServiceContext) toPositionJSONLEntry(item binance.PositionResp) positionJSONLEntry {
	pricePrecision, quantityPrecision := s.symbolPrecisions(item.Symbol)
	positionAmt := parseFloatString(item.PositionAmt)
	positionSide := "BOTH"
	if positionAmt > 0 {
		positionSide = "LONG"
	} else if positionAmt < 0 {
		positionSide = "SHORT"
	}
	now := time.Now().UTC()
	return positionJSONLEntry{
		Symbol:            item.Symbol,
		PositionSide:      positionSide,
		PositionAmt:       formatDecimalString(item.PositionAmt, quantityPrecision, "0"),
		BaseQty:           formatFloatWithPrecision(absFloat(positionAmt), quantityPrecision),
		EntryPrice:        formatDecimalString(item.EntryPrice, pricePrecision, ""),
		MarkPrice:         formatDecimalString(item.MarkPrice, pricePrecision, ""),
		UnrealizedProfit:  item.UnrealizedProfit,
		LiquidationPrice:  formatDecimalString(item.LiquidationPrice, pricePrecision, ""),
		Leverage:          item.Leverage,
		MarginType:        item.MarginType,
		TimeUTC:           now.Format("2006-01-02 15:04:05"),
		TimeLocal:         now.In(orderDisplayLocation()).Format("2006-01-02 15:04:05"),
		SnapshotTimestamp: now.UnixMilli(),
	}
}

func (s *ServiceContext) toUserTradeJSONLEntry(item binance.UserTrade) userTradeJSONLEntry {
	pricePrecision, quantityPrecision := s.symbolPrecisions(item.Symbol)
	quoteQty := parseFloatString(item.QuoteQty)
	if quoteQty == 0 {
		quoteQty = parseFloatString(item.Price) * parseFloatString(item.Qty)
	}
	estimatedFee := quoteQty * estimateFeeRate("", item.Maker)
	return userTradeJSONLEntry{
		ID:              item.ID,
		Symbol:          item.Symbol,
		OrderID:         item.OrderID,
		Side:            item.Side,
		PositionSide:    item.PositionSide,
		Price:           formatDecimalString(item.Price, pricePrecision, "0"),
		Qty:             formatDecimalString(item.Qty, quantityPrecision, "0"),
		RealizedPnl:     item.RealizedPnl,
		MarginAsset:     item.MarginAsset,
		QuoteQty:        formatDecimalString(item.QuoteQty, 2, "0"),
		Commission:      item.Commission,
		CommissionAsset: item.CommissionAsset,
		EstimatedFee:    formatFloatWithPrecision(estimatedFee, 8),
		ActualFee:       formatDecimalString(item.Commission, 8, "0"),
		Time:            formatOrderTime(item.Time),
		TimeLocal:       formatOrderTimeLocal(item.Time),
		Buyer:           item.Buyer,
		Maker:           item.Maker,
	}
}

func (s *ServiceContext) symbolPrecisions(symbol string) (int, int) {
	if s == nil || s.client == nil || strings.TrimSpace(symbol) == "" {
		return 2, 3
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	pricePrecision, quantityPrecision, err := s.client.GetSymbolPrecisions(ctx, symbol)
	if err != nil {
		return 2, 3
	}
	if pricePrecision < 0 {
		pricePrecision = 2
	}
	if quantityPrecision < 0 {
		quantityPrecision = 3
	}
	if pricePrecision > 8 {
		pricePrecision = 8
	}
	if quantityPrecision > 8 {
		quantityPrecision = 8
	}
	return pricePrecision, quantityPrecision
}

func formatDecimalString(v string, precision int, zeroValue string) string {
	raw := strings.TrimSpace(v)
	if raw == "" {
		return ""
	}
	f, err := strconv.ParseFloat(raw, 64)
	if err != nil {
		return raw
	}
	if f == 0 {
		return zeroValue
	}
	return formatFloatWithPrecision(f, precision)
}

func formatOptionalFloatWithPrecision(v float64, precision int) string {
	if v == 0 {
		return ""
	}
	return formatFloatWithPrecision(v, precision)
}

func formatFloatWithPrecision(v float64, precision int) string {
	if precision < 0 {
		precision = 0
	}
	if precision > 8 {
		precision = 8
	}
	return strconv.FormatFloat(v, 'f', precision, 64)
}

const (
	defaultMakerFeeRate = 0.0002
	defaultTakerFeeRate = 0.0005
)

func estimateFeeRate(orderType string, maker bool) float64 {
	if maker {
		return defaultMakerFeeRate
	}
	if strings.EqualFold(strings.TrimSpace(orderType), "MARKET") {
		return defaultTakerFeeRate
	}
	return defaultMakerFeeRate
}

func (s *ServiceContext) cacheTradeFeeSummaries(symbol string, trades []binance.UserTrade) {
	if s == nil || strings.TrimSpace(symbol) == "" {
		return
	}
	symbol = strings.ToUpper(strings.TrimSpace(symbol))
	summaryMap := buildTradeFeeSummaryMap(trades)

	s.feeSummaryMu.Lock()
	s.feeSummaries[symbol] = summaryMap
	s.feeSummaryMu.Unlock()
}

func (s *ServiceContext) tradeFeeSummary(symbol string, orderID int64) tradeFeeSummary {
	if s == nil || strings.TrimSpace(symbol) == "" || orderID == 0 {
		return tradeFeeSummary{}
	}
	symbol = strings.ToUpper(strings.TrimSpace(symbol))
	s.feeSummaryMu.RLock()
	defer s.feeSummaryMu.RUnlock()
	if orderMap, ok := s.feeSummaries[symbol]; ok {
		return orderMap[orderID]
	}
	return tradeFeeSummary{}
}

func (s *ServiceContext) AllOrderFeeFields(order binance.AllOrder) (string, string, string) {
	summary := s.tradeFeeSummary(order.Symbol, order.OrderID)
	baseQty := parseFloatString(order.ExecutedQty)
	if baseQty == 0 {
		baseQty = parseFloatString(order.OrigQty)
	}
	quoteQty := parseFloatString(order.AvgPrice) * baseQty
	estimatedFee := summary.EstimatedFee
	if estimatedFee == 0 {
		estimatedFee = quoteQty * estimateFeeRate(order.Type, false)
	}
	actualFeeAsset := summary.ActualFeeAsset
	if summary.MixedAsset {
		actualFeeAsset = "MIXED"
	}
	return formatFloatWithPrecision(estimatedFee, 8), formatFloatWithPrecision(summary.ActualFee, 8), actualFeeAsset
}

func (s *ServiceContext) AllOrderLifecycleFields(order binance.AllOrder, lifecycleMap map[int64]OrderLifecycleInfo) (string, string) {
	if len(lifecycleMap) == 0 {
		return inferOrderActionType(order), ""
	}
	info := lifecycleMap[order.OrderID]
	return info.ActionType, info.PositionCycleID
}

// AllOrderHarvestPathFields 汇总 execution 日志沉淀的解释字段，供 order query 统一输出。
func (s *ServiceContext) AllOrderHarvestPathFields(order binance.AllOrder) (string, string, string, string, string, string, string, string, string, string, string, string, string, string, *orderpb.SignalReason, *orderpb.ProtectionStatus) {
	meta := s.allOrderHarvestPathMeta(order)
	pricePrecision, _ := s.symbolPrecisions(order.Symbol)
	return formatOptionalFloatWithPrecision(meta.HarvestPathProbability, 4),
		formatOptionalFloatWithPrecision(meta.HarvestPathRuleProbability, 4),
		formatOptionalFloatWithPrecision(meta.HarvestPathLSTMProbability, 4),
		formatOptionalFloatWithPrecision(meta.HarvestPathBookProbability, 4),
		strings.TrimSpace(meta.HarvestPathBookSummary),
		strings.TrimSpace(meta.HarvestPathVolatilityRegime),
		strings.TrimSpace(meta.HarvestPathThresholdSource),
		formatOptionalFloatWithPrecision(meta.HarvestPathAppliedThreshold, 4),
		meta.HarvestPathAction,
		meta.HarvestPathRiskLevel,
		meta.HarvestPathTargetSide,
		formatOptionalFloatWithPrecision(meta.HarvestPathReferencePrice, pricePrecision),
		formatOptionalFloatWithPrecision(meta.HarvestPathMarketPrice, pricePrecision),
		meta.Reason,
		cloneSignalReasonPB(meta.SignalReason),
		cloneProtectionStatusPB(meta.Protection)
}

func signalReasonJSONToPB(v *signalReasonJSON) *orderpb.SignalReason {
	if v == nil {
		return nil
	}
	out := &orderpb.SignalReason{
		Summary:          strings.TrimSpace(v.Summary),
		Phase:            strings.TrimSpace(v.Phase),
		TrendContext:     strings.TrimSpace(v.TrendContext),
		SetupContext:     strings.TrimSpace(v.SetupContext),
		PathContext:      strings.TrimSpace(v.PathContext),
		ExecutionContext: strings.TrimSpace(v.ExecutionContext),
		ExitReasonKind:   strings.TrimSpace(v.ExitReasonKind),
		ExitReasonLabel:  strings.TrimSpace(v.ExitReasonLabel),
		RouteBucket:      strings.TrimSpace(v.RouteBucket),
		RouteReason:      strings.TrimSpace(v.RouteReason),
		RouteTemplate:    strings.TrimSpace(v.RouteTemplate),
		Allocator:        allocatorStatusJSONToPB(v.Allocator),
		Range:            rangeSignalReasonJSONToPB(v.Range),
	}
	if len(v.Tags) > 0 {
		out.Tags = append([]string(nil), v.Tags...)
	}
	if out.Summary == "" && out.Phase == "" && out.TrendContext == "" && out.SetupContext == "" && out.PathContext == "" && out.ExecutionContext == "" && out.ExitReasonKind == "" && out.ExitReasonLabel == "" && out.RouteBucket == "" && out.RouteReason == "" && out.RouteTemplate == "" && len(out.Tags) == 0 && out.GetAllocator() == nil && out.GetRange() == nil {
		return nil
	}
	return out
}

func signalReasonPBToJSON(v *orderpb.SignalReason) *signalReasonJSON {
	if v == nil {
		return nil
	}
	out := &signalReasonJSON{
		Summary:          strings.TrimSpace(v.GetSummary()),
		Phase:            strings.TrimSpace(v.GetPhase()),
		TrendContext:     strings.TrimSpace(v.GetTrendContext()),
		SetupContext:     strings.TrimSpace(v.GetSetupContext()),
		PathContext:      strings.TrimSpace(v.GetPathContext()),
		ExecutionContext: strings.TrimSpace(v.GetExecutionContext()),
		ExitReasonKind:   strings.TrimSpace(v.GetExitReasonKind()),
		ExitReasonLabel:  strings.TrimSpace(v.GetExitReasonLabel()),
		RouteBucket:      strings.TrimSpace(v.GetRouteBucket()),
		RouteReason:      strings.TrimSpace(v.GetRouteReason()),
		RouteTemplate:    strings.TrimSpace(v.GetRouteTemplate()),
		Allocator:        allocatorStatusPBToJSON(v.GetAllocator()),
		Range:            rangeSignalReasonPBToJSON(v.GetRange()),
	}
	if tags := v.GetTags(); len(tags) > 0 {
		out.Tags = append([]string(nil), tags...)
	}
	if out.Summary == "" && out.Phase == "" && out.TrendContext == "" && out.SetupContext == "" && out.PathContext == "" && out.ExecutionContext == "" && out.ExitReasonKind == "" && out.ExitReasonLabel == "" && out.RouteBucket == "" && out.RouteReason == "" && out.RouteTemplate == "" && len(out.Tags) == 0 && out.Allocator == nil && out.Range == nil {
		return nil
	}
	return out
}

func cloneSignalReasonPB(v *orderpb.SignalReason) *orderpb.SignalReason {
	if v == nil {
		return nil
	}
	return &orderpb.SignalReason{
		Summary:          v.GetSummary(),
		Phase:            v.GetPhase(),
		TrendContext:     v.GetTrendContext(),
		SetupContext:     v.GetSetupContext(),
		PathContext:      v.GetPathContext(),
		ExecutionContext: v.GetExecutionContext(),
		ExitReasonKind:   v.GetExitReasonKind(),
		ExitReasonLabel:  v.GetExitReasonLabel(),
		Tags:             append([]string(nil), v.GetTags()...),
		RouteBucket:      v.GetRouteBucket(),
		RouteReason:      v.GetRouteReason(),
		RouteTemplate:    v.GetRouteTemplate(),
		Allocator:        cloneAllocatorStatusPB(v.GetAllocator()),
		Range:            cloneRangeSignalReasonPB(v.GetRange()),
	}
}

// rangeSignalReasonJSONToPB 将订单日志中的 range 摘要 JSON 转换为 protobuf。
func rangeSignalReasonJSONToPB(v *rangeSignalReasonJSON) *orderpb.RangeSignalReason {
	if v == nil {
		return nil
	}
	out := &orderpb.RangeSignalReason{
		H1RangeOk:      v.H1RangeOK,
		H1AdxOk:        v.H1AdxOK,
		H1BollWidthOk:  v.H1BollWidthOK,
		M15TouchLower:  v.M15TouchLower,
		M15RsiTurnUp:   v.M15RsiTurnUp,
		M15TouchUpper:  v.M15TouchUpper,
		M15RsiTurnDown: v.M15RsiTurnDown,
	}
	if !out.GetH1RangeOk() && !out.GetH1AdxOk() && !out.GetH1BollWidthOk() && !out.GetM15TouchLower() && !out.GetM15RsiTurnUp() && !out.GetM15TouchUpper() && !out.GetM15RsiTurnDown() {
		return nil
	}
	return out
}

// rangeSignalReasonPBToJSON 将 protobuf range 摘要转换为订单日志 JSON 结构。
func rangeSignalReasonPBToJSON(v *orderpb.RangeSignalReason) *rangeSignalReasonJSON {
	if v == nil {
		return nil
	}
	out := &rangeSignalReasonJSON{
		H1RangeOK:      v.GetH1RangeOk(),
		H1AdxOK:        v.GetH1AdxOk(),
		H1BollWidthOK:  v.GetH1BollWidthOk(),
		M15TouchLower:  v.GetM15TouchLower(),
		M15RsiTurnUp:   v.GetM15RsiTurnUp(),
		M15TouchUpper:  v.GetM15TouchUpper(),
		M15RsiTurnDown: v.GetM15RsiTurnDown(),
	}
	if !out.H1RangeOK && !out.H1AdxOK && !out.H1BollWidthOK && !out.M15TouchLower && !out.M15RsiTurnUp && !out.M15TouchUpper && !out.M15RsiTurnDown {
		return nil
	}
	return out
}

// cloneRangeSignalReasonPB 复制 range 摘要，避免外部共享同一实例。
func cloneRangeSignalReasonPB(v *orderpb.RangeSignalReason) *orderpb.RangeSignalReason {
	if v == nil {
		return nil
	}
	return &orderpb.RangeSignalReason{
		H1RangeOk:      v.GetH1RangeOk(),
		H1AdxOk:        v.GetH1AdxOk(),
		H1BollWidthOk:  v.GetH1BollWidthOk(),
		M15TouchLower:  v.GetM15TouchLower(),
		M15RsiTurnUp:   v.GetM15RsiTurnUp(),
		M15TouchUpper:  v.GetM15TouchUpper(),
		M15RsiTurnDown: v.GetM15RsiTurnDown(),
	}
}

// protectionStatusJSONToPB 将订单日志中的 protection JSON 快照转换为 protobuf。
func protectionStatusJSONToPB(v *protectionStatusJSON) *orderpb.ProtectionStatus {
	if v == nil {
		return nil
	}
	out := &orderpb.ProtectionStatus{
		Requested:  v.Requested,
		Status:     strings.TrimSpace(v.Status),
		Reason:     strings.TrimSpace(v.Reason),
		StopLoss:   protectionLegStatusJSONToPB(v.StopLoss),
		TakeProfit: protectionLegStatusJSONToPB(v.TakeProfit),
	}
	if !out.Requested && out.Status == "" && out.Reason == "" && out.GetStopLoss() == nil && out.GetTakeProfit() == nil {
		return nil
	}
	return out
}

// protectionLegStatusJSONToPB 将单条 protection 腿的 JSON 快照转换为 protobuf。
func protectionLegStatusJSONToPB(v *protectionLegStatusJSON) *orderpb.ProtectionLegStatus {
	if v == nil {
		return nil
	}
	out := &orderpb.ProtectionLegStatus{
		Requested:     v.Requested,
		Status:        strings.TrimSpace(v.Status),
		TriggerPrice:  v.TriggerPrice,
		Reason:        strings.TrimSpace(v.Reason),
		OrderId:       strings.TrimSpace(v.OrderID),
		ClientOrderId: strings.TrimSpace(v.ClientOrderID),
	}
	if !out.Requested && out.Status == "" && out.TriggerPrice == 0 && out.Reason == "" && out.OrderId == "" && out.ClientOrderId == "" {
		return nil
	}
	return out
}

// protectionStatusPBToJSON 将 protobuf protection 快照转换为订单日志 JSON 结构。
func protectionStatusPBToJSON(v *orderpb.ProtectionStatus) *protectionStatusJSON {
	if v == nil {
		return nil
	}
	out := &protectionStatusJSON{
		Requested:  v.GetRequested(),
		Status:     strings.TrimSpace(v.GetStatus()),
		Reason:     strings.TrimSpace(v.GetReason()),
		StopLoss:   protectionLegStatusPBToJSON(v.GetStopLoss()),
		TakeProfit: protectionLegStatusPBToJSON(v.GetTakeProfit()),
	}
	if !out.Requested && out.Status == "" && out.Reason == "" && out.StopLoss == nil && out.TakeProfit == nil {
		return nil
	}
	return out
}

// protectionLegStatusPBToJSON 将 protobuf protection 腿快照转换为订单日志 JSON 结构。
func protectionLegStatusPBToJSON(v *orderpb.ProtectionLegStatus) *protectionLegStatusJSON {
	if v == nil {
		return nil
	}
	out := &protectionLegStatusJSON{
		Requested:     v.GetRequested(),
		Status:        strings.TrimSpace(v.GetStatus()),
		TriggerPrice:  v.GetTriggerPrice(),
		Reason:        strings.TrimSpace(v.GetReason()),
		OrderID:       strings.TrimSpace(v.GetOrderId()),
		ClientOrderID: strings.TrimSpace(v.GetClientOrderId()),
	}
	if !out.Requested && out.Status == "" && out.TriggerPrice == 0 && out.Reason == "" && out.OrderID == "" && out.ClientOrderID == "" {
		return nil
	}
	return out
}

// cloneProtectionStatusPB 复制 protection 快照，避免调用方共享原对象。
func cloneProtectionStatusPB(v *orderpb.ProtectionStatus) *orderpb.ProtectionStatus {
	if v == nil {
		return nil
	}
	return &orderpb.ProtectionStatus{
		Requested:  v.GetRequested(),
		Status:     v.GetStatus(),
		Reason:     v.GetReason(),
		StopLoss:   cloneProtectionLegStatusPB(v.GetStopLoss()),
		TakeProfit: cloneProtectionLegStatusPB(v.GetTakeProfit()),
	}
}

// cloneProtectionLegStatusPB 复制单条 protection 腿快照，避免调用方共享原对象。
func cloneProtectionLegStatusPB(v *orderpb.ProtectionLegStatus) *orderpb.ProtectionLegStatus {
	if v == nil {
		return nil
	}
	return &orderpb.ProtectionLegStatus{
		Requested:     v.GetRequested(),
		Status:        v.GetStatus(),
		TriggerPrice:  v.GetTriggerPrice(),
		Reason:        v.GetReason(),
		OrderId:       v.GetOrderId(),
		ClientOrderId: v.GetClientOrderId(),
	}
}

// allocatorStatusJSONToPB 将订单日志中的 allocator JSON 快照转换为 protobuf。
func allocatorStatusJSONToPB(v *positionAllocatorStatusJSON) *orderpb.PositionAllocatorStatus {
	if v == nil {
		return nil
	}
	out := &orderpb.PositionAllocatorStatus{
		Template:       strings.TrimSpace(v.Template),
		RouteBucket:    strings.TrimSpace(v.RouteBucket),
		RouteReason:    strings.TrimSpace(v.RouteReason),
		Score:          v.Score,
		ScoreSource:    strings.TrimSpace(v.ScoreSource),
		BucketBudget:   v.BucketBudget,
		StrategyWeight: v.StrategyWeight,
		SymbolWeight:   v.SymbolWeight,
		RiskScale:      v.RiskScale,
		PositionBudget: v.PositionBudget,
		TradingPaused:  v.TradingPaused,
		PauseReason:    strings.TrimSpace(v.PauseReason),
	}
	if out.Template == "" && out.RouteBucket == "" && out.RouteReason == "" && out.Score == 0 && out.ScoreSource == "" && out.BucketBudget == 0 && out.StrategyWeight == 0 && out.SymbolWeight == 0 && out.RiskScale == 0 && out.PositionBudget == 0 && !out.TradingPaused && out.PauseReason == "" {
		return nil
	}
	return out
}

// allocatorStatusPBToJSON 将 protobuf allocator 快照转换为订单日志 JSON 结构。
func allocatorStatusPBToJSON(v *orderpb.PositionAllocatorStatus) *positionAllocatorStatusJSON {
	if v == nil {
		return nil
	}
	out := &positionAllocatorStatusJSON{
		Template:       strings.TrimSpace(v.GetTemplate()),
		RouteBucket:    strings.TrimSpace(v.GetRouteBucket()),
		RouteReason:    strings.TrimSpace(v.GetRouteReason()),
		Score:          v.GetScore(),
		ScoreSource:    strings.TrimSpace(v.GetScoreSource()),
		BucketBudget:   v.GetBucketBudget(),
		StrategyWeight: v.GetStrategyWeight(),
		SymbolWeight:   v.GetSymbolWeight(),
		RiskScale:      v.GetRiskScale(),
		PositionBudget: v.GetPositionBudget(),
		TradingPaused:  v.GetTradingPaused(),
		PauseReason:    strings.TrimSpace(v.GetPauseReason()),
	}
	if out.Template == "" && out.RouteBucket == "" && out.RouteReason == "" && out.Score == 0 && out.ScoreSource == "" && out.BucketBudget == 0 && out.StrategyWeight == 0 && out.SymbolWeight == 0 && out.RiskScale == 0 && out.PositionBudget == 0 && !out.TradingPaused && out.PauseReason == "" {
		return nil
	}
	return out
}

// cloneAllocatorStatusPB 复制 allocator 快照，避免调用方共享原对象。
func cloneAllocatorStatusPB(v *orderpb.PositionAllocatorStatus) *orderpb.PositionAllocatorStatus {
	if v == nil {
		return nil
	}
	return &orderpb.PositionAllocatorStatus{
		Template:       v.GetTemplate(),
		RouteBucket:    v.GetRouteBucket(),
		RouteReason:    v.GetRouteReason(),
		Score:          v.GetScore(),
		ScoreSource:    v.GetScoreSource(),
		BucketBudget:   v.GetBucketBudget(),
		StrategyWeight: v.GetStrategyWeight(),
		SymbolWeight:   v.GetSymbolWeight(),
		RiskScale:      v.GetRiskScale(),
		PositionBudget: v.GetPositionBudget(),
		TradingPaused:  v.GetTradingPaused(),
		PauseReason:    v.GetPauseReason(),
	}
}

func (s *ServiceContext) UserTradeFeeFields(trade binance.UserTrade) (string, string) {
	quoteQty := parseFloatString(trade.QuoteQty)
	if quoteQty == 0 {
		quoteQty = parseFloatString(trade.Price) * parseFloatString(trade.Qty)
	}
	estimatedFee := quoteQty * estimateFeeRate("", trade.Maker)
	return formatFloatWithPrecision(estimatedFee, 8), formatDecimalString(trade.Commission, 8, "0")
}

func buildTradeFeeSummaryMap(trades []binance.UserTrade) map[int64]tradeFeeSummary {
	result := make(map[int64]tradeFeeSummary)
	for _, trade := range trades {
		if trade.OrderID == 0 {
			continue
		}
		summary := result[trade.OrderID]
		summary.ActualFee += parseFloatString(trade.Commission)

		quoteQty := parseFloatString(trade.QuoteQty)
		if quoteQty == 0 {
			quoteQty = parseFloatString(trade.Price) * parseFloatString(trade.Qty)
		}
		summary.EstimatedFee += quoteQty * estimateFeeRate("", trade.Maker)

		asset := strings.ToUpper(strings.TrimSpace(trade.CommissionAsset))
		if asset != "" {
			if summary.ActualFeeAsset == "" {
				summary.ActualFeeAsset = asset
			} else if summary.ActualFeeAsset != asset {
				summary.MixedAsset = true
			}
		}

		result[trade.OrderID] = summary
	}
	return result
}

// BuildOrderLifecycleMap 根据订单序列和 execution 日志里的 signal_type 推导每笔委托所属的动作类型与持仓周期。
func (s *ServiceContext) BuildOrderLifecycleMap(orders []binance.AllOrder) map[int64]OrderLifecycleInfo {
	result := make(map[int64]OrderLifecycleInfo, len(orders))
	if len(orders) == 0 {
		return result
	}

	sorted := append([]binance.AllOrder(nil), orders...)
	sort.SliceStable(sorted, func(i, j int) bool {
		ti := orderEventTime(sorted[i])
		tj := orderEventTime(sorted[j])
		if ti == tj {
			return sorted[i].OrderID < sorted[j].OrderID
		}
		return ti < tj
	})

	type cycleTracker struct {
		seq     int
		balance float64
		cycleID string
	}
	trackers := map[string]*cycleTracker{
		"LONG":  {},
		"SHORT": {},
	}

	for _, order := range sorted {
		actionType := inferOrderActionTypeWithSignal(order, s.allOrderHarvestPathMeta(order).SignalType)
		info := OrderLifecycleInfo{ActionType: actionType}
		qty := parseFloatString(order.ExecutedQty)
		if qty <= 0 {
			result[order.OrderID] = info
			continue
		}

		positionSide := normalizeOrderPositionSide(order.PositionSide)
		tracker, ok := trackers[positionSide]
		if !ok || actionType == "" {
			result[order.OrderID] = info
			continue
		}

		if isOpenAction(actionType) {
			if tracker.balance <= 1e-9 || tracker.cycleID == "" {
				tracker.seq++
				tracker.cycleID = buildPositionCycleID(order.Symbol, positionSide, orderEventTime(order), tracker.seq)
			}
			tracker.balance += qty
			info.PositionCycleID = tracker.cycleID
		} else {
			if tracker.balance <= 1e-9 || tracker.cycleID == "" {
				tracker.seq++
				tracker.cycleID = buildPositionCycleID(order.Symbol, positionSide, orderEventTime(order), tracker.seq)
			}
			info.PositionCycleID = tracker.cycleID
			tracker.balance -= qty
			if tracker.balance <= 1e-9 {
				tracker.balance = 0
				tracker.cycleID = ""
			}
		}
		result[order.OrderID] = info
	}

	return result
}

// inferOrderActionType 保留原有兜底路径，在缺少 signal_type 时仅根据方向推断 open/close。
func inferOrderActionType(order binance.AllOrder) string {
	return inferOrderActionTypeWithSignal(order, "")
}

// inferOrderActionTypeWithSignal 优先使用 execution 日志里的 signal_type，区分 CLOSE 和 PARTIAL_CLOSE。
func inferOrderActionTypeWithSignal(order binance.AllOrder, signalType string) string {
	side := strings.ToUpper(strings.TrimSpace(order.Side))
	positionSide := normalizeOrderPositionSide(order.PositionSide)
	signalType = strings.ToUpper(strings.TrimSpace(signalType))
	switch positionSide {
	case "LONG":
		if side == "BUY" {
			return "OPEN_LONG"
		}
		if side == "SELL" {
			if signalType == "PARTIAL_CLOSE" {
				return "PARTIAL_CLOSE_LONG"
			}
			return "CLOSE_LONG"
		}
	case "SHORT":
		if side == "SELL" {
			return "OPEN_SHORT"
		}
		if side == "BUY" {
			if signalType == "PARTIAL_CLOSE" {
				return "PARTIAL_CLOSE_SHORT"
			}
			return "CLOSE_SHORT"
		}
	}
	return ""
}

func normalizeOrderPositionSide(positionSide string) string {
	ps := strings.ToUpper(strings.TrimSpace(positionSide))
	switch ps {
	case "LONG", "SHORT":
		return ps
	default:
		return ""
	}
}

func isOpenAction(actionType string) bool {
	return strings.HasPrefix(strings.ToUpper(strings.TrimSpace(actionType)), "OPEN_")
}

func buildPositionCycleID(symbol, positionSide string, eventTime int64, seq int) string {
	ts := time.UnixMilli(eventTime).UTC().Format("20060102T150405")
	return fmt.Sprintf("%s-%s-%s-%04d", strings.ToUpper(strings.TrimSpace(symbol)), positionSide, ts, seq)
}

func orderEventTime(order binance.AllOrder) int64 {
	if order.UpdateTime > 0 {
		return order.UpdateTime
	}
	return order.Time
}

func maxInt(a, b int) int {
	if a > b {
		return a
	}
	return b
}

func absFloat(v float64) float64 {
	if v < 0 {
		return -v
	}
	return v
}

func normalizeAllOrdersStartTime(startTime int64) int64 {
	if startTime > 0 {
		return startTime
	}
	return defaultAllOrdersStartTime()
}

func defaultAllOrdersStartTime() int64 {
	return time.Now().Add(-defaultAllOrdersLookback).UnixMilli()
}

func (s *ServiceContext) Close() error {
	if s == nil {
		return nil
	}
	if s.cancel != nil {
		s.cancel()
	}
	var firstErr error
	if s.orderConsumer != nil {
		if err := s.orderConsumer.Close(); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	if s.postgresDB != nil {
		if err := s.postgresDB.Close(); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	return firstErr
}

// FetchAllData 拉取全部合约数据并保存到本地 JSONL
func (s *ServiceContext) FetchAllData(ctx context.Context, symbol string, startTime, endTime int64) error {
	log.Printf("[Order服务] 开始拉取全部合约数据 | symbol=%s", symbol)

	// 1. 当前委托
	orders, err := s.GetOpenOrders(ctx, symbol)
	if err != nil {
		log.Printf("[Order服务] 当前委托拉取失败: %v", err)
	} else if len(orders) > 0 {
		grouped := make(map[string][]binance.OpenOrder)
		for _, o := range orders {
			sym := o.Symbol
			if sym == "" {
				sym = symbol
			}
			grouped[sym] = append(grouped[sym], o)
		}
		for sym, items := range grouped {
			if err := s.writeJSONL("open_orders", sym, items); err != nil {
				log.Printf("[Order服务] 写入当前委托失败: %v", err)
			}
		}
		log.Printf("[Order服务] 当前委托 | 共 %d 条", len(orders))
	} else if symbol != "" {
		if err := s.writeJSONL("open_orders", symbol, []binance.OpenOrder{}); err != nil {
			log.Printf("[Order服务] 清空当前委托快照失败: %v", err)
		}
	}

	// 2. 历史委托
	if symbol != "" {
		allOrders, err := s.GetAllOrders(ctx, symbol, startTime, endTime, 1000)
		if err != nil {
			log.Printf("[Order服务] 历史委托拉取失败: %v", err)
		} else {
			log.Printf("[Order服务] 历史委托 | 共 %d 条", len(allOrders))
		}
	}

	// 3. 历史成交
	if symbol != "" {
		trades, err := s.GetUserTrades(ctx, symbol, startTime, endTime, 1000)
		if err != nil {
			log.Printf("[Order服务] 历史成交拉取失败: %v", err)
		} else {
			if err := s.writeJSONL("user_trades", symbol, trades); err != nil {
				log.Printf("[Order服务] 写入历史成交失败: %v", err)
			}
			log.Printf("[Order服务] 历史成交 | 共 %d 条", len(trades))
		}
	}

	// 4. 资金流水
	incomes, err := s.GetIncomeHistory(ctx, symbol, "", startTime, endTime, 1000)
	if err != nil {
		log.Printf("[Order服务] 资金流水拉取失败: %v", err)
	} else if len(incomes) > 0 {
		grouped := make(map[string][]binance.Income)
		for _, inc := range incomes {
			sym := inc.Symbol
			if sym == "" {
				sym = symbol
			}
			if sym == "" {
				sym = "ALL"
			}
			grouped[sym] = append(grouped[sym], inc)
		}
		for sym, items := range grouped {
			if err := s.writeJSONL("income", sym, items); err != nil {
				log.Printf("[Order服务] 写入资金流水失败: %v", err)
			}
		}
		log.Printf("[Order服务] 资金流水 | 共 %d 条", len(incomes))
	}

	// 5. 资金费用
	fees, err := s.GetFundingFees(ctx, symbol, startTime, endTime, 1000)
	if err != nil {
		log.Printf("[Order服务] 资金费用拉取失败: %v", err)
	} else if len(fees) > 0 {
		grouped := make(map[string][]binance.FundingFee)
		for _, fee := range fees {
			sym := fee.Symbol
			if sym == "" {
				sym = symbol
			}
			if sym == "" {
				sym = "ALL"
			}
			grouped[sym] = append(grouped[sym], fee)
		}
		for sym, items := range grouped {
			if err := s.writeJSONL("funding_fee", sym, items); err != nil {
				log.Printf("[Order服务] 写入资金费用失败: %v", err)
			}
		}
		log.Printf("[Order服务] 资金费用 | 共 %d 条", len(fees))
	} else if symbol != "" {
		if err := s.writeJSONL("funding_fee", symbol, []binance.FundingFee{}); err != nil {
			log.Printf("[Order服务] 清空资金费用快照失败: %v", err)
		}
	}

	log.Printf("[Order服务] 全部数据拉取完成 | symbol=%s", symbol)
	return nil
}
