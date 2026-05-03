package kafka

import (
	"context"
	"encoding/json"
	"fmt"

	commonkafka "exchange-system/common/kafka"
	strategypb "exchange-system/common/pb/strategy"

	"github.com/Shopify/sarama"
	"go.uber.org/zap"
	"google.golang.org/protobuf/encoding/protojson"
)

// ---------------------------------------------------------------------------
// Consumer — 消费策略信号（Trade Signal）
//
// execution 只消费"可执行信号"，不消费原始K线
// 数据流：Market(Kline) → Strategy(分析+决策) → Signal → Execution(下单+风控)
// ---------------------------------------------------------------------------

// Consumer Kafka 消费者，订阅 signal topic
type Consumer struct {
	group   sarama.ConsumerGroup
	topic   string
	groupID string
	logger  *zap.Logger
}

type HarvestPathConsumer struct {
	group   sarama.ConsumerGroup
	topic   string
	groupID string
	logger  *zap.Logger
}

// SignalHandler 信号处理回调
type SignalHandler func(signal *strategypb.Signal) error

type HarvestPathSignal struct {
	Symbol                 string  `json:"symbol"`
	EventTime              int64   `json:"event_time"`
	Interval               string  `json:"interval"`
	Model                  string  `json:"model"`
	EntrySide              string  `json:"entry_side"`
	TargetSide             string  `json:"target_side"`
	TargetZoneLow          float64 `json:"target_zone_low"`
	TargetZoneHigh         float64 `json:"target_zone_high"`
	ReferencePrice         float64 `json:"reference_price"`
	MarketPrice            float64 `json:"market_price"`
	StopDensityScore       float64 `json:"stop_density_score"`
	TriggerScore           float64 `json:"trigger_score"`
	RuleProbability        float64 `json:"rule_probability"`
	LSTMProbability        float64 `json:"lstm_probability"`
	BookProbability        float64 `json:"book_probability"`
	BookSummary            string  `json:"book_summary"`
	VolatilityRegime       string  `json:"volatility_regime"`
	ThresholdSource        string  `json:"threshold_source"`
	AppliedThreshold       float64 `json:"applied_threshold"`
	HarvestPathProbability float64 `json:"harvest_path_probability"`
	ExpectedPathDepth      float64 `json:"expected_path_depth"`
	ExpectedReversalSpeed  float64 `json:"expected_reversal_speed"`
	PathAction             string  `json:"path_action"`
	RiskLevel              string  `json:"risk_level"`
	IsClosed               bool    `json:"is_closed"`
	IsFinal                bool    `json:"is_final"`
	IsTradable             bool    `json:"is_tradable"`
	Volume                 float64 `json:"volume"`
	QuoteVolume            float64 `json:"quote_volume"`
	TakerBuyVolume         float64 `json:"taker_buy_volume"`
}

type HarvestPathHandler func(signal *HarvestPathSignal) error

// NewConsumer 创建信号消费者
func NewConsumer(brokers []string, groupID string, topic string) (*Consumer, error) {
	if groupID == "" {
		groupID = fmt.Sprintf("cg-%s", topic)
	}
	config := commonkafka.NewConsumerGroupConfig()

	group, err := sarama.NewConsumerGroup(brokers, groupID, config)
	if err != nil {
		return nil, err
	}
	return &Consumer{
		group:   group,
		topic:   topic,
		groupID: groupID,
		logger:  zap.L().With(zap.String("component", "kafka-consumer"), zap.String("topic", topic)),
	}, nil
}

func NewHarvestPathConsumer(brokers []string, groupID string, topic string) (*HarvestPathConsumer, error) {
	if groupID == "" {
		groupID = fmt.Sprintf("cg-%s", topic)
	}
	config := commonkafka.NewConsumerGroupConfig()

	group, err := sarama.NewConsumerGroup(brokers, groupID, config)
	if err != nil {
		return nil, err
	}
	return &HarvestPathConsumer{
		group:   group,
		topic:   topic,
		groupID: groupID,
		logger:  zap.L().With(zap.String("component", "kafka-harvest-path-consumer"), zap.String("topic", topic)),
	}, nil
}

// StartConsuming 启动消费循环
func (c *Consumer) StartConsuming(ctx context.Context, handler SignalHandler) error {
	if c == nil || c.group == nil {
		return fmt.Errorf("consumer group not initialized")
	}
	if handler == nil {
		return fmt.Errorf("handler is nil")
	}

	h := &signalGroupHandler{
		handler: handler,
		logger:  c.logger,
	}
	go func() {
		for {
			if err := c.group.Consume(ctx, []string{c.topic}, h); err != nil {
				c.logger.Error("consume error", zap.String("group", c.groupID), zap.Error(err))
			}
			if ctx.Err() != nil {
				return
			}
		}
	}()
	return nil
}

// Close 关闭消费者
func (c *Consumer) Close() error {
	if c == nil || c.group == nil {
		return nil
	}
	return c.group.Close()
}

func (c *HarvestPathConsumer) StartConsuming(ctx context.Context, handler HarvestPathHandler) error {
	if c == nil || c.group == nil {
		return fmt.Errorf("harvest path consumer group not initialized")
	}
	if handler == nil {
		return fmt.Errorf("harvest path handler is nil")
	}

	h := &harvestPathGroupHandler{
		handler: handler,
		logger:  c.logger,
	}
	go func() {
		for {
			if err := c.group.Consume(ctx, []string{c.topic}, h); err != nil {
				c.logger.Error("consume harvest path error", zap.String("group", c.groupID), zap.Error(err))
			}
			if ctx.Err() != nil {
				return
			}
		}
	}()
	return nil
}

func (c *HarvestPathConsumer) Close() error {
	if c == nil || c.group == nil {
		return nil
	}
	return c.group.Close()
}

// ---------------------------------------------------------------------------
// 内部消费组处理器
// ---------------------------------------------------------------------------

type signalGroupHandler struct {
	handler SignalHandler
	logger  *zap.Logger
}

func (h *signalGroupHandler) Setup(sarama.ConsumerGroupSession) error   { return nil }
func (h *signalGroupHandler) Cleanup(sarama.ConsumerGroupSession) error { return nil }

type harvestPathGroupHandler struct {
	handler HarvestPathHandler
	logger  *zap.Logger
}

func (h *harvestPathGroupHandler) Setup(sarama.ConsumerGroupSession) error   { return nil }
func (h *harvestPathGroupHandler) Cleanup(sarama.ConsumerGroupSession) error { return nil }

func (h *signalGroupHandler) ConsumeClaim(session sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	for msg := range claim.Messages() {
		if msg == nil {
			continue
		}

		// 使用 protojson 反序列化，支持 proto 的 map<string,double> 等类型
		var sig strategypb.Signal
		if err := protojson.Unmarshal(msg.Value, &sig); err != nil {
			// protojson 失败时尝试标准 json 兼容模式（strategy 发的是标准 JSON）
			sig.Reset()
			if err2 := unmarshalCompat(msg.Value, &sig); err2 != nil {
				h.logger.Warn("signal unmarshal failed, skipping",
					zap.Int64("offset", msg.Offset),
					zap.Error(err),
					zap.NamedError("compat_error", err2))
				session.MarkMessage(msg, "")
				continue
			}
		}

		// 调用业务处理
		if err := h.handler(&sig); err != nil {
			h.logger.Error("handle signal failed",
				zap.String("symbol", sig.GetSymbol()),
				zap.String("action", sig.GetAction()),
				zap.Error(err))
		}
		session.MarkMessage(msg, "")
	}
	return nil
}

func (h *harvestPathGroupHandler) ConsumeClaim(session sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	for msg := range claim.Messages() {
		if msg == nil {
			continue
		}

		var signal HarvestPathSignal
		if err := json.Unmarshal(msg.Value, &signal); err != nil {
			h.logger.Warn("harvest path signal unmarshal failed, skipping",
				zap.Int32("partition", msg.Partition),
				zap.Int64("offset", msg.Offset),
				zap.Error(err))
			session.MarkMessage(msg, "")
			continue
		}

		if err := h.handler(&signal); err != nil {
			h.logger.Error("handle harvest path signal failed",
				zap.String("symbol", signal.Symbol),
				zap.String("risk_level", signal.RiskLevel),
				zap.Error(err))
		}
		session.MarkMessage(msg, "")
	}
	return nil
}

// ---------------------------------------------------------------------------
// 辅助函数
// ---------------------------------------------------------------------------

// unmarshalCompat 兼容标准 JSON 格式的反序列化
// strategy 发送的 indicators 是 map[string]interface{}，
// protojson 对 map<string,double> 的解析更严格，
// 这里用标准 json + 手动赋值兜底
func unmarshalCompat(data []byte, sig *strategypb.Signal) error {
	// 临时结构体，JSON tag 与 strategy 发送的键名一致（下划线格式）
	var raw struct {
		StrategyID   string                 `json:"strategy_id"`
		Symbol       string                 `json:"symbol"`
		Action       string                 `json:"action"`
		Side         string                 `json:"side"`
		Quantity     float64                `json:"quantity"`
		EntryPrice   float64                `json:"entry_price"`
		StopLoss     float64                `json:"stop_loss"`
		TakeProfits  []float64              `json:"take_profits"`
		Reason       string                 `json:"reason"`
		Timestamp    int64                  `json:"timestamp"`
		SignalType   string                 `json:"signal_type"`
		Interval     string                 `json:"interval"`
		Atr          float64                `json:"atr"`
		RiskReward   float64                `json:"risk_reward"`
		Indicators   map[string]interface{} `json:"indicators"`
		SignalReason *struct {
			Summary          string   `json:"summary"`
			Phase            string   `json:"phase"`
			TrendContext     string   `json:"trend_context"`
			SetupContext     string   `json:"setup_context"`
			PathContext      string   `json:"path_context"`
			ExecutionContext string   `json:"execution_context"`
			ExitReasonKind   string   `json:"exit_reason_kind"`
			ExitReasonLabel  string   `json:"exit_reason_label"`
			Tags             []string `json:"tags"`
			RouteBucket      string   `json:"route_bucket"`
			RouteReason      string   `json:"route_reason"`
			RouteTemplate    string   `json:"route_template"`
			Range            *struct {
				H1RangeOK      bool `json:"h1_range_ok"`
				H1AdxOK        bool `json:"h1_adx_ok"`
				H1BollWidthOK  bool `json:"h1_boll_width_ok"`
				M15TouchLower  bool `json:"m15_touch_lower"`
				M15RsiTurnUp   bool `json:"m15_rsi_turn_up"`
				M15TouchUpper  bool `json:"m15_touch_upper"`
				M15RsiTurnDown bool `json:"m15_rsi_turn_down"`
			} `json:"range"`
			Allocator *struct {
				Template       string  `json:"template"`
				RouteBucket    string  `json:"route_bucket"`
				RouteReason    string  `json:"route_reason"`
				Score          float64 `json:"score"`
				ScoreSource    string  `json:"score_source"`
				BucketBudget   float64 `json:"bucket_budget"`
				StrategyWeight float64 `json:"strategy_weight"`
				SymbolWeight   float64 `json:"symbol_weight"`
				RiskScale      float64 `json:"risk_scale"`
				PositionBudget float64 `json:"position_budget"`
				TradingPaused  bool    `json:"trading_paused"`
				PauseReason    string  `json:"pause_reason"`
			} `json:"allocator"`
		} `json:"signal_reason"`
	}

	if err := json.Unmarshal(data, &raw); err != nil {
		return err
	}

	sig.StrategyId = raw.StrategyID
	sig.Symbol = raw.Symbol
	sig.Action = raw.Action
	sig.Side = raw.Side
	sig.Quantity = raw.Quantity
	sig.EntryPrice = raw.EntryPrice
	sig.StopLoss = raw.StopLoss
	sig.TakeProfits = raw.TakeProfits
	sig.Reason = raw.Reason
	sig.Timestamp = raw.Timestamp
	sig.SignalType = raw.SignalType
	sig.Interval = raw.Interval
	sig.Atr = raw.Atr
	sig.RiskReward = raw.RiskReward
	if raw.SignalReason != nil {
		sig.SignalReason = &strategypb.SignalReason{
			Summary:          raw.SignalReason.Summary,
			Phase:            raw.SignalReason.Phase,
			TrendContext:     raw.SignalReason.TrendContext,
			SetupContext:     raw.SignalReason.SetupContext,
			PathContext:      raw.SignalReason.PathContext,
			ExecutionContext: raw.SignalReason.ExecutionContext,
			ExitReasonKind:   raw.SignalReason.ExitReasonKind,
			ExitReasonLabel:  raw.SignalReason.ExitReasonLabel,
			Tags:             raw.SignalReason.Tags,
			RouteBucket:      raw.SignalReason.RouteBucket,
			RouteReason:      raw.SignalReason.RouteReason,
			RouteTemplate:    raw.SignalReason.RouteTemplate,
		}
		if raw.SignalReason.Allocator != nil {
			sig.SignalReason.Allocator = &strategypb.PositionAllocatorStatus{
				Template:       raw.SignalReason.Allocator.Template,
				RouteBucket:    raw.SignalReason.Allocator.RouteBucket,
				RouteReason:    raw.SignalReason.Allocator.RouteReason,
				Score:          raw.SignalReason.Allocator.Score,
				ScoreSource:    raw.SignalReason.Allocator.ScoreSource,
				BucketBudget:   raw.SignalReason.Allocator.BucketBudget,
				StrategyWeight: raw.SignalReason.Allocator.StrategyWeight,
				SymbolWeight:   raw.SignalReason.Allocator.SymbolWeight,
				RiskScale:      raw.SignalReason.Allocator.RiskScale,
				PositionBudget: raw.SignalReason.Allocator.PositionBudget,
				TradingPaused:  raw.SignalReason.Allocator.TradingPaused,
				PauseReason:    raw.SignalReason.Allocator.PauseReason,
			}
		}
		if raw.SignalReason.Range != nil {
			sig.SignalReason.Range = &strategypb.RangeSignalReason{
				H1RangeOk:      raw.SignalReason.Range.H1RangeOK,
				H1AdxOk:        raw.SignalReason.Range.H1AdxOK,
				H1BollWidthOk:  raw.SignalReason.Range.H1BollWidthOK,
				M15TouchLower:  raw.SignalReason.Range.M15TouchLower,
				M15RsiTurnUp:   raw.SignalReason.Range.M15RsiTurnUp,
				M15TouchUpper:  raw.SignalReason.Range.M15TouchUpper,
				M15RsiTurnDown: raw.SignalReason.Range.M15RsiTurnDown,
			}
		}
		if sig.Reason == "" {
			sig.Reason = raw.SignalReason.Summary
		}
	}

	// indicators: map[string]interface{} → map[string]float64
	if raw.Indicators != nil {
		sig.Indicators = make(map[string]float64, len(raw.Indicators))
		for k, v := range raw.Indicators {
			if f, ok := toFloat64(v); ok {
				sig.Indicators[k] = f
			}
		}
	}

	return nil
}

// toFloat64 将 interface{} 转换为 float64
// 支持 float64, int, int64, json.Number 等类型
func toFloat64(v interface{}) (float64, bool) {
	switch n := v.(type) {
	case float64:
		return n, true
	case float32:
		return float64(n), true
	case int:
		return float64(n), true
	case int64:
		return float64(n), true
	case json.Number:
		f, err := n.Float64()
		return f, err == nil
	default:
		return 0, false
	}
}
