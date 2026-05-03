package handler

import (
	"encoding/csv"
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"time"

	"exchange-system/app/api/gateway/internal/svc"
	orderpb "exchange-system/common/pb/order"
)

type allOrderView struct {
	OrderID                     int64                 `json:"order_id"`
	Symbol                      string                `json:"symbol"`
	Status                      string                `json:"status"`
	Side                        string                `json:"side"`
	PositionSide                string                `json:"position_side"`
	Type                        string                `json:"type"`
	OrigQty                     string                `json:"orig_qty"`
	ExecutedQty                 string                `json:"executed_qty"`
	AvgPrice                    string                `json:"avg_price"`
	Price                       string                `json:"price"`
	StopPrice                   string                `json:"stop_price"`
	ClientOrderID               string                `json:"client_order_id"`
	Time                        int64                 `json:"time"`
	TimeText                    string                `json:"time_text"`
	UpdateTime                  int64                 `json:"update_time"`
	UpdateTimeText              string                `json:"update_time_text"`
	ReduceOnly                  bool                  `json:"reduce_only"`
	ClosePosition               bool                  `json:"close_position"`
	TimeInForce                 string                `json:"time_in_force"`
	EstimatedFee                string                `json:"estimated_fee"`
	ActualFee                   string                `json:"actual_fee"`
	ActualFeeAsset              string                `json:"actual_fee_asset"`
	ActionType                  string                `json:"action_type"`
	ActionLabel                 string                `json:"action_label"`
	PositionCycleID             string                `json:"position_cycle_id"`
	Reason                      string                `json:"reason"`
	ExitReasonKind              string                `json:"exit_reason_kind,omitempty"`
	ExitReasonLabel             string                `json:"exit_reason_label,omitempty"`
	ExitReasonDetail            string                `json:"exit_reason_detail,omitempty"`
	SignalReasonListSummary     string                `json:"signal_reason_list_summary,omitempty"`
	SignalReasonEntrySummary    string                `json:"signal_reason_entry_summary,omitempty"`
	SignalReasonFullSummary     string                `json:"signal_reason_full_summary,omitempty"`
	SignalReason                *signalReasonView     `json:"signal_reason,omitempty"`
	Protection                  *protectionStatusView `json:"protection,omitempty"`
	HarvestPathProbability      string                `json:"harvest_path_probability"`
	HarvestPathRuleProbability  string                `json:"harvest_path_rule_probability"`
	HarvestPathLSTMProbability  string                `json:"harvest_path_lstm_probability"`
	HarvestPathBookProbability  string                `json:"harvest_path_book_probability"`
	HarvestPathBookSummary      string                `json:"harvest_path_book_summary"`
	HarvestPathBookExplain      string                `json:"harvest_path_book_explain"`
	HarvestPathVolatilityRegime string                `json:"harvest_path_volatility_regime"`
	HarvestPathThresholdSource  string                `json:"harvest_path_threshold_source"`
	HarvestPathAppliedThreshold string                `json:"harvest_path_applied_threshold"`
	HarvestPathThresholdExplain string                `json:"harvest_path_threshold_explain"`
	HarvestPathFusionExplain    string                `json:"harvest_path_fusion_explain"`
	HarvestPathScoreFinal       float64               `json:"harvest_path_score_final"`
	HarvestPathScoreRule        float64               `json:"harvest_path_score_rule"`
	HarvestPathScoreLSTM        float64               `json:"harvest_path_score_lstm"`
	HarvestPathScoreBreakdown   string                `json:"harvest_path_score_breakdown"`
	HarvestPathModelLabel       string                `json:"harvest_path_model_label"`
	HarvestPathModelSummary     string                `json:"harvest_path_model_summary"`
	HarvestPathAction           string                `json:"harvest_path_action"`
	HarvestPathRiskLevel        string                `json:"harvest_path_risk_level"`
	HarvestPathTargetSide       string                `json:"harvest_path_target_side"`
	HarvestPathReferencePrice   string                `json:"harvest_path_reference_price"`
	HarvestPathMarketPrice      string                `json:"harvest_path_market_price"`
	HarvestPathActionLabel      string                `json:"harvest_path_action_label"`
	HarvestPathRiskLabel        string                `json:"harvest_path_risk_label"`
	HarvestPathSummary          string                `json:"harvest_path_summary"`
}

type userTradeView struct {
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
	Time            int64  `json:"time"`
	TimeText        string `json:"time_text"`
	Buyer           bool   `json:"buyer"`
	Maker           bool   `json:"maker"`
	EstimatedFee    string `json:"estimated_fee"`
	ActualFee       string `json:"actual_fee"`
}

type positionCycleView struct {
	PositionCycleID               string            `json:"position_cycle_id"`
	Symbol                        string            `json:"symbol"`
	PositionSide                  string            `json:"position_side"`
	PositionSideLabel             string            `json:"position_side_label"`
	CycleStatus                   string            `json:"cycle_status"`
	CycleStatusLabel              string            `json:"cycle_status_label"`
	OpenTime                      int64             `json:"open_time"`
	OpenTimeText                  string            `json:"open_time_text"`
	CloseTime                     int64             `json:"close_time"`
	CloseTimeText                 string            `json:"close_time_text"`
	OrderCount                    int               `json:"order_count"`
	OpenOrderCount                int               `json:"open_order_count"`
	CloseOrderCount               int               `json:"close_order_count"`
	OpenOrderIDs                  []int64           `json:"open_order_ids"`
	CloseOrderIDs                 []int64           `json:"close_order_ids"`
	OpenedQty                     string            `json:"opened_qty"`
	ClosedQty                     string            `json:"closed_qty"`
	RemainingQty                  string            `json:"remaining_qty"`
	OpenAvgPrice                  string            `json:"open_avg_price"`
	CloseAvgPrice                 string            `json:"close_avg_price"`
	CloseProgress                 string            `json:"close_progress"`
	EstimatedFee                  string            `json:"estimated_fee"`
	ActualFee                     string            `json:"actual_fee"`
	ActualFeeAsset                string            `json:"actual_fee_asset"`
	MaxHarvestPathProbability     string            `json:"max_harvest_path_probability"`
	MaxHarvestPathRuleProbability string            `json:"max_harvest_path_rule_probability"`
	MaxHarvestPathLSTMProbability string            `json:"max_harvest_path_lstm_probability"`
	MaxHarvestPathBookProbability string            `json:"max_harvest_path_book_probability"`
	HarvestPathBookSummary        string            `json:"harvest_path_book_summary"`
	HarvestPathBookExplain        string            `json:"harvest_path_book_explain"`
	HarvestPathVolatilityRegime   string            `json:"harvest_path_volatility_regime"`
	HarvestPathThresholdSource    string            `json:"harvest_path_threshold_source"`
	HarvestPathAppliedThreshold   string            `json:"harvest_path_applied_threshold"`
	HarvestPathThresholdExplain   string            `json:"harvest_path_threshold_explain"`
	HarvestPathFusionExplain      string            `json:"harvest_path_fusion_explain"`
	HarvestPathScoreFinal         float64           `json:"harvest_path_score_final"`
	HarvestPathScoreRule          float64           `json:"harvest_path_score_rule"`
	HarvestPathScoreLSTM          float64           `json:"harvest_path_score_lstm"`
	HarvestPathScoreBreakdown     string            `json:"harvest_path_score_breakdown"`
	HarvestPathModelLabel         string            `json:"harvest_path_model_label"`
	HarvestPathModelSummary       string            `json:"harvest_path_model_summary"`
	HarvestPathAction             string            `json:"harvest_path_action"`
	HarvestPathRiskLevel          string            `json:"harvest_path_risk_level"`
	HarvestPathTargetSide         string            `json:"harvest_path_target_side"`
	HarvestPathReferencePrice     string            `json:"harvest_path_reference_price"`
	HarvestPathMarketPrice        string            `json:"harvest_path_market_price"`
	HarvestPathActionLabel        string            `json:"harvest_path_action_label"`
	HarvestPathRiskLabel          string            `json:"harvest_path_risk_label"`
	HarvestPathSummary            string            `json:"harvest_path_summary"`
	EntryReasonSummary            string            `json:"entry_reason_summary"`
	ExitReasonSummary             string            `json:"exit_reason_summary"`
	ExitReasonKind                string            `json:"exit_reason_kind,omitempty"`
	ExitReasonLabel               string            `json:"exit_reason_label,omitempty"`
	ExitReasonDetail              string            `json:"exit_reason_detail,omitempty"`
	DominantReasonSummary         string            `json:"dominant_reason_summary"`
	EntryPhase                    string            `json:"entry_phase"`
	ExitPhase                     string            `json:"exit_phase"`
	EntryTags                     []string          `json:"entry_tags,omitempty"`
	ExitTags                      []string          `json:"exit_tags,omitempty"`
	EntryTagsText                 string            `json:"entry_tags_text"`
	ExitTagsText                  string            `json:"exit_tags_text"`
	PartialCloseOrderCount        int               `json:"partial_close_order_count"`
	FinalCloseOrderCount          int               `json:"final_close_order_count"`
	ExitActionSummary             string            `json:"exit_action_summary"`
	CycleReasonBrief              string            `json:"cycle_reason_brief"`
	EntrySignalReason             *signalReasonView `json:"entry_signal_reason,omitempty"`
	ExitSignalReason              *signalReasonView `json:"exit_signal_reason,omitempty"`
	DominantSignalReason          *signalReasonView `json:"dominant_signal_reason,omitempty"`
	SignalReason                  *signalReasonView `json:"signal_reason,omitempty"`
}

type signalReasonView struct {
	Summary          string                 `json:"summary,omitempty"`
	Phase            string                 `json:"phase,omitempty"`
	TrendContext     string                 `json:"trend_context,omitempty"`
	SetupContext     string                 `json:"setup_context,omitempty"`
	PathContext      string                 `json:"path_context,omitempty"`
	ExecutionContext string                 `json:"execution_context,omitempty"`
	ExitReasonKind   string                 `json:"exit_reason_kind,omitempty"`
	ExitReasonLabel  string                 `json:"exit_reason_label,omitempty"`
	Tags             []string               `json:"tags,omitempty"`
	RouteBucket      string                 `json:"route_bucket,omitempty"`
	RouteReason      string                 `json:"route_reason,omitempty"`
	RouteTemplate    string                 `json:"route_template,omitempty"`
	Router           *signalRouterView      `json:"router,omitempty"`
	Allocator        *positionAllocatorView `json:"allocator,omitempty"`
	Range            *signalRangeView       `json:"range,omitempty"`
}

type signalRangeView struct {
	H1RangeOK      bool   `json:"h1_range_ok"`
	H1AdxOK        bool   `json:"h1_adx_ok"`
	H1BollWidthOK  bool   `json:"h1_boll_width_ok"`
	M15TouchLower  bool   `json:"m15_touch_lower"`
	M15RsiTurnUp   bool   `json:"m15_rsi_turn_up"`
	M15TouchUpper  bool   `json:"m15_touch_upper"`
	M15RsiTurnDown bool   `json:"m15_rsi_turn_down"`
	RegimeSummary  string `json:"regime_summary,omitempty"`
	EntrySummary   string `json:"entry_summary,omitempty"`
	FullSummary    string `json:"full_summary,omitempty"`
	Summary        string `json:"summary,omitempty"`
}

type signalRouterView struct {
	RouteBucket  string `json:"route_bucket,omitempty"`
	TargetReason string `json:"target_reason,omitempty"`
	Template     string `json:"template,omitempty"`
}

type positionAllocatorView struct {
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

type protectionStatusView struct {
	Requested  bool                     `json:"requested"`
	Status     string                   `json:"status,omitempty"`
	Reason     string                   `json:"reason,omitempty"`
	StopLoss   *protectionLegStatusView `json:"stop_loss,omitempty"`
	TakeProfit *protectionLegStatusView `json:"take_profit,omitempty"`
	Summary    string                   `json:"summary,omitempty"`
}

type protectionLegStatusView struct {
	Requested     bool    `json:"requested"`
	Status        string  `json:"status,omitempty"`
	TriggerPrice  float64 `json:"trigger_price,omitempty"`
	Reason        string  `json:"reason,omitempty"`
	OrderID       string  `json:"order_id,omitempty"`
	ClientOrderID string  `json:"client_order_id,omitempty"`
}

type positionCycleAgg struct {
	positionCycleID               string
	symbol                        string
	positionSide                  string
	openTime                      int64
	closeTime                     int64
	openOrderCount                int
	closeOrderCount               int
	partialCloseOrderCount        int
	finalCloseOrderCount          int
	openOrderIDs                  []int64
	closeOrderIDs                 []int64
	openQty                       float64
	closeQty                      float64
	openNotional                  float64
	closeNotional                 float64
	estimatedFee                  float64
	actualFee                     float64
	actualFeeAsset                string
	actualFeeMixed                bool
	maxHarvestPathProbability     float64
	maxHarvestPathRuleProbability float64
	maxHarvestPathLSTMProbability float64
	maxHarvestPathBookProbability float64
	harvestPathBookSummary        string
	harvestPathVolatilityRegime   string
	harvestPathThresholdSource    string
	harvestPathAppliedThreshold   float64
	harvestPathAction             string
	harvestPathRiskLevel          string
	harvestPathTargetSide         string
	harvestPathReferencePrice     string
	harvestPathMarketPrice        string
	entrySignalReason             *signalReasonView
	entrySignalTime               int64
	exitSignalReason              *signalReasonView
	exitSignalTime                int64
	exitActionType                string
	exitReason                    string
	exitEventTime                 int64
	signalReason                  *signalReasonView
}

// exitReasonView 将不同策略/执行路径的出场原因统一归一为稳定字段，避免前端重复解析自由文本。
type exitReasonView struct {
	Kind   string
	Label  string
	Detail string
}

func AllOrdersHandler(serviceContext *svc.ServiceContext) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		req, ok := parseOrderQueryRequest(w, r)
		if !ok {
			return
		}
		resp, err := serviceContext.Order.GetAllOrders(r.Context(), req)
		if err != nil {
			writeError(w, http.StatusBadGateway, err.Error())
			return
		}
		items := make([]allOrderView, 0, len(resp.GetItems()))
		for _, item := range resp.GetItems() {
			action := item.GetHarvestPathAction()
			riskLevel := item.GetHarvestPathRiskLevel()
			targetSide := item.GetHarvestPathTargetSide()
			signalReasonView := toSignalReasonView(item.GetSignalReason())
			exitReason := normalizeExitReason(item.GetActionType(), item.GetReason(), signalReasonView)
			items = append(items, allOrderView{
				OrderID:                     item.GetOrderId(),
				Symbol:                      item.GetSymbol(),
				Status:                      item.GetStatus(),
				Side:                        item.GetSide(),
				PositionSide:                item.GetPositionSide(),
				Type:                        item.GetType(),
				OrigQty:                     item.GetOrigQty(),
				ExecutedQty:                 item.GetExecutedQty(),
				AvgPrice:                    item.GetAvgPrice(),
				Price:                       item.GetPrice(),
				StopPrice:                   item.GetStopPrice(),
				ClientOrderID:               item.GetClientOrderId(),
				Time:                        item.GetTime(),
				TimeText:                    formatGatewayMillis(item.GetTime()),
				UpdateTime:                  item.GetUpdateTime(),
				UpdateTimeText:              formatGatewayMillis(item.GetUpdateTime()),
				ReduceOnly:                  item.GetReduceOnly(),
				ClosePosition:               item.GetClosePosition(),
				TimeInForce:                 item.GetTimeInForce(),
				EstimatedFee:                item.GetEstimatedFee(),
				ActualFee:                   item.GetActualFee(),
				ActualFeeAsset:              item.GetActualFeeAsset(),
				ActionType:                  item.GetActionType(),
				ActionLabel:                 actionTypeLabel(item.GetActionType()),
				PositionCycleID:             item.GetPositionCycleId(),
				Reason:                      item.GetReason(),
				ExitReasonKind:              exitReason.Kind,
				ExitReasonLabel:             exitReason.Label,
				ExitReasonDetail:            exitReason.Detail,
				SignalReasonListSummary:     signalReasonListSummary(signalReasonView),
				SignalReasonEntrySummary:    signalReasonEntrySummary(signalReasonView),
				SignalReasonFullSummary:     signalReasonFullSummary(signalReasonView),
				SignalReason:                signalReasonView,
				Protection:                  toProtectionStatusView(item.GetProtection()),
				HarvestPathProbability:      item.GetHarvestPathProbability(),
				HarvestPathRuleProbability:  item.GetHarvestPathRuleProbability(),
				HarvestPathLSTMProbability:  item.GetHarvestPathLstmProbability(),
				HarvestPathBookProbability:  item.GetHarvestPathBookProbability(),
				HarvestPathBookSummary:      item.GetHarvestPathBookSummary(),
				HarvestPathBookExplain:      harvestPathBookExplain(item.GetHarvestPathBookProbability(), item.GetHarvestPathBookSummary()),
				HarvestPathVolatilityRegime: item.GetHarvestPathVolatilityRegime(),
				HarvestPathThresholdSource:  item.GetHarvestPathThresholdSource(),
				HarvestPathAppliedThreshold: item.GetHarvestPathAppliedThreshold(),
				HarvestPathThresholdExplain: harvestPathThresholdExplain(
					item.GetHarvestPathVolatilityRegime(),
					item.GetHarvestPathThresholdSource(),
					item.GetHarvestPathAppliedThreshold(),
				),
				HarvestPathFusionExplain: harvestPathFusionExplain(
					item.GetHarvestPathRuleProbability(),
					item.GetHarvestPathLstmProbability(),
					item.GetHarvestPathBookProbability(),
					item.GetHarvestPathProbability(),
				),
				HarvestPathScoreFinal:     parseFloatString(item.GetHarvestPathProbability()),
				HarvestPathScoreRule:      parseFloatString(item.GetHarvestPathRuleProbability()),
				HarvestPathScoreLSTM:      parseFloatString(item.GetHarvestPathLstmProbability()),
				HarvestPathScoreBreakdown: harvestPathScoreBreakdown(item.GetHarvestPathRuleProbability(), item.GetHarvestPathLstmProbability(), item.GetHarvestPathBookProbability(), item.GetHarvestPathProbability()),
				HarvestPathModelLabel:     harvestPathModelLabel(),
				HarvestPathModelSummary:   harvestPathModelSummary(),
				HarvestPathAction:         action,
				HarvestPathRiskLevel:      riskLevel,
				HarvestPathTargetSide:     targetSide,
				HarvestPathReferencePrice: item.GetHarvestPathReferencePrice(),
				HarvestPathMarketPrice:    item.GetHarvestPathMarketPrice(),
				HarvestPathActionLabel:    harvestPathActionLabel(action),
				HarvestPathRiskLabel:      harvestPathRiskLabel(riskLevel),
				HarvestPathSummary:        harvestPathSummary(item.GetHarvestPathProbability(), action, riskLevel, targetSide),
			})
		}
		writeJSON(w, http.StatusOK, map[string]any{"items": items})
	}
}

func UserTradesHandler(serviceContext *svc.ServiceContext) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		req, ok := parseOrderQueryRequest(w, r)
		if !ok {
			return
		}
		resp, err := serviceContext.Order.GetUserTrades(r.Context(), req)
		if err != nil {
			writeError(w, http.StatusBadGateway, err.Error())
			return
		}
		items := make([]userTradeView, 0, len(resp.GetItems()))
		for _, item := range resp.GetItems() {
			items = append(items, userTradeView{
				ID:              item.GetId(),
				Symbol:          item.GetSymbol(),
				OrderID:         item.GetOrderId(),
				Side:            item.GetSide(),
				PositionSide:    item.GetPositionSide(),
				Price:           item.GetPrice(),
				Qty:             item.GetQty(),
				RealizedPnl:     item.GetRealizedPnl(),
				MarginAsset:     item.GetMarginAsset(),
				QuoteQty:        item.GetQuoteQty(),
				Commission:      item.GetCommission(),
				CommissionAsset: item.GetCommissionAsset(),
				Time:            item.GetTime(),
				TimeText:        formatGatewayMillis(item.GetTime()),
				Buyer:           item.GetBuyer(),
				Maker:           item.GetMaker(),
				EstimatedFee:    item.GetEstimatedFee(),
				ActualFee:       item.GetActualFee(),
			})
		}
		writeJSON(w, http.StatusOK, map[string]any{"items": items})
	}
}

func PositionCyclesHandler(serviceContext *svc.ServiceContext) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		req, ok := parseOrderQueryRequest(w, r)
		if !ok {
			return
		}
		resp, err := serviceContext.Order.GetAllOrders(r.Context(), req)
		if err != nil {
			writeError(w, http.StatusBadGateway, err.Error())
			return
		}
		items, _ := buildPositionCycleViews(resp.GetItems())
		writeJSON(w, http.StatusOK, map[string]any{"items": items})
	}
}

func PositionCyclesCSVHandler(serviceContext *svc.ServiceContext) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		req, ok := parseOrderQueryRequest(w, r)
		if !ok {
			return
		}
		orderResp, err := serviceContext.Order.GetAllOrders(r.Context(), req)
		if err != nil {
			writeError(w, http.StatusBadGateway, err.Error())
			return
		}
		cycleViews, orderToCycle := buildPositionCycleViews(orderResp.GetItems())

		tradeResp, err := serviceContext.Order.GetUserTrades(r.Context(), req)
		if err != nil {
			writeError(w, http.StatusBadGateway, err.Error())
			return
		}
		realizedPnlByCycle := make(map[string]float64, len(cycleViews))
		for _, item := range tradeResp.GetItems() {
			cycleID := strings.TrimSpace(orderToCycle[item.GetOrderId()])
			if cycleID == "" {
				continue
			}
			realizedPnlByCycle[cycleID] += parseFloatString(item.GetRealizedPnl())
		}

		fileName := fmt.Sprintf("position-cycles-%s.csv", req.GetSymbol())
		w.Header().Set("Content-Type", "text/csv; charset=utf-8")
		w.Header().Set("Content-Disposition", fmt.Sprintf("attachment; filename=%q", fileName))
		writer := csv.NewWriter(w)
		defer writer.Flush()

		if err := writer.Write([]string{
			"symbol",
			"trade_time",
			"order_ids",
			"entry_price",
			"position_qty",
			"fee",
			"fill_price",
			"realized_pnl",
		}); err != nil {
			writeError(w, http.StatusInternalServerError, err.Error())
			return
		}

		for _, item := range cycleViews {
			cycleID := strings.TrimSpace(item.PositionCycleID)
			if cycleID == "" {
				continue
			}
			pricePrecision, quantityPrecision := serviceContext.SymbolPrecisions(item.Symbol)
			if err := writer.Write([]string{
				item.Symbol,
				cycleTradeTime(item),
				joinInt64s(item.OpenOrderIDs, item.CloseOrderIDs),
				formatDecimalString(item.OpenAvgPrice, pricePrecision, "0"),
				formatDecimalString(item.OpenedQty, quantityPrecision, "0"),
				formatDecimalString(item.ActualFee, 8, "0"),
				formatDecimalString(cycleFillPrice(item), pricePrecision, "0"),
				formatFloatFixed(realizedPnlByCycle[cycleID], 8),
			}); err != nil {
				writeError(w, http.StatusInternalServerError, err.Error())
				return
			}
		}

		if err := writer.Error(); err != nil {
			writeError(w, http.StatusInternalServerError, err.Error())
		}
	}
}

func parseOrderQueryRequest(w http.ResponseWriter, r *http.Request) (*orderpb.OrderQueryRequest, bool) {
	symbol := strings.TrimSpace(strings.ToUpper(r.URL.Query().Get("symbol")))
	if symbol == "" {
		writeError(w, http.StatusBadRequest, "symbol is required")
		return nil, false
	}
	startTime, err := parseInt64Query(r, "start_time")
	if err != nil {
		writeError(w, http.StatusBadRequest, "invalid start_time")
		return nil, false
	}
	endTime, err := parseInt64Query(r, "end_time")
	if err != nil {
		writeError(w, http.StatusBadRequest, "invalid end_time")
		return nil, false
	}
	limit, err := parseInt32Query(r, "limit")
	if err != nil {
		writeError(w, http.StatusBadRequest, "invalid limit")
		return nil, false
	}
	return &orderpb.OrderQueryRequest{
		Symbol:    symbol,
		StartTime: startTime,
		EndTime:   endTime,
		Limit:     limit,
	}, true
}

func parseInt64Query(r *http.Request, key string) (int64, error) {
	raw := strings.TrimSpace(r.URL.Query().Get(key))
	if raw == "" {
		return 0, nil
	}
	return strconv.ParseInt(raw, 10, 64)
}

func parseInt32Query(r *http.Request, key string) (int32, error) {
	raw := strings.TrimSpace(r.URL.Query().Get(key))
	if raw == "" {
		return 0, nil
	}
	n, err := strconv.ParseInt(raw, 10, 32)
	return int32(n), err
}

func formatGatewayMillis(ms int64) string {
	if ms <= 0 {
		return ""
	}
	return time.UnixMilli(ms).UTC().Format("2006-01-02 15:04:05")
}

func buildPositionCycleViews(items []*orderpb.AllOrderItem) ([]positionCycleView, map[int64]string) {
	cycles := make(map[string]*positionCycleAgg)
	order := make([]string, 0)
	orderToCycle := make(map[int64]string, len(items))
	for _, item := range items {
		cycleID := strings.TrimSpace(item.GetPositionCycleId())
		if cycleID == "" {
			continue
		}
		orderToCycle[item.GetOrderId()] = cycleID
		agg, exists := cycles[cycleID]
		if !exists {
			agg = &positionCycleAgg{
				positionCycleID: cycleID,
				symbol:          item.GetSymbol(),
				positionSide:    item.GetPositionSide(),
			}
			cycles[cycleID] = agg
			order = append(order, cycleID)
		}
		agg.apply(item)
	}

	views := make([]positionCycleView, 0, len(order))
	for _, cycleID := range order {
		agg := cycles[cycleID]
		if agg == nil {
			continue
		}
		views = append(views, agg.view())
	}
	return views, orderToCycle
}

func cycleTradeTime(item positionCycleView) string {
	if strings.TrimSpace(item.CloseTimeText) != "" {
		return item.CloseTimeText
	}
	return item.OpenTimeText
}

func cycleFillPrice(item positionCycleView) string {
	if strings.TrimSpace(item.CloseAvgPrice) != "" && item.CloseAvgPrice != "0" {
		return item.CloseAvgPrice
	}
	return item.OpenAvgPrice
}

func joinInt64s(parts ...[]int64) string {
	total := 0
	for _, part := range parts {
		total += len(part)
	}
	values := make([]string, 0, total)
	for _, part := range parts {
		for _, v := range part {
			values = append(values, strconv.FormatInt(v, 10))
		}
	}
	return strings.Join(values, ",")
}

func (a *positionCycleAgg) apply(item *orderpb.AllOrderItem) {
	if a == nil || item == nil {
		return
	}
	actionType := strings.TrimSpace(item.GetActionType())
	eventTime := item.GetUpdateTime()
	if eventTime <= 0 {
		eventTime = item.GetTime()
	}
	qty := parseFloatString(item.GetExecutedQty())
	if qty == 0 {
		qty = parseFloatString(item.GetOrigQty())
	}
	avgPrice := parseFloatString(item.GetAvgPrice())

	a.estimatedFee += parseFloatString(item.GetEstimatedFee())
	a.actualFee += parseFloatString(item.GetActualFee())
	asset := strings.TrimSpace(item.GetActualFeeAsset())
	if asset != "" {
		if a.actualFeeAsset == "" {
			a.actualFeeAsset = asset
		} else if a.actualFeeAsset != asset {
			a.actualFeeMixed = true
		}
	}
	a.updateHarvestPathRisk(item)

	switch actionType {
	case "OPEN_LONG", "OPEN_SHORT":
		a.openOrderCount++
		a.openOrderIDs = append(a.openOrderIDs, item.GetOrderId())
		a.openQty += qty
		a.openNotional += qty * avgPrice
		if a.openTime == 0 || eventTime < a.openTime {
			a.openTime = eventTime
		}
		a.updateEntrySignalReason(item, eventTime)
	case "CLOSE_LONG", "CLOSE_SHORT", "PARTIAL_CLOSE_LONG", "PARTIAL_CLOSE_SHORT":
		a.closeOrderCount++
		a.closeOrderIDs = append(a.closeOrderIDs, item.GetOrderId())
		a.closeQty += qty
		a.closeNotional += qty * avgPrice
		if strings.HasPrefix(actionType, "PARTIAL_CLOSE_") {
			a.partialCloseOrderCount++
		} else {
			a.finalCloseOrderCount++
		}
		if eventTime > a.closeTime {
			a.closeTime = eventTime
		}
		a.updateExitReason(item, eventTime)
		a.updateExitSignalReason(item, eventTime)
	}
}

// actionTypeLabel 将订单动作类型转换为更直观的中文标签，方便查询列表直接展示。
func actionTypeLabel(actionType string) string {
	switch strings.TrimSpace(actionType) {
	case "OPEN_LONG":
		return "开多"
	case "OPEN_SHORT":
		return "开空"
	case "CLOSE_LONG":
		return "平多"
	case "CLOSE_SHORT":
		return "平空"
	case "PARTIAL_CLOSE_LONG":
		return "部分平多"
	case "PARTIAL_CLOSE_SHORT":
		return "部分平空"
	default:
		return ""
	}
}

func (a *positionCycleAgg) updateEntrySignalReason(item *orderpb.AllOrderItem, eventTime int64) {
	if a == nil || item == nil {
		return
	}
	reason := toSignalReasonView(item.GetSignalReason())
	if reason == nil {
		return
	}
	if a.entrySignalReason == nil || a.entrySignalTime == 0 || (eventTime > 0 && eventTime < a.entrySignalTime) {
		a.entrySignalReason = reason
		a.entrySignalTime = eventTime
	}
}

func (a *positionCycleAgg) updateExitSignalReason(item *orderpb.AllOrderItem, eventTime int64) {
	if a == nil || item == nil {
		return
	}
	reason := toSignalReasonView(item.GetSignalReason())
	if reason == nil {
		return
	}
	if a.exitSignalReason == nil || eventTime >= a.exitSignalTime {
		a.exitSignalReason = reason
		a.exitSignalTime = eventTime
	}
}

// updateExitReason 记录最新一笔出场动作的类型和原始原因，供持仓周期聚合生成稳定摘要。
func (a *positionCycleAgg) updateExitReason(item *orderpb.AllOrderItem, eventTime int64) {
	if a == nil || item == nil {
		return
	}
	if a.exitEventTime > eventTime {
		return
	}
	a.exitActionType = strings.TrimSpace(item.GetActionType())
	a.exitReason = strings.TrimSpace(item.GetReason())
	a.exitEventTime = eventTime
}

func (a *positionCycleAgg) updateHarvestPathRisk(item *orderpb.AllOrderItem) {
	if a == nil || item == nil {
		return
	}
	probability := parseFloatString(item.GetHarvestPathProbability())
	if probability <= 0 {
		return
	}
	if probability < a.maxHarvestPathProbability {
		return
	}
	a.maxHarvestPathProbability = probability
	a.maxHarvestPathRuleProbability = parseFloatString(item.GetHarvestPathRuleProbability())
	a.maxHarvestPathLSTMProbability = parseFloatString(item.GetHarvestPathLstmProbability())
	a.maxHarvestPathBookProbability = parseFloatString(item.GetHarvestPathBookProbability())
	a.harvestPathBookSummary = strings.TrimSpace(item.GetHarvestPathBookSummary())
	a.harvestPathVolatilityRegime = strings.TrimSpace(item.GetHarvestPathVolatilityRegime())
	a.harvestPathThresholdSource = strings.TrimSpace(item.GetHarvestPathThresholdSource())
	a.harvestPathAppliedThreshold = parseFloatString(item.GetHarvestPathAppliedThreshold())
	a.harvestPathAction = strings.TrimSpace(item.GetHarvestPathAction())
	a.harvestPathRiskLevel = strings.TrimSpace(item.GetHarvestPathRiskLevel())
	a.harvestPathTargetSide = strings.TrimSpace(item.GetHarvestPathTargetSide())
	a.harvestPathReferencePrice = strings.TrimSpace(item.GetHarvestPathReferencePrice())
	a.harvestPathMarketPrice = strings.TrimSpace(item.GetHarvestPathMarketPrice())
	a.signalReason = toSignalReasonView(item.GetSignalReason())
}

func (a *positionCycleAgg) view() positionCycleView {
	if a == nil {
		return positionCycleView{}
	}
	status := "OPEN"
	remaining := a.openQty - a.closeQty
	if remaining < 0 {
		remaining = 0
	}
	if a.openQty > 0 && a.closeQty+1e-9 >= a.openQty {
		status = "CLOSED"
	} else if a.closeQty > 0 {
		status = "PARTIALLY_CLOSED"
	}
	actualFeeAsset := a.actualFeeAsset
	if a.actualFeeMixed {
		actualFeeAsset = "MIXED"
	}
	exitReason := normalizeExitReason(a.exitActionType, a.exitReason, a.exitSignalReason)
	return positionCycleView{
		PositionCycleID:               a.positionCycleID,
		Symbol:                        a.symbol,
		PositionSide:                  a.positionSide,
		PositionSideLabel:             positionSideLabel(a.positionSide),
		CycleStatus:                   status,
		CycleStatusLabel:              cycleStatusLabel(status),
		OpenTime:                      a.openTime,
		OpenTimeText:                  formatGatewayMillis(a.openTime),
		CloseTime:                     a.closeTime,
		CloseTimeText:                 formatGatewayMillis(a.closeTime),
		OrderCount:                    a.openOrderCount + a.closeOrderCount,
		OpenOrderCount:                a.openOrderCount,
		CloseOrderCount:               a.closeOrderCount,
		OpenOrderIDs:                  a.openOrderIDs,
		CloseOrderIDs:                 a.closeOrderIDs,
		OpenedQty:                     formatFloatTrimmed(a.openQty, 8),
		ClosedQty:                     formatFloatTrimmed(a.closeQty, 8),
		RemainingQty:                  formatFloatTrimmed(remaining, 8),
		OpenAvgPrice:                  weightedAverage(a.openNotional, a.openQty),
		CloseAvgPrice:                 weightedAverage(a.closeNotional, a.closeQty),
		CloseProgress:                 closeProgressSummary(a.openQty, a.closeQty, remaining),
		EstimatedFee:                  formatFloatTrimmed(a.estimatedFee, 8),
		ActualFee:                     formatFloatTrimmed(a.actualFee, 8),
		ActualFeeAsset:                actualFeeAsset,
		MaxHarvestPathProbability:     formatFloatTrimmed(a.maxHarvestPathProbability, 4),
		MaxHarvestPathRuleProbability: formatFloatTrimmed(a.maxHarvestPathRuleProbability, 4),
		MaxHarvestPathLSTMProbability: formatFloatTrimmed(a.maxHarvestPathLSTMProbability, 4),
		MaxHarvestPathBookProbability: formatFloatTrimmed(a.maxHarvestPathBookProbability, 4),
		HarvestPathBookSummary:        a.harvestPathBookSummary,
		HarvestPathBookExplain:        harvestPathBookExplain(formatFloatTrimmed(a.maxHarvestPathBookProbability, 4), a.harvestPathBookSummary),
		HarvestPathVolatilityRegime:   a.harvestPathVolatilityRegime,
		HarvestPathThresholdSource:    a.harvestPathThresholdSource,
		HarvestPathAppliedThreshold:   formatFloatTrimmed(a.harvestPathAppliedThreshold, 4),
		HarvestPathThresholdExplain: harvestPathThresholdExplain(
			a.harvestPathVolatilityRegime,
			a.harvestPathThresholdSource,
			formatFloatTrimmed(a.harvestPathAppliedThreshold, 4),
		),
		HarvestPathFusionExplain: harvestPathFusionExplain(
			formatFloatTrimmed(a.maxHarvestPathRuleProbability, 4),
			formatFloatTrimmed(a.maxHarvestPathLSTMProbability, 4),
			formatFloatTrimmed(a.maxHarvestPathBookProbability, 4),
			formatFloatTrimmed(a.maxHarvestPathProbability, 4),
		),
		HarvestPathScoreFinal: a.maxHarvestPathProbability,
		HarvestPathScoreRule:  a.maxHarvestPathRuleProbability,
		HarvestPathScoreLSTM:  a.maxHarvestPathLSTMProbability,
		HarvestPathScoreBreakdown: harvestPathScoreBreakdown(
			formatFloatTrimmed(a.maxHarvestPathRuleProbability, 4),
			formatFloatTrimmed(a.maxHarvestPathLSTMProbability, 4),
			formatFloatTrimmed(a.maxHarvestPathBookProbability, 4),
			formatFloatTrimmed(a.maxHarvestPathProbability, 4),
		),
		HarvestPathModelLabel:     harvestPathModelLabel(),
		HarvestPathModelSummary:   harvestPathModelSummary(),
		HarvestPathAction:         a.harvestPathAction,
		HarvestPathRiskLevel:      a.harvestPathRiskLevel,
		HarvestPathTargetSide:     a.harvestPathTargetSide,
		HarvestPathReferencePrice: a.harvestPathReferencePrice,
		HarvestPathMarketPrice:    a.harvestPathMarketPrice,
		HarvestPathActionLabel:    harvestPathActionLabel(a.harvestPathAction),
		HarvestPathRiskLabel:      harvestPathRiskLabel(a.harvestPathRiskLevel),
		HarvestPathSummary:        harvestPathSummary(formatFloatTrimmed(a.maxHarvestPathProbability, 4), a.harvestPathAction, a.harvestPathRiskLevel, a.harvestPathTargetSide),
		EntryReasonSummary:        signalReasonEntrySummary(a.entrySignalReason),
		ExitReasonSummary:         signalReasonSummary(a.exitSignalReason),
		ExitReasonKind:            exitReason.Kind,
		ExitReasonLabel:           exitReason.Label,
		ExitReasonDetail:          exitReason.Detail,
		DominantReasonSummary:     signalReasonFullSummary(a.signalReason),
		EntryPhase:                signalReasonPhase(a.entrySignalReason),
		ExitPhase:                 signalReasonPhase(a.exitSignalReason),
		EntryTags:                 signalReasonTags(a.entrySignalReason),
		ExitTags:                  signalReasonTags(a.exitSignalReason),
		EntryTagsText:             signalReasonTagsText(a.entrySignalReason),
		ExitTagsText:              signalReasonTagsText(a.exitSignalReason),
		PartialCloseOrderCount:    a.partialCloseOrderCount,
		FinalCloseOrderCount:      a.finalCloseOrderCount,
		ExitActionSummary:         exitActionSummary(status, a.partialCloseOrderCount, a.finalCloseOrderCount),
		CycleReasonBrief:          cycleReasonBrief(a.entrySignalReason, a.signalReason),
		EntrySignalReason:         cloneSignalReasonView(a.entrySignalReason),
		ExitSignalReason:          cloneSignalReasonView(a.exitSignalReason),
		DominantSignalReason:      cloneSignalReasonView(a.signalReason),
		SignalReason:              cloneSignalReasonView(a.signalReason),
	}
}

// positionSideLabel 将持仓方向枚举转换为中文标签，方便列表直接展示多空含义。
func positionSideLabel(positionSide string) string {
	switch strings.TrimSpace(positionSide) {
	case "LONG":
		return "多头"
	case "SHORT":
		return "空头"
	default:
		return ""
	}
}

// cycleStatusLabel 将持仓周期状态转换为中文标签，避免前端重复维护映射。
func cycleStatusLabel(status string) string {
	switch strings.TrimSpace(status) {
	case "OPEN":
		return "持仓中"
	case "PARTIALLY_CLOSED":
		return "部分平仓"
	case "CLOSED":
		return "已平仓"
	default:
		return ""
	}
}

// closeProgressSummary 生成平仓进度摘要，直接展示已平数量与剩余数量。
func closeProgressSummary(openQty, closeQty, remaining float64) string {
	if openQty <= 0 {
		return ""
	}
	return fmt.Sprintf("已平%s / %s，剩余%s", formatFloatTrimmed(closeQty, 8), formatFloatTrimmed(openQty, 8), formatFloatTrimmed(remaining, 8))
}

// exitActionSummary 生成平仓动作摘要，区分部分止盈与最终平仓的组合情况。
func exitActionSummary(status string, partialCloseCount, finalCloseCount int) string {
	switch {
	case partialCloseCount > 0 && finalCloseCount > 0:
		return fmt.Sprintf("部分止盈%d次后最终平仓", partialCloseCount)
	case partialCloseCount > 0 && strings.TrimSpace(status) == "PARTIALLY_CLOSED":
		return fmt.Sprintf("已部分平仓%d次", partialCloseCount)
	case finalCloseCount > 0 && partialCloseCount == 0:
		return "直接平仓"
	default:
		return ""
	}
}

func harvestPathActionLabel(action string) string {
	switch strings.TrimSpace(action) {
	case "WAIT_FOR_RECLAIM":
		return "等待回收确认"
	case "REDUCE_PROBE_SIZE":
		return "缩小试探仓位"
	case "FOLLOW_PATH":
		return "顺路执行"
	default:
		return ""
	}
}

func harvestPathRiskLabel(level string) string {
	switch strings.TrimSpace(level) {
	case "PATH_ALERT":
		return "路径预警"
	case "PATH_PRESSURE":
		return "路径压力"
	case "PATH_CLEAR":
		return "路径通畅"
	default:
		return ""
	}
}

func harvestPathSummary(probability, action, riskLevel, targetSide string) string {
	parts := make([]string, 0, 4)
	if label := harvestPathRiskLabel(riskLevel); label != "" {
		parts = append(parts, label)
	}
	if label := harvestPathActionLabel(action); label != "" {
		parts = append(parts, label)
	}
	if probability = strings.TrimSpace(probability); probability != "" {
		parts = append(parts, "概率="+probability)
	}
	if targetSide = strings.TrimSpace(targetSide); targetSide != "" {
		parts = append(parts, "目标="+targetSide)
	}
	return strings.Join(parts, " | ")
}

func harvestPathModelLabel() string {
	return "LSTM + Book"
}

func harvestPathModelSummary() string {
	return "liquidity sweep risk + LSTM + 订单簿轻量编码器"
}

func toSignalReasonView(v *orderpb.SignalReason) *signalReasonView {
	if v == nil {
		return nil
	}
	out := &signalReasonView{
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
		Router:           toSignalRouterView(v),
		Allocator:        toPositionAllocatorView(v.GetAllocator()),
		Range:            toSignalRangeView(v.GetRange()),
	}
	if tags := v.GetTags(); len(tags) > 0 {
		out.Tags = append([]string(nil), tags...)
	}
	if out.Summary == "" && out.Phase == "" && out.TrendContext == "" && out.SetupContext == "" && out.PathContext == "" && out.ExecutionContext == "" && out.ExitReasonKind == "" && out.ExitReasonLabel == "" && out.RouteBucket == "" && out.RouteReason == "" && out.RouteTemplate == "" && len(out.Tags) == 0 && out.Router == nil && out.Allocator == nil && out.Range == nil {
		return nil
	}
	return out
}

// toSignalRouterView 将 signal_reason 中的统一路由字段整理成 query 侧一致的 router 视图。
func toSignalRouterView(v *orderpb.SignalReason) *signalRouterView {
	if v == nil {
		return nil
	}
	out := &signalRouterView{
		RouteBucket:  strings.TrimSpace(v.GetRouteBucket()),
		TargetReason: strings.TrimSpace(v.GetRouteReason()),
		Template:     strings.TrimSpace(v.GetRouteTemplate()),
	}
	if out.RouteBucket == "" && out.TargetReason == "" && out.Template == "" {
		return nil
	}
	return out
}

// toSignalRangeView 将 signal_reason 中的 range 摘要整理为前端可直接消费的布尔位。
func toSignalRangeView(v *orderpb.RangeSignalReason) *signalRangeView {
	if v == nil {
		return nil
	}
	out := &signalRangeView{
		H1RangeOK:      v.GetH1RangeOk(),
		H1AdxOK:        v.GetH1AdxOk(),
		H1BollWidthOK:  v.GetH1BollWidthOk(),
		M15TouchLower:  v.GetM15TouchLower(),
		M15RsiTurnUp:   v.GetM15RsiTurnUp(),
		M15TouchUpper:  v.GetM15TouchUpper(),
		M15RsiTurnDown: v.GetM15RsiTurnDown(),
	}
	out.RegimeSummary = signalRangeRegimeSummary(out)
	out.EntrySummary = signalRangeEntrySummary(out)
	out.FullSummary = signalRangeFullSummary(out)
	out.Summary = out.FullSummary
	if !out.H1RangeOK && !out.H1AdxOK && !out.H1BollWidthOK && !out.M15TouchLower && !out.M15RsiTurnUp && !out.M15TouchUpper && !out.M15RsiTurnDown && out.FullSummary == "" {
		return nil
	}
	return out
}

// signalRangeRegimeSummary 生成 1H 震荡状态摘要，适合列表快速展示大周期状态。
func signalRangeRegimeSummary(v *signalRangeView) string {
	if v == nil {
		return ""
	}
	if v.H1RangeOK {
		return "1H震荡成立"
	}
	if v.H1AdxOK || v.H1BollWidthOK {
		return "1H震荡未完全成立"
	}
	return ""
}

// signalRangeEntrySummary 生成 15M 入场触发摘要，适合详情区单独展示触发条件。
func signalRangeEntrySummary(v *signalRangeView) string {
	if v == nil {
		return ""
	}
	parts := make([]string, 0, 2)
	if v.M15TouchLower {
		parts = append(parts, "15M触下轨")
	}
	if v.M15RsiTurnUp {
		parts = append(parts, "RSI拐头向上")
	}
	if v.M15TouchUpper {
		parts = append(parts, "15M触上轨")
	}
	if v.M15RsiTurnDown {
		parts = append(parts, "RSI拐头向下")
	}
	return strings.Join(parts, " | ")
}

// signalRangeFullSummary 组合大周期与入场周期摘要，生成完整展示文案。
func signalRangeFullSummary(v *signalRangeView) string {
	if v == nil {
		return ""
	}
	parts := make([]string, 0, 2)
	if regime := signalRangeRegimeSummary(v); regime != "" {
		parts = append(parts, regime)
	}
	if entry := signalRangeEntrySummary(v); entry != "" {
		parts = append(parts, entry)
	}
	return strings.Join(parts, " | ")
}

// toPositionAllocatorView 将 protobuf allocator 快照整理为 query 侧可直接展示的结构。
func toPositionAllocatorView(v *orderpb.PositionAllocatorStatus) *positionAllocatorView {
	if v == nil {
		return nil
	}
	out := &positionAllocatorView{
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

func cloneSignalReasonView(v *signalReasonView) *signalReasonView {
	if v == nil {
		return nil
	}
	out := *v
	if len(v.Tags) > 0 {
		out.Tags = append([]string(nil), v.Tags...)
	}
	if v.Router != nil {
		router := *v.Router
		out.Router = &router
	}
	if v.Allocator != nil {
		allocator := *v.Allocator
		out.Allocator = &allocator
	}
	if v.Range != nil {
		rangeView := *v.Range
		out.Range = &rangeView
	}
	return &out
}

// toProtectionStatusView 将 protobuf protection 结果整理为前端可直接展示的结构。
func toProtectionStatusView(v *orderpb.ProtectionStatus) *protectionStatusView {
	if v == nil {
		return nil
	}
	out := &protectionStatusView{
		Requested:  v.GetRequested(),
		Status:     strings.TrimSpace(v.GetStatus()),
		Reason:     strings.TrimSpace(v.GetReason()),
		StopLoss:   toProtectionLegStatusView(v.GetStopLoss()),
		TakeProfit: toProtectionLegStatusView(v.GetTakeProfit()),
	}
	out.Summary = protectionSummary(out)
	if !out.Requested && out.Status == "" && out.Reason == "" && out.StopLoss == nil && out.TakeProfit == nil && out.Summary == "" {
		return nil
	}
	return out
}

// toProtectionLegStatusView 将 protobuf protection 腿结果整理为前端结构。
func toProtectionLegStatusView(v *orderpb.ProtectionLegStatus) *protectionLegStatusView {
	if v == nil {
		return nil
	}
	out := &protectionLegStatusView{
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

// protectionSummary 生成保护单结果摘要，方便前端列表直接展示。
func protectionSummary(v *protectionStatusView) string {
	if v == nil {
		return ""
	}
	parts := make([]string, 0, 3)
	if status := strings.TrimSpace(v.Status); status != "" {
		parts = append(parts, "status="+status)
	}
	if stopLoss := v.StopLoss; stopLoss != nil && stopLoss.Requested {
		parts = append(parts, "sl="+firstNonEmpty(stopLoss.Status, "requested"))
	}
	if takeProfit := v.TakeProfit; takeProfit != nil && takeProfit.Requested {
		parts = append(parts, "tp="+firstNonEmpty(takeProfit.Status, "requested"))
	}
	if len(parts) == 0 {
		return strings.TrimSpace(v.Reason)
	}
	summary := strings.Join(parts, " | ")
	if reason := strings.TrimSpace(v.Reason); reason != "" {
		return summary + " | " + reason
	}
	return summary
}

// firstNonEmpty 返回第一个非空字符串，方便组合摘要文案。
func firstNonEmpty(values ...string) string {
	for _, value := range values {
		if strings.TrimSpace(value) != "" {
			return strings.TrimSpace(value)
		}
	}
	return ""
}

func signalReasonSummary(v *signalReasonView) string {
	if v == nil {
		return ""
	}
	return strings.TrimSpace(v.Summary)
}

// normalizeExitReason 统一归一化出场原因，优先使用动作类型，其次回退到 signal_reason 和原始 reason 文本。
func normalizeExitReason(actionType, rawReason string, signalReason *signalReasonView) exitReasonView {
	actionType = strings.TrimSpace(actionType)
	rawReason = firstNonEmpty(strings.TrimSpace(rawReason), signalReasonSummary(signalReason))
	if !isExitAction(actionType) {
		return exitReasonView{}
	}
	if signalReason != nil && strings.TrimSpace(signalReason.ExitReasonKind) != "" {
		return exitReasonView{
			Kind:   strings.TrimSpace(signalReason.ExitReasonKind),
			Label:  firstNonEmpty(strings.TrimSpace(signalReason.ExitReasonLabel), exitReasonLabelFromKind(signalReason.ExitReasonKind)),
			Detail: rawReason,
		}
	}

	switch {
	case strings.HasPrefix(actionType, "PARTIAL_CLOSE_"):
		return exitReasonView{Kind: "partial_take_profit", Label: "分批止盈", Detail: rawReason}
	case containsAny(rawReason, "RSI止盈", "rsi exit", "rsi_take_profit"):
		return exitReasonView{Kind: "rsi_take_profit", Label: "RSI止盈", Detail: rawReason}
	case containsAny(rawReason, "时间止损", "time stop", "time_stop"):
		return exitReasonView{Kind: "time_stop", Label: "时间止损", Detail: rawReason}
	case containsAny(rawReason, "目标止盈", "take profit", "tp hit"):
		return exitReasonView{Kind: "take_profit", Label: "目标止盈", Detail: rawReason}
	case containsAny(rawReason, "止损", "stop loss"):
		if isBreakevenExit(rawReason, signalReason) {
			return exitReasonView{Kind: "break_even_stop", Label: "保本止损", Detail: rawReason}
		}
		return exitReasonView{Kind: "stop_loss", Label: "止损", Detail: rawReason}
	default:
		return exitReasonView{Kind: "final_close", Label: "最终平仓", Detail: rawReason}
	}
}

// exitReasonLabelFromKind 为结构化 kind 提供稳定中文标签，兼容旧数据只下发 kind 不下发 label 的场景。
func exitReasonLabelFromKind(kind string) string {
	switch strings.TrimSpace(kind) {
	case "partial_take_profit":
		return "分批止盈"
	case "take_profit":
		return "目标止盈"
	case "break_even_stop":
		return "保本止损"
	case "rsi_take_profit":
		return "RSI止盈"
	case "time_stop":
		return "时间止损"
	case "stop_loss":
		return "止损"
	case "final_close":
		return "最终平仓"
	default:
		return ""
	}
}

// isExitAction 判断当前动作是否属于离场动作，避免把开仓信号错误归类到 exit reason。
func isExitAction(actionType string) bool {
	actionType = strings.TrimSpace(actionType)
	return strings.HasPrefix(actionType, "CLOSE_") || strings.HasPrefix(actionType, "PARTIAL_CLOSE_")
}

// isBreakevenExit 使用 execution_context 里的预计盈亏近似识别保本止损，兼容略有滑点的实盘结果。
func isBreakevenExit(rawReason string, signalReason *signalReasonView) bool {
	if !containsAny(rawReason, "止损", "stop loss") {
		return false
	}
	pnl, ok := extractPNLFromExecutionContext(signalReason)
	if !ok {
		return false
	}
	return pnl >= -0.05
}

// extractPNLFromExecutionContext 从 execution_context 中提取“预计盈亏”字段，供保本止损判定复用。
func extractPNLFromExecutionContext(signalReason *signalReasonView) (float64, bool) {
	if signalReason == nil {
		return 0, false
	}
	executionContext := strings.TrimSpace(signalReason.ExecutionContext)
	if executionContext == "" {
		return 0, false
	}
	for _, marker := range []string{"预计盈亏=", "已实现盈亏="} {
		idx := strings.Index(executionContext, marker)
		if idx < 0 {
			continue
		}
		start := idx + len(marker)
		end := start
		for end < len(executionContext) {
			ch := executionContext[end]
			if (ch >= '0' && ch <= '9') || ch == '.' || ch == '-' || ch == '+' {
				end++
				continue
			}
			break
		}
		if end <= start {
			continue
		}
		if value, err := strconv.ParseFloat(executionContext[start:end], 64); err == nil {
			return value, true
		}
	}
	return 0, false
}

// containsAny 判断文本是否包含任一关键片段，用于把自由文本 reason 归一化为稳定分类。
func containsAny(text string, subs ...string) bool {
	text = strings.ToLower(strings.TrimSpace(text))
	if text == "" {
		return false
	}
	for _, sub := range subs {
		if strings.Contains(text, strings.ToLower(strings.TrimSpace(sub))) {
			return true
		}
	}
	return false
}

// signalReasonListSummary 返回订单列表默认展示的简短摘要，优先使用 range 的 regime_summary。
func signalReasonListSummary(v *signalReasonView) string {
	if v == nil {
		return ""
	}
	if v.Range != nil && strings.TrimSpace(v.Range.RegimeSummary) != "" {
		return strings.TrimSpace(v.Range.RegimeSummary)
	}
	return signalReasonSummary(v)
}

// signalReasonEntrySummary 返回详情页更适合展示的入场触发摘要，优先使用 range 的 entry_summary。
func signalReasonEntrySummary(v *signalReasonView) string {
	if v == nil {
		return ""
	}
	if v.Range != nil && strings.TrimSpace(v.Range.EntrySummary) != "" {
		return strings.TrimSpace(v.Range.EntrySummary)
	}
	return signalReasonSummary(v)
}

// signalReasonFullSummary 返回详情页完整摘要，优先使用 range 的 full_summary。
func signalReasonFullSummary(v *signalReasonView) string {
	if v == nil {
		return ""
	}
	if v.Range != nil && strings.TrimSpace(v.Range.FullSummary) != "" {
		return strings.TrimSpace(v.Range.FullSummary)
	}
	return signalReasonSummary(v)
}

func signalReasonPhase(v *signalReasonView) string {
	if v == nil {
		return ""
	}
	return strings.TrimSpace(v.Phase)
}

func signalReasonTags(v *signalReasonView) []string {
	if v == nil || len(v.Tags) == 0 {
		return nil
	}
	return append([]string(nil), v.Tags...)
}

func signalReasonTagsText(v *signalReasonView) string {
	if v == nil || len(v.Tags) == 0 {
		return ""
	}
	return strings.Join(v.Tags, " / ")
}

func harvestPathScoreBreakdown(ruleProbability, lstmProbability, bookProbability, finalProbability string) string {
	parts := make([]string, 0, 4)
	if v := strings.TrimSpace(ruleProbability); v != "" {
		parts = append(parts, "rule="+v)
	}
	if v := strings.TrimSpace(lstmProbability); v != "" {
		parts = append(parts, "lstm="+v)
	}
	if v := strings.TrimSpace(bookProbability); v != "" {
		parts = append(parts, "book="+v)
	}
	if v := strings.TrimSpace(finalProbability); v != "" {
		parts = append(parts, "final="+v)
	}
	return strings.Join(parts, " | ")
}

func harvestPathThresholdExplain(regime, source, threshold string) string {
	parts := make([]string, 0, 3)
	if v := strings.TrimSpace(regime); v != "" {
		parts = append(parts, v)
	}
	if v := strings.TrimSpace(source); v != "" {
		parts = append(parts, v)
	}
	if v := strings.TrimSpace(threshold); v != "" {
		parts = append(parts, v)
	}
	return strings.Join(parts, " / ")
}

func harvestPathBookExplain(probability, summary string) string {
	parts := make([]string, 0, 2)
	if v := strings.TrimSpace(probability); v != "" {
		parts = append(parts, v)
	}
	if v := strings.TrimSpace(summary); v != "" {
		parts = append(parts, v)
	}
	return strings.Join(parts, " / ")
}

func harvestPathFusionExplain(ruleProbability, lstmProbability, bookProbability, finalProbability string) string {
	parts := make([]string, 0, 4)
	if v := strings.TrimSpace(ruleProbability); v != "" {
		parts = append(parts, "rule="+v)
	}
	if v := strings.TrimSpace(lstmProbability); v != "" {
		parts = append(parts, "lstm="+v)
	}
	if v := strings.TrimSpace(bookProbability); v != "" {
		parts = append(parts, "book="+v)
	}
	if v := strings.TrimSpace(finalProbability); v != "" {
		parts = append(parts, "final="+v)
	}
	return strings.Join(parts, " / ")
}

func cycleReasonBrief(entry, dominant *signalReasonView) string {
	base := entry
	if base == nil {
		base = dominant
	}
	if base == nil {
		return ""
	}

	parts := make([]string, 0, 3)
	if phase := signalReasonPhase(base); phase != "" {
		parts = append(parts, phase)
	}
	if tagsText := signalReasonTagsText(base); tagsText != "" {
		parts = append(parts, tagsText)
	}
	if summary := signalReasonFullSummary(base); summary != "" {
		parts = append(parts, summary)
	}
	return strings.Join(parts, " | ")
}

func weightedAverage(notional, qty float64) string {
	if qty <= 0 {
		return "0"
	}
	return formatFloatTrimmed(notional/qty, 8)
}

func parseFloatString(v string) float64 {
	raw := strings.TrimSpace(v)
	if raw == "" {
		return 0
	}
	f, err := strconv.ParseFloat(raw, 64)
	if err != nil {
		return 0
	}
	return f
}

func formatFloatTrimmed(v float64, precision int) string {
	if precision < 0 {
		precision = 0
	}
	s := strconv.FormatFloat(v, 'f', precision, 64)
	s = strings.TrimRight(s, "0")
	s = strings.TrimRight(s, ".")
	if s == "" || s == "-0" {
		return "0"
	}
	return s
}

func formatDecimalString(v string, precision int, zeroValue string) string {
	raw := strings.TrimSpace(v)
	if raw == "" {
		return zeroValue
	}
	f, err := strconv.ParseFloat(raw, 64)
	if err != nil {
		return raw
	}
	if precision < 0 {
		precision = 0
	}
	return strconv.FormatFloat(f, 'f', precision, 64)
}

func formatFloatFixed(v float64, precision int) string {
	if precision < 0 {
		precision = 0
	}
	return strconv.FormatFloat(v, 'f', precision, 64)
}
