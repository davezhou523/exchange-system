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
	OrderID                     int64             `json:"order_id"`
	Symbol                      string            `json:"symbol"`
	Status                      string            `json:"status"`
	Side                        string            `json:"side"`
	PositionSide                string            `json:"position_side"`
	Type                        string            `json:"type"`
	OrigQty                     string            `json:"orig_qty"`
	ExecutedQty                 string            `json:"executed_qty"`
	AvgPrice                    string            `json:"avg_price"`
	Price                       string            `json:"price"`
	StopPrice                   string            `json:"stop_price"`
	ClientOrderID               string            `json:"client_order_id"`
	Time                        int64             `json:"time"`
	TimeText                    string            `json:"time_text"`
	UpdateTime                  int64             `json:"update_time"`
	UpdateTimeText              string            `json:"update_time_text"`
	ReduceOnly                  bool              `json:"reduce_only"`
	ClosePosition               bool              `json:"close_position"`
	TimeInForce                 string            `json:"time_in_force"`
	EstimatedFee                string            `json:"estimated_fee"`
	ActualFee                   string            `json:"actual_fee"`
	ActualFeeAsset              string            `json:"actual_fee_asset"`
	ActionType                  string            `json:"action_type"`
	PositionCycleID             string            `json:"position_cycle_id"`
	Reason                      string            `json:"reason"`
	SignalReason                *signalReasonView `json:"signal_reason,omitempty"`
	HarvestPathProbability      string            `json:"harvest_path_probability"`
	HarvestPathRuleProbability  string            `json:"harvest_path_rule_probability"`
	HarvestPathLSTMProbability  string            `json:"harvest_path_lstm_probability"`
	HarvestPathBookProbability  string            `json:"harvest_path_book_probability"`
	HarvestPathBookSummary      string            `json:"harvest_path_book_summary"`
	HarvestPathBookExplain      string            `json:"harvest_path_book_explain"`
	HarvestPathVolatilityRegime string            `json:"harvest_path_volatility_regime"`
	HarvestPathThresholdSource  string            `json:"harvest_path_threshold_source"`
	HarvestPathAppliedThreshold string            `json:"harvest_path_applied_threshold"`
	HarvestPathThresholdExplain string            `json:"harvest_path_threshold_explain"`
	HarvestPathFusionExplain    string            `json:"harvest_path_fusion_explain"`
	HarvestPathScoreFinal       float64           `json:"harvest_path_score_final"`
	HarvestPathScoreRule        float64           `json:"harvest_path_score_rule"`
	HarvestPathScoreLSTM        float64           `json:"harvest_path_score_lstm"`
	HarvestPathScoreBreakdown   string            `json:"harvest_path_score_breakdown"`
	HarvestPathModelLabel       string            `json:"harvest_path_model_label"`
	HarvestPathModelSummary     string            `json:"harvest_path_model_summary"`
	HarvestPathAction           string            `json:"harvest_path_action"`
	HarvestPathRiskLevel        string            `json:"harvest_path_risk_level"`
	HarvestPathTargetSide       string            `json:"harvest_path_target_side"`
	HarvestPathReferencePrice   string            `json:"harvest_path_reference_price"`
	HarvestPathMarketPrice      string            `json:"harvest_path_market_price"`
	HarvestPathActionLabel      string            `json:"harvest_path_action_label"`
	HarvestPathRiskLabel        string            `json:"harvest_path_risk_label"`
	HarvestPathSummary          string            `json:"harvest_path_summary"`
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
	CycleStatus                   string            `json:"cycle_status"`
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
	DominantReasonSummary         string            `json:"dominant_reason_summary"`
	EntryPhase                    string            `json:"entry_phase"`
	ExitPhase                     string            `json:"exit_phase"`
	EntryTags                     []string          `json:"entry_tags,omitempty"`
	ExitTags                      []string          `json:"exit_tags,omitempty"`
	EntryTagsText                 string            `json:"entry_tags_text"`
	ExitTagsText                  string            `json:"exit_tags_text"`
	CycleReasonBrief              string            `json:"cycle_reason_brief"`
	EntrySignalReason             *signalReasonView `json:"entry_signal_reason,omitempty"`
	ExitSignalReason              *signalReasonView `json:"exit_signal_reason,omitempty"`
	DominantSignalReason          *signalReasonView `json:"dominant_signal_reason,omitempty"`
	SignalReason                  *signalReasonView `json:"signal_reason,omitempty"`
}

type signalReasonView struct {
	Summary          string   `json:"summary,omitempty"`
	Phase            string   `json:"phase,omitempty"`
	TrendContext     string   `json:"trend_context,omitempty"`
	SetupContext     string   `json:"setup_context,omitempty"`
	PathContext      string   `json:"path_context,omitempty"`
	ExecutionContext string   `json:"execution_context,omitempty"`
	Tags             []string `json:"tags,omitempty"`
}

type positionCycleAgg struct {
	positionCycleID               string
	symbol                        string
	positionSide                  string
	openTime                      int64
	closeTime                     int64
	openOrderCount                int
	closeOrderCount               int
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
	signalReason                  *signalReasonView
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
				PositionCycleID:             item.GetPositionCycleId(),
				Reason:                      item.GetReason(),
				SignalReason:                toSignalReasonView(item.GetSignalReason()),
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
	case "CLOSE_LONG", "CLOSE_SHORT":
		a.closeOrderCount++
		a.closeOrderIDs = append(a.closeOrderIDs, item.GetOrderId())
		a.closeQty += qty
		a.closeNotional += qty * avgPrice
		if eventTime > a.closeTime {
			a.closeTime = eventTime
		}
		a.updateExitSignalReason(item, eventTime)
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
	return positionCycleView{
		PositionCycleID:               a.positionCycleID,
		Symbol:                        a.symbol,
		PositionSide:                  a.positionSide,
		CycleStatus:                   status,
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
		EntryReasonSummary:        signalReasonSummary(a.entrySignalReason),
		ExitReasonSummary:         signalReasonSummary(a.exitSignalReason),
		DominantReasonSummary:     signalReasonSummary(a.signalReason),
		EntryPhase:                signalReasonPhase(a.entrySignalReason),
		ExitPhase:                 signalReasonPhase(a.exitSignalReason),
		EntryTags:                 signalReasonTags(a.entrySignalReason),
		ExitTags:                  signalReasonTags(a.exitSignalReason),
		EntryTagsText:             signalReasonTagsText(a.entrySignalReason),
		ExitTagsText:              signalReasonTagsText(a.exitSignalReason),
		CycleReasonBrief:          cycleReasonBrief(a.entrySignalReason, a.signalReason),
		EntrySignalReason:         cloneSignalReasonView(a.entrySignalReason),
		ExitSignalReason:          cloneSignalReasonView(a.exitSignalReason),
		DominantSignalReason:      cloneSignalReasonView(a.signalReason),
		SignalReason:              cloneSignalReasonView(a.signalReason),
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
	}
	if tags := v.GetTags(); len(tags) > 0 {
		out.Tags = append([]string(nil), tags...)
	}
	if out.Summary == "" && out.Phase == "" && out.TrendContext == "" && out.SetupContext == "" && out.PathContext == "" && out.ExecutionContext == "" && len(out.Tags) == 0 {
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
	return &out
}

func signalReasonSummary(v *signalReasonView) string {
	if v == nil {
		return ""
	}
	return strings.TrimSpace(v.Summary)
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
	if summary := signalReasonSummary(base); summary != "" {
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
