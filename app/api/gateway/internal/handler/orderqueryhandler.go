package handler

import (
	"net/http"
	"strconv"
	"strings"
	"time"

	"exchange-system/app/api/gateway/internal/svc"
	orderpb "exchange-system/common/pb/order"
)

type allOrderView struct {
	OrderID         int64  `json:"order_id"`
	Symbol          string `json:"symbol"`
	Status          string `json:"status"`
	Side            string `json:"side"`
	PositionSide    string `json:"position_side"`
	Type            string `json:"type"`
	OrigQty         string `json:"orig_qty"`
	ExecutedQty     string `json:"executed_qty"`
	AvgPrice        string `json:"avg_price"`
	Price           string `json:"price"`
	StopPrice       string `json:"stop_price"`
	ClientOrderID   string `json:"client_order_id"`
	Time            int64  `json:"time"`
	TimeText        string `json:"time_text"`
	UpdateTime      int64  `json:"update_time"`
	UpdateTimeText  string `json:"update_time_text"`
	ReduceOnly      bool   `json:"reduce_only"`
	ClosePosition   bool   `json:"close_position"`
	TimeInForce     string `json:"time_in_force"`
	EstimatedFee    string `json:"estimated_fee"`
	ActualFee       string `json:"actual_fee"`
	ActualFeeAsset  string `json:"actual_fee_asset"`
	ActionType      string `json:"action_type"`
	PositionCycleID string `json:"position_cycle_id"`
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
	PositionCycleID string  `json:"position_cycle_id"`
	Symbol          string  `json:"symbol"`
	PositionSide    string  `json:"position_side"`
	CycleStatus     string  `json:"cycle_status"`
	OpenTime        int64   `json:"open_time"`
	OpenTimeText    string  `json:"open_time_text"`
	CloseTime       int64   `json:"close_time"`
	CloseTimeText   string  `json:"close_time_text"`
	OrderCount      int     `json:"order_count"`
	OpenOrderCount  int     `json:"open_order_count"`
	CloseOrderCount int     `json:"close_order_count"`
	OpenOrderIDs    []int64 `json:"open_order_ids"`
	CloseOrderIDs   []int64 `json:"close_order_ids"`
	OpenedQty       string  `json:"opened_qty"`
	ClosedQty       string  `json:"closed_qty"`
	RemainingQty    string  `json:"remaining_qty"`
	OpenAvgPrice    string  `json:"open_avg_price"`
	CloseAvgPrice   string  `json:"close_avg_price"`
	EstimatedFee    string  `json:"estimated_fee"`
	ActualFee       string  `json:"actual_fee"`
	ActualFeeAsset  string  `json:"actual_fee_asset"`
}

type positionCycleAgg struct {
	positionCycleID string
	symbol          string
	positionSide    string
	openTime        int64
	closeTime       int64
	openOrderCount  int
	closeOrderCount int
	openOrderIDs    []int64
	closeOrderIDs   []int64
	openQty         float64
	closeQty        float64
	openNotional    float64
	closeNotional   float64
	estimatedFee    float64
	actualFee       float64
	actualFeeAsset  string
	actualFeeMixed  bool
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
			items = append(items, allOrderView{
				OrderID:         item.GetOrderId(),
				Symbol:          item.GetSymbol(),
				Status:          item.GetStatus(),
				Side:            item.GetSide(),
				PositionSide:    item.GetPositionSide(),
				Type:            item.GetType(),
				OrigQty:         item.GetOrigQty(),
				ExecutedQty:     item.GetExecutedQty(),
				AvgPrice:        item.GetAvgPrice(),
				Price:           item.GetPrice(),
				StopPrice:       item.GetStopPrice(),
				ClientOrderID:   item.GetClientOrderId(),
				Time:            item.GetTime(),
				TimeText:        formatGatewayMillis(item.GetTime()),
				UpdateTime:      item.GetUpdateTime(),
				UpdateTimeText:  formatGatewayMillis(item.GetUpdateTime()),
				ReduceOnly:      item.GetReduceOnly(),
				ClosePosition:   item.GetClosePosition(),
				TimeInForce:     item.GetTimeInForce(),
				EstimatedFee:    item.GetEstimatedFee(),
				ActualFee:       item.GetActualFee(),
				ActualFeeAsset:  item.GetActualFeeAsset(),
				ActionType:      item.GetActionType(),
				PositionCycleID: item.GetPositionCycleId(),
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

		cycles := make(map[string]*positionCycleAgg)
		order := make([]string, 0)
		for _, item := range resp.GetItems() {
			cycleID := strings.TrimSpace(item.GetPositionCycleId())
			if cycleID == "" {
				continue
			}
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

		items := make([]positionCycleView, 0, len(order))
		for _, cycleID := range order {
			agg := cycles[cycleID]
			if agg == nil {
				continue
			}
			items = append(items, agg.view())
		}
		writeJSON(w, http.StatusOK, map[string]any{"items": items})
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

	switch actionType {
	case "OPEN_LONG", "OPEN_SHORT":
		a.openOrderCount++
		a.openOrderIDs = append(a.openOrderIDs, item.GetOrderId())
		a.openQty += qty
		a.openNotional += qty * avgPrice
		if a.openTime == 0 || eventTime < a.openTime {
			a.openTime = eventTime
		}
	case "CLOSE_LONG", "CLOSE_SHORT":
		a.closeOrderCount++
		a.closeOrderIDs = append(a.closeOrderIDs, item.GetOrderId())
		a.closeQty += qty
		a.closeNotional += qty * avgPrice
		if eventTime > a.closeTime {
			a.closeTime = eventTime
		}
	}
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
		PositionCycleID: a.positionCycleID,
		Symbol:          a.symbol,
		PositionSide:    a.positionSide,
		CycleStatus:     status,
		OpenTime:        a.openTime,
		OpenTimeText:    formatGatewayMillis(a.openTime),
		CloseTime:       a.closeTime,
		CloseTimeText:   formatGatewayMillis(a.closeTime),
		OrderCount:      a.openOrderCount + a.closeOrderCount,
		OpenOrderCount:  a.openOrderCount,
		CloseOrderCount: a.closeOrderCount,
		OpenOrderIDs:    a.openOrderIDs,
		CloseOrderIDs:   a.closeOrderIDs,
		OpenedQty:       formatFloatTrimmed(a.openQty, 8),
		ClosedQty:       formatFloatTrimmed(a.closeQty, 8),
		RemainingQty:    formatFloatTrimmed(remaining, 8),
		OpenAvgPrice:    weightedAverage(a.openNotional, a.openQty),
		CloseAvgPrice:   weightedAverage(a.closeNotional, a.closeQty),
		EstimatedFee:    formatFloatTrimmed(a.estimatedFee, 8),
		ActualFee:       formatFloatTrimmed(a.actualFee, 8),
		ActualFeeAsset:  actualFeeAsset,
	}
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
