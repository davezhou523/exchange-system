package handler

import (
	"encoding/json"
	"net/http"
	"strings"

	"exchange-system/app/api/gateway/internal/svc"
	"exchange-system/common/pb/strategy"
)

type strategyStatusSummaryView struct {
	Status          string                              `json:"status,omitempty"`
	Regime          *strategyStatusSummaryRegimeView    `json:"regime,omitempty"`
	Warmup          *strategyStatusSummaryWarmupView    `json:"warmup,omitempty"`
	Target          *strategyStatusSummaryTargetView    `json:"target,omitempty"`
	Runtime         *strategyStatusSummaryRuntimeView   `json:"runtime,omitempty"`
	LatestSecondBar *strategyStatusSummarySecondBarView `json:"latest_second_bar,omitempty"`
	Allocator       *strategyStatusSummaryAllocatorView `json:"allocator,omitempty"`
	Blockers        []string                            `json:"blockers,omitempty"`
}

type strategyStatusSummaryTargetView struct {
	Enabled      bool   `json:"enabled"`
	Template     string `json:"template,omitempty"`
	Bucket       string `json:"bucket,omitempty"`
	Reason       string `json:"reason,omitempty"`
	ReasonDesc   string `json:"reason_desc,omitempty"`
	BaseTemplate string `json:"base_template,omitempty"`
}

type strategyStatusSummaryRuntimeView struct {
	Enabled         bool   `json:"enabled"`
	Template        string `json:"template,omitempty"`
	Action          string `json:"action,omitempty"`
	ActionDesc      string `json:"action_desc,omitempty"`
	Gate            string `json:"gate,omitempty"`
	GateDesc        string `json:"gate_desc,omitempty"`
	HasStrategy     bool   `json:"has_strategy"`
	HasOpenPosition bool   `json:"has_open_position"`
}

type strategyStatusSummaryAllocatorView struct {
	Template        string  `json:"template,omitempty"`
	Bucket          string  `json:"bucket,omitempty"`
	RouteReason     string  `json:"route_reason,omitempty"`
	RouteReasonDesc string  `json:"route_reason_desc,omitempty"`
	Budget          float64 `json:"budget"`
	BucketBudget    float64 `json:"bucket_budget"`
	Score           float64 `json:"score"`
	Source          string  `json:"source,omitempty"`
	Paused          bool    `json:"paused"`
	PauseReason     string  `json:"pause_reason,omitempty"`
	PauseReasonDesc string  `json:"pause_reason_desc,omitempty"`
}

type strategyStatusSummaryWarmupView struct {
	Status            string   `json:"status,omitempty"`
	Source            string   `json:"source,omitempty"`
	HistoryLen4H      int32    `json:"history_len_4h"`
	HistoryLen1H      int32    `json:"history_len_1h"`
	HistoryLen15M     int32    `json:"history_len_15m"`
	HistoryLen1M      int32    `json:"history_len_1m"`
	IncompleteReasons []string `json:"incomplete_reasons,omitempty"`
}

type strategyStatusSummaryRegimeView struct {
	FusedState      string                                `json:"fused_state,omitempty"`
	FusedReason     string                                `json:"fused_reason,omitempty"`
	FusedReasonDesc string                                `json:"fused_reason_desc,omitempty"`
	FusedScore      float64                               `json:"fused_score"`
	H1              *strategyStatusSummaryRegimeFrameView `json:"h1,omitempty"`
	M15             *strategyStatusSummaryRegimeFrameView `json:"m15,omitempty"`
}

type strategyStatusSummaryRegimeFrameView struct {
	Interval        string  `json:"interval,omitempty"`
	State           string  `json:"state,omitempty"`
	Reason          string  `json:"reason,omitempty"`
	ReasonDesc      string  `json:"reason_desc,omitempty"`
	RouteReason     string  `json:"route_reason,omitempty"`
	RouteReasonDesc string  `json:"route_reason_desc,omitempty"`
	Confidence      float64 `json:"confidence"`
	LastUpdate      int64   `json:"last_update"`
	Healthy         bool    `json:"healthy"`
	Fresh           bool    `json:"fresh"`
}

type strategyStatusSummarySecondBarView struct {
	Source      string  `json:"source,omitempty"`
	Synthetic   bool    `json:"synthetic"`
	IsFinal     bool    `json:"is_final"`
	OpenTimeMs  int64   `json:"open_time_ms"`
	CloseTimeMs int64   `json:"close_time_ms"`
	Open        float64 `json:"open"`
	High        float64 `json:"high"`
	Low         float64 `json:"low"`
	Close       float64 `json:"close"`
	Volume      float64 `json:"volume"`
}

type startStrategyReq struct {
	Symbol     string             `json:"symbol"`
	Name       string             `json:"name"`
	Enabled    bool               `json:"enabled"`
	Parameters map[string]float64 `json:"parameters"`
}

type stopStrategyReq struct {
	StrategyId string `json:"strategy_id"`
}

func StartStrategyHandler(serviceContext *svc.ServiceContext) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req startStrategyReq
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			writeError(w, http.StatusBadRequest, "invalid json")
			return
		}
		req.Symbol = strings.TrimSpace(strings.ToUpper(req.Symbol))
		req.Name = strings.TrimSpace(req.Name)
		resp, err := serviceContext.Strategy.StartStrategy(r.Context(), &strategy.StrategyConfig{
			Symbol:     req.Symbol,
			Name:       req.Name,
			Enabled:    req.Enabled,
			Parameters: req.Parameters,
		})
		if err != nil {
			writeError(w, http.StatusBadGateway, err.Error())
			return
		}
		writeJSON(w, http.StatusOK, resp)
	}
}

func StopStrategyHandler(serviceContext *svc.ServiceContext) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req stopStrategyReq
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			writeError(w, http.StatusBadRequest, "invalid json")
			return
		}
		req.StrategyId = strings.TrimSpace(req.StrategyId)
		if req.StrategyId == "" {
			writeError(w, http.StatusBadRequest, "strategy_id is required")
			return
		}
		resp, err := serviceContext.Strategy.StopStrategy(r.Context(), &strategy.StrategyRequest{StrategyId: req.StrategyId})
		if err != nil {
			writeError(w, http.StatusBadGateway, err.Error())
			return
		}
		writeJSON(w, http.StatusOK, resp)
	}
}

func StrategyStatusHandler(serviceContext *svc.ServiceContext) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		strategyId := strings.TrimSpace(r.URL.Query().Get("strategy_id"))
		if strategyId == "" {
			writeError(w, http.StatusBadRequest, "strategy_id is required")
			return
		}
		resp, err := serviceContext.Strategy.GetStrategyStatus(r.Context(), &strategy.StrategyRequest{StrategyId: strategyId})
		if err != nil {
			writeError(w, http.StatusBadGateway, err.Error())
			return
		}
		writeJSON(w, http.StatusOK, buildStrategyStatusResponse(resp))
	}
}

// buildStrategyStatusResponse 在保留原始 proto 字段的基础上补充前端友好的 summary 结构。
func buildStrategyStatusResponse(resp *strategy.StrategyStatus) map[string]interface{} {
	if resp == nil {
		return map[string]interface{}{}
	}
	raw := make(map[string]interface{})
	if payload, err := json.Marshal(resp); err == nil {
		_ = json.Unmarshal(payload, &raw)
	}
	if len(raw) == 0 {
		raw["strategy_id"] = resp.GetStrategyId()
		raw["status"] = resp.GetStatus()
		raw["message"] = resp.GetMessage()
		raw["last_update"] = resp.GetLastUpdate()
	}
	if summary := buildStrategyStatusSummary(resp); summary != nil {
		raw["summary"] = summary
	}
	return raw
}

// buildStrategyStatusSummary 生成适合前端直接展示的状态摘要。
func buildStrategyStatusSummary(resp *strategy.StrategyStatus) *strategyStatusSummaryView {
	if resp == nil {
		return nil
	}
	summary := &strategyStatusSummaryView{
		Status: strings.TrimSpace(resp.GetStatus()),
	}
	if router := resp.GetRouter(); router != nil {
		if fusion := router.GetRegimeFusion(); fusion != nil {
			summary.Regime = buildStrategyStatusSummaryRegime(fusion)
		}
		if warmup := router.GetWarmup(); warmup != nil {
			summary.Warmup = buildStrategyStatusSummaryWarmup(warmup)
			if strings.TrimSpace(warmup.GetStatus()) == "warmup_incomplete" {
				summary.Blockers = append(summary.Blockers, warmup.GetIncompleteReasons()...)
			}
		}
		summary.Target = buildStrategyStatusSummaryTarget(router)
		summary.Runtime = buildStrategyStatusSummaryRuntime(router)
		summary.LatestSecondBar = buildStrategyStatusSummarySecondBar(router)
		if gate := strings.TrimSpace(router.GetApplyGateReason()); gate != "" {
			summary.Blockers = append(summary.Blockers, gate)
		}
	}
	if allocator := resp.GetAllocator(); allocator != nil {
		summary.Allocator = buildStrategyStatusSummaryAllocator(allocator)
		if allocator.GetTradingPaused() {
			pauseReason := strings.TrimSpace(allocator.GetPauseReason())
			if pauseReason == "" {
				pauseReason = "trading_paused"
			}
			summary.Blockers = append(summary.Blockers, pauseReason)
		}
	}
	if summary.Status == "" &&
		summary.Regime == nil &&
		summary.Warmup == nil &&
		summary.Target == nil &&
		summary.Runtime == nil &&
		summary.LatestSecondBar == nil &&
		summary.Allocator == nil &&
		len(summary.Blockers) == 0 {
		return nil
	}
	return summary
}

// buildStrategyStatusSummaryTarget 生成目标路由摘要对象，避免前端再解析 target 拼接字符串。
func buildStrategyStatusSummaryTarget(router *strategy.StrategyRouteRuntimeStatus) *strategyStatusSummaryTargetView {
	if router == nil {
		return nil
	}
	return &strategyStatusSummaryTargetView{
		Enabled:      router.GetEnabled(),
		Template:     strings.TrimSpace(router.GetTemplate()),
		Bucket:       strings.TrimSpace(router.GetRouteBucket()),
		Reason:       strings.TrimSpace(router.GetTargetReason()),
		ReasonDesc:   strings.TrimSpace(router.GetTargetReasonDesc()),
		BaseTemplate: strings.TrimSpace(router.GetBaseTemplate()),
	}
}

// buildStrategyStatusSummaryWarmup 生成 warmup 结构化对象，避免前端继续解析拼接字符串。
func buildStrategyStatusSummaryWarmup(warmup *strategy.StrategyWarmupStatus) *strategyStatusSummaryWarmupView {
	if warmup == nil {
		return nil
	}
	return &strategyStatusSummaryWarmupView{
		Status:            strings.TrimSpace(warmup.GetStatus()),
		Source:            strings.TrimSpace(warmup.GetSource()),
		HistoryLen4H:      warmup.GetHistoryLen_4H(),
		HistoryLen1H:      warmup.GetHistoryLen_1H(),
		HistoryLen15M:     warmup.GetHistoryLen_15M(),
		HistoryLen1M:      warmup.GetHistoryLen_1M(),
		IncompleteReasons: append([]string(nil), warmup.GetIncompleteReasons()...),
	}
}

// buildStrategyStatusSummaryRegime 生成多周期融合判态对象，便于前端直接消费 fused/h1/m15 结构。
func buildStrategyStatusSummaryRegime(fusion *strategy.RegimeFusionStatus) *strategyStatusSummaryRegimeView {
	if fusion == nil {
		return nil
	}
	return &strategyStatusSummaryRegimeView{
		FusedState:      strings.TrimSpace(fusion.GetFusedState()),
		FusedReason:     strings.TrimSpace(fusion.GetFusedReason()),
		FusedReasonDesc: strings.TrimSpace(fusion.GetFusedReasonDesc()),
		FusedScore:      fusion.GetFusedScore(),
		H1:              buildStrategyStatusSummaryRegimeFrame(fusion.GetH1()),
		M15:             buildStrategyStatusSummaryRegimeFrame(fusion.GetM15()),
	}
}

// buildStrategyStatusSummaryRegimeFrame 生成单周期判态对象，保留 reason/route/confidence 等字段供前端直接展示。
func buildStrategyStatusSummaryRegimeFrame(frame *strategy.RegimeFrameStatus) *strategyStatusSummaryRegimeFrameView {
	if frame == nil {
		return nil
	}
	return &strategyStatusSummaryRegimeFrameView{
		Interval:        strings.TrimSpace(frame.GetInterval()),
		State:           strings.TrimSpace(frame.GetState()),
		Reason:          strings.TrimSpace(frame.GetReason()),
		ReasonDesc:      strings.TrimSpace(frame.GetReasonDesc()),
		RouteReason:     strings.TrimSpace(frame.GetRouteReason()),
		RouteReasonDesc: strings.TrimSpace(frame.GetRouteReasonDesc()),
		Confidence:      frame.GetConfidence(),
		LastUpdate:      frame.GetLastUpdate(),
		Healthy:         frame.GetHealthy(),
		Fresh:           frame.GetFresh(),
	}
}

// buildStrategyStatusSummaryRuntime 生成运行态摘要对象，直接暴露动作和门禁字段，减少前端拼接成本。
func buildStrategyStatusSummaryRuntime(router *strategy.StrategyRouteRuntimeStatus) *strategyStatusSummaryRuntimeView {
	if router == nil {
		return nil
	}
	return &strategyStatusSummaryRuntimeView{
		Enabled:         router.GetRuntimeEnabled(),
		Template:        strings.TrimSpace(router.GetRuntimeTemplate()),
		Action:          strings.TrimSpace(router.GetApplyAction()),
		ActionDesc:      strings.TrimSpace(router.GetApplyActionDesc()),
		Gate:            strings.TrimSpace(router.GetApplyGateReason()),
		GateDesc:        strings.TrimSpace(router.GetApplyGateReasonDesc()),
		HasStrategy:     router.GetHasStrategy(),
		HasOpenPosition: router.GetHasOpenPosition(),
	}
}

// buildStrategyStatusSummarySecondBar 生成最近一条秒级输入行情摘要，便于前端直接识别是否命中 depth fallback。
func buildStrategyStatusSummarySecondBar(router *strategy.StrategyRouteRuntimeStatus) *strategyStatusSummarySecondBarView {
	if router == nil || router.GetLatestSecondBar() == nil {
		return nil
	}
	bar := router.GetLatestSecondBar()
	return &strategyStatusSummarySecondBarView{
		Source:      strings.TrimSpace(bar.GetSource()),
		Synthetic:   bar.GetSynthetic(),
		IsFinal:     bar.GetIsFinal(),
		OpenTimeMs:  bar.GetOpenTimeMs(),
		CloseTimeMs: bar.GetCloseTimeMs(),
		Open:        bar.GetOpen(),
		High:        bar.GetHigh(),
		Low:         bar.GetLow(),
		Close:       bar.GetClose(),
		Volume:      bar.GetVolume(),
	}
}

// buildStrategyStatusSummaryAllocator 生成 allocator 摘要对象，避免继续把预算与暂停状态压成单个字符串。
func buildStrategyStatusSummaryAllocator(allocator *strategy.PositionAllocatorStatus) *strategyStatusSummaryAllocatorView {
	if allocator == nil {
		return nil
	}
	return &strategyStatusSummaryAllocatorView{
		Template:        strings.TrimSpace(allocator.GetTemplate()),
		Bucket:          strings.TrimSpace(allocator.GetRouteBucket()),
		RouteReason:     strings.TrimSpace(allocator.GetRouteReason()),
		RouteReasonDesc: strings.TrimSpace(allocator.GetRouteReasonDesc()),
		Budget:          allocator.GetPositionBudget(),
		BucketBudget:    allocator.GetBucketBudget(),
		Score:           allocator.GetScore(),
		Source:          strings.TrimSpace(allocator.GetScoreSource()),
		Paused:          allocator.GetTradingPaused(),
		PauseReason:     strings.TrimSpace(allocator.GetPauseReason()),
		PauseReasonDesc: strings.TrimSpace(allocator.GetPauseReasonDesc()),
	}
}
