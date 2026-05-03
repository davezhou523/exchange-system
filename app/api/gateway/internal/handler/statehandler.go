package handler

import (
	"encoding/json"
	"fmt"
	"net/http"
	"strings"

	"exchange-system/app/api/gateway/internal/svc"
	"exchange-system/common/pb/strategy"
)

type strategyStatusSummaryView struct {
	Status    string   `json:"status,omitempty"`
	Regime    string   `json:"regime,omitempty"`
	Warmup    string   `json:"warmup,omitempty"`
	Target    string   `json:"target,omitempty"`
	Runtime   string   `json:"runtime,omitempty"`
	Allocator string   `json:"allocator,omitempty"`
	Blockers  []string `json:"blockers,omitempty"`
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
			summary.Regime = formatRegimeFusionSummary(fusion)
		}
		if warmup := router.GetWarmup(); warmup != nil {
			summary.Warmup = formatWarmupSummary(warmup)
			if strings.TrimSpace(warmup.GetStatus()) == "warmup_incomplete" {
				summary.Blockers = append(summary.Blockers, warmup.GetIncompleteReasons()...)
			}
		}
		summary.Target = fmt.Sprintf(
			"target enabled=%t template=%s bucket=%s reason=%s",
			router.GetEnabled(),
			strings.TrimSpace(router.GetTemplate()),
			strings.TrimSpace(router.GetRouteBucket()),
			strings.TrimSpace(router.GetTargetReason()),
		)
		summary.Runtime = fmt.Sprintf(
			"runtime enabled=%t template=%s action=%s gate=%s",
			router.GetRuntimeEnabled(),
			strings.TrimSpace(router.GetRuntimeTemplate()),
			strings.TrimSpace(router.GetApplyAction()),
			strings.TrimSpace(router.GetApplyGateReason()),
		)
		if gate := strings.TrimSpace(router.GetApplyGateReason()); gate != "" {
			summary.Blockers = append(summary.Blockers, gate)
		}
	}
	if allocator := resp.GetAllocator(); allocator != nil {
		summary.Allocator = fmt.Sprintf(
			"budget=%.4f bucket=%.4f score=%.4f source=%s paused=%t",
			allocator.GetPositionBudget(),
			allocator.GetBucketBudget(),
			allocator.GetScore(),
			strings.TrimSpace(allocator.GetScoreSource()),
			allocator.GetTradingPaused(),
		)
		if allocator.GetTradingPaused() {
			pauseReason := strings.TrimSpace(allocator.GetPauseReason())
			if pauseReason == "" {
				pauseReason = "trading_paused"
			}
			summary.Blockers = append(summary.Blockers, pauseReason)
		}
	}
	if summary.Status == "" && summary.Regime == "" && summary.Warmup == "" && summary.Target == "" && summary.Runtime == "" && summary.Allocator == "" && len(summary.Blockers) == 0 {
		return nil
	}
	return summary
}

// formatRegimeFusionSummary 生成前端可直接展示的多周期融合摘要，避免页面再自行拼接 1H/15M 信息。
func formatRegimeFusionSummary(fusion *strategy.RegimeFusionStatus) string {
	if fusion == nil {
		return ""
	}
	parts := make([]string, 0, 3)
	if fusedState := strings.TrimSpace(fusion.GetFusedState()); fusedState != "" {
		parts = append(parts, fmt.Sprintf("fused=%s score=%.2f", fusedState, fusion.GetFusedScore()))
	}
	if h1 := fusion.GetH1(); h1 != nil {
		parts = append(parts, fmt.Sprintf("1h=%s/%s", strings.TrimSpace(h1.GetState()), strings.TrimSpace(h1.GetRouteReason())))
	}
	if m15 := fusion.GetM15(); m15 != nil {
		parts = append(parts, fmt.Sprintf("15m=%s/%s", strings.TrimSpace(m15.GetState()), strings.TrimSpace(m15.GetRouteReason())))
	}
	if reason := strings.TrimSpace(fusion.GetFusedReason()); reason != "" {
		parts = append(parts, "reason="+reason)
	}
	return strings.Join(parts, " ")
}

// formatWarmupSummary 生成当前实例多周期 warmup 摘要，便于快速判断冷启动恢复是否完整。
func formatWarmupSummary(warmup *strategy.StrategyWarmupStatus) string {
	if warmup == nil {
		return ""
	}
	summary := fmt.Sprintf(
		"%s source=%s 4h=%d 1h=%d 15m=%d 1m=%d",
		strings.TrimSpace(warmup.GetStatus()),
		strings.TrimSpace(warmup.GetSource()),
		warmup.GetHistoryLen_4H(),
		warmup.GetHistoryLen_1H(),
		warmup.GetHistoryLen_15M(),
		warmup.GetHistoryLen_1M(),
	)
	if reasons := warmup.GetIncompleteReasons(); len(reasons) > 0 {
		summary += " reasons=" + strings.Join(reasons, ",")
	}
	return summary
}
