package handler

import (
	"encoding/json"
	"net/http"
	"strings"

	"exchange-system/app/api/gateway/internal/svc"
	"exchange-system/common/pb/strategy"
)

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
		writeJSON(w, http.StatusOK, resp)
	}
}
