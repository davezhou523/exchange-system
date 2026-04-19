package harvestpath

import (
	"fmt"
	"strings"
)

type BookEncoder struct {
	TopN         int
	SpreadBpsCap float64
}

func NewBookEncoder(topN int, spreadBpsCap float64) *BookEncoder {
	if topN <= 0 {
		topN = 10
	}
	if spreadBpsCap <= 0 {
		spreadBpsCap = 12
	}
	return &BookEncoder{
		TopN:         topN,
		SpreadBpsCap: spreadBpsCap,
	}
}

func (e *BookEncoder) Encode(snapshot *OrderBookSnapshot, lastPrice float64, entrySide EntrySide) *BookFeatures {
	if e == nil || snapshot == nil || len(snapshot.Bids) == 0 || len(snapshot.Asks) == 0 || lastPrice <= 0 {
		return nil
	}
	topN := e.TopN
	if topN > len(snapshot.Bids) {
		topN = len(snapshot.Bids)
	}
	if topN > len(snapshot.Asks) {
		topN = len(snapshot.Asks)
	}
	if topN <= 0 {
		return nil
	}

	bestBid := snapshot.Bids[0].Price
	bestAsk := snapshot.Asks[0].Price
	mid := (bestBid + bestAsk) / 2
	if bestBid <= 0 || bestAsk <= 0 || mid <= 0 {
		mid = lastPrice
	}
	spreadBps := 0.0
	if mid > 0 && bestAsk >= bestBid {
		spreadBps = (bestAsk - bestBid) / mid * 10000
	}

	bidTopQty := max(snapshot.Bids[0].Quantity, 0)
	askTopQty := max(snapshot.Asks[0].Quantity, 0)
	bidTopNotional := bidTopQty * max(snapshot.Bids[0].Price, 0)
	askTopNotional := askTopQty * max(snapshot.Asks[0].Price, 0)
	bidDepthNotional := 0.0
	askDepthNotional := 0.0
	for i := 0; i < topN; i++ {
		bidDepthNotional += max(snapshot.Bids[i].Price, 0) * max(snapshot.Bids[i].Quantity, 0)
		askDepthNotional += max(snapshot.Asks[i].Price, 0) * max(snapshot.Asks[i].Quantity, 0)
	}

	topImbalance := signedImbalance(bidTopNotional, askTopNotional)
	depthImbalance := signedImbalance(bidDepthNotional, askDepthNotional)
	adversePressure := 0.0
	if entrySide == EntrySideLong {
		adversePressure = clamp((-topImbalance)*0.4+(-depthImbalance)*0.6, 0, 1)
	} else {
		adversePressure = clamp(topImbalance*0.4+depthImbalance*0.6, 0, 1)
	}
	spreadScore := clamp(spreadBps/max(e.SpreadBpsCap, 1e-6), 0, 1)
	probability := clamp(adversePressure*0.65+spreadScore*0.35, 0, 1)

	return &BookFeatures{
		Probability:         probability,
		SpreadBps:           spreadBps,
		TopLevelImbalance:   topImbalance,
		DepthImbalance:      depthImbalance,
		DirectionalPressure: adversePressure,
		Summary:             buildBookSummary(entrySide, spreadBps, adversePressure, topImbalance, depthImbalance),
	}
}

func signedImbalance(bid, ask float64) float64 {
	total := bid + ask
	if total <= 0 {
		return 0
	}
	return clamp((bid-ask)/total, -1, 1)
}

func buildBookSummary(entrySide EntrySide, spreadBps, adversePressure, topImbalance, depthImbalance float64) string {
	parts := make([]string, 0, 4)
	if spreadBps > 0 {
		parts = append(parts, fmt.Sprintf("spread=%.2fbps", spreadBps))
	}
	if adversePressure >= 0.65 {
		parts = append(parts, "adverse_pressure=high")
	} else if adversePressure >= 0.40 {
		parts = append(parts, "adverse_pressure=mid")
	}
	switch entrySide {
	case EntrySideLong:
		if topImbalance < -0.10 {
			parts = append(parts, "ask_top_heavy")
		}
		if depthImbalance < -0.10 {
			parts = append(parts, "ask_depth_heavy")
		}
	case EntrySideShort:
		if topImbalance > 0.10 {
			parts = append(parts, "bid_top_heavy")
		}
		if depthImbalance > 0.10 {
			parts = append(parts, "bid_depth_heavy")
		}
	}
	return strings.Join(parts, " | ")
}

func max(a, b float64) float64 {
	if a > b {
		return a
	}
	return b
}
