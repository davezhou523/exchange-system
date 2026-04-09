package kline

import "time"

const TimeframeLayout = "2006-01-02 15:04:05"

type Kline struct {
	Symbol    string `json:"symbol"`
	Timeframe string `json:"timeframe"`

	Open   float64 `json:"open"`
	High   float64 `json:"high"`
	Low    float64 `json:"low"`
	Close  float64 `json:"close"`
	Volume float64 `json:"volume"`

	StartTime int64 `json:"start_time"`
	EndTime   int64 `json:"end_time"`
}

func (k *Kline) NormalizeTimeframe() {
	if k == nil {
		return
	}
	if k.Timeframe != "" {
		return
	}
	if k.StartTime <= 0 {
		return
	}
	k.Timeframe = time.UnixMilli(k.StartTime).Format(TimeframeLayout)
}
