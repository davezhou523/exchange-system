package config

import (
	"time"

	"github.com/zeromicro/go-zero/zrpc"
)

type StrategyConfig struct {
	Name       string
	Symbol     string
	Enabled    bool
	Template   string
	Parameters map[string]float64
	Overrides  map[string]float64
}

type Config struct {
	zrpc.RpcServerConf
	Kafka struct {
		Addrs         []string
		Group         string
		InitialOffset string
		Topics        struct {
			Kline             string
			Depth             string
			Signal            string
			HarvestPathSignal string
		}
	}

	Execution zrpc.RpcClientConf

	KlineLogDir  string
	SignalLogDir string

	Strategies []StrategyConfig
	Templates  map[string]map[string]float64
	Universe   struct {
		Enabled            bool
		BootstrapDuration  time.Duration
		EvaluateInterval   time.Duration
		FreshnessWindow    time.Duration
		MinEnabledDuration time.Duration
		CooldownDuration   time.Duration
		RequireFinal       bool
		RequireTradable    bool
		RequireClean       bool
		CandidateSymbols   []string
		RouterConfig       struct {
			StaticTemplateMap     map[string]string `json:",optional"`
			RangeTemplate         string            `json:",optional"`
			BreakoutTemplate      string            `json:",optional"`
			BTCTrendTemplate      string            `json:",optional"`
			BTCTrendAtrPctMax     float64           `json:",optional"`
			HighBetaSafeTemplate  string            `json:",optional"`
			HighBetaSafeSymbols   []string          `json:",optional"`
			HighBetaSafeAtrPct    float64           `json:",optional"`
			HighBetaDisableAtrPct float64           `json:",optional"`
		} `json:",optional"`
	}

	Strategy struct {
		Symbol      string
		Name        string
		RuntimeMode string
	}

	MarketState struct {
		FreshnessWindow   time.Duration
		RangeAtrPctMax    float64
		BreakoutAtrPctMin float64
	}

	Weights struct {
		DefaultTrendWeight    float64
		DefaultRangeWeight    float64
		DefaultBreakoutWeight float64
		DefaultRiskScale      float64
		LossStreakThreshold   int
		DailyLossSoftLimit    float64
		DrawdownSoftLimit     float64
		CoolingPauseDuration  time.Duration
		AtrSpikeRatioMin      float64
		VolumeSpikeRatioMin   float64
		CoolingMinSamples     int
		TrendStrategyMix      map[string]float64
		BreakoutStrategyMix   map[string]float64
		RangeStrategyMix      map[string]float64
		TrendSymbolWeights    map[string]float64
		BreakoutSymbolWeights map[string]float64
		RangeSymbolWeights    map[string]float64
	}

	HarvestPathLSTM struct {
		Enabled      bool
		PythonBin    string
		ScriptPath   string
		DataDir      string
		ArtifactsDir string
		TimeoutMs    int64
	}
}
