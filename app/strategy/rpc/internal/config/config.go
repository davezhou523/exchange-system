package config

import "github.com/zeromicro/go-zero/zrpc"

type StrategyConfig struct {
	Name       string
	Symbol     string
	Enabled    bool
	Parameters map[string]float64
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

	Strategy struct {
		Symbol      string
		Name        string
		RuntimeMode string
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
