package harvestpath

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"time"
)

type LSTMPredictorConfig struct {
	Enabled      bool
	PythonBin    string
	ScriptPath   string
	DataDir      string
	ArtifactsDir string
	Timeout      time.Duration
}

type LSTMPredictor struct {
	config        LSTMPredictorConfig
	metadataMu    sync.RWMutex
	metadataCache map[string]*lstmArtifactMetadata
}

type LSTMPrediction struct {
	Symbol                 string  `json:"symbol"`
	Model                  string  `json:"model"`
	HarvestPathProbability float64 `json:"harvest_path_probability"`
	WindowSize             int     `json:"window_size"`
	Lookahead              int     `json:"lookahead"`
}

type lstmArtifactMetadata struct {
	RegimeAwareThresholdStrategy *regimeAwareThresholdStrategy `json:"regime_aware_threshold_strategy"`
}

type regimeAwareThresholdStrategy struct {
	DefaultThreshold float64                             `json:"default_threshold"`
	Regimes          map[string]regimeAwareThresholdRule `json:"regimes"`
}

type regimeAwareThresholdRule struct {
	RecommendedThreshold float64 `json:"recommended_threshold"`
	UseRegimeThreshold   bool    `json:"use_regime_threshold"`
}

func NewLSTMPredictor(cfg LSTMPredictorConfig) *LSTMPredictor {
	if !cfg.Enabled {
		return nil
	}
	if strings.TrimSpace(cfg.PythonBin) == "" {
		cfg.PythonBin = "python3"
	}
	if cfg.Timeout <= 0 {
		cfg.Timeout = 2500 * time.Millisecond
	}
	return &LSTMPredictor{
		config:        cfg,
		metadataCache: make(map[string]*lstmArtifactMetadata),
	}
}

func (p *LSTMPredictor) Predict(ctx context.Context, symbol string) (*LSTMPrediction, error) {
	if p == nil || !p.config.Enabled {
		return nil, nil
	}
	scriptPath := strings.TrimSpace(p.config.ScriptPath)
	dataDir := strings.TrimSpace(p.config.DataDir)
	artifactsDir := strings.TrimSpace(p.config.ArtifactsDir)
	if scriptPath == "" || dataDir == "" || artifactsDir == "" {
		return nil, fmt.Errorf("lstm liquidity sweep risk recognizer is not fully configured")
	}

	runCtx := ctx
	var cancel context.CancelFunc
	if _, ok := ctx.Deadline(); !ok && p.config.Timeout > 0 {
		runCtx, cancel = context.WithTimeout(ctx, p.config.Timeout)
		defer cancel()
	}

	cmd := exec.CommandContext(
		runCtx,
		p.config.PythonBin,
		filepath.Clean(scriptPath),
		"--data-dir",
		filepath.Clean(dataDir),
		"--symbol",
		strings.TrimSpace(symbol),
		"--artifacts-dir",
		filepath.Clean(artifactsDir),
	)
	output, err := cmd.Output()
	if err != nil {
		return nil, err
	}

	var prediction LSTMPrediction
	if err := json.Unmarshal(output, &prediction); err != nil {
		return nil, fmt.Errorf("decode lstm liquidity sweep risk output: %w", err)
	}
	if prediction.HarvestPathProbability < 0 || prediction.HarvestPathProbability > 1 {
		return nil, fmt.Errorf("invalid lstm probability: %f", prediction.HarvestPathProbability)
	}
	return &prediction, nil
}

func (p *LSTMPredictor) RegimeThreshold(symbol, regime string, fallback float64) (float64, bool) {
	if p == nil || !p.config.Enabled {
		return fallback, false
	}
	metadata, err := p.metadata(symbol)
	if err != nil || metadata == nil || metadata.RegimeAwareThresholdStrategy == nil {
		return fallback, false
	}
	strategy := metadata.RegimeAwareThresholdStrategy
	regime = strings.ToUpper(strings.TrimSpace(regime))
	if rule, ok := strategy.Regimes[regime]; ok && rule.UseRegimeThreshold && rule.RecommendedThreshold > 0 {
		return rule.RecommendedThreshold, true
	}
	if strategy.DefaultThreshold > 0 {
		return strategy.DefaultThreshold, false
	}
	return fallback, false
}

func (p *LSTMPredictor) metadata(symbol string) (*lstmArtifactMetadata, error) {
	if p == nil {
		return nil, nil
	}
	key := strings.ToUpper(strings.TrimSpace(symbol))
	p.metadataMu.RLock()
	if cached, ok := p.metadataCache[key]; ok {
		p.metadataMu.RUnlock()
		return cached, nil
	}
	p.metadataMu.RUnlock()

	path := p.metadataPath(key)
	payload, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	var metadata lstmArtifactMetadata
	if err := json.Unmarshal(payload, &metadata); err != nil {
		return nil, fmt.Errorf("decode lstm metadata: %w", err)
	}

	p.metadataMu.Lock()
	p.metadataCache[key] = &metadata
	p.metadataMu.Unlock()
	return &metadata, nil
}

func (p *LSTMPredictor) metadataPath(symbol string) string {
	name := strings.ToLower(strings.TrimSpace(symbol))
	return filepath.Join(filepath.Clean(strings.TrimSpace(p.config.ArtifactsDir)), name+"_harvest_path_lstm.json")
}
