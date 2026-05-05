package harvestpath

import (
	"os"
	"path/filepath"
	"testing"
)

func TestLSTMPredictorRegimeThreshold(t *testing.T) {
	tempDir := t.TempDir()
	metadataPath := filepath.Join(tempDir, "ethusdt_harvest_path_lstm.json")
	payload := `{
  "regime_aware_threshold_strategy": {
    "default_threshold": 0.61,
    "regimes": {
      "LOW": {
        "recommended_threshold": 0.72,
        "use_regime_threshold": true
      },
      "MID": {
        "recommended_threshold": 0.60,
        "use_regime_threshold": true
      },
      "HIGH": {
        "recommended_threshold": 0.48,
        "use_regime_threshold": false
      }
    }
  }
}`
	if err := os.WriteFile(metadataPath, []byte(payload), 0o644); err != nil {
		t.Fatalf("WriteFile() error = %v", err)
	}

	predictor := NewLSTMPredictor(LSTMPredictorConfig{
		Enabled:      true,
		ArtifactsDir: tempDir,
	})

	threshold, matched := predictor.RegimeThreshold("ETHUSDT", "LOW", 0.80)
	if !matched || threshold != 0.72 {
		t.Fatalf("RegimeThreshold(LOW) = (%v, %v), want (0.72, true)", threshold, matched)
	}

	threshold, matched = predictor.RegimeThreshold("ETHUSDT", "HIGH", 0.80)
	if matched || threshold != 0.61 {
		t.Fatalf("RegimeThreshold(HIGH) = (%v, %v), want (0.61, false)", threshold, matched)
	}

	threshold, matched = predictor.RegimeThreshold("ETHUSDT", "UNKNOWN", 0.80)
	if matched || threshold != 0.61 {
		t.Fatalf("RegimeThreshold(UNKNOWN) = (%v, %v), want (0.61, false)", threshold, matched)
	}
}
