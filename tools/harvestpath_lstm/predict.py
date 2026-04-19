from __future__ import annotations

import argparse
import json
from pathlib import Path

import numpy as np
import torch

from data import DatasetConfig, latest_window
from model import HarvestPathLSTM, LSTMConfig


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Recognize liquidity sweep risk with LSTM")
    parser.add_argument("--data-dir", default="app/strategy/rpc/data/kline", help="1m Kline root directory")
    parser.add_argument("--symbol", default="ETHUSDT")
    parser.add_argument("--artifacts-dir", default="tools/harvestpath_lstm/artifacts")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    artifacts_dir = Path(args.artifacts_dir)
    metadata_path = artifacts_dir / f"{args.symbol.lower()}_harvest_path_lstm.json"
    checkpoint_path = artifacts_dir / f"{args.symbol.lower()}_harvest_path_lstm.pt"
    metadata = json.loads(metadata_path.read_text(encoding="utf-8"))

    config = DatasetConfig(
        data_dir=Path(args.data_dir),
        symbol=args.symbol,
        window_size=int(metadata["window_size"]),
        lookahead=int(metadata["lookahead"]),
        intervals=tuple(metadata.get("intervals", ["1m", "5m", "15m"])),
    )
    mean = np.asarray(metadata["mean"], dtype=np.float32)
    std = np.asarray(metadata["std"], dtype=np.float32)
    window = latest_window(config, mean, std)

    model = HarvestPathLSTM(
        LSTMConfig(
            input_size=window.shape[-1],
            hidden_size=int(metadata["hidden_size"]),
            num_layers=int(metadata["num_layers"]),
            dropout=float(metadata["dropout"]),
        )
    )
    state_dict = torch.load(checkpoint_path, map_location="cpu")
    model.load_state_dict(state_dict)
    model.eval()

    with torch.no_grad():
        tensor = torch.from_numpy(window).unsqueeze(0)
        logits = model(tensor)
        probability = torch.sigmoid(logits).item()

    print(
        json.dumps(
            {
                "symbol": args.symbol,
                "model": "harvest_path_liquidity_sweep_risk_v1",
                "harvest_path_probability": round(probability, 6),
                "window_size": config.window_size,
                "lookahead": config.lookahead,
            },
            ensure_ascii=False,
            indent=2,
        )
    )


if __name__ == "__main__":
    main()