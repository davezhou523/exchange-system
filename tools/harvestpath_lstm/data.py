from __future__ import annotations

import json
import math
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path

import numpy as np


@dataclass
class DatasetConfig:
    data_dir: Path
    symbol: str = "ETHUSDT"
    window_size: int = 64
    lookahead: int = 8
    train_ratio: float = 0.8
    spike_atr_multiple: float = 1.8
    reversal_atr_multiple: float = 0.6
    intervals: tuple[str, ...] = ("1m", "5m", "15m")


@dataclass
class DatasetBundle:
    train_x: np.ndarray
    train_y: np.ndarray
    val_x: np.ndarray
    val_y: np.ndarray
    mean: np.ndarray
    std: np.ndarray
    feature_names: list[str]
    train_timestamps: np.ndarray
    val_timestamps: np.ndarray


@dataclass
class SampleBundle:
    x: np.ndarray
    y: np.ndarray
    timestamps: np.ndarray
    feature_names: list[str]


def load_dataset(config: DatasetConfig) -> DatasetBundle:
    samples = build_samples(config)
    x = samples.x
    y = samples.y
    timestamps = samples.timestamps
    split = max(1, int(len(x) * config.train_ratio))
    split = min(split, len(x) - 1)
    train_x = x[:split]
    val_x = x[split:]
    train_y = y[:split]
    val_y = y[split:]
    train_timestamps = timestamps[:split]
    val_timestamps = timestamps[split:]

    mean = train_x.mean(axis=(0, 1), keepdims=True)
    std = train_x.std(axis=(0, 1), keepdims=True)
    std = np.where(std < 1e-6, 1.0, std)

    train_x = ((train_x - mean) / std).astype(np.float32)
    val_x = ((val_x - mean) / std).astype(np.float32)

    return DatasetBundle(
        train_x=train_x,
        train_y=train_y,
        val_x=val_x,
        val_y=val_y,
        mean=mean.squeeze(0),
        std=std.squeeze(0),
        feature_names=samples.feature_names,
        train_timestamps=train_timestamps,
        val_timestamps=val_timestamps,
    )


def latest_window(config: DatasetConfig, mean: np.ndarray, std: np.ndarray) -> np.ndarray:
    aligned = build_aligned_rows(config)
    features = aligned["features"]
    if len(features) < config.window_size:
        raise ValueError("not enough aligned candles for inference")
    window = features[-config.window_size:].astype(np.float32)
    return ((window - mean) / std).astype(np.float32)


def build_samples(config: DatasetConfig) -> SampleBundle:
    aligned = build_aligned_rows(config)
    features = aligned["features"]
    primary_candles = aligned["primary_candles"]
    timestamps = aligned["timestamps"]
    if len(primary_candles) < config.window_size + config.lookahead + 1:
        raise ValueError("not enough aligned candles for LSTM dataset")

    labels = build_labels(primary_candles, config)
    start = config.window_size - 1
    end = len(primary_candles) - config.lookahead
    xs: list[np.ndarray] = []
    ys: list[np.ndarray] = []
    ts: list[int] = []
    for idx in range(start, end):
        window = features[idx - config.window_size + 1 : idx + 1]
        xs.append(window)
        ys.append(np.float32(labels[idx]))
        ts.append(int(timestamps[idx]))

    return SampleBundle(
        x=np.stack(xs).astype(np.float32),
        y=np.asarray(ys, dtype=np.float32),
        timestamps=np.asarray(ts, dtype=np.int64),
        feature_names=feature_names(config.intervals),
    )


def feature_names(intervals: tuple[str, ...]) -> list[str]:
    names: list[str] = []
    for interval in intervals:
        for base_name in base_feature_names():
            names.append(f"{interval}_{base_name}")
    return names


def base_feature_names() -> list[str]:
    return [
        "log_return",
        "range_over_atr",
        "body_over_atr",
        "upper_wick_over_atr",
        "lower_wick_over_atr",
        "atr_pct",
        "ema_spread_pct",
        "close_to_ema21_pct",
        "close_to_ema55_pct",
        "rsi_centered",
        "volume_zscore",
        "quote_volume_zscore",
        "trade_count_log",
        "taker_buy_ratio",
    ]


def build_aligned_rows(config: DatasetConfig) -> dict[str, object]:
    intervals = tuple(config.intervals)
    if not intervals:
        raise ValueError("at least one interval is required")

    tables = {interval: build_interval_feature_table(read_candles(config.data_dir, config.symbol, interval)) for interval in intervals}
    primary_interval = intervals[0]
    primary_table = tables[primary_interval]
    primary_close_times = primary_table["close_times"]

    pointers = {interval: 0 for interval in intervals}
    rows: list[np.ndarray] = []
    primary_candles: list[dict] = []
    timestamps: list[int] = []
    for idx, close_time in enumerate(primary_close_times):
        aligned_row: list[float] = []
        aligned = True
        for interval in intervals:
            table = tables[interval]
            current = pointers[interval]
            while current + 1 < len(table["close_times"]) and table["close_times"][current + 1] <= close_time:
                current += 1
            pointers[interval] = current
            if table["close_times"][current] > close_time:
                aligned = False
                break
            aligned_row.extend(table["features"][current].tolist())
        if not aligned:
            continue
        rows.append(np.asarray(aligned_row, dtype=np.float32))
        primary_candles.append(primary_table["candles"][idx])
        timestamps.append(int(close_time))

    if not rows:
        raise ValueError("no aligned multiframe candles available for dataset")

    return {
        "features": np.stack(rows).astype(np.float32),
        "primary_candles": primary_candles,
        "timestamps": np.asarray(timestamps, dtype=np.int64),
    }


def build_interval_feature_table(candles: list[dict]) -> dict[str, object]:
    if not candles:
        raise ValueError("empty candle series")
    features = build_features(candles)
    close_times = np.asarray([parse_time_millis(c["closeTime"]) for c in candles], dtype=np.int64)
    return {
        "candles": candles,
        "features": features,
        "close_times": close_times,
    }


def build_features(candles: list[dict]) -> np.ndarray:
    rows: list[list[float]] = []
    volumes = np.asarray([float(c["volume"]) for c in candles], dtype=np.float64)
    quote_volumes = np.asarray([float(c["quoteVolume"]) for c in candles], dtype=np.float64)
    volume_logs = np.log1p(np.maximum(volumes, 0.0))
    quote_logs = np.log1p(np.maximum(quote_volumes, 0.0))
    vol_mean = rolling_mean(volume_logs, 20)
    vol_std = rolling_std(volume_logs, 20)
    qv_mean = rolling_mean(quote_logs, 20)
    qv_std = rolling_std(quote_logs, 20)

    prev_close = float(candles[0]["close"])
    for idx, candle in enumerate(candles):
        open_price = float(candle["open"])
        high = float(candle["high"])
        low = float(candle["low"])
        close = float(candle["close"])
        atr = max(float(candle.get("atr", 0.0)), max(close * 0.0005, 1e-6))
        ema21 = float(candle.get("ema21", close))
        ema55 = float(candle.get("ema55", close))
        rsi = float(candle.get("rsi", 50.0))
        volume = volume_logs[idx]
        quote_volume = quote_logs[idx]
        taker_buy_volume = max(float(candle.get("takerBuyVolume", 0.0)), 0.0)
        total_volume = max(float(candle.get("volume", 0.0)), 0.0)
        body = close - open_price
        upper_wick = max(high - max(open_price, close), 0.0)
        lower_wick = max(min(open_price, close) - low, 0.0)
        rows.append(
            [
                safe_log_return(close, prev_close),
                (high - low) / atr,
                abs(body) / atr,
                upper_wick / atr,
                lower_wick / atr,
                atr / max(close, 1e-6),
                (ema21 - ema55) / max(close, 1e-6),
                (close - ema21) / max(close, 1e-6),
                (close - ema55) / max(close, 1e-6),
                (rsi - 50.0) / 50.0,
                zscore(volume, vol_mean[idx], vol_std[idx]),
                zscore(quote_volume, qv_mean[idx], qv_std[idx]),
                math.log1p(max(float(candle.get("numTrades", 0.0)), 0.0)),
                taker_buy_volume / max(total_volume, 1e-6),
            ]
        )
        prev_close = close
    return np.asarray(rows, dtype=np.float32)


def build_labels(candles: list[dict], config: DatasetConfig) -> np.ndarray:
    labels = np.zeros(len(candles), dtype=np.float32)
    for idx in range(len(candles) - config.lookahead):
        current = candles[idx]
        future = candles[idx + 1 : idx + 1 + config.lookahead]
        close = float(current["close"])
        atr = max(float(current.get("atr", 0.0)), max(close * 0.0005, 1e-6))
        final_close = float(future[-1]["close"])
        future_high = max(float(c["high"]) for c in future)
        future_low = min(float(c["low"]) for c in future)

        up_sweep = (
            future_high >= close + config.spike_atr_multiple * atr
            and final_close <= close + config.reversal_atr_multiple * atr
        )
        down_sweep = (
            future_low <= close - config.spike_atr_multiple * atr
            and final_close >= close - config.reversal_atr_multiple * atr
        )
        labels[idx] = 1.0 if up_sweep or down_sweep else 0.0
    return labels


def read_candles(data_dir: Path, symbol: str, interval: str = "1m") -> list[dict]:
    base = Path(data_dir) / symbol / interval
    if not base.exists():
        raise FileNotFoundError(f"{interval} data directory not found: {base}")

    rows: list[dict] = []
    for path in sorted(base.glob("*.jsonl")):
        rows.extend(read_jsonl(path))
    rows.sort(key=lambda item: parse_time_millis(item.get("openTime", "")))
    return rows


def read_jsonl(path: Path) -> list[dict]:
    out: list[dict] = []
    with path.open("r", encoding="utf-8") as handle:
        for line in handle:
            raw = line.strip()
            if not raw:
                continue
            out.append(json.loads(raw))
    return out


def rolling_mean(values: np.ndarray, window: int) -> np.ndarray:
    out = np.zeros_like(values)
    for idx in range(len(values)):
        start = max(0, idx - window + 1)
        out[idx] = values[start : idx + 1].mean()
    return out


def rolling_std(values: np.ndarray, window: int) -> np.ndarray:
    out = np.zeros_like(values)
    for idx in range(len(values)):
        start = max(0, idx - window + 1)
        chunk = values[start : idx + 1]
        std = chunk.std()
        out[idx] = std if std >= 1e-6 else 1.0
    return out


def zscore(value: float, mean: float, std: float) -> float:
    return (value - mean) / max(std, 1e-6)


def safe_log_return(value: float, prev: float) -> float:
    return math.log(max(value, 1e-6) / max(prev, 1e-6))


def parse_time_millis(value: str) -> int:
    text = str(value).strip()
    if not text:
        return 0
    if text.endswith("Z"):
        text = text[:-1] + "+00:00"
    dt = datetime.fromisoformat(text)
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return int(dt.timestamp() * 1000)