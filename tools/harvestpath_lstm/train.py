from __future__ import annotations

import argparse
import json
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path

import numpy as np
import torch
from torch import nn
from torch.utils.data import DataLoader, TensorDataset

from data import DatasetConfig, SampleBundle, build_samples
from model import HarvestPathLSTM, LSTMConfig


@dataclass
class TrainResult:
    best_state: dict[str, torch.Tensor]
    best_val_loss: float
    mean: np.ndarray
    std: np.ndarray
    val_accuracy: float
    val_precision: float
    val_recall: float
    val_f1: float
    best_threshold: float
    best_threshold_metrics: "EvalMetrics"
    threshold_scan: list[dict[str, float | int]]
    segmented_threshold_scan: dict[str, dict[str, list[dict[str, float | int]]]]
    train_positive_ratio: float
    train_range: tuple[int, int]
    val_range: tuple[int, int]


@dataclass
class EvalMetrics:
    threshold: float
    accuracy: float
    precision: float
    recall: float
    f1: float
    tp: int
    fp: int
    tn: int
    fn: int


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Train LSTM for liquidity sweep risk recognition")
    parser.add_argument("--data-dir", default="app/strategy/rpc/data/kline", help="1m Kline root directory")
    parser.add_argument("--symbol", default="ETHUSDT")
    parser.add_argument("--intervals", default="1m,5m,15m", help="comma separated intervals, first one is the primary interval")
    parser.add_argument("--window-size", type=int, default=64)
    parser.add_argument("--lookahead", type=int, default=8)
    parser.add_argument("--epochs", type=int, default=20)
    parser.add_argument("--batch-size", type=int, default=128)
    parser.add_argument("--lr", type=float, default=1e-3)
    parser.add_argument("--hidden-size", type=int, default=64)
    parser.add_argument("--num-layers", type=int, default=2)
    parser.add_argument("--dropout", type=float, default=0.15)
    parser.add_argument("--rolling-train-ratio", type=float, default=0.6)
    parser.add_argument("--rolling-val-ratio", type=float, default=0.2)
    parser.add_argument("--rolling-step-ratio", type=float, default=0.1)
    parser.add_argument("--max-windows", type=int, default=6)
    parser.add_argument("--thresholds", default="0.30,0.40,0.50,0.60,0.70", help="comma separated decision thresholds for validation scan")
    parser.add_argument("--output-dir", default="tools/harvestpath_lstm/artifacts")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    config = DatasetConfig(
        data_dir=Path(args.data_dir),
        symbol=args.symbol,
        window_size=args.window_size,
        lookahead=args.lookahead,
        intervals=parse_intervals(args.intervals),
    )
    samples = build_samples(config)
    thresholds = parse_thresholds(args.thresholds)

    device = torch.device("cuda" if torch.cuda.is_available() else "cpu")
    model_config = LSTMConfig(
        input_size=samples.x.shape[-1],
        hidden_size=args.hidden_size,
        num_layers=args.num_layers,
        dropout=args.dropout,
    )
    windows = generate_rolling_windows(
        total=len(samples.x),
        train_ratio=args.rolling_train_ratio,
        val_ratio=args.rolling_val_ratio,
        step_ratio=args.rolling_step_ratio,
        max_windows=args.max_windows,
    )
    if not windows:
        raise ValueError("rolling window split is empty")

    window_metrics: list[dict[str, object]] = []
    train_results: list[TrainResult] = []
    latest_result: TrainResult | None = None
    for index, window in enumerate(windows, start=1):
        print(
            f"rolling_window={index}/{len(windows)} "
            f"train=[{window['train_start']},{window['train_end']}) "
            f"val=[{window['val_start']},{window['val_end']})"
        )
        result = train_one_window(
            samples=samples,
            model_config=model_config,
            batch_size=args.batch_size,
            epochs=args.epochs,
            lr=args.lr,
            device=device,
            train_start=window["train_start"],
            train_end=window["train_end"],
            val_start=window["val_start"],
            val_end=window["val_end"],
            thresholds=thresholds,
        )
        latest_result = result
        train_results.append(result)
        window_metrics.append(
            {
                "window_index": index,
                "train_range": [window["train_start"], window["train_end"]],
                "val_range": [window["val_start"], window["val_end"]],
                "train_start_time": format_millis(samples.timestamps[window["train_start"]]),
                "train_end_time": format_millis(samples.timestamps[window["train_end"] - 1]),
                "val_start_time": format_millis(samples.timestamps[window["val_start"]]),
                "val_end_time": format_millis(samples.timestamps[window["val_end"] - 1]),
                "train_positive_ratio": result.train_positive_ratio,
                "best_val_loss": result.best_val_loss,
                "val_accuracy": result.val_accuracy,
                "val_precision": result.val_precision,
                "val_recall": result.val_recall,
                "val_f1": result.val_f1,
                "best_threshold": result.best_threshold,
                "best_threshold_metrics": metrics_to_dict(result.best_threshold_metrics),
                "threshold_scan": result.threshold_scan,
            }
        )

    if latest_result is None:
        raise RuntimeError("no rolling window training result produced")
    threshold_stability_report = summarize_threshold_stability(train_results)
    threshold_recommendation_report = summarize_threshold_recommendations(train_results)
    segmented_performance_report = summarize_segmented_performance(train_results, threshold_recommendation_report)
    regime_aware_threshold_strategy = summarize_regime_aware_threshold_strategy(train_results, threshold_recommendation_report)
    print(
        f"threshold_stability recommended={threshold_stability_report['recommended_threshold']:.2f} "
        f"dominant_ratio={threshold_stability_report['dominant_ratio']:.4f} "
        f"std={threshold_stability_report['std_best_threshold']:.4f} "
        f"band={threshold_stability_report['stability_band']}"
    )
    print(
        "threshold_recommendations "
        f"precision={threshold_recommendation_report['precision_priority']['threshold']:.2f} "
        f"recall={threshold_recommendation_report['recall_priority']['threshold']:.2f} "
        f"f1={threshold_recommendation_report['f1_balance']['threshold']:.2f}"
    )
    print(
        "regime_aware_thresholds "
        f"LOW={regime_aware_threshold_strategy['regimes'].get('LOW', {}).get('recommended_threshold', regime_aware_threshold_strategy['default_threshold']):.2f} "
        f"MID={regime_aware_threshold_strategy['regimes'].get('MID', {}).get('recommended_threshold', regime_aware_threshold_strategy['default_threshold']):.2f} "
        f"HIGH={regime_aware_threshold_strategy['regimes'].get('HIGH', {}).get('recommended_threshold', regime_aware_threshold_strategy['default_threshold']):.2f}"
    )

    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    checkpoint_path = output_dir / f"{args.symbol.lower()}_harvest_path_lstm.pt"
    metadata_path = output_dir / f"{args.symbol.lower()}_harvest_path_lstm.json"
    torch.save(latest_result.best_state, checkpoint_path)
    metadata = {
        "symbol": args.symbol,
        "window_size": args.window_size,
        "lookahead": args.lookahead,
        "intervals": list(config.intervals),
        "feature_names": samples.feature_names,
        "mean": latest_result.mean.tolist(),
        "std": latest_result.std.tolist(),
        "hidden_size": args.hidden_size,
        "num_layers": args.num_layers,
        "dropout": args.dropout,
        "rolling_train_ratio": args.rolling_train_ratio,
        "rolling_val_ratio": args.rolling_val_ratio,
        "rolling_step_ratio": args.rolling_step_ratio,
        "max_windows": args.max_windows,
        "thresholds": thresholds,
        "selected_window": "latest",
        "positive_ratio": latest_result.train_positive_ratio,
        "best_val_loss": latest_result.best_val_loss,
        "val_accuracy": latest_result.val_accuracy,
        "val_precision": latest_result.val_precision,
        "val_recall": latest_result.val_recall,
        "val_f1": latest_result.val_f1,
        "best_threshold": latest_result.best_threshold,
        "best_threshold_metrics": metrics_to_dict(latest_result.best_threshold_metrics),
        "threshold_stability_report": threshold_stability_report,
        "threshold_recommendation_report": threshold_recommendation_report,
        "segmented_performance_report": segmented_performance_report,
        "regime_aware_threshold_strategy": regime_aware_threshold_strategy,
        "window_metrics": window_metrics,
        "checkpoint": str(checkpoint_path),
    }
    metadata_path.write_text(json.dumps(metadata, indent=2), encoding="utf-8")
    print(f"saved_checkpoint={checkpoint_path}")
    print(f"saved_metadata={metadata_path}")


def train_one_window(
    samples: SampleBundle,
    model_config: LSTMConfig,
    batch_size: int,
    epochs: int,
    lr: float,
    device: torch.device,
    train_start: int,
    train_end: int,
    val_start: int,
    val_end: int,
    thresholds: list[float],
) -> TrainResult:
    train_x_raw = samples.x[train_start:train_end]
    train_y = samples.y[train_start:train_end]
    val_x_raw = samples.x[val_start:val_end]
    val_y = samples.y[val_start:val_end]

    mean = train_x_raw.mean(axis=(0, 1), keepdims=True)
    std = train_x_raw.std(axis=(0, 1), keepdims=True)
    std = np.where(std < 1e-6, 1.0, std)

    train_x = ((train_x_raw - mean) / std).astype(np.float32)
    val_x = ((val_x_raw - mean) / std).astype(np.float32)

    model = HarvestPathLSTM(model_config).to(device)
    train_loader = DataLoader(
        TensorDataset(torch.from_numpy(train_x), torch.from_numpy(train_y)),
        batch_size=batch_size,
        shuffle=True,
    )
    val_loader = DataLoader(
        TensorDataset(torch.from_numpy(val_x), torch.from_numpy(val_y)),
        batch_size=batch_size,
        shuffle=False,
    )

    positive_ratio = float(train_y.mean()) if len(train_y) > 0 else 0.0
    pos_weight = torch.tensor([(1.0 - positive_ratio) / max(positive_ratio, 1e-6)], device=device)
    criterion = nn.BCEWithLogitsLoss(pos_weight=pos_weight)
    optimizer = torch.optim.Adam(model.parameters(), lr=lr)

    best_val_loss = float("inf")
    best_state: dict[str, torch.Tensor] | None = None
    best_metrics = EvalMetrics(threshold=0.5, accuracy=0.0, precision=0.0, recall=0.0, f1=0.0, tp=0, fp=0, tn=0, fn=0)
    for epoch in range(1, epochs + 1):
        train_loss = run_epoch(model, train_loader, criterion, optimizer, device, train=True)
        val_loss = run_epoch(model, val_loader, criterion, optimizer, device, train=False)
        metrics = evaluate_classification_metrics(model, val_loader, device, threshold=0.5)
        print(
            f"  epoch={epoch} train_loss={train_loss:.6f} "
            f"val_loss={val_loss:.6f} val_accuracy={metrics.accuracy:.4f} "
            f"val_precision={metrics.precision:.4f} val_recall={metrics.recall:.4f} "
            f"val_f1={metrics.f1:.4f}"
        )
        if val_loss < best_val_loss:
            best_val_loss = val_loss
            best_metrics = metrics
            best_state = {k: v.detach().cpu() for k, v in model.state_dict().items()}

    if best_state is None:
        raise RuntimeError("failed to capture rolling window model state")

    model.load_state_dict(best_state)
    probabilities, labels = collect_probabilities_and_labels(model, val_loader, device)
    threshold_metrics, best_threshold_metrics = scan_thresholds(probabilities, labels, thresholds)
    segmented_threshold_scan = build_segmented_threshold_scan(
        probabilities=probabilities,
        labels=labels,
        thresholds=thresholds,
        timestamps=samples.timestamps[val_start:val_end],
        train_x_raw=train_x_raw,
        val_x_raw=val_x_raw,
        feature_names=samples.feature_names,
    )
    print(
        f"  threshold_scan best_threshold={best_threshold_metrics.threshold:.2f} "
        f"precision={best_threshold_metrics.precision:.4f} "
        f"recall={best_threshold_metrics.recall:.4f} "
        f"f1={best_threshold_metrics.f1:.4f}"
    )

    return TrainResult(
        best_state=best_state,
        best_val_loss=best_val_loss,
        mean=mean.squeeze(0),
        std=std.squeeze(0),
        val_accuracy=best_metrics.accuracy,
        val_precision=best_metrics.precision,
        val_recall=best_metrics.recall,
        val_f1=best_metrics.f1,
        best_threshold=best_threshold_metrics.threshold,
        best_threshold_metrics=best_threshold_metrics,
        threshold_scan=[metrics_to_dict(item) for item in threshold_metrics],
        segmented_threshold_scan=segmented_threshold_scan,
        train_positive_ratio=positive_ratio,
        train_range=(train_start, train_end),
        val_range=(val_start, val_end),
    )


def run_epoch(
    model: HarvestPathLSTM,
    loader: DataLoader,
    criterion: nn.Module,
    optimizer: torch.optim.Optimizer,
    device: torch.device,
    train: bool,
) -> float:
    model.train(mode=train)
    total_loss = 0.0
    total_count = 0
    with torch.set_grad_enabled(train):
        for features, labels in loader:
            features = features.to(device)
            labels = labels.to(device)
            logits = model(features)
            loss = criterion(logits, labels)
            if train:
                optimizer.zero_grad()
                loss.backward()
                optimizer.step()
            batch_size = features.size(0)
            total_loss += float(loss.detach().cpu()) * batch_size
            total_count += batch_size
    return total_loss / max(total_count, 1)


def evaluate_classification_metrics(model: HarvestPathLSTM, loader: DataLoader, device: torch.device, threshold: float) -> EvalMetrics:
    probabilities, labels = collect_probabilities_and_labels(model, loader, device)
    return compute_metrics(probabilities, labels, threshold)


def collect_probabilities_and_labels(model: HarvestPathLSTM, loader: DataLoader, device: torch.device) -> tuple[np.ndarray, np.ndarray]:
    model.eval()
    probs: list[np.ndarray] = []
    labels_out: list[np.ndarray] = []
    with torch.no_grad():
        for features, labels in loader:
            features = features.to(device)
            logits = model(features)
            probs.append(torch.sigmoid(logits).detach().cpu().numpy())
            labels_out.append(labels.detach().cpu().numpy())
    return np.concatenate(probs).astype(np.float32), np.concatenate(labels_out).astype(np.float32)


def compute_metrics(probabilities: np.ndarray, labels: np.ndarray, threshold: float) -> EvalMetrics:
    preds = probabilities >= threshold
    actuals = labels >= 0.5
    tp = int(np.logical_and(preds, actuals).sum())
    fp = int(np.logical_and(preds, np.logical_not(actuals)).sum())
    tn = int(np.logical_and(np.logical_not(preds), np.logical_not(actuals)).sum())
    fn = int(np.logical_and(np.logical_not(preds), actuals).sum())
    return compute_metrics_from_counts(threshold=threshold, tp=tp, fp=fp, tn=tn, fn=fn)


def compute_metrics_from_counts(threshold: float, tp: int, fp: int, tn: int, fn: int) -> EvalMetrics:
    total = tp + fp + tn + fn
    accuracy = (tp + tn) / max(total, 1)
    precision = tp / max(tp + fp, 1)
    recall = tp / max(tp + fn, 1)
    if precision + recall == 0:
        f1 = 0.0
    else:
        f1 = 2 * precision * recall / (precision + recall)
    return EvalMetrics(
        threshold=threshold,
        accuracy=accuracy,
        precision=precision,
        recall=recall,
        f1=f1,
        tp=tp,
        fp=fp,
        tn=tn,
        fn=fn,
    )


def scan_thresholds(probabilities: np.ndarray, labels: np.ndarray, thresholds: list[float]) -> tuple[list[EvalMetrics], EvalMetrics]:
    scans = [compute_metrics(probabilities, labels, threshold) for threshold in thresholds]
    best = max(scans, key=lambda item: (item.f1, item.precision, item.recall, -abs(item.threshold - 0.5)))
    return scans, best


def parse_intervals(value: str) -> tuple[str, ...]:
    intervals = tuple(part.strip() for part in value.split(",") if part.strip())
    if not intervals:
        raise ValueError("intervals cannot be empty")
    return intervals


def parse_thresholds(value: str) -> list[float]:
    thresholds = [float(part.strip()) for part in value.split(",") if part.strip()]
    thresholds = sorted({min(1.0, max(0.0, item)) for item in thresholds})
    if not thresholds:
        raise ValueError("thresholds cannot be empty")
    return thresholds


def generate_rolling_windows(
    total: int,
    train_ratio: float,
    val_ratio: float,
    step_ratio: float,
    max_windows: int,
) -> list[dict[str, int]]:
    if total < 2:
        return []
    train_size = max(1, int(total * train_ratio))
    val_size = max(1, int(total * val_ratio))
    if train_size + val_size > total:
        train_size = max(1, total - val_size)
    step_size = max(1, int(total * step_ratio))

    windows: list[dict[str, int]] = []
    start = 0
    while start + train_size + val_size <= total:
        train_end = start + train_size
        val_end = train_end + val_size
        windows.append(
            {
                "train_start": start,
                "train_end": train_end,
                "val_start": train_end,
                "val_end": val_end,
            }
        )
        start += step_size

    if not windows:
        train_end = max(1, total - val_size)
        windows.append(
            {
                "train_start": 0,
                "train_end": train_end,
                "val_start": train_end,
                "val_end": total,
            }
        )
    if max_windows > 0 and len(windows) > max_windows:
        windows = windows[-max_windows:]
    return windows


def format_millis(value: np.int64) -> str:
    ts = int(value)
    if ts <= 0:
        return ""
    return datetime.fromtimestamp(ts / 1000.0, tz=timezone.utc).isoformat()


def metrics_to_dict(metrics: EvalMetrics) -> dict[str, float | int]:
    return {
        "threshold": metrics.threshold,
        "accuracy": metrics.accuracy,
        "precision": metrics.precision,
        "recall": metrics.recall,
        "f1": metrics.f1,
        "tp": metrics.tp,
        "fp": metrics.fp,
        "tn": metrics.tn,
        "fn": metrics.fn,
    }


def metrics_from_dict(payload: dict[str, float | int]) -> EvalMetrics:
    return EvalMetrics(
        threshold=float(payload["threshold"]),
        accuracy=float(payload["accuracy"]),
        precision=float(payload["precision"]),
        recall=float(payload["recall"]),
        f1=float(payload["f1"]),
        tp=int(payload["tp"]),
        fp=int(payload["fp"]),
        tn=int(payload["tn"]),
        fn=int(payload["fn"]),
    )


def merge_metrics(left: EvalMetrics, right: EvalMetrics) -> EvalMetrics:
    if abs(left.threshold - right.threshold) > 1e-9:
        raise ValueError("cannot merge metrics with different thresholds")
    return compute_metrics_from_counts(
        threshold=left.threshold,
        tp=left.tp + right.tp,
        fp=left.fp + right.fp,
        tn=left.tn + right.tn,
        fn=left.fn + right.fn,
    )


def summarize_threshold_stability(results: list[TrainResult]) -> dict[str, object]:
    if not results:
        return {
            "window_count": 0,
            "recommended_threshold": 0.5,
            "dominant_threshold": 0.5,
            "dominant_count": 0,
            "dominant_ratio": 0.0,
            "mean_best_threshold": 0.5,
            "std_best_threshold": 0.0,
            "min_best_threshold": 0.5,
            "max_best_threshold": 0.5,
            "mean_best_f1": 0.0,
            "min_best_f1": 0.0,
            "max_best_f1": 0.0,
            "stability_band": "UNSTABLE",
            "is_stable": False,
            "best_thresholds": [],
            "threshold_frequency": [],
        }

    best_thresholds = np.asarray([result.best_threshold for result in results], dtype=np.float64)
    best_f1s = np.asarray([result.best_threshold_metrics.f1 for result in results], dtype=np.float64)
    unique_thresholds, counts = np.unique(best_thresholds, return_counts=True)
    best_index = int(np.argmax(counts))
    dominant_threshold = float(unique_thresholds[best_index])
    dominant_count = int(counts[best_index])
    dominant_ratio = dominant_count / len(results)
    std_best_threshold = float(best_thresholds.std()) if len(best_thresholds) > 1 else 0.0
    stability_band = classify_threshold_stability(dominant_ratio, std_best_threshold)

    frequency = []
    for threshold, count in zip(unique_thresholds.tolist(), counts.tolist()):
        frequency.append(
            {
                "threshold": float(threshold),
                "count": int(count),
                "ratio": count / len(results),
            }
        )

    return {
        "window_count": len(results),
        "recommended_threshold": dominant_threshold,
        "dominant_threshold": dominant_threshold,
        "dominant_count": dominant_count,
        "dominant_ratio": dominant_ratio,
        "mean_best_threshold": float(best_thresholds.mean()),
        "std_best_threshold": std_best_threshold,
        "min_best_threshold": float(best_thresholds.min()),
        "max_best_threshold": float(best_thresholds.max()),
        "mean_best_f1": float(best_f1s.mean()),
        "min_best_f1": float(best_f1s.min()),
        "max_best_f1": float(best_f1s.max()),
        "stability_band": stability_band,
        "is_stable": stability_band == "STABLE",
        "best_thresholds": [float(item) for item in best_thresholds.tolist()],
        "threshold_frequency": frequency,
    }


def summarize_threshold_recommendations(results: list[TrainResult]) -> dict[str, object]:
    threshold_groups = aggregate_threshold_metrics(results)
    if not threshold_groups:
        fallback = {
            "strategy": "f1_balance",
            "threshold": 0.5,
            "window_coverage": 0,
            "mean_accuracy": 0.0,
            "mean_precision": 0.0,
            "mean_recall": 0.0,
            "mean_f1": 0.0,
        }
        return {
            "precision_priority": {**fallback, "strategy": "precision_priority"},
            "recall_priority": {**fallback, "strategy": "recall_priority"},
            "f1_balance": fallback,
        }

    return {
        "precision_priority": select_threshold_recommendation(threshold_groups, strategy="precision_priority"),
        "recall_priority": select_threshold_recommendation(threshold_groups, strategy="recall_priority"),
        "f1_balance": select_threshold_recommendation(threshold_groups, strategy="f1_balance"),
    }


def summarize_segmented_performance(
    results: list[TrainResult],
    threshold_recommendation_report: dict[str, object],
) -> dict[str, object]:
    selected_thresholds = {
        "precision_priority": float(threshold_recommendation_report["precision_priority"]["threshold"]),
        "recall_priority": float(threshold_recommendation_report["recall_priority"]["threshold"]),
        "f1_balance": float(threshold_recommendation_report["f1_balance"]["threshold"]),
    }
    return {
        "by_month": summarize_segment_category(results, "by_month", selected_thresholds),
        "by_volatility_regime": summarize_segment_category(results, "by_volatility_regime", selected_thresholds),
    }


def summarize_regime_aware_threshold_strategy(
    results: list[TrainResult],
    threshold_recommendation_report: dict[str, object],
) -> dict[str, object]:
    default_threshold = float(threshold_recommendation_report["f1_balance"]["threshold"])
    grouped = aggregate_segment_category(results, "by_volatility_regime")
    regimes: dict[str, object] = {}
    for regime_name, threshold_map in grouped.items():
        threshold_groups = threshold_metrics_summary(threshold_map)
        if not threshold_groups:
            continue
        precision_priority = select_threshold_recommendation(threshold_groups, strategy="precision_priority")
        recall_priority = select_threshold_recommendation(threshold_groups, strategy="recall_priority")
        f1_balance = select_threshold_recommendation(threshold_groups, strategy="f1_balance")
        sample_metrics = next(iter(threshold_map.values()))
        sample_count = sample_metrics.tp + sample_metrics.fp + sample_metrics.tn + sample_metrics.fn
        positive_count = sample_metrics.tp + sample_metrics.fn
        regimes[regime_name] = {
            "sample_count": sample_count,
            "positive_ratio": positive_count / max(sample_count, 1),
            "fallback_threshold": default_threshold,
            "recommended_threshold": float(f1_balance["threshold"]),
            "use_regime_threshold": sample_count >= 30,
            "precision_priority": precision_priority,
            "recall_priority": recall_priority,
            "f1_balance": f1_balance,
        }

    return {
        "default_threshold": default_threshold,
        "selection_strategy": "f1_balance_by_regime",
        "fallback_strategy": "global_f1_balance",
        "regimes": regimes,
    }


def summarize_segment_category(
    results: list[TrainResult],
    category: str,
    selected_thresholds: dict[str, float],
) -> dict[str, object]:
    grouped = aggregate_segment_category(results, category)
    summary: dict[str, object] = {}
    for group_name, threshold_map in grouped.items():
        sample_count = 0
        positive_count = 0
        strategies: dict[str, object] = {}
        for strategy_name, threshold in selected_thresholds.items():
            metrics = threshold_map.get(threshold)
            if metrics is None:
                continue
            if sample_count == 0:
                sample_count = metrics.tp + metrics.fp + metrics.tn + metrics.fn
                positive_count = metrics.tp + metrics.fn
            strategies[strategy_name] = metrics_to_dict(metrics)
        if sample_count == 0 and threshold_map:
            first_metrics = next(iter(threshold_map.values()))
            sample_count = first_metrics.tp + first_metrics.fp + first_metrics.tn + first_metrics.fn
            positive_count = first_metrics.tp + first_metrics.fn
        summary[group_name] = {
            "sample_count": sample_count,
            "positive_ratio": positive_count / max(sample_count, 1),
            **strategies,
        }
    return summary


def aggregate_segment_category(results: list[TrainResult], category: str) -> dict[str, dict[float, EvalMetrics]]:
    grouped: dict[str, dict[float, EvalMetrics]] = {}
    for result in results:
        category_map = result.segmented_threshold_scan.get(category, {})
        for group_name, scans in category_map.items():
            threshold_map = grouped.setdefault(group_name, {})
            for scan in scans:
                threshold = float(scan["threshold"])
                incoming = metrics_from_dict(scan)
                existing = threshold_map.get(threshold)
                if existing is None:
                    threshold_map[threshold] = incoming
                    continue
                threshold_map[threshold] = merge_metrics(existing, incoming)
    return grouped


def threshold_metrics_summary(threshold_map: dict[float, EvalMetrics]) -> dict[float, dict[str, float | int]]:
    summary: dict[float, dict[str, float | int]] = {}
    for threshold, metrics in threshold_map.items():
        sample_count = metrics.tp + metrics.fp + metrics.tn + metrics.fn
        summary[threshold] = {
            "threshold": threshold,
            "window_coverage": sample_count,
            "mean_accuracy": metrics.accuracy,
            "mean_precision": metrics.precision,
            "mean_recall": metrics.recall,
            "mean_f1": metrics.f1,
        }
    return summary


def build_segmented_threshold_scan(
    probabilities: np.ndarray,
    labels: np.ndarray,
    thresholds: list[float],
    timestamps: np.ndarray,
    train_x_raw: np.ndarray,
    val_x_raw: np.ndarray,
    feature_names: list[str],
) -> dict[str, dict[str, list[dict[str, float | int]]]]:
    months = [format_month(ts) for ts in timestamps]
    volatility_regimes = assign_volatility_regimes(train_x_raw, val_x_raw, feature_names)
    return {
        "by_month": build_threshold_scan_for_groups(probabilities, labels, thresholds, months),
        "by_volatility_regime": build_threshold_scan_for_groups(probabilities, labels, thresholds, volatility_regimes),
    }


def build_threshold_scan_for_groups(
    probabilities: np.ndarray,
    labels: np.ndarray,
    thresholds: list[float],
    groups: list[str],
) -> dict[str, list[dict[str, float | int]]]:
    out: dict[str, list[dict[str, float | int]]] = {}
    if len(groups) != len(probabilities):
        raise ValueError("groups length does not match probabilities length")
    unique_groups = sorted({group for group in groups if group})
    for group in unique_groups:
        mask = np.asarray([item == group for item in groups], dtype=bool)
        if not mask.any():
            continue
        out[group] = [metrics_to_dict(compute_metrics(probabilities[mask], labels[mask], threshold)) for threshold in thresholds]
    return out


def assign_volatility_regimes(train_x_raw: np.ndarray, val_x_raw: np.ndarray, feature_names: list[str]) -> list[str]:
    atr_idx = primary_atr_pct_feature_index(feature_names)
    train_atr = train_x_raw[:, -1, atr_idx]
    val_atr = val_x_raw[:, -1, atr_idx]
    low_cut = float(np.quantile(train_atr, 0.33))
    high_cut = float(np.quantile(train_atr, 0.67))

    labels: list[str] = []
    for value in val_atr.tolist():
        if value <= low_cut:
            labels.append("LOW")
        elif value >= high_cut:
            labels.append("HIGH")
        else:
            labels.append("MID")
    return labels


def primary_atr_pct_feature_index(feature_names: list[str]) -> int:
    for idx, name in enumerate(feature_names):
        if name.endswith("_atr_pct"):
            return idx
    raise ValueError("primary atr_pct feature not found")


def format_month(timestamp_ms: np.int64) -> str:
    ts = int(timestamp_ms)
    if ts <= 0:
        return "UNKNOWN"
    return datetime.fromtimestamp(ts / 1000.0, tz=timezone.utc).strftime("%Y-%m")


def aggregate_threshold_metrics(results: list[TrainResult]) -> dict[float, dict[str, float | int]]:
    grouped: dict[float, list[dict[str, float | int]]] = {}
    for result in results:
        for scan in result.threshold_scan:
            threshold = float(scan["threshold"])
            grouped.setdefault(threshold, []).append(scan)

    summary: dict[float, dict[str, float | int]] = {}
    for threshold, scans in grouped.items():
        accuracy = np.asarray([float(item["accuracy"]) for item in scans], dtype=np.float64)
        precision = np.asarray([float(item["precision"]) for item in scans], dtype=np.float64)
        recall = np.asarray([float(item["recall"]) for item in scans], dtype=np.float64)
        f1 = np.asarray([float(item["f1"]) for item in scans], dtype=np.float64)
        summary[threshold] = {
            "threshold": threshold,
            "window_coverage": len(scans),
            "mean_accuracy": float(accuracy.mean()),
            "mean_precision": float(precision.mean()),
            "mean_recall": float(recall.mean()),
            "mean_f1": float(f1.mean()),
        }
    return summary


def select_threshold_recommendation(
    threshold_groups: dict[float, dict[str, float | int]],
    strategy: str,
) -> dict[str, object]:
    items = list(threshold_groups.values())
    if strategy == "precision_priority":
        best = max(
            items,
            key=lambda item: (
                float(item["mean_precision"]),
                float(item["mean_f1"]),
                float(item["mean_recall"]),
                float(item["threshold"]),
            ),
        )
    elif strategy == "recall_priority":
        best = max(
            items,
            key=lambda item: (
                float(item["mean_recall"]),
                float(item["mean_f1"]),
                float(item["mean_precision"]),
                -float(item["threshold"]),
            ),
        )
    else:
        best = max(
            items,
            key=lambda item: (
                float(item["mean_f1"]),
                float(item["mean_precision"]),
                float(item["mean_recall"]),
                -abs(float(item["threshold"]) - 0.5),
            ),
        )
    return {
        "strategy": strategy,
        "threshold": float(best["threshold"]),
        "window_coverage": int(best["window_coverage"]),
        "mean_accuracy": float(best["mean_accuracy"]),
        "mean_precision": float(best["mean_precision"]),
        "mean_recall": float(best["mean_recall"]),
        "mean_f1": float(best["mean_f1"]),
    }


def classify_threshold_stability(dominant_ratio: float, std_best_threshold: float) -> str:
    if dominant_ratio >= 0.7 and std_best_threshold <= 0.05:
        return "STABLE"
    if dominant_ratio >= 0.5 and std_best_threshold <= 0.10:
        return "MODERATE"
    return "UNSTABLE"


if __name__ == "__main__":
    main()