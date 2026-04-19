from __future__ import annotations

from dataclasses import dataclass

import torch
from torch import nn


@dataclass
class LSTMConfig:
    input_size: int
    hidden_size: int = 64
    num_layers: int = 2
    dropout: float = 0.15


class HarvestPathLSTM(nn.Module):
    def __init__(self, config: LSTMConfig) -> None:
        super().__init__()
        dropout = config.dropout if config.num_layers > 1 else 0.0
        self.lstm = nn.LSTM(
            input_size=config.input_size,
            hidden_size=config.hidden_size,
            num_layers=config.num_layers,
            dropout=dropout,
            batch_first=True,
        )
        self.classifier = nn.Sequential(
            nn.Linear(config.hidden_size, config.hidden_size),
            nn.ReLU(),
            nn.Dropout(config.dropout),
            nn.Linear(config.hidden_size, 1),
        )

    def forward(self, features: torch.Tensor) -> torch.Tensor:
        output, _ = self.lstm(features)
        last_state = output[:, -1, :]
        logits = self.classifier(last_state)
        return logits.squeeze(-1)