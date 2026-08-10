"""Bounded Reconnect Policy with Exponential Backoff and Jitter (Phase 1C Item 7C).

Calculates deterministic or jittered reconnect delays for WebSocket reconnect loops.
"""

from __future__ import annotations

import random
from dataclasses import dataclass


@dataclass(frozen=True)
class ReconnectConfig:
    initial_delay_sec: float = 1.0
    max_delay_sec: float = 30.0
    backoff_multiplier: float = 2.0
    jitter_ratio: float = 0.2
    max_attempts: int = 10


def compute_reconnect_delay(
    attempt: int,
    config: ReconnectConfig | None = None,
    seed: int | None = None,
) -> float:
    if config is None:
        config = ReconnectConfig()
    """Computes bounded exponential backoff delay with jitter.

    Formula:
      raw_delay = min(max_delay, initial_delay * (multiplier ** (attempt - 1)))
      jitter = raw_delay * jitter_ratio * random_factor
      delay = raw_delay + jitter
    """
    if attempt <= 0:
        return 0.0

    raw_delay = min(
        config.max_delay_sec,
        config.initial_delay_sec * (config.backoff_multiplier ** (attempt - 1)),
    )

    if config.jitter_ratio > 0:
        rng = random.Random(seed) if seed is not None else random
        # Uniform jitter in range [-jitter_ratio * raw_delay, +jitter_ratio * raw_delay]
        jitter_delta = (rng.random() * 2 - 1) * (raw_delay * config.jitter_ratio)
        delay = max(0.1, raw_delay + jitter_delta)
    else:
        delay = raw_delay

    return round(delay, 4)
