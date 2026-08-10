"""UTC-only time helpers. Naive timestamps are intentionally rejected."""

from __future__ import annotations

from datetime import UTC, datetime
from typing import Literal


def utc_now() -> datetime:
    return datetime.now(UTC)


def require_utc(value: datetime) -> datetime:
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError("Timestamp must be timezone-aware")
    return value.astimezone(UTC)


def parse_epoch(value: int, *, unit: Literal["ms", "us"]) -> datetime:
    """Parse an exchange epoch only when the producer explicitly states its unit."""
    divisor = {"ms": 1_000, "us": 1_000_000}[unit]
    return datetime.fromtimestamp(value / divisor, tz=UTC)


def knowledge_available(*, knowledge_time: datetime, decision_time: datetime) -> bool:
    return require_utc(knowledge_time) <= require_utc(decision_time)
