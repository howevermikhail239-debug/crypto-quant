"""Collector Operational Health and Data Quality Guards (Phase 1C Item 7E).

Provides decoupled status tracking separating Availability vs Completeness:
- Availability Status: HEALTHY, DEGRADED, RECONNECTING, FAILED
- Completeness Status: COMPLETE, RECOVERED, PARTIAL, GAPPED, UNKNOWN
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from enum import StrEnum
from pathlib import Path

from ..time import utc_now
from .gap_registry import GapRegistry, GapStatus


class AvailabilityStatus(StrEnum):
    HEALTHY = "HEALTHY"
    DEGRADED = "DEGRADED"
    RECONNECTING = "RECONNECTING"
    FAILED = "FAILED"


class CompletenessStatus(StrEnum):
    COMPLETE = "COMPLETE"
    RECOVERED = "RECOVERED"
    PARTIAL = "PARTIAL"
    GAPPED = "GAPPED"
    UNKNOWN = "UNKNOWN"


@dataclass
class CollectorHealthState:
    exchange: str
    market_type: str
    symbol: str
    availability: AvailabilityStatus
    completeness: CompletenessStatus
    open_gap_count: int
    partial_gap_count: int
    unrecoverable_gap_count: int
    last_updated_at: datetime


def compute_collector_health(
    *,
    exchange: str,
    market_type: str,
    symbol: str,
    root: Path,
    current_availability: AvailabilityStatus = AvailabilityStatus.HEALTHY,
) -> CollectorHealthState:
    """Evaluates operational availability and data completeness using GapRegistry."""
    registry = GapRegistry(root)
    all_gaps = registry.list_gaps()

    instrument_gaps = [
        g for g in all_gaps if g.exchange == exchange and g.market_type == market_type
    ]

    open_count = sum(1 for g in instrument_gaps if g.status == GapStatus.OPEN)
    partial_count = sum(1 for g in instrument_gaps if g.status == GapStatus.PARTIAL)
    unrecoverable_count = sum(1 for g in instrument_gaps if g.status == GapStatus.UNRECOVERABLE)

    if open_count > 0:
        completeness = CompletenessStatus.GAPPED
    elif partial_count > 0 or unrecoverable_count > 0:
        completeness = CompletenessStatus.PARTIAL
    elif any(g.status == GapStatus.RECOVERED for g in instrument_gaps):
        completeness = CompletenessStatus.RECOVERED
    else:
        completeness = CompletenessStatus.COMPLETE

    return CollectorHealthState(
        exchange=exchange,
        market_type=market_type,
        symbol=symbol,
        availability=current_availability,
        completeness=completeness,
        open_gap_count=open_count,
        partial_gap_count=partial_count,
        unrecoverable_gap_count=unrecoverable_count,
        last_updated_at=utc_now(),
    )
