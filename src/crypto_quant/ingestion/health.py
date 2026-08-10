"""Collector Operational Health and Data Quality Guards (Phase 1C Item 7E Audit).

Provides decoupled status tracking separating Availability vs Completeness and disk thresholds:
- Availability Status: HEALTHY, DEGRADED, RECONNECTING, FAILED
- Completeness Status: COMPLETE, RECOVERED, PARTIAL, GAPPED, UNKNOWN
- Disk Status: OK, WARNING, CRITICAL_STOP
"""

from __future__ import annotations

import shutil
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


class DiskThresholdStatus(StrEnum):
    OK = "OK"
    WARNING = "WARNING"
    CRITICAL_STOP = "CRITICAL_STOP"


@dataclass
class CollectorHealthState:
    exchange: str
    market_type: str
    symbol: str
    availability: AvailabilityStatus
    completeness: CompletenessStatus
    disk_status: DiskThresholdStatus
    disk_free_bytes: int
    open_gap_count: int
    partial_gap_count: int
    unrecoverable_gap_count: int
    last_updated_at: datetime

    def to_dict(self) -> dict[str, str | int]:
        return {
            "exchange": self.exchange,
            "market_type": self.market_type,
            "symbol": self.symbol,
            "availability": self.availability.value,
            "completeness": self.completeness.value,
            "disk_status": self.disk_status.value,
            "disk_free_bytes": self.disk_free_bytes,
            "disk_free_gb": round(self.disk_free_bytes / (1024**3), 2),
            "open_gap_count": self.open_gap_count,
            "partial_gap_count": self.partial_gap_count,
            "unrecoverable_gap_count": self.unrecoverable_gap_count,
            "last_updated_at": self.last_updated_at.isoformat(),
        }


def compute_collector_health(
    *,
    exchange: str,
    market_type: str,
    symbol: str,
    root: Path,
    current_availability: AvailabilityStatus = AvailabilityStatus.HEALTHY,
    last_message_age_sec: float | None = None,
    liveness_threshold_sec: float = 60.0,
    warning_disk_gb: float = 100.0,
    critical_disk_gb: float = 50.0,
) -> CollectorHealthState:
    """Evaluates operational availability, completeness, stale feed liveness, and disk thresholds."""
    # Stale Feed Liveness Check
    if (
        current_availability == AvailabilityStatus.HEALTHY
        and last_message_age_sec is not None
        and last_message_age_sec > liveness_threshold_sec
    ):
        current_availability = AvailabilityStatus.DEGRADED
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

    # Disk Space Check
    total, used, free = shutil.disk_usage(root)
    free_gb = free / (1024**3)

    if free_gb < critical_disk_gb:
        disk_status = DiskThresholdStatus.CRITICAL_STOP
    elif free_gb < warning_disk_gb:
        disk_status = DiskThresholdStatus.WARNING
    else:
        disk_status = DiskThresholdStatus.OK

    return CollectorHealthState(
        exchange=exchange,
        market_type=market_type,
        symbol=symbol,
        availability=current_availability,
        completeness=completeness,
        disk_status=disk_status,
        disk_free_bytes=free,
        open_gap_count=open_count,
        partial_gap_count=partial_count,
        unrecoverable_gap_count=unrecoverable_count,
        last_updated_at=utc_now(),
    )
