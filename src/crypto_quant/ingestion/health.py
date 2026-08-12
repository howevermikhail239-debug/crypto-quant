"""Collector Operational Health and Data Quality Guards (Phase 1C Item 7E).

Provides decoupled status tracking separating Availability vs Completeness
with three-tier disk thresholds matching config/default.yaml:
  warning: 80 GB
  bootstrap_stop: 50 GB
  critical_ingestion_stop: 20 GB

Availability: HEALTHY, DEGRADED, RECONNECTING, FAILED
Completeness: COMPLETE, RECOVERED, PARTIAL, GAPPED, UNKNOWN
Disk: OK, WARNING, BOOTSTRAP_STOP, CRITICAL_INGESTION_STOP

Liveness evaluation distinguishes transport-level liveness (socket state,
heartbeat/ping-pong) from market-data freshness (last trade age).
Transport liveness failures transition to DEGRADED/FAILED regardless of
trade activity.  Market data staleness alone is NOT treated as connection
failure — some instruments legitimately have long inter-trade gaps.
"""

from __future__ import annotations

import shutil
from dataclasses import dataclass
from datetime import datetime
from enum import StrEnum
from pathlib import Path

from ..time import utc_now
from .gap_registry import GapRegistry, GapStatus, GapType


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
    BOOTSTRAP_STOP = "BOOTSTRAP_STOP"
    CRITICAL_INGESTION_STOP = "CRITICAL_INGESTION_STOP"


class FreshnessStatus(StrEnum):
    ACTIVE = "ACTIVE"
    LOW_ACTIVITY_QUIET = "LOW_ACTIVITY_QUIET"
    STALE_BY_POLICY = "STALE_BY_POLICY"
    UNKNOWN = "UNKNOWN"


class FeedActivityMode(StrEnum):
    SCHEDULED = "SCHEDULED"
    EVENT_DRIVEN = "EVENT_DRIVEN"


class SideExpectation(StrEnum):
    REQUIRED = "REQUIRED"
    SOURCE_LIMITATION = "SOURCE_LIMITATION"
    NOT_PROVIDED = "NOT_PROVIDED"


class DQEligibilityStatus(StrEnum):
    USABLE = "USABLE"
    DEGRADED = "DEGRADED"
    UNAVAILABLE = "UNAVAILABLE"


@dataclass(frozen=True)
class FreshnessMeasurement:
    status: FreshnessStatus
    age_seconds: float | None
    policy_name: str
    stale_after_seconds: float | None
    transport_healthy: bool


@dataclass(frozen=True)
class RateMeasurement:
    applicable: bool
    observations: int
    issue_count: int
    rate: float | None
    rule: str


@dataclass(frozen=True)
class QueueWriterDQ:
    applicable: bool
    capacity: int | None
    current_size: int | None
    utilization: float | None
    high_watermark: int | None
    writer_lag_sec: float | None


@dataclass(frozen=True)
class GapExposureSummary:
    total: int
    by_status: dict[GapStatus, int]
    by_type: dict[GapType, int]


@dataclass(frozen=True)
class DQEligibilityDecision:
    status: DQEligibilityStatus
    reasons: tuple[str, ...]


def measure_freshness(
    *,
    last_event_time: datetime | None,
    observed_at: datetime,
    feed_mode: FeedActivityMode,
    policy_name: str,
    stale_after_seconds: float | None = None,
    transport_healthy: bool = True,
) -> FreshnessMeasurement:
    """Classify freshness only against an explicit source/feed policy.

    Quiet event-driven feeds are not declared stale merely because no market
    event arrived.  No threshold is inferred when policy is unknown.
    """
    if not policy_name.strip():
        raise ValueError("policy_name must be explicit")
    if stale_after_seconds is not None and stale_after_seconds < 0:
        raise ValueError("stale_after_seconds must be non-negative")
    if not transport_healthy:
        return FreshnessMeasurement(
            FreshnessStatus.UNKNOWN, None, policy_name, stale_after_seconds, False
        )
    if last_event_time is None:
        status = (
            FreshnessStatus.LOW_ACTIVITY_QUIET
            if feed_mode == FeedActivityMode.EVENT_DRIVEN
            else FreshnessStatus.UNKNOWN
        )
        return FreshnessMeasurement(status, None, policy_name, stale_after_seconds, True)
    age = (observed_at - last_event_time).total_seconds()
    if age < 0:
        raise ValueError("last_event_time cannot be after observed_at")
    if stale_after_seconds is None:
        status = FreshnessStatus.UNKNOWN
    elif age <= stale_after_seconds:
        status = FreshnessStatus.ACTIVE
    else:
        status = FreshnessStatus.STALE_BY_POLICY
    return FreshnessMeasurement(status, age, policy_name, stale_after_seconds, True)


def measure_duplicate_rate(
    *, observations: int, duplicate_count: int, identity_rule: str
) -> RateMeasurement:
    """Measure duplicates only under a proven, named source identity rule."""
    if not identity_rule.strip():
        raise ValueError("identity_rule must name proven source semantics")
    if observations < 0 or duplicate_count < 0 or duplicate_count > observations:
        raise ValueError("duplicate counts are inconsistent")
    rate = duplicate_count / observations if observations else 0.0
    return RateMeasurement(True, observations, duplicate_count, rate, identity_rule)


def measure_unknown_side_rate(
    *,
    observations: int,
    unknown_count: int,
    expectation: SideExpectation,
    field_name: str,
) -> RateMeasurement:
    """Avoid treating documented source-side absence as a quality defect."""
    if not field_name.strip():
        raise ValueError("field_name must be explicit")
    if observations < 0 or unknown_count < 0 or unknown_count > observations:
        raise ValueError("unknown-side counts are inconsistent")
    applicable = expectation == SideExpectation.REQUIRED
    rate = unknown_count / observations if applicable and observations else (0.0 if applicable else None)
    return RateMeasurement(
        applicable,
        observations,
        unknown_count,
        rate,
        f"{field_name}:{expectation.value}",
    )


def measure_queue_writer(metrics: object | None) -> QueueWriterDQ:
    """Expose accepted bounded-queue measurements without inventing thresholds."""
    if metrics is None:
        return QueueWriterDQ(False, None, None, None, None, None)
    return QueueWriterDQ(
        applicable=True,
        capacity=int(metrics.capacity),
        current_size=int(metrics.current_size),
        utilization=float(metrics.utilization),
        high_watermark=int(metrics.high_watermark),
        writer_lag_sec=float(metrics.writer_lag_sec),
    )


def summarize_gap_exposure(
    root: Path,
    *,
    exchange: str | None = None,
    market_type: str | None = None,
    instrument_id: str | None = None,
    dataset_class: str | None = None,
) -> GapExposureSummary:
    """Summarize the latest state of existing append-only gap records."""
    gaps = GapRegistry(root).list_gaps()
    if exchange is not None:
        gaps = [gap for gap in gaps if gap.exchange == exchange]
    if market_type is not None:
        gaps = [gap for gap in gaps if gap.market_type == market_type]
    if instrument_id is not None:
        gaps = [gap for gap in gaps if gap.instrument_id == instrument_id]
    if dataset_class is not None:
        gaps = [gap for gap in gaps if gap.dataset_class == dataset_class]
    return GapExposureSummary(
        total=len(gaps),
        by_status={status: sum(gap.status == status for gap in gaps) for status in GapStatus},
        by_type={gap_type: sum(gap.gap_type == gap_type for gap in gaps) for gap_type in GapType},
    )


def classify_dq_eligibility(
    *,
    hard_fail_reasons: tuple[str, ...] = (),
    degradation_reasons: tuple[str, ...] = (),
) -> DQEligibilityDecision:
    """Fail-closed interface for future consumers; thresholds stay external/versioned."""
    if hard_fail_reasons:
        return DQEligibilityDecision(DQEligibilityStatus.UNAVAILABLE, hard_fail_reasons)
    if degradation_reasons:
        return DQEligibilityDecision(DQEligibilityStatus.DEGRADED, degradation_reasons)
    return DQEligibilityDecision(DQEligibilityStatus.USABLE, ())


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

    def to_dict(self) -> dict[str, str | int | float]:
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
    # Transport-level liveness (socket/heartbeat, not market data freshness)
    transport_healthy: bool = True,
    # Configurable disk thresholds (matching config/default.yaml)
    warning_disk_gb: float = 80.0,
    bootstrap_stop_disk_gb: float = 50.0,
    critical_ingestion_stop_disk_gb: float = 20.0,
) -> CollectorHealthState:
    """Evaluates operational availability, completeness, and disk thresholds.

    Liveness semantics:
    - transport_healthy = False transitions HEALTHY → DEGRADED.
      This flag should reflect socket state / heartbeat / ping-pong,
      NOT absence of trade messages.
    - Absence of trades is a market-data freshness concern and does
      not by itself indicate a dead connection.
    """
    # Transport liveness check
    if current_availability == AvailabilityStatus.HEALTHY and not transport_healthy:
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

    # Disk space – three-tier thresholds per config/default.yaml
    _total, _used, free = shutil.disk_usage(root)
    free_gb = free / (1024**3)

    if free_gb < critical_ingestion_stop_disk_gb:
        disk_status = DiskThresholdStatus.CRITICAL_INGESTION_STOP
    elif free_gb < bootstrap_stop_disk_gb:
        disk_status = DiskThresholdStatus.BOOTSTRAP_STOP
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
