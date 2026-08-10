"""Retention Policy Enforcement Module (Phase 1C Item 7E).

Enforces retention policies across datasets:
- Raw WS Envelopes (raw/ws/): 30 days retention
- Normalized Realtime Trades (normalized/realtime/): 30 days retention
- 1s and 5s Derived Buckets (derived/trade_bucket/.../granularity=1s|5s): 90 days retention
- 1m Derived Buckets (derived/trade_bucket/.../granularity=60s): Permanent (no retention deletion)
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from datetime import datetime, timedelta
from pathlib import Path

from ..time import utc_now

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class RetentionPolicy:
    raw_ws_envelope_days: int = 30
    normalized_realtime_days: int = 30
    sub_minute_bucket_days: int = 90
    minute_bucket_days: int | None = None  # None = Permanent retention


def enforce_retention_policy(
    root: Path,
    policy: RetentionPolicy | None = None,
    *,
    dry_run: bool = True,
) -> dict[str, int]:
    policy = policy or RetentionPolicy()
    """Scans dataset directories and removes files older than retention cutoff thresholds.

    Returns dict mapping dataset category to count of pruned files.
    """
    now = utc_now()
    pruned_counts = {
        "raw_ws": 0,
        "normalized_realtime": 0,
        "sub_minute_buckets": 0,
        "minute_buckets": 0,
    }

    # 1. Raw WS Envelopes (30 days)
    raw_ws_dir = root / "raw" / "ws"
    if raw_ws_dir.exists():
        cutoff = now - timedelta(days=policy.raw_ws_envelope_days)
        for path in raw_ws_dir.rglob("*.jsonl"):
            try:
                mtime = datetime.fromtimestamp(path.stat().st_mtime, tz=now.tzinfo)
                if mtime < cutoff:
                    pruned_counts["raw_ws"] += 1
                    if not dry_run:
                        path.unlink()
            except Exception as exc:
                logger.warning(f"Error checking mtime for {path}: {exc}")

    # 2. Normalized Realtime (30 days)
    norm_realtime_dir = root / "normalized" / "realtime"
    if norm_realtime_dir.exists():
        cutoff = now - timedelta(days=policy.normalized_realtime_days)
        for path in norm_realtime_dir.rglob("*.parquet"):
            try:
                mtime = datetime.fromtimestamp(path.stat().st_mtime, tz=now.tzinfo)
                if mtime < cutoff:
                    pruned_counts["normalized_realtime"] += 1
                    if not dry_run:
                        path.unlink()
            except Exception as exc:
                logger.warning(f"Error checking mtime for {path}: {exc}")

    # 3. Sub-minute Buckets (90 days)
    derived_dir = root / "derived" / "trade_bucket"
    if derived_dir.exists():
        cutoff = now - timedelta(days=policy.sub_minute_bucket_days)
        for path in derived_dir.rglob("*.parquet"):
            if "granularity=1s" in str(path) or "granularity=5s" in str(path):
                try:
                    mtime = datetime.fromtimestamp(path.stat().st_mtime, tz=now.tzinfo)
                    if mtime < cutoff:
                        pruned_counts["sub_minute_buckets"] += 1
                        if not dry_run:
                            path.unlink()
                except Exception as exc:
                    logger.warning(f"Error checking mtime for {path}: {exc}")

    return pruned_counts
