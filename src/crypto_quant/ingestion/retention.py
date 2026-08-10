"""Retention Policy, Hold Management, Audit Trail, and Deletion Ledger (Phase 1C Item 7E Final Audit).

Enforces retention policies with explicit hold management, gap/conflict evidence protection,
append-only hold event logs, and append-only deletion ledgers:
- Raw WS Envelopes (raw/ws/): 30 days retention
- Normalized Realtime Trades (normalized/realtime/): 30 days retention
- Normalized Historical Archives (normalized/individual_trade/v1/): Permanent
- 1s and 5s Derived Buckets (derived/trade_bucket/.../granularity=1s|5s): 90 days retention
- 1m Derived Buckets (derived/trade_bucket/.../granularity=60s): Permanent (NEVER deleted)
- Active Holds, Open/Partial Gaps, and Reconciliation Conflict Evidence: Fully protected from deletion.
- Append-Only Auditing:
  - Deletion Ledger: control/retention/v1/deletion_ledger.jsonl
  - Hold Events Audit: control/retention/v1/hold_events.jsonl
"""

from __future__ import annotations

import json
import logging
import os
import uuid
from dataclasses import asdict, dataclass
from datetime import datetime, timedelta
from enum import StrEnum
from pathlib import Path
from typing import Any

from ..hashing import sha256_text
from ..time import utc_now
from .gap_registry import GapRegistry, GapStatus
from .reconciliation import ReconciliationRegistry

logger = logging.getLogger(__name__)


class HoldType(StrEnum):
    INCIDENT_HOLD = "INCIDENT_HOLD"
    GAP_INVESTIGATION_HOLD = "GAP_INVESTIGATION_HOLD"
    SCHEMA_DRIFT_HOLD = "SCHEMA_DRIFT_HOLD"
    RECONCILIATION_CONFLICT_HOLD = "RECONCILIATION_CONFLICT_HOLD"
    MANUAL_HOLD = "MANUAL_HOLD"


@dataclass
class RetentionHold:
    hold_id: str
    hold_type: HoldType
    target_ref: str
    reason: str
    created_at: datetime
    active: bool = True


@dataclass
class HoldEvent:
    event_id: str
    action: str  # CREATED, REMOVED
    hold_id: str
    hold_type: str
    target_ref: str
    reason: str
    timestamp: datetime

    def to_dict(self) -> dict[str, Any]:
        res = asdict(self)
        res["timestamp"] = self.timestamp.isoformat()
        return res


class HoldRegistry:
    """Manages active retention holds and append-only audit event trail in control/retention/v1/."""

    def __init__(self, root: Path) -> None:
        self.root = root
        self.dir_path = root / "control" / "retention" / "v1"
        self.holds_file = self.dir_path / "retention_holds.json"
        self.hold_events_file = self.dir_path / "hold_events.jsonl"
        self.dir_path.mkdir(parents=True, exist_ok=True)

    def add_hold(self, hold_type: HoldType, target_ref: str, reason: str) -> RetentionHold:
        holds = self.list_holds()
        hold_id = f"hold_{uuid.uuid4().hex[:12]}"
        now = utc_now()

        hold = RetentionHold(
            hold_id=hold_id,
            hold_type=hold_type,
            target_ref=target_ref,
            reason=reason,
            created_at=now,
            active=True,
        )
        holds.append(hold)
        self._save_holds(holds)

        # Audit Event Log
        evt = HoldEvent(
            event_id=f"evt_{uuid.uuid4().hex[:12]}",
            action="CREATED",
            hold_id=hold_id,
            hold_type=hold_type.value,
            target_ref=target_ref,
            reason=reason,
            timestamp=now,
        )
        self._record_hold_event(evt)
        return hold

    def remove_hold(self, hold_id: str, reason: str) -> bool:
        holds = self.list_holds()
        target_hold = next((h for h in holds if h.hold_id == hold_id and h.active), None)
        if target_hold is None:
            return False

        target_hold.active = False
        self._save_holds(holds)

        now = utc_now()
        evt = HoldEvent(
            event_id=f"evt_{uuid.uuid4().hex[:12]}",
            action="REMOVED",
            hold_id=hold_id,
            hold_type=target_hold.hold_type.value,
            target_ref=target_hold.target_ref,
            reason=reason,
            timestamp=now,
        )
        self._record_hold_event(evt)
        return True

    def is_held(self, target_ref: str) -> bool:
        return any(h.active and h.target_ref in target_ref for h in self.list_holds())

    def list_holds(self) -> list[RetentionHold]:
        if not self.holds_file.exists():
            return []
        try:
            data = json.loads(self.holds_file.read_text(encoding="utf-8"))
            res = []
            for item in data:
                item_copy = dict(item)
                item_copy["hold_type"] = HoldType(item_copy["hold_type"])
                item_copy["created_at"] = datetime.fromisoformat(item_copy["created_at"])
                res.append(RetentionHold(**item_copy))
            return res
        except Exception as exc:
            logger.warning(f"Error reading holds file: {exc}")
            return []

    def _save_holds(self, holds: list[RetentionHold]) -> None:
        serialized = []
        for h in holds:
            d = asdict(h)
            d["hold_type"] = h.hold_type.value
            d["created_at"] = h.created_at.isoformat()
            serialized.append(d)
        self.holds_file.write_text(json.dumps(serialized, indent=2), encoding="utf-8")

    def _record_hold_event(self, event: HoldEvent) -> None:
        line = json.dumps(event.to_dict(), sort_keys=True) + "\n"
        with self.hold_events_file.open("a", encoding="utf-8") as f:
            f.write(line)
            f.flush()
            os.fsync(f.fileno())


@dataclass
class DeletionRecord:
    deletion_id: str
    artifact_ref: str
    artifact_hash: str
    dataset_class: str
    coverage: str
    original_size_bytes: int
    retention_policy: str
    eligibility_reason: str
    deleted_at: datetime
    manifest_ref: str | None = None

    def to_dict(self) -> dict[str, Any]:
        res = asdict(self)
        res["deleted_at"] = self.deleted_at.isoformat()
        return res


class DeletionLedger:
    """Persistent, append-only JSONL audit log under control/retention/v1/deletion_ledger.jsonl."""

    def __init__(self, root: Path) -> None:
        self.root = root
        self.dir_path = root / "control" / "retention" / "v1"
        self.ledger_file = self.dir_path / "deletion_ledger.jsonl"
        self.dir_path.mkdir(parents=True, exist_ok=True)

    def record_deletion(self, record: DeletionRecord) -> None:
        line = json.dumps(record.to_dict(), sort_keys=True) + "\n"
        with self.ledger_file.open("a", encoding="utf-8") as f:
            f.write(line)
            f.flush()
            os.fsync(f.fileno())

    def list_deletions(self) -> list[DeletionRecord]:
        if not self.ledger_file.exists():
            return []
        recs = []
        with self.ledger_file.open("r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if line:
                    data = json.loads(line)
                    data["deleted_at"] = datetime.fromisoformat(data["deleted_at"])
                    recs.append(DeletionRecord(**data))
        return recs


@dataclass(frozen=True)
class RetentionPolicy:
    raw_ws_envelope_days: int = 30
    normalized_realtime_days: int = 30
    sub_minute_bucket_days: int = 90
    minute_bucket_days: int | None = None  # Permanent retention (NEVER deleted)


def enforce_retention_policy(
    root: Path,
    policy: RetentionPolicy | None = None,
    *,
    dry_run: bool = True,
) -> dict[str, int]:
    """Scans dataset directories, enforces policy, respects holds, and records deletion ledger."""
    policy = policy or RetentionPolicy()
    now = utc_now()
    hold_reg = HoldRegistry(root)
    ledger = DeletionLedger(root)
    gap_reg = GapRegistry(root)
    rec_reg = ReconciliationRegistry(root)

    # 1. Protect unresolved gap artifacts (OPEN, PARTIAL, UNKNOWN)
    unresolved_gaps = gap_reg.list_gaps()
    unresolved_gap_ids = {
        g.gap_id for g in unresolved_gaps if g.status in (GapStatus.OPEN, GapStatus.PARTIAL, GapStatus.UNKNOWN)
    }

    # 2. Protect unresolved reconciliation conflict evidence
    unresolved_reconciliations = rec_reg.list_reconciliations()
    conflict_trade_ids = set()
    for r in unresolved_reconciliations:
        if not r.coverage_proven or r.status != "MATCH":
            for d in r.discrepancy_details:
                if d.get("trade_id"):
                    conflict_trade_ids.add(str(d["trade_id"]))

    pruned_counts = {
        "raw_ws": 0,
        "normalized_realtime": 0,
        "sub_minute_buckets": 0,
        "minute_buckets": 0,
    }

    def _evaluate_file(path: Path, max_days: int, category: str, dataset_class: str) -> None:
        # Check active holds, open/partial gaps, and conflict evidence
        if hold_reg.is_held(path.name):
            logger.info(f"Preserving held artifact: {path.name}")
            return

        if any(gid in path.name for gid in unresolved_gap_ids):
            logger.info(f"Preserving gap evidence artifact: {path.name}")
            return

        if any(tid in path.name for tid in conflict_trade_ids):
            logger.info(f"Preserving reconciliation conflict evidence artifact: {path.name}")
            return

        try:
            mtime = datetime.fromtimestamp(path.stat().st_mtime, tz=now.tzinfo)
            if mtime < now - timedelta(days=max_days):
                pruned_counts[category] += 1
                if not dry_run:
                    file_hash = sha256_text(path.name)
                    file_size = path.stat().st_size if path.is_file() else 0
                    rec = DeletionRecord(
                        deletion_id=f"del_{uuid.uuid4().hex[:12]}",
                        artifact_ref=str(path.relative_to(root)),
                        artifact_hash=file_hash,
                        dataset_class=dataset_class,
                        coverage=f"mtime_{mtime.isoformat()}",
                        original_size_bytes=file_size,
                        retention_policy=f"{max_days}_days_max",
                        eligibility_reason=f"Exceeded {max_days} days retention threshold",
                        deleted_at=utc_now(),
                    )
                    path.unlink()
                    ledger.record_deletion(rec)
        except Exception as exc:
            logger.warning(f"Error checking artifact {path}: {exc}")

    # 1. Raw WS Envelopes (30 days)
    raw_ws_dir = root / "raw" / "ws"
    if raw_ws_dir.exists():
        for path in raw_ws_dir.rglob("*.jsonl"):
            _evaluate_file(path, policy.raw_ws_envelope_days, "raw_ws", "raw_ws_envelope")

    # 2. Normalized Realtime (30 days)
    norm_realtime_dir = root / "normalized" / "realtime"
    if norm_realtime_dir.exists():
        for path in norm_realtime_dir.rglob("*.parquet"):
            _evaluate_file(path, policy.normalized_realtime_days, "normalized_realtime", "individual_trade")

    # 3. Sub-minute Buckets (90 days)
    derived_dir = root / "derived" / "trade_bucket"
    if derived_dir.exists():
        for path in derived_dir.rglob("*.parquet"):
            if "granularity=1s" in str(path) or "granularity=5s" in str(path):
                _evaluate_file(path, policy.sub_minute_bucket_days, "sub_minute_buckets", "derived_trade_bucket")
            elif "granularity=60s" in str(path):
                # 1m Derived Buckets are Permanent (NEVER DELETED)
                pass

    return pruned_counts
