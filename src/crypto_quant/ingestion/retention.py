"""Retention Policy, Hold Management, Audit Trail, and Deletion Ledger (Phase 1C Item 7E Final Audit).

Enforces retention policies with explicit hold management, gap/conflict evidence protection,
append-only hold event logs, append-only deletion ledgers, and semantic-age resolution:
- Semantic Age Resolution Priority:
  1. Manifest coverage_end / artifact semantic timestamp (age_basis='manifest_coverage_end')
  2. Explicit artifact metadata / filename timestamp (age_basis='artifact_metadata')
  3. Filesystem mtime fallback (age_basis='filesystem_mtime')
- Approved Durations:
  - Raw WS Envelopes (raw/ws/): 30 days retention
  - Normalized Realtime Trades (normalized/realtime/): 30 days retention
  - Normalized Historical Archives (normalized/individual_trade/v1/): Permanent
  - 1s and 5s Derived Buckets (derived/trade_bucket/.../granularity=1s|5s): 90 days retention
  - 1m Derived Buckets (derived/trade_bucket/.../granularity=60s): Permanent (NEVER deleted)
- Active Holds, Open/Partial Gaps, and Reconciliation Conflict Evidence: Fully protected from deletion.
- Append-Only Auditing:
  - Deletion Ledger: control/retention/v1/deletion_ledger.jsonl (with age_basis and semantic_age_timestamp)
  - Hold Events Audit: control/retention/v1/hold_events.jsonl
"""

from __future__ import annotations

import json
import logging
import os
import re
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
    age_basis: str = "filesystem_mtime"
    semantic_age_timestamp: str | None = None

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
                    data.setdefault("age_basis", "filesystem_mtime")
                    data.setdefault("semantic_age_timestamp", None)
                    recs.append(DeletionRecord(**data))
        return recs


@dataclass(frozen=True)
class RetentionPolicy:
    raw_ws_envelope_days: int = 30
    normalized_realtime_days: int = 30
    sub_minute_bucket_days: int = 90
    minute_bucket_days: int | None = None  # Permanent retention (NEVER deleted)


def _build_manifest_coverage_index(root: Path) -> dict[str, tuple[datetime, str]]:
    """Scans control/manifests/ and builds lookup: artifact relative path / filename -> (coverage_end, manifest_rel_path)."""
    index: dict[str, tuple[datetime, str]] = {}
    manifest_dir = root / "control" / "manifests"
    if not manifest_dir.exists():
        return index

    for mfile in manifest_dir.glob("*.jsonl"):
        rel_mfile = str(mfile.relative_to(root)).replace("\\", "/")
        try:
            with mfile.open("r", encoding="utf-8") as f:
                for line in f:
                    line = line.strip()
                    if not line:
                        continue
                    rec = json.loads(line)
                    cov_end_str = rec.get("coverage_end") or rec.get("timestamp") or rec.get("processed_at")
                    if not cov_end_str:
                        continue
                    try:
                        cov_dt = datetime.fromisoformat(cov_end_str)
                    except Exception:
                        continue

                    # Index by object_id / raw_object_ref / filename
                    for key_field in ("object_id", "raw_object_ref", "artifact_ref"):
                        val = rec.get(key_field)
                        if val:
                            norm_key = str(val).replace("\\", "/")
                            index[norm_key] = (cov_dt, rel_mfile)
                            index[Path(norm_key).name] = (cov_dt, rel_mfile)
        except Exception as exc:
            logger.warning(f"Error reading manifest {mfile}: {exc}")
    return index


def resolve_artifact_semantic_age(
    path: Path,
    root: Path,
    manifest_index: dict[str, tuple[datetime, str]] | None = None,
    now: datetime | None = None,
) -> tuple[datetime, str, str | None]:
    """Determines the semantic age timestamp of an artifact.

    Priority order:
    1. Manifest coverage_end / artifact semantic timestamp (age_basis='manifest_coverage_end')
    2. Explicit artifact metadata / filename timestamp (age_basis='artifact_metadata')
    3. Filesystem mtime fallback (age_basis='filesystem_mtime')
    """
    now = now or utc_now()
    try:
        norm_rel = str(path.relative_to(root)).replace("\\", "/")
    except ValueError:
        norm_rel = str(path).replace("\\", "/")
    fname = path.name

    # 1. Manifest Index lookup
    if manifest_index:
        if norm_rel in manifest_index:
            dt, mref = manifest_index[norm_rel]
            return dt, "manifest_coverage_end", mref
        if fname in manifest_index:
            dt, mref = manifest_index[fname]
            return dt, "manifest_coverage_end", mref

    # 2. Explicit metadata in path / filename (e.g. date=YYYY-MM-DD or YYYY-MM-DD)
    match = re.search(r"(\d{4}-\d{2}-\d{2})(?:T(\d{2})[_-](\d{2})[_-](\d{2}))?", fname)
    if not match:
        match = re.search(r"date=(\d{4}-\d{2}-\d{2})", norm_rel)
    if match:
        date_str = match.group(1)
        try:
            if match.lastindex and match.lastindex >= 4 and match.group(2):
                h, m, s = match.group(2), match.group(3), match.group(4)
                dt = datetime.fromisoformat(f"{date_str}T{h}:{m}:{s}+00:00")
            else:
                dt = datetime.fromisoformat(f"{date_str}T23:59:59+00:00")
            return dt, "artifact_metadata", None
        except Exception:
            pass

    # 3. Filesystem mtime fallback
    mtime_dt = datetime.fromtimestamp(path.stat().st_mtime, tz=now.tzinfo)
    return mtime_dt, "filesystem_mtime", None


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
    manifest_index = _build_manifest_coverage_index(root)

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
            semantic_dt, age_basis, manifest_ref = resolve_artifact_semantic_age(
                path, root, manifest_index, now=now
            )
            if semantic_dt < now - timedelta(days=max_days):
                pruned_counts[category] += 1
                if not dry_run:
                    file_hash = sha256_text(path.name)
                    file_size = path.stat().st_size if path.is_file() else 0
                    rec = DeletionRecord(
                        deletion_id=f"del_{uuid.uuid4().hex[:12]}",
                        artifact_ref=str(path.relative_to(root)).replace("\\", "/"),
                        artifact_hash=file_hash,
                        dataset_class=dataset_class,
                        coverage=f"{age_basis}_{semantic_dt.isoformat()}",
                        original_size_bytes=file_size,
                        retention_policy=f"{max_days}_days_max",
                        eligibility_reason=f"Exceeded {max_days} days retention threshold (basis: {age_basis}, semantic_age: {semantic_dt.isoformat()})",
                        deleted_at=utc_now(),
                        manifest_ref=manifest_ref,
                        age_basis=age_basis,
                        semantic_age_timestamp=semantic_dt.isoformat(),
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
