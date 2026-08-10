"""Auditable Gap Registry and Gap Taxonomy (Phase 1C Item 7C).

Manages auditable gap records for missing data intervals caused by collector disconnects,
restarts, or source gaps.
"""

from __future__ import annotations

import json
import os
import uuid
from dataclasses import asdict, dataclass
from datetime import datetime
from enum import StrEnum
from pathlib import Path
from typing import Any

from ..time import utc_now


class GapType(StrEnum):
    LOCAL_COLLECTOR_GAP = "LOCAL_COLLECTOR_GAP"
    SOURCE_GAP = "SOURCE_GAP"
    SEQUENCE_GAP = "SEQUENCE_GAP"
    ARCHIVE_GAP = "ARCHIVE_GAP"
    RECOVERED_GAP = "RECOVERED_GAP"
    PARTIALLY_RECOVERED_GAP = "PARTIALLY_RECOVERED_GAP"
    UNKNOWN_GAP = "UNKNOWN_GAP"


class GapStatus(StrEnum):
    OPEN = "OPEN"
    RECOVERED = "RECOVERED"
    PARTIAL = "PARTIAL"
    UNRECOVERABLE = "UNRECOVERABLE"
    UNKNOWN = "UNKNOWN"


@dataclass
class GapRecord:
    gap_id: str
    exchange: str
    market_type: str
    instrument_id: str
    dataset_class: str
    source_stream: str
    detected_at: datetime
    gap_start: datetime
    gap_end: datetime
    gap_type: GapType
    status: GapStatus
    session_before: str | None = None
    session_after: str | None = None
    recovery_attempted: bool = False
    recovery_source: str | None = None
    recovery_started_at: datetime | None = None
    recovery_completed_at: datetime | None = None
    records_recovered: int = 0
    evidence: dict[str, Any] | None = None
    notes: str | None = None

    def to_dict(self) -> dict[str, Any]:
        res = asdict(self)
        res["detected_at"] = self.detected_at.isoformat()
        res["gap_start"] = self.gap_start.isoformat()
        res["gap_end"] = self.gap_end.isoformat()
        res["gap_type"] = self.gap_type.value
        res["status"] = self.status.value
        if self.recovery_started_at:
            res["recovery_started_at"] = self.recovery_started_at.isoformat()
        if self.recovery_completed_at:
            res["recovery_completed_at"] = self.recovery_completed_at.isoformat()
        return res

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> GapRecord:
        data_copy = dict(data)
        data_copy["detected_at"] = datetime.fromisoformat(data_copy["detected_at"])
        data_copy["gap_start"] = datetime.fromisoformat(data_copy["gap_start"])
        data_copy["gap_end"] = datetime.fromisoformat(data_copy["gap_end"])
        data_copy["gap_type"] = GapType(data_copy["gap_type"])
        data_copy["status"] = GapStatus(data_copy["status"])
        if data_copy.get("recovery_started_at"):
            data_copy["recovery_started_at"] = datetime.fromisoformat(data_copy["recovery_started_at"])
        if data_copy.get("recovery_completed_at"):
            data_copy["recovery_completed_at"] = datetime.fromisoformat(data_copy["recovery_completed_at"])
        return cls(**data_copy)


class GapRegistry:
    """Persistent, append-only JSONL Gap Registry under control/gap_registry/v1/."""

    def __init__(self, root: Path) -> None:
        self.root = root
        self.dir_path = root / "control" / "gap_registry" / "v1"
        self.manifest_file = self.dir_path / "gap_manifest.jsonl"
        self.dir_path.mkdir(parents=True, exist_ok=True)

    def register_gap(
        self,
        *,
        exchange: str,
        market_type: str,
        instrument_id: str,
        dataset_class: str,
        source_stream: str,
        gap_start: datetime,
        gap_end: datetime,
        gap_type: GapType = GapType.LOCAL_COLLECTOR_GAP,
        session_before: str | None = None,
        session_after: str | None = None,
        evidence: dict[str, Any] | None = None,
        notes: str | None = None,
    ) -> GapRecord:
        gap_id = f"gap_{uuid.uuid4().hex[:16]}"
        now = utc_now()
        rec = GapRecord(
            gap_id=gap_id,
            exchange=exchange,
            market_type=market_type,
            instrument_id=instrument_id,
            dataset_class=dataset_class,
            source_stream=source_stream,
            detected_at=now,
            gap_start=gap_start,
            gap_end=gap_end,
            gap_type=gap_type,
            status=GapStatus.OPEN,
            session_before=session_before,
            session_after=session_after,
            evidence=evidence,
            notes=notes,
        )
        self._append_record(rec)
        return rec

    def _append_record(self, record: GapRecord) -> None:
        line = json.dumps(record.to_dict(), sort_keys=True) + "\n"
        with self.manifest_file.open("a", encoding="utf-8") as f:
            f.write(line)
            f.flush()
            os.fsync(f.fileno())

    def update_gap(self, record: GapRecord) -> None:
        """Appends updated state of an existing gap to maintaining audit history."""
        self._append_record(record)

    def list_gaps(self, status_filter: GapStatus | None = None) -> list[GapRecord]:
        if not self.manifest_file.exists():
            return []
        records_by_id: dict[str, GapRecord] = {}
        with self.manifest_file.open("r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                rec = GapRecord.from_dict(json.loads(line))
                records_by_id[rec.gap_id] = rec

        result = list(records_by_id.values())
        if status_filter:
            result = [r for r in result if r.status == status_filter]
        return result
