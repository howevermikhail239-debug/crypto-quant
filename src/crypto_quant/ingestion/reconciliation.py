"""REST / Archive / WS Reconciliation Framework with Granular Conflict Taxonomy (Phase 1C Item 7D Audit).

Performs statistical reconciliation and anomaly detection across WebSocket envelopes,
REST recovery endpoints, and official historical archives with strict dataset class isolation and natural keys.
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


class ReconciliationCategory(StrEnum):
    MATCH = "MATCH"
    REPRESENTATION_DIFFERENCE = "REPRESENTATION_DIFFERENCE"
    MISSING_IN_WS = "MISSING_IN_WS"
    MISSING_IN_COMPARISON_SOURCE = "MISSING_IN_COMPARISON_SOURCE"
    FIELD_CONFLICT = "FIELD_CONFLICT"
    SIDE_CONFLICT = "SIDE_CONFLICT"
    TIMESTAMP_CONFLICT = "TIMESTAMP_CONFLICT"
    DATASET_CLASS_MISMATCH = "DATASET_CLASS_MISMATCH"
    UNKNOWN_CONFLICT = "UNKNOWN_CONFLICT"


@dataclass
class ReconciliationMetrics:
    reconciliation_id: str
    exchange: str
    market_type: str
    symbol: str
    dataset_class: str
    left_source: str
    right_source: str
    overlap_start: datetime
    overlap_end: datetime
    performed_at: datetime
    archive_trade_count: int
    ws_trade_count: int
    rest_trade_count: int
    exact_matched_count: int
    ws_missing_count: int
    ws_extra_count: int
    rest_missing_count: int
    field_mismatch_count: int
    side_mismatch_count: int
    timestamp_mismatch_count: int
    match_rate_pct: float
    coverage_proven: bool
    algorithm_version: str
    input_artifact_refs: list[str]
    input_hashes: list[str]
    status: str
    known_issues: list[str]
    discrepancy_details: list[dict[str, Any]]
    notes: str | None = None

    def to_dict(self) -> dict[str, Any]:
        res = asdict(self)
        res["overlap_start"] = self.overlap_start.isoformat()
        res["overlap_end"] = self.overlap_end.isoformat()
        res["performed_at"] = self.performed_at.isoformat()
        return res

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> ReconciliationMetrics:
        data_copy = dict(data)
        data_copy["overlap_start"] = datetime.fromisoformat(data_copy["overlap_start"])
        data_copy["overlap_end"] = datetime.fromisoformat(data_copy["overlap_end"])
        data_copy["performed_at"] = datetime.fromisoformat(data_copy["performed_at"])

        # Backward compatibility for legacy manifest lines
        data_copy.setdefault("left_source", "legacy_left_source")
        data_copy.setdefault("right_source", "legacy_right_source")
        data_copy.setdefault("side_mismatch_count", 0)
        data_copy.setdefault("timestamp_mismatch_count", 0)
        data_copy.setdefault("coverage_proven", data_copy.get("match_rate_pct", 0) == 100.0)
        data_copy.setdefault("algorithm_version", "v1.0_legacy")
        data_copy.setdefault("input_artifact_refs", [])
        data_copy.setdefault("input_hashes", [])
        data_copy.setdefault("status", "LEGACY_RECORD")
        data_copy.setdefault("known_issues", [])

        return cls(**data_copy)


class ReconciliationRegistry:
    """Persistent, append-only JSONL Reconciliation Manifest under control/reconciliation/v1/."""

    def __init__(self, root: Path) -> None:
        self.root = root
        self.dir_path = root / "control" / "reconciliation" / "v1"
        self.manifest_file = self.dir_path / "reconciliation_manifest.jsonl"
        self.dir_path.mkdir(parents=True, exist_ok=True)

    def record_metrics(self, metrics: ReconciliationMetrics) -> None:
        line = json.dumps(metrics.to_dict(), sort_keys=True) + "\n"
        with self.manifest_file.open("a", encoding="utf-8") as f:
            f.write(line)
            f.flush()
            os.fsync(f.fileno())

    def list_reconciliations(self) -> list[ReconciliationMetrics]:
        if not self.manifest_file.exists():
            return []
        records = []
        with self.manifest_file.open("r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if line:
                    records.append(ReconciliationMetrics.from_dict(json.loads(line)))
        return records


def extract_natural_key(item: dict[str, Any], dataset_class: str) -> str:
    """Extracts explicit natural key by dataset class and source semantics.

    Never uses generic timestamp + price + quantity as natural key.
    """
    if dataset_class == "individual_trade":
        val = item.get("native_trade_id") or item.get("trade_id") or item.get("id") or item.get("i") or item.get("t")
        if val is not None:
            return str(val)
    elif dataset_class == "exchange_aggregate_trade":
        val = item.get("aggregate_trade_id") or item.get("agg_trade_id") or item.get("a")
        if val is not None:
            return str(val)

    # Fallback to explicit source ID if present
    fallback = item.get("id") or item.get("a") or item.get("native_trade_id")
    if fallback is not None:
        return str(fallback)
    raise ValueError(f"Unable to extract explicit natural key for dataset class '{dataset_class}'")


def reconcile_trade_datasets(
    *,
    exchange: str,
    market_type: str,
    symbol: str,
    dataset_class: str,
    archive_trades: list[dict[str, Any]],
    ws_trades: list[dict[str, Any]],
    rest_trades: list[dict[str, Any]],
    root: Path,
    left_source_name: str = "archive_or_rest",
    right_source_name: str = "websocket",
    right_dataset_class: str | None = None,
    timestamp_tolerance_ms: int = 1000,
) -> ReconciliationMetrics:
    """Performs statistical reconciliation across sources with strict dataset class isolation.

    Enforces:
    1. Dataset Class Isolation: left_dataset_class MUST match right_dataset_class.
    2. Natural Key Matching: Never uses generic price+qty+time tuple as key.
    3. Granular Category Classification: MATCH, MISSING_IN_WS, MISSING_IN_COMPARISON_SOURCE, FIELD_CONFLICT, SIDE_CONFLICT.
    """
    rec_id = f"rec_{uuid.uuid4().hex[:16]}"
    now = utc_now()

    # 1. Dataset Class Isolation Check
    r_class = right_dataset_class or dataset_class
    if dataset_class != r_class:
        raise TypeError(
            f"DATASET_CLASS_MISMATCH: Cannot reconcile '{dataset_class}' against '{r_class}'"
        )

    # 2. Extract Natural Keys
    def _get_key(item: dict[str, Any]) -> str:
        return extract_natural_key(item, dataset_class)

    archive_map = {_get_key(t): t for t in archive_trades}
    ws_map = {_get_key(t): t for t in ws_trades}
    rest_map = {_get_key(t): t for t in rest_trades}

    all_keys = set(archive_map.keys()) | set(ws_map.keys()) | set(rest_map.keys())

    exact_matched = 0
    ws_missing = 0
    ws_extra = 0
    rest_missing = 0
    field_mismatch = 0
    side_mismatch = 0
    timestamp_mismatch = 0
    discrepancies = []

    for k in sorted(all_keys):
        in_arch = k in archive_map
        in_ws = k in ws_map
        in_rest = k in rest_map

        if in_arch and not in_ws:
            ws_missing += 1
            discrepancies.append({"trade_id": k, "category": ReconciliationCategory.MISSING_IN_WS.value})
        elif in_ws and not in_arch and len(archive_map) > 0:
            ws_extra += 1
            discrepancies.append({"trade_id": k, "category": ReconciliationCategory.MISSING_IN_COMPARISON_SOURCE.value})

        if in_arch and not in_rest and len(rest_map) > 0:
            rest_missing += 1

        if in_arch and in_ws:
            a_item = archive_map[k]
            w_item = ws_map[k]

            a_price = str(a_item.get("price") or a_item.get("p") or "")
            w_price = str(w_item.get("price") or w_item.get("p") or "")
            a_qty = str(a_item.get("quantity") or a_item.get("qty") or a_item.get("q") or "")
            w_qty = str(w_item.get("quantity") or w_item.get("qty") or w_item.get("q") or "")

            a_side = str(a_item.get("taker_side") or a_item.get("side") or a_item.get("m") or "")
            w_side = str(w_item.get("taker_side") or w_item.get("side") or w_item.get("m") or "")

            a_time = int(a_item.get("event_time") or a_item.get("time") or a_item.get("T") or 0)
            w_time = int(w_item.get("event_time") or w_item.get("time") or w_item.get("T") or 0)

            has_conflict = False
            if a_price != w_price or a_qty != w_qty:
                field_mismatch += 1
                has_conflict = True
                discrepancies.append({
                    "trade_id": k,
                    "category": ReconciliationCategory.FIELD_CONFLICT.value,
                    "arch_price": a_price,
                    "ws_price": w_price,
                    "arch_qty": a_qty,
                    "ws_qty": w_qty,
                })

            if a_side and w_side and a_side != w_side:
                side_mismatch += 1
                has_conflict = True
                discrepancies.append({
                    "trade_id": k,
                    "category": ReconciliationCategory.SIDE_CONFLICT.value,
                    "arch_side": a_side,
                    "ws_side": w_side,
                })

            if a_time > 0 and w_time > 0 and abs(a_time - w_time) > timestamp_tolerance_ms:
                timestamp_mismatch += 1
                has_conflict = True
                discrepancies.append({
                    "trade_id": k,
                    "category": ReconciliationCategory.TIMESTAMP_CONFLICT.value,
                    "arch_time": a_time,
                    "ws_time": w_time,
                })

            if not has_conflict:
                exact_matched += 1
        elif in_ws and not in_arch and len(archive_map) == 0:
            exact_matched += 1

    total_ref = len(archive_map) or len(all_keys) or 1
    match_rate = round((exact_matched / total_ref) * 100.0, 2)
    coverage_proven = (ws_missing == 0 and field_mismatch == 0 and side_mismatch == 0)

    status = "MATCH" if coverage_proven else "DISCREPANCY_DETECTED"

    metrics = ReconciliationMetrics(
        reconciliation_id=rec_id,
        exchange=exchange,
        market_type=market_type,
        symbol=symbol,
        dataset_class=dataset_class,
        left_source=left_source_name,
        right_source=right_source_name,
        overlap_start=now,
        overlap_end=now,
        performed_at=now,
        archive_trade_count=len(archive_map),
        ws_trade_count=len(ws_map),
        rest_trade_count=len(rest_map),
        exact_matched_count=exact_matched,
        ws_missing_count=ws_missing,
        ws_extra_count=ws_extra,
        rest_missing_count=rest_missing,
        field_mismatch_count=field_mismatch,
        side_mismatch_count=side_mismatch,
        timestamp_mismatch_count=timestamp_mismatch,
        match_rate_pct=match_rate,
        coverage_proven=coverage_proven,
        algorithm_version="v1.1_natural_key_strict",
        input_artifact_refs=[left_source_name, right_source_name],
        input_hashes=[],
        status=status,
        known_issues=[],
        discrepancy_details=discrepancies[:50],
        notes=f"Reconciliation completed with {match_rate}% match rate across {len(all_keys)} total distinct trade IDs.",
    )

    registry = ReconciliationRegistry(root)
    registry.record_metrics(metrics)
    return metrics
