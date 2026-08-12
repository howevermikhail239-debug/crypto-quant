"""Bounded four-stream liquidation soak and source-local DQ control plane.

This module orchestrates the accepted Bybit Linear and Binance USD-M adapters.
It deliberately does not infer market-event completeness from a healthy socket,
and it never treats quiet event time as a gap.
"""

from __future__ import annotations

import argparse
import asyncio
import ctypes
import hashlib
import json
import os
import shutil
import time
import uuid
from collections.abc import Awaitable, Callable, Sequence
from dataclasses import asdict, dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Any

import pyarrow.parquet as pq

from ..time import utc_now
from .binance.funding import funding_identity as binance_identity
from .binance.liquidations import (
    CONTRACT_ID as BINANCE_CONTRACT_ID,
)
from .binance.liquidations import (
    DATASET_ID as BINANCE_DATASET_ID,
)
from .binance.liquidations import (
    collect_binance_liquidations_live,
)
from .bybit.funding import funding_identity as bybit_identity
from .bybit.liquidations import (
    CONTRACT_ID as BYBIT_CONTRACT_ID,
)
from .bybit.liquidations import (
    DATASET_ID as BYBIT_DATASET_ID,
)
from .bybit.liquidations import (
    collect_bybit_liquidations_live,
)
from .gap_registry import GapRecord, GapRegistry, GapStatus, GapType
from .reconnect import ReconnectConfig, compute_reconnect_delay

LOCAL_NO_GAP = "NO_DETECTED_LOCAL_GAP"
LOCAL_GAPPED = "GAPPED_UNRECOVERABLE"
LOCAL_UNKNOWN = "UNKNOWN"
NO_HISTORY = "NO_VERIFIED_PUBLIC_LIQUIDATION_BACKFILL"


@dataclass(frozen=True)
class LiquidationStreamSpec:
    exchange: str
    symbol: str
    instrument_id: str
    source_stream: str
    source_dataset_id: str
    source_contract_version: str
    source_claimed_completeness: str
    delivery_semantics: str
    source_inherent_limitation: str

    @property
    def key(self) -> str:
        return f"{self.exchange}:{self.symbol}"


def default_streams() -> tuple[LiquidationStreamSpec, ...]:
    specs: list[LiquidationStreamSpec] = []
    for symbol in ("BTCUSDT", "ETHUSDT"):
        specs.append(
            LiquidationStreamSpec(
                exchange="bybit",
                symbol=symbol,
                instrument_id=bybit_identity(symbol).instrument_id,
                source_stream=f"allLiquidation.{symbol}",
                source_dataset_id=BYBIT_DATASET_ID,
                source_contract_version=BYBIT_CONTRACT_ID,
                source_claimed_completeness="ALL_LIQUIDATIONS",
                delivery_semantics="BATCHED_500MS_PUSH",
                source_inherent_limitation=(
                    "NO_RELIABLE_SEQUENCE_ID; source claim is not locally provable"
                ),
            )
        )
        specs.append(
            LiquidationStreamSpec(
                exchange="binance",
                symbol=symbol,
                instrument_id=binance_identity(symbol).instrument_id,
                source_stream=f"{symbol.lower()}@forceOrder",
                source_dataset_id=BINANCE_DATASET_ID,
                source_contract_version=BINANCE_CONTRACT_ID,
                source_claimed_completeness="INCOMPLETE_THROTTLED_SNAPSHOT",
                delivery_semantics="MAX_ONE_SELECTED_PER_SYMBOL_PER_1000MS",
                source_inherent_limitation=(
                    "DOC_CONFLICT_LATEST_VS_LARGEST; NO_RELIABLE_SEQUENCE_ID"
                ),
            )
        )
    return tuple(specs)


@dataclass(frozen=True)
class SoakConfig:
    duration_seconds: float = 600.0
    flush_interval_seconds: float = 5.0
    min_disk_free_gb: float = 20.0
    reconnect: ReconnectConfig = field(
        default_factory=lambda: ReconnectConfig(max_attempts=3, max_delay_sec=10.0)
    )

    def __post_init__(self) -> None:
        if self.duration_seconds <= 0:
            raise ValueError("duration_seconds must be positive")
        if self.flush_interval_seconds <= 0:
            raise ValueError("flush_interval_seconds must be positive")
        if self.reconnect.max_attempts < 1:
            raise ValueError("reconnect.max_attempts must be at least one")


AttemptRunner = Callable[
    [LiquidationStreamSpec, Path, float, float, float, str, str],
    Awaitable[dict[str, Any]],
]


def _append_jsonl(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a", encoding="utf-8", newline="\n") as handle:
        handle.write(json.dumps(payload, sort_keys=True) + "\n")
        handle.flush()
        os.fsync(handle.fileno())


def _write_json_atomic(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    partial = path.with_suffix(path.suffix + ".partial")
    with partial.open("w", encoding="utf-8", newline="\n") as handle:
        json.dump(payload, handle, indent=2, sort_keys=True)
        handle.write("\n")
        handle.flush()
        os.fsync(handle.fileno())
    os.replace(partial, path)


def _sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _available_ram_bytes() -> int | None:
    if os.name != "nt":
        return None

    class MemoryStatus(ctypes.Structure):
        _fields_ = [
            ("length", ctypes.c_ulong),
            ("memory_load", ctypes.c_ulong),
            ("total_phys", ctypes.c_ulonglong),
            ("avail_phys", ctypes.c_ulonglong),
            ("total_page_file", ctypes.c_ulonglong),
            ("avail_page_file", ctypes.c_ulonglong),
            ("total_virtual", ctypes.c_ulonglong),
            ("avail_virtual", ctypes.c_ulonglong),
            ("avail_extended_virtual", ctypes.c_ulonglong),
        ]

    status = MemoryStatus()
    status.length = ctypes.sizeof(MemoryStatus)
    if not ctypes.windll.kernel32.GlobalMemoryStatusEx(ctypes.byref(status)):
        return None
    return int(status.avail_phys)


def _tree_size(root: Path) -> int:
    if not root.exists():
        return 0
    return sum(path.stat().st_size for path in root.rglob("*") if path.is_file())


def resource_gate(root: Path, min_disk_free_gb: float) -> dict[str, Any]:
    """Read-only resource snapshot before a bounded soak."""
    root.mkdir(parents=True, exist_ok=True)
    usage = shutil.disk_usage(root)
    liquidation_roots = (root / "raw", root / "normalized" / "liquidations")
    liquidation_bytes = sum(_tree_size(path) for path in liquidation_roots)
    free_gib = usage.free / (1024**3)
    return {
        "status": "PASS" if free_gib >= min_disk_free_gb else "FAIL",
        "free_disk_bytes": usage.free,
        "free_disk_gib": round(free_gib, 3),
        "liquidation_data_bytes": liquidation_bytes,
        "available_ram_bytes": _available_ram_bytes(),
        "logical_cpu_count": os.cpu_count(),
        "cpu_baseline": "NOT_MEASURED",
        "load_expectation": "TRIVIAL_TO_MODERATE_FOR_FOUR_LOW_RATE_STREAMS",
    }


async def _production_attempt(
    spec: LiquidationStreamSpec,
    root: Path,
    duration: float,
    flush_interval: float,
    min_disk_free_gb: float,
    run_id: str,
    session_id: str,
) -> dict[str, Any]:
    if spec.exchange == "bybit":
        return await collect_bybit_liquidations_live(
            spec.symbol,
            root,
            flush_interval_seconds=flush_interval,
            max_duration_seconds=duration,
            min_disk_free_gb=min_disk_free_gb,
            ingestion_run_id=run_id,
            session_id=session_id,
        )
    return await collect_binance_liquidations_live(
        root,
        symbol=spec.symbol,
        flush_interval_seconds=flush_interval,
        max_duration_seconds=duration,
        min_disk_free_gb=min_disk_free_gb,
        ingestion_run_id=run_id,
        session_id=session_id,
    )


def _classify_failure(error: BaseException) -> tuple[str, int, int]:
    message = str(error).upper()
    if "SYMBOL" in message or "TOPIC" in message:
        return "WRONG_SYMBOL", 1, 1
    if isinstance(error, (KeyError, json.JSONDecodeError)):
        return "UNKNOWN_SCHEMA", 1, 0
    if isinstance(error, ValueError):
        return "INVALID_SOURCE_MESSAGE", 1, 0
    if isinstance(error, OSError) or "CONNECT" in message or "WEBSOCKET" in message:
        return "NETWORK_FAILURE", 0, 0
    return "PROCESS_ERROR", 0, 0


def _unrecoverable_gap(
    registry: GapRegistry,
    spec: LiquidationStreamSpec,
    start: datetime,
    end: datetime,
    before: str,
    after: str | None,
    reason: str,
    run_id: str,
) -> GapRecord:
    record = registry.register_gap(
        exchange=spec.exchange,
        market_type="perpetual",
        instrument_id=spec.instrument_id,
        dataset_class="liquidations",
        source_stream=spec.source_stream,
        gap_start=start,
        gap_end=max(end, start),
        gap_type=GapType.LOCAL_COLLECTOR_GAP,
        session_before=before,
        session_after=after,
        evidence={
            "ingestion_run_id": run_id,
            "disconnect_reason": reason,
            "recovery_availability": NO_HISTORY,
        },
        notes="Observed local disconnect; no verified public liquidation history can fill it",
    )
    record.status = GapStatus.UNRECOVERABLE
    record.coverage_proven = False
    record.recovery_attempted = False
    record.recovery_source = NO_HISTORY
    registry.update_gap(record)
    return record


async def _run_stream(
    spec: LiquidationStreamSpec,
    root: Path,
    config: SoakConfig,
    run_id: str,
    attempt_runner: AttemptRunner,
) -> dict[str, Any]:
    sessions_path = (
        root / "control" / "ingestion_runs" / "liquidation_soak" / "v1" / "sessions.jsonl"
    )
    incidents_path = root / "control" / "dq" / "liquidations" / "v1" / "incidents.jsonl"
    registry = GapRegistry(root)
    deadline = time.monotonic() + config.duration_seconds
    totals: dict[str, int] = {
        "connection_attempts": 0,
        "successful_connects": 0,
        "subscription_acks": 0,
        "disconnects": 0,
        "reconnects": 0,
        "raw_message_count": 0,
        "source_event_count": 0,
        "persisted_row_count": 0,
        "parser_rejects": 0,
        "wrong_symbol_rejects": 0,
        "duplicate_exact_wire_deliveries": 0,
        "dropped_messages": 0,
    }
    gap_ids: list[str] = []
    session_ids: list[str] = []
    errors: list[dict[str, str]] = []
    first_event_time: str | None = None
    last_event_time: str | None = None
    wire_hashes_seen: set[str] = set()
    termination_reason = "BOUNDED_SOAK_COMPLETED"
    local_status = LOCAL_NO_GAP
    previous_failed_at: datetime | None = None
    previous_session: str | None = None

    for attempt in range(1, config.reconnect.max_attempts + 1):
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            break
        session_id = f"liq_session_{uuid.uuid4().hex}"
        session_ids.append(session_id)
        totals["connection_attempts"] += 1
        attempted_at = utc_now()
        if attempt > 1:
            totals["reconnects"] += 1
        try:
            outcome = await attempt_runner(
                spec,
                root,
                remaining,
                config.flush_interval_seconds,
                config.min_disk_free_gb,
                run_id,
                session_id,
            )
            ended_at = datetime.fromisoformat(outcome.get("ended_at", utc_now().isoformat()))
            totals["successful_connects"] += int(outcome.get("transport_status") == "PASS")
            totals["subscription_acks"] += int(outcome.get("subscription_status", "PASS") == "PASS")
            totals["raw_message_count"] += int(outcome.get("total_messages_received", 0))
            event_count = int(
                outcome.get(
                    "total_source_events_observed", outcome.get("total_records_persisted", 0)
                )
            )
            totals["source_event_count"] += event_count
            totals["persisted_row_count"] += int(outcome.get("total_records_persisted", 0))
            totals["dropped_messages"] += int(outcome.get("dropped_messages", 0))
            for wire_hash in outcome.get("wire_sha256_seen", []):
                if wire_hash in wire_hashes_seen:
                    totals["duplicate_exact_wire_deliveries"] += 1
                wire_hashes_seen.add(wire_hash)
            first_event_time = first_event_time or outcome.get("first_event_time")
            last_event_time = outcome.get("last_event_time") or last_event_time
            if previous_failed_at and previous_session:
                gap = _unrecoverable_gap(
                    registry,
                    spec,
                    previous_failed_at,
                    attempted_at,
                    previous_session,
                    session_id,
                    errors[-1]["reason_code"],
                    run_id,
                )
                gap_ids.append(gap.gap_id)
                local_status = LOCAL_GAPPED
                previous_failed_at = None
            if int(outcome.get("dropped_messages", 0)) > 0:
                drop_start = datetime.fromisoformat(
                    outcome.get("connected_at")
                    or outcome.get("started_at")
                    or attempted_at.isoformat()
                )
                gap = _unrecoverable_gap(
                    registry,
                    spec,
                    drop_start,
                    ended_at,
                    session_id,
                    None,
                    "CONFIRMED_LOCAL_DROP",
                    run_id,
                )
                gap.evidence = {
                    **(gap.evidence or {}),
                    "dropped_messages": int(outcome["dropped_messages"]),
                    "exact_drop_boundaries": "UNKNOWN",
                }
                registry.update_gap(gap)
                gap_ids.append(gap.gap_id)
                local_status = LOCAL_GAPPED
            session_record = {
                "record_type": "SESSION_COMPLETED",
                "ingestion_run_id": run_id,
                "session_id": session_id,
                "attempt": attempt,
                **asdict(spec),
                **outcome,
                "ended_at": ended_at.isoformat(),
            }
            _append_jsonl(sessions_path, session_record)
            termination_reason = outcome.get("termination_reason", "BOUNDED_SOAK_COMPLETED")
            break
        except asyncio.CancelledError:
            raise
        except Exception as error:  # isolated stream failure is recorded, then bounded retry
            failed_at = utc_now()
            reason_code, parser_rejects, wrong_symbol_rejects = _classify_failure(error)
            totals["disconnects"] += 1
            totals["parser_rejects"] += parser_rejects
            totals["wrong_symbol_rejects"] += wrong_symbol_rejects
            error_record = {
                "reason_code": reason_code,
                "error_type": type(error).__name__,
                "message": str(error),
            }
            errors.append(error_record)
            _append_jsonl(
                incidents_path,
                {
                    "record_type": "CAPTURE_INCIDENT",
                    "ingestion_run_id": run_id,
                    "session_id": session_id,
                    "exchange": spec.exchange,
                    "symbol": spec.symbol,
                    "instrument_id": spec.instrument_id,
                    "source_stream": spec.source_stream,
                    "occurred_at": failed_at.isoformat(),
                    **error_record,
                    "creates_interval_gap": reason_code not in {"WRONG_SYMBOL"},
                },
            )
            _append_jsonl(
                sessions_path,
                {
                    "record_type": "SESSION_FAILED",
                    "ingestion_run_id": run_id,
                    "session_id": session_id,
                    "attempt": attempt,
                    **asdict(spec),
                    "started_at": attempted_at.isoformat(),
                    "ended_at": failed_at.isoformat(),
                    "termination_reason": reason_code,
                    "error_type": type(error).__name__,
                },
            )
            if reason_code != "WRONG_SYMBOL":
                if previous_failed_at and previous_session:
                    gap = _unrecoverable_gap(
                        registry,
                        spec,
                        previous_failed_at,
                        attempted_at,
                        previous_session,
                        session_id,
                        errors[-2]["reason_code"],
                        run_id,
                    )
                    gap_ids.append(gap.gap_id)
                previous_failed_at = failed_at
                previous_session = session_id
                local_status = LOCAL_GAPPED
            if attempt >= config.reconnect.max_attempts:
                termination_reason = "RETRY_LIMIT_EXHAUSTED"
                break
            delay = min(
                compute_reconnect_delay(attempt, config.reconnect),
                max(0.0, deadline - time.monotonic()),
            )
            if delay:
                await asyncio.sleep(delay)

    if previous_failed_at and previous_session:
        gap = _unrecoverable_gap(
            registry,
            spec,
            previous_failed_at,
            utc_now(),
            previous_session,
            None,
            errors[-1]["reason_code"],
            run_id,
        )
        gap_ids.append(gap.gap_id)

    if totals["dropped_messages"] > 0:
        local_status = LOCAL_GAPPED
    return {
        **asdict(spec),
        **totals,
        "session_ids": session_ids,
        "gap_ids": gap_ids,
        "errors": errors,
        "first_event_time": first_event_time,
        "last_event_time": last_event_time,
        "event_observation_status": (
            "REAL_EVENT_OBSERVED"
            if totals["source_event_count"]
            else "NO_EVENT_OBSERVED_WITHIN_WINDOW"
        ),
        "transport_status": "PASS" if totals["successful_connects"] else "FAIL",
        "local_capture_status": local_status,
        "row_structural_status": "PASS" if not totals["parser_rejects"] else "DEGRADED",
        "silent_loss_detectability": "NOT_PROVABLE_WITHOUT_RELIABLE_SOURCE_SEQUENCE",
        "historical_recovery": NO_HISTORY,
        "termination_reason": termination_reason,
        "queue_mode": "NOT_APPLICABLE_SYNCHRONOUS_READ_FLUSH",
        "queue_high_water_mark": None,
        "queue_capacity": None,
    }


async def run_liquidation_soak(
    root: Path,
    *,
    config: SoakConfig | None = None,
    streams: Sequence[LiquidationStreamSpec] | None = None,
    attempt_runner: AttemptRunner = _production_attempt,
    run_id: str | None = None,
) -> dict[str, Any]:
    """Run isolated bounded collectors and durably publish one machine-readable run report."""
    config = config or SoakConfig()
    selected = tuple(streams or default_streams())
    if len({spec.key for spec in selected}) != len(selected):
        raise ValueError("Duplicate liquidation stream identity")
    gate = resource_gate(root, config.min_disk_free_gb)
    if gate["status"] != "PASS":
        raise OSError("Liquidation soak resource gate failed")
    run_id = run_id or f"liq_soak_{uuid.uuid4().hex}"
    started_at = utc_now()
    results = await asyncio.gather(
        *(_run_stream(spec, root, config, run_id, attempt_runner) for spec in selected)
    )
    ended_at = utc_now()
    report = {
        "record_type": "LIQUIDATION_SOAK_RUN",
        "ingestion_run_id": run_id,
        "started_at": started_at.isoformat(),
        "ended_at": ended_at.isoformat(),
        "configured_duration_seconds": config.duration_seconds,
        "flush_interval_seconds": config.flush_interval_seconds,
        "max_attempts_per_stream": config.reconnect.max_attempts,
        "resource_gate_before": gate,
        "resource_snapshot_after": resource_gate(root, config.min_disk_free_gb),
        "stream_results": {
            result["exchange"] + ":" + result["symbol"]: result for result in results
        },
        "status": (
            "PASS"
            if all(result["transport_status"] == "PASS" for result in results)
            else "PARTIAL_SOAK"
        ),
    }
    report_path = root / "control" / "ingestion_runs" / "liquidation_soak" / "v1" / f"{run_id}.json"
    _write_json_atomic(report_path, report)
    report["report_path"] = str(report_path)
    return report


def reconcile_liquidation_run(root: Path, run_id: str) -> dict[str, Any]:
    """Verify manifest object references and hashes for one instrumented soak run."""
    rows: list[dict[str, Any]] = []
    for name in ("bybit_linear_liquidations.jsonl", "binance_usdm_liquidations.jsonl"):
        path = root / "control" / "manifests" / name
        if not path.exists():
            continue
        for line in path.read_text(encoding="utf-8").splitlines():
            row = json.loads(line)
            if row.get("ingestion_run_id") == run_id:
                rows.append(row)
    broken_refs: list[str] = []
    raw_messages = 0
    events = 0
    for row in rows:
        raw_messages += int(row.get("raw_message_count", 0))
        events += int(row.get("event_count", row.get("observation_count", 0)))
        raw_ref = root / row["raw_object_ref"]
        if not raw_ref.exists() or _sha256_file(raw_ref) != row["raw_sha256"]:
            broken_refs.append(str(raw_ref))
        for ref, expected in zip(
            row.get("created_parquets", []), row.get("parquet_sha256", []), strict=True
        ):
            path = root / ref
            if not path.exists() or _sha256_file(path) != expected:
                broken_refs.append(str(path))
    return {
        "ingestion_run_id": run_id,
        "manifest_records": len(rows),
        "raw_message_count": raw_messages,
        "expected_canonical_observations": events,
        "hash_valid_records": len(rows) if not broken_refs else None,
        "broken_refs": broken_refs,
        "status": "PASS" if not broken_refs else "FAIL",
    }


def audit_liquidation_data_root(root: Path) -> dict[str, Any]:
    """Read back every small liquidation manifest and active stream namespace."""
    expected = {(spec.exchange, spec.symbol): spec for spec in default_streams()}
    manifest_rows: list[dict[str, Any]] = []
    for name in ("bybit_linear_liquidations.jsonl", "binance_usdm_liquidations.jsonl"):
        path = root / "control" / "manifests" / name
        if path.exists():
            manifest_rows.extend(
                json.loads(line) for line in path.read_text(encoding="utf-8").splitlines() if line
            )

    quarantine_rows = [row for row in manifest_rows if row.get("action") == "QUARANTINED"]
    quarantined_raw_names = {
        Path(row["quarantined_raw_object_ref"]).name
        for row in quarantine_rows
        if row.get("quarantined_raw_object_ref")
    }
    normalized_rows = [row for row in manifest_rows if row.get("action") == "NORMALIZED"]
    active_rows = [
        row
        for row in normalized_rows
        if Path(row.get("raw_object_ref", "")).name not in quarantined_raw_names
    ]
    broken_refs: list[str] = []
    valid_artifact_records = 0
    for row in normalized_rows:
        raw_ref = root / row["raw_object_ref"]
        parquet_pairs = list(
            zip(row.get("created_parquets", []), row.get("parquet_sha256", []), strict=True)
        )
        is_quarantined = raw_ref.name in quarantined_raw_names
        if is_quarantined:
            quarantine = next(
                item
                for item in quarantine_rows
                if Path(item.get("quarantined_raw_object_ref", "")).name == raw_ref.name
            )
            raw_ref = root / quarantine["quarantined_raw_object_ref"]
            relocated = {
                Path(path).name: root / path for path in quarantine["quarantined_parquets"]
            }
            parquet_pairs = [(relocated[Path(ref).name], digest) for ref, digest in parquet_pairs]
        else:
            parquet_pairs = [(root / ref, digest) for ref, digest in parquet_pairs]
        valid = raw_ref.exists() and _sha256_file(raw_ref) == row["raw_sha256"]
        if not valid:
            broken_refs.append(str(raw_ref))
        for parquet, digest in parquet_pairs:
            if not parquet.exists() or _sha256_file(parquet) != digest:
                valid = False
                broken_refs.append(str(parquet))
        valid_artifact_records += int(valid)

    stream_audit: dict[str, dict[str, Any]] = {}
    identity_mismatches: list[str] = []
    checkpoint_inconsistencies: list[str] = []
    for (exchange, symbol), spec in expected.items():
        raw_dir = root / "raw" / exchange / "perpetual" / "liquidations" / symbol
        normalized_dir = (
            root
            / "normalized"
            / "liquidations"
            / "v1"
            / f"exchange={exchange}"
            / "market_type=perpetual"
            / f"symbol={symbol}"
        )
        raw_objects = sorted(raw_dir.rglob("*.jsonl")) if raw_dir.exists() else []
        generations = sorted(normalized_dir.rglob("*.parquet")) if normalized_dir.exists() else []
        generation_rows = [pq.ParquetFile(path).metadata.num_rows for path in generations]
        for path in generations:
            table = pq.ParquetFile(path).read(columns=["instrument_id", "symbol", "source"])
            if set(table["instrument_id"].to_pylist()) != {spec.instrument_id}:
                identity_mismatches.append(str(path))
            if set(table["symbol"].to_pylist()) != {symbol}:
                identity_mismatches.append(str(path))
            if set(table["source"].to_pylist()) != {spec.source_dataset_id}:
                identity_mismatches.append(str(path))
        prefix = "bybit_linear" if exchange == "bybit" else "binance_usdm"
        checkpoint = root / "control" / "checkpoints" / f"{prefix}_liquidations_{symbol}.json"
        checkpoint_payload = (
            json.loads(checkpoint.read_text(encoding="utf-8")) if checkpoint.exists() else None
        )
        if checkpoint_payload:
            if checkpoint_payload.get("symbol") != symbol:
                checkpoint_inconsistencies.append(str(checkpoint))
            explicit_identity = checkpoint_payload.get("instrument_id")
            if explicit_identity is not None and explicit_identity != spec.instrument_id:
                checkpoint_inconsistencies.append(str(checkpoint))
        gaps = [
            gap
            for gap in GapRegistry(root).list_gaps()
            if gap.dataset_class == "liquidations"
            and gap.exchange == exchange
            and gap.instrument_id == spec.instrument_id
        ]
        stream_audit[spec.key] = {
            "raw_objects": len(raw_objects),
            "normalized_generations": len(generations),
            "active_rows": max(generation_rows, default=0),
            "checkpoint": str(checkpoint) if checkpoint.exists() else None,
            "checkpoint_identity": (
                checkpoint_payload.get("instrument_id", "LEGACY_IMPLICIT_FROM_SYMBOL")
                if checkpoint_payload
                else None
            ),
            "gaps": len(gaps),
        }

    active_synthetic = sum(
        int(
            row.get("source_dataset_id") not in {BYBIT_DATASET_ID, BINANCE_DATASET_ID}
            or "synthetic" in json.dumps(row).lower()
        )
        for row in active_rows
    )
    accepted_btc = {
        "raw_sha256": "ec277af4f4238c71fd347b258df64c7e4537a454c34d010841ab65f88f483ad8",
        "parquet_sha256": "5cdc49ae06f6437807b0ad4ba5aade722804b3a805b490130a8cff6a418b3cc7",
    }
    binance_btc = next(
        (
            row
            for row in active_rows
            if row.get("exchange") == "binance" and row.get("symbol") == "BTCUSDT"
        ),
        None,
    )
    accepted_hashes_unchanged = bool(
        binance_btc
        and binance_btc.get("raw_sha256") == accepted_btc["raw_sha256"]
        and binance_btc.get("parquet_sha256") == [accepted_btc["parquet_sha256"]]
    )
    return {
        "stream_audit": stream_audit,
        "manifest_records_total": len(manifest_rows),
        "artifact_manifest_records": len(normalized_rows),
        "artifact_manifest_records_hash_valid": valid_artifact_records,
        "quarantine_records": len(quarantine_rows),
        "active_manifest_records": len(active_rows),
        "broken_refs": broken_refs,
        "identity_mismatches": identity_mismatches,
        "checkpoint_inconsistencies": checkpoint_inconsistencies,
        "active_synthetic_observations": active_synthetic,
        "accepted_binance_btc_g1_hashes_unchanged": accepted_hashes_unchanged,
        "status": (
            "PASS"
            if not broken_refs
            and not identity_mismatches
            and not checkpoint_inconsistencies
            and not active_synthetic
            and accepted_hashes_unchanged
            else "FAIL"
        ),
    }


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Run the bounded four-stream liquidation soak")
    parser.add_argument("--data-root", type=Path, required=True)
    parser.add_argument("--duration", type=float, default=600.0)
    parser.add_argument("--flush-interval", type=float, default=5.0)
    parser.add_argument("--min-disk-free-gb", type=float, default=20.0)
    parser.add_argument("--max-attempts", type=int, default=3)
    return parser


def main(argv: list[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    config = SoakConfig(
        duration_seconds=args.duration,
        flush_interval_seconds=args.flush_interval,
        min_disk_free_gb=args.min_disk_free_gb,
        reconnect=ReconnectConfig(max_attempts=args.max_attempts, max_delay_sec=10.0),
    )
    report = asyncio.run(run_liquidation_soak(args.data_root, config=config))
    print(json.dumps(report, indent=2, sort_keys=True))
    return 0 if report["status"] == "PASS" else 1


if __name__ == "__main__":
    raise SystemExit(main())
