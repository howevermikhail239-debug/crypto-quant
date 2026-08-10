"""Exchange-neutral, immutable OHLCV v2 storage primitives.

V1 Binance files remain readable and immutable.  V2 intentionally makes
source-specific fields nullable so a venue cannot pretend it supplied fields
that its documented endpoint does not publish.
"""

from __future__ import annotations

import hashlib
import json
import os
import tempfile
import time
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import pyarrow as pa
import pyarrow.parquet as pq

DEC = pa.decimal128(38, 18)
ARROW_SCHEMA_V2 = pa.schema(
    [
        pa.field("instrument_id", pa.string(), False),
        pa.field("exchange", pa.string(), False),
        pa.field("market_type", pa.string(), False),
        pa.field("contract_type", pa.string(), False),
        pa.field("native_symbol", pa.string(), False),
        pa.field("interval", pa.string(), False),
        pa.field("is_closed", pa.bool_(), False),
        pa.field("open_time", pa.timestamp("ms", tz="UTC"), False),
        pa.field("close_time", pa.timestamp("ms", tz="UTC"), False),
        *[pa.field(x, DEC, False) for x in ("open", "high", "low", "close")],
        pa.field("base_volume", DEC, True),
        pa.field("quote_volume", DEC, True),
        pa.field("source_volume", DEC, False),
        pa.field("source_volume_unit", pa.string(), False),
        pa.field("source_turnover", DEC, True),
        pa.field("source_turnover_unit", pa.string(), True),
        pa.field("trade_count", pa.int64(), True),
        pa.field("taker_buy_base_volume", DEC, True),
        pa.field("taker_buy_quote_volume", DEC, True),
        pa.field("source_method", pa.string(), False),
        pa.field("source_dataset_id", pa.string(), False),
        pa.field("source_uri", pa.string(), False),
        pa.field("observation_id", pa.string(), False),
        pa.field("raw_object_ref", pa.string(), False),
        pa.field("source_object_sha256", pa.string(), False),
        pa.field("retrieved_at", pa.timestamp("ms", tz="UTC"), False),
        pa.field("processed_at", pa.timestamp("ms", tz="UTC"), False),
        pa.field("knowledge_time", pa.timestamp("ms", tz="UTC"), True),
        pa.field("knowledge_time_basis", pa.string(), False),
        pa.field("schema_version", pa.string(), False),
        pa.field("collector_version", pa.string(), False),
        pa.field("normalization_version", pa.string(), False),
        pa.field("data_contract_version", pa.string(), False),
        pa.field("candle_source", pa.string(), True),
        pa.field("aggregation_version", pa.string(), True),
        pa.field("source_revision_id", pa.string(), True),
        pa.field("exchange_timestamp", pa.timestamp("ms", tz="UTC"), True),
        pa.field("source_published_at", pa.timestamp("ms", tz="UTC"), True),
        pa.field("received_at", pa.timestamp("ms", tz="UTC"), True),
        pa.field("clock_offset_ms", pa.int64(), True),
        pa.field("clock_uncertainty_ms", pa.int64(), True),
        pa.field("ingestion_run_id", pa.string(), True),
        pa.field("dq_flags", pa.list_(pa.string()), True),
    ]
)


@dataclass(frozen=True)
class OhlcvSourceDescriptor:
    exchange: str
    market_type: str
    contract_type: str
    dataset_id: str

    def namespace(self) -> str:
        return f"{self.exchange}_{self.market_type}_{self.contract_type}_{self.dataset_id.replace('.', '_')}"


def sha256(value: bytes) -> str:
    return hashlib.sha256(value).hexdigest()


def atomic_bytes(path: Path, value: bytes) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.NamedTemporaryFile("wb", dir=path.parent, delete=False, suffix=".partial") as f:
        f.write(value)
        f.flush()
        os.fsync(f.fileno())
        temporary = Path(f.name)
    os.replace(temporary, path)


def atomic_json(path: Path, value: dict[str, Any]) -> None:
    atomic_bytes(path, json.dumps(value, sort_keys=True, default=str).encode())


def validate_descriptor(row: dict[str, Any], descriptor: OhlcvSourceDescriptor) -> None:
    if (row["exchange"], row["market_type"], row["contract_type"]) != (
        descriptor.exchange,
        descriptor.market_type,
        descriptor.contract_type,
    ):
        raise ValueError("row identity does not match typed source descriptor")
    if row["source_dataset_id"] != descriptor.dataset_id:
        raise ValueError("dataset id does not match typed source descriptor")


def validate_identity(identity: Any, descriptor: OhlcvSourceDescriptor) -> None:
    if (identity.exchange, identity.market_type, identity.contract_type) != (
        descriptor.exchange,
        descriptor.market_type,
        descriptor.contract_type,
    ):
        raise ValueError("instrument identity does not match typed source descriptor")


def record_unknown_gaps(root: Path, descriptor: OhlcvSourceDescriptor, rows: list[dict[str, Any]]) -> None:
    """Persist absence as UNKNOWN_GAP; source absence alone is not exchange proof."""
    for before, after in zip(rows, rows[1:], strict=False):
        expected = before["open_time"].timestamp() * 1000 + 60_000
        actual = after["open_time"].timestamp() * 1000
        if actual > expected:
            path = root / "control" / "gap_registry" / f"gap_{before['instrument_id']}_{int(expected)}.json"
            if not path.exists():
                atomic_json(path, {"kind":"UNKNOWN_GAP","source_dataset_id":descriptor.dataset_id,"instrument_id":before["instrument_id"],"started_at":datetime.fromtimestamp(expected / 1000, tz=UTC).isoformat(),"ended_at":after["open_time"].isoformat(),"reason":"missing_in_completed_source_object","data_quality_flags":["MISSING_PERIOD"]})


def recover_stale_partials(root: Path, *, stale_after_seconds: int = 600, now: float | None = None) -> list[Path]:
    """Only abandoned partials are quarantined; a current writer is untouched."""
    now = time.time() if now is None else now
    moved: list[Path] = []
    quarantine = root / "quarantine" / "stale_partials"
    for partial in root.rglob("*.partial"):
        if quarantine in partial.parents or now - partial.stat().st_mtime < stale_after_seconds:
            continue
        quarantine.mkdir(parents=True, exist_ok=True)
        target = quarantine / f"{partial.name}.{sha256(str(partial).encode())[:12]}"
        os.replace(partial, target)
        moved.append(target)
    return moved


def commit_rows(
    *,
    root: Path,
    descriptor: OhlcvSourceDescriptor,
    identity: Any,
    rows: list[dict[str, Any]],
    raw_bytes: bytes,
    source_uri: str,
    request: dict[str, Any],
    generation: str,
) -> Path:
    if not rows:
        raise ValueError("cannot commit empty OHLCV generation")
    validate_identity(identity, descriptor)
    for row in rows:
        validate_descriptor(row, descriptor)
    times = [row["open_time"] for row in rows]
    if times != sorted(times) or len(set(times)) != len(times):
        raise ValueError("OHLCV rows must be ascending and unique")
    record_unknown_gaps(root, descriptor, rows)
    raw_hash = sha256(raw_bytes)
    raw = (
        root
        / "raw"
        / descriptor.exchange
        / descriptor.market_type
        / "ohlcv_1m_rest"
        / identity.native_symbol
        / f"{raw_hash}.json"
    )
    if not raw.exists():
        atomic_bytes(raw, raw_bytes)
    for row in rows:
        row["raw_object_ref"] = str(raw.relative_to(root))
        row["source_object_sha256"] = raw_hash
    # Short components keep atomic temporary paths below legacy Windows MAX_PATH.
    output = (
        root
        / "normalized"
        / "ohlcv"
        / "v2"
        / descriptor.exchange
        / descriptor.market_type
        / identity.instrument_id
        / "1m"
        / generation
        / f"part-{raw_hash[:16]}.parquet"
    )
    if not output.exists():
        output.parent.mkdir(parents=True, exist_ok=True)
        partial = output.parent / f"{output.name}.partial"
        pq.write_table(
            pa.Table.from_pylist(rows, schema=ARROW_SCHEMA_V2),
            partial,
            compression="zstd",
            use_dictionary=True,
        )
        with partial.open("r+b") as handle:
            os.fsync(handle.fileno())
        if pq.ParquetFile(partial).metadata.num_rows != len(rows):
            raise ValueError("Parquet validation failed")
        os.replace(partial, output)
    event = {
        "object_id": str(output.relative_to(root)),
        "parquet_sha256": sha256(output.read_bytes()),
        "parquet_bytes": output.stat().st_size,
        "raw_sha256": raw_hash,
        "raw_bytes": len(raw_bytes),
        "raw_object_ref": str(raw.relative_to(root)),
        "source_uri": source_uri,
        "source_kind": "rest_bootstrap",
        "source_dataset_id": descriptor.dataset_id,
        "exchange": descriptor.exchange,
        "market_type": descriptor.market_type,
        "contract_type": descriptor.contract_type,
        "instrument_id": identity.instrument_id,
        "native_symbol": identity.native_symbol,
        "coverage_start": rows[0]["open_time"].isoformat(),
        "coverage_end": rows[-1]["close_time"].isoformat(),
        "row_count": len(rows),
        "retrieved_at": rows[0]["retrieved_at"].isoformat(),
        "processed_at": rows[0]["processed_at"].isoformat(),
        "request": request,
        "schema_version": rows[0]["schema_version"],
        "collector_version": "0.2.0",
        "normalization_version": "2.0.0",
        "source_contract_version": rows[0]["data_contract_version"],
    }
    manifest = root / "control" / "manifests" / f"{descriptor.namespace()}_ohlcv.jsonl"
    manifest.parent.mkdir(parents=True, exist_ok=True)
    existing = manifest.read_text(encoding="utf-8") if manifest.exists() else ""
    if event["parquet_sha256"] not in existing:
        with manifest.open("a", encoding="utf-8") as f:
            f.write(json.dumps(event, sort_keys=True, default=str) + "\n")
            f.flush()
            os.fsync(f.fileno())
    atomic_json(
        root
        / "control"
        / "checkpoints"
        / f"{descriptor.namespace()}_{identity.native_symbol.lower()}_1m.json",
        {
            "source_dataset_id": descriptor.dataset_id,
            "instrument_id": identity.instrument_id,
            "cursor": generation,
            "last_event_time": rows[-1]["close_time"].isoformat(),
            "last_knowledge_time": None,
            "committed_at": rows[0]["processed_at"].isoformat(),
        },
    )
    return output
