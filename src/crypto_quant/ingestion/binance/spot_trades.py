"""Immutable Binance Spot individual-trade archive vertical slice.

This module deliberately implements *only* Binance Spot ``trades`` archives.
It cannot read or substitute ``aggTrades``: their exchange aggregation semantics
are a different physical dataset and must receive a separate contract/adapter.
"""

from __future__ import annotations

import csv
import hashlib
import json
import os
import tempfile
import time
import zipfile
from collections.abc import Iterator
from dataclasses import dataclass
from datetime import date, datetime
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Any, Literal

import httpx
import pyarrow as pa
import pyarrow.parquet as pq

from ...identity import InstrumentIdentity
from ...paths import disk_free_bytes
from ...time import parse_epoch, utc_now

DATASET_ID = "binance.spot.individual_trade.archive"
CONTRACT_ID = "binance.spot.archive.individual-trade.v1"
ARCHIVE_BASE = "https://data.binance.vision/data/spot/daily/trades"
EXPECTED_COLUMNS = ("id", "price", "qty", "quoteQty", "time", "isBuyerMaker", "isBestMatch")
DEC = pa.decimal128(38, 18)

INDIVIDUAL_TRADE_SCHEMA = pa.schema(
    [
        pa.field("instrument_id", pa.string(), False),
        pa.field("exchange", pa.string(), False),
        pa.field("market_type", pa.string(), False),
        pa.field("contract_type", pa.string(), False),
        pa.field("native_symbol", pa.string(), False),
        pa.field("dataset_class", pa.string(), False),
        pa.field("source_dataset_id", pa.string(), False),
        pa.field("native_trade_id", pa.string(), False),
        pa.field("source_ordinal", pa.int64(), False),
        pa.field("event_time", pa.timestamp("us", tz="UTC"), False),
        pa.field("exchange_timestamp", pa.timestamp("us", tz="UTC"), False),
        pa.field("source_timestamp", pa.int64(), False),
        pa.field("source_timestamp_unit", pa.string(), False),
        pa.field("price", DEC, False),
        pa.field("quantity", DEC, False),
        pa.field("quantity_unit", pa.string(), False),
        pa.field("quote_quantity", DEC, False),
        pa.field("notional_unit", pa.string(), False),
        pa.field("taker_side", pa.string(), False),
        pa.field("signed_quantity", DEC, False),
        pa.field("is_buyer_maker", pa.bool_(), False),
        pa.field("is_best_match", pa.bool_(), False),
        pa.field("is_block_trade", pa.bool_(), True),
        pa.field("is_rpi_trade", pa.bool_(), True),
        pa.field("received_at", pa.timestamp("us", tz="UTC"), False),
        pa.field("processed_at", pa.timestamp("us", tz="UTC"), False),
        pa.field("knowledge_time", pa.timestamp("us", tz="UTC"), True),
        pa.field("knowledge_time_basis", pa.string(), False),
        pa.field("source_uri", pa.string(), False),
        pa.field("raw_object_ref", pa.string(), False),
        pa.field("source_object_sha256", pa.string(), False),
        pa.field("schema_version", pa.string(), False),
        pa.field("collector_version", pa.string(), False),
        pa.field("normalization_version", pa.string(), False),
        pa.field("data_contract_version", pa.string(), False),
        pa.field("classification_version", pa.string(), False),
        pa.field("dq_flags", pa.list_(pa.string()), True),
    ]
)


@dataclass(frozen=True)
class PilotMeasurement:
    trading_date: str
    rows: int
    archive_bytes: int
    extracted_bytes: int
    parquet_bytes: int
    elapsed_seconds: float
    archive_sha256: str
    disk_free_before_bytes: int
    disk_free_after_bytes: int
    resource_gate_passed: bool
    peak_rss_bytes: int | None


def btcusdt_spot_identity() -> InstrumentIdentity:
    return binance_spot_identity("BTCUSDT")


def binance_spot_identity(symbol: str) -> InstrumentIdentity:
    if symbol not in {"BTCUSDT", "ETHUSDT"}:
        raise ValueError("PHASE 1C permits only BTCUSDT and ETHUSDT Binance Spot")
    base = symbol.removesuffix("USDT")
    return InstrumentIdentity(
        exchange="binance",
        native_symbol=symbol,
        market_type="spot",
        contract_type="spot",
        base_asset=base,
        quote_asset="USDT",
        quantity_unit=base,
        notional_unit="USDT",
    )


def archive_url(symbol: str, trading_date: date) -> str:
    return f"{ARCHIVE_BASE}/{symbol}/{symbol}-trades-{trading_date.isoformat()}.zip"


def _sha256(value: bytes) -> str:
    return hashlib.sha256(value).hexdigest()


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as source:
        for chunk in iter(lambda: source.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _expected_checksum(value: str) -> str:
    token = value.strip().split()[0].lower()
    if len(token) != 64 or any(c not in "0123456789abcdef" for c in token):
        raise ValueError("invalid Binance CHECKSUM object")
    return token


def fetch_archive(
    symbol: str, trading_date: date, client: httpx.Client | None = None
) -> tuple[bytes, str]:
    """Fetch exactly one official daily archive and verify its sidecar SHA-256."""
    owned = client is None
    client = client or httpx.Client(timeout=120, follow_redirects=True)
    try:
        url = archive_url(symbol, trading_date)
        payload = client.get(url)
        payload.raise_for_status()
        checksum = client.get(f"{url}.CHECKSUM")
        checksum.raise_for_status()
        expected = _expected_checksum(checksum.text)
        if _sha256(payload.content) != expected:
            raise ValueError("Binance archive checksum mismatch")
        return payload.content, expected
    finally:
        if owned:
            client.close()


def fetch_archive_to_path(symbol: str, trading_date: date, destination: Path) -> str:
    """Stream one ZIP to an atomic file and verify Binance's checksum sidecar."""
    destination.parent.mkdir(parents=True, exist_ok=True)
    url = archive_url(symbol, trading_date)
    with httpx.Client(timeout=120, follow_redirects=True) as client:
        checksum_response = client.get(f"{url}.CHECKSUM")
        checksum_response.raise_for_status()
        expected = _expected_checksum(checksum_response.text)
        partial = destination.with_suffix(destination.suffix + ".partial")
        digest = hashlib.sha256()
        with client.stream("GET", url) as response, partial.open("wb") as target:
            response.raise_for_status()
            for chunk in response.iter_bytes(1024 * 1024):
                digest.update(chunk)
                target.write(chunk)
            target.flush()
            os.fsync(target.fileno())
    if digest.hexdigest() != expected:
        partial.unlink(missing_ok=True)
        raise ValueError("Binance archive checksum mismatch")
    os.replace(partial, destination)
    return expected


def source_timestamp_unit(trading_date: date) -> Literal["ms", "us"]:
    # Binance public-data README documents Spot archives as microseconds from 2025-01-01.
    return "us" if trading_date >= date(2025, 1, 1) else "ms"


def source_contract_id(trading_date: date) -> str:
    return (
        "binance.spot.archive.individual-trade.us.v1"
        if trading_date >= date(2025, 1, 1)
        else "binance.spot.archive.individual-trade.ms.v1"
    )


def iter_archive_rows(
    archive_path: Path, *, expected_symbol: str | None = None, expected_date: date | None = None
) -> tuple[Iterator[dict[str, str]], int]:
    """Return a bounded-memory CSV iterator and the authoritative ZIP member size."""
    try:
        archive = zipfile.ZipFile(archive_path)
    except zipfile.BadZipFile as error:
        raise ValueError("invalid trade archive") from error
    names = [name for name in archive.namelist() if name.endswith(".csv")]
    if len(names) != 1:
        archive.close()
        raise ValueError("archive must contain exactly one CSV")
    info = archive.getinfo(names[0])
    expected_name = (
        f"{expected_symbol}-trades-{expected_date.isoformat()}.csv"
        if expected_symbol and expected_date
        else None
    )
    if "-trades-" not in Path(info.filename).name or (
        expected_name and Path(info.filename).name != expected_name
    ):
        archive.close()
        raise ValueError("archive member symbol/date naming contract mismatch")

    def rows() -> Iterator[dict[str, str]]:
        try:
            with archive.open(info) as source:
                reader = csv.reader(
                    __import__("io").TextIOWrapper(source, encoding="utf-8", newline="")
                )
                first = next(reader, None)
                if first is None:
                    raise ValueError("empty individual trade archive")
                if tuple(first) != EXPECTED_COLUMNS:
                    if len(first) != len(EXPECTED_COLUMNS):
                        raise ValueError(
                            f"unexpected individual trade archive columns; expected {EXPECTED_COLUMNS}"
                        )
                    yield dict(zip(EXPECTED_COLUMNS, first, strict=True))
                for value in reader:
                    if len(value) != len(EXPECTED_COLUMNS):
                        raise ValueError("malformed individual trade archive row")
                    yield dict(zip(EXPECTED_COLUMNS, value, strict=True))
        finally:
            archive.close()

    return rows(), info.file_size


def archive_rows(archive_path: Path) -> list[dict[str, str]]:
    rows, _ = iter_archive_rows(archive_path)
    return list(rows)


def peak_rss_bytes() -> int | None:
    """Best-effort resident-set measurement without a third-party dependency."""
    if os.name != "nt":
        return None
    import ctypes
    from ctypes import wintypes

    class Counters(ctypes.Structure):
        _fields_ = [
            ("cb", wintypes.DWORD),
            ("PageFaultCount", wintypes.DWORD),
            ("PeakWorkingSetSize", ctypes.c_size_t),
            ("WorkingSetSize", ctypes.c_size_t),
        ]

    counters = Counters(ctypes.sizeof(Counters))
    process = ctypes.windll.kernel32.GetCurrentProcess()
    if not ctypes.windll.psapi.GetProcessMemoryInfo(process, ctypes.byref(counters), counters.cb):
        return None
    return int(counters.PeakWorkingSetSize)


def _decimal(value: str, field: str) -> Decimal:
    try:
        result = Decimal(value)
    except (InvalidOperation, ValueError) as error:
        raise ValueError(f"invalid decimal {field}: {value!r}") from error
    if not result.is_finite() or result <= 0:
        raise ValueError(f"{field} must be positive and finite")
    return result


def _bool(value: str, field: str) -> bool:
    if value == "True":
        return True
    if value == "False":
        return False
    raise ValueError(f"invalid boolean {field}: {value!r}")


def normalize_trade(
    row: dict[str, str],
    *,
    source_ordinal: int,
    identity: InstrumentIdentity,
    trading_date: date,
    source_uri: str,
    raw_object_ref: str,
    source_sha256: str,
    retrieved_at: datetime,
) -> dict[str, Any]:
    """Map a seven-column official individual-trade row without aggregate fallback."""
    if tuple(row) != EXPECTED_COLUMNS:
        raise ValueError("individual trade source contract mismatch")
    unit = source_timestamp_unit(trading_date)
    timestamp = int(row["time"])
    buyer_maker = _bool(row["isBuyerMaker"], "isBuyerMaker")
    quantity = _decimal(row["qty"], "qty")
    quote_quantity = _decimal(row["quoteQty"], "quoteQty")
    price = _decimal(row["price"], "price")
    event_time = parse_epoch(timestamp, unit=unit)
    if event_time.date() != trading_date:
        raise ValueError("trade event is outside requested UTC trading date")
    taker_side = "SELL" if buyer_maker else "BUY"
    return {
        "instrument_id": identity.instrument_id,
        "exchange": "binance",
        "market_type": "spot",
        "contract_type": "spot",
        "native_symbol": identity.native_symbol,
        "dataset_class": "individual_trade",
        "source_dataset_id": DATASET_ID,
        "native_trade_id": str(int(row["id"])),
        "source_ordinal": source_ordinal,
        "event_time": event_time,
        "exchange_timestamp": event_time,
        "source_timestamp": timestamp,
        "source_timestamp_unit": unit,
        "price": price,
        "quantity": quantity,
        "quantity_unit": identity.quantity_unit,
        "quote_quantity": quote_quantity,
        "notional_unit": identity.notional_unit,
        "taker_side": taker_side,
        "signed_quantity": quantity if taker_side == "BUY" else -quantity,
        "is_buyer_maker": buyer_maker,
        "is_best_match": _bool(row["isBestMatch"], "isBestMatch"),
        "is_block_trade": None,
        "is_rpi_trade": None,
        "received_at": retrieved_at,
        "processed_at": utc_now(),
        "knowledge_time": None,
        "knowledge_time_basis": "unknown_historical_retrieval_only",
        "source_uri": source_uri,
        "raw_object_ref": raw_object_ref,
        "source_object_sha256": source_sha256,
        "schema_version": "1.0.0",
        "collector_version": "0.3.0",
        "normalization_version": "1.0.0",
        "data_contract_version": source_contract_id(trading_date),
        "classification_version": "binance.isBuyerMaker.v1",
        "dq_flags": [],
    }


def estimated_required_bytes(
    *, archive_bytes: int, extracted_bytes: int = 0, parquet_bytes: int = 0
) -> int:
    """Measured-component estimator; no opaque archive multiplier."""
    return archive_bytes + extracted_bytes + parquet_bytes + max(64 * 1024**2, parquet_bytes)


def resource_gate(
    root: Path,
    *,
    estimated_download_bytes: int,
    extracted_bytes: int = 0,
    parquet_bytes: int = 0,
    min_free_gib: int = 50,
) -> bool:
    """Require 50 GiB after explicit measured components and atomic headroom."""
    root.mkdir(parents=True, exist_ok=True)
    reserve = estimated_required_bytes(
        archive_bytes=estimated_download_bytes,
        extracted_bytes=extracted_bytes,
        parquet_bytes=parquet_bytes,
    )
    return disk_free_bytes(root) - reserve >= min_free_gib * 1024**3


def _atomic_bytes(path: Path, value: bytes) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.NamedTemporaryFile(
        "wb", dir=path.parent, delete=False, suffix=".partial"
    ) as handle:
        handle.write(value)
        handle.flush()
        os.fsync(handle.fileno())
        temporary = Path(handle.name)
    os.replace(temporary, path)


def _pid_alive(pid: int) -> bool:
    if pid == os.getpid():
        return True
    try:
        os.kill(pid, 0)
    except OSError:
        return False
    return True


def acquire_writer_lease(root: Path, *, lease_id: str, stale_after_seconds: int = 600) -> Path:
    """Fail closed if another writer owns this dataset; lease is created atomically."""
    path = root / "control" / "leases" / "binance_spot_individual_trade.lock"
    path.parent.mkdir(parents=True, exist_ok=True)
    if path.exists():
        try:
            owner = json.loads(path.read_text(encoding="utf-8"))
            heartbeat = datetime.fromisoformat(owner["heartbeat"])
            stale = (utc_now() - heartbeat).total_seconds() >= stale_after_seconds
            alive = _pid_alive(int(owner["pid"]))
        except (ValueError, KeyError, json.JSONDecodeError):
            stale, alive = True, False
        if alive and not stale:
            raise RuntimeError("individual trade writer lease already held by active owner")
        quarantine = root / "quarantine" / "stale_trade_writer"
        quarantine.mkdir(parents=True, exist_ok=True)
        os.replace(path, quarantine / f"lease-{int(time.time() * 1000)}.json")
    try:
        with path.open("x", encoding="utf-8") as handle:
            now = utc_now().isoformat()
            handle.write(
                json.dumps(
                    {"run_id": lease_id, "pid": os.getpid(), "created_at": now, "heartbeat": now},
                    sort_keys=True,
                )
            )
            handle.flush()
            os.fsync(handle.fileno())
    except FileExistsError as error:
        raise RuntimeError("individual trade writer lease already held") from error
    return path


def quarantine_stale_trade_partials(
    root: Path, *, stale_after_seconds: int = 600, now: float | None = None
) -> list[Path]:
    """Quarantine abandoned trade partials by age; current writer files remain."""
    now = time.time() if now is None else now
    moved = []
    quarantine = root / "quarantine" / "stale_trade_partials"
    for partial in root.rglob("*.partial"):
        if quarantine in partial.parents or now - partial.stat().st_mtime < stale_after_seconds:
            continue
        quarantine.mkdir(parents=True, exist_ok=True)
        target = (
            quarantine / f"{partial.name}.{hashlib.sha256(str(partial).encode()).hexdigest()[:12]}"
        )
        os.replace(partial, target)
        moved.append(target)
    return moved


def _append_manifest(path: Path, event: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    encoded = json.dumps(event, sort_keys=True, default=str)
    if path.exists():
        for line in path.read_text(encoding="utf-8").splitlines():
            current = json.loads(line)
            if current.get("parquet_sha256") == event["parquet_sha256"]:
                if current != event:
                    raise ValueError("manifest checksum collision with different event")
                return
    with path.open("a", encoding="utf-8") as handle:
        handle.write(encoded + "\n")
        handle.flush()
        os.fsync(handle.fileno())


def _commit_day(
    *,
    root: Path,
    identity: InstrumentIdentity,
    trading_date: date,
    archive_path: Path,
    expected_raw_sha256: str,
    retrieved_at: datetime | None = None,
) -> PilotMeasurement:
    """Durably persist raw archive, canonical Parquet, manifest and checkpoint for one UTC day."""
    if (
        identity.exchange != "binance"
        or identity.market_type != "spot"
        or identity.contract_type != "spot"
    ):
        raise ValueError("this adapter accepts only Binance Spot identities")
    if not archive_path.exists():
        matches = list(
            (
                root
                / "raw"
                / "binance"
                / "spot"
                / "individual_trade_archive"
                / identity.native_symbol
            ).glob(f"{trading_date}-{expected_raw_sha256}.zip")
        )
        if len(matches) != 1:
            raise ValueError("missing archive and no unique content-addressed raw recovery object")
        archive_path = matches[0]
    if sha256_file(archive_path) != expected_raw_sha256:
        raise ValueError("archive checksum mismatch before persistence")
    if not resource_gate(root, estimated_download_bytes=archive_path.stat().st_size):
        raise RuntimeError("disk resource gate failed: preserving 50 GiB free is mandatory")
    retrieved_at = retrieved_at or utc_now()
    before = disk_free_bytes(root)
    raw_hash = expected_raw_sha256
    raw = (
        root
        / "raw"
        / "binance"
        / "spot"
        / "individual_trade_archive"
        / identity.native_symbol
        / f"{trading_date}-{raw_hash}.zip"
    )
    if not raw.exists():
        raw.parent.mkdir(parents=True, exist_ok=True)
        os.replace(archive_path, raw)
    started = time.perf_counter()
    source_uri = archive_url(identity.native_symbol, trading_date)
    output = (
        root
        / "normalized"
        / "individual_trade"
        / "v1"
        / "exchange=binance"
        / "market_type=spot"
        / f"instrument_id={identity.instrument_id}"
        / f"date={trading_date}"
        / f"part-{raw_hash[:16]}.parquet"
    )
    output.parent.mkdir(parents=True, exist_ok=True)
    extracted_bytes = 0
    row_count = 0
    first_event_time: datetime | None = None
    last_event_time: datetime | None = None
    last_trade_id: int | None = None
    final_trade_id: str | None = None
    if not output.exists():
        partial = output.parent / f".{output.name}.partial"
        source_rows, extracted_bytes = iter_archive_rows(
            raw, expected_symbol=identity.native_symbol, expected_date=trading_date
        )
        with pq.ParquetWriter(
            partial, INDIVIDUAL_TRADE_SCHEMA, compression="zstd", use_dictionary=True
        ) as writer:
            batch: list[dict[str, Any]] = []
            for index, source_row in enumerate(source_rows):
                row = normalize_trade(
                    source_row,
                    source_ordinal=index,
                    identity=identity,
                    trading_date=trading_date,
                    source_uri=source_uri,
                    raw_object_ref=str(raw.relative_to(root)),
                    source_sha256=raw_hash,
                    retrieved_at=retrieved_at,
                )
                trade_id = int(row["native_trade_id"])
                if last_trade_id is not None and trade_id <= last_trade_id:
                    raise ValueError(
                        "individual trade IDs must strictly increase in completed archive"
                    )
                last_trade_id, final_trade_id = trade_id, row["native_trade_id"]
                first_event_time = first_event_time or row["event_time"]
                last_event_time = row["event_time"]
                batch.append(row)
                row_count += 1
                if len(batch) >= 25_000:
                    writer.write_table(pa.Table.from_pylist(batch, schema=INDIVIDUAL_TRADE_SCHEMA))
                    batch.clear()
            if batch:
                writer.write_table(pa.Table.from_pylist(batch, schema=INDIVIDUAL_TRADE_SCHEMA))
        if (
            row_count == 0
            or first_event_time is None
            or last_event_time is None
            or final_trade_id is None
        ):
            raise ValueError("empty individual trade archive")
        if pq.ParquetFile(partial).metadata.num_rows != row_count:
            raise ValueError("Parquet validation failed")
        if not resource_gate(
            root,
            estimated_download_bytes=raw.stat().st_size,
            extracted_bytes=extracted_bytes,
            parquet_bytes=partial.stat().st_size,
        ):
            raise RuntimeError("per-write disk resource gate failed")
        with partial.open("r+b") as handle:
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(partial, output)
    else:
        metadata = pq.ParquetFile(output).metadata
        row_count = metadata.num_rows
        extracted_bytes = iter_archive_rows(raw)[1]
        # Reruns do not rewrite the immutable partition; checkpoint information is recovered
        # from the canonical table with a bounded final row-group read.
        table = pq.ParquetFile(output).read_row_group(
            metadata.num_row_groups - 1, columns=["event_time", "native_trade_id"]
        )
        first_event_time = (
            pq.ParquetFile(output).read_row_group(0, columns=["event_time"]).column(0)[0].as_py()
        )
        last_event_time = table.column("event_time")[-1].as_py()
        final_trade_id = table.column("native_trade_id")[-1].as_py()
    parquet_hash = sha256_file(output)
    _append_manifest(
        root / "control" / "manifests" / "binance_spot_individual_trade.jsonl",
        {
            "action": "NORMALIZED",
            "object_id": str(output.relative_to(root)),
            "parquet_sha256": parquet_hash,
            "raw_sha256": raw_hash,
            "external_checksum_sha256": raw_hash,
            "local_raw_sha256": sha256_file(raw),
            "checksum_sidecar_uri": f"{source_uri}.CHECKSUM",
            "raw_object_ref": str(raw.relative_to(root)),
            "source_uri": source_uri,
            "source_kind": "daily_archive",
            "source_dataset_id": DATASET_ID,
            "dataset_class": "individual_trade",
            "exchange": "binance",
            "market_type": "spot",
            "contract_type": "spot",
            "instrument_id": identity.instrument_id,
            "coverage_start": first_event_time.isoformat(),
            "coverage_end": last_event_time.isoformat(),
            "row_count": row_count,
            "raw_bytes": raw.stat().st_size,
            "extracted_bytes": extracted_bytes,
            "parquet_bytes": output.stat().st_size,
            "retrieved_at": retrieved_at.isoformat(),
            "processed_at": retrieved_at.isoformat(),
            "schema_version": "1.0.0",
            "collector_version": "0.3.0",
            "normalization_version": "1.0.0",
            "source_contract_version": source_contract_id(trading_date),
            "known_limitations": [
                "historical knowledge_time unknown; archive retrieval time is not market availability"
            ],
        },
    )
    _atomic_bytes(
        root
        / "control"
        / "checkpoints"
        / f"binance_spot_{identity.native_symbol.lower()}_individual_trade.json",
        json.dumps(
            {
                "source_dataset_id": DATASET_ID,
                "instrument_id": identity.instrument_id,
                "cursor": final_trade_id,
                "last_event_time": last_event_time.isoformat(),
                "last_knowledge_time": None,
                "committed_at": retrieved_at.isoformat(),
            },
            sort_keys=True,
        ).encode(),
    )
    after = disk_free_bytes(root)
    return PilotMeasurement(
        trading_date.isoformat(),
        row_count,
        raw.stat().st_size,
        extracted_bytes,
        output.stat().st_size,
        time.perf_counter() - started,
        raw_hash,
        before,
        after,
        resource_gate(root, estimated_download_bytes=raw.stat().st_size),
        peak_rss_bytes(),
    )


def commit_day(**kwargs: Any) -> PilotMeasurement:
    """Single-writer wrapper; lock removal happens even if normalization fails."""
    root = Path(kwargs["root"])
    lease = acquire_writer_lease(
        root, lease_id=f"pid={os.getpid()} started={utc_now().isoformat()}"
    )
    try:
        return _commit_day(**kwargs)
    finally:
        lease.unlink(missing_ok=True)


def plan_retention(root: Path, *, instrument_id: str, trading_date: date) -> list[dict[str, Any]]:
    """Plan deletion only when the immutable 60s derivative is complete."""
    derived_manifest = root / "control" / "manifests" / "derived_trade_bucket.jsonl"
    events = (
        [json.loads(line) for line in derived_manifest.read_text(encoding="utf-8").splitlines()]
        if derived_manifest.exists()
        else []
    )
    minute = [
        event
        for event in events
        if event.get("instrument_id") == instrument_id
        and event.get("trading_date") == trading_date.isoformat()
        and event.get("bucket_seconds") == 60
    ]
    if len(minute) != 1:
        raise ValueError("retention requires exactly one complete 60s manifest event")
    source_id = minute[0]["source_object_id"]
    source_manifest = root / "control" / "manifests" / "binance_spot_individual_trade.jsonl"
    source_events = [
        json.loads(line) for line in source_manifest.read_text(encoding="utf-8").splitlines()
    ]
    sources = [event for event in source_events if event.get("object_id") == source_id]
    if len(sources) != 1 or sources[0]["row_count"] != minute[0]["source_row_count"]:
        raise ValueError("60s/source completeness mismatch")
    targets = [sources[0]["raw_object_ref"], sources[0]["object_id"]]
    targets += [
        event["object_id"]
        for event in events
        if event.get("instrument_id") == instrument_id
        and event.get("trading_date") == trading_date.isoformat()
        and event.get("bucket_seconds") in {1, 5}
    ]
    return [
        {
            "action": "DELETION_PLANNED",
            "object_id": target,
            "checksum_sha256": sha256_file(root / target),
            "reason": "retention_after_complete_60s",
            "trading_date": trading_date.isoformat(),
        }
        for target in targets
    ]


def apply_retention(root: Path, events: list[dict[str, Any]], *, dry_run: bool = True) -> Path:
    """Append a stable audit; actual deletion is opt-in and intended for tested policy jobs."""
    ledger = root / "control" / "deletion_ledger" / "trade_retention.jsonl"
    ledger.parent.mkdir(parents=True, exist_ok=True)
    existing = (
        {
            json.loads(line)["deletion_id"]
            for line in ledger.read_text(encoding="utf-8").splitlines()
        }
        if ledger.exists()
        else set()
    )
    with ledger.open("a", encoding="utf-8") as handle:
        for event in events:
            deletion_id = hashlib.sha256(
                json.dumps({"event": event, "dry_run": dry_run}, sort_keys=True).encode()
            ).hexdigest()
            if deletion_id in existing:
                continue
            path = root / event["object_id"]
            record = {
                **event,
                "deletion_id": deletion_id,
                "status": "DRY_RUN" if dry_run else "DELETED",
                "deleted_at": None if dry_run else utc_now().isoformat(),
            }
            if not dry_run:
                if sha256_file(path) != event["checksum_sha256"]:
                    raise ValueError("retention target checksum changed")
                path.unlink()
            handle.write(json.dumps(record, sort_keys=True) + "\n")
            handle.flush()
            os.fsync(handle.fileno())
            existing.add(deletion_id)
    return ledger
