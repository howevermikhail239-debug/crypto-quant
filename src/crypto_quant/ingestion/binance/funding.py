"""Binance USD-M Funding Rate Ingestion and Historical Normalization (Phase 1D.1A).

Fetches realized funding rates from /fapi/v1/fundingRate and current metadata from /fapi/v1/fundingInfo.
Enforces:
- Canonical identity: market_type='perpetual', contract_type='linear_perpetual', venue_product_type='usdm'
- Natural key: (exchange, instrument_id, funding_time, rate_type)
- Decimal fraction preservation: raw decimal fraction (e.g. 0.00007054), never multiplied by 100
- Interval separation: observed_interval_minutes (from event delta) vs configured_interval_minutes (point-in-time snapshot)
- Conservative knowledge_time: None (UNKNOWN) for historical bootstrap to prevent look-ahead
- Ascending pagination traversal: next_start = last_funding_time + 1ms with deduplication
- Immutable Parquet storage, manifests, checkpoints, and data quality checks
"""

from __future__ import annotations

import hashlib
import json
import logging
import os
import time
from dataclasses import asdict, dataclass
from datetime import datetime
from decimal import Decimal
from pathlib import Path
from typing import Any

import httpx
import pyarrow as pa
import pyarrow.parquet as pq

from ...hashing import sha256_text
from ...identity import InstrumentIdentity
from ...paths import disk_free_bytes
from ...time import parse_epoch, utc_now

logger = logging.getLogger(__name__)

DATASET_ID = "binance.usdm.funding_rate.rest"
CONTRACT_ID = "binance.usdm.rest.funding-rate.v1"
METADATA_DATASET_ID = "binance.usdm.funding_info.rest"
METADATA_CONTRACT_ID = "binance.usdm.rest.funding-info.v1"
COLLECTOR_VERSION = "0.4.0"
NORMALIZATION_VERSION = "1.0.0"
SCHEMA_VERSION = "1.0.0"

BINANCE_FAPI_BASE = "https://fapi.binance.com"

CANONICAL_FUNDING_SCHEMA = pa.schema([
    ("exchange", pa.string()),
    ("instrument_id", pa.string()),
    ("symbol", pa.string()),
    ("market_type", pa.string()),
    ("contract_type", pa.string()),
    ("venue_product_type", pa.string()),
    ("funding_time", pa.timestamp("us", tz="UTC")),
    ("funding_rate", pa.string()),  # Raw Decimal fraction string (e.g. "0.00007054")
    ("source_rate_type", pa.string()),  # "Regular", "Special", etc.
    ("canonical_rate_type", pa.string()),  # "REGULAR", "SPECIAL"
    ("mark_price", pa.string()),  # Decimal string or None
    ("observed_interval_minutes", pa.int64()),  # Derived delta or None
    ("configured_interval_minutes", pa.int64()),  # None in historical rows (never retrofitted)
    ("interval_source", pa.string()),  # "OBSERVED_EVENT_DELTA" / "UNKNOWN"
    ("event_time", pa.timestamp("us", tz="UTC")),
    ("knowledge_time", pa.timestamp("us", tz="UTC")),  # None / null (UNKNOWN)
    ("source", pa.string()),
    ("source_contract_version", pa.string()),
    ("schema_version", pa.string()),
    ("collector_version", pa.string()),
    ("normalization_version", pa.string()),
])


def funding_identity(symbol: str) -> InstrumentIdentity:
    """Canonical instrument identity for Binance USD-M linear perpetuals."""
    if symbol not in {"BTCUSDT", "ETHUSDT"}:
        raise ValueError("Binance USD-M funding permits BTCUSDT/ETHUSDT only")
    base = symbol.removesuffix("USDT")
    return InstrumentIdentity(
        exchange="binance",
        native_symbol=symbol,
        market_type="perpetual",
        contract_type="linear_perpetual",
        base_asset=base,
        quote_asset="USDT",
        settle_asset="USDT",
        quantity_unit=base,
        notional_unit="USDT",
    )


@dataclass(frozen=True)
class CanonicalFundingRecord:
    exchange: str
    instrument_id: str
    symbol: str
    market_type: str
    contract_type: str
    venue_product_type: str
    funding_time: datetime
    funding_rate: str
    source_rate_type: str | None
    canonical_rate_type: str
    mark_price: str | None
    observed_interval_minutes: int | None
    configured_interval_minutes: int | None
    interval_source: str
    event_time: datetime
    knowledge_time: datetime | None
    source: str
    source_contract_version: str
    schema_version: str
    collector_version: str
    normalization_version: str

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


def parse_binance_funding_rate_item(
    raw: dict[str, Any],
    ident: InstrumentIdentity,
    prev_funding_time: datetime | None = None,
) -> CanonicalFundingRecord:
    """Parses and normalizes a single raw item from GET /fapi/v1/fundingRate."""
    sym = str(raw.get("symbol", ""))
    if sym != ident.native_symbol:
        raise ValueError(f"Symbol mismatch: expected {ident.native_symbol}, got {sym}")

    raw_time = raw.get("fundingTime")
    if raw_time is None:
        raise ValueError("Missing mandatory field 'fundingTime'")
    funding_time = parse_epoch(int(raw_time), unit="ms")

    raw_rate = raw.get("fundingRate")
    if raw_rate is None or str(raw_rate).strip() == "":
        raise ValueError("Missing mandatory field 'fundingRate'")
    # Validate Decimal without changing precision or multiplying by 100
    dec_rate = Decimal(str(raw_rate).strip())
    funding_rate_str = str(dec_rate)

    # Mark price is optional in early archives (may be empty string)
    raw_mark = raw.get("markPrice")
    mark_price_str: str | None = None
    if raw_mark is not None and str(raw_mark).strip() != "":
        dec_mark = Decimal(str(raw_mark).strip())
        mark_price_str = str(dec_mark)

    source_rate_type = raw.get("rateType")
    if source_rate_type is not None:
        source_rate_type = str(source_rate_type).strip()
        canonical_rate_type = source_rate_type.upper()
    else:
        source_rate_type = "Regular"
        canonical_rate_type = "REGULAR"

    # Calculate observed interval from previous event delta
    observed_interval_minutes: int | None = None
    if prev_funding_time is not None:
        delta_sec = (funding_time - prev_funding_time).total_seconds()
        observed_interval_minutes = int(round(delta_sec / 60))

    interval_source = "OBSERVED_EVENT_DELTA" if observed_interval_minutes is not None else "UNKNOWN"

    return CanonicalFundingRecord(
        exchange="binance",
        instrument_id=ident.instrument_id,
        symbol=ident.native_symbol,
        market_type="perpetual",
        contract_type="linear_perpetual",
        venue_product_type="usdm",
        funding_time=funding_time,
        funding_rate=funding_rate_str,
        source_rate_type=source_rate_type,
        canonical_rate_type=canonical_rate_type,
        mark_price=mark_price_str,
        observed_interval_minutes=observed_interval_minutes,
        configured_interval_minutes=None,  # Never backfill current snapshot into past events
        interval_source=interval_source,
        event_time=funding_time,
        knowledge_time=None,  # UNKNOWN for historical bootstrap
        source=DATASET_ID,
        source_contract_version=CONTRACT_ID,
        schema_version=SCHEMA_VERSION,
        collector_version=COLLECTOR_VERSION,
        normalization_version=NORMALIZATION_VERSION,
    )


def fetch_binance_funding_info(
    root: Path,
    client: httpx.Client | None = None,
) -> list[dict[str, Any]]:
    """Fetches point-in-time metadata snapshot from GET /fapi/v1/fundingInfo.

    Stores snapshot in control/instrument_metadata/ without backfilling to historical records.
    """
    url = f"{BINANCE_FAPI_BASE}/fapi/v1/fundingInfo"
    should_close = False
    if client is None:
        client = httpx.Client(timeout=30)
        should_close = True
    try:
        resp = client.get(url)
        resp.raise_for_status()
        data = resp.json()
        now = utc_now()
        retrieved_iso = now.isoformat()

        target_dir = root / "control" / "instrument_metadata"
        target_dir.mkdir(parents=True, exist_ok=True)
        ts_slug = now.strftime("%Y%m%dT%H%M%SZ")
        snapshot_file = target_dir / f"binance_usdm_funding_info_{ts_slug}.json"

        payload = {
            "dataset_id": METADATA_DATASET_ID,
            "contract_id": METADATA_CONTRACT_ID,
            "retrieved_at": retrieved_iso,
            "items": data,
        }
        snapshot_file.write_text(json.dumps(payload, indent=2), encoding="utf-8")
        return data
    finally:
        if should_close:
            client.close()


def fetch_binance_funding_history(
    symbol: str,
    *,
    start_time_ms: int | None = None,
    end_time_ms: int | None = None,
    client: httpx.Client | None = None,
    limit: int = 1000,
) -> list[dict[str, Any]]:
    """Traverses GET /fapi/v1/fundingRate across multiple pages using ascending start_time."""
    url = f"{BINANCE_FAPI_BASE}/fapi/v1/fundingRate"
    should_close = False
    if client is None:
        client = httpx.Client(timeout=30)
        should_close = True

    all_items: list[dict[str, Any]] = []
    current_start = start_time_ms

    try:
        while True:
            params: dict[str, Any] = {"symbol": symbol, "limit": limit}
            if current_start is not None:
                params["startTime"] = current_start
            if end_time_ms is not None:
                params["endTime"] = end_time_ms

            resp = client.get(url, params=params)
            resp.raise_for_status()
            batch: list[dict[str, Any]] = resp.json()
            if not batch:
                break

            all_items.extend(batch)
            if len(batch) < limit:
                break

            last_ts = int(batch[-1]["fundingTime"])
            if end_time_ms is not None and last_ts >= end_time_ms:
                break

            # Advance by 1 ms to prevent duplicate boundary items
            current_start = last_ts + 1
            time.sleep(0.05)  # Cooperative pacing

        return all_items
    finally:
        if should_close:
            client.close()


def records_to_pyarrow_table(records: list[CanonicalFundingRecord]) -> pa.Table:
    """Converts normalized funding records to PyArrow Table conforming to CANONICAL_FUNDING_SCHEMA."""
    cols: dict[str, list[Any]] = {field.name: [] for field in CANONICAL_FUNDING_SCHEMA}
    for r in records:
        cols["exchange"].append(r.exchange)
        cols["instrument_id"].append(r.instrument_id)
        cols["symbol"].append(r.symbol)
        cols["market_type"].append(r.market_type)
        cols["contract_type"].append(r.contract_type)
        cols["venue_product_type"].append(r.venue_product_type)
        cols["funding_time"].append(r.funding_time)
        cols["funding_rate"].append(r.funding_rate)
        cols["source_rate_type"].append(r.source_rate_type)
        cols["canonical_rate_type"].append(r.canonical_rate_type)
        cols["mark_price"].append(r.mark_price)
        cols["observed_interval_minutes"].append(r.observed_interval_minutes)
        cols["configured_interval_minutes"].append(r.configured_interval_minutes)
        cols["interval_source"].append(r.interval_source)
        cols["event_time"].append(r.event_time)
        cols["knowledge_time"].append(r.knowledge_time)
        cols["source"].append(r.source)
        cols["source_contract_version"].append(r.source_contract_version)
        cols["schema_version"].append(r.schema_version)
        cols["collector_version"].append(r.collector_version)
        cols["normalization_version"].append(r.normalization_version)

    return pa.Table.from_pydict(cols, schema=CANONICAL_FUNDING_SCHEMA)


def validate_funding_records_dq(records: list[CanonicalFundingRecord]) -> list[str]:
    """Performs Data Quality checks on normalized funding records."""
    issues: list[str] = []
    seen_keys: set[tuple[str, str, datetime, str]] = set()
    prev_time: datetime | None = None

    for idx, r in enumerate(records):
        # 1. Natural key uniqueness
        key = (r.exchange, r.instrument_id, r.funding_time, r.canonical_rate_type)
        if key in seen_keys:
            issues.append(f"Row {idx}: Duplicate natural key {key}")
        seen_keys.add(key)

        # 2. Strict monotonic ascending ordering
        if prev_time is not None and r.funding_time <= prev_time:
            issues.append(f"Row {idx}: Non-monotonic timestamp {r.funding_time} <= {prev_time}")
        prev_time = r.funding_time

        # 3. Decimal format validation
        try:
            Decimal(r.funding_rate)
        except Exception:
            issues.append(f"Row {idx}: Invalid Decimal funding_rate '{r.funding_rate}'")

        if r.mark_price is not None:
            try:
                dec_mark = Decimal(r.mark_price)
                if dec_mark <= 0:
                    issues.append(f"Row {idx}: Non-positive mark_price '{r.mark_price}'")
            except Exception:
                issues.append(f"Row {idx}: Invalid Decimal mark_price '{r.mark_price}'")

        # 4. Mandatory fields
        if not r.instrument_id.startswith("ins_"):
            issues.append(f"Row {idx}: Invalid instrument_id '{r.instrument_id}'")
        if r.market_type != "perpetual":
            issues.append(f"Row {idx}: Unexpected market_type '{r.market_type}'")

    return issues


def ingest_binance_funding_rate(
    symbol: str,
    root: Path,
    *,
    start_time_ms: int | None = None,
    end_time_ms: int | None = None,
    client: httpx.Client | None = None,
    min_disk_free_gb: float = 20.0,
) -> dict[str, Any]:
    """End-to-end ingestion, normalization, Parquet persistence, manifest and checkpointing."""
    free_gb = disk_free_bytes(root) / (1024**3)
    if free_gb < min_disk_free_gb:
        raise OSError(f"Disk space below threshold: {free_gb:.2f} GB < {min_disk_free_gb} GB")

    ident = funding_identity(symbol)
    retrieved_at = utc_now()

    # 1. Fetch metadata snapshot (best effort point-in-time)
    try:
        _ = fetch_binance_funding_info(root, client=client)
    except Exception as exc:
        logger.warning(f"Could not fetch fundingInfo snapshot: {exc}")

    # 2. Fetch raw history via paginated REST traversal
    raw_items = fetch_binance_funding_history(
        symbol, start_time_ms=start_time_ms, end_time_ms=end_time_ms, client=client
    )
    if not raw_items:
        return {
            "symbol": symbol,
            "status": "EMPTY",
            "records_count": 0,
            "coverage_start": None,
            "coverage_end": None,
        }

    # 3. Normalize records in strict ascending order
    normalized_records: list[CanonicalFundingRecord] = []
    prev_time: datetime | None = None
    for item in raw_items:
        rec = parse_binance_funding_rate_item(item, ident, prev_funding_time=prev_time)
        normalized_records.append(rec)
        prev_time = rec.funding_time

    # 4. Data Quality validation
    dq_issues = validate_funding_records_dq(normalized_records)
    if dq_issues:
        raise ValueError(f"Funding DQ validation failed: {dq_issues[:5]}")

    # 5. Persist raw JSONL
    min_ts_iso = normalized_records[0].funding_time.strftime("%Y%m%dT%H%M%SZ")
    max_ts_iso = normalized_records[-1].funding_time.strftime("%Y%m%dT%H%M%SZ")
    raw_dir = root / "raw" / "binance" / "perpetual" / "funding_rate" / symbol
    raw_dir.mkdir(parents=True, exist_ok=True)
    raw_file = raw_dir / f"funding_{min_ts_iso}_{max_ts_iso}.jsonl"
    with raw_file.open("w", encoding="utf-8") as f:
        for item in raw_items:
            f.write(json.dumps(item) + "\n")
    raw_hash = sha256_text(raw_file.read_text(encoding="utf-8"))

    # 6. Group by Year and Persist Canonical Parquet
    norm_base = (
        root
        / "normalized"
        / "funding"
        / "v1"
        / "exchange=binance"
        / "market_type=perpetual"
        / f"symbol={symbol}"
    )
    norm_base.mkdir(parents=True, exist_ok=True)

    records_by_year: dict[int, list[CanonicalFundingRecord]] = {}
    for r in normalized_records:
        yr = r.funding_time.year
        records_by_year.setdefault(yr, []).append(r)

    created_parquet_files: list[Path] = []
    for yr, yr_records in sorted(records_by_year.items()):
        yr_table = records_to_pyarrow_table(yr_records)
        yr_dir = norm_base / f"year={yr}"
        yr_dir.mkdir(parents=True, exist_ok=True)
        target_parquet = yr_dir / f"part-{symbol.lower()}_{yr}.parquet"
        partial_parquet = target_parquet.with_suffix(".parquet.partial")

        pq.write_table(yr_table, partial_parquet, compression="zstd", flavor="spark")
        os.replace(partial_parquet, target_parquet)
        created_parquet_files.append(target_parquet)

    # 7. Record Manifest
    manifest_dir = root / "control" / "manifests"
    manifest_dir.mkdir(parents=True, exist_ok=True)
    manifest_file = manifest_dir / "binance_usdm_funding_rate.jsonl"

    retrieved_iso = retrieved_at.isoformat()
    manifest_record = {
        "action": "NORMALIZED",
        "exchange": "binance",
        "market_type": "perpetual",
        "venue_product_type": "usdm",
        "symbol": symbol,
        "instrument_id": ident.instrument_id,
        "dataset_class": "funding_rate",
        "requested_coverage_start": str(start_time_ms) if start_time_ms else "INCEPTION",
        "requested_coverage_end": str(end_time_ms) if end_time_ms else "LATEST",
        "observed_coverage_start": normalized_records[0].funding_time.isoformat(),
        "observed_coverage_end": normalized_records[-1].funding_time.isoformat(),
        "row_count": len(normalized_records),
        "raw_object_ref": str(raw_file.relative_to(root)),
        "raw_sha256": raw_hash,
        "created_parquets": [str(p.relative_to(root)) for p in created_parquet_files],
        "source_dataset_id": DATASET_ID,
        "source_contract_version": CONTRACT_ID,
        "schema_version": SCHEMA_VERSION,
        "collector_version": COLLECTOR_VERSION,
        "normalization_version": NORMALIZATION_VERSION,
        "retrieved_at": retrieved_iso,
        "processed_at": retrieved_iso,
    }
    with manifest_file.open("a", encoding="utf-8") as mf:
        mf.write(json.dumps(manifest_record) + "\n")

    # 8. Record Checkpoint
    chk_dir = root / "control" / "checkpoints"
    chk_dir.mkdir(parents=True, exist_ok=True)
    chk_file = chk_dir / f"binance_usdm_funding_rate_{symbol}.json"
    chk_payload = {
        "symbol": symbol,
        "last_funding_time_ms": int(normalized_records[-1].funding_time.timestamp() * 1000),
        "last_funding_time_iso": normalized_records[-1].funding_time.isoformat(),
        "total_records": len(normalized_records),
        "updated_at": retrieved_iso,
    }
    chk_file.write_text(json.dumps(chk_payload, indent=2), encoding="utf-8")

    return {
        "symbol": symbol,
        "status": "PASS",
        "records_count": len(normalized_records),
        "coverage_start": normalized_records[0].funding_time.isoformat(),
        "coverage_end": normalized_records[-1].funding_time.isoformat(),
        "years": sorted(records_by_year.keys()),
        "raw_file": str(raw_file),
        "parquet_files": [str(p) for p in created_parquet_files],
    }
