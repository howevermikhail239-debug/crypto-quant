"""Bybit Linear Funding Rate Ingestion and Historical Normalization (Phase 1D.1B).

Fetches realized funding rates from /v5/market/funding/history and metadata from /v5/market/instruments-info.
Enforces:
- Canonical identity: market_type='perpetual', contract_type='linear_perpetual', venue_product_type='linear'
- Natural key: (exchange, instrument_id, funding_time, rate_type) -> rate_type='NOT_PROVIDED'
- Decimal fraction preservation: raw decimal fraction (e.g. 0.00005639), never multiplied by 100
- Mark price provenance: Bybit funding history does not provide markPrice -> mark_price=None (no silent enrichment)
- Rate type provenance: Bybit funding history does not provide rateType -> source_rate_type=None, canonical_rate_type='NOT_PROVIDED'
- Interval separation: observed_interval_minutes (from event delta) vs configured_interval_minutes (point-in-time snapshot)
- Conservative knowledge_time: None (UNKNOWN) for historical bootstrap to prevent look-ahead
- Traversal & Ordering: traverses history backwards via endTime, then sorts strictly ascending by funding_time
- Immutable Parquet storage, manifests, checkpoints, and data quality checks
"""

from __future__ import annotations

import json
import logging
import os
import time
from datetime import datetime
from decimal import Decimal
from pathlib import Path
from typing import Any

import httpx
import pyarrow.parquet as pq

from ...hashing import sha256_text
from ...identity import InstrumentIdentity
from ...paths import disk_free_bytes
from ...time import parse_epoch, utc_now
from ..binance.funding import (
    CanonicalFundingRecord,
    records_to_pyarrow_table,
    validate_funding_records_dq,
)

logger = logging.getLogger(__name__)

DATASET_ID = "bybit.linear.funding_rate.rest"
CONTRACT_ID = "bybit.linear.rest.funding-rate.v1"
METADATA_DATASET_ID = "bybit.linear.instruments-info.rest"
METADATA_CONTRACT_ID = "bybit.linear.instruments-info.v1"
COLLECTOR_VERSION = "0.4.0"
NORMALIZATION_VERSION = "1.0.0"
SCHEMA_VERSION = "1.0.0"

BYBIT_API_BASE = "https://api.bybit.com"


def funding_identity(symbol: str) -> InstrumentIdentity:
    """Canonical instrument identity for Bybit Linear USDT perpetuals."""
    if symbol not in {"BTCUSDT", "ETHUSDT"}:
        raise ValueError("Bybit linear funding permits BTCUSDT/ETHUSDT only")
    base = symbol.removesuffix("USDT")
    return InstrumentIdentity(
        exchange="bybit",
        native_symbol=symbol,
        market_type="perpetual",
        contract_type="linear_perpetual",
        base_asset=base,
        quote_asset="USDT",
        settle_asset="USDT",
        quantity_unit=base,
        notional_unit="USDT",
    )


def parse_bybit_funding_rate_item(
    raw: dict[str, Any],
    ident: InstrumentIdentity,
    prev_funding_time: datetime | None = None,
) -> CanonicalFundingRecord:
    """Parses and normalizes a single raw item from GET /v5/market/funding/history."""
    sym = str(raw.get("symbol", ""))
    if sym != ident.native_symbol:
        raise ValueError(f"Symbol mismatch: expected {ident.native_symbol}, got {sym}")

    raw_time = raw.get("fundingRateTimestamp")
    if raw_time is None:
        raise ValueError("Missing mandatory field 'fundingRateTimestamp'")
    funding_time = parse_epoch(int(raw_time), unit="ms")

    raw_rate = raw.get("fundingRate")
    if raw_rate is None or str(raw_rate).strip() == "":
        raise ValueError("Missing mandatory field 'fundingRate'")
    dec_rate = Decimal(str(raw_rate).strip())
    funding_rate_str = str(dec_rate)

    # Calculate observed interval from previous event delta
    observed_interval_minutes: int | None = None
    if prev_funding_time is not None:
        delta_sec = (funding_time - prev_funding_time).total_seconds()
        observed_interval_minutes = int(round(delta_sec / 60))

    interval_source = "OBSERVED_EVENT_DELTA" if observed_interval_minutes is not None else "UNKNOWN"

    return CanonicalFundingRecord(
        exchange="bybit",
        instrument_id=ident.instrument_id,
        symbol=ident.native_symbol,
        market_type="perpetual",
        contract_type="linear_perpetual",
        venue_product_type="linear",
        funding_time=funding_time,
        funding_rate=funding_rate_str,
        source_rate_type=None,  # Bybit does not provide rateType
        canonical_rate_type="NOT_PROVIDED",
        mark_price=None,  # Bybit funding history does not provide markPrice
        observed_interval_minutes=observed_interval_minutes,
        configured_interval_minutes=None,  # Never backfill current snapshot historically
        interval_source=interval_source,
        event_time=funding_time,
        knowledge_time=None,  # UNKNOWN for historical bootstrap
        source=DATASET_ID,
        source_contract_version=CONTRACT_ID,
        schema_version=SCHEMA_VERSION,
        collector_version=COLLECTOR_VERSION,
        normalization_version=NORMALIZATION_VERSION,
    )


def fetch_bybit_instruments_info_snapshot(
    symbol: str,
    root: Path,
    client: httpx.Client | None = None,
) -> dict[str, Any]:
    """Fetches point-in-time metadata snapshot from GET /v5/market/instruments-info."""
    url = f"{BYBIT_API_BASE}/v5/market/instruments-info"
    params = {"category": "linear", "symbol": symbol}
    should_close = False
    if client is None:
        client = httpx.Client(timeout=30)
        should_close = True

    try:
        resp = client.get(url, params=params)
        resp.raise_for_status()
        data = resp.json()
        if data.get("retCode") != 0:
            raise ValueError(f"Bybit instruments-info error: {data.get('retMsg')}")

        items = data.get("result", {}).get("list", [])
        item = items[0] if items else {}
        now = utc_now()
        retrieved_iso = now.isoformat()

        target_dir = root / "control" / "instrument_metadata"
        target_dir.mkdir(parents=True, exist_ok=True)
        ts_slug = now.strftime("%Y%m%dT%H%M%SZ")
        snapshot_file = target_dir / f"bybit_linear_instruments_info_{symbol}_{ts_slug}.json"

        payload = {
            "dataset_id": METADATA_DATASET_ID,
            "contract_id": METADATA_CONTRACT_ID,
            "retrieved_at": retrieved_iso,
            "symbol": symbol,
            "item": item,
        }
        snapshot_file.write_text(json.dumps(payload, indent=2), encoding="utf-8")
        return item
    finally:
        if should_close:
            client.close()


def fetch_bybit_funding_history(
    symbol: str,
    *,
    start_time_ms: int | None = None,
    end_time_ms: int | None = None,
    client: httpx.Client | None = None,
    limit: int = 200,
) -> list[dict[str, Any]]:
    """Traverses Bybit GET /v5/market/funding/history backwards via endTime, returning records in ascending order."""
    url = f"{BYBIT_API_BASE}/v5/market/funding/history"
    should_close = False
    if client is None:
        client = httpx.Client(timeout=30)
        should_close = True

    all_raw_items: list[dict[str, Any]] = []
    current_end = end_time_ms
    seen_timestamps: set[int] = set()

    try:
        while True:
            params: dict[str, Any] = {
                "category": "linear",
                "symbol": symbol,
                "limit": limit,
            }
            if current_end is not None:
                params["endTime"] = current_end

            resp = client.get(url, params=params)
            resp.raise_for_status()
            data = resp.json()
            if data.get("retCode") != 0:
                raise ValueError(f"Bybit API error {data.get('retCode')}: {data.get('retMsg')}")

            items: list[dict[str, Any]] = data.get("result", {}).get("list", [])
            if not items:
                break

            new_in_batch = 0
            for item in items:
                ts = int(item["fundingRateTimestamp"])
                if start_time_ms is not None and ts < start_time_ms:
                    continue
                if ts not in seen_timestamps:
                    seen_timestamps.add(ts)
                    all_raw_items.append(item)
                    new_in_batch += 1

            if len(items) < limit or new_in_batch == 0:
                break

            oldest_batch_ts = int(items[-1]["fundingRateTimestamp"])
            if start_time_ms is not None and oldest_batch_ts <= start_time_ms:
                break

            current_end = oldest_batch_ts - 1
            time.sleep(0.05)  # Cooperative pacing

        # Bybit returns descending; sort strictly ascending
        all_raw_items.sort(key=lambda x: int(x["fundingRateTimestamp"]))
        return all_raw_items
    finally:
        if should_close:
            client.close()


def ingest_bybit_funding_rate(
    symbol: str,
    root: Path,
    *,
    start_time_ms: int | None = None,
    end_time_ms: int | None = None,
    client: httpx.Client | None = None,
    min_disk_free_gb: float = 20.0,
) -> dict[str, Any]:
    """End-to-end Bybit Linear Funding Rate ingestion, normalization, Parquet persistence, and manifest."""
    free_gb = disk_free_bytes(root) / (1024**3)
    if free_gb < min_disk_free_gb:
        raise OSError(f"Disk space below threshold: {free_gb:.2f} GB < {min_disk_free_gb} GB")

    ident = funding_identity(symbol)
    retrieved_at = utc_now()

    # 1. Fetch metadata snapshot (point-in-time)
    try:
        _ = fetch_bybit_instruments_info_snapshot(symbol, root, client=client)
    except Exception as exc:
        logger.warning(f"Could not fetch Bybit instruments-info snapshot for {symbol}: {exc}")

    # 2. Fetch raw history via backwards traversal
    raw_items = fetch_bybit_funding_history(
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
        rec = parse_bybit_funding_rate_item(item, ident, prev_funding_time=prev_time)
        normalized_records.append(rec)
        prev_time = rec.funding_time

    # 4. Data Quality validation
    dq_issues = validate_funding_records_dq(normalized_records)
    if dq_issues:
        raise ValueError(f"Bybit Funding DQ validation failed: {dq_issues[:5]}")

    # 5. Persist raw JSONL
    min_ts_iso = normalized_records[0].funding_time.strftime("%Y%m%dT%H%M%SZ")
    max_ts_iso = normalized_records[-1].funding_time.strftime("%Y%m%dT%H%M%SZ")
    raw_dir = root / "raw" / "bybit" / "perpetual" / "funding_rate" / symbol
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
        / "exchange=bybit"
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
    manifest_file = manifest_dir / "bybit_linear_funding_rate.jsonl"

    retrieved_iso = retrieved_at.isoformat()
    manifest_record = {
        "action": "NORMALIZED",
        "exchange": "bybit",
        "market_type": "perpetual",
        "venue_product_type": "linear",
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
    chk_file = chk_dir / f"bybit_linear_funding_rate_{symbol}.json"
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
