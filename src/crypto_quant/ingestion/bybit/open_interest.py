"""Bybit Linear Open Interest Ingestion and Historical Normalization (Phase 1D.2B).

Fetches historical Open Interest from /v5/market/open-interest.
Enforces:
- Canonical identity: market_type='perpetual', contract_type='linear_perpetual', venue_product_type='linear'
- Natural key: (exchange, instrument_id, period, observation_time)
- Decimal preservation: raw string decimal for openInterest (both sides) and singleOpenInterest (single side)
- Provenance integrity: Bybit does NOT provide notional value in REST history -> oi_notional=None (no silent enrichment)
- Semantics: oi_semantic='SUM_BOTH_SIDES_BASE', single_side_oi_base preserved
- Granularity: primary 5m ('5min' API intervalTime -> canonical period '5m')
- Pagination: cursor-based via nextPageCursor until exhausted, sorted strictly ascending
- Conservative knowledge_time: None (UNKNOWN) for historical bootstrap to prevent look-ahead
- Immutable Parquet storage, manifests, checkpoints, and data quality checks
"""

from __future__ import annotations

import json
import logging
import time
from decimal import Decimal
from pathlib import Path
from typing import Any

import httpx

from ...hashing import sha256_text
from ...identity import InstrumentIdentity
from ...paths import disk_free_bytes
from ...time import parse_epoch, utc_now
from ..binance.funding import funding_identity
from ..binance.open_interest import (
    CanonicalOpenInterestRecord,
    validate_open_interest_records_dq,
)

logger = logging.getLogger(__name__)

DATASET_ID = "bybit.linear.open_interest.rest"
CONTRACT_ID = "bybit.linear.rest.open-interest.v1"
COLLECTOR_VERSION = "0.4.0"
NORMALIZATION_VERSION = "1.0.0"
SCHEMA_VERSION = "1.0.0"

BYBIT_API_BASE = "https://api.bybit.com"

INTERVAL_TO_PERIOD_MAP = {
    "5min": "5m",
    "15min": "15m",
    "30min": "30m",
    "1h": "1h",
    "4h": "4h",
    "1d": "1d",
}
PERIOD_TO_INTERVAL_MAP = {v: k for k, v in INTERVAL_TO_PERIOD_MAP.items()}


def parse_bybit_open_interest_item(
    raw: dict[str, Any],
    ident: InstrumentIdentity,
    period: str,
) -> CanonicalOpenInterestRecord:
    """Parses a single raw item from Bybit GET /v5/market/open-interest."""
    raw_time = raw.get("timestamp")
    if raw_time is None:
        raise ValueError("Missing mandatory field 'timestamp'")
    obs_time = parse_epoch(int(raw_time), unit="ms")

    raw_sum_oi = raw.get("openInterest")
    if raw_sum_oi is None or str(raw_sum_oi).strip() == "":
        raise ValueError("Missing mandatory field 'openInterest'")
    oi_base_dec = Decimal(str(raw_sum_oi).strip())

    raw_single_oi = raw.get("singleOpenInterest")
    single_oi_str = str(Decimal(str(raw_single_oi).strip())) if raw_single_oi is not None and str(raw_single_oi).strip() != "" else None

    return CanonicalOpenInterestRecord(
        exchange="bybit",
        instrument_id=ident.instrument_id,
        symbol=ident.native_symbol,
        market_type="perpetual",
        contract_type="linear_perpetual",
        venue_product_type="linear",
        period=period,
        observation_time=obs_time,
        oi_base=str(oi_base_dec),
        oi_notional=None,  # Bybit history does not provide notional value; no silent conversion
        single_side_oi_base=single_oi_str,
        oi_semantic="SUM_BOTH_SIDES_BASE",
        event_time=obs_time,
        knowledge_time=None,  # UNKNOWN for historical bootstrap
        source=DATASET_ID,
        source_contract_version=CONTRACT_ID,
        schema_version=SCHEMA_VERSION,
        collector_version=COLLECTOR_VERSION,
        normalization_version=NORMALIZATION_VERSION,
    )


def fetch_bybit_open_interest_history(
    symbol: str,
    *,
    period: str = "5m",
    start_time_ms: int | None = None,
    end_time_ms: int | None = None,
    client: httpx.Client | None = None,
    limit: int = 200,
    max_pages: int | None = 1000,
) -> tuple[list[dict[str, Any]], str]:
    """Traverses Bybit GET /v5/market/open-interest via cursor until exhausted or max_pages reached.

    Returns: (records, termination_reason)
    termination_reason is one of: "CURSOR_EMPTY", "SOURCE_EMPTY", "PAGE_LIMIT_REACHED".
    """
    api_interval = PERIOD_TO_INTERVAL_MAP.get(period)
    if api_interval is None:
        raise ValueError(f"Invalid period '{period}'. Supported periods: {list(PERIOD_TO_INTERVAL_MAP.keys())}")

    url = f"{BYBIT_API_BASE}/v5/market/open-interest"
    should_close = False
    if client is None:
        client = httpx.Client(timeout=30)
        should_close = True

    all_raw_items: list[dict[str, Any]] = []
    seen_timestamps: set[int] = set()
    cursor: str | None = None
    page = 0
    termination_reason = "CURSOR_EMPTY"

    try:
        while True:
            if max_pages is not None and page >= max_pages:
                termination_reason = "PAGE_LIMIT_REACHED"
                break

            page += 1
            params: dict[str, Any] = {
                "category": "linear",
                "symbol": symbol,
                "intervalTime": api_interval,
                "limit": limit,
            }
            if cursor:
                params["cursor"] = cursor

            resp = client.get(url, params=params)
            resp.raise_for_status()
            data = resp.json()
            if data.get("retCode") != 0:
                raise ValueError(f"Bybit open-interest error {data.get('retCode')}: {data.get('retMsg')}")

            result_obj = data.get("result", {})
            items: list[dict[str, Any]] = result_obj.get("list", [])
            if not items:
                termination_reason = "SOURCE_EMPTY"
                break

            new_in_batch = 0
            for item in items:
                ts = int(item["timestamp"])
                if start_time_ms is not None and ts < start_time_ms:
                    continue
                if end_time_ms is not None and ts > end_time_ms:
                    continue
                if ts not in seen_timestamps:
                    seen_timestamps.add(ts)
                    all_raw_items.append(item)
                    new_in_batch += 1

            next_cursor = result_obj.get("nextPageCursor")
            if not next_cursor or new_in_batch == 0 or len(items) < limit:
                termination_reason = "CURSOR_EMPTY"
                break

            cursor = next_cursor
            time.sleep(0.05)  # Cooperative pacing

        # Guarantee strict ascending order
        all_raw_items.sort(key=lambda x: int(x["timestamp"]))
        return all_raw_items, termination_reason
    finally:
        if should_close:
            client.close()


def ingest_bybit_open_interest(
    symbol: str,
    root: Path,
    *,
    period: str = "5m",
    start_time_ms: int | None = None,
    end_time_ms: int | None = None,
    client: httpx.Client | None = None,
    max_pages: int | None = 1000,
    min_disk_free_gb: float = 20.0,
) -> dict[str, Any]:
    """End-to-end Bybit Linear Open Interest ingestion, normalization, Parquet persistence, and manifest."""
    free_gb = disk_free_bytes(root) / (1024**3)
    if free_gb < min_disk_free_gb:
        raise OSError(f"Disk space below threshold: {free_gb:.2f} GB < {min_disk_free_gb} GB")

    ident = funding_identity(symbol)
    retrieved_at = utc_now()

    # 1. Fetch raw history via cursor pagination
    raw_items, termination_reason = fetch_bybit_open_interest_history(
        symbol, period=period, start_time_ms=start_time_ms, end_time_ms=end_time_ms, client=client, max_pages=max_pages
    )
    if not raw_items:
        return {
            "symbol": symbol,
            "period": period,
            "status": "EMPTY",
            "records_count": 0,
            "coverage_status": "EMPTY",
            "termination_reason": termination_reason,
            "observed_source_coverage_start": None,
            "observed_source_coverage_end": None,
        }

    # 2. Normalize records in strict ascending order
    normalized_records: list[CanonicalOpenInterestRecord] = [
        parse_bybit_open_interest_item(item, ident, period=period) for item in raw_items
    ]

    # 3. Data Quality validation
    dq_issues = validate_open_interest_records_dq(normalized_records)
    if dq_issues:
        raise ValueError(f"Bybit Open Interest DQ validation failed: {dq_issues[:5]}")

    # 4. Persist raw JSONL
    min_ts_iso = normalized_records[0].observation_time.strftime("%Y%m%dT%H%M%SZ")
    max_ts_iso = normalized_records[-1].observation_time.strftime("%Y%m%dT%H%M%SZ")
    raw_dir = root / "raw" / "bybit" / "perpetual" / "open_interest" / symbol / period
    raw_dir.mkdir(parents=True, exist_ok=True)
    raw_file = raw_dir / f"oi_{min_ts_iso}_{max_ts_iso}.jsonl"
    with raw_file.open("w", encoding="utf-8") as f:
        for item in raw_items:
            f.write(json.dumps(item) + "\n")
    raw_hash = sha256_text(raw_file.read_text(encoding="utf-8"))

    # 5. Group by Year and Persist Canonical Parquet with Safe Accumulation
    norm_base = (
        root
        / "normalized"
        / "open_interest"
        / "v1"
        / "exchange=bybit"
        / "market_type=perpetual"
        / f"symbol={symbol}"
        / f"period={period}"
    )
    norm_base.mkdir(parents=True, exist_ok=True)

    records_by_year: dict[int, list[CanonicalOpenInterestRecord]] = {}
    for r in normalized_records:
        yr = r.observation_time.year
        records_by_year.setdefault(yr, []).append(r)

    created_parquet_files: list[Path] = []
    from ..binance.open_interest import merge_and_write_oi_parquet

    total_dataset_rows = 0
    for yr, yr_records in sorted(records_by_year.items()):
        yr_dir = norm_base / f"year={yr}"
        yr_dir.mkdir(parents=True, exist_ok=True)
        target_parquet = yr_dir / f"part-{symbol.lower()}_{period}_{yr}.parquet"
        partition_rows = merge_and_write_oi_parquet(target_parquet, yr_records)
        total_dataset_rows += partition_rows
        created_parquet_files.append(target_parquet)

    coverage_status = "COMPLETE" if termination_reason != "PAGE_LIMIT_REACHED" else "PARTIAL_TRUNCATED_BY_PAGE_LIMIT"

    # 6. Record Manifest
    manifest_dir = root / "control" / "manifests"
    manifest_dir.mkdir(parents=True, exist_ok=True)
    manifest_file = manifest_dir / "bybit_linear_open_interest.jsonl"

    retrieved_iso = retrieved_at.isoformat()
    manifest_record = {
        "action": "NORMALIZED",
        "exchange": "bybit",
        "market_type": "perpetual",
        "venue_product_type": "linear",
        "symbol": symbol,
        "instrument_id": ident.instrument_id,
        "dataset_class": "open_interest",
        "period": period,
        "requested_coverage_start": str(start_time_ms) if start_time_ms else "INCEPTION",
        "requested_coverage_end": str(end_time_ms) if end_time_ms else "LATEST",
        "observed_coverage_start": normalized_records[0].observation_time.isoformat(),
        "observed_coverage_end": normalized_records[-1].observation_time.isoformat(),
        "row_count": len(normalized_records),
        "total_accumulated_rows": total_dataset_rows,
        "coverage_status": coverage_status,
        "termination_reason": termination_reason,
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

    # 7. Record Checkpoint
    chk_dir = root / "control" / "checkpoints"
    chk_dir.mkdir(parents=True, exist_ok=True)
    chk_file = chk_dir / f"bybit_linear_open_interest_{symbol}_{period}.json"
    chk_payload = {
        "symbol": symbol,
        "period": period,
        "last_observation_time_ms": int(normalized_records[-1].observation_time.timestamp() * 1000),
        "last_observation_time_iso": normalized_records[-1].observation_time.isoformat(),
        "observed_source_coverage_start": normalized_records[0].observation_time.isoformat(),
        "observed_source_coverage_end": normalized_records[-1].observation_time.isoformat(),
        "batch_records": len(normalized_records),
        "total_records": total_dataset_rows,
        "coverage_status": coverage_status,
        "termination_reason": termination_reason,
        "updated_at": retrieved_iso,
    }
    chk_file.write_text(json.dumps(chk_payload, indent=2), encoding="utf-8")

    return {
        "symbol": symbol,
        "period": period,
        "status": "PASS",
        "records_count": len(normalized_records),
        "total_accumulated_rows": total_dataset_rows,
        "coverage_status": coverage_status,
        "termination_reason": termination_reason,
        "observed_source_coverage_start": normalized_records[0].observation_time.isoformat(),
        "observed_source_coverage_end": normalized_records[-1].observation_time.isoformat(),
        "normalized_dataset_coverage_start": normalized_records[0].observation_time.isoformat(),
        "normalized_dataset_coverage_end": normalized_records[-1].observation_time.isoformat(),
        "years": sorted(records_by_year.keys()),
        "raw_file": str(raw_file),
        "parquet_files": [str(p) for p in created_parquet_files],
    }
