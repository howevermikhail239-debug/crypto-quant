"""Binance USD-M Open Interest Ingestion and Historical Normalization (Phase 1D.2A).

Fetches historical Open Interest from /futures/data/openInterestHist and point snapshot from /fapi/v1/openInterest.
Enforces:
- Canonical identity: market_type='perpetual', contract_type='linear_perpetual', venue_product_type='usdm'
- Natural key: (exchange, instrument_id, period, observation_time)
- Decimal preservation: raw string decimal for sumOpenInterest (base asset) and sumOpenInterestValue (USDT notional)
- Semantics: oi_semantic='SUM_TOTAL_BASE_AND_NOTIONAL', single_side_oi_base=None (Binance does not provide single-side)
- Granularity: primary 5m (300s baseline), supports 15m, 30m, 1h, 2h, 4h, 6h, 12h, 1d
- 30-Day Window: official REST history is limited to latest 30 days; continuous accumulation enables long-term history
- Conservative knowledge_time: None (UNKNOWN) for historical bootstrap to prevent look-ahead
- Immutable Parquet storage, manifests, checkpoints, and data quality checks
"""

from __future__ import annotations

import json
import logging
import os
import time
from dataclasses import dataclass
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
from .funding import funding_identity

logger = logging.getLogger(__name__)

DATASET_ID_HIST = "binance.usdm.open_interest_hist.rest"
CONTRACT_ID_HIST = "binance.usdm.rest.open-interest-hist.v1"
DATASET_ID_CURR = "binance.usdm.open_interest_current.rest"
CONTRACT_ID_CURR = "binance.usdm.rest.open-interest-current.v1"
COLLECTOR_VERSION = "0.4.0"
NORMALIZATION_VERSION = "1.0.0"
SCHEMA_VERSION = "1.0.0"

BINANCE_FAPI_BASE = "https://fapi.binance.com"
ALLOWED_PERIODS = {"5m", "15m", "30m", "1h", "2h", "4h", "6h", "12h", "1d"}

CANONICAL_OI_SCHEMA = pa.schema([
    ("exchange", pa.string()),
    ("instrument_id", pa.string()),
    ("symbol", pa.string()),
    ("market_type", pa.string()),
    ("contract_type", pa.string()),
    ("venue_product_type", pa.string()),
    ("period", pa.string()),
    ("observation_time", pa.timestamp("us", tz="UTC")),
    ("oi_base", pa.string()),  # Decimal string of total base asset open interest
    ("oi_notional", pa.string()),  # Decimal string of total USDT notional or None
    ("single_side_oi_base", pa.string()),  # Decimal string of single-side open interest or None
    ("oi_semantic", pa.string()),  # "SUM_TOTAL_BASE_AND_NOTIONAL" / "SUM_BOTH_SIDES_BASE"
    ("event_time", pa.timestamp("us", tz="UTC")),
    ("knowledge_time", pa.timestamp("us", tz="UTC")),  # None / UNKNOWN for historical bootstrap
    ("source", pa.string()),
    ("source_contract_version", pa.string()),
    ("schema_version", pa.string()),
    ("collector_version", pa.string()),
    ("normalization_version", pa.string()),
])


@dataclass(frozen=True)
class CanonicalOpenInterestRecord:
    exchange: str
    instrument_id: str
    symbol: str
    market_type: str
    contract_type: str
    venue_product_type: str
    period: str
    observation_time: datetime
    oi_base: str
    oi_notional: str | None
    single_side_oi_base: str | None
    oi_semantic: str
    event_time: datetime
    knowledge_time: datetime | None
    source: str
    source_contract_version: str
    schema_version: str
    collector_version: str
    normalization_version: str


def parse_binance_open_interest_item(
    raw: dict[str, Any],
    ident: InstrumentIdentity,
    period: str,
) -> CanonicalOpenInterestRecord:
    """Parses a single raw item from Binance /futures/data/openInterestHist."""
    sym = str(raw.get("symbol", ""))
    if sym != ident.native_symbol:
        raise ValueError(f"Symbol mismatch: expected {ident.native_symbol}, got {sym}")

    raw_time = raw.get("timestamp")
    if raw_time is None:
        raise ValueError("Missing mandatory field 'timestamp'")
    obs_time = parse_epoch(int(raw_time), unit="ms")

    raw_sum_oi = raw.get("sumOpenInterest")
    if raw_sum_oi is None or str(raw_sum_oi).strip() == "":
        raise ValueError("Missing mandatory field 'sumOpenInterest'")
    oi_base_dec = Decimal(str(raw_sum_oi).strip())

    raw_notional = raw.get("sumOpenInterestValue")
    oi_notional_str = str(Decimal(str(raw_notional).strip())) if raw_notional is not None and str(raw_notional).strip() != "" else None

    return CanonicalOpenInterestRecord(
        exchange="binance",
        instrument_id=ident.instrument_id,
        symbol=ident.native_symbol,
        market_type="perpetual",
        contract_type="linear_perpetual",
        venue_product_type="usdm",
        period=period,
        observation_time=obs_time,
        oi_base=str(oi_base_dec),
        oi_notional=oi_notional_str,
        single_side_oi_base=None,  # Binance does not provide single-side explicitly
        oi_semantic="SUM_TOTAL_BASE_AND_NOTIONAL",
        event_time=obs_time,
        knowledge_time=None,  # UNKNOWN for historical bootstrap
        source=DATASET_ID_HIST,
        source_contract_version=CONTRACT_ID_HIST,
        schema_version=SCHEMA_VERSION,
        collector_version=COLLECTOR_VERSION,
        normalization_version=NORMALIZATION_VERSION,
    )


def records_to_pyarrow_oi_table(records: list[CanonicalOpenInterestRecord]) -> pa.Table:
    """Converts normalized open interest records to PyArrow Table conforming to CANONICAL_OI_SCHEMA."""
    cols: dict[str, list[Any]] = {field.name: [] for field in CANONICAL_OI_SCHEMA}
    for r in records:
        cols["exchange"].append(r.exchange)
        cols["instrument_id"].append(r.instrument_id)
        cols["symbol"].append(r.symbol)
        cols["market_type"].append(r.market_type)
        cols["contract_type"].append(r.contract_type)
        cols["venue_product_type"].append(r.venue_product_type)
        cols["period"].append(r.period)
        cols["observation_time"].append(r.observation_time)
        cols["oi_base"].append(r.oi_base)
        cols["oi_notional"].append(r.oi_notional)
        cols["single_side_oi_base"].append(r.single_side_oi_base)
        cols["oi_semantic"].append(r.oi_semantic)
        cols["event_time"].append(r.event_time)
        cols["knowledge_time"].append(r.knowledge_time)
        cols["source"].append(r.source)
        cols["source_contract_version"].append(r.source_contract_version)
        cols["schema_version"].append(r.schema_version)
        cols["collector_version"].append(r.collector_version)
        cols["normalization_version"].append(r.normalization_version)

    return pa.Table.from_pydict(cols, schema=CANONICAL_OI_SCHEMA)


def validate_open_interest_records_dq(records: list[CanonicalOpenInterestRecord]) -> list[str]:
    """Performs Data Quality checks on normalized open interest records."""
    issues: list[str] = []
    seen_keys: set[tuple[str, str, str, datetime]] = set()
    prev_time: datetime | None = None

    for idx, r in enumerate(records):
        # 1. Natural key uniqueness
        key = (r.exchange, r.instrument_id, r.period, r.observation_time)
        if key in seen_keys:
            issues.append(f"Row {idx}: Duplicate natural key {key}")
        seen_keys.add(key)

        # 2. Strict monotonic ascending ordering
        if prev_time is not None and r.observation_time <= prev_time:
            issues.append(f"Row {idx}: Non-monotonic timestamp {r.observation_time} <= {prev_time}")
        prev_time = r.observation_time

        # 3. Positive open interest
        try:
            val = Decimal(r.oi_base)
            if val < 0:
                issues.append(f"Row {idx}: Negative oi_base {r.oi_base}")
        except Exception:
            issues.append(f"Row {idx}: Non-numeric oi_base {r.oi_base}")

        if r.oi_notional is not None:
            try:
                notional_val = Decimal(r.oi_notional)
                if notional_val < 0:
                    issues.append(f"Row {idx}: Negative oi_notional {r.oi_notional}")
            except Exception:
                issues.append(f"Row {idx}: Non-numeric oi_notional {r.oi_notional}")

    return issues


def fetch_binance_open_interest_current(
    symbol: str,
    root: Path,
    client: httpx.Client | None = None,
) -> dict[str, Any]:
    """Fetches point-in-time snapshot from GET /fapi/v1/openInterest."""
    url = f"{BINANCE_FAPI_BASE}/fapi/v1/openInterest"
    params = {"symbol": symbol}
    should_close = False
    if client is None:
        client = httpx.Client(timeout=30)
        should_close = True

    try:
        resp = client.get(url, params=params)
        resp.raise_for_status()
        data = resp.json()
        now = utc_now()
        retrieved_iso = now.isoformat()

        target_dir = root / "control" / "instrument_metadata"
        target_dir.mkdir(parents=True, exist_ok=True)
        ts_slug = now.strftime("%Y%m%dT%H%M%SZ")
        snapshot_file = target_dir / f"binance_usdm_open_interest_current_{symbol}_{ts_slug}.json"

        payload = {
            "dataset_id": DATASET_ID_CURR,
            "contract_id": CONTRACT_ID_CURR,
            "retrieved_at": retrieved_iso,
            "symbol": symbol,
            "data": data,
        }
        snapshot_file.write_text(json.dumps(payload, indent=2), encoding="utf-8")
        return data
    finally:
        if should_close:
            client.close()


def fetch_binance_open_interest_history(
    symbol: str,
    *,
    period: str = "5m",
    start_time_ms: int | None = None,
    end_time_ms: int | None = None,
    client: httpx.Client | None = None,
    limit: int = 500,
) -> list[dict[str, Any]]:
    """Traverses Binance GET /futures/data/openInterestHist backwards via endTime, returning sorted ascending records."""
    if period not in ALLOWED_PERIODS:
        raise ValueError(f"Invalid period '{period}'. Must be one of {ALLOWED_PERIODS}")

    url = f"{BINANCE_FAPI_BASE}/futures/data/openInterestHist"
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
                "symbol": symbol,
                "period": period,
                "limit": limit,
            }
            if current_end is not None:
                params["endTime"] = current_end

            resp = client.get(url, params=params)
            if resp.status_code == 400:
                err_data = resp.json()
                logger.warning(f"Binance openInterestHist 400 response: {err_data}")
                break
            resp.raise_for_status()
            items: list[dict[str, Any]] = resp.json()
            if not items or not isinstance(items, list):
                break

            new_in_batch = 0
            for item in items:
                ts = int(item["timestamp"])
                if start_time_ms is not None and ts < start_time_ms:
                    continue
                if ts not in seen_timestamps:
                    seen_timestamps.add(ts)
                    all_raw_items.append(item)
                    new_in_batch += 1

            if len(items) < limit or new_in_batch == 0:
                break

            # Binance returns chronological order within batch; items[0] is oldest
            oldest_ts = int(items[0]["timestamp"])
            if start_time_ms is not None and oldest_ts <= start_time_ms:
                break

            current_end = oldest_ts - 1
            time.sleep(0.05)  # Cooperative pacing

        # Guarantee strict ascending order
        all_raw_items.sort(key=lambda x: int(x["timestamp"]))
        return all_raw_items
    finally:
        if should_close:
            client.close()



def ingest_binance_open_interest(
    symbol: str,
    root: Path,
    *,
    period: str = "5m",
    start_time_ms: int | None = None,
    end_time_ms: int | None = None,
    client: httpx.Client | None = None,
    min_disk_free_gb: float = 20.0,
) -> dict[str, Any]:
    """End-to-end Binance USD-M Open Interest ingestion, normalization, Parquet persistence, and manifest."""
    free_gb = disk_free_bytes(root) / (1024**3)
    if free_gb < min_disk_free_gb:
        raise OSError(f"Disk space below threshold: {free_gb:.2f} GB < {min_disk_free_gb} GB")

    ident = funding_identity(symbol)
    retrieved_at = utc_now()

    # 1. Fetch current point snapshot (best-effort)
    try:
        _ = fetch_binance_open_interest_current(symbol, root, client=client)
    except Exception as exc:
        logger.warning(f"Could not fetch Binance current OI snapshot for {symbol}: {exc}")

    # 2. Fetch raw history via sliding window pagination
    raw_items = fetch_binance_open_interest_history(
        symbol, period=period, start_time_ms=start_time_ms, end_time_ms=end_time_ms, client=client
    )
    if not raw_items:
        return {
            "symbol": symbol,
            "period": period,
            "status": "EMPTY",
            "records_count": 0,
            "observed_source_coverage_start": None,
            "observed_source_coverage_end": None,
        }

    # 3. Normalize records in strict ascending order
    normalized_records: list[CanonicalOpenInterestRecord] = [
        parse_binance_open_interest_item(item, ident, period=period) for item in raw_items
    ]

    # 4. Data Quality validation
    dq_issues = validate_open_interest_records_dq(normalized_records)
    if dq_issues:
        raise ValueError(f"Binance Open Interest DQ validation failed: {dq_issues[:5]}")

    # 5. Persist raw JSONL
    min_ts_iso = normalized_records[0].observation_time.strftime("%Y%m%dT%H%M%SZ")
    max_ts_iso = normalized_records[-1].observation_time.strftime("%Y%m%dT%H%M%SZ")
    raw_dir = root / "raw" / "binance" / "perpetual" / "open_interest" / symbol / period
    raw_dir.mkdir(parents=True, exist_ok=True)
    raw_file = raw_dir / f"oi_{min_ts_iso}_{max_ts_iso}.jsonl"
    with raw_file.open("w", encoding="utf-8") as f:
        for item in raw_items:
            f.write(json.dumps(item) + "\n")
    raw_hash = sha256_text(raw_file.read_text(encoding="utf-8"))

    # 6. Group by Year and Persist Canonical Parquet
    norm_base = (
        root
        / "normalized"
        / "open_interest"
        / "v1"
        / "exchange=binance"
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
    for yr, yr_records in sorted(records_by_year.items()):
        yr_table = records_to_pyarrow_oi_table(yr_records)
        yr_dir = norm_base / f"year={yr}"
        yr_dir.mkdir(parents=True, exist_ok=True)
        target_parquet = yr_dir / f"part-{symbol.lower()}_{period}_{yr}.parquet"
        partial_parquet = target_parquet.with_suffix(".parquet.partial")

        pq.write_table(yr_table, partial_parquet, compression="zstd", flavor="spark")
        os.replace(partial_parquet, target_parquet)
        created_parquet_files.append(target_parquet)

    # 7. Record Manifest
    manifest_dir = root / "control" / "manifests"
    manifest_dir.mkdir(parents=True, exist_ok=True)
    manifest_file = manifest_dir / "binance_usdm_open_interest.jsonl"

    retrieved_iso = retrieved_at.isoformat()
    manifest_record = {
        "action": "NORMALIZED",
        "exchange": "binance",
        "market_type": "perpetual",
        "venue_product_type": "usdm",
        "symbol": symbol,
        "instrument_id": ident.instrument_id,
        "dataset_class": "open_interest",
        "period": period,
        "requested_coverage_start": str(start_time_ms) if start_time_ms else "30D_WINDOW",
        "requested_coverage_end": str(end_time_ms) if end_time_ms else "LATEST",
        "observed_coverage_start": normalized_records[0].observation_time.isoformat(),
        "observed_coverage_end": normalized_records[-1].observation_time.isoformat(),
        "row_count": len(normalized_records),
        "raw_object_ref": str(raw_file.relative_to(root)),
        "raw_sha256": raw_hash,
        "created_parquets": [str(p.relative_to(root)) for p in created_parquet_files],
        "source_dataset_id": DATASET_ID_HIST,
        "source_contract_version": CONTRACT_ID_HIST,
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
    chk_file = chk_dir / f"binance_usdm_open_interest_{symbol}_{period}.json"
    chk_payload = {
        "symbol": symbol,
        "period": period,
        "last_observation_time_ms": int(normalized_records[-1].observation_time.timestamp() * 1000),
        "last_observation_time_iso": normalized_records[-1].observation_time.isoformat(),
        "observed_source_coverage_start": normalized_records[0].observation_time.isoformat(),
        "observed_source_coverage_end": normalized_records[-1].observation_time.isoformat(),
        "total_records": len(normalized_records),
        "updated_at": retrieved_iso,
    }
    chk_file.write_text(json.dumps(chk_payload, indent=2), encoding="utf-8")

    return {
        "symbol": symbol,
        "period": period,
        "status": "PASS",
        "records_count": len(normalized_records),
        "observed_source_coverage_start": normalized_records[0].observation_time.isoformat(),
        "observed_source_coverage_end": normalized_records[-1].observation_time.isoformat(),
        "normalized_dataset_coverage_start": normalized_records[0].observation_time.isoformat(),
        "normalized_dataset_coverage_end": normalized_records[-1].observation_time.isoformat(),
        "years": sorted(records_by_year.keys()),
        "raw_file": str(raw_file),
        "parquet_files": [str(p) for p in created_parquet_files],
    }
