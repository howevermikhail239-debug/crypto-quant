"""Immutable Bybit Spot and Linear perpetual individual-trade archive ingestion.

This module implements Bybit daily trade archive processing for Spot and Linear perpetuals.
Data sources:
- Spot Archive: https://public.bybit.com/spot/{symbol}/{symbol}_{YYYY-MM-DD}.csv.gz
- Linear Archive: https://public.bybit.com/trading/{symbol}/{symbol}{YYYY-MM-DD}.csv.gz

Taker side semantics:
- Bybit directly provides taker side ('buy'/'Buy' -> BUY, 'sell'/'Sell' -> SELL).
- Ambiguous or unrecognized side values are mapped to UNKNOWN.
"""

from __future__ import annotations

import csv
import gzip
import hashlib
import json
import os
import time
from datetime import date
from decimal import Decimal
from pathlib import Path

import httpx
import pyarrow as pa
import pyarrow.parquet as pq

from ...identity import InstrumentIdentity
from ...paths import disk_free_bytes
from ...time import parse_epoch, utc_now
from ..binance.spot_trades import (
    INDIVIDUAL_TRADE_SCHEMA,
    PilotMeasurement,
    acquire_writer_lease,
    peak_rss_bytes,
    resource_gate,
    sha256_file,
)

BYBIT_SPOT_ARCHIVE_BASE = "https://public.bybit.com/spot"
BYBIT_LINEAR_ARCHIVE_BASE = "https://public.bybit.com/trading"

SPOT_CONTRACT_ID = "bybit.spot.archive.individual-trade.v1"
LINEAR_CONTRACT_ID = "bybit.linear.archive.individual-trade.v1"

SPOT_DATASET_ID = "bybit.spot.individual_trade.archive"
LINEAR_DATASET_ID = "bybit.linear.individual_trade.archive"

DEC = pa.decimal128(38, 18)


def bybit_spot_identity(symbol: str) -> InstrumentIdentity:
    if symbol not in {"BTCUSDT", "ETHUSDT"}:
        raise ValueError("PHASE 1C permits only BTCUSDT and ETHUSDT Bybit Spot")
    base = symbol.removesuffix("USDT")
    return InstrumentIdentity(
        exchange="bybit",
        native_symbol=symbol,
        market_type="spot",
        contract_type="spot",
        base_asset=base,
        quote_asset="USDT",
        quantity_unit=base,
        notional_unit="USDT",
    )


def bybit_linear_identity(symbol: str) -> InstrumentIdentity:
    if symbol not in {"BTCUSDT", "ETHUSDT"}:
        raise ValueError("PHASE 1C permits only BTCUSDT and ETHUSDT Bybit Linear")
    base = symbol.removesuffix("USDT")
    return InstrumentIdentity(
        exchange="bybit",
        native_symbol=symbol,
        market_type="perpetual",
        contract_type="linear_perpetual",
        base_asset=base,
        quote_asset="USDT",
        quantity_unit=base,
        notional_unit="USDT",
    )


def get_bybit_archive_url(market_type: str, symbol: str, date_val: date) -> str:
    date_str = date_val.strftime("%Y-%m-%d")
    if market_type == "spot":
        return f"{BYBIT_SPOT_ARCHIVE_BASE}/{symbol}/{symbol}_{date_str}.csv.gz"
    elif market_type in ("perpetual", "linear"):
        return f"{BYBIT_LINEAR_ARCHIVE_BASE}/{symbol}/{symbol}{date_str}.csv.gz"
    else:
        raise ValueError(f"Unsupported market_type for Bybit: {market_type}")


def map_bybit_taker_side(raw_side: str) -> str:
    s = (raw_side or "").strip().upper()
    if s in ("BUY", "B"):
        return "BUY"
    elif s in ("SELL", "S"):
        return "SELL"
    return "UNKNOWN"


def parse_bybit_timestamp_to_us(val_str: str, is_float_seconds: bool) -> int:
    val_str = val_str.strip()
    if is_float_seconds or "." in val_str:
        sec_float = float(val_str)
        return int(round(sec_float * 1_000_000))
    else:
        ms_int = int(val_str)
        return ms_int * 1000


def build_bybit_individual_trade_batch(
    rows: list[dict[str, str]],
    market_type: str,
    symbol: str,
    date_val: date,
    source_uri: str,
    source_sha256: str,
    collector_version: str = "1.0.0",
    normalization_version: str = "1.0.0",
    start_ordinal: int = 1,
) -> tuple[pa.RecordBatch, int]:
    if market_type == "spot":
        identity = bybit_spot_identity(symbol)
        instrument_id = identity.instrument_id
        contract_type = "spot"
        dataset_id = SPOT_DATASET_ID
        contract_id = SPOT_CONTRACT_ID
        is_float_sec = False
    else:
        identity = bybit_linear_identity(symbol)
        instrument_id = identity.instrument_id
        contract_type = "linear_perpetual"
        dataset_id = LINEAR_DATASET_ID
        contract_id = LINEAR_CONTRACT_ID
        is_float_sec = True

    now_dt = utc_now()
    now_us = int(now_dt.timestamp() * 1_000_000)

    col_instrument_id = []
    col_exchange = []
    col_market_type = []
    col_contract_type = []
    col_native_symbol = []
    col_dataset_class = []
    col_source_dataset_id = []
    col_native_trade_id = []
    col_source_ordinal = []
    col_event_time = []
    col_exchange_timestamp = []
    col_source_timestamp = []
    col_source_timestamp_unit = []
    col_price = []
    col_quantity = []
    col_quantity_unit = []
    col_quote_quantity = []
    col_notional_unit = []
    col_taker_side = []
    col_signed_quantity = []
    col_is_buyer_maker = []
    col_is_best_match = []
    col_is_block_trade = []
    col_is_rpi_trade = []
    col_received_at = []
    col_processed_at = []
    col_knowledge_time = []
    col_knowledge_time_basis = []
    col_source_uri = []
    col_raw_object_ref = []
    col_source_object_sha256 = []
    col_schema_version = []
    col_collector_version = []
    col_normalization_version = []
    col_data_contract_version = []
    col_classification_version = []
    col_dq_flags = []

    ordinal = start_ordinal

    for row in rows:
        if market_type == "spot":
            trade_id = row.get("id") or str(ordinal)
            ts_str = row.get("timestamp", "0")
            price_str = row.get("price", "0")
            qty_str = row.get("volume") or row.get("size") or "0"
            side_str = row.get("side", "")
            rpi_str = row.get("rpi")
        else:
            trade_id = row.get("trdMatchID") or row.get("id") or str(ordinal)
            ts_str = row.get("timestamp", "0")
            price_str = row.get("price", "0")
            qty_str = row.get("size") or row.get("volume") or "0"
            side_str = row.get("side", "")
            rpi_str = row.get("RPI") or row.get("rpi")

        event_us = parse_bybit_timestamp_to_us(ts_str, is_float_sec)
        price_dec = Decimal(price_str)
        qty_dec = Decimal(qty_str)
        quote_qty_dec = price_dec * qty_dec

        taker_side = map_bybit_taker_side(side_str)
        if taker_side == "BUY":
            signed_qty = qty_dec
            is_buyer_maker = False
        elif taker_side == "SELL":
            signed_qty = -qty_dec
            is_buyer_maker = True
        else:
            signed_qty = Decimal(0)
            is_buyer_maker = False

        rpi_flag = None
        if rpi_str is not None:
            rpi_flag = (rpi_str.strip() in ("1", "true", "True"))

        dq_flags_list = []
        if taker_side == "UNKNOWN":
            dq_flags_list.append("UNKNOWN_TAKER_SIDE")

        col_instrument_id.append(instrument_id)
        col_exchange.append("bybit")
        col_market_type.append(market_type if market_type == "spot" else "perpetual")
        col_contract_type.append(contract_type)
        col_native_symbol.append(symbol)
        col_dataset_class.append("individual_trade")
        col_source_dataset_id.append(dataset_id)
        col_native_trade_id.append(str(trade_id))
        col_source_ordinal.append(ordinal)
        col_event_time.append(event_us)
        col_exchange_timestamp.append(event_us)
        col_source_timestamp.append(int(float(ts_str)) if is_float_sec else int(ts_str))
        col_source_timestamp_unit.append("epoch_s_float" if is_float_sec else "epoch_ms")
        col_price.append(price_dec)
        col_quantity.append(qty_dec)
        col_quantity_unit.append(symbol.replace("USDT", ""))
        col_quote_quantity.append(quote_qty_dec)
        col_notional_unit.append("USDT")
        col_taker_side.append(taker_side)
        col_signed_quantity.append(signed_qty)
        col_is_buyer_maker.append(is_buyer_maker)
        col_is_best_match.append(False)
        col_is_block_trade.append(None)
        col_is_rpi_trade.append(rpi_flag)
        col_received_at.append(now_us)
        col_processed_at.append(now_us)
        col_knowledge_time.append(None)
        col_knowledge_time_basis.append("retrieval_time_only")
        col_source_uri.append(source_uri)
        col_raw_object_ref.append(f"{symbol}-{date_val.strftime('%Y-%m-%d')}.csv.gz")
        col_source_object_sha256.append(source_sha256)
        col_schema_version.append("1.0.0")
        col_collector_version.append(collector_version)
        col_normalization_version.append(normalization_version)
        col_data_contract_version.append(contract_id)
        col_classification_version.append("1.0.0")
        col_dq_flags.append(dq_flags_list if dq_flags_list else None)

        ordinal += 1

    rb = pa.RecordBatch.from_arrays(
        [
            pa.array(col_instrument_id, pa.string()),
            pa.array(col_exchange, pa.string()),
            pa.array(col_market_type, pa.string()),
            pa.array(col_contract_type, pa.string()),
            pa.array(col_native_symbol, pa.string()),
            pa.array(col_dataset_class, pa.string()),
            pa.array(col_source_dataset_id, pa.string()),
            pa.array(col_native_trade_id, pa.string()),
            pa.array(col_source_ordinal, pa.int64()),
            pa.array(col_event_time, pa.timestamp("us", tz="UTC")),
            pa.array(col_exchange_timestamp, pa.timestamp("us", tz="UTC")),
            pa.array(col_source_timestamp, pa.int64()),
            pa.array(col_source_timestamp_unit, pa.string()),
            pa.array(col_price, DEC),
            pa.array(col_quantity, DEC),
            pa.array(col_quantity_unit, pa.string()),
            pa.array(col_quote_quantity, DEC),
            pa.array(col_notional_unit, pa.string()),
            pa.array(col_taker_side, pa.string()),
            pa.array(col_signed_quantity, DEC),
            pa.array(col_is_buyer_maker, pa.bool_()),
            pa.array(col_is_best_match, pa.bool_()),
            pa.array(col_is_block_trade, pa.bool_()),
            pa.array(col_is_rpi_trade, pa.bool_()),
            pa.array(col_received_at, pa.timestamp("us", tz="UTC")),
            pa.array(col_processed_at, pa.timestamp("us", tz="UTC")),
            pa.array(col_knowledge_time, pa.timestamp("us", tz="UTC")),
            pa.array(col_knowledge_time_basis, pa.string()),
            pa.array(col_source_uri, pa.string()),
            pa.array(col_raw_object_ref, pa.string()),
            pa.array(col_source_object_sha256, pa.string()),
            pa.array(col_schema_version, pa.string()),
            pa.array(col_collector_version, pa.string()),
            pa.array(col_normalization_version, pa.string()),
            pa.array(col_data_contract_version, pa.string()),
            pa.array(col_classification_version, pa.string()),
            pa.array(col_dq_flags, pa.list_(pa.string())),
        ],
        schema=INDIVIDUAL_TRADE_SCHEMA,
    )
    return rb, ordinal


def fetch_bybit_archive(
    market_type: str, symbol: str, trading_date: date, destination: Path
) -> str:
    """Stream one Bybit CSV.GZ archive to an atomic file and compute its SHA-256 digest."""
    destination.parent.mkdir(parents=True, exist_ok=True)
    url = get_bybit_archive_url(market_type, symbol, trading_date)
    partial = destination.with_suffix(destination.suffix + ".partial")
    digest = hashlib.sha256()

    headers = {"User-Agent": "Mozilla/5.0"}
    with httpx.Client(timeout=120, follow_redirects=True, headers=headers) as client:
        with client.stream("GET", url) as response, partial.open("wb") as target:
            response.raise_for_status()
            for chunk in response.iter_bytes(1024 * 1024):
                digest.update(chunk)
                target.write(chunk)
            target.flush()
            os.fsync(target.fileno())

    sha256_val = digest.hexdigest()
    os.replace(partial, destination)
    return sha256_val


def commit_bybit_trade_day(
    root: Path,
    market_type: str,
    symbol: str,
    trading_date: date,
    *,
    chunk_size: int = 100_000,
) -> PilotMeasurement:
    """Download, parse, normalize and commit one day of Bybit individual trade archive."""
    if not resource_gate(root, estimated_download_bytes=100 * 1024 * 1024):
        raise RuntimeError("disk space gate rejected Bybit trade download")

    started = time.perf_counter()
    before = disk_free_bytes(root)
    lease = acquire_writer_lease(
        root, lease_id=f"bybit_{market_type}_{symbol}_pid={os.getpid()}"
    )

    try:
        raw_dir = root / "raw" / "bybit" / (market_type if market_type == "spot" else "perpetual") / "individual_trade_archive" / symbol
        filename = f"{symbol}_{trading_date.strftime('%Y-%m-%d')}.csv.gz" if market_type == "spot" else f"{symbol}{trading_date.strftime('%Y-%m-%d')}.csv.gz"
        raw_file = raw_dir / filename

        if not raw_file.exists():
            raw_hash = fetch_bybit_archive(market_type, symbol, trading_date, raw_file)
        else:
            raw_hash = sha256_file(raw_file)

        source_url = get_bybit_archive_url(market_type, symbol, trading_date)
        identity = bybit_spot_identity(symbol) if market_type == "spot" else bybit_linear_identity(symbol)

        output_dir = (
            root
            / "normalized"
            / "individual_trade"
            / "v1"
            / "exchange=bybit"
            / f"market_type={identity.market_type}"
            / f"symbol={symbol}"
            / f"date={trading_date.isoformat()}"
        )
        output_dir.mkdir(parents=True, exist_ok=True)
        output_file = output_dir / f"bybit_{identity.market_type}_{symbol}_{trading_date.isoformat()}.parquet"
        partial_output = output_file.with_suffix(".parquet.partial")

        row_count = 0
        extracted_bytes = 0

        with gzip.open(raw_file, "rt", encoding="utf-8") as gz:
            header_line = gz.readline()
            extracted_bytes += len(header_line.encode("utf-8"))
            reader = csv.DictReader([header_line] + list(gz))

            buf = []
            current_ordinal = 1
            writer = None

            for row in reader:
                extracted_bytes += sum(len(str(v).encode("utf-8")) for v in row.values()) + len(row)
                buf.append(row)
                row_count += 1

                if len(buf) >= chunk_size:
                    rb, current_ordinal = build_bybit_individual_trade_batch(
                        buf,
                        market_type=market_type,
                        symbol=symbol,
                        date_val=trading_date,
                        source_uri=source_url,
                        source_sha256=raw_hash,
                        start_ordinal=current_ordinal,
                    )
                    buf.clear()
                    if writer is None:
                        writer = pq.ParquetWriter(partial_output, INDIVIDUAL_TRADE_SCHEMA, compression="zstd")
                    writer.write_batch(rb)

            if buf:
                rb, current_ordinal = build_bybit_individual_trade_batch(
                    buf,
                    market_type=market_type,
                    symbol=symbol,
                    date_val=trading_date,
                    source_uri=source_url,
                    source_sha256=raw_hash,
                    start_ordinal=current_ordinal,
                )
                buf.clear()
                if writer is None:
                    writer = pq.ParquetWriter(partial_output, INDIVIDUAL_TRADE_SCHEMA, compression="zstd")
                writer.write_batch(rb)

            if writer is not None:
                writer.close()

        os.replace(partial_output, output_file)

        pf = pq.ParquetFile(output_file)
        meta = pf.metadata
        first_row_group = pf.read_row_group(0, columns=["event_time"])
        last_row_group = pf.read_row_group(meta.num_row_groups - 1, columns=["event_time", "native_trade_id"])

        first_ts = first_row_group.column("event_time")[0].value
        last_ts = last_row_group.column("event_time")[-1].value
        first_event_time = parse_epoch(first_ts, unit="us")
        last_event_time = parse_epoch(last_ts, unit="us")
        final_trade_id = last_row_group.column("native_trade_id")[-1].as_py()

        parquet_hash = sha256_file(output_file)
        manifest_name = "bybit_spot_individual_trade.jsonl" if market_type == "spot" else "bybit_linear_individual_trade.jsonl"
        manifest_file = root / "control" / "manifests" / manifest_name
        manifest_file.parent.mkdir(parents=True, exist_ok=True)

        retrieved_at = utc_now()
        manifest_record = {
            "action": "NORMALIZED",
            "object_id": str(output_file.relative_to(root)).replace("\\", "/"),
            "parquet_sha256": parquet_hash,
            "raw_sha256": raw_hash,
            "external_checksum_sha256": raw_hash,
            "local_raw_sha256": raw_hash,
            "checksum_sidecar_uri": None,
            "raw_object_ref": str(raw_file.relative_to(root)).replace("\\", "/"),
            "source_uri": source_url,
            "source_kind": "daily_archive",
            "source_dataset_id": SPOT_DATASET_ID if market_type == "spot" else LINEAR_DATASET_ID,
            "dataset_class": "individual_trade",
            "exchange": "bybit",
            "market_type": identity.market_type,
            "contract_type": identity.contract_type,
            "instrument_id": identity.instrument_id,
            "coverage_start": first_event_time.isoformat(),
            "coverage_end": last_event_time.isoformat(),
            "row_count": row_count,
            "raw_bytes": raw_file.stat().st_size,
            "extracted_bytes": extracted_bytes,
            "parquet_bytes": output_file.stat().st_size,
            "retrieved_at": retrieved_at.isoformat(),
            "processed_at": retrieved_at.isoformat(),
            "schema_version": "1.0.0",
            "collector_version": "1.0.0",
            "normalization_version": "1.0.0",
            "source_contract_version": SPOT_CONTRACT_ID if market_type == "spot" else LINEAR_CONTRACT_ID,
            "known_limitations": [
                "historical knowledge_time unknown; archive retrieval time is not market availability"
            ],
        }

        with manifest_file.open("a", encoding="utf-8") as mf:
            mf.write(json.dumps(manifest_record) + "\n")
            mf.flush()
            os.fsync(mf.fileno())

        checkpoint_file = (
            root / "control" / "checkpoints" / f"bybit_{identity.market_type}_{symbol.lower()}_individual_trade.json"
        )
        checkpoint_file.parent.mkdir(parents=True, exist_ok=True)
        chk_data = {
            "source_dataset_id": SPOT_DATASET_ID if market_type == "spot" else LINEAR_DATASET_ID,
            "instrument_id": identity.instrument_id,
            "cursor": str(final_trade_id),
            "last_event_time": last_event_time.isoformat(),
            "last_knowledge_time": None,
            "committed_at": retrieved_at.isoformat(),
        }
        with checkpoint_file.open("w", encoding="utf-8") as cf:
            cf.write(json.dumps(chk_data, sort_keys=True, indent=2))

        after = disk_free_bytes(root)
        return PilotMeasurement(
            trading_date.isoformat(),
            row_count,
            raw_file.stat().st_size,
            extracted_bytes,
            output_file.stat().st_size,
            time.perf_counter() - started,
            raw_hash,
            before,
            after,
            resource_gate(root, estimated_download_bytes=raw_file.stat().st_size),
            peak_rss_bytes(),
        )

    finally:
        lease.unlink(missing_ok=True)
