"""Binance USD-M daily individual-trade archives; never aliases Spot/aggTrade semantics."""

from __future__ import annotations

import csv
import hashlib
import json
import os
import time
import zipfile
from datetime import date, datetime
from decimal import Decimal
from pathlib import Path
from typing import Any

import httpx
import pyarrow as pa
import pyarrow.parquet as pq

from ...identity import InstrumentIdentity
from ...paths import disk_free_bytes
from ...time import parse_epoch, utc_now
from .spot_trades import INDIVIDUAL_TRADE_SCHEMA, peak_rss_bytes

DATASET_ID = "binance.usdm.individual_trade.archive"
CONTRACT_ID = "binance.usdm.archive.individual-trade.6col-ms.v1"
FIELDS = ("id", "price", "qty", "quote_qty", "time", "is_buyer_maker")
BASE = "https://data.binance.vision/data/futures/um/daily/trades"


def identity(symbol: str) -> InstrumentIdentity:
    if symbol not in {"BTCUSDT", "ETHUSDT"}:
        raise ValueError("USD-M pilot permits BTCUSDT/ETHUSDT only")
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


def archive_url(symbol: str, day: date) -> str:
    return f"{BASE}/{symbol}/{symbol}-trades-{day}.zip"


def sha(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as f:
        for b in iter(lambda: f.read(1024 * 1024), b""):
            h.update(b)
    return h.hexdigest()


def fetch(symbol: str, day: date, destination: Path) -> str:
    destination.parent.mkdir(parents=True, exist_ok=True)
    url = archive_url(symbol, day)
    with httpx.Client(timeout=180, follow_redirects=True) as c:
        check = c.get(url + ".CHECKSUM")
        check.raise_for_status()
        expected = check.text.split()[0].lower()
        partial = destination.with_suffix(".zip.partial")
        h = hashlib.sha256()
        with c.stream("GET", url) as response, partial.open("wb") as out:
            response.raise_for_status()
            for chunk in response.iter_bytes(1024 * 1024):
                h.update(chunk)
                out.write(chunk)
            out.flush()
            os.fsync(out.fileno())
        if h.hexdigest() != expected:
            raise ValueError("USD-M archive checksum mismatch")
        os.replace(partial, destination)
        return expected


def normalize(
    row: dict[str, str],
    *,
    ordinal: int,
    ident: InstrumentIdentity,
    day: date,
    raw_ref: str,
    raw_hash: str,
    retrieved_at: datetime,
) -> dict[str, Any]:
    if tuple(row) != FIELDS:
        raise ValueError("USD-M six-field source contract mismatch")
    event = parse_epoch(int(row["time"]), unit="ms")
    if event.date() != day:
        raise ValueError("USD-M event outside requested UTC date")
    maker = row["is_buyer_maker"].lower()
    if maker not in {"true", "false"}:
        raise ValueError("invalid USD-M is_buyer_maker")
    buyer_maker = maker == "true"
    side = "SELL" if buyer_maker else "BUY"
    qty = Decimal(row["qty"])
    return {
        "instrument_id": ident.instrument_id,
        "exchange": "binance",
        "market_type": "perpetual",
        "contract_type": "linear_perpetual",
        "native_symbol": ident.native_symbol,
        "dataset_class": "individual_trade",
        "source_dataset_id": DATASET_ID,
        "native_trade_id": str(int(row["id"])),
        "source_ordinal": ordinal,
        "event_time": event,
        "exchange_timestamp": event,
        "source_timestamp": int(row["time"]),
        "source_timestamp_unit": "ms",
        "price": Decimal(row["price"]),
        "quantity": qty,
        "quantity_unit": ident.base_asset,
        "quote_quantity": Decimal(row["quote_qty"]),
        "notional_unit": "USDT",
        "taker_side": side,
        "signed_quantity": qty if side == "BUY" else -qty,
        "is_buyer_maker": buyer_maker,
        "is_best_match": False,
        "is_block_trade": None,
        "is_rpi_trade": None,
        "received_at": retrieved_at,
        "processed_at": utc_now(),
        "knowledge_time": None,
        "knowledge_time_basis": "unknown_historical_retrieval_only",
        "source_uri": archive_url(ident.native_symbol, day),
        "raw_object_ref": raw_ref,
        "source_object_sha256": raw_hash,
        "schema_version": "1.0.0",
        "collector_version": "0.4.0",
        "normalization_version": "1.0.0",
        "data_contract_version": CONTRACT_ID,
        "classification_version": "binance.usdm.isBuyerMaker.v1",
        "dq_flags": [],
    }


def commit(
    root: Path,
    *,
    symbol: str,
    day: date,
    archive: Path,
    expected_hash: str,
    retrieved_at: datetime | None = None,
) -> dict[str, Any]:
    ident = identity(symbol)
    retrieved_at = retrieved_at or utc_now()
    if sha(archive) != expected_hash:
        raise ValueError("USD-M checksum mismatch before persistence")
    if disk_free_bytes(root) - archive.stat().st_size - 64 * 1024**2 < 50 * 1024**3:
        raise RuntimeError("USD-M disk gate failed")
    raw = (
        root
        / "raw"
        / "binance"
        / "perpetual"
        / "individual_trade_archive"
        / symbol
        / f"{day}-{expected_hash}.zip"
    )
    raw.parent.mkdir(parents=True, exist_ok=True)
    if not raw.exists():
        os.replace(archive, raw)
    started = time.perf_counter()
    output = (
        root
        / "normalized"
        / "individual_trade"
        / "v1"
        / "exchange=binance"
        / "market_type=perpetual"
        / f"instrument_id={ident.instrument_id}"
        / f"date={day}"
        / f"part-{expected_hash[:16]}.parquet"
    )
    output.parent.mkdir(parents=True, exist_ok=True)
    count = 0
    first = None
    last = None
    last_id = None
    extracted = 0
    if not output.exists():
        partial = output.with_suffix(".parquet.partial")
        with zipfile.ZipFile(raw) as z:
            name = f"{symbol}-trades-{day}.csv"
            if z.namelist() != [name]:
                raise ValueError("USD-M exact member mismatch")
            extracted = z.getinfo(name).file_size
            with (
                z.open(name) as source,
                pq.ParquetWriter(partial, INDIVIDUAL_TRADE_SCHEMA, compression="zstd") as writer,
            ):
                reader = csv.DictReader(__import__("io").TextIOWrapper(source, encoding="utf-8"))
                if tuple(reader.fieldnames or ()) != FIELDS:
                    raise ValueError("USD-M observed header mismatch")
                batch = []
                for ordinal, row in enumerate(reader):
                    item = normalize(
                        row,
                        ordinal=ordinal,
                        ident=ident,
                        day=day,
                        raw_ref=str(raw.relative_to(root)),
                        raw_hash=expected_hash,
                        retrieved_at=retrieved_at,
                    )
                    tid = int(item["native_trade_id"])
                    if last_id is not None and tid <= last_id:
                        raise ValueError("USD-M IDs not strictly increasing")
                    last_id = tid
                    first = first or item["event_time"]
                    last = item["event_time"]
                    count += 1
                    batch.append(item)
                    if len(batch) == 25000:
                        writer.write_table(
                            pa.Table.from_pylist(batch, schema=INDIVIDUAL_TRADE_SCHEMA)
                        )
                        batch = []
                if batch:
                    writer.write_table(pa.Table.from_pylist(batch, schema=INDIVIDUAL_TRADE_SCHEMA))
        with partial.open("r+b") as f:
            f.flush()
            os.fsync(f.fileno())
        os.replace(partial, output)
    event = {
        "object_id": str(output.relative_to(root)),
        "parquet_sha256": sha(output),
        "raw_sha256": expected_hash,
        "raw_object_ref": str(raw.relative_to(root)),
        "source_dataset_id": DATASET_ID,
        "exchange": "binance",
        "market_type": "perpetual",
        "contract_type": "linear_perpetual",
        "instrument_id": ident.instrument_id,
        "row_count": count,
        "coverage_start": first.isoformat(),
        "coverage_end": last.isoformat(),
        "source_contract_version": CONTRACT_ID,
        "archive_bytes": raw.stat().st_size,
        "extracted_bytes": extracted,
        "parquet_bytes": output.stat().st_size,
        "runtime_seconds": time.perf_counter() - started,
        "peak_rss_bytes": peak_rss_bytes(),
    }
    manifest = root / "control" / "manifests" / "binance_usdm_individual_trade.jsonl"
    manifest.parent.mkdir(parents=True, exist_ok=True)
    with manifest.open("a", encoding="utf-8") as f:
        f.write(json.dumps(event, sort_keys=True) + "\n")
        f.flush()
        os.fsync(f.fileno())
    return event
