"""Immutable, manifest-validated trade buckets derived from individual trades."""

from __future__ import annotations

import hashlib
import json
import os
import statistics
import tempfile
from dataclasses import dataclass
from datetime import UTC, date, datetime, timedelta
from decimal import ROUND_HALF_EVEN, Decimal
from pathlib import Path
from typing import Any

import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq

DEC = pa.decimal128(38, 18)
SOURCE_DATASET = "binance.spot.individual_trade.archive"


@dataclass(frozen=True)
class TradeSourceDescriptor:
    dataset_id: str
    market_type: str
    contract_type: str
    manifest_name: str
    exchange: str = "binance"


SPOT_SOURCE = TradeSourceDescriptor(
    dataset_id=SOURCE_DATASET,
    market_type="spot",
    contract_type="spot",
    manifest_name="binance_spot_individual_trade.jsonl",
    exchange="binance",
)
USDM_SOURCE = TradeSourceDescriptor(
    dataset_id="binance.usdm.individual_trade.archive",
    market_type="perpetual",
    contract_type="linear_perpetual",
    manifest_name="binance_usdm_individual_trade.jsonl",
    exchange="binance",
)
BYBIT_SPOT_SOURCE = TradeSourceDescriptor(
    dataset_id="bybit.spot.individual_trade.archive",
    market_type="spot",
    contract_type="spot",
    manifest_name="bybit_spot_individual_trade.jsonl",
    exchange="bybit",
)
BYBIT_LINEAR_SOURCE = TradeSourceDescriptor(
    dataset_id="bybit.linear.individual_trade.archive",
    market_type="perpetual",
    contract_type="linear_perpetual",
    manifest_name="bybit_linear_individual_trade.jsonl",
    exchange="bybit",
)
SCHEMA = pa.schema(
    [
        pa.field("instrument_id", pa.string(), False),
        pa.field("exchange", pa.string(), False),
        pa.field("market_type", pa.string(), False),
        pa.field("contract_type", pa.string(), False),
        pa.field("native_symbol", pa.string(), False),
        pa.field("source_dataset_id", pa.string(), False),
        pa.field("trading_date", pa.date32(), False),
        pa.field("bucket_seconds", pa.int32(), False),
        pa.field("bucket_start", pa.timestamp("us", tz="UTC"), False),
        pa.field("bucket_end", pa.timestamp("us", tz="UTC"), False),
        pa.field("trade_count", pa.int64(), False),
        pa.field("buy_count", pa.int64(), False),
        pa.field("sell_count", pa.int64(), False),
        pa.field("open", DEC, False),
        pa.field("high", DEC, False),
        pa.field("low", DEC, False),
        pa.field("close", DEC, False),
        pa.field("base_volume", DEC, False),
        pa.field("buy_base_volume", DEC, False),
        pa.field("sell_base_volume", DEC, False),
        pa.field("base_delta", DEC, False),
        pa.field("quote_volume", DEC, False),
        pa.field("buy_quote_volume", DEC, False),
        pa.field("sell_quote_volume", DEC, False),
        pa.field("quote_delta", DEC, False),
        pa.field("avg_base_size", DEC, False),
        pa.field("median_base_size", DEC, False),
        pa.field("max_base_size", DEC, False),
        pa.field("quantity_unit", pa.string(), False),
        pa.field("notional_unit", pa.string(), False),
        pa.field("aggregation_version", pa.string(), False),
        pa.field("source_parquet_sha256", pa.string(), False),
        pa.field("dq_flags", pa.list_(pa.string()), False),
    ]
)


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _source_event(
    root: Path,
    source: Path,
    instrument_id: str,
    trading_date: date,
    descriptor: TradeSourceDescriptor,
) -> dict[str, Any]:
    manifest = root / "control" / "manifests" / descriptor.manifest_name
    matches = []
    target_obj = str(source.relative_to(root)).replace("\\", "/")
    for line in manifest.read_text(encoding="utf-8").splitlines():
        if not line.strip():
            continue
        event = json.loads(line)
        if event.get("object_id", "").replace("\\", "/") == target_obj:
            matches.append(event)
    if not matches:
        raise ValueError("source must have at least one manifest event")
    event = matches[-1]
    expected = {
        "source_dataset_id": descriptor.dataset_id,
        "instrument_id": instrument_id,
        "exchange": descriptor.exchange,
        "market_type": descriptor.market_type,
        "contract_type": descriptor.contract_type,
    }
    if any(event.get(k) != v for k, v in expected.items()):
        raise ValueError("source manifest descriptor mismatch")
    if (
        sha256_file(source) != event["parquet_sha256"]
        or pq.ParquetFile(source).metadata.num_rows != event["row_count"]
    ):
        raise ValueError("source manifest hash/row_count mismatch")
    if f"date={trading_date.isoformat()}" not in str(source):
        raise ValueError("source path/date mismatch")
    return event


def _finish(
    state: dict[str, Any], seconds: int, checksum: str, trading_date: date
) -> dict[str, Any]:
    count = state["count"]
    sizes = state["sizes"]
    quantum = Decimal("0.000000000000000001")
    return {
        **{
            k: state[k]
            for k in (
                "instrument_id",
                "exchange",
                "market_type",
                "contract_type",
                "native_symbol",
                "source_dataset_id",
                "quantity_unit",
                "notional_unit",
            )
        },
        "trading_date": trading_date,
        "bucket_seconds": seconds,
        "bucket_start": state["start"],
        "bucket_end": state["start"] + timedelta(seconds=seconds),
        "trade_count": count,
        "buy_count": state["buy_count"],
        "sell_count": state["sell_count"],
        "open": state["open"],
        "high": state["high"],
        "low": state["low"],
        "close": state["close"],
        "base_volume": state["buy_base"] + state["sell_base"],
        "buy_base_volume": state["buy_base"],
        "sell_base_volume": state["sell_base"],
        "base_delta": state["buy_base"] - state["sell_base"],
        "quote_volume": state["buy_quote"] + state["sell_quote"],
        "buy_quote_volume": state["buy_quote"],
        "sell_quote_volume": state["sell_quote"],
        "quote_delta": state["buy_quote"] - state["sell_quote"],
        "avg_base_size": (sum(sizes, Decimal(0)) / count).quantize(
            quantum, rounding=ROUND_HALF_EVEN
        ),
        "median_base_size": Decimal(statistics.median(sizes)).quantize(
            quantum, rounding=ROUND_HALF_EVEN
        ),
        "max_base_size": max(sizes),
        "aggregation_version": "1.0.0",
        "source_parquet_sha256": checksum,
        "dq_flags": [],
    }


def build_buckets(
    source: Path,
    root: Path,
    seconds: int,
    *,
    instrument_id: str | None = None,
    trading_date: date | None = None,
    descriptor: TradeSourceDescriptor = SPOT_SOURCE,
) -> Path:
    if seconds not in {1, 5, 60}:
        raise ValueError("only 1s, 5s and 60s are approved")
    # Validate immutable provenance before trusting/parsing the source bytes.
    inferred_instrument = (
        instrument_id
        or next((part.split("=", 1)[1] for part in source.parts if part.startswith("instrument_id=")), None)
    )
    if not inferred_instrument:
        inferred_instrument = pq.ParquetFile(source).read_row_group(0, columns=["instrument_id"])["instrument_id"][0].as_py()
    inferred_date = trading_date or date.fromisoformat(
        next(part for part in source.parts if part.startswith("date=")).split("=", 1)[1]
    )
    event = _source_event(root, source, inferred_instrument, inferred_date, descriptor)
    pf = pq.ParquetFile(source)
    first = pf.read_row_group(0, columns=["instrument_id"])
    inferred_instrument = first["instrument_id"][0].as_py()
    instrument_id = instrument_id or inferred_instrument
    trading_date = trading_date or inferred_date
    checksum = event["parquet_sha256"]
    rows = []
    state = None
    last_key = None
    first_event_time = None
    last_event_time = None
    total = 0
    cols = [
        "instrument_id",
        "exchange",
        "market_type",
        "contract_type",
        "native_symbol",
        "source_dataset_id",
        "native_trade_id",
        "source_ordinal",
        "event_time",
        "price",
        "quantity",
        "quantity_unit",
        "quote_quantity",
        "notional_unit",
        "taker_side",
    ]
    for batch in pf.iter_batches(columns=cols, batch_size=50_000):
        event_index = batch.schema.get_field_index("event_time")
        batch = batch.set_column(
            event_index, "event_time_us", pc.cast(batch.column(event_index), pa.int64())
        )
        for item in batch.to_pylist():
            item["event_time"] = datetime.fromtimestamp(
                item.pop("event_time_us") / 1_000_000, tz=UTC
            )
            key = (item["event_time"], item["source_ordinal"], item["native_trade_id"])
            if last_key is not None and key <= last_key:
                raise ValueError("source is not in deterministic total order")
            last_key = key
            first_event_time = first_event_time or item["event_time"]
            last_event_time = item["event_time"]
            if (
                item["instrument_id"] != instrument_id
                or item["source_dataset_id"] != descriptor.dataset_id
                or item["market_type"] != descriptor.market_type
                or item["contract_type"] != descriptor.contract_type
                or item["event_time"].date() != trading_date
            ):
                raise ValueError("mixed descriptor/date source")
            if item["taker_side"] not in {"BUY", "SELL"}:
                raise ValueError("unknown taker side")
            start_us = (
                int(item["event_time"].timestamp() * 1_000_000) // (seconds * 1_000_000)
            ) * (seconds * 1_000_000)
            start = datetime.fromtimestamp(start_us / 1_000_000, tz=UTC)
            if state is not None and start != state["start"]:
                rows.append(_finish(state, seconds, checksum, trading_date))
                state = None
            if state is None:
                state = {
                    **{
                        k: item[k]
                        for k in (
                            "instrument_id",
                            "exchange",
                            "market_type",
                            "contract_type",
                            "native_symbol",
                            "source_dataset_id",
                            "quantity_unit",
                            "notional_unit",
                        )
                    },
                    "start": start,
                    "count": 0,
                    "buy_count": 0,
                    "sell_count": 0,
                    "open": item["price"],
                    "high": item["price"],
                    "low": item["price"],
                    "close": item["price"],
                    "buy_base": Decimal(0),
                    "sell_base": Decimal(0),
                    "buy_quote": Decimal(0),
                    "sell_quote": Decimal(0),
                    "sizes": [],
                }
            state["count"] += 1
            total += 1
            state["close"] = item["price"]
            state["high"] = max(state["high"], item["price"])
            state["low"] = min(state["low"], item["price"])
            state["sizes"].append(item["quantity"])
            side = item["taker_side"].lower()
            state[f"{side}_count"] += 1
            state[f"{side}_base"] += item["quantity"]
            state[f"{side}_quote"] += item["quote_quantity"]
    if state is not None:
        rows.append(_finish(state, seconds, checksum, trading_date))
    if total != event["row_count"]:
        raise ValueError("truncated source traversal")
    if first_event_time is None or last_event_time is None or first_event_time.isoformat() != event["coverage_start"] or last_event_time.isoformat() != event["coverage_end"]:
        raise ValueError("source manifest coverage mismatch")
    output = (
        root
        / "derived"
        / "trade_bucket"
        / "v1"
        / f"exchange={descriptor.exchange}"
        / f"market_type={descriptor.market_type}"
        / f"instrument_id={instrument_id}"
        / f"date={trading_date}"
        / f"bucket={seconds}s"
        / f"part-{checksum[:16]}.parquet"
    )
    output.parent.mkdir(parents=True, exist_ok=True)
    if not output.exists():
        partial = output.with_suffix(".parquet.partial")
        pq.write_table(pa.Table.from_pylist(rows, schema=SCHEMA), partial, compression="zstd")
        with partial.open("r+b") as handle:
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(partial, output)
    manifest = root / "control" / "manifests" / "derived_trade_bucket.jsonl"
    manifest.parent.mkdir(parents=True, exist_ok=True)
    out_hash = sha256_file(output)
    record = {
        "object_id": str(output.relative_to(root)),
        "parquet_sha256": out_hash,
        "source_object_id": event["object_id"],
        "source_parquet_sha256": checksum,
        "source_row_count": total,
        "row_count": len(rows),
        "instrument_id": instrument_id,
        "trading_date": trading_date.isoformat(),
        "bucket_seconds": seconds,
        "aggregation_version": "1.0.0",
    }
    existing = manifest.read_text(encoding="utf-8") if manifest.exists() else ""
    if out_hash not in existing:
        with manifest.open("a", encoding="utf-8") as handle:
            handle.write(json.dumps(record, sort_keys=True) + "\n")
            handle.flush()
            os.fsync(handle.fileno())
    checkpoint = (
        root
        / "control"
        / "checkpoints"
        / f"derived_trade_bucket_{instrument_id}_{trading_date}_{seconds}s.json"
    )
    checkpoint.parent.mkdir(parents=True, exist_ok=True)
    payload = json.dumps(record, sort_keys=True).encode()
    with tempfile.NamedTemporaryFile(
        "wb", dir=checkpoint.parent, delete=False, suffix=".partial"
    ) as handle:
        handle.write(payload)
        handle.flush()
        os.fsync(handle.fileno())
        temp = Path(handle.name)
    os.replace(temp, checkpoint)
    return output
