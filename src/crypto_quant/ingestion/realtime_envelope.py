"""Source-faithful WebSocket Envelope Capture and Deterministic Normalization (Phase 1C Item 7A).

This module implements append-only raw WebSocket envelope capture, session metadata,
atomic segment storage (OPEN/PARTIAL -> SEALED), and deterministic normalization into
canonical dataset classes (`individual_trade` vs `exchange_aggregate_trade`).

Invariants:
- Raw envelopes preserve un-mutated source payload + payload_hash + lineage metadata.
- 1 envelope -> N canonical trades (auditable via source_envelope_id).
- Strict Dataset Isolation: aggTrade payloads MUST NOT enter individual_trade pipeline.
- Bybit `seq` is retained as a source context ordinal, NEVER as a unique trade ID.
"""

from __future__ import annotations

import hashlib
import json
import os
import uuid
from dataclasses import asdict, dataclass
from datetime import datetime
from decimal import Decimal
from pathlib import Path
from typing import Any

import pyarrow as pa

from ..time import utc_now
from .binance.aggregate_trades import (
    EXCHANGE_AGGREGATE_TRADE_SCHEMA,
    validate_dataset_class_isolation,
)
from .binance.spot_trades import INDIVIDUAL_TRADE_SCHEMA
from .bybit.trades import map_bybit_taker_side

DEC = pa.decimal128(38, 18)


@dataclass(frozen=True)
class WsSessionInfo:
    session_id: str
    connection_id: str
    exchange: str
    market_type: str
    stream_topic: str
    connected_at: datetime
    disconnected_at: datetime | None = None
    disconnect_reason: str | None = None


@dataclass(frozen=True)
class RawWsEnvelope:
    envelope_id: str
    source: str
    exchange: str
    market_type: str
    instrument_id: str
    stream_topic: str
    connection_id: str
    session_id: str
    received_at: datetime
    processed_at: datetime
    source_contract_version: str
    collector_version: str
    payload: dict[str, Any]
    payload_hash: str


def create_raw_ws_envelope(
    *,
    exchange: str,
    market_type: str,
    instrument_id: str,
    stream_topic: str,
    session: WsSessionInfo,
    payload: dict[str, Any],
    source_contract_version: str,
    collector_version: str = "1.0.0",
) -> RawWsEnvelope:
    now = utc_now()
    payload_bytes = json.dumps(payload, sort_keys=True).encode("utf-8")
    payload_hash = hashlib.sha256(payload_bytes).hexdigest()
    envelope_id = f"env_{uuid.uuid4().hex[:16]}"

    return RawWsEnvelope(
        envelope_id=envelope_id,
        source=f"{exchange}.{market_type}.ws",
        exchange=exchange,
        market_type=market_type,
        instrument_id=instrument_id,
        stream_topic=stream_topic,
        connection_id=session.connection_id,
        session_id=session.session_id,
        received_at=now,
        processed_at=now,
        source_contract_version=source_contract_version,
        collector_version=collector_version,
        payload=payload,
        payload_hash=payload_hash,
    )


class RawWsSegmentWriter:
    """Atomic segment writer for raw WebSocket envelopes (OPEN/PARTIAL -> SEALED)."""

    def __init__(self, root: Path, exchange: str, market_type: str, symbol: str) -> None:
        self.root = root
        self.exchange = exchange
        self.market_type = market_type
        self.symbol = symbol
        self._current_file: Path | None = None
        self._partial_file: Path | None = None
        self._handle: Any = None
        self._count = 0
        self._bytes_written = 0

    def _ensure_open(self, now: datetime) -> None:
        if self._handle is not None:
            return
        date_str = now.strftime("%Y-%m-%d")
        hour_str = now.strftime("%H")
        dir_path = (
            self.root
            / "raw"
            / "ws"
            / f"exchange={self.exchange}"
            / f"market_type={self.market_type}"
            / f"symbol={self.symbol}"
            / f"date={date_str}"
            / f"hour={hour_str}"
        )
        dir_path.mkdir(parents=True, exist_ok=True)
        seg_id = uuid.uuid4().hex[:8]
        self._current_file = dir_path / f"segment_{seg_id}.jsonl"
        self._partial_file = dir_path / f"segment_{seg_id}.jsonl.partial"
        self._handle = self._partial_file.open("a", encoding="utf-8")

    def write_envelope(self, envelope: RawWsEnvelope) -> None:
        self._ensure_open(envelope.received_at)
        data = {
            **asdict(envelope),
            "received_at": envelope.received_at.isoformat(),
            "processed_at": envelope.processed_at.isoformat(),
        }
        line = json.dumps(data, sort_keys=True) + "\n"
        self._handle.write(line)
        self._handle.flush()
        os.fsync(self._handle.fileno())
        self._count += 1
        self._bytes_written += len(line.encode("utf-8"))

    def seal(self) -> Path | None:
        if self._handle is not None:
            self._handle.close()
            self._handle = None
            if self._partial_file and self._partial_file.exists() and self._current_file:
                os.replace(self._partial_file, self._current_file)
                sealed_path = self._current_file
                self._current_file = None
                self._partial_file = None
                return sealed_path
        return None

    @property
    def envelope_count(self) -> int:
        return self._count

    @property
    def bytes_written(self) -> int:
        return self._bytes_written


def recover_stale_ws_partials(root: Path) -> list[Path]:
    """Recover stale .jsonl.partial WS segment files into quarantine or sealed state."""
    recovered = []
    ws_raw_dir = root / "raw" / "ws"
    if not ws_raw_dir.exists():
        return recovered
    for partial_path in ws_raw_dir.rglob("*.jsonl.partial"):
        sealed_path = partial_path.with_suffix("")
        os.replace(partial_path, sealed_path)
        recovered.append(sealed_path)
    return recovered


def normalize_ws_envelope_to_individual_trades(
    envelope: RawWsEnvelope,
) -> pa.RecordBatch:
    """Normalize a RawWsEnvelope into an individual_trade RecordBatch.

    Strict Dataset Isolation Invariant:
    If envelope payload represents an aggregate trade (e.g. Binance aggTrade),
    this function MUST fail closed with TypeError.
    """
    payload = envelope.payload

    # 1. Dataset class isolation gate for Binance
    if envelope.exchange == "binance":
        event_type = payload.get("e")
        if event_type == "aggTrade":
            raise TypeError(
                "Dataset Class Isolation Defect: Binance 'aggTrade' payload "
                "MUST NOT enter individual_trade pipeline."
            )
        if event_type != "trade":
            raise ValueError(f"Unsupported Binance WS event type: {event_type}")

    # 2. Extract trade items from envelope (1 envelope -> N trades for Bybit multi-trade)
    trade_items: list[dict[str, Any]] = []
    if envelope.exchange == "binance":
        trade_items.append(payload)
    elif envelope.exchange == "bybit":
        data = payload.get("data")
        if isinstance(data, list):
            trade_items.extend(data)
        elif isinstance(data, dict):
            trade_items.append(data)
        else:
            raise ValueError("Bybit WS payload missing 'data' list/dict")

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

    now_us = int(envelope.processed_at.timestamp() * 1_000_000)
    rec_us = int(envelope.received_at.timestamp() * 1_000_000)

    for idx, item in enumerate(trade_items, start=1):
        if envelope.exchange == "binance":
            native_trade_id = str(item["t"])
            price_dec = Decimal(str(item["p"]))
            qty_dec = Decimal(str(item["q"]))
            quote_qty_dec = price_dec * qty_dec
            event_time_us = int(item["T"]) * 1000
            is_bm = bool(item["m"])
            taker_side = "SELL" if is_bm else "BUY"
            signed_qty = -qty_dec if is_bm else qty_dec
            is_best_match = bool(item.get("M", True))
            is_block_trade = None
            is_rpi_trade = None
            source_ts = int(item["T"])
            source_unit = "epoch_ms"
            dq_flags = None
            seq_ordinal = idx
        elif envelope.exchange == "bybit":
            native_trade_id = str(item.get("i") or f"{envelope.envelope_id}_{idx}")
            dq_flags = []

            try:
                price_dec = Decimal(str(item.get("p", "0")))
                qty_dec = Decimal(str(item.get("v") or item.get("size") or "0"))
                quote_qty_dec = price_dec * qty_dec
            except Exception:
                price_dec = Decimal(0)
                qty_dec = Decimal(0)
                quote_qty_dec = Decimal(0)
                dq_flags.append("MALFORMED_PAYLOAD")

            try:
                event_time_us = int(item.get("T") or payload.get("ts") or 0) * 1000
                source_ts = int(item.get("T") or 0)
            except Exception:
                event_time_us = int(envelope.received_at.timestamp() * 1_000_000)
                source_ts = 0
                dq_flags.append("MALFORMED_TIMESTAMP")

            raw_side = str(item.get("S", ""))
            taker_side = map_bybit_taker_side(raw_side)
            if taker_side == "BUY":
                signed_qty = qty_dec
                is_bm = False
            elif taker_side == "SELL":
                signed_qty = -qty_dec
                is_bm = True
            else:
                signed_qty = Decimal(0)
                is_bm = False
                dq_flags.append("UNKNOWN_TAKER_SIDE")

            is_best_match = False
            is_block_trade = bool(item.get("BT")) if "BT" in item else None
            is_rpi_trade = None
            source_unit = "epoch_ms"
            seq_ordinal = int(item["seq"]) if "seq" in item and str(item["seq"]).isdigit() else idx

            if not dq_flags:
                dq_flags = None

        col_instrument_id.append(envelope.instrument_id)
        col_exchange.append(envelope.exchange)
        col_market_type.append(envelope.market_type)
        col_contract_type.append("spot" if envelope.market_type == "spot" else "linear_perpetual")
        col_native_symbol.append(item.get("s") or item.get("symbol") or "BTCUSDT")
        col_dataset_class.append("individual_trade")
        col_source_dataset_id.append(f"{envelope.exchange}.{envelope.market_type}.individual_trade.ws")
        col_native_trade_id.append(native_trade_id)
        col_source_ordinal.append(seq_ordinal)
        col_event_time.append(event_time_us)
        col_exchange_timestamp.append(event_time_us)
        col_source_timestamp.append(source_ts)
        col_source_timestamp_unit.append(source_unit)
        col_price.append(price_dec)
        col_quantity.append(qty_dec)
        col_quantity_unit.append("BTC")
        col_quote_quantity.append(quote_qty_dec)
        col_notional_unit.append("USDT")
        col_taker_side.append(taker_side)
        col_signed_quantity.append(signed_qty)
        col_is_buyer_maker.append(is_bm)
        col_is_best_match.append(is_best_match)
        col_is_block_trade.append(is_block_trade)
        col_is_rpi_trade.append(is_rpi_trade)
        col_received_at.append(rec_us)
        col_processed_at.append(now_us)
        col_knowledge_time.append(rec_us)
        col_knowledge_time_basis.append("realtime_ws_receipt")
        col_source_uri.append(f"ws://{envelope.exchange}/{envelope.stream_topic}")
        col_raw_object_ref.append(f"envelope_id={envelope.envelope_id}")
        col_source_object_sha256.append(envelope.payload_hash)
        col_schema_version.append("1.0.0")
        col_collector_version.append(envelope.collector_version)
        col_normalization_version.append("1.0.0")
        col_data_contract_version.append(envelope.source_contract_version)
        col_classification_version.append("1.0.0")
        col_dq_flags.append(dq_flags)

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
    return rb


def normalize_ws_envelope_to_aggregate_trades(
    envelope: RawWsEnvelope,
) -> pa.RecordBatch:
    """Normalize a RawWsEnvelope into an exchange_aggregate_trade RecordBatch."""
    payload = envelope.payload
    if envelope.exchange == "binance" and payload.get("e") != "aggTrade":
        raise TypeError(
            "Dataset Class Isolation Defect: Expected Binance 'aggTrade' payload for aggregate pipeline."
        )

    agg_trade_id = str(payload["a"])
    price_str = str(payload["p"])
    qty_str = str(payload["q"])
    first_id = str(payload["f"])
    last_id = str(payload["l"])
    ts_ms = int(payload["T"])
    is_bm = bool(payload["m"])

    price_dec = Decimal(price_str)
    qty_dec = Decimal(qty_str)
    quote_qty_dec = price_dec * qty_dec
    signed_qty = -qty_dec if is_bm else qty_dec
    taker_side = "SELL" if is_bm else "BUY"
    event_us = ts_ms * 1000
    now_us = int(envelope.processed_at.timestamp() * 1_000_000)

    rb = pa.RecordBatch.from_arrays(
        [
            pa.array([envelope.instrument_id], pa.string()),
            pa.array([envelope.exchange], pa.string()),
            pa.array([envelope.market_type], pa.string()),
            pa.array(["spot" if envelope.market_type == "spot" else "linear_perpetual"], pa.string()),
            pa.array([payload.get("s", "BTCUSDT")], pa.string()),
            pa.array(["exchange_aggregate_trade"], pa.string()),
            pa.array([f"{envelope.exchange}.{envelope.market_type}.exchange_aggregate_trade.ws"], pa.string()),
            pa.array([agg_trade_id], pa.string()),
            pa.array([first_id], pa.string()),
            pa.array([last_id], pa.string()),
            pa.array([1], pa.int64()),
            pa.array([event_us], pa.timestamp("us", tz="UTC")),
            pa.array([event_us], pa.timestamp("us", tz="UTC")),
            pa.array([price_dec], DEC),
            pa.array([qty_dec], DEC),
            pa.array(["BTC"], pa.string()),
            pa.array([quote_qty_dec], DEC),
            pa.array(["USDT"], pa.string()),
            pa.array([taker_side], pa.string()),
            pa.array([signed_qty], DEC),
            pa.array([is_bm], pa.bool_()),
            pa.array([bool(payload.get("M", True))], pa.bool_()),
            pa.array([int(envelope.received_at.timestamp() * 1_000_000)], pa.timestamp("us", tz="UTC")),
            pa.array([now_us], pa.timestamp("us", tz="UTC")),
            pa.array([f"ws://{envelope.exchange}/{envelope.stream_topic}"], pa.string()),
            pa.array([envelope.payload_hash], pa.string()),
            pa.array(["1.0.0"], pa.string()),
            pa.array([None], pa.list_(pa.string())),
        ],
        schema=EXCHANGE_AGGREGATE_TRADE_SCHEMA,
    )
    validate_dataset_class_isolation(rb, "exchange_aggregate_trade")
    return rb
