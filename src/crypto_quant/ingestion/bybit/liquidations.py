"""Bybit Linear Liquidations Ingestion and Normalization (Phase 1D.3).

Consumes real-time liquidations from WebSocket topic `allLiquidation.{symbol}`.
Bybit claims all liquidations are reported (source_claimed_completeness=ALL_LIQUIDATIONS).
Delivery is batched every 500 ms (delivery_semantics=BATCHED_500MS_PUSH).

Enforces:
- Canonical identity: market_type='perpetual', contract_type='linear_perpetual', venue_product_type='linear'
- Natural key: (exchange, instrument_id, event_time, dedup_fingerprint)
- Explicit Side Semantics:
    * S="Buy"  -> position_side_liquidated="LONG"  (long position liquidated via forced buy order)
    * S="Sell" -> position_side_liquidated="SHORT" (short position liquidated via forced sell order)
- Explicit Price Semantics: price_semantic="bankruptcy_price" (p is bankruptcy price in Bybit contract)
- Explicit T Semantics: T is the liquidation event *updated* timestamp (ms); NOT a fill/execution/trade time.
  Source: Bybit V5 allLiquidation official documentation (VERIFIED)
- Quantity semantics: v is executed size in the instrument base coin for the in-scope
  USDT linear perpetuals. The base asset is carried by the canonical instrument identity.
  Source: Bybit V5 all-liquidation and linear product-term documentation (VERIFIED for
  BTCUSDT and ETHUSDT).
- Decimal preservation: raw string decimal for size (v) and price (p)
- Realtime Knowledge Time: knowledge_time = received_at (UTC arrival timestamp to eliminate look-ahead leakage)
- Message ID = SHA-256 of raw WS envelope. No native per-event ID in stream.
  Dedup guarantee boundary:
    exact-wire replay: GUARANTEED (message_id + event_index fingerprint)
    cross-envelope economic-event dedup: NOT GUARANTEED (no native event_id from Bybit)
- Immutable Parquet storage with content-addressed generations, manifests, checkpoints, and DQ validation.
"""

from __future__ import annotations

import asyncio
import hashlib
import json
import logging
import os
import tempfile
import time
from dataclasses import dataclass
from datetime import UTC, datetime
from decimal import Decimal
from pathlib import Path
from typing import Any

import pyarrow as pa
import pyarrow.parquet as pq

from ...contracts import ContractField, DataContract
from ...identity import InstrumentIdentity
from ...paths import disk_free_bytes
from ...time import parse_epoch, utc_now
from .funding import funding_identity

logger = logging.getLogger(__name__)

DATASET_ID = "bybit.linear.liquidations.ws"
CONTRACT_ID = "bybit.linear.ws.all-liquidation.v1"
COLLECTOR_VERSION = "0.4.0"
NORMALIZATION_VERSION = "1.0.0"
SCHEMA_VERSION = "1.0.0"
DOCS_URL = "https://bybit-exchange.github.io/docs/v5/websocket/public/all-liquidation"

BYBIT_WS_LINEAR_URL = "wss://stream.bybit.com/v5/public/linear"


def bybit_linear_liquidation_data_contract() -> DataContract:
    """Returns the official DataContract specification for Bybit Linear Liquidations."""
    fields = (
        ContractField(
            source_field="topic",
            semantic_meaning="Subscription topic",
            nullable=False,
            canonical_field="routing",
            transformation="preserve",
            validation_rules=("matches_symbol",),
        ),
        ContractField(
            source_field="type",
            semantic_meaning="WebSocket message batch label",
            nullable=False,
            canonical_field="message_type",
            transformation="preserve",
            known_limitations=("label snapshot does not mean replaceable state",),
        ),
        ContractField(
            source_field="ts",
            semantic_meaning="System message generation timestamp, ms",
            source_unit="ms",
            timestamp_meaning="exchange_generation_time",
            nullable=False,
            canonical_field="exchange_timestamp",
            transformation="parse_epoch_ms_utc",
            normalized_unit="timestamp[us, tz=UTC]",
        ),
        ContractField(
            source_field="data[].T",
            semantic_meaning="Liquidation event updated timestamp, ms (official: 'updated time')",
            source_unit="ms",
            timestamp_meaning="event_updated_time",  # VERIFIED: Bybit docs say 'updated timestamp', NOT fill/trade time
            nullable=False,
            canonical_field="event_time",
            transformation="parse_epoch_ms_utc",
            normalized_unit="timestamp[us, tz=UTC]",
        ),
        ContractField(
            source_field="data[].s",
            semantic_meaning="Symbol name",
            nullable=False,
            canonical_field="symbol",
            transformation="preserve",
            validation_rules=("matches_symbol",),
        ),
        ContractField(
            source_field="data[].S",
            semantic_meaning="Liquidated position side (Buy=long liquidated, Sell=short liquidated)",
            nullable=False,
            canonical_field="position_side_liquidated",
            transformation="map_bybit_side",
            validation_rules=("in_LONG_SHORT",),
        ),
        ContractField(
            source_field="data[].v",
            # VERIFIED: official docs label = 'Executed size' (string). For the in-scope
            # USDT linear perpetuals, quantity is denominated in the instrument base coin.
            # Source: bybit-exchange.github.io/docs/v5/websocket/public/all-liquidation
            semantic_meaning="Executed liquidation size in the instrument base coin",
            source_unit="base_coin",
            nullable=False,
            canonical_field="source_quantity",
            transformation="decimal_str",
            normalized_unit="base_coin",
            validation_rules=("strictly_positive",),
        ),
        ContractField(
            source_field="data[].p",
            semantic_meaning="Bankruptcy price",
            nullable=False,
            canonical_field="source_price",
            transformation="decimal_str",
            known_limitations=("price is bankruptcy price, not execution fill price",),
            validation_rules=("strictly_positive",),
        ),
    )
    return DataContract(
        contract_id=CONTRACT_ID,
        source_dataset_id=DATASET_ID,
        exchange="bybit",
        market_type="perpetual",
        source_kind="websocket",
        official_documentation_url=DOCS_URL,  # type: ignore[arg-type]
        verified_at=datetime(2026, 8, 11, 0, 0, 0, tzinfo=UTC),
        schema_version=SCHEMA_VERSION,
        fields=fields,
    )


@dataclass(frozen=True)
class CanonicalLiquidationRecord:
    """Normalized canonical liquidation record with explicit side, price, unit and knowledge semantics."""

    # 1. Identity
    exchange: str
    instrument_id: str
    symbol: str
    market_type: str
    contract_type: str
    venue_product_type: str

    # 2. Time & Knowledge
    event_time: datetime
    exchange_timestamp: datetime
    received_at: datetime
    processed_at: datetime
    knowledge_time: datetime

    # 3. Position & Side Semantics
    position_side_liquidated: str  # 'LONG' | 'SHORT' | 'UNKNOWN'
    source_side: str  # Raw string: 'Buy' | 'Sell'
    source_side_semantic: str  # 'LIQUIDATED_POSITION_SIDE'

    # 4. Quantity & Units
    source_quantity: str  # Decimal string: e.g. '0.003'
    source_quantity_unit: str  # 'base_coin'
    quantity_base: str  # Decimal string: e.g. '0.003'
    notional_quote: str | None  # None (no silent synthetic multiplication)
    last_filled_quantity: str | None  # None for Bybit
    accumulated_filled_quantity: str | None  # None for Bybit

    # 5. Price Semantics
    source_price: str  # Decimal string: e.g. '43511.70'
    price_semantic: str  # 'bankruptcy_price'
    average_fill_price: str | None  # None for Bybit

    # 6. Order Attributes
    order_type: str | None  # None for Bybit
    time_in_force: str | None  # None for Bybit
    order_status: str | None  # None for Bybit

    # 7. Completeness & Provenance
    # source_claimed_completeness: Bybit claims ALL_LIQUIDATIONS are pushed
    # delivery_semantics: BATCHED_500MS_PUSH (not unthrottled; batched at 500ms intervals)
    # local_capture_completeness: depends on connection uptime and local gaps
    source_claimed_completeness: str  # 'ALL_LIQUIDATIONS' (Bybit claim)
    delivery_semantics: str  # 'BATCHED_500MS_PUSH'
    # Dedup boundary:
    #   exact-wire replay dedup: GUARANTEED (message_id = raw envelope hash; dedup_fingerprint = event content hash)
    #   cross-envelope economic-event dedup: NOT GUARANTEED (no native per-event ID in stream)
    message_id: str  # SHA-256 of raw WebSocket envelope string
    dedup_fingerprint: str  # SHA-256 of (symbol|T|S|p|v|message_id|event_index)
    dedup_guarantee: str  # 'EXACT_WIRE_REPLAY_ONLY'
    source: str
    source_contract_version: str
    schema_version: str
    collector_version: str
    normalization_version: str


def parse_bybit_liquidation_message(
    raw_msg: dict[str, Any],
    ident: InstrumentIdentity,
    received_at: datetime,
    raw_msg_str: str | None = None,
) -> list[CanonicalLiquidationRecord]:
    """Parses a raw Bybit V5 `allLiquidation.{symbol}` WebSocket message into canonical records.

    Handles batched observations within a single message, ensuring each event receives a unique
    deduplication fingerprint while sharing the message_id.
    """
    topic = raw_msg.get("topic", "")
    expected_topic = f"allLiquidation.{ident.native_symbol}"
    if topic != expected_topic:
        raise ValueError(f"Topic mismatch: expected '{expected_topic}', got '{topic}'")

    raw_ts = raw_msg.get("ts")
    if raw_ts is None:
        raise ValueError("Missing mandatory field 'ts' in Bybit liquidation message")
    exchange_ts = parse_epoch(int(raw_ts), unit="ms")

    if raw_msg_str is None:
        raw_msg_str = json.dumps(raw_msg, sort_keys=True)
    msg_id = hashlib.sha256(raw_msg_str.encode("utf-8")).hexdigest()

    data_items = raw_msg.get("data", [])
    if not isinstance(data_items, list):
        raise ValueError("Field 'data' must be a list of liquidation items")

    proc_at = utc_now()
    records: list[CanonicalLiquidationRecord] = []

    for event_idx, item in enumerate(data_items):
        sym = item.get("s")
        if sym != ident.native_symbol:
            raise ValueError(f"Symbol mismatch in data item: expected '{ident.native_symbol}', got '{sym}'")

        raw_event_t = item.get("T")
        if raw_event_t is None:
            raise ValueError("Missing mandatory field 'T' in liquidation data item")
        event_time = parse_epoch(int(raw_event_t), unit="ms")

        raw_side = item.get("S")
        if raw_side is None or str(raw_side).strip() == "":
            raise ValueError("Missing mandatory field 'S' in liquidation data item")
        raw_side_str = str(raw_side).strip()

        # Official Bybit semantics: Buy = Long position liquidated; Sell = Short position liquidated
        if raw_side_str == "Buy":
            pos_side = "LONG"
        elif raw_side_str == "Sell":
            pos_side = "SHORT"
        else:
            pos_side = "UNKNOWN"

        raw_size = item.get("v")
        if raw_size is None or str(raw_size).strip() == "":
            raise ValueError("Missing mandatory field 'v' (size) in liquidation data item")
        size_dec = Decimal(str(raw_size).strip())
        if size_dec <= 0:
            raise ValueError(f"Non-positive size value: {size_dec}")

        raw_price = item.get("p")
        if raw_price is None or str(raw_price).strip() == "":
            raise ValueError("Missing mandatory field 'p' (price) in liquidation data item")
        price_dec = Decimal(str(raw_price).strip())
        if price_dec <= 0:
            raise ValueError(f"Non-positive price value: {price_dec}")

        # Deterministic event fingerprint: incorporates message identity and intra-batch event_index.
        # Guarantees:
        #   1. Same raw WS envelope re-delivered on reconnect -> same dedup_fingerprint -> deduplicated.
        #   2. Two distinct events in one batch with identical content -> different event_idx -> distinct fingerprints.
        # Limitation:
        #   Bybit allLiquidation has NO native per-event ID or sequence number.
        #   If Bybit re-delivers the same economic event in a DIFFERENT envelope (different ts or batch),
        #   it will produce a different message_id and therefore a different dedup_fingerprint.
        #   Cross-envelope economic-event dedup is NOT GUARANTEED.
        fp_str = (
            f"bybit|{ident.native_symbol}|{raw_event_t}|{raw_side_str}|{str(price_dec)}|{str(size_dec)}|"
            f"{msg_id}|{event_idx}"
        )
        dedup_fp = hashlib.sha256(fp_str.encode("utf-8")).hexdigest()

        rec = CanonicalLiquidationRecord(
            exchange="bybit",
            instrument_id=ident.instrument_id,
            symbol=ident.native_symbol,
            market_type="perpetual",
            contract_type="linear_perpetual",
            venue_product_type="linear",
            event_time=event_time,
            exchange_timestamp=exchange_ts,
            received_at=received_at,
            processed_at=proc_at,
            knowledge_time=received_at,  # Realtime: knowledge_time = received_at (arrival UTC timestamp)
            position_side_liquidated=pos_side,
            source_side=raw_side_str,
            source_side_semantic="LIQUIDATED_POSITION_SIDE",
            source_quantity=str(size_dec),
            source_quantity_unit="base_coin",  # Verified for the in-scope USDT linear perpetuals.
            quantity_base=str(size_dec),
            notional_quote=None,  # No silent synthetic multiplication
            last_filled_quantity=None,
            accumulated_filled_quantity=None,
            source_price=str(price_dec),
            price_semantic="bankruptcy_price",  # VERIFIED: p = bankruptcy price, not fill price
            average_fill_price=None,
            order_type=None,
            time_in_force=None,
            order_status=None,
            source_claimed_completeness="ALL_LIQUIDATIONS",  # Bybit claim: all liquidations pushed
            delivery_semantics="BATCHED_500MS_PUSH",         # Delivery: batched at 500ms intervals
            message_id=msg_id,
            dedup_fingerprint=dedup_fp,
            dedup_guarantee="EXACT_WIRE_REPLAY_ONLY",  # Cross-envelope dedup NOT guaranteed (no native event ID)
            source=DATASET_ID,
            source_contract_version=CONTRACT_ID,
            schema_version=SCHEMA_VERSION,
            collector_version=COLLECTOR_VERSION,
            normalization_version=NORMALIZATION_VERSION,
        )
        records.append(rec)

    return records


def validate_liquidation_records_dq(records: list[CanonicalLiquidationRecord]) -> list[str]:
    """Validates data quality invariants for a batch of canonical liquidation records."""
    issues: list[str] = []
    if not records:
        return issues

    for i, r in enumerate(records):
        if r.exchange != "bybit":
            issues.append(f"Row {i}: Invalid exchange '{r.exchange}'")
        if r.market_type != "perpetual" or r.contract_type != "linear_perpetual":
            issues.append(f"Row {i}: Invalid canonical market identity")
        if r.position_side_liquidated not in ("LONG", "SHORT", "UNKNOWN"):
            issues.append(f"Row {i}: Invalid position side '{r.position_side_liquidated}'")
        if r.price_semantic != "bankruptcy_price":
            issues.append(f"Row {i}: Invalid price_semantic '{r.price_semantic}'")
        try:
            if Decimal(r.source_quantity) <= 0:
                issues.append(f"Row {i}: Non-positive source_quantity '{r.source_quantity}'")
            if Decimal(r.source_price) <= 0:
                issues.append(f"Row {i}: Non-positive source_price '{r.source_price}'")
        except Exception as exc:
            issues.append(f"Row {i}: Decimal parse failure: {exc}")
        if r.knowledge_time != r.received_at:
            issues.append(f"Row {i}: Realtime knowledge_time mismatch: {r.knowledge_time} != {r.received_at}")

    return issues


def records_to_pyarrow_liquidation_table(records: list[CanonicalLiquidationRecord]) -> pa.Table:
    """Converts a sequence of CanonicalLiquidationRecords into a typed PyArrow Table."""
    schema = pa.schema(
        [
            ("exchange", pa.string()),
            ("instrument_id", pa.string()),
            ("symbol", pa.string()),
            ("market_type", pa.string()),
            ("contract_type", pa.string()),
            ("venue_product_type", pa.string()),
            ("event_time", pa.timestamp("us", tz="UTC")),
            ("exchange_timestamp", pa.timestamp("us", tz="UTC")),
            ("received_at", pa.timestamp("us", tz="UTC")),
            ("processed_at", pa.timestamp("us", tz="UTC")),
            ("knowledge_time", pa.timestamp("us", tz="UTC")),
            ("position_side_liquidated", pa.string()),
            ("source_side", pa.string()),
            ("source_side_semantic", pa.string()),
            ("source_quantity", pa.string()),
            ("source_quantity_unit", pa.string()),
            ("quantity_base", pa.string()),
            ("notional_quote", pa.string()),
            ("last_filled_quantity", pa.string()),
            ("accumulated_filled_quantity", pa.string()),
            ("source_price", pa.string()),
            ("price_semantic", pa.string()),
            ("average_fill_price", pa.string()),
            ("order_type", pa.string()),
            ("time_in_force", pa.string()),
            ("order_status", pa.string()),
            ("source_claimed_completeness", pa.string()),
            ("delivery_semantics", pa.string()),
            ("message_id", pa.string()),
            ("dedup_fingerprint", pa.string()),
            ("dedup_guarantee", pa.string()),
            ("source", pa.string()),
            ("source_contract_version", pa.string()),
            ("schema_version", pa.string()),
            ("collector_version", pa.string()),
            ("normalization_version", pa.string()),
        ]
    )

    data_dict = {
        "exchange": [r.exchange for r in records],
        "instrument_id": [r.instrument_id for r in records],
        "symbol": [r.symbol for r in records],
        "market_type": [r.market_type for r in records],
        "contract_type": [r.contract_type for r in records],
        "venue_product_type": [r.venue_product_type for r in records],
        "event_time": [r.event_time for r in records],
        "exchange_timestamp": [r.exchange_timestamp for r in records],
        "received_at": [r.received_at for r in records],
        "processed_at": [r.processed_at for r in records],
        "knowledge_time": [r.knowledge_time for r in records],
        "position_side_liquidated": [r.position_side_liquidated for r in records],
        "source_side": [r.source_side for r in records],
        "source_side_semantic": [r.source_side_semantic for r in records],
        "source_quantity": [r.source_quantity for r in records],
        "source_quantity_unit": [r.source_quantity_unit for r in records],
        "quantity_base": [r.quantity_base for r in records],
        "notional_quote": [r.notional_quote for r in records],
        "last_filled_quantity": [r.last_filled_quantity for r in records],
        "accumulated_filled_quantity": [r.accumulated_filled_quantity for r in records],
        "source_price": [r.source_price for r in records],
        "price_semantic": [r.price_semantic for r in records],
        "average_fill_price": [r.average_fill_price for r in records],
        "order_type": [r.order_type for r in records],
        "time_in_force": [r.time_in_force for r in records],
        "order_status": [r.order_status for r in records],
        "source_claimed_completeness": [r.source_claimed_completeness for r in records],
        "delivery_semantics": [r.delivery_semantics for r in records],
        "message_id": [r.message_id for r in records],
        "dedup_fingerprint": [r.dedup_fingerprint for r in records],
        "dedup_guarantee": [r.dedup_guarantee for r in records],
        "source": [r.source for r in records],
        "source_contract_version": [r.source_contract_version for r in records],
        "schema_version": [r.schema_version for r in records],
        "collector_version": [r.collector_version for r in records],
        "normalization_version": [r.normalization_version for r in records],
    }
    return pa.Table.from_pydict(data_dict, schema=schema)


def _ensure_utc(dt: datetime | None) -> datetime | None:
    if dt is None:
        return None
    if dt.tzinfo is None:
        return dt.replace(tzinfo=UTC)
    return dt.astimezone(UTC)


def merge_and_write_liquidation_parquet(
    yr_dir: Path,
    symbol: str,
    yr: int,
    new_records: list[CanonicalLiquidationRecord],
) -> tuple[Path, int, str, int]:
    """Merges new liquidation records with existing Parquet partitions by natural key.

    Publishes an immutable Parquet generation with deterministic content-addressed naming:
    `part-{symbol.lower()}_{yr}_{gen_hash}.parquet`.
    Returns: (output_parquet_path, total_rows_in_partition, parquet_sha256, parquet_bytes)
    """
    records_by_key: dict[tuple[str, str, datetime, str], CanonicalLiquidationRecord] = {}

    # 1. Read existing generation files if any exist in partition directory
    if yr_dir.exists():
        existing_parquets = sorted(yr_dir.glob("part-*.parquet"))
        for pfile in existing_parquets:
            existing_table = pq.ParquetFile(pfile).read()
            for i in range(len(existing_table)):
                ev_t = _ensure_utc(existing_table["event_time"][i].as_py())
                ex_t = _ensure_utc(existing_table["exchange_timestamp"][i].as_py())
                rc_t = _ensure_utc(existing_table["received_at"][i].as_py())
                pr_t = _ensure_utc(existing_table["processed_at"][i].as_py())
                kn_t = _ensure_utc(existing_table["knowledge_time"][i].as_py())

                rec = CanonicalLiquidationRecord(
                    exchange=existing_table["exchange"][i].as_py(),
                    instrument_id=existing_table["instrument_id"][i].as_py(),
                    symbol=existing_table["symbol"][i].as_py(),
                    market_type=existing_table["market_type"][i].as_py(),
                    contract_type=existing_table["contract_type"][i].as_py(),
                    venue_product_type=existing_table["venue_product_type"][i].as_py(),
                    event_time=ev_t,  # type: ignore[arg-type]
                    exchange_timestamp=ex_t,  # type: ignore[arg-type]
                    received_at=rc_t,  # type: ignore[arg-type]
                    processed_at=pr_t,  # type: ignore[arg-type]
                    knowledge_time=kn_t,  # type: ignore[arg-type]
                    position_side_liquidated=existing_table["position_side_liquidated"][i].as_py(),
                    source_side=existing_table["source_side"][i].as_py(),
                    source_side_semantic=existing_table["source_side_semantic"][i].as_py(),
                    source_quantity=existing_table["source_quantity"][i].as_py(),
                    source_quantity_unit=existing_table["source_quantity_unit"][i].as_py(),
                    quantity_base=existing_table["quantity_base"][i].as_py(),
                    notional_quote=existing_table["notional_quote"][i].as_py(),
                    last_filled_quantity=existing_table["last_filled_quantity"][i].as_py(),
                    accumulated_filled_quantity=existing_table["accumulated_filled_quantity"][i].as_py(),
                    source_price=existing_table["source_price"][i].as_py(),
                    price_semantic=existing_table["price_semantic"][i].as_py(),
                    average_fill_price=existing_table["average_fill_price"][i].as_py(),
                    order_type=existing_table["order_type"][i].as_py(),
                    time_in_force=existing_table["time_in_force"][i].as_py(),
                    order_status=existing_table["order_status"][i].as_py(),
                    source_claimed_completeness=existing_table["source_claimed_completeness"][i].as_py(),
                    delivery_semantics=existing_table["delivery_semantics"][i].as_py(),
                    message_id=existing_table["message_id"][i].as_py(),
                    dedup_fingerprint=existing_table["dedup_fingerprint"][i].as_py(),
                    dedup_guarantee=existing_table["dedup_guarantee"][i].as_py(),
                    source=existing_table["source"][i].as_py(),
                    source_contract_version=existing_table["source_contract_version"][i].as_py(),
                    schema_version=existing_table["schema_version"][i].as_py(),
                    collector_version=existing_table["collector_version"][i].as_py(),
                    normalization_version=existing_table["normalization_version"][i].as_py(),
                )
                key = (rec.exchange, rec.instrument_id, rec.event_time, rec.dedup_fingerprint)
                records_by_key[key] = rec

    # 2. Merge new incoming records
    for rec in new_records:
        rec_utc = CanonicalLiquidationRecord(
            exchange=rec.exchange,
            instrument_id=rec.instrument_id,
            symbol=rec.symbol,
            market_type=rec.market_type,
            contract_type=rec.contract_type,
            venue_product_type=rec.venue_product_type,
            event_time=_ensure_utc(rec.event_time),  # type: ignore[arg-type]
            exchange_timestamp=_ensure_utc(rec.exchange_timestamp),  # type: ignore[arg-type]
            received_at=_ensure_utc(rec.received_at),  # type: ignore[arg-type]
            processed_at=_ensure_utc(rec.processed_at),  # type: ignore[arg-type]
            knowledge_time=_ensure_utc(rec.knowledge_time),  # type: ignore[arg-type]
            position_side_liquidated=rec.position_side_liquidated,
            source_side=rec.source_side,
            source_side_semantic=rec.source_side_semantic,
            source_quantity=rec.source_quantity,
            source_quantity_unit=rec.source_quantity_unit,
            quantity_base=rec.quantity_base,
            notional_quote=rec.notional_quote,
            last_filled_quantity=rec.last_filled_quantity,
            accumulated_filled_quantity=rec.accumulated_filled_quantity,
            source_price=rec.source_price,
            price_semantic=rec.price_semantic,
            average_fill_price=rec.average_fill_price,
            order_type=rec.order_type,
            time_in_force=rec.time_in_force,
            order_status=rec.order_status,
            source_claimed_completeness=rec.source_claimed_completeness,
            delivery_semantics=rec.delivery_semantics,
            message_id=rec.message_id,
            dedup_fingerprint=rec.dedup_fingerprint,
            dedup_guarantee=rec.dedup_guarantee,
            source=rec.source,
            source_contract_version=rec.source_contract_version,
            schema_version=rec.schema_version,
            collector_version=rec.collector_version,
            normalization_version=rec.normalization_version,
        )
        key = (rec_utc.exchange, rec_utc.instrument_id, rec_utc.event_time, rec_utc.dedup_fingerprint)
        records_by_key[key] = rec_utc

    sorted_records = sorted(records_by_key.values(), key=lambda r: (r.event_time, r.dedup_fingerprint))

    # 3. Compute deterministic generation fingerprint
    fingerprint_items = [
        f"{r.exchange}|{r.instrument_id}|{int(r.event_time.timestamp()*1000)}|{r.position_side_liquidated}|{r.source_price}|{r.source_quantity}|{r.dedup_fingerprint}"
        for r in sorted_records
    ]
    gen_hash = hashlib.sha256("\n".join(fingerprint_items).encode("utf-8")).hexdigest()[:12]

    yr_dir.mkdir(parents=True, exist_ok=True)
    target_parquet = yr_dir / f"part-{symbol.lower()}_{yr}_{gen_hash}.parquet"

    if target_parquet.exists():
        p_bytes = target_parquet.stat().st_size
        p_sha = hashlib.sha256(target_parquet.read_bytes()).hexdigest()
        return target_parquet, len(sorted_records), p_sha, p_bytes

    merged_table = records_to_pyarrow_liquidation_table(sorted_records)
    partial_parquet = target_parquet.with_suffix(".parquet.partial")
    pq.write_table(merged_table, partial_parquet, compression="zstd", flavor="spark")

    if pq.ParquetFile(partial_parquet).metadata.num_rows != len(sorted_records):
        raise ValueError("Parquet validation failed: row count mismatch")

    os.replace(partial_parquet, target_parquet)
    p_bytes = target_parquet.stat().st_size
    p_sha = hashlib.sha256(target_parquet.read_bytes()).hexdigest()
    return target_parquet, len(sorted_records), p_sha, p_bytes


def persist_bybit_liquidation_batch(
    raw_messages: list[dict[str, Any] | tuple[dict[str, Any], str]],
    symbol: str,
    root: Path,
    received_at: datetime | None = None,
    min_disk_free_gb: float = 20.0,
) -> dict[str, Any]:
    """Normalizes and persists a batch of Bybit liquidation WebSocket messages to raw and canonical Parquet storage."""
    free_gb = disk_free_bytes(root) / (1024**3)
    if free_gb < min_disk_free_gb:
        raise OSError(f"Disk space below threshold: {free_gb:.2f} GB < {min_disk_free_gb} GB")

    if received_at is None:
        received_at = utc_now()

    ident = funding_identity(symbol)

    # 1. Parse and normalize all items in the batch
    all_records: list[CanonicalLiquidationRecord] = []
    unpacked_raw_msgs: list[dict[str, Any]] = []
    unpacked_raw_strs: list[str] = []

    for item in raw_messages:
        if isinstance(item, tuple):
            msg_dict, msg_str = item
        else:
            msg_dict, msg_str = item, json.dumps(item, sort_keys=True)
        unpacked_raw_msgs.append(msg_dict)
        unpacked_raw_strs.append(msg_str)
        recs = parse_bybit_liquidation_message(msg_dict, ident, received_at=received_at, raw_msg_str=msg_str)
        all_records.extend(recs)

    if not all_records:
        return {
            "symbol": symbol,
            "status": "PASS",
            "event_observation_status": "NO_EVENT_OBSERVED_WITHIN_WINDOW",
            "records_count": 0,
            "total_accumulated_rows": 0,
        }

    # 2. Validate Data Quality
    dq_issues = validate_liquidation_records_dq(all_records)
    if dq_issues:
        raise ValueError(f"Bybit Liquidation DQ validation failed: {dq_issues[:5]}")

    # 3. Persist Raw JSONL with exact content hash
    sorted_records = sorted(all_records, key=lambda r: (r.event_time, r.dedup_fingerprint))
    min_ts_iso = sorted_records[0].event_time.strftime("%Y%m%dT%H%M%SZ")
    max_ts_iso = sorted_records[-1].event_time.strftime("%Y%m%dT%H%M%SZ")
    raw_bytes = ("\n".join(unpacked_raw_strs) + "\n").encode("utf-8")
    raw_hash = hashlib.sha256(raw_bytes).hexdigest()

    date_str = sorted_records[0].event_time.strftime("%Y-%m-%d")
    raw_dir = root / "raw" / "bybit" / "perpetual" / "liquidations" / symbol / f"date={date_str}"
    raw_dir.mkdir(parents=True, exist_ok=True)
    raw_file = raw_dir / f"liq_{min_ts_iso}_{max_ts_iso}_{raw_hash[:8]}.jsonl"

    if not raw_file.exists():
        with tempfile.NamedTemporaryFile("wb", dir=raw_dir, delete=False, suffix=".partial") as tmp:
            tmp.write(raw_bytes)
            tmp.flush()
            os.fsync(tmp.fileno())
            temp_path = Path(tmp.name)
        os.replace(temp_path, raw_file)

    # 4. Group by Year and Persist Canonical Parquet with Immutable Generations
    norm_base = (
        root
        / "normalized"
        / "liquidations"
        / "v1"
        / "exchange=bybit"
        / "market_type=perpetual"
        / f"symbol={symbol}"
    )
    norm_base.mkdir(parents=True, exist_ok=True)

    records_by_year: dict[int, list[CanonicalLiquidationRecord]] = {}
    for r in sorted_records:
        yr = r.event_time.year
        records_by_year.setdefault(yr, []).append(r)

    created_parquet_files: list[Path] = []
    parquet_hashes: list[str] = []
    total_dataset_rows = 0

    for yr, yr_records in sorted(records_by_year.items()):
        yr_dir = norm_base / f"year={yr}"
        target_parquet, partition_rows, p_sha, _ = merge_and_write_liquidation_parquet(
            yr_dir, symbol, yr, yr_records
        )
        total_dataset_rows += partition_rows
        created_parquet_files.append(target_parquet)
        parquet_hashes.append(p_sha)

    # 5. Record Manifest (Idempotent Append)
    manifest_dir = root / "control" / "manifests"
    manifest_dir.mkdir(parents=True, exist_ok=True)
    manifest_file = manifest_dir / "bybit_linear_liquidations.jsonl"

    retrieved_iso = received_at.isoformat()
    manifest_record = {
        "action": "NORMALIZED",
        "exchange": "bybit",
        "market_type": "perpetual",
        "contract_type": "linear_perpetual",
        "venue_product_type": "linear",
        "symbol": symbol,
        "instrument_id": ident.instrument_id,
        "dataset_class": "liquidations",
        "observed_coverage_start": sorted_records[0].event_time.isoformat(),
        "observed_coverage_end": sorted_records[-1].event_time.isoformat(),
        "row_count": len(sorted_records),
        "total_accumulated_rows": total_dataset_rows,
        "source_claimed_completeness": "ALL_LIQUIDATIONS",
        "delivery_semantics": "BATCHED_500MS_PUSH",
        "raw_object_ref": str(raw_file.relative_to(root)).replace("\\", "/"),
        "raw_sha256": raw_hash,
        "raw_bytes": len(raw_bytes),
        "created_parquets": [str(p.relative_to(root)).replace("\\", "/") for p in created_parquet_files],
        "parquet_sha256": parquet_hashes,
        "parquet_bytes": sum(p.stat().st_size for p in created_parquet_files),
        "source_dataset_id": DATASET_ID,
        "source_contract_version": CONTRACT_ID,
        "schema_version": SCHEMA_VERSION,
        "collector_version": COLLECTOR_VERSION,
        "normalization_version": NORMALIZATION_VERSION,
        "known_limitations": [
            "price represents bankruptcy price, not execution fill price",
            "historical archive unavailable from venue; local history starts from first captured realtime event",
            "message label snapshot is batch envelope metadata tag and does not imply state replacement",
        ],
        "retrieved_at": retrieved_iso,
        "processed_at": utc_now().isoformat(),
    }

    existing_manifest_content = manifest_file.read_text(encoding="utf-8") if manifest_file.exists() else ""
    if raw_hash not in existing_manifest_content or any(h not in existing_manifest_content for h in parquet_hashes):
        with manifest_file.open("a", encoding="utf-8") as mf:
            mf.write(json.dumps(manifest_record) + "\n")

    # 6. Record Checkpoint
    chk_dir = root / "control" / "checkpoints"
    chk_dir.mkdir(parents=True, exist_ok=True)
    chk_file = chk_dir / f"bybit_linear_liquidations_{symbol}.json"
    chk_payload = {
        "symbol": symbol,
        "last_event_time_ms": int(sorted_records[-1].event_time.timestamp() * 1000),
        "last_event_time_iso": sorted_records[-1].event_time.isoformat(),
        "observed_source_coverage_start": sorted_records[0].event_time.isoformat(),
        "observed_source_coverage_end": sorted_records[-1].event_time.isoformat(),
        "batch_records": len(sorted_records),
        "total_records": total_dataset_rows,
        "source_claimed_completeness": "ALL_LIQUIDATIONS",
        "delivery_semantics": "BATCHED_500MS_PUSH",
        "updated_at": retrieved_iso,
    }
    chk_file.write_text(json.dumps(chk_payload, indent=2), encoding="utf-8")

    return {
        "symbol": symbol,
        "status": "PASS",
        "event_observation_status": "REAL_EVENT_OBSERVED",
        "records_count": len(sorted_records),
        "total_accumulated_rows": total_dataset_rows,
        "observed_source_coverage_start": sorted_records[0].event_time.isoformat(),
        "observed_source_coverage_end": sorted_records[-1].event_time.isoformat(),
        "raw_file": str(raw_file),
        "parquet_files": [str(p) for p in created_parquet_files],
    }


async def collect_bybit_liquidations_live(
    symbol: str,
    root: Path,
    *,
    ws_url: str = BYBIT_WS_LINEAR_URL,
    flush_interval_seconds: float = 5.0,
    max_duration_seconds: float | None = None,
    max_messages: int | None = None,
    min_disk_free_gb: float = 20.0,
) -> dict[str, Any]:
    """Asynchronous WebSocket collector for Bybit Linear real-time liquidations."""
    import websockets

    topic = f"allLiquidation.{symbol}"
    logger.info(f"Connecting to Bybit WebSocket: {ws_url} (subscribing to {topic})")

    buffered_raw_messages: list[tuple[dict[str, Any], str]] = []
    total_messages_received = 0
    total_records_persisted = 0
    start_time = time.time()
    last_flush_time = time.time()
    persist_results: list[dict[str, Any]] = []

    async with websockets.connect(ws_url, ping_interval=20, ping_timeout=10) as ws:
        sub_msg = {"op": "subscribe", "args": [topic]}
        await ws.send(json.dumps(sub_msg))
        ack_str = await ws.recv()
        ack = json.loads(ack_str)
        if not ack.get("success", False):
            raise RuntimeError(f"Bybit WebSocket subscription failed: {ack}")
        logger.info(f"Subscribed successfully to {topic}")

        while True:
            # Check duration limits
            if max_duration_seconds and (time.time() - start_time) >= max_duration_seconds:
                logger.info(f"Reached max duration {max_duration_seconds}s; stopping collection.")
                break
            if max_messages and total_messages_received >= max_messages:
                logger.info(f"Reached max messages {max_messages}; stopping collection.")
                break

            # Poll for message with timeout
            try:
                msg_str = await asyncio.wait_for(ws.recv(), timeout=1.0)
                msg = json.loads(msg_str)
                if msg.get("topic") == topic and "data" in msg:
                    buffered_raw_messages.append((msg, msg_str))
                    total_messages_received += 1
            except TimeoutError:
                pass

            # Flush buffer periodically if records exist
            now = time.time()
            if buffered_raw_messages and (now - last_flush_time >= flush_interval_seconds or len(buffered_raw_messages) >= 50):
                res = persist_bybit_liquidation_batch(
                    buffered_raw_messages,
                    symbol=symbol,
                    root=root,
                    received_at=utc_now(),
                    min_disk_free_gb=min_disk_free_gb,
                )
                persist_results.append(res)
                total_records_persisted += res.get("records_count", 0)
                buffered_raw_messages.clear()
                last_flush_time = now

        # Final flush on exit
        if buffered_raw_messages:
            res = persist_bybit_liquidation_batch(
                buffered_raw_messages,
                symbol=symbol,
                root=root,
                received_at=utc_now(),
                min_disk_free_gb=min_disk_free_gb,
            )
            persist_results.append(res)
            total_records_persisted += res.get("records_count", 0)
            buffered_raw_messages.clear()

    obs_status = "REAL_EVENT_OBSERVED" if total_records_persisted > 0 else "NO_EVENT_OBSERVED_WITHIN_WINDOW"

    return {
        "symbol": symbol,
        "status": "PASS",
        "transport_status": "PASS",
        "event_observation_status": obs_status,
        "total_messages_received": total_messages_received,
        "total_records_persisted": total_records_persisted,
        "duration_seconds": round(time.time() - start_time, 2),
        "flush_count": len(persist_results),
    }
