"""Binance USD-M BTCUSDT/ETHUSDT public liquidation observations.

The source is incomplete by venue design: at most one selected force-order
observation per symbol per 1000 ms is published. Current official materials
conflict on whether the selected order is the latest or largest, so the
normalization records that conflict and never claims a complete event stream.
"""

from __future__ import annotations

import asyncio
import hashlib
import json
import logging
import os
import tempfile
import time
from dataclasses import asdict, dataclass
from datetime import UTC, datetime, timedelta
from decimal import Decimal, InvalidOperation
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

DATASET_ID = "binance.usdm.liquidations.ws"
CONTRACT_ID = "binance.usdm.ws.liquidation-order.v1"
SCHEMA_VERSION = "1.0.0"
COLLECTOR_VERSION = "0.1.0"
NORMALIZATION_VERSION = "1.0.0"
DOCS_URL = (
    "https://developers.binance.com/en/docs/catalog/"
    "core-trading-derivatives-trading-usd-s-m-futures/api/ws-streams"
)
BINANCE_USDM_MARKET_WS_URL = "wss://fstream.binance.com/market/ws"

SOURCE_COMPLETENESS = "INCOMPLETE_THROTTLED_SNAPSHOT"
DELIVERY_SEMANTICS = "MAX_ONE_SELECTED_PER_SYMBOL_PER_1000MS"
SELECTION_RULE = "DOC_CONFLICT_LATEST_VS_LARGEST"
LOCAL_CAPTURE_COMPLETENESS = "OBSERVED_DURING_CONNECTED_WINDOW_ONLY"
SOURCE_WINDOW_MS = 1000
SUPPORTED_SYMBOLS = frozenset({"BTCUSDT", "ETHUSDT"})


def binance_usdm_liquidation_data_contract() -> DataContract:
    """Return the frozen, field-level public force-order contract."""
    fields = (
        ContractField(
            source_field="e",
            semantic_meaning="event_type",
            nullable=False,
            canonical_field="source_event_type",
            transformation="preserve",
            validation_rules=("equals_forceOrder",),
        ),
        ContractField(
            source_field="E",
            semantic_meaning="websocket_event_time",
            source_unit="epoch_ms",
            timestamp_meaning="exchange_event_push_time",
            nullable=False,
            canonical_field="exchange_timestamp",
            transformation="epoch_ms_to_utc",
            normalized_unit="UTC",
        ),
        ContractField(
            source_field="o",
            semantic_meaning="selected_force_liquidation_order_snapshot",
            source_unit="object",
            nullable=False,
            canonical_field="source_order",
            transformation="preserve_raw",
        ),
        ContractField(
            source_field="o.s",
            semantic_meaning="native_symbol",
            nullable=False,
            canonical_field="symbol",
            transformation="exact_match",
            validation_rules=("matches_requested_symbol",),
        ),
        ContractField(
            source_field="o.S",
            semantic_meaning="forced_liquidation_order_side",
            nullable=False,
            canonical_field="source_side",
            transformation="preserve",
            validation_rules=("in_BUY_SELL",),
            known_limitations=("position_side_liquidated remains UNKNOWN",),
        ),
        ContractField(
            source_field="o.o",
            semantic_meaning="order_type",
            nullable=False,
            canonical_field="order_type",
            transformation="preserve",
        ),
        ContractField(
            source_field="o.f",
            semantic_meaning="time_in_force",
            nullable=False,
            canonical_field="time_in_force",
            transformation="preserve",
        ),
        ContractField(
            source_field="o.q",
            semantic_meaning="original_order_quantity",
            source_unit="canonical_instrument_base_asset",
            nullable=False,
            canonical_field="source_quantity",
            transformation="preserve_decimal_string",
            normalized_unit="canonical_instrument_base_asset",
        ),
        ContractField(
            source_field="o.p",
            semantic_meaning="order_price",
            source_unit="canonical_quote_per_base_asset",
            nullable=False,
            canonical_field="source_price",
            transformation="preserve_decimal_string",
            normalized_unit="canonical_quote_per_base_asset",
        ),
        ContractField(
            source_field="o.ap",
            semantic_meaning="average_price",
            source_unit="canonical_quote_per_base_asset",
            nullable=False,
            canonical_field="average_fill_price",
            transformation="preserve_decimal_string",
            normalized_unit="canonical_quote_per_base_asset",
        ),
        ContractField(
            source_field="o.X",
            semantic_meaning="order_status",
            nullable=False,
            canonical_field="order_status",
            transformation="preserve",
        ),
        ContractField(
            source_field="o.l",
            semantic_meaning="order_last_filled_quantity",
            source_unit="canonical_instrument_base_asset",
            nullable=False,
            canonical_field="last_filled_quantity",
            transformation="preserve_decimal_string",
            normalized_unit="canonical_instrument_base_asset",
        ),
        ContractField(
            source_field="o.z",
            semantic_meaning="order_filled_accumulated_quantity",
            source_unit="canonical_instrument_base_asset",
            nullable=False,
            canonical_field="accumulated_filled_quantity",
            transformation="preserve_decimal_string",
            normalized_unit="canonical_instrument_base_asset",
        ),
        ContractField(
            source_field="o.T",
            semantic_meaning="order_trade_time",
            source_unit="epoch_ms",
            timestamp_meaning="order_trade_time",
            nullable=False,
            canonical_field="event_time",
            transformation="epoch_ms_to_utc",
            normalized_unit="UTC",
        ),
    )
    return DataContract(
        contract_id=CONTRACT_ID,
        source_dataset_id=DATASET_ID,
        exchange="binance",
        market_type="perpetual",
        source_kind="websocket",
        official_documentation_url=DOCS_URL,  # type: ignore[arg-type]
        verified_at=datetime(2026, 8, 11, tzinfo=UTC),
        schema_version=SCHEMA_VERSION,
        fields=fields,
    )


@dataclass(frozen=True)
class BinanceLiquidationRecord:
    exchange: str
    instrument_id: str
    symbol: str
    market_type: str
    contract_type: str
    venue_product_type: str
    event_time: datetime
    exchange_timestamp: datetime
    received_at: datetime
    processed_at: datetime
    knowledge_time: datetime
    source_event_time_ms: int
    source_order_trade_time_ms: int
    position_side_liquidated: str
    source_side: str
    source_side_semantic: str
    source_quantity: str
    source_quantity_unit: str
    quantity_semantic: str
    quantity_base: str
    notional_quote: str | None
    last_filled_quantity: str
    accumulated_filled_quantity: str
    source_price: str
    price_semantic: str
    average_fill_price: str
    order_type: str
    time_in_force: str
    order_status: str
    source_claimed_completeness: str
    delivery_semantics: str
    source_window_ms: int
    max_emitted_per_symbol_per_window: int
    selection_rule: str
    local_capture_completeness: str
    native_event_id: str | None
    native_sequence_id: str | None
    message_id: str
    dedup_fingerprint: str
    dedup_guarantee: str
    source: str
    source_contract_version: str
    schema_version: str
    collector_version: str
    normalization_version: str
    dq_flags: tuple[str, ...]


BINANCE_LIQUIDATION_SCHEMA = pa.schema(
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
        ("source_event_time_ms", pa.int64()),
        ("source_order_trade_time_ms", pa.int64()),
        ("position_side_liquidated", pa.string()),
        ("source_side", pa.string()),
        ("source_side_semantic", pa.string()),
        ("source_quantity", pa.string()),
        ("source_quantity_unit", pa.string()),
        ("quantity_semantic", pa.string()),
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
        ("source_window_ms", pa.int32()),
        ("max_emitted_per_symbol_per_window", pa.int8()),
        ("selection_rule", pa.string()),
        ("local_capture_completeness", pa.string()),
        ("native_event_id", pa.string()),
        ("native_sequence_id", pa.string()),
        ("message_id", pa.string()),
        ("dedup_fingerprint", pa.string()),
        ("dedup_guarantee", pa.string()),
        ("source", pa.string()),
        ("source_contract_version", pa.string()),
        ("schema_version", pa.string()),
        ("collector_version", pa.string()),
        ("normalization_version", pa.string()),
        ("dq_flags", pa.list_(pa.string())),
    ]
)


def _require_text(value: Any, field: str) -> str:
    if value is None or not str(value).strip():
        raise ValueError(f"Missing mandatory field '{field}'")
    return str(value).strip()


def _decimal_lexeme(value: Any, field: str, *, strictly_positive: bool) -> str:
    lexeme = _require_text(value, field)
    try:
        number = Decimal(lexeme)
    except InvalidOperation as exc:
        raise ValueError(f"Invalid decimal in '{field}': {lexeme}") from exc
    if not number.is_finite() or number < 0 or (strictly_positive and number == 0):
        rule = "strictly positive" if strictly_positive else "non-negative"
        raise ValueError(f"Field '{field}' must be {rule}: {lexeme}")
    return lexeme


def _validate_identity(identity: InstrumentIdentity) -> None:
    if identity.native_symbol not in SUPPORTED_SYMBOLS:
        raise ValueError("Binance USD-M liquidations permit BTCUSDT/ETHUSDT only")
    expected = funding_identity(identity.native_symbol)
    if identity != expected:
        raise ValueError("Binance liquidation identity must match the canonical USD-M instrument")


def parse_binance_liquidation_message(
    raw_msg: dict[str, Any],
    identity: InstrumentIdentity,
    received_at: datetime,
    *,
    raw_msg_str: str,
) -> BinanceLiquidationRecord:
    """Parse one source-faithful `<symbol>@forceOrder` text envelope."""
    _validate_identity(identity)
    if received_at.tzinfo is None:
        raise ValueError("received_at must be timezone-aware")
    received_at = received_at.astimezone(UTC)
    if "\n" in raw_msg_str or "\r" in raw_msg_str:
        raise ValueError("raw WebSocket envelope must be a single JSONL-safe text frame")
    if raw_msg.get("e") != "forceOrder":
        raise ValueError("Binance liquidation event type must equal 'forceOrder'")
    raw_e = raw_msg.get("E")
    if raw_e is None:
        raise ValueError("Missing mandatory field 'E'")
    order = raw_msg.get("o")
    if not isinstance(order, dict):
        raise ValueError("Missing mandatory object 'o'")
    symbol = _require_text(order.get("s"), "o.s")
    if symbol != identity.native_symbol:
        raise ValueError(f"Symbol mismatch: expected '{identity.native_symbol}', got '{symbol}'")

    side = _require_text(order.get("S"), "o.S")
    if side not in {"BUY", "SELL"}:
        raise ValueError(f"Unsupported forced-order side: {side}")
    order_type = _require_text(order.get("o"), "o.o")
    tif = _require_text(order.get("f"), "o.f")
    status = _require_text(order.get("X"), "o.X")
    original_qty = _decimal_lexeme(order.get("q"), "o.q", strictly_positive=True)
    order_price = _decimal_lexeme(order.get("p"), "o.p", strictly_positive=False)
    average_price = _decimal_lexeme(order.get("ap"), "o.ap", strictly_positive=False)
    last_qty = _decimal_lexeme(order.get("l"), "o.l", strictly_positive=False)
    accumulated_qty = _decimal_lexeme(order.get("z"), "o.z", strictly_positive=False)
    raw_t = order.get("T")
    if raw_t is None:
        raise ValueError("Missing mandatory field 'o.T'")
    event_ms = int(raw_e)
    trade_ms = int(raw_t)
    exchange_timestamp = parse_epoch(event_ms, unit="ms")
    event_time = parse_epoch(trade_ms, unit="ms")
    processed_at = utc_now()

    message_id = hashlib.sha256(raw_msg_str.encode("utf-8")).hexdigest()
    dedup_fingerprint = hashlib.sha256(
        f"binance|{identity.instrument_id}|{message_id}|0".encode()
    ).hexdigest()
    dq_flags = ["SOURCE_SELECTION_INCOMPLETENESS"]
    future_tolerance = received_at + timedelta(minutes=5)
    if event_time > future_tolerance or exchange_timestamp > future_tolerance:
        dq_flags.append("SOURCE_CLOCK_SKEW_FUTURE_TIMESTAMP")
    if Decimal(last_qty) > Decimal(accumulated_qty) or Decimal(accumulated_qty) > Decimal(
        original_qty
    ):
        dq_flags.append("SOURCE_QUANTITY_RELATION_ANOMALY")

    return BinanceLiquidationRecord(
        exchange="binance",
        instrument_id=identity.instrument_id,
        symbol=symbol,
        market_type="perpetual",
        contract_type="linear_perpetual",
        venue_product_type="usdm",
        event_time=event_time,
        exchange_timestamp=exchange_timestamp,
        received_at=received_at,
        processed_at=processed_at,
        knowledge_time=received_at,
        source_event_time_ms=event_ms,
        source_order_trade_time_ms=trade_ms,
        position_side_liquidated="UNKNOWN",
        source_side=side,
        source_side_semantic="FORCED_LIQUIDATION_ORDER_SIDE",
        source_quantity=original_qty,
        source_quantity_unit=identity.quantity_unit,
        quantity_semantic="ORIGINAL_ORDER_QUANTITY",
        quantity_base=original_qty,
        notional_quote=None,
        last_filled_quantity=last_qty,
        accumulated_filled_quantity=accumulated_qty,
        source_price=order_price,
        price_semantic="ORDER_PRICE",
        average_fill_price=average_price,
        order_type=order_type,
        time_in_force=tif,
        order_status=status,
        source_claimed_completeness=SOURCE_COMPLETENESS,
        delivery_semantics=DELIVERY_SEMANTICS,
        source_window_ms=SOURCE_WINDOW_MS,
        max_emitted_per_symbol_per_window=1,
        selection_rule=SELECTION_RULE,
        local_capture_completeness=LOCAL_CAPTURE_COMPLETENESS,
        native_event_id=None,
        native_sequence_id=None,
        message_id=message_id,
        dedup_fingerprint=dedup_fingerprint,
        dedup_guarantee="EXACT_WIRE_REPLAY_ONLY",
        source=DATASET_ID,
        source_contract_version=CONTRACT_ID,
        schema_version=SCHEMA_VERSION,
        collector_version=COLLECTOR_VERSION,
        normalization_version=NORMALIZATION_VERSION,
        dq_flags=tuple(dq_flags),
    )


def validate_binance_liquidation_records(records: list[BinanceLiquidationRecord]) -> list[str]:
    issues: list[str] = []
    for index, record in enumerate(records):
        try:
            expected = funding_identity(record.symbol)
        except ValueError:
            expected = None
        if expected is None or (
            record.exchange,
            record.instrument_id,
            record.source_quantity_unit,
        ) != ("binance", expected.instrument_id, expected.quantity_unit):
            issues.append(f"Row {index}: canonical identity mismatch")
        if record.knowledge_time != record.received_at:
            issues.append(f"Row {index}: knowledge_time must equal received_at")
        if record.position_side_liquidated != "UNKNOWN":
            issues.append(f"Row {index}: unsupported position-side inference")
        if record.selection_rule != SELECTION_RULE or record.source_window_ms != 1000:
            issues.append(f"Row {index}: source selection semantics mismatch")
        if record.source_claimed_completeness != SOURCE_COMPLETENESS:
            issues.append(f"Row {index}: false completeness claim")
        if "SOURCE_SELECTION_INCOMPLETENESS" not in record.dq_flags:
            issues.append(f"Row {index}: missing source-incompleteness DQ flag")
    return issues


def records_to_table(records: list[BinanceLiquidationRecord]) -> pa.Table:
    rows = []
    for record in records:
        row = asdict(record)
        row["dq_flags"] = list(record.dq_flags)
        rows.append(row)
    return pa.Table.from_pylist(rows, schema=BINANCE_LIQUIDATION_SCHEMA)


def _table_rows_without_zoneinfo(table: pa.Table) -> list[dict[str, Any]]:
    """Convert Arrow rows without asking Windows for an external UTC tzdata file."""
    timestamp_names = {field.name for field in table.schema if pa.types.is_timestamp(field.type)}
    timestamp_values = {name: table[name].cast(pa.int64()) for name in timestamp_names}
    rows: list[dict[str, Any]] = []
    for index in range(table.num_rows):
        row: dict[str, Any] = {}
        for name in table.schema.names:
            if name in timestamp_names:
                value_us = timestamp_values[name][index].as_py()
                row[name] = (
                    None
                    if value_us is None
                    else datetime.fromtimestamp(value_us / 1_000_000, tz=UTC)
                )
            else:
                row[name] = table[name][index].as_py()
        rows.append(row)
    return rows


def _sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _fsync_path(path: Path) -> None:
    with path.open("r+b") as handle:
        handle.flush()
        os.fsync(handle.fileno())


def _write_json_atomic(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    partial = path.with_suffix(path.suffix + ".partial")
    with partial.open("w", encoding="utf-8", newline="\n") as handle:
        json.dump(payload, handle, indent=2, sort_keys=True)
        handle.write("\n")
        handle.flush()
        os.fsync(handle.fileno())
    os.replace(partial, path)


def merge_and_write_binance_liquidation_parquet(
    year_dir: Path,
    records: list[BinanceLiquidationRecord],
) -> tuple[Path, int, str, int]:
    """Publish an immutable content-addressed cumulative generation."""
    rows_by_key: dict[tuple[str, str, datetime, str], dict[str, Any]] = {}
    if year_dir.exists():
        for existing_path in sorted(year_dir.glob("part-*.parquet")):
            existing = pq.ParquetFile(existing_path).read()
            if existing.schema != BINANCE_LIQUIDATION_SCHEMA:
                raise ValueError(f"Incompatible Binance liquidation schema: {existing_path}")
            for row in _table_rows_without_zoneinfo(existing):
                key = (
                    row["exchange"],
                    row["instrument_id"],
                    row["event_time"],
                    row["dedup_fingerprint"],
                )
                rows_by_key.setdefault(key, row)
    new_rows = []
    for record in records:
        row = asdict(record)
        row["dq_flags"] = list(record.dq_flags)
        new_rows.append(row)
    for row in new_rows:
        key = (row["exchange"], row["instrument_id"], row["event_time"], row["dedup_fingerprint"])
        rows_by_key.setdefault(key, row)

    ordered = sorted(
        rows_by_key.values(), key=lambda row: (row["event_time"], row["dedup_fingerprint"])
    )
    fingerprint = "\n".join(
        f"{row['instrument_id']}|{row['source_order_trade_time_ms']}|{row['dedup_fingerprint']}"
        for row in ordered
    )
    generation_hash = hashlib.sha256(fingerprint.encode()).hexdigest()[:12]
    symbols = {row["symbol"] for row in ordered}
    if len(symbols) != 1:
        raise ValueError("A Binance liquidation generation cannot mix symbols")
    symbol = symbols.pop()
    year = ordered[0]["event_time"].year
    year_dir.mkdir(parents=True, exist_ok=True)
    target = year_dir / f"part-{symbol.lower()}_{year}_{generation_hash}.parquet"
    if target.exists():
        return target, len(ordered), _sha256_file(target), target.stat().st_size

    partial = target.with_suffix(".parquet.partial")
    table = pa.Table.from_pylist(ordered, schema=BINANCE_LIQUIDATION_SCHEMA)
    pq.write_table(table, partial, compression="zstd")
    if pq.ParquetFile(partial).metadata.num_rows != len(ordered):
        raise ValueError("Binance liquidation Parquet row-count validation failed")
    _fsync_path(partial)
    os.replace(partial, target)
    return target, len(ordered), _sha256_file(target), target.stat().st_size


RawMessage = dict[str, Any] | tuple[dict[str, Any], str] | tuple[dict[str, Any], str, datetime]


def persist_binance_liquidation_batch(
    raw_messages: list[RawMessage],
    root: Path,
    *,
    symbol: str = "BTCUSDT",
    received_at: datetime | None = None,
    min_disk_free_gb: float = 20.0,
    ingestion_run_id: str | None = None,
    session_id: str | None = None,
) -> dict[str, Any]:
    """Persist genuine or fixture messages with raw/normalized/control isolation."""
    if symbol not in SUPPORTED_SYMBOLS:
        raise ValueError("Binance USD-M liquidations permit BTCUSDT/ETHUSDT only")
    if not raw_messages:
        return {
            "symbol": symbol,
            "status": "PASS",
            "event_observation_status": "NO_EVENT_OBSERVED_WITHIN_WINDOW",
            "records_count": 0,
        }
    free_gb = disk_free_bytes(root) / (1024**3)
    if free_gb < min_disk_free_gb:
        raise OSError(f"Disk space below threshold: {free_gb:.2f} GB < {min_disk_free_gb} GB")

    identity = funding_identity(symbol)
    fallback_received = (received_at or utc_now()).astimezone(UTC)
    parsed: list[tuple[dict[str, Any], str, datetime, BinanceLiquidationRecord]] = []
    for item in raw_messages:
        if isinstance(item, tuple) and len(item) == 3:
            payload, raw_text, observed_at = item
        elif isinstance(item, tuple):
            payload, raw_text = item
            observed_at = fallback_received
        else:
            payload = item
            raw_text = json.dumps(item, separators=(",", ":"), ensure_ascii=False)
            observed_at = fallback_received
        record = parse_binance_liquidation_message(
            payload,
            identity,
            observed_at,
            raw_msg_str=raw_text,
        )
        parsed.append((payload, raw_text, observed_at, record))

    records = [entry[3] for entry in parsed]
    issues = validate_binance_liquidation_records(records)
    if issues:
        raise ValueError(f"Binance liquidation DQ validation failed: {issues[:5]}")
    event_dates = {record.event_time.date() for record in records}
    if len(event_dates) != 1:
        raise ValueError("A Binance liquidation persistence batch cannot span UTC dates")

    unique_records: dict[str, BinanceLiquidationRecord] = {}
    for record in records:
        unique_records.setdefault(record.dedup_fingerprint, record)
    ordered_records = sorted(
        unique_records.values(), key=lambda record: (record.event_time, record.dedup_fingerprint)
    )
    raw_texts = [entry[1] for entry in parsed]
    raw_bytes = ("\n".join(raw_texts) + "\n").encode("utf-8")
    raw_hash = hashlib.sha256(raw_bytes).hexdigest()
    coverage_start = ordered_records[0].event_time
    coverage_end = ordered_records[-1].event_time
    date_str = coverage_start.strftime("%Y-%m-%d")
    raw_dir = root / "raw" / "binance" / "perpetual" / "liquidations" / symbol / f"date={date_str}"
    raw_dir.mkdir(parents=True, exist_ok=True)
    raw_file = raw_dir / (
        f"liq_{coverage_start:%Y%m%dT%H%M%SZ}_{coverage_end:%Y%m%dT%H%M%SZ}_{raw_hash[:8]}.jsonl"
    )
    if not raw_file.exists():
        with tempfile.NamedTemporaryFile(
            "wb", dir=raw_dir, delete=False, suffix=".partial"
        ) as handle:
            handle.write(raw_bytes)
            handle.flush()
            os.fsync(handle.fileno())
            partial_raw = Path(handle.name)
        os.replace(partial_raw, raw_file)
    elif _sha256_file(raw_file) != raw_hash:
        raise ValueError("Existing raw Binance liquidation object hash conflict")

    records_by_year: dict[int, list[BinanceLiquidationRecord]] = {}
    for record in ordered_records:
        records_by_year.setdefault(record.event_time.year, []).append(record)
    parquet_files: list[Path] = []
    parquet_hashes: list[str] = []
    total_rows = 0
    new_rows_persisted = 0
    for year, year_records in sorted(records_by_year.items()):
        year_dir = (
            root
            / "normalized"
            / "liquidations"
            / "v1"
            / "exchange=binance"
            / "market_type=perpetual"
            / f"symbol={symbol}"
            / f"year={year}"
        )
        previous_rows = max(
            (pq.ParquetFile(path).metadata.num_rows for path in year_dir.glob("part-*.parquet")),
            default=0,
        )
        parquet, rows, parquet_hash, _ = merge_and_write_binance_liquidation_parquet(
            year_dir, year_records
        )
        parquet_files.append(parquet)
        parquet_hashes.append(parquet_hash)
        total_rows += rows
        new_rows_persisted += max(0, rows - previous_rows)

    manifest_dir = root / "control" / "manifests"
    manifest_dir.mkdir(parents=True, exist_ok=True)
    manifest_file = manifest_dir / "binance_usdm_liquidations.jsonl"
    manifest = {
        "action": "NORMALIZED",
        "exchange": "binance",
        "market_type": "perpetual",
        "contract_type": "linear_perpetual",
        "venue_product_type": "usdm",
        "symbol": symbol,
        "instrument_id": identity.instrument_id,
        "dataset_class": "liquidations",
        "observed_coverage_start": coverage_start.isoformat(),
        "observed_coverage_end": coverage_end.isoformat(),
        "raw_message_count": len(raw_messages),
        "event_count": len(ordered_records),
        "observation_count": len(records),
        "row_count": len(ordered_records),
        "total_accumulated_rows": total_rows,
        "source_claimed_completeness": SOURCE_COMPLETENESS,
        "delivery_semantics": DELIVERY_SEMANTICS,
        "source_window_ms": SOURCE_WINDOW_MS,
        "max_emitted_per_symbol_per_window": 1,
        "selection_rule": SELECTION_RULE,
        "local_capture_completeness": LOCAL_CAPTURE_COMPLETENESS,
        "raw_object_ref": str(raw_file.relative_to(root)).replace("\\", "/"),
        "raw_sha256": raw_hash,
        "raw_bytes": len(raw_bytes),
        "created_parquets": [
            str(path.relative_to(root)).replace("\\", "/") for path in parquet_files
        ],
        "parquet_sha256": parquet_hashes,
        "parquet_bytes": sum(path.stat().st_size for path in parquet_files),
        "source_dataset_id": DATASET_ID,
        "source_contract_version": CONTRACT_ID,
        "schema_version": SCHEMA_VERSION,
        "collector_version": COLLECTOR_VERSION,
        "normalization_version": NORMALIZATION_VERSION,
        "dq_flags": ["SOURCE_SELECTION_INCOMPLETENESS"],
        "known_limitations": [
            "latest-versus-largest selection rule is an unresolved official documentation conflict",
            "not ground truth for event count, cascade reconstruction, or liquidation volume",
            "no public historical backfill source verified; missed observations are unrecoverable or unknown",
        ],
        "ingestion_run_id": ingestion_run_id,
        "session_id": session_id,
        "retrieved_at": max(entry[2] for entry in parsed).isoformat(),
        "processed_at": utc_now().isoformat(),
    }
    manifest_key = (manifest["raw_sha256"], tuple(manifest["parquet_sha256"]))
    existing_manifest_keys: set[tuple[str, tuple[str, ...]]] = set()
    if manifest_file.exists():
        for line in manifest_file.read_text(encoding="utf-8").splitlines():
            existing = json.loads(line)
            existing_manifest_keys.add(
                (existing.get("raw_sha256", ""), tuple(existing.get("parquet_sha256", [])))
            )
    if manifest_key not in existing_manifest_keys:
        with manifest_file.open("a", encoding="utf-8", newline="\n") as handle:
            handle.write(json.dumps(manifest, sort_keys=True) + "\n")
            handle.flush()
            os.fsync(handle.fileno())

    checkpoint = root / "control" / "checkpoints" / f"binance_usdm_liquidations_{symbol}.json"
    _write_json_atomic(
        checkpoint,
        {
            "source_dataset_id": DATASET_ID,
            "source_contract_version": CONTRACT_ID,
            "instrument_id": identity.instrument_id,
            "symbol": symbol,
            "last_event_time": coverage_end.isoformat(),
            "last_knowledge_time": max(record.knowledge_time for record in records).isoformat(),
            "last_message_id": ordered_records[-1].message_id,
            "batch_records": len(ordered_records),
            "total_records": total_rows,
            "last_raw_object_ref": manifest["raw_object_ref"],
            "last_raw_sha256": manifest["raw_sha256"],
            "last_parquet_refs": manifest["created_parquets"],
            "last_parquet_sha256": manifest["parquet_sha256"],
            "source_claimed_completeness": SOURCE_COMPLETENESS,
            "selection_rule": SELECTION_RULE,
            "updated_at": utc_now().isoformat(),
        },
    )
    return {
        "symbol": symbol,
        "status": "PASS",
        "event_observation_status": "REAL_EVENT_OBSERVED",
        "records_count": len(ordered_records),
        "new_rows_persisted": new_rows_persisted,
        "total_accumulated_rows": total_rows,
        "observed_source_coverage_start": coverage_start.isoformat(),
        "observed_source_coverage_end": coverage_end.isoformat(),
        "raw_file": str(raw_file),
        "parquet_files": [str(path) for path in parquet_files],
    }


def _quarantine_failed_live_buffer(
    root: Path,
    symbol: str,
    buffered: list[tuple[dict[str, Any], str, datetime]],
    error: BaseException,
) -> Path:
    """Durably retain received wire frames that could not be normalized."""
    raw_bytes = ("\n".join(raw for _, raw, _ in buffered) + "\n").encode("utf-8")
    digest = hashlib.sha256(raw_bytes).hexdigest()
    directory = root / "quarantine" / "liquidation_rejected_frames" / "binance" / symbol
    directory.mkdir(parents=True, exist_ok=True)
    target = directory / f"rejected_{digest}.jsonl"
    if not target.exists():
        with tempfile.NamedTemporaryFile(
            "wb", dir=directory, delete=False, suffix=".partial"
        ) as handle:
            handle.write(raw_bytes)
            handle.flush()
            os.fsync(handle.fileno())
            partial = Path(handle.name)
        os.replace(partial, target)
    reason = target.with_suffix(".reason.json")
    if not reason.exists():
        payload = {
            "source_dataset_id": DATASET_ID,
            "source_contract_version": CONTRACT_ID,
            "symbol": symbol,
            "raw_sha256": digest,
            "raw_message_count": len(buffered),
            "reason_code": "PERSISTENCE_OR_NORMALIZATION_REJECTED",
            "error_type": type(error).__name__,
            "error": str(error),
            "quarantined_at": utc_now().isoformat(),
        }
        with reason.open("w", encoding="utf-8", newline="\n") as handle:
            json.dump(payload, handle, sort_keys=True)
            handle.write("\n")
            handle.flush()
            os.fsync(handle.fileno())
    return target


async def collect_binance_liquidations_live(
    root: Path,
    *,
    symbol: str = "BTCUSDT",
    ws_url: str = BINANCE_USDM_MARKET_WS_URL,
    flush_interval_seconds: float = 5.0,
    max_duration_seconds: float = 60.0,
    max_messages: int | None = None,
    min_disk_free_gb: float = 20.0,
    ingestion_run_id: str | None = None,
    session_id: str | None = None,
) -> dict[str, Any]:
    """Collect a bounded symbol-specific public market stream."""
    import websockets

    if symbol not in SUPPORTED_SYMBOLS:
        raise ValueError("Binance USD-M liquidations permit BTCUSDT/ETHUSDT only")
    topic = f"{symbol.lower()}@forceOrder"
    buffered: list[tuple[dict[str, Any], str, datetime]] = []
    total_messages = 0
    total_source_events_observed = 0
    total_records = 0
    flush_count = 0
    started_at = utc_now()
    started = time.monotonic()
    last_flush = started
    heartbeat_status = "NOT_CHECKED"
    connected_at: datetime | None = None
    subscribed_at: datetime | None = None
    wire_sha256_seen: list[str] = []
    first_event_time: str | None = None
    last_event_time: str | None = None

    async with websockets.connect(ws_url, ping_interval=20, ping_timeout=10) as websocket:
        connected_at = utc_now()
        await websocket.send(json.dumps({"method": "SUBSCRIBE", "params": [topic], "id": 1}))
        ack_deadline = time.monotonic() + 10
        while True:
            remaining = ack_deadline - time.monotonic()
            if remaining <= 0:
                raise TimeoutError("Binance WebSocket subscription acknowledgement timed out")
            raw_ack = await asyncio.wait_for(websocket.recv(), timeout=remaining)
            ack_received = utc_now()
            ack = json.loads(raw_ack)
            if ack.get("id") == 1:
                if ack.get("result") is not None:
                    raise RuntimeError(f"Binance WebSocket subscription failed: {ack}")
                subscribed_at = utc_now()
                break
            if ack.get("e") == "forceOrder":
                buffered.append((ack, raw_ack, ack_received))
                total_messages += 1
                total_source_events_observed += 1
                wire_sha256_seen.append(hashlib.sha256(raw_ack.encode()).hexdigest())

        pong_waiter = await websocket.ping()
        await asyncio.wait_for(pong_waiter, timeout=10)
        heartbeat_status = "PASS"

        while True:
            now = time.monotonic()
            if now - started >= max_duration_seconds:
                break
            if max_messages is not None and total_messages >= max_messages:
                break
            try:
                raw_text = await asyncio.wait_for(websocket.recv(), timeout=1.0)
                observed_at = utc_now()
                payload = json.loads(raw_text)
                if payload.get("e") == "forceOrder":
                    buffered.append((payload, raw_text, observed_at))
                    total_messages += 1
                    total_source_events_observed += 1
                    wire_sha256_seen.append(hashlib.sha256(raw_text.encode()).hexdigest())
            except TimeoutError:
                pass

            now = time.monotonic()
            if buffered and (now - last_flush >= flush_interval_seconds or len(buffered) >= 50):
                try:
                    result = persist_binance_liquidation_batch(
                        buffered,
                        root,
                        symbol=symbol,
                        min_disk_free_gb=min_disk_free_gb,
                        ingestion_run_id=ingestion_run_id,
                        session_id=session_id,
                    )
                except Exception as error:
                    _quarantine_failed_live_buffer(root, symbol, buffered, error)
                    raise
                total_records += result["new_rows_persisted"]
                first_event_time = first_event_time or result.get("observed_source_coverage_start")
                last_event_time = result.get("observed_source_coverage_end") or last_event_time
                flush_count += 1
                buffered.clear()
                last_flush = now

        if buffered:
            try:
                result = persist_binance_liquidation_batch(
                    buffered,
                    root,
                    symbol=symbol,
                    min_disk_free_gb=min_disk_free_gb,
                    ingestion_run_id=ingestion_run_id,
                    session_id=session_id,
                )
            except Exception as error:
                _quarantine_failed_live_buffer(root, symbol, buffered, error)
                raise
            total_records += result["new_rows_persisted"]
            first_event_time = first_event_time or result.get("observed_source_coverage_start")
            last_event_time = result.get("observed_source_coverage_end") or last_event_time
            flush_count += 1

    ended_at = utc_now()
    return {
        "symbol": symbol,
        "topic": topic,
        "ws_endpoint": ws_url,
        "status": "PASS",
        "transport_status": "PASS",
        "subscription_status": "PASS",
        "heartbeat_liveness": heartbeat_status,
        "event_observation_status": (
            "REAL_EVENT_OBSERVED"
            if total_source_events_observed
            else "NO_EVENT_OBSERVED_WITHIN_WINDOW"
        ),
        "capture_completeness": LOCAL_CAPTURE_COMPLETENESS,
        "source_claimed_completeness": SOURCE_COMPLETENESS,
        "selection_rule": SELECTION_RULE,
        "total_messages_received": total_messages,
        "total_source_events_observed": total_source_events_observed,
        "total_records_persisted": total_records,
        "wire_sha256_seen": wire_sha256_seen,
        "first_event_time": first_event_time,
        "last_event_time": last_event_time,
        "duration_seconds": round(time.monotonic() - started, 2),
        "flush_count": flush_count,
        "ingestion_run_id": ingestion_run_id,
        "session_id": session_id,
        "started_at": started_at.isoformat(),
        "connected_at": connected_at.isoformat() if connected_at else None,
        "subscribed_at": subscribed_at.isoformat() if subscribed_at else None,
        "ended_at": ended_at.isoformat(),
        "termination_reason": "BOUNDED_SOAK_COMPLETED",
        "queue_mode": "NOT_APPLICABLE_SYNCHRONOUS_READ_FLUSH",
        "queue_high_water_mark": None,
        "queue_capacity": None,
        "dropped_messages": 0,
    }
