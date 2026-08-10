"""Exchange Aggregate Trade ingestion adapter.

Strict Dataset Isolation Invariant:
individual_trade != exchange_aggregate_trade != derived_trade_bucket.

Exchange aggregate trades (e.g. Binance aggTrades) represent exchange-side combined executions
and MUST NOT be implicitly converted to or substituted for individual trades.
"""

from __future__ import annotations

from datetime import date
from decimal import Decimal

import pyarrow as pa

from ...time import utc_now

DEC = pa.decimal128(38, 18)

EXCHANGE_AGGREGATE_TRADE_SCHEMA = pa.schema(
    [
        pa.field("instrument_id", pa.string(), False),
        pa.field("exchange", pa.string(), False),
        pa.field("market_type", pa.string(), False),
        pa.field("contract_type", pa.string(), False),
        pa.field("native_symbol", pa.string(), False),
        pa.field("dataset_class", pa.string(), False),
        pa.field("source_dataset_id", pa.string(), False),
        pa.field("aggregate_trade_id", pa.string(), False),
        pa.field("first_trade_id", pa.string(), False),
        pa.field("last_trade_id", pa.string(), False),
        pa.field("source_ordinal", pa.int64(), False),
        pa.field("event_time", pa.timestamp("us", tz="UTC"), False),
        pa.field("exchange_timestamp", pa.timestamp("us", tz="UTC"), False),
        pa.field("price", DEC, False),
        pa.field("quantity", DEC, False),
        pa.field("quantity_unit", pa.string(), False),
        pa.field("quote_quantity", DEC, False),
        pa.field("notional_unit", pa.string(), False),
        pa.field("taker_side", pa.string(), False),
        pa.field("signed_quantity", DEC, False),
        pa.field("is_buyer_maker", pa.bool_(), False),
        pa.field("is_best_match", pa.bool_(), True),
        pa.field("received_at", pa.timestamp("us", tz="UTC"), False),
        pa.field("processed_at", pa.timestamp("us", tz="UTC"), False),
        pa.field("source_uri", pa.string(), False),
        pa.field("source_object_sha256", pa.string(), False),
        pa.field("schema_version", pa.string(), False),
        pa.field("dq_flags", pa.list_(pa.string()), True),
    ]
)


def validate_dataset_class_isolation(table_or_batch: pa.Table | pa.RecordBatch, expected_class: str) -> None:
    """Enforce dataset class isolation at runtime. Fails closed on mismatch."""
    if "dataset_class" not in table_or_batch.schema.names:
        raise ValueError("schema missing mandatory dataset_class field")
    dataset_classes = set(table_or_batch.column("dataset_class").to_pylist())
    if any(dc != expected_class for dc in dataset_classes):
        raise TypeError(
            f"Dataset class mismatch: expected '{expected_class}', got {dataset_classes}. "
            f"Aggregate trades and individual trades must not be mixed."
        )


def build_binance_aggregate_trade_batch(
    rows: list[dict[str, str] | list[str]],
    market_type: str,
    symbol: str,
    date_val: date,
    source_uri: str,
    source_sha256: str,
    start_ordinal: int = 1,
) -> tuple[pa.RecordBatch, int]:
    instrument_id = f"binance:{market_type}:{symbol}"
    contract_type = "spot" if market_type == "spot" else "linear_perpetual"
    source_dataset_id = f"binance.{market_type}.exchange_aggregate_trade.archive"

    now_dt = utc_now()
    now_us = int(now_dt.timestamp() * 1_000_000)

    col_instrument_id = []
    col_exchange = []
    col_market_type = []
    col_contract_type = []
    col_native_symbol = []
    col_dataset_class = []
    col_source_dataset_id = []
    col_aggregate_trade_id = []
    col_first_trade_id = []
    col_last_trade_id = []
    col_source_ordinal = []
    col_event_time = []
    col_exchange_timestamp = []
    col_price = []
    col_quantity = []
    col_quantity_unit = []
    col_quote_quantity = []
    col_notional_unit = []
    col_taker_side = []
    col_signed_quantity = []
    col_is_buyer_maker = []
    col_is_best_match = []
    col_received_at = []
    col_processed_at = []
    col_source_uri = []
    col_source_object_sha256 = []
    col_schema_version = []
    col_dq_flags = []

    ordinal = start_ordinal

    for row in rows:
        if isinstance(row, list):
            agg_trade_id = row[0]
            price_str = row[1]
            qty_str = row[2]
            first_id = row[3]
            last_id = row[4]
            ts_str = row[5]
            is_buyer_maker_str = str(row[6]).lower()
            is_best_match_val = (str(row[7]).lower() == "true") if len(row) > 7 else None
        else:
            agg_trade_id = row["agg_trade_id"] if "agg_trade_id" in row else row["id"]
            price_str = row["price"]
            qty_str = row["qty"]
            first_id = row["first_trade_id"]
            last_id = row["last_trade_id"]
            ts_str = row.get("trans_time") or row.get("time") or "0"
            is_buyer_maker_str = str(row["is_buyer_maker"]).lower()
            is_best_match_val = (str(row["is_best_match"]).lower() == "true") if "is_best_match" in row else None

        is_bm = is_buyer_maker_str in ("true", "1")
        taker_side = "SELL" if is_bm else "BUY"

        price_dec = Decimal(price_str)
        qty_dec = Decimal(qty_str)
        quote_qty_dec = price_dec * qty_dec
        signed_qty = -qty_dec if is_bm else qty_dec

        event_us = int(ts_str) * 1000

        col_instrument_id.append(instrument_id)
        col_exchange.append("binance")
        col_market_type.append(market_type)
        col_contract_type.append(contract_type)
        col_native_symbol.append(symbol)
        col_dataset_class.append("exchange_aggregate_trade")
        col_source_dataset_id.append(source_dataset_id)
        col_aggregate_trade_id.append(str(agg_trade_id))
        col_first_trade_id.append(str(first_id))
        col_last_trade_id.append(str(last_id))
        col_source_ordinal.append(ordinal)
        col_event_time.append(event_us)
        col_exchange_timestamp.append(event_us)
        col_price.append(price_dec)
        col_quantity.append(qty_dec)
        col_quantity_unit.append(symbol.removesuffix("USDT"))
        col_quote_quantity.append(quote_qty_dec)
        col_notional_unit.append("USDT")
        col_taker_side.append(taker_side)
        col_signed_quantity.append(signed_qty)
        col_is_buyer_maker.append(is_bm)
        col_is_best_match.append(is_best_match_val)
        col_received_at.append(now_us)
        col_processed_at.append(now_us)
        col_source_uri.append(source_uri)
        col_source_object_sha256.append(source_sha256)
        col_schema_version.append("1.0.0")
        col_dq_flags.append(None)

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
            pa.array(col_aggregate_trade_id, pa.string()),
            pa.array(col_first_trade_id, pa.string()),
            pa.array(col_last_trade_id, pa.string()),
            pa.array(col_source_ordinal, pa.int64()),
            pa.array(col_event_time, pa.timestamp("us", tz="UTC")),
            pa.array(col_exchange_timestamp, pa.timestamp("us", tz="UTC")),
            pa.array(col_price, DEC),
            pa.array(col_quantity, DEC),
            pa.array(col_quantity_unit, pa.string()),
            pa.array(col_quote_quantity, DEC),
            pa.array(col_notional_unit, pa.string()),
            pa.array(col_taker_side, pa.string()),
            pa.array(col_signed_quantity, DEC),
            pa.array(col_is_buyer_maker, pa.bool_()),
            pa.array(col_is_best_match, pa.bool_()),
            pa.array(col_received_at, pa.timestamp("us", tz="UTC")),
            pa.array(col_processed_at, pa.timestamp("us", tz="UTC")),
            pa.array(col_source_uri, pa.string()),
            pa.array(col_source_object_sha256, pa.string()),
            pa.array(col_schema_version, pa.string()),
            pa.array(col_dq_flags, pa.list_(pa.string())),
        ],
        schema=EXCHANGE_AGGREGATE_TRADE_SCHEMA,
    )
    return rb, ordinal
