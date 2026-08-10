import hashlib
import json
from datetime import UTC, date, datetime
from decimal import Decimal
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from crypto_quant.ingestion.binance.spot_trades import (
    INDIVIDUAL_TRADE_SCHEMA,
    apply_retention,
    plan_retention,
)
from crypto_quant.ingestion.trade_buckets import SOURCE_DATASET, USDM_SOURCE, build_buckets

INS = "ins_test"


def _source(root: Path, *, bad_side: bool = False) -> Path:
    path = (
        root
        / "normalized"
        / "individual_trade"
        / "v1"
        / "exchange=binance"
        / "market_type=spot"
        / f"instrument_id={INS}"
        / "date=2026-01-01"
        / "part-x.parquet"
    )
    path.parent.mkdir(parents=True)
    values = []
    for ordinal, (second, side, price, qty, quote) in enumerate(
        [(0, "BUY", "10", "2", "20"), (1, "SELL", "11", "1", "11")]
    ):
        event = datetime(2026, 1, 1, 0, 0, second, tzinfo=UTC)
        values.append(
            {
                "instrument_id": INS,
                "exchange": "binance",
                "market_type": "spot",
                "contract_type": "spot",
                "native_symbol": "BTCUSDT",
                "dataset_class": "individual_trade",
                "source_dataset_id": SOURCE_DATASET,
                "native_trade_id": str(ordinal + 1),
                "source_ordinal": ordinal,
                "event_time": event,
                "exchange_timestamp": event,
                "source_timestamp": int(event.timestamp() * 1_000_000),
                "source_timestamp_unit": "us",
                "price": Decimal(price),
                "quantity": Decimal(qty),
                "quantity_unit": "BTC",
                "quote_quantity": Decimal(quote),
                "notional_unit": "USDT",
                "taker_side": "UNKNOWN" if bad_side and ordinal else side,
                "signed_quantity": Decimal(qty) if side == "BUY" else -Decimal(qty),
                "is_buyer_maker": side == "SELL",
                "is_best_match": True,
                "is_block_trade": None,
                "is_rpi_trade": None,
                "received_at": event,
                "processed_at": event,
                "knowledge_time": None,
                "knowledge_time_basis": "unknown_historical_retrieval_only",
                "source_uri": "fixture://",
                "raw_object_ref": "raw.zip",
                "source_object_sha256": "a" * 64,
                "schema_version": "1.0.0",
                "collector_version": "x",
                "normalization_version": "1.0.0",
                "data_contract_version": "1.0.0",
                "classification_version": "v1",
                "dq_flags": [],
            }
        )
    pq.write_table(pa.Table.from_pylist(values, schema=INDIVIDUAL_TRADE_SCHEMA), path)
    digest = hashlib.sha256(path.read_bytes()).hexdigest()
    manifest = root / "control" / "manifests" / "binance_spot_individual_trade.jsonl"
    manifest.parent.mkdir(parents=True)
    manifest.write_text(
        json.dumps(
            {
                "object_id": str(path.relative_to(root)),
                "parquet_sha256": digest,
                "row_count": 2,
                "coverage_start": values[0]["event_time"].isoformat(),
                "coverage_end": values[-1]["event_time"].isoformat(),
                "source_dataset_id": SOURCE_DATASET,
                "instrument_id": INS,
                "exchange": "binance",
                "market_type": "spot",
                "contract_type": "spot",
            }
        )
        + "\n"
    )
    return path


def test_complete_fields_conservation_and_idempotency(tmp_path: Path):
    source = _source(tmp_path)
    first = build_buckets(source, tmp_path, 5)
    second = build_buckets(source, tmp_path, 5)
    table = (
        pq.ParquetFile(first)
        .read(
            columns=[
                "trade_count",
                "buy_count",
                "sell_count",
                "open",
                "high",
                "low",
                "close",
                "base_volume",
                "quote_volume",
                "base_delta",
                "quote_delta",
                "avg_base_size",
                "median_base_size",
                "max_base_size",
            ]
        )
        .to_pylist()
    )
    assert first == second and len(table) == 1
    row = table[0]
    assert row["trade_count"] == row["buy_count"] + row["sell_count"] == 2
    assert row["base_volume"] == Decimal("3") and row["quote_volume"] == Decimal("31")
    assert row["base_delta"] == Decimal("1") and row["quote_delta"] == Decimal("9")
    assert (row["open"], row["high"], row["low"], row["close"]) == (
        Decimal("10"), Decimal("11"), Decimal("10"), Decimal("11")
    )
    assert row["avg_base_size"] == row["median_base_size"] == Decimal("1.5")
    assert row["max_base_size"] == Decimal("2")
    assert len(
        (tmp_path / "control" / "manifests" / "derived_trade_bucket.jsonl")
        .read_text()
        .splitlines()
    ) == 1


def test_usdm_descriptor_routes_without_spot_alias(tmp_path: Path):
    source = _source(tmp_path)
    table = pq.ParquetFile(source).read()
    replacements = {
        "market_type": "perpetual",
        "contract_type": "linear_perpetual",
        "source_dataset_id": USDM_SOURCE.dataset_id,
    }
    for name, value in replacements.items():
        index = table.schema.get_field_index(name)
        table = table.set_column(index, name, pa.array([value] * table.num_rows))
    usdm_source = Path(
        str(source).replace("market_type=spot", "market_type=perpetual")
    )
    usdm_source.parent.mkdir(parents=True)
    pq.write_table(table, usdm_source)
    digest = hashlib.sha256(usdm_source.read_bytes()).hexdigest()
    source_event = json.loads(
        (tmp_path / "control" / "manifests" / "binance_spot_individual_trade.jsonl")
        .read_text()
        .strip()
    )
    source_event.update(
        object_id=str(usdm_source.relative_to(tmp_path)),
        parquet_sha256=digest,
        source_dataset_id=USDM_SOURCE.dataset_id,
        market_type="perpetual",
        contract_type="linear_perpetual",
    )
    (tmp_path / "control" / "manifests" / USDM_SOURCE.manifest_name).write_text(
        json.dumps(source_event) + "\n"
    )

    output = build_buckets(usdm_source, tmp_path, 5, descriptor=USDM_SOURCE)
    assert "market_type=perpetual" in str(output)
    assert (
        pq.ParquetFile(output).read(columns=["source_dataset_id"])["source_dataset_id"][0].as_py()
        == USDM_SOURCE.dataset_id
    )
    with pytest.raises(ValueError, match="manifest event|descriptor mismatch"):
        build_buckets(usdm_source, tmp_path, 5)


@pytest.mark.parametrize("seconds", [1, 5, 60])
def test_approved_grains_and_identity_date_paths(tmp_path: Path, seconds: int):
    output = build_buckets(_source(tmp_path), tmp_path, seconds)
    assert f"instrument_id={INS}" in str(output) and "date=2026-01-01" in str(output)


def test_rejects_unknown_side_before_write(tmp_path: Path):
    with pytest.raises(ValueError, match="unknown taker side"):
        build_buckets(_source(tmp_path, bad_side=True), tmp_path, 1)
    assert not list((tmp_path / "derived").rglob("*.parquet"))


def test_rejects_manifest_hash_and_descriptor_mismatch(tmp_path: Path):
    source = _source(tmp_path)
    source.write_bytes(source.read_bytes() + b"x")
    with pytest.raises(ValueError, match="hash/row_count"):
        build_buckets(source, tmp_path, 1)


def test_rejects_wrong_requested_date(tmp_path: Path):
    with pytest.raises(ValueError, match="path/date"):
        build_buckets(_source(tmp_path), tmp_path, 1, trading_date=date(2026, 1, 2))


def test_retention_requires_complete_60s_and_actual_delete_only_fixture(tmp_path: Path):
    source = _source(tmp_path)
    source_manifest = tmp_path / "control" / "manifests" / "binance_spot_individual_trade.jsonl"
    event = json.loads(source_manifest.read_text())
    raw = tmp_path / "raw" / "fixture.zip"
    raw.parent.mkdir(parents=True)
    raw.write_bytes(b"raw")
    event["raw_object_ref"] = str(raw.relative_to(tmp_path))
    source_manifest.write_text(json.dumps(event) + "\n")
    with pytest.raises(ValueError, match="60s"):
        plan_retention(tmp_path, instrument_id=INS, trading_date=date(2026, 1, 1))
    build_buckets(source, tmp_path, 60)
    events = plan_retention(tmp_path, instrument_id=INS, trading_date=date(2026, 1, 1))
    ledger = apply_retention(tmp_path, events, dry_run=True)
    assert source.exists() and raw.exists()
    apply_retention(tmp_path, events, dry_run=False)
    assert not source.exists() and not raw.exists()
    assert len(ledger.read_text().splitlines()) == len(events) * 2
