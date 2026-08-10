"""Unit and regression tests for exchange_aggregate_trade datasets and strict isolation."""

from datetime import date
from pathlib import Path

import pytest
import yaml

from crypto_quant.ingestion.binance.aggregate_trades import (
    build_binance_aggregate_trade_batch,
    validate_dataset_class_isolation,
)

FIXTURES_DIR = Path(__file__).parent / "fixtures" / "binance"


def test_binance_spot_aggregate_contract():
    contract_path = Path("schemas/contracts/binance_spot_archive_aggregate_trade_v1.yaml")
    assert contract_path.exists()
    with open(contract_path, encoding="utf-8") as f:
        data = yaml.safe_load(f)
    assert data["contract_id"] == "binance.spot.archive.aggregate-trade.v1"
    assert data["exchange"] == "binance"
    assert data["market_type"] == "spot"


def test_binance_usdm_aggregate_contract():
    contract_path = Path("schemas/contracts/binance_usdm_archive_aggregate_trade_v1.yaml")
    assert contract_path.exists()
    with open(contract_path, encoding="utf-8") as f:
        data = yaml.safe_load(f)
    assert data["contract_id"] == "binance.usdm.archive.aggregate-trade.v1"
    assert data["exchange"] == "binance"
    assert data["market_type"] == "perpetual"


def test_binance_spot_aggregate_trade_parser():
    rows = [
        ["1001", "58630.00", "0.125000", "2001", "2003", "1782864000554", "true", "true"],
        ["1002", "58630.50", "0.050000", "2004", "2004", "1782864000580", "false", "true"],
    ]
    rb, count = build_binance_aggregate_trade_batch(
        rows,
        market_type="spot",
        symbol="BTCUSDT",
        date_val=date(2026, 7, 1),
        source_uri="dummy_uri",
        source_sha256="dummy_sha256",
    )
    assert rb.num_rows == 2
    assert rb.column("dataset_class")[0].as_py() == "exchange_aggregate_trade"
    assert rb.column("taker_side")[0].as_py() == "SELL"
    assert rb.column("taker_side")[1].as_py() == "BUY"


def test_binance_usdm_aggregate_trade_parser():
    rows = [
        {
            "agg_trade_id": "5001",
            "price": "58600.00",
            "qty": "0.010",
            "first_trade_id": "7001",
            "last_trade_id": "7005",
            "trans_time": "1782864000100",
            "is_buyer_maker": "true",
        },
        {
            "agg_trade_id": "5002",
            "price": "58600.10",
            "qty": "0.025",
            "first_trade_id": "7006",
            "last_trade_id": "7006",
            "trans_time": "1782864000150",
            "is_buyer_maker": "false",
        },
    ]
    rb, count = build_binance_aggregate_trade_batch(
        rows,
        market_type="perpetual",
        symbol="BTCUSDT",
        date_val=date(2026, 7, 1),
        source_uri="dummy_uri",
        source_sha256="dummy_sha256",
    )
    assert rb.num_rows == 2
    assert rb.column("dataset_class")[0].as_py() == "exchange_aggregate_trade"
    assert rb.column("taker_side")[0].as_py() == "SELL"
    assert rb.column("taker_side")[1].as_py() == "BUY"


def test_strict_dataset_class_isolation_regression():
    """Regression test proving pipeline fails closed when given incorrect dataset_class."""
    rows = [
        ["1001", "58630.00", "0.125000", "2001", "2003", "1782864000554", "true", "true"],
    ]
    agg_batch, _ = build_binance_aggregate_trade_batch(
        rows,
        market_type="spot",
        symbol="BTCUSDT",
        date_val=date(2026, 7, 1),
        source_uri="dummy_uri",
        source_sha256="dummy_sha256",
    )
    # Validation passes for expected class
    validate_dataset_class_isolation(agg_batch, "exchange_aggregate_trade")

    # Validation fails closed when pipeline expected individual_trade
    with pytest.raises(TypeError, match="Dataset class mismatch"):
        validate_dataset_class_isolation(agg_batch, "individual_trade")
