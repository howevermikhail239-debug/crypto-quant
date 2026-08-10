"""Unit tests for Bybit individual trade contracts, parsers, and fixtures."""

import json
from datetime import date
from pathlib import Path

import yaml

from crypto_quant.ingestion.bybit.trades import (
    build_bybit_individual_trade_batch,
    bybit_linear_identity,
    bybit_spot_identity,
    get_bybit_archive_url,
    map_bybit_taker_side,
    parse_bybit_timestamp_to_us,
)

FIXTURES_DIR = Path(__file__).parent / "fixtures" / "bybit"


def test_bybit_identities():
    spot_ident = bybit_spot_identity("BTCUSDT")
    assert spot_ident.exchange == "bybit"
    assert spot_ident.market_type == "spot"
    assert spot_ident.native_symbol == "BTCUSDT"
    assert spot_ident.instrument_id == "ins_382b67a5ff90e4cd6ae4"

    linear_ident = bybit_linear_identity("BTCUSDT")
    assert linear_ident.exchange == "bybit"
    assert linear_ident.market_type == "perpetual"
    assert linear_ident.contract_type == "linear_perpetual"
    assert linear_ident.instrument_id == "ins_b833d257c75fe11aa2c3"


def test_bybit_spot_archive_contract():
    contract_path = Path("schemas/contracts/bybit_spot_archive_individual_trade_v1.yaml")
    assert contract_path.exists()
    with open(contract_path, encoding="utf-8") as f:
        data = yaml.safe_load(f)
    assert data["contract_id"] == "bybit.spot.archive.individual-trade.v1"
    assert data["exchange"] == "bybit"
    assert data["market_type"] == "spot"


def test_bybit_linear_archive_contract():
    contract_path = Path("schemas/contracts/bybit_linear_archive_individual_trade_v1.yaml")
    assert contract_path.exists()
    with open(contract_path, encoding="utf-8") as f:
        data = yaml.safe_load(f)
    assert data["contract_id"] == "bybit.linear.archive.individual-trade.v1"
    assert data["exchange"] == "bybit"
    assert data["market_type"] == "perpetual"


def test_bybit_archive_url_generator():
    d = date(2026, 7, 1)
    spot_url = get_bybit_archive_url("spot", "BTCUSDT", d)
    assert spot_url == "https://public.bybit.com/spot/BTCUSDT/BTCUSDT_2026-07-01.csv.gz"

    linear_url = get_bybit_archive_url("perpetual", "BTCUSDT", d)
    assert linear_url == "https://public.bybit.com/trading/BTCUSDT/BTCUSDT2026-07-01.csv.gz"


def test_map_bybit_taker_side():
    assert map_bybit_taker_side("buy") == "BUY"
    assert map_bybit_taker_side("Buy") == "BUY"
    assert map_bybit_taker_side("SELL") == "SELL"
    assert map_bybit_taker_side("sell") == "SELL"
    assert map_bybit_taker_side("invalid") == "UNKNOWN"
    assert map_bybit_taker_side("") == "UNKNOWN"


def test_parse_bybit_timestamp():
    # Spot ms int string
    ts_spot = parse_bybit_timestamp_to_us("1782864000554", is_float_seconds=False)
    assert ts_spot == 1782864000554000

    # Linear float sec string
    ts_linear = parse_bybit_timestamp_to_us("1782864000.0435", is_float_seconds=True)
    assert ts_linear == 1782864000043500


def test_bybit_spot_archive_parser():
    rows = [
        {"id": "1", "timestamp": "1782864000554", "price": "58631.50", "volume": "0.00016", "side": "buy", "rpi": "0"},
        {"id": "2", "timestamp": "1782864000579", "price": "58631.00", "volume": "0.051024", "side": "sell", "rpi": "0"},
        {"id": "3", "timestamp": "1782864000585", "price": "58631.25", "volume": "0.030000", "side": "unknown_side", "rpi": "1"},
    ]
    rb, count = build_bybit_individual_trade_batch(
        rows,
        market_type="spot",
        symbol="BTCUSDT",
        date_val=date(2026, 7, 1),
        source_uri="https://public.bybit.com/spot/BTCUSDT/BTCUSDT_2026-07-01.csv.gz",
        source_sha256="dummy_hash",
    )
    assert rb.num_rows == 3
    assert rb.column("taker_side")[0].as_py() == "BUY"
    assert rb.column("taker_side")[1].as_py() == "SELL"
    assert rb.column("taker_side")[2].as_py() == "UNKNOWN"

    # UNKNOWN side should set UNKNOWN_TAKER_SIDE in dq_flags
    dq_flags_row2 = rb.column("dq_flags")[2].as_py()
    assert dq_flags_row2 == ["UNKNOWN_TAKER_SIDE"]


def test_bybit_linear_archive_parser():
    rows = [
        {
            "timestamp": "1782864000.0435",
            "symbol": "BTCUSDT",
            "side": "Sell",
            "size": "0.001",
            "price": "58603.40",
            "trdMatchID": "e5df326c-57cc-5599-b0c0-f67ae2129e0f",
            "RPI": "0",
        },
        {
            "timestamp": "1782864000.0870",
            "symbol": "BTCUSDT",
            "side": "Buy",
            "size": "0.002",
            "price": "58603.50",
            "trdMatchID": "f73615e5-71d1-5818-a8c0-89b25af56f52",
            "RPI": "0",
        },
    ]
    rb, count = build_bybit_individual_trade_batch(
        rows,
        market_type="perpetual",
        symbol="BTCUSDT",
        date_val=date(2026, 7, 1),
        source_uri="https://public.bybit.com/trading/BTCUSDT/BTCUSDT2026-07-01.csv.gz",
        source_sha256="dummy_hash",
    )
    assert rb.num_rows == 2
    assert rb.column("native_trade_id")[0].as_py() == "e5df326c-57cc-5599-b0c0-f67ae2129e0f"
    assert rb.column("taker_side")[0].as_py() == "SELL"
    assert rb.column("taker_side")[1].as_py() == "BUY"


def test_bybit_rest_and_ws_fixtures():
    rest_file = FIXTURES_DIR / "rest_trades_sample.json"
    assert rest_file.exists()
    with open(rest_file, encoding="utf-8") as f:
        rest_data = json.load(f)
    assert rest_data["retCode"] == 0
    trades = rest_data["result"]["list"]
    assert len(trades) == 2
    assert trades[0]["side"] == "Buy"
    assert trades[1]["side"] == "Sell"

    ws_file = FIXTURES_DIR / "ws_trades_sample.json"
    assert ws_file.exists()
    with open(ws_file, encoding="utf-8") as f:
        ws_data = json.load(f)
    ws_trades = ws_data["data"]
    assert len(ws_trades) == 2
    # Verify non-unique sequence numbers in WS message payload
    assert ws_trades[0]["seq"] == ws_trades[1]["seq"]
    assert ws_trades[0]["S"] == "Buy"
    assert ws_trades[1]["S"] == "Sell"
