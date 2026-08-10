"""Unit tests for Item 7A raw WebSocket envelope capture and normalization."""

import json
import tempfile
from pathlib import Path

import pytest

from crypto_quant.ingestion.realtime_envelope import (
    RawWsSegmentWriter,
    WsSessionInfo,
    create_raw_ws_envelope,
    normalize_ws_envelope_to_aggregate_trades,
    normalize_ws_envelope_to_individual_trades,
    recover_stale_ws_partials,
)
from crypto_quant.time import utc_now

BINANCE_FIXTURES = Path(__file__).parent / "fixtures" / "binance"
BYBIT_FIXTURES = Path(__file__).parent / "fixtures" / "bybit"


def create_dummy_session(exchange: str, market_type: str, topic: str) -> WsSessionInfo:
    return WsSessionInfo(
        session_id="sess_12345",
        connection_id="conn_67890",
        exchange=exchange,
        market_type=market_type,
        stream_topic=topic,
        connected_at=utc_now(),
    )


def test_create_raw_ws_envelope():
    session = create_dummy_session("binance", "spot", "btcusdt@trade")
    payload = {"e": "trade", "E": 1782864000555, "s": "BTCUSDT", "t": 123, "p": "50000", "q": "1", "m": True}
    env = create_raw_ws_envelope(
        exchange="binance",
        market_type="spot",
        instrument_id="ins_382b67a5ff90e4cd6ae4",
        stream_topic="btcusdt@trade",
        session=session,
        payload=payload,
        source_contract_version="binance.spot.ws.trade.v1",
    )
    assert env.exchange == "binance"
    assert env.market_type == "spot"
    assert env.envelope_id.startswith("env_")
    assert len(env.payload_hash) == 64


def test_segment_writer_and_sealing():
    with tempfile.TemporaryDirectory() as tmp_dir:
        root = Path(tmp_dir)
        writer = RawWsSegmentWriter(root, "binance", "spot", "BTCUSDT")
        session = create_dummy_session("binance", "spot", "btcusdt@trade")
        payload = {"e": "trade", "E": 1782864000555, "s": "BTCUSDT", "t": 123, "p": "50000", "q": "1", "m": True}
        env = create_raw_ws_envelope(
            exchange="binance",
            market_type="spot",
            instrument_id="ins_382b67a5ff90e4cd6ae4",
            stream_topic="btcusdt@trade",
            session=session,
            payload=payload,
            source_contract_version="binance.spot.ws.trade.v1",
        )
        writer.write_envelope(env)
        assert writer.envelope_count == 1
        assert writer.bytes_written > 0

        sealed_file = writer.seal()
        assert sealed_file is not None
        assert sealed_file.exists()
        assert not str(sealed_file).endswith(".partial")
        lines = sealed_file.read_text(encoding="utf-8").splitlines()
        assert len(lines) == 1
        data = json.loads(lines[0])
        assert data["envelope_id"] == env.envelope_id


def test_recover_stale_ws_partials():
    with tempfile.TemporaryDirectory() as tmp_dir:
        root = Path(tmp_dir)
        partial_dir = root / "raw" / "ws" / "exchange=binance" / "market_type=spot" / "symbol=BTCUSDT" / "date=2026-08-10" / "hour=14"
        partial_dir.mkdir(parents=True, exist_ok=True)
        partial_file = partial_dir / "segment_stale123.jsonl.partial"
        partial_file.write_text('{"stale": true}\n', encoding="utf-8")

        recovered = recover_stale_ws_partials(root)
        assert len(recovered) == 1
        assert recovered[0] == partial_dir / "segment_stale123.jsonl"
        assert not partial_file.exists()
        assert recovered[0].exists()


def test_normalize_binance_spot_ws_trade():
    fixture = BINANCE_FIXTURES / "ws_trade_spot_sample.json"
    with open(fixture, encoding="utf-8") as f:
        payload = json.load(f)

    session = create_dummy_session("binance", "spot", "btcusdt@trade")
    env = create_raw_ws_envelope(
        exchange="binance",
        market_type="spot",
        instrument_id="ins_382b67a5ff90e4cd6ae4",
        stream_topic="btcusdt@trade",
        session=session,
        payload=payload,
        source_contract_version="binance.spot.ws.trade.v1",
    )

    rb = normalize_ws_envelope_to_individual_trades(env)
    assert rb.num_rows == 1
    assert rb.column("dataset_class")[0].as_py() == "individual_trade"
    assert rb.column("native_trade_id")[0].as_py() == "123456789"
    assert rb.column("taker_side")[0].as_py() == "SELL"
    assert rb.column("raw_object_ref")[0].as_py() == f"envelope_id={env.envelope_id}"


def test_dataset_class_isolation_aggtrade_reject():
    """Regression test: aggTrade payload MUST NOT enter individual_trade pipeline."""
    fixture = BINANCE_FIXTURES / "ws_agg_trade_spot_sample.json"
    with open(fixture, encoding="utf-8") as f:
        payload = json.load(f)

    session = create_dummy_session("binance", "spot", "btcusdt@aggTrade")
    env = create_raw_ws_envelope(
        exchange="binance",
        market_type="spot",
        instrument_id="ins_382b67a5ff90e4cd6ae4",
        stream_topic="btcusdt@aggTrade",
        session=session,
        payload=payload,
        source_contract_version="binance.spot.ws.aggTrade.v1",
    )

    with pytest.raises(TypeError, match="Dataset Class Isolation Defect"):
        normalize_ws_envelope_to_individual_trades(env)


def test_normalize_binance_spot_ws_aggtrade():
    fixture = BINANCE_FIXTURES / "ws_agg_trade_spot_sample.json"
    with open(fixture, encoding="utf-8") as f:
        payload = json.load(f)

    session = create_dummy_session("binance", "spot", "btcusdt@aggTrade")
    env = create_raw_ws_envelope(
        exchange="binance",
        market_type="spot",
        instrument_id="ins_382b67a5ff90e4cd6ae4",
        stream_topic="btcusdt@aggTrade",
        session=session,
        payload=payload,
        source_contract_version="binance.spot.ws.aggTrade.v1",
    )

    rb = normalize_ws_envelope_to_aggregate_trades(env)
    assert rb.num_rows == 1
    assert rb.column("dataset_class")[0].as_py() == "exchange_aggregate_trade"
    assert rb.column("aggregate_trade_id")[0].as_py() == "59382001"
    assert rb.column("taker_side")[0].as_py() == "SELL"


def test_normalize_bybit_multi_trade_ws_message():
    """Verify 1 Bybit WS envelope containing N trades maps to N canonical trades with lineage."""
    fixture = BYBIT_FIXTURES / "ws_multi_trade_sample.json"
    with open(fixture, encoding="utf-8") as f:
        payload = json.load(f)

    session = create_dummy_session("bybit", "spot", "publicTrade.BTCUSDT")
    env = create_raw_ws_envelope(
        exchange="bybit",
        market_type="spot",
        instrument_id="ins_382b67a5ff90e4cd6ae4",
        stream_topic="publicTrade.BTCUSDT",
        session=session,
        payload=payload,
        source_contract_version="bybit.spot.ws.individual-trade.v1",
    )

    rb = normalize_ws_envelope_to_individual_trades(env)
    # 1 envelope -> 2 trades
    assert rb.num_rows == 2
    assert rb.column("native_trade_id")[0].as_py() == "2100000000000000001"
    assert rb.column("native_trade_id")[1].as_py() == "2100000000000000002"
    assert rb.column("taker_side")[0].as_py() == "BUY"
    assert rb.column("taker_side")[1].as_py() == "SELL"

    # Both trades reference the exact same envelope_id in lineage
    assert rb.column("raw_object_ref")[0].as_py() == f"envelope_id={env.envelope_id}"
    assert rb.column("raw_object_ref")[1].as_py() == f"envelope_id={env.envelope_id}"

    # Verify non-unique sequence numbers: seq is source_ordinal, NOT native_trade_id
    assert rb.column("source_ordinal")[0].as_py() == 1000200
    assert rb.column("source_ordinal")[1].as_py() == 1000200


def test_normalize_bybit_malformed_ws_payload():
    fixture = BYBIT_FIXTURES / "ws_malformed_sample.json"
    with open(fixture, encoding="utf-8") as f:
        payload = json.load(f)

    session = create_dummy_session("bybit", "spot", "publicTrade.BTCUSDT")
    env = create_raw_ws_envelope(
        exchange="bybit",
        market_type="spot",
        instrument_id="ins_382b67a5ff90e4cd6ae4",
        stream_topic="publicTrade.BTCUSDT",
        session=session,
        payload=payload,
        source_contract_version="bybit.spot.ws.individual-trade.v1",
    )

    rb = normalize_ws_envelope_to_individual_trades(env)
    assert rb.num_rows == 1
    assert rb.column("taker_side")[0].as_py() == "UNKNOWN"
    dq_flags = rb.column("dq_flags")[0].as_py()
    assert "MALFORMED_PAYLOAD" in dq_flags
    assert "UNKNOWN_TAKER_SIDE" in dq_flags
