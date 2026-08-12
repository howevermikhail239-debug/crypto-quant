"""Binance USD-M BTCUSDT/ETHUSDT liquidation source-contract tests."""

from __future__ import annotations

import asyncio
import hashlib
import json
from datetime import UTC, datetime
from pathlib import Path

import pyarrow.parquet as pq
import pytest
import yaml

from crypto_quant.ingestion.binance.funding import funding_identity as binance_identity
from crypto_quant.ingestion.binance.liquidations import (
    BINANCE_USDM_MARKET_WS_URL,
    CONTRACT_ID,
    DELIVERY_SEMANTICS,
    LOCAL_CAPTURE_COMPLETENESS,
    SELECTION_RULE,
    SOURCE_COMPLETENESS,
    binance_usdm_liquidation_data_contract,
    collect_binance_liquidations_live,
    parse_binance_liquidation_message,
    persist_binance_liquidation_batch,
    validate_binance_liquidation_records,
)
from crypto_quant.ingestion.binance.spot_trades import binance_spot_identity
from crypto_quant.ingestion.bybit.funding import funding_identity as bybit_identity

FIXTURE_DIR = Path("tests/fixtures/binance")
SELL_FIXTURE = FIXTURE_DIR / "ws_force_order_usdm_btc_sell.json"
BUY_FIXTURE = FIXTURE_DIR / "ws_force_order_usdm_btc_buy.json"
ETH_SELL_FIXTURE = FIXTURE_DIR / "ws_force_order_usdm_eth_sell.json"
ETH_BUY_FIXTURE = FIXTURE_DIR / "ws_force_order_usdm_eth_buy.json"


def _wire(path: Path) -> tuple[dict, str]:
    raw = path.read_text(encoding="utf-8").strip()
    return json.loads(raw), raw


def _received() -> datetime:
    return datetime(2026, 8, 11, 13, 0, tzinfo=UTC)


def test_frozen_source_contract_records_document_conflict_and_private_history():
    path = Path("schemas/contracts/binance_usdm_liquidation_ws_v1.yaml")
    data = yaml.safe_load(path.read_text(encoding="utf-8"))
    contract = binance_usdm_liquidation_data_contract()

    assert data["contract_id"] == contract.contract_id == CONTRACT_ID
    assert data["stream_rules"]["endpoint"] == BINANCE_USDM_MARKET_WS_URL
    assert data["stream_rules"]["topic"] == "{symbol_lower}@forceOrder"
    assert data["stream_rules"]["source_window_ms"] == 1000
    assert data["stream_rules"]["max_emitted_per_symbol_per_window"] == 1
    assert data["stream_rules"]["selection_rule"] == SELECTION_RULE
    assert data["stream_rules"]["source_claimed_completeness"] == SOURCE_COMPLETENESS
    assert data["stream_rules"]["delivery_semantics"] == DELIVERY_SEMANTICS
    assert data["historical_sources"]["public_market_bootstrap"] == "NO_VERIFIED_PUBLIC_SOURCE"
    assert data["historical_sources"]["fapi_v1_forceOrders"]["security_type"] == "USER_DATA"
    assert (
        data["historical_sources"]["fapi_v1_forceOrders"]["classification"]
        == "NOT_A_PUBLIC_MARKET_LIQUIDATION_BACKFILL_SOURCE"
    )

    yaml_fields = {field["source_field"] for field in data["fields"]}
    contract_fields = {field.source_field for field in contract.fields}
    assert yaml_fields == contract_fields == {
        "e",
        "E",
        "o",
        "o.s",
        "o.S",
        "o.o",
        "o.f",
        "o.q",
        "o.p",
        "o.ap",
        "o.X",
        "o.l",
        "o.z",
        "o.T",
    }
    yaml_by_source = {field["source_field"]: field for field in data["fields"]}
    contract_by_source = {field.source_field: field for field in contract.fields}
    for source_field in ("o.q", "o.l", "o.z"):
        assert yaml_by_source[source_field]["normalized_unit"] == (
            "canonical_instrument_base_asset"
        )
        assert contract_by_source[source_field].normalized_unit == (
            "canonical_instrument_base_asset"
        )


@pytest.mark.parametrize(
    ("fixture", "expected_side", "q", "last", "accumulated", "price", "average", "status"),
    [
        (SELL_FIXTURE, "SELL", "0.010000", "0.003000", "0.007000", "60000.10", "59950.25", "PARTIALLY_FILLED"),
        (BUY_FIXTURE, "BUY", "0.020000", "0.013000", "0.020000", "60100.20", "60080.15", "FILLED"),
    ],
)
def test_source_fields_are_preserved_without_liquidation_volume_or_position_inference(
    fixture: Path,
    expected_side: str,
    q: str,
    last: str,
    accumulated: str,
    price: str,
    average: str,
    status: str,
):
    payload, raw = _wire(fixture)
    identity = binance_identity("BTCUSDT")
    record = parse_binance_liquidation_message(
        payload,
        identity,
        _received(),
        raw_msg_str=raw,
    )

    assert record.instrument_id == identity.instrument_id
    assert record.instrument_id != bybit_identity("BTCUSDT").instrument_id
    assert record.source_side == expected_side
    assert record.source_side_semantic == "FORCED_LIQUIDATION_ORDER_SIDE"
    assert record.position_side_liquidated == "UNKNOWN"
    assert record.source_quantity == q
    assert record.quantity_semantic == "ORIGINAL_ORDER_QUANTITY"
    assert record.last_filled_quantity == last
    assert record.accumulated_filled_quantity == accumulated
    assert record.source_quantity_unit == "BTC"
    assert record.quantity_base == q
    assert record.notional_quote is None
    assert record.source_price == price
    assert record.price_semantic == "ORDER_PRICE"
    assert record.average_fill_price == average
    assert record.order_status == status
    assert record.order_type == payload["o"]["o"]
    assert record.time_in_force == payload["o"]["f"]
    assert record.source_event_time_ms == payload["E"]
    assert record.source_order_trade_time_ms == payload["o"]["T"]
    assert record.exchange_timestamp.timestamp() * 1000 == payload["E"]
    assert record.event_time.timestamp() * 1000 == payload["o"]["T"]
    assert record.knowledge_time == record.received_at == _received()
    assert record.message_id == hashlib.sha256(raw.encode()).hexdigest()
    assert record.native_event_id is None
    assert record.native_sequence_id is None
    assert record.source_claimed_completeness == SOURCE_COMPLETENESS
    assert record.selection_rule == SELECTION_RULE
    assert record.local_capture_completeness == LOCAL_CAPTURE_COMPLETENESS
    assert "SOURCE_SELECTION_INCOMPLETENESS" in record.dq_flags
    assert validate_binance_liquidation_records([record]) == []


def test_unknown_nonempty_order_attributes_are_preserved_and_clock_skew_is_flagged():
    payload, raw = _wire(SELL_FIXTURE)
    payload["o"]["o"] = "FUTURE_ORDER_TYPE"
    payload["o"]["f"] = "FUTURE_TIF"
    payload["o"]["X"] = "FUTURE_STATUS"
    payload["E"] = 1786434743207 + 24 * 60 * 60 * 1000
    raw = json.dumps(payload, separators=(",", ":"))
    record = parse_binance_liquidation_message(
        payload,
        binance_identity("BTCUSDT"),
        _received(),
        raw_msg_str=raw,
    )
    assert (record.order_type, record.time_in_force, record.order_status) == (
        "FUTURE_ORDER_TYPE",
        "FUTURE_TIF",
        "FUTURE_STATUS",
    )
    assert "SOURCE_CLOCK_SKEW_FUTURE_TIMESTAMP" in record.dq_flags


@pytest.mark.parametrize(
    ("fixture", "expected_side", "q", "last", "accumulated", "price", "average"),
    [
        (ETH_SELL_FIXTURE, "SELL", "1.250000", "0.300000", "0.700000", "4200.10", "4198.25"),
        (ETH_BUY_FIXTURE, "BUY", "2.500000", "1.300000", "2.000000", "4210.20", "4208.15"),
    ],
)
def test_eth_source_semantics_and_canonical_identity_are_independently_preserved(
    fixture: Path,
    expected_side: str,
    q: str,
    last: str,
    accumulated: str,
    price: str,
    average: str,
):
    payload, raw = _wire(fixture)
    identity = binance_identity("ETHUSDT")
    record = parse_binance_liquidation_message(payload, identity, _received(), raw_msg_str=raw)

    assert identity.instrument_id == "ins_13dce2c0972bec4044d9"
    assert len(
        {
            identity.instrument_id,
            binance_identity("BTCUSDT").instrument_id,
            bybit_identity("BTCUSDT").instrument_id,
            bybit_identity("ETHUSDT").instrument_id,
            binance_spot_identity("ETHUSDT").instrument_id,
        }
    ) == 5
    assert (identity.base_asset, identity.quote_asset, identity.settle_asset) == (
        "ETH",
        "USDT",
        "USDT",
    )
    assert identity.quantity_unit == record.source_quantity_unit == "ETH"
    assert record.quantity_base == record.source_quantity == q
    assert record.last_filled_quantity == last
    assert record.accumulated_filled_quantity == accumulated
    assert record.source_price == price
    assert record.average_fill_price == average
    assert record.source_side == expected_side
    assert record.position_side_liquidated == "UNKNOWN"
    assert record.source_claimed_completeness == SOURCE_COMPLETENESS
    assert record.selection_rule == SELECTION_RULE
    assert record.source_window_ms == 1000
    assert validate_binance_liquidation_records([record]) == []


def test_eth_malformed_and_wrong_identity_fail_closed():
    payload, _ = _wire(ETH_SELL_FIXTURE)
    payload["o"].pop("z")
    raw = json.dumps(payload, separators=(",", ":"))
    with pytest.raises(ValueError, match="o.z"):
        parse_binance_liquidation_message(
            payload,
            binance_identity("ETHUSDT"),
            _received(),
            raw_msg_str=raw,
        )

    good, good_raw = _wire(ETH_BUY_FIXTURE)
    with pytest.raises(ValueError, match="canonical USD-M instrument"):
        parse_binance_liquidation_message(
            good,
            binance_spot_identity("ETHUSDT"),
            _received(),
            raw_msg_str=good_raw,
        )


@pytest.mark.parametrize(
    "mutator,match",
    [
        (lambda payload: payload.update(e="trade"), "event type"),
        (lambda payload: payload["o"].update(s="ETHUSDT"), "Symbol mismatch"),
        (lambda payload: payload["o"].update(S="Long"), "Unsupported forced-order side"),
        (lambda payload: payload["o"].pop("q"), "o.q"),
        (lambda payload: payload["o"].update(q="NaN"), "strictly positive"),
    ],
)
def test_malformed_or_wrong_routing_fails_closed(mutator, match):
    payload, _ = _wire(SELL_FIXTURE)
    mutator(payload)
    raw = json.dumps(payload, separators=(",", ":"))
    with pytest.raises(ValueError, match=match):
        parse_binance_liquidation_message(
            payload,
            binance_identity("BTCUSDT"),
            _received(),
            raw_msg_str=raw,
        )


def test_wrong_symbol_batch_fails_before_any_authoritative_write(tmp_path: Path):
    good, good_raw = _wire(SELL_FIXTURE)
    wrong, _ = _wire(BUY_FIXTURE)
    wrong["o"]["s"] = "ETHUSDT"
    wrong_raw = json.dumps(wrong, separators=(",", ":"))
    with pytest.raises(ValueError, match="Symbol mismatch"):
        persist_binance_liquidation_batch(
            [(good, good_raw, _received()), (wrong, wrong_raw, _received())],
            tmp_path,
        )
    assert not (tmp_path / "raw").exists()
    assert not (tmp_path / "normalized").exists()
    assert not (tmp_path / "control").exists()


def test_requested_eth_with_btc_payload_and_inverse_fail_before_write(tmp_path: Path):
    btc, btc_raw = _wire(SELL_FIXTURE)
    with pytest.raises(ValueError, match="Symbol mismatch"):
        persist_binance_liquidation_batch(
            [(btc, btc_raw, _received())], tmp_path, symbol="ETHUSDT"
        )
    eth, eth_raw = _wire(ETH_SELL_FIXTURE)
    with pytest.raises(ValueError, match="Symbol mismatch"):
        persist_binance_liquidation_batch(
            [(eth, eth_raw, _received())], tmp_path, symbol="BTCUSDT"
        )
    assert not any(tmp_path.iterdir())


def test_snapshot_observations_append_exact_replay_deduplicates_and_lineage_is_consistent(
    tmp_path: Path,
):
    sell, sell_raw = _wire(SELL_FIXTURE)
    buy, buy_raw = _wire(BUY_FIXTURE)

    first = persist_binance_liquidation_batch([(sell, sell_raw, _received())], tmp_path)
    first_path = Path(first["parquet_files"][0])
    first_bytes = first_path.read_bytes()
    replay = persist_binance_liquidation_batch([(sell, sell_raw, _received())], tmp_path)
    assert replay["total_accumulated_rows"] == 1
    assert first_path.read_bytes() == first_bytes

    second = persist_binance_liquidation_batch([(buy, buy_raw, _received())], tmp_path)
    assert second["total_accumulated_rows"] == 2
    assert first_path.read_bytes() == first_bytes
    generations = list(
        (
            tmp_path
            / "normalized"
            / "liquidations"
            / "v1"
            / "exchange=binance"
            / "market_type=perpetual"
            / "symbol=BTCUSDT"
        ).rglob("*.parquet")
    )
    assert sorted(pq.ParquetFile(path).metadata.num_rows for path in generations) == [1, 2]
    cumulative = next(path for path in generations if pq.ParquetFile(path).metadata.num_rows == 2)
    table = pq.ParquetFile(cumulative).read()
    assert table["message_id"].to_pylist() == [
        hashlib.sha256(sell_raw.encode()).hexdigest(),
        hashlib.sha256(buy_raw.encode()).hexdigest(),
    ]
    assert table["position_side_liquidated"].to_pylist() == ["UNKNOWN", "UNKNOWN"]
    assert table["selection_rule"].to_pylist() == [SELECTION_RULE, SELECTION_RULE]

    raw_files = list((tmp_path / "raw" / "binance" / "perpetual" / "liquidations" / "BTCUSDT").rglob("*.jsonl"))
    assert len(raw_files) == 2
    assert {path.read_bytes() for path in raw_files} == {
        (sell_raw + "\n").encode(),
        (buy_raw + "\n").encode(),
    }

    manifest_file = tmp_path / "control" / "manifests" / "binance_usdm_liquidations.jsonl"
    manifest_rows = [json.loads(line) for line in manifest_file.read_text(encoding="utf-8").splitlines()]
    assert len(manifest_rows) == 2
    latest = manifest_rows[-1]
    assert latest["instrument_id"] == binance_identity("BTCUSDT").instrument_id
    assert latest["raw_message_count"] == latest["event_count"] == latest["observation_count"] == 1
    assert latest["row_count"] == 1
    assert latest["source_claimed_completeness"] == SOURCE_COMPLETENESS
    assert latest["selection_rule"] == SELECTION_RULE
    assert latest["dq_flags"] == ["SOURCE_SELECTION_INCOMPLETENESS"]
    assert pq.ParquetFile(cumulative).read()["instrument_id"].to_pylist() == [
        latest["instrument_id"],
        latest["instrument_id"],
    ]

    checkpoint = json.loads(
        (tmp_path / "control" / "checkpoints" / "binance_usdm_liquidations_BTCUSDT.json").read_text(
            encoding="utf-8"
        )
    )
    assert checkpoint["instrument_id"] == latest["instrument_id"]
    assert checkpoint["source_claimed_completeness"] == SOURCE_COMPLETENESS
    assert checkpoint["selection_rule"] == SELECTION_RULE
    assert checkpoint["last_raw_object_ref"] == latest["raw_object_ref"]
    assert checkpoint["last_raw_sha256"] == latest["raw_sha256"]
    assert checkpoint["last_parquet_refs"] == latest["created_parquets"]
    assert checkpoint["last_parquet_sha256"] == latest["parquet_sha256"]
    assert "coverage_complete" not in checkpoint


def test_different_wire_envelopes_are_not_heuristically_economic_deduplicated(tmp_path: Path):
    payload, raw = _wire(SELL_FIXTURE)
    replay_payload = json.loads(raw)
    replay_payload["E"] += 1
    replay_raw = json.dumps(replay_payload, separators=(",", ":"))
    result = persist_binance_liquidation_batch(
        [(payload, raw, _received()), (replay_payload, replay_raw, _received())],
        tmp_path,
    )
    assert result["records_count"] == 2
    assert result["total_accumulated_rows"] == 2


def test_btc_eth_storage_dedup_manifest_and_checkpoint_isolation(tmp_path: Path):
    btc, btc_raw = _wire(SELL_FIXTURE)
    eth = json.loads(btc_raw)
    eth["o"]["s"] = "ETHUSDT"
    eth_raw = json.dumps(eth, separators=(",", ":"))

    btc_result = persist_binance_liquidation_batch([(btc, btc_raw, _received())], tmp_path)
    btc_parquet = Path(btc_result["parquet_files"][0])
    btc_parquet_hash = hashlib.sha256(btc_parquet.read_bytes()).hexdigest()
    btc_checkpoint = tmp_path / "control" / "checkpoints" / "binance_usdm_liquidations_BTCUSDT.json"
    btc_checkpoint_bytes = btc_checkpoint.read_bytes()

    eth_result = persist_binance_liquidation_batch(
        [(eth, eth_raw, _received())], tmp_path, symbol="ETHUSDT"
    )
    eth_parquet = Path(eth_result["parquet_files"][0])

    assert btc_parquet != eth_parquet
    assert "symbol=BTCUSDT" in str(btc_parquet)
    assert "symbol=ETHUSDT" in str(eth_parquet)
    assert hashlib.sha256(btc_parquet.read_bytes()).hexdigest() == btc_parquet_hash
    assert btc_checkpoint.read_bytes() == btc_checkpoint_bytes
    assert Path(btc_result["raw_file"]).parent != Path(eth_result["raw_file"]).parent
    assert hashlib.sha256(Path(btc_result["raw_file"]).read_bytes()).hexdigest() != hashlib.sha256(
        Path(eth_result["raw_file"]).read_bytes()
    ).hexdigest()
    assert btc_parquet_hash != hashlib.sha256(eth_parquet.read_bytes()).hexdigest()

    btc_table = pq.ParquetFile(btc_parquet).read()
    eth_table = pq.ParquetFile(eth_parquet).read()
    assert btc_table["instrument_id"][0].as_py() == binance_identity("BTCUSDT").instrument_id
    assert eth_table["instrument_id"][0].as_py() == binance_identity("ETHUSDT").instrument_id
    assert btc_table["message_id"][0].as_py() != eth_table["message_id"][0].as_py()

    eth_replay = persist_binance_liquidation_batch(
        [(eth, eth_raw, _received()), (eth, eth_raw, _received())],
        tmp_path,
        symbol="ETHUSDT",
    )
    assert eth_replay["records_count"] == eth_replay["total_accumulated_rows"] == 1

    manifests = [
        json.loads(line)
        for line in (tmp_path / "control" / "manifests" / "binance_usdm_liquidations.jsonl")
        .read_text(encoding="utf-8")
        .splitlines()
    ]
    by_symbol = {
        symbol: [row for row in manifests if row["symbol"] == symbol]
        for symbol in {row["symbol"] for row in manifests}
    }
    assert set(by_symbol) == {"BTCUSDT", "ETHUSDT"}
    assert len(by_symbol["BTCUSDT"]) == 1
    assert len(by_symbol["ETHUSDT"]) == 2
    for symbol, rows in by_symbol.items():
        assert {row["instrument_id"] for row in rows} == {
            binance_identity(symbol).instrument_id
        }
        assert {row["event_count"] for row in rows} == {1}
        assert {row["row_count"] for row in rows} == {1}
    assert by_symbol["BTCUSDT"][0]["raw_message_count"] == 1
    assert by_symbol["BTCUSDT"][0]["observation_count"] == 1
    assert by_symbol["ETHUSDT"][-1]["raw_message_count"] == 2
    assert by_symbol["ETHUSDT"][-1]["observation_count"] == 2

    eth_checkpoint = json.loads(
        (tmp_path / "control" / "checkpoints" / "binance_usdm_liquidations_ETHUSDT.json").read_text(
            encoding="utf-8"
        )
    )
    assert eth_checkpoint["symbol"] == "ETHUSDT"
    assert eth_checkpoint["instrument_id"] == binance_identity("ETHUSDT").instrument_id


def test_same_batch_exact_wire_replay_has_explicit_manifest_counts(tmp_path: Path):
    payload, raw = _wire(SELL_FIXTURE)
    result = persist_binance_liquidation_batch(
        [(payload, raw, _received()), (payload, raw, _received())],
        tmp_path,
    )
    assert result["records_count"] == 1
    manifest = json.loads(
        (tmp_path / "control" / "manifests" / "binance_usdm_liquidations.jsonl")
        .read_text(encoding="utf-8")
        .strip()
    )
    assert manifest["raw_message_count"] == 2
    assert manifest["observation_count"] == 2
    assert manifest["event_count"] == 1
    assert manifest["row_count"] == 1


def test_empty_batch_and_unapproved_symbol_write_nothing(tmp_path: Path):
    result = persist_binance_liquidation_batch([], tmp_path)
    assert result["event_observation_status"] == "NO_EVENT_OBSERVED_WITHIN_WINDOW"
    assert not any(tmp_path.iterdir())
    with pytest.raises(ValueError, match="permit BTCUSDT/ETHUSDT"):
        persist_binance_liquidation_batch([], tmp_path, symbol="SOLUSDT")


@pytest.mark.anyio
async def test_bounded_collector_subscription_heartbeat_and_persistence(monkeypatch, tmp_path: Path):
    sell, sell_raw = _wire(SELL_FIXTURE)
    buy, buy_raw = _wire(BUY_FIXTURE)
    queue = [json.dumps({"result": None, "id": 1}), sell_raw, buy_raw]

    class MockWebSocket:
        def __init__(self):
            self.sent = []

        async def send(self, value):
            self.sent.append(value)

        async def recv(self):
            if queue:
                return queue.pop(0)
            await asyncio.sleep(10)

        async def ping(self):
            future = asyncio.get_running_loop().create_future()
            future.set_result(None)
            return future

    websocket = MockWebSocket()

    class Context:
        async def __aenter__(self):
            return websocket

        async def __aexit__(self, exc_type, exc, tb):
            return None

    import websockets

    monkeypatch.setattr(websockets, "connect", lambda *args, **kwargs: Context())
    result = await collect_binance_liquidations_live(
        tmp_path,
        max_messages=2,
        max_duration_seconds=5,
        flush_interval_seconds=0.01,
    )
    subscription = json.loads(websocket.sent[0])
    assert subscription == {"method": "SUBSCRIBE", "params": ["btcusdt@forceOrder"], "id": 1}
    assert result["transport_status"] == "PASS"
    assert result["subscription_status"] == "PASS"
    assert result["heartbeat_liveness"] == "PASS"
    assert result["event_observation_status"] == "REAL_EVENT_OBSERVED"
    assert result["total_messages_received"] == result["total_records_persisted"] == 2


@pytest.mark.anyio
async def test_bounded_zero_event_transport_is_not_an_acceptance_blocker(monkeypatch, tmp_path: Path):
    queue = [json.dumps({"result": None, "id": 1})]

    class MockWebSocket:
        async def send(self, value):
            return None

        async def recv(self):
            return queue.pop(0)

        async def ping(self):
            future = asyncio.get_running_loop().create_future()
            future.set_result(None)
            return future

    class Context:
        async def __aenter__(self):
            return MockWebSocket()

        async def __aexit__(self, exc_type, exc, tb):
            return None

    import websockets

    monkeypatch.setattr(websockets, "connect", lambda *args, **kwargs: Context())
    result = await collect_binance_liquidations_live(tmp_path, max_duration_seconds=0)
    assert result["event_observation_status"] == "NO_EVENT_OBSERVED_WITHIN_WINDOW"
    assert result["capture_completeness"] == LOCAL_CAPTURE_COMPLETENESS
    assert not any(tmp_path.iterdir())


@pytest.mark.anyio
async def test_eth_topic_btc_payload_mismatch_fails_before_authoritative_write(
    monkeypatch, tmp_path: Path
):
    _, btc_raw = _wire(SELL_FIXTURE)
    queue = [json.dumps({"result": None, "id": 1}), btc_raw]

    class MockWebSocket:
        def __init__(self):
            self.sent = []

        async def send(self, value):
            self.sent.append(value)

        async def recv(self):
            return queue.pop(0)

        async def ping(self):
            future = asyncio.get_running_loop().create_future()
            future.set_result(None)
            return future

    websocket = MockWebSocket()

    class Context:
        async def __aenter__(self):
            return websocket

        async def __aexit__(self, exc_type, exc, tb):
            return None

    import websockets

    monkeypatch.setattr(websockets, "connect", lambda *args, **kwargs: Context())
    with pytest.raises(ValueError, match="Symbol mismatch"):
        await collect_binance_liquidations_live(
            tmp_path,
            symbol="ETHUSDT",
            max_messages=1,
            max_duration_seconds=5,
        )
    assert json.loads(websocket.sent[0])["params"] == ["ethusdt@forceOrder"]
    assert not (tmp_path / "raw").exists()
    assert not (tmp_path / "normalized").exists()
    assert not (tmp_path / "control").exists()
    rejected = list((tmp_path / "quarantine" / "liquidation_rejected_frames").rglob("*.jsonl"))
    assert len(rejected) == 1
    assert rejected[0].read_text(encoding="utf-8") == btc_raw + "\n"
