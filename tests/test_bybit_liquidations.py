"""Tests for Bybit Linear Liquidations Normalization, Storage and Invariants (Phase 1D.3A)."""

import hashlib
import json
import tempfile
from datetime import UTC, datetime
from decimal import Decimal
from pathlib import Path

import pyarrow.parquet as pq
import pytest
import yaml

from crypto_quant.contracts import DataContract
from crypto_quant.ingestion.binance.funding import funding_identity as binance_funding_identity
from crypto_quant.ingestion.bybit.funding import funding_identity
from crypto_quant.ingestion.bybit.liquidations import (
    CONTRACT_ID,
    DATASET_ID,
    CanonicalLiquidationRecord,
    bybit_linear_liquidation_data_contract,
    merge_and_write_liquidation_parquet,
    parse_bybit_liquidation_message,
    persist_bybit_liquidation_batch,
    validate_liquidation_records_dq,
)
from crypto_quant.ingestion.bybit.trades import bybit_spot_identity
from crypto_quant.time import parse_epoch


def test_bybit_liquidation_frozen_yaml_contract_validates():
    """Validates frozen YAML contract file and Python DataContract definition."""
    yaml_path = Path("schemas/contracts/bybit_linear_all_liquidation_ws_v1.yaml")
    assert yaml_path.exists(), "Frozen YAML contract must exist"
    data = yaml.safe_load(yaml_path.read_text(encoding="utf-8"))
    assert data["contract_id"] == "bybit.linear.ws.all-liquidation.v1"
    assert data["exchange"] == "bybit"
    assert data["market_type"] == "perpetual"
    assert data["source_kind"] == "websocket"
    assert len(data["fields"]) == 8

    contract = bybit_linear_liquidation_data_contract()
    assert isinstance(contract, DataContract)
    assert contract.contract_id == CONTRACT_ID
    assert contract.source_dataset_id == DATASET_ID
    assert contract.exchange == "bybit"
    assert contract.market_type == "perpetual"
    assert len(contract.fields) == 8

    # T field must be documented as event_updated_time (NOT event_trade_time or fill time)
    t_field = next(f for f in contract.fields if f.source_field == "data[].T")
    assert t_field.timestamp_meaning == "event_updated_time", (
        "T is official Bybit 'updated time', not trade/fill/execution time"
    )

    # v field unit must be base_coin for the in-scope Bybit USDT linear perpetuals.
    v_field = next(f for f in contract.fields if f.source_field == "data[].v")
    assert v_field.source_unit == "base_coin"

    # YAML must document completeness split and native_event_id
    assert data["stream_rules"]["source_claimed_completeness"] == "ALL_LIQUIDATIONS"
    assert data["stream_rules"]["delivery_semantics"] == "BATCHED_500MS_PUSH"
    assert data["native_event_id"] == "NONE"


def test_bybit_liquidation_side_semantics_truth_table():
    """Proves official Bybit side semantics:

    - S='Buy'  -> Long position was liquidated
    - S='Sell' -> Short position was liquidated
    """
    ident = funding_identity("BTCUSDT")
    recv_t = datetime(2026, 8, 11, 10, 0, 0, tzinfo=UTC)

    # 1. Long liquidation (S='Buy')
    msg_buy = {
        "topic": "allLiquidation.BTCUSDT",
        "type": "snapshot",
        "ts": 1786434825553,
        "data": [{"T": 1786434825244, "s": "BTCUSDT", "S": "Buy", "v": "1.500", "p": "62500.00"}],
    }
    recs_buy = parse_bybit_liquidation_message(msg_buy, ident, received_at=recv_t)
    assert len(recs_buy) == 1
    assert recs_buy[0].source_side == "Buy"
    assert recs_buy[0].position_side_liquidated == "LONG", "S='Buy' must map to LONG liquidated position"
    assert recs_buy[0].source_side_semantic == "LIQUIDATED_POSITION_SIDE"

    # 2. Short liquidation (S='Sell')
    msg_sell = {
        "topic": "allLiquidation.BTCUSDT",
        "type": "snapshot",
        "ts": 1786434825553,
        "data": [{"T": 1786434825244, "s": "BTCUSDT", "S": "Sell", "v": "2.000", "p": "64000.00"}],
    }
    recs_sell = parse_bybit_liquidation_message(msg_sell, ident, received_at=recv_t)
    assert len(recs_sell) == 1
    assert recs_sell[0].source_side == "Sell"
    assert recs_sell[0].position_side_liquidated == "SHORT", "S='Sell' must map to SHORT liquidated position"


def test_bybit_liquidation_price_semantics_and_units():
    """Proves price semantic is explicitly 'bankruptcy_price' and volume is base coin."""
    ident = funding_identity("BTCUSDT")
    recv_t = datetime(2026, 8, 11, 10, 0, 0, tzinfo=UTC)

    raw_msg = {
        "topic": "allLiquidation.BTCUSDT",
        "type": "snapshot",
        "ts": 1786434825553,
        "data": [{"T": 1786434825244, "s": "BTCUSDT", "S": "Buy", "v": "0.12345678", "p": "58999.50"}],
    }
    recs = parse_bybit_liquidation_message(raw_msg, ident, received_at=recv_t)
    rec = recs[0]

    assert rec.price_semantic == "bankruptcy_price"
    assert rec.source_price == "58999.50"
    assert rec.average_fill_price is None, "Bybit does not provide execution fill price; no synthetic guessing"
    assert rec.source_quantity == "0.12345678"
    assert rec.source_quantity_unit == "base_coin"
    assert rec.quantity_base == "0.12345678"
    assert rec.notional_quote is None, "No silent synthetic multiplication without versioned derivation lineage"

    # T semantic: event_time must be the updated timestamp T from the message (not ts/exchange_timestamp)
    from crypto_quant.time import parse_epoch
    expected_event_time = parse_epoch(1786434825244, unit="ms")
    assert rec.event_time == expected_event_time, "event_time must map to T (updated timestamp), not ts"
    expected_exchange_ts = parse_epoch(1786434825553, unit="ms")
    assert rec.exchange_timestamp == expected_exchange_ts, "exchange_timestamp must map to ts (system push time)"


def test_bybit_liquidation_eth_side_decimal_and_time_semantics():
    """ETHUSDT has the same verified stream semantics without inheriting a BTC identity."""
    ident = funding_identity("ETHUSDT")
    recv_t = datetime(2026, 8, 11, 10, 0, 0, 123456, tzinfo=UTC)
    raw_msg = {
        "topic": "allLiquidation.ETHUSDT",
        "type": "snapshot",
        "ts": 1786434825553,
        "data": [
            {"T": 1786434825100, "s": "ETHUSDT", "S": "Buy", "v": "12.345000", "p": "3050.1250"},
            {"T": 1786434825200, "s": "ETHUSDT", "S": "Sell", "v": "0.000100", "p": "3051.0000"},
        ],
    }

    records = parse_bybit_liquidation_message(raw_msg, ident, received_at=recv_t)

    assert ident.exchange == "bybit"
    assert records[0].instrument_id == ident.instrument_id
    assert records[0].instrument_id != funding_identity("BTCUSDT").instrument_id
    assert [r.position_side_liquidated for r in records] == ["LONG", "SHORT"]
    assert [r.source_quantity for r in records] == ["12.345000", "0.000100"]
    assert [r.source_price for r in records] == ["3050.1250", "3051.0000"]
    assert [Decimal(r.source_quantity) for r in records] == [Decimal("12.345000"), Decimal("0.000100")]
    assert [Decimal(r.source_price) for r in records] == [Decimal("3050.1250"), Decimal("3051.0000")]
    assert records[0].event_time == parse_epoch(1786434825100, unit="ms")
    assert records[0].exchange_timestamp == parse_epoch(1786434825553, unit="ms")
    assert all(r.knowledge_time == recv_t for r in records)
    assert all(r.source_quantity_unit == "base_coin" for r in records)


def test_perpetual_identity_matrix_is_exchange_and_symbol_specific():
    """The four in-scope perpetuals retain all canonical identity dimensions."""
    identities = {
        ("binance", "BTCUSDT"): binance_funding_identity("BTCUSDT"),
        ("binance", "ETHUSDT"): binance_funding_identity("ETHUSDT"),
        ("bybit", "BTCUSDT"): funding_identity("BTCUSDT"),
        ("bybit", "ETHUSDT"): funding_identity("ETHUSDT"),
    }

    assert len({ident.instrument_id for ident in identities.values()}) == 4
    for (exchange, symbol), ident in identities.items():
        base = symbol.removesuffix("USDT")
        assert ident.exchange == exchange
        assert ident.venue_environment == "production"
        assert ident.native_symbol == symbol
        assert ident.market_type == "perpetual"
        assert ident.contract_type == "linear_perpetual"
        assert ident.base_asset == base
        assert ident.quote_asset == "USDT"
        assert ident.settle_asset == "USDT"
        assert ident.quantity_unit == base
        assert ident.notional_unit == "USDT"
        assert ident.expiry is None

    # A real existing Bybit Spot identity differs only in product semantics that
    # must participate in canonical identity; no Spot collector is introduced.
    bybit_btc_spot = bybit_spot_identity("BTCUSDT")
    bybit_btc_perpetual = identities[("bybit", "BTCUSDT")]
    assert bybit_btc_spot.market_type == "spot"
    assert bybit_btc_spot.contract_type == "spot"
    assert bybit_btc_spot.instrument_id != bybit_btc_perpetual.instrument_id


def test_bybit_liquidation_eth_multiplicity_replay_and_lineage(tmp_path: Path):
    """ETH exact-wire replay deduplicates while identical batch events retain multiplicity and lineage."""
    received_at = datetime(2026, 8, 11, 10, 0, 0, tzinfo=UTC)
    raw_msg = {
        "topic": "allLiquidation.ETHUSDT",
        "type": "snapshot",
        "ts": 1786434825553,
        "data": [
            {"T": 1786434825100, "s": "ETHUSDT", "S": "Buy", "v": "1.5", "p": "3050.0"},
            {"T": 1786434825100, "s": "ETHUSDT", "S": "Buy", "v": "1.5", "p": "3050.0"},
        ],
    }
    raw_wire = json.dumps(raw_msg, separators=(",", ":"))

    first = persist_bybit_liquidation_batch([(raw_msg, raw_wire)], "ETHUSDT", tmp_path, received_at=received_at)
    replay = persist_bybit_liquidation_batch([(raw_msg, raw_wire)], "ETHUSDT", tmp_path, received_at=received_at)

    assert first["records_count"] == 2
    assert replay["total_accumulated_rows"] == 2
    raw_files = list((tmp_path / "raw" / "bybit" / "perpetual" / "liquidations" / "ETHUSDT").rglob("*.jsonl"))
    parquet_files = list(
        (tmp_path / "normalized" / "liquidations" / "v1" / "exchange=bybit" / "market_type=perpetual" / "symbol=ETHUSDT").rglob("*.parquet")
    )
    manifest_file = tmp_path / "control" / "manifests" / "bybit_linear_liquidations.jsonl"
    checkpoint_file = tmp_path / "control" / "checkpoints" / "bybit_linear_liquidations_ETHUSDT.json"

    assert len(raw_files) == 1
    assert len(parquet_files) == 1
    assert pq.ParquetFile(parquet_files[0]).metadata.num_rows == 2
    assert checkpoint_file.exists()
    manifest_rows = [json.loads(line) for line in manifest_file.read_text(encoding="utf-8").splitlines()]
    assert len(manifest_rows) == 1
    assert manifest_rows[0]["symbol"] == "ETHUSDT"
    assert manifest_rows[0]["raw_message_count"] == 1
    assert manifest_rows[0]["event_count"] == 2
    assert manifest_rows[0]["row_count"] == 2
    assert "symbol=ETHUSDT" in manifest_rows[0]["created_parquets"][0]
    assert "liquidations/ETHUSDT" in manifest_rows[0]["raw_object_ref"]


def test_bybit_liquidation_cross_symbol_same_event_never_collides(tmp_path: Path):
    """BTC and ETH retain independent lineage in one root, even for equal event values."""
    received_at = datetime(2026, 8, 11, 10, 0, 0, tzinfo=UTC)
    common = {"T": 1786434825100, "S": "Buy", "v": "1.0", "p": "3000.0"}
    btc_message = {"topic": "allLiquidation.BTCUSDT", "ts": 1786434825553, "data": [{**common, "s": "BTCUSDT"}]}
    eth_message = {"topic": "allLiquidation.ETHUSDT", "ts": 1786434825553, "data": [{**common, "s": "ETHUSDT"}]}

    btc_result = persist_bybit_liquidation_batch([btc_message], "BTCUSDT", tmp_path, received_at=received_at)
    btc_checkpoint = tmp_path / "control" / "checkpoints" / "bybit_linear_liquidations_BTCUSDT.json"
    btc_checkpoint_before_eth = btc_checkpoint.read_bytes()
    btc_parquet_before_eth = list(
        (tmp_path / "normalized" / "liquidations" / "v1" / "exchange=bybit" / "market_type=perpetual" / "symbol=BTCUSDT").rglob("*.parquet")
    )
    assert len(btc_parquet_before_eth) == 1
    btc_parquet_bytes_before_eth = btc_parquet_before_eth[0].read_bytes()
    btc_parquet_hash_before_eth = hashlib.sha256(btc_parquet_bytes_before_eth).hexdigest()
    eth_result = persist_bybit_liquidation_batch([eth_message], "ETHUSDT", tmp_path, received_at=received_at)

    manifest_file = tmp_path / "control" / "manifests" / "bybit_linear_liquidations.jsonl"
    manifest_rows = [json.loads(line) for line in manifest_file.read_text(encoding="utf-8").splitlines()]
    manifest_by_symbol = {row["symbol"]: row for row in manifest_rows}
    eth_checkpoint = tmp_path / "control" / "checkpoints" / "bybit_linear_liquidations_ETHUSDT.json"
    btc_raw = list((tmp_path / "raw" / "bybit" / "perpetual" / "liquidations" / "BTCUSDT").rglob("*.jsonl"))
    eth_raw = list((tmp_path / "raw" / "bybit" / "perpetual" / "liquidations" / "ETHUSDT").rglob("*.jsonl"))
    btc_parquet = list(
        (tmp_path / "normalized" / "liquidations" / "v1" / "exchange=bybit" / "market_type=perpetual" / "symbol=BTCUSDT").rglob("*.parquet")
    )
    eth_parquet = list(
        (tmp_path / "normalized" / "liquidations" / "v1" / "exchange=bybit" / "market_type=perpetual" / "symbol=ETHUSDT").rglob("*.parquet")
    )

    assert btc_result["total_accumulated_rows"] == 1
    assert eth_result["total_accumulated_rows"] == 1
    assert btc_checkpoint.read_bytes() == btc_checkpoint_before_eth
    assert btc_checkpoint.name != eth_checkpoint.name
    assert btc_checkpoint.exists() and eth_checkpoint.exists()
    assert btc_checkpoint.read_bytes() != eth_checkpoint.read_bytes()
    assert len(btc_raw) == len(eth_raw) == 1
    assert len(btc_parquet) == len(eth_parquet) == 1
    assert pq.ParquetFile(btc_parquet[0]).metadata.num_rows == 1
    assert pq.ParquetFile(eth_parquet[0]).metadata.num_rows == 1
    assert btc_parquet[0].read_bytes() == btc_parquet_bytes_before_eth
    assert hashlib.sha256(btc_parquet[0].read_bytes()).hexdigest() == btc_parquet_hash_before_eth
    assert set(manifest_by_symbol) == {"BTCUSDT", "ETHUSDT"}
    assert manifest_by_symbol["BTCUSDT"]["instrument_id"] != manifest_by_symbol["ETHUSDT"]["instrument_id"]
    assert manifest_by_symbol["BTCUSDT"]["instrument_id"] == funding_identity("BTCUSDT").instrument_id
    assert manifest_by_symbol["ETHUSDT"]["instrument_id"] == funding_identity("ETHUSDT").instrument_id
    assert manifest_by_symbol["BTCUSDT"]["raw_message_count"] == manifest_by_symbol["ETHUSDT"]["raw_message_count"] == 1
    assert manifest_by_symbol["BTCUSDT"]["event_count"] == manifest_by_symbol["ETHUSDT"]["event_count"] == 1
    assert pq.ParquetFile(btc_parquet[0]).read()["instrument_id"].to_pylist() == [manifest_by_symbol["BTCUSDT"]["instrument_id"]]
    assert pq.ParquetFile(eth_parquet[0]).read()["instrument_id"].to_pylist() == [manifest_by_symbol["ETHUSDT"]["instrument_id"]]
    assert manifest_by_symbol["BTCUSDT"]["raw_object_ref"] != manifest_by_symbol["ETHUSDT"]["raw_object_ref"]
    assert manifest_by_symbol["BTCUSDT"]["created_parquets"] != manifest_by_symbol["ETHUSDT"]["created_parquets"]

    persist_bybit_liquidation_batch([eth_message], "ETHUSDT", tmp_path, received_at=received_at)
    assert btc_checkpoint.read_bytes() == btc_checkpoint_before_eth

    btc = parse_bybit_liquidation_message(btc_message, funding_identity("BTCUSDT"), received_at)[0]
    eth = parse_bybit_liquidation_message(eth_message, funding_identity("ETHUSDT"), received_at)[0]

    assert btc.instrument_id != eth.instrument_id
    assert btc.dedup_fingerprint != eth.dedup_fingerprint
    assert (btc.exchange, btc.instrument_id, btc.event_time, btc.dedup_fingerprint) != (
        eth.exchange,
        eth.instrument_id,
        eth.event_time,
        eth.dedup_fingerprint,
    )


def test_bybit_liquidation_eth_wrong_routing_fails_before_storage_writes(tmp_path: Path):
    """ETH routing and payload-symbol mismatches fail closed before raw or normalized lineage exists."""
    wrong_topic = {
        "topic": "allLiquidation.BTCUSDT",
        "ts": 1786434825553,
        "data": [{"T": 1786434825100, "s": "ETHUSDT", "S": "Buy", "v": "1", "p": "3000"}],
    }
    wrong_payload_symbol = {
        "topic": "allLiquidation.ETHUSDT",
        "ts": 1786434825553,
        "data": [{"T": 1786434825100, "s": "BTCUSDT", "S": "Buy", "v": "1", "p": "3000"}],
    }
    mixed_eth_batch = {
        "topic": "allLiquidation.ETHUSDT",
        "ts": 1786434825553,
        "data": [
            {"T": 1786434825100, "s": "ETHUSDT", "S": "Buy", "v": "1", "p": "3000"},
            {"T": 1786434825200, "s": "BTCUSDT", "S": "Sell", "v": "1", "p": "60000"},
        ],
    }
    mixed_btc_batch = {
        "topic": "allLiquidation.BTCUSDT",
        "ts": 1786434825553,
        "data": [
            {"T": 1786434825100, "s": "BTCUSDT", "S": "Buy", "v": "1", "p": "60000"},
            {"T": 1786434825200, "s": "ETHUSDT", "S": "Sell", "v": "1", "p": "3000"},
        ],
    }

    with pytest.raises(ValueError, match="Topic mismatch"):
        persist_bybit_liquidation_batch([wrong_topic], "ETHUSDT", tmp_path)
    with pytest.raises(ValueError, match="Symbol mismatch"):
        persist_bybit_liquidation_batch([wrong_payload_symbol], "ETHUSDT", tmp_path)
    with pytest.raises(ValueError, match="Symbol mismatch"):
        persist_bybit_liquidation_batch([mixed_eth_batch], "ETHUSDT", tmp_path)
    with pytest.raises(ValueError, match="Symbol mismatch"):
        persist_bybit_liquidation_batch([mixed_btc_batch], "BTCUSDT", tmp_path)

    assert not (tmp_path / "raw").exists()
    assert not (tmp_path / "normalized").exists()
    assert not (tmp_path / "control").exists()


def test_bybit_liquidation_batch_identical_events_preserves_multiplicity():
    """Proves that two identical-content events in a single batch are NOT silently collapsed."""
    ident = funding_identity("BTCUSDT")
    recv_t = datetime(2026, 8, 11, 10, 0, 0, tzinfo=UTC)

    raw_msg = {
        "topic": "allLiquidation.BTCUSDT",
        "type": "snapshot",
        "ts": 1786434825553,
        "data": [
            {"T": 1786434825100, "s": "BTCUSDT", "S": "Buy", "v": "1.0", "p": "60000.00"},
            {"T": 1786434825100, "s": "BTCUSDT", "S": "Buy", "v": "1.0", "p": "60000.00"},  # Identical content
        ],
    }
    recs = parse_bybit_liquidation_message(raw_msg, ident, received_at=recv_t)
    assert len(recs) == 2

    # Both share identical message_id
    assert recs[0].message_id == recs[1].message_id

    # Each has a distinct dedup_fingerprint (incorporating event_index)
    assert recs[0].dedup_fingerprint != recs[1].dedup_fingerprint

    # Writing to Parquet preserves both distinct events
    with tempfile.TemporaryDirectory() as tmp_dir:
        yr_dir = Path(tmp_dir) / "year=2026"
        p_path, total_rows, _, _ = merge_and_write_liquidation_parquet(yr_dir, "BTCUSDT", 2026, recs)
        assert total_rows == 2, "Both identical events in one batch must be preserved without loss"
        tbl = pq.ParquetFile(p_path).read()
        assert len(tbl) == 2


def test_bybit_liquidation_duplicate_delivery_is_safely_deduplicated():
    """Proves that re-delivering the exact same raw message does not duplicate rows."""
    ident = funding_identity("BTCUSDT")
    recv_t = datetime(2026, 8, 11, 10, 0, 0, tzinfo=UTC)

    raw_msg_str = json.dumps({
        "topic": "allLiquidation.BTCUSDT",
        "type": "snapshot",
        "ts": 1786434825553,
        "data": [{"T": 1786434825100, "s": "BTCUSDT", "S": "Buy", "v": "1.0", "p": "60000.00"}],
    })
    raw_msg = json.loads(raw_msg_str)

    recs_first = parse_bybit_liquidation_message(raw_msg, ident, received_at=recv_t, raw_msg_str=raw_msg_str)
    recs_replay = parse_bybit_liquidation_message(raw_msg, ident, received_at=recv_t, raw_msg_str=raw_msg_str)

    with tempfile.TemporaryDirectory() as tmp_dir:
        yr_dir = Path(tmp_dir) / "year=2026"
        p_path, total_rows, _, _ = merge_and_write_liquidation_parquet(yr_dir, "BTCUSDT", 2026, recs_first)
        assert total_rows == 1

        # Re-merge identical replay
        p_path2, total_rows2, _, _ = merge_and_write_liquidation_parquet(yr_dir, "BTCUSDT", 2026, recs_replay)
        assert total_rows2 == 1, "Duplicate re-delivery must deduplicate to 1 row"


def test_bybit_liquidation_snapshot_does_not_replace_accumulated_data():
    """Proves that subsequent snapshot messages append/accumulate rather than replace old data."""
    ident = funding_identity("BTCUSDT")
    t1 = datetime(2026, 8, 11, 10, 0, 0, tzinfo=UTC)
    t2 = datetime(2026, 8, 11, 10, 1, 0, tzinfo=UTC)

    msg1 = {
        "topic": "allLiquidation.BTCUSDT",
        "type": "snapshot",
        "ts": 1786434825000,
        "data": [{"T": 1786434825000, "s": "BTCUSDT", "S": "Buy", "v": "1.0", "p": "60000.00"}],
    }
    msg2 = {
        "topic": "allLiquidation.BTCUSDT",
        "type": "snapshot",
        "ts": 1786434826000,
        "data": [{"T": 1786434826000, "s": "BTCUSDT", "S": "Sell", "v": "2.0", "p": "61000.00"}],
    }

    recs1 = parse_bybit_liquidation_message(msg1, ident, received_at=t1)
    recs2 = parse_bybit_liquidation_message(msg2, ident, received_at=t2)

    with tempfile.TemporaryDirectory() as tmp_dir:
        yr_dir = Path(tmp_dir) / "year=2026"
        _, rows1, _, _ = merge_and_write_liquidation_parquet(yr_dir, "BTCUSDT", 2026, recs1)
        assert rows1 == 1

        p2, rows2, _, _ = merge_and_write_liquidation_parquet(yr_dir, "BTCUSDT", 2026, recs2)
        assert rows2 == 2, "Second snapshot message must accumulate with first snapshot"
        tbl = pq.ParquetFile(p2).read()
        assert len(tbl) == 2


def test_bybit_liquidation_realtime_knowledge_time_must_equal_received_at():
    """Proves that knowledge_time strictly equals received_at for realtime stream ingestion."""
    ident = funding_identity("BTCUSDT")
    recv_t = datetime(2026, 8, 11, 10, 15, 30, 123456, tzinfo=UTC)

    raw_msg = {
        "topic": "allLiquidation.BTCUSDT",
        "type": "snapshot",
        "ts": 1786434825553,
        "data": [{"T": 1786434825244, "s": "BTCUSDT", "S": "Buy", "v": "0.500", "p": "63000.00"}],
    }
    recs = parse_bybit_liquidation_message(raw_msg, ident, received_at=recv_t)
    assert recs[0].knowledge_time == recv_t
    assert recs[0].received_at == recv_t


def test_bybit_liquidation_dq_validation_catches_errors():
    ident = funding_identity("BTCUSDT")
    recv_t = datetime(2026, 8, 11, 10, 0, 0, tzinfo=UTC)

    # Valid
    raw_msg = {
        "topic": "allLiquidation.BTCUSDT",
        "type": "snapshot",
        "ts": 1786434825553,
        "data": [{"T": 1786434825244, "s": "BTCUSDT", "S": "Buy", "v": "1.000", "p": "62000.00"}],
    }
    recs = parse_bybit_liquidation_message(raw_msg, ident, received_at=recv_t)
    assert len(validate_liquidation_records_dq(recs)) == 0

    invalid_side = dict(raw_msg)
    invalid_side["data"] = [dict(raw_msg["data"][0], S="Unknown")]
    with pytest.raises(ValueError, match="Unsupported liquidation side"):
        parse_bybit_liquidation_message(invalid_side, ident, received_at=recv_t)

    # Invalid topic
    with pytest.raises(ValueError, match="Topic mismatch"):
        parse_bybit_liquidation_message({"topic": "allLiquidation.ETHUSDT", "ts": 123, "data": []}, ident, recv_t)

    # Missing mandatory price
    with pytest.raises(ValueError, match="Missing mandatory field 'p'"):
        parse_bybit_liquidation_message(
            {"topic": "allLiquidation.BTCUSDT", "ts": 123, "data": [{"T": 123, "s": "BTCUSDT", "S": "Buy", "v": "1"}]},
            ident,
            recv_t,
        )

    # Negative price in DQ
    rec_bad = CanonicalLiquidationRecord(
        exchange="bybit",
        instrument_id=ident.instrument_id,
        symbol=ident.native_symbol,
        market_type="perpetual",
        contract_type="linear_perpetual",
        venue_product_type="linear",
        event_time=recv_t,
        exchange_timestamp=recv_t,
        received_at=recv_t,
        processed_at=recv_t,
        knowledge_time=recv_t,
        position_side_liquidated="LONG",
        source_side="Buy",
        source_side_semantic="LIQUIDATED_POSITION_SIDE",
        source_quantity="1.0",
        source_quantity_unit="base_coin",
        quantity_base="1.0",
        notional_quote=None,
        last_filled_quantity=None,
        accumulated_filled_quantity=None,
        source_price="-500.0",
        price_semantic="bankruptcy_price",
        average_fill_price=None,
        order_type=None,
        time_in_force=None,
        order_status=None,
        source_claimed_completeness="ALL_LIQUIDATIONS",
        delivery_semantics="BATCHED_500MS_PUSH",
        message_id="msg1",
        dedup_fingerprint="fp1",
        dedup_guarantee="EXACT_WIRE_REPLAY_ONLY",
        source=DATASET_ID,
        source_contract_version=CONTRACT_ID,
        schema_version="1.0.0",
        collector_version="0.4.0",
        normalization_version="1.0.0",
    )
    issues = validate_liquidation_records_dq([rec_bad])
    assert any("Non-positive source_price" in issue for issue in issues)


def test_merge_and_write_liquidation_parquet_immutable_generations():
    """Proves that accumulation creates new immutable generation files without mutating previous generations."""
    with tempfile.TemporaryDirectory() as tmp_dir:
        root = Path(tmp_dir)
        ident = funding_identity("BTCUSDT")
        t1 = datetime(2026, 8, 11, 10, 0, 0, tzinfo=UTC)
        t2 = datetime(2026, 8, 11, 10, 1, 0, tzinfo=UTC)

        msg1 = {
            "topic": "allLiquidation.BTCUSDT",
            "type": "snapshot",
            "ts": int(t1.timestamp() * 1000),
            "data": [{"T": int(t1.timestamp() * 1000), "s": "BTCUSDT", "S": "Buy", "v": "1.0", "p": "60000.0"}],
        }
        recs1 = parse_bybit_liquidation_message(msg1, ident, received_at=t1)

        yr_dir = (
            root
            / "normalized"
            / "liquidations"
            / "v1"
            / "exchange=bybit"
            / "market_type=perpetual"
            / "symbol=BTCUSDT"
            / f"year={t1.year}"
        )

        # 1. Write Generation G1
        g1_path, g1_rows, g1_sha, _ = merge_and_write_liquidation_parquet(yr_dir, "BTCUSDT", t1.year, recs1)
        assert g1_path.exists()
        assert g1_rows == 1
        g1_bytes_before = g1_path.read_bytes()

        # 2. Write Generation G2 with additional observation
        msg2 = {
            "topic": "allLiquidation.BTCUSDT",
            "type": "snapshot",
            "ts": int(t2.timestamp() * 1000),
            "data": [{"T": int(t2.timestamp() * 1000), "s": "BTCUSDT", "S": "Sell", "v": "2.0", "p": "61000.0"}],
        }
        recs2 = parse_bybit_liquidation_message(msg2, ident, received_at=t2)

        g2_path, g2_rows, g2_sha, _ = merge_and_write_liquidation_parquet(yr_dir, "BTCUSDT", t2.year, recs2)
        assert g2_path.exists()
        assert g2_rows == 2

        # 3. Prove G1 remains byte-identical on disk (immutable)
        assert g1_path.read_bytes() == g1_bytes_before

        # 4. Prove G2 has both records strictly sorted
        tbl2 = pq.ParquetFile(g2_path).read()
        assert len(tbl2) == 2
        assert tbl2["position_side_liquidated"][0].as_py() == "LONG"
        assert tbl2["position_side_liquidated"][1].as_py() == "SHORT"


def test_persist_bybit_liquidation_batch_end_to_end_and_idempotent():
    """Proves full batch persistence, raw JSONL hashing, Parquet creation, manifest logging, and idempotency."""
    with tempfile.TemporaryDirectory() as tmp_dir:
        root = Path(tmp_dir)
        recv_t = datetime(2026, 8, 11, 10, 0, 0, tzinfo=UTC)

        raw_messages = [
            {
                "topic": "allLiquidation.BTCUSDT",
                "type": "snapshot",
                "ts": 1786434825553,
                "data": [
                    {"T": 1786434825100, "s": "BTCUSDT", "S": "Buy", "v": "0.500", "p": "62000.00"},
                    {"T": 1786434825200, "s": "BTCUSDT", "S": "Sell", "v": "1.200", "p": "62100.00"},
                ],
            }
        ]

        # 1. First Ingestion
        res1 = persist_bybit_liquidation_batch(raw_messages, "BTCUSDT", root, received_at=recv_t)
        assert res1["status"] == "PASS"
        assert res1["event_observation_status"] == "REAL_EVENT_OBSERVED"
        assert res1["records_count"] == 2
        assert res1["total_accumulated_rows"] == 2

        # Verify raw JSONL
        raw_files = list((root / "raw" / "bybit" / "perpetual" / "liquidations" / "BTCUSDT").rglob("*.jsonl"))
        assert len(raw_files) == 1

        # Verify Parquet
        parquet_files = list((root / "normalized" / "liquidations" / "v1" / "exchange=bybit" / "market_type=perpetual" / "symbol=BTCUSDT").rglob("*.parquet"))
        assert len(parquet_files) == 1

        # Verify Manifest
        manifest_file = root / "control" / "manifests" / "bybit_linear_liquidations.jsonl"
        assert manifest_file.exists()
        manifest_lines = manifest_file.read_text(encoding="utf-8").strip().splitlines()
        assert len(manifest_lines) == 1
        mdata = json.loads(manifest_lines[0])
        assert mdata["exchange"] == "bybit"
        assert mdata["symbol"] == "BTCUSDT"
        assert mdata["raw_message_count"] == 1
        assert mdata["event_count"] == 2
        assert mdata["row_count"] == 2
        assert mdata["source_claimed_completeness"] == "ALL_LIQUIDATIONS"
        assert mdata["delivery_semantics"] == "BATCHED_500MS_PUSH"

        # Verify Checkpoint
        chk_file = root / "control" / "checkpoints" / "bybit_linear_liquidations_BTCUSDT.json"
        assert chk_file.exists()
        chk = json.loads(chk_file.read_text(encoding="utf-8"))
        assert chk["total_records"] == 2

        # 2. Re-running identical batch must be idempotent (no duplicate manifest lines, no duplicate parquet generations)
        res2 = persist_bybit_liquidation_batch(raw_messages, "BTCUSDT", root, received_at=recv_t)
        assert res2["status"] == "PASS"

        manifest_lines_after = manifest_file.read_text(encoding="utf-8").strip().splitlines()
        assert len(manifest_lines_after) == 1, "Idempotent rerun must not duplicate manifest entries"

        parquet_files_after = list((root / "normalized" / "liquidations" / "v1" / "exchange=bybit" / "market_type=perpetual" / "symbol=BTCUSDT").rglob("*.parquet"))
        assert len(parquet_files_after) == 1, "Idempotent rerun must not spawn duplicate generation files"


def test_empty_liquidation_batch_does_not_mutate_storage_or_manifest():
    """Proves that 0-event intervals do not write empty files or false manifest rows."""
    with tempfile.TemporaryDirectory() as tmp_dir:
        root = Path(tmp_dir)
        res = persist_bybit_liquidation_batch([], "BTCUSDT", root)
        assert res["status"] == "PASS"
        assert res["event_observation_status"] == "NO_EVENT_OBSERVED_WITHIN_WINDOW"
        assert res["records_count"] == 0
        assert not (root / "control" / "manifests" / "bybit_linear_liquidations.jsonl").exists()


@pytest.mark.anyio
async def test_collect_bybit_liquidations_live_mocked(monkeypatch):
    """Proves the asynchronous Bybit real-time liquidation collector lifecycle with mocked WebSocket."""
    import asyncio
    from unittest.mock import AsyncMock

    from crypto_quant.ingestion.bybit.liquidations import collect_bybit_liquidations_live

    with tempfile.TemporaryDirectory() as tmp_dir:
        root = Path(tmp_dir)

        mock_ws = AsyncMock()
        mock_ws.send = AsyncMock()
        # Sequence of messages: ACK followed by 2 liquidation messages, then timeout
        ack_str = json.dumps({"success": True, "ret_msg": "", "conn_id": "test_conn", "op": "subscribe"})
        msg1_str = json.dumps({
            "topic": "allLiquidation.BTCUSDT",
            "type": "snapshot",
            "ts": 1786434825553,
            "data": [{"T": 1786434825100, "s": "BTCUSDT", "S": "Buy", "v": "1.0", "p": "62000.00"}],
        })
        msg2_str = json.dumps({
            "topic": "allLiquidation.BTCUSDT",
            "type": "snapshot",
            "ts": 1786434826000,
            "data": [{"T": 1786434825900, "s": "BTCUSDT", "S": "Sell", "v": "2.0", "p": "62100.00"}],
        })

        recv_queue = [ack_str, msg1_str, msg2_str]

        async def mock_recv():
            if recv_queue:
                return recv_queue.pop(0)
            await asyncio.sleep(10.0)  # Trigger timeout
            raise TimeoutError()

        mock_ws.recv = mock_recv

        class MockWSContext:
            async def __aenter__(self):
                return mock_ws

            async def __aexit__(self, exc_type, exc, tb):
                pass

        import websockets

        monkeypatch.setattr(websockets, "connect", lambda *args, **kwargs: MockWSContext())

        res = await collect_bybit_liquidations_live(
            "BTCUSDT",
            root,
            max_messages=2,
            flush_interval_seconds=0.1,
        )

        assert res["status"] == "PASS"
        assert res["transport_status"] == "PASS"
        assert res["subscription_status"] == "PASS"
        assert res["event_observation_status"] == "REAL_EVENT_OBSERVED"
        assert res["total_messages_received"] == 2
        assert res["total_records_persisted"] == 2

        # Verify Parquet on disk
        parquet_files = list((root / "normalized" / "liquidations" / "v1" / "exchange=bybit" / "market_type=perpetual" / "symbol=BTCUSDT").rglob("*.parquet"))
        assert len(parquet_files) == 1
        tbl = pq.ParquetFile(parquet_files[0]).read()
        assert len(tbl) == 2


@pytest.mark.anyio
async def test_received_bybit_frame_is_persisted_before_disconnect_propagates(
    monkeypatch, tmp_path: Path
):
    from crypto_quant.ingestion.bybit.liquidations import collect_bybit_liquidations_live

    payload = {
        "topic": "allLiquidation.BTCUSDT",
        "type": "snapshot",
        "ts": 1786434825553,
        "data": [
            {"T": 1786434825501, "s": "BTCUSDT", "S": "Buy", "v": "0.01", "p": "60000"},
            {"T": 1786434825502, "s": "BTCUSDT", "S": "Sell", "v": "0.02", "p": "60001"},
        ],
    }
    wire = json.dumps(payload, separators=(",", ":"))
    queue = [json.dumps({"success": True, "op": "subscribe"}), wire]

    class MockWebSocket:
        async def send(self, value):
            return None

        async def recv(self):
            if queue:
                return queue.pop(0)
            raise ConnectionError("controlled disconnect after received frame")

    class Context:
        async def __aenter__(self):
            return MockWebSocket()

        async def __aexit__(self, exc_type, exc, tb):
            return None

    import websockets

    monkeypatch.setattr(websockets, "connect", lambda *args, **kwargs: Context())
    with pytest.raises(ConnectionError, match="controlled disconnect"):
        await collect_bybit_liquidations_live(
            "BTCUSDT", tmp_path, flush_interval_seconds=60, max_duration_seconds=5
        )
    parquet = list((tmp_path / "normalized").rglob("*.parquet"))
    assert len(parquet) == 1
    assert pq.ParquetFile(parquet[0]).metadata.num_rows == 2


def test_dedup_guarantee_boundary_cross_envelope_not_guaranteed():
    """Proves the dedup_guarantee boundary:
    - exact-wire replay (same raw envelope): GUARANTEED deduplicated
    - same economic event in DIFFERENT envelope (different ts): NOT deduplicated

    Bybit allLiquidation provides NO native per-event ID.
    Cross-envelope economic-event dedup is NOT guaranteed by design.
    Project policy: preserve uncertain events rather than risk dropping distinct events.
    """
    ident = funding_identity("BTCUSDT")
    recv_t = datetime(2026, 8, 11, 10, 0, 0, tzinfo=UTC)

    # Same event content, but delivered in two different WS envelopes with different 'ts'
    # This simulates Bybit re-delivering the event in a new envelope on reconnect or rebroadcast.
    msg_envelope_1 = json.dumps({
        "topic": "allLiquidation.BTCUSDT",
        "type": "snapshot",
        "ts": 1786434825553,  # first envelope timestamp
        "data": [{"T": 1786434825100, "s": "BTCUSDT", "S": "Buy", "v": "1.0", "p": "62000.00"}],
    })
    msg_envelope_2 = json.dumps({
        "topic": "allLiquidation.BTCUSDT",
        "type": "snapshot",
        "ts": 1786434825999,  # different envelope timestamp (same economic event)
        "data": [{"T": 1786434825100, "s": "BTCUSDT", "S": "Buy", "v": "1.0", "p": "62000.00"}],
    })

    recs_1 = parse_bybit_liquidation_message(json.loads(msg_envelope_1), ident, received_at=recv_t, raw_msg_str=msg_envelope_1)
    recs_2 = parse_bybit_liquidation_message(json.loads(msg_envelope_2), ident, received_at=recv_t, raw_msg_str=msg_envelope_2)

    # Each envelope gets a different message_id (different ts)
    assert recs_1[0].message_id != recs_2[0].message_id

    # Therefore dedup_fingerprint differs -> cross-envelope dedup is NOT guaranteed
    assert recs_1[0].dedup_fingerprint != recs_2[0].dedup_fingerprint

    # The dedup_guarantee field explicitly documents this limitation
    assert recs_1[0].dedup_guarantee == "EXACT_WIRE_REPLAY_ONLY"

    # Same envelope re-delivered -> same fingerprint -> IS deduplicated
    recs_1_replay = parse_bybit_liquidation_message(json.loads(msg_envelope_1), ident, received_at=recv_t, raw_msg_str=msg_envelope_1)
    assert recs_1[0].dedup_fingerprint == recs_1_replay[0].dedup_fingerprint, "Exact-wire replay MUST produce same fingerprint"


def test_transport_pass_does_not_imply_data_completeness():
    """Proves that status=PASS + event_observation_status=NO_EVENT_OBSERVED does NOT mean data is complete.

    This is the critical transport vs. observation vs. completeness distinction:
    - transport_status = PASS: WebSocket connection and subscription confirmed.
    - event_observation_status = NO_EVENT_OBSERVED_WITHIN_WINDOW: 0 events in capture window (e.g., quiet market).
    - local capture completeness: UNKNOWN / PARTIAL (there may have been events we missed before connecting).

    These three must not be conflated. A 0-event PASS window does NOT certify historical completeness.
    """
    with tempfile.TemporaryDirectory() as tmp_dir:
        root = Path(tmp_dir)
        # Zero-event batch
        res = persist_bybit_liquidation_batch([], "BTCUSDT", root)

        # Transport / pipeline status
        assert res["status"] == "PASS"

        # Event observation status: honestly NO_EVENT_OBSERVED
        assert res["event_observation_status"] == "NO_EVENT_OBSERVED_WITHIN_WINDOW"
        assert res["records_count"] == 0

        # No storage mutations: no manifest, no parquet written
        assert not (root / "control" / "manifests" / "bybit_linear_liquidations.jsonl").exists()
        norm_dir = root / "normalized" / "liquidations"
        assert not norm_dir.exists() or list(norm_dir.rglob("*.parquet")) == []

        # status=PASS here means pipeline health, NOT completeness
        # local_capture_completeness is implicitly UNKNOWN for this window
