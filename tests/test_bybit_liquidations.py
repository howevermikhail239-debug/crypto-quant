"""Tests for Bybit Linear Liquidations Normalization, Storage and Invariants (Phase 1D.3A)."""

import json
import tempfile
from datetime import UTC, datetime
from pathlib import Path

import pyarrow.parquet as pq
import pytest
import yaml

from crypto_quant.contracts import DataContract
from crypto_quant.ingestion.binance.funding import funding_identity
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
        completeness_class="UNTHROTTLED_EVENT_STREAM",
        message_id="msg1",
        dedup_fingerprint="fp1",
        dedup_collision_risk="LOW",
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
        assert mdata["row_count"] == 2
        assert mdata["completeness_class"] == "UNTHROTTLED_EVENT_STREAM"

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
        assert res["event_observation_status"] == "REAL_EVENT_OBSERVED"
        assert res["total_messages_received"] == 2
        assert res["total_records_persisted"] == 2

        # Verify Parquet on disk
        parquet_files = list((root / "normalized" / "liquidations" / "v1" / "exchange=bybit" / "market_type=perpetual" / "symbol=BTCUSDT").rglob("*.parquet"))
        assert len(parquet_files) == 1
        tbl = pq.ParquetFile(parquet_files[0]).read()
        assert len(tbl) == 2
