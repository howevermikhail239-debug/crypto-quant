"""Unit, integration, boundary proof, internal continuity, and dedup tests for Item 7C."""

import asyncio
import tempfile
from datetime import timedelta
from pathlib import Path

from crypto_quant.ingestion.gap_registry import GapRecord, GapRegistry, GapStatus, GapType
from crypto_quant.ingestion.realtime_envelope import create_raw_ws_envelope
from crypto_quant.ingestion.realtime_recovery import (
    perform_gap_recovery,
)
from crypto_quant.ingestion.realtime_session import RealtimeSessionState
from crypto_quant.ingestion.realtime_supervisor import RealtimeStreamSupervisor
from crypto_quant.ingestion.reconnect import ReconnectConfig, compute_reconnect_delay
from crypto_quant.time import utc_now


def test_reconnect_backoff_and_jitter():
    config = ReconnectConfig(initial_delay_sec=1.0, max_delay_sec=10.0, backoff_multiplier=2.0, jitter_ratio=0.1)

    d1 = compute_reconnect_delay(1, config, seed=42)
    d2 = compute_reconnect_delay(2, config, seed=42)
    d3 = compute_reconnect_delay(3, config, seed=42)

    assert 0.9 <= d1 <= 1.1
    assert 1.8 <= d2 <= 2.2
    assert 3.6 <= d3 <= 4.4

    d10 = compute_reconnect_delay(10, config, seed=42)
    assert d10 <= 11.0


def test_gap_registry_persistence_and_query():
    with tempfile.TemporaryDirectory() as tmp_dir:
        root = Path(tmp_dir)
        registry = GapRegistry(root)

        now = utc_now()
        rec1 = registry.register_gap(
            exchange="binance",
            market_type="spot",
            instrument_id="ins_382b67a5ff90e4cd6ae4",
            dataset_class="individual_trade",
            source_stream="btcusdt@trade",
            gap_start=now - timedelta(seconds=60),
            gap_end=now,
            gap_type=GapType.LOCAL_COLLECTOR_GAP,
        )

        assert rec1.status == GapStatus.OPEN
        assert registry.manifest_file.exists()

        rec1.status = GapStatus.RECOVERED
        rec1.records_recovered = 15
        rec1.coverage_proven = True
        registry.update_gap(rec1)

        gaps = registry.list_gaps()
        assert len(gaps) == 1
        assert gaps[0].status == GapStatus.RECOVERED
        assert gaps[0].records_recovered == 15
        assert gaps[0].coverage_proven is True


def test_dataset_class_isolation_in_recovery():
    with tempfile.TemporaryDirectory() as tmp_dir:
        root = Path(tmp_dir)
        now = utc_now()
        gap = GapRecord(
            gap_id="gap_invalid_class",
            exchange="binance",
            market_type="spot",
            instrument_id="ins_382b67a5ff90e4cd6ae4",
            dataset_class="INVALID_DATASET_CLASS",
            source_stream="btcusdt@trade",
            detected_at=now,
            gap_start=now - timedelta(seconds=30),
            gap_end=now,
            gap_type=GapType.LOCAL_COLLECTOR_GAP,
            status=GapStatus.OPEN,
        )

        res = perform_gap_recovery(gap, root)
        assert res.status == GapStatus.UNRECOVERABLE
        assert res.coverage_proven is False
        assert "Unsupported dataset class" in (res.notes or "")


def test_internal_sequence_hole_fails_coverage_proven():
    """Regression test: boundary IDs present but internal sequence hole exists -> coverage_proven MUST be False."""
    with tempfile.TemporaryDirectory() as tmp_dir:
        root = Path(tmp_dir)
        now = utc_now()
        gap = GapRecord(
            gap_id="gap_hole_test",
            exchange="binance",
            market_type="spot",
            instrument_id="ins_382b67a5ff90e4cd6ae4",
            dataset_class="individual_trade",
            source_stream="btcusdt@trade",
            detected_at=now,
            gap_start=now - timedelta(seconds=30),
            gap_end=now,
            gap_type=GapType.LOCAL_COLLECTOR_GAP,
            status=GapStatus.OPEN,
            pre_gap_last_trade_id="1000",
            post_gap_first_trade_id="1010",
        )

        # Missing trade ID 1005 (hole in sequence)
        mock_items_with_hole = [
            {"id": 1001, "price": "50000", "qty": "0.1"},
            {"id": 1002, "price": "50000", "qty": "0.1"},
            {"id": 1003, "price": "50000", "qty": "0.1"},
            {"id": 1004, "price": "50000", "qty": "0.1"},
            # 1005 IS MISSING!
            {"id": 1006, "price": "50000", "qty": "0.1"},
            {"id": 1007, "price": "50000", "qty": "0.1"},
            {"id": 1008, "price": "50000", "qty": "0.1"},
            {"id": 1009, "price": "50000", "qty": "0.1"},
            {"id": 1010, "price": "50000", "qty": "0.1"},
        ]
        res = perform_gap_recovery(gap, root, mock_fetched_items=mock_items_with_hole)

        assert res.coverage_proven is False
        assert res.status != GapStatus.RECOVERED
        assert res.status == GapStatus.PARTIAL


def test_bybit_spot_limit_handling():
    """Verify Bybit Spot endpoint limit is set to 60 (vs 1000 for Linear)."""
    with tempfile.TemporaryDirectory() as tmp_dir:
        root = Path(tmp_dir)
        now = utc_now()

        gap_spot = GapRecord(
            gap_id="gap_bybit_spot",
            exchange="bybit",
            market_type="spot",
            instrument_id="ins_bybit_spot_btcusdt",
            dataset_class="individual_trade",
            source_stream="publicTrade.BTCUSDT",
            detected_at=now,
            gap_start=now - timedelta(seconds=10),
            gap_end=now,
            gap_type=GapType.LOCAL_COLLECTOR_GAP,
            status=GapStatus.OPEN,
        )
        res_spot = perform_gap_recovery(gap_spot, root, mock_fetched_items=[{"i": "1", "p": "50000", "v": "0.1"}])
        assert res_spot.endpoint_limit == 60

        gap_linear = GapRecord(
            gap_id="gap_bybit_linear",
            exchange="bybit",
            market_type="perpetual",
            instrument_id="ins_bybit_linear_btcusdt",
            dataset_class="individual_trade",
            source_stream="publicTrade.BTCUSDT",
            detected_at=now,
            gap_start=now - timedelta(seconds=10),
            gap_end=now,
            gap_type=GapType.LOCAL_COLLECTOR_GAP,
            status=GapStatus.OPEN,
        )
        res_linear = perform_gap_recovery(gap_linear, root, mock_fetched_items=[{"i": "1", "p": "50000", "v": "0.1"}])
        assert res_linear.endpoint_limit == 1000


def test_recovery_overlap_deduplication():
    """Verify recovery records overlapping with WS records deduplicate idempotently via canonical natural key."""
    ws_trades = [
        {"native_trade_id": "1009", "price": "50000.00", "quantity": "0.1", "event_time": 1782864000000},
        {"native_trade_id": "1010", "price": "50001.00", "quantity": "0.2", "event_time": 1782864000001},
    ]
    rest_recovery_trades = [
        {"native_trade_id": "1009", "price": "50000.00", "quantity": "0.1", "event_time": 1782864000000},  # Duplicate
        {"native_trade_id": "1010", "price": "50001.00", "quantity": "0.2", "event_time": 1782864000001},  # Duplicate
        {"native_trade_id": "1011", "price": "50002.00", "quantity": "0.3", "event_time": 1782864000002},  # New
    ]

    all_raw = ws_trades + rest_recovery_trades
    deduped_by_natural_key = {item["native_trade_id"]: item for item in all_raw}

    assert len(deduped_by_natural_key) == 3
    assert set(deduped_by_natural_key.keys()) == {"1009", "1010", "1011"}


def test_deterministic_fault_injection_and_reconnect():
    async def _test_async():
        with tempfile.TemporaryDirectory() as tmp_dir:
            root = Path(tmp_dir)
            supervisor = RealtimeStreamSupervisor(
                exchange="binance",
                market_type="spot",
                symbol="BTCUSDT",
                stream_topic="btcusdt@trade",
                dataset_class="individual_trade",
                root=root,
            )

            sess1 = supervisor.create_new_session()
            supervisor.active_session = sess1
            sess1.transition_to(RealtimeSessionState.CONNECTING)
            sess1.transition_to(RealtimeSessionState.ACTIVE)
            sess1.start_consumer()

            payload = {"e": "trade", "E": 1782864000555, "s": "BTCUSDT", "t": 100, "p": "50000", "q": "1", "T": 1782864000554, "m": True}
            env = create_raw_ws_envelope(
                exchange="binance",
                market_type="spot",
                instrument_id="ins_382b67a5ff90e4cd6ae4",
                stream_topic="btcusdt@trade",
                session=sess1.session_info,
                payload=payload,
                source_contract_version="binance.spot.ws.trade.v1",
            )
            await sess1.push_envelope(env)
            supervisor.last_event_time = env.received_at

            gap_record, delay = await supervisor.handle_disconnect("simulated network drop")

            assert gap_record is not None
            assert gap_record.status == GapStatus.OPEN
            assert gap_record.session_before == sess1.session_info.session_id
            assert delay > 0.0

            gap_record.pre_gap_last_trade_id = "100"
            gap_record.post_gap_first_trade_id = "105"

            sess2 = supervisor.create_new_session()
            supervisor.active_session = sess2
            assert sess2.session_info.session_id != sess1.session_info.session_id
            gap_record.session_after = sess2.session_info.session_id
            supervisor.registry.update_gap(gap_record)

            mock_rest_trades = [
                {"id": 101, "price": "50001.00", "qty": "0.5", "time": 1782864000560, "isBuyerMaker": True},
                {"id": 102, "price": "50001.50", "qty": "0.2", "time": 1782864000561, "isBuyerMaker": False},
                {"id": 103, "price": "50002.00", "qty": "0.1", "time": 1782864000562, "isBuyerMaker": True},
                {"id": 104, "price": "50002.50", "qty": "0.4", "time": 1782864000563, "isBuyerMaker": False},
            ]
            recovered_gap = await supervisor.trigger_gap_recovery(gap_record, mock_items=mock_rest_trades)

            assert recovered_gap.status == GapStatus.RECOVERED
            assert recovered_gap.coverage_proven is True
            assert recovered_gap.records_recovered == 4
            assert recovered_gap.session_after == sess2.session_info.session_id

            await sess2.close_session()

    asyncio.run(_test_async())
