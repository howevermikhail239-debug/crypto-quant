"""Unit, integration, and fault-injection tests for Item 7C Reconnect, Gap Recovery, and Boundary Proof."""

import asyncio
import tempfile
from datetime import timedelta
from pathlib import Path

from crypto_quant.ingestion.gap_registry import GapRecord, GapRegistry, GapStatus, GapType
from crypto_quant.ingestion.realtime_envelope import (
    create_raw_ws_envelope,
)
from crypto_quant.ingestion.realtime_recovery import (
    audit_revision_previous_smoke_gap,
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


def test_single_page_max_limit_truncation_risk():
    """Regression test: response returning max endpoint limit without reaching boundary MUST NOT become RECOVERED."""
    with tempfile.TemporaryDirectory() as tmp_dir:
        root = Path(tmp_dir)
        now = utc_now()
        gap = GapRecord(
            gap_id="gap_truncation_test",
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
            post_gap_first_trade_id="5000",  # Unreached target
        )

        # Mock 1000 returned trades (1001..2000) which fails to reach target 5000
        mock_items = [{"id": 1000 + i, "price": "50000", "qty": "0.1"} for i in range(1, 1001)]
        res = perform_gap_recovery(gap, root, mock_fetched_items=mock_items)

        assert res.status == GapStatus.PARTIAL
        assert res.coverage_proven is False
        assert "TRUNCATION_RISK" in (res.notes or "")


def test_multi_page_boundary_proven_recovery():
    """Test multi-page gap recovery proving boundary coverage across trade IDs."""
    with tempfile.TemporaryDirectory() as tmp_dir:
        root = Path(tmp_dir)
        now = utc_now()
        gap = GapRecord(
            gap_id="gap_multipage_test",
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
            post_gap_first_trade_id="1500",  # Reached target
        )

        mock_items = [{"id": 1000 + i, "price": "50000", "qty": "0.1"} for i in range(1, 500)]
        res = perform_gap_recovery(gap, root, mock_fetched_items=mock_items)

        assert res.status == GapStatus.RECOVERED
        assert res.coverage_proven is True
        assert res.coverage_method == "trade_id_sequence_complete"


def test_audit_revision_previous_smoke_gap():
    """Test appending an audit revision for gap_11588b0a09dc43ff updating status to PARTIAL."""
    with tempfile.TemporaryDirectory() as tmp_dir:
        root = Path(tmp_dir)
        registry = GapRegistry(root)

        now = utc_now()
        orig = registry.register_gap(
            exchange="binance",
            market_type="spot",
            instrument_id="ins_382b67a5ff90e4cd6ae4",
            dataset_class="individual_trade",
            source_stream="btcusdt@trade",
            gap_start=now - timedelta(seconds=10),
            gap_end=now,
            gap_type=GapType.LOCAL_COLLECTOR_GAP,
        )
        orig.gap_id = "gap_11588b0a09dc43ff"
        orig.status = GapStatus.RECOVERED
        registry.update_gap(orig)

        rev = audit_revision_previous_smoke_gap(root, target_gap_id="gap_11588b0a09dc43ff")
        assert rev is not None
        assert rev.status == GapStatus.PARTIAL
        assert rev.coverage_proven is False
        assert "Audit Revision" in (rev.notes or "")

        target_res = next(g for g in registry.list_gaps() if g.gap_id == "gap_11588b0a09dc43ff")
        assert target_res.status == GapStatus.PARTIAL


def test_deterministic_fault_injection_and_reconnect():
    """Deterministic Fault Injection Test (7C.32):
    1. Start session
    2. Enqueue envelopes
    3. Force disconnect
    4. Verify candidate gap registration & session lineage
    5. Reconnect (create new session)
    6. Execute REST recovery & verify GapStatus.RECOVERED with boundary proof
    """
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
