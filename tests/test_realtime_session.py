"""Unit and integration tests for Item 7B bounded queues and session lifecycle."""

import asyncio
import json
import tempfile
from pathlib import Path
from typing import Any

import pytest

from crypto_quant.ingestion.realtime_envelope import (
    WsSessionInfo,
    create_raw_ws_envelope,
)
from crypto_quant.ingestion.realtime_session import (
    BoundedWsEnvelopeQueue,
    RealtimeSessionLifecycle,
    RealtimeSessionState,
)
from crypto_quant.time import utc_now


def create_dummy_session_info(symbol: str = "BTCUSDT") -> WsSessionInfo:
    return WsSessionInfo(
        session_id="test_sess_001",
        connection_id="test_conn_001",
        exchange="binance",
        market_type="spot",
        stream_topic=f"{symbol.lower()}@trade",
        connected_at=utc_now(),
    )


def create_dummy_envelope(session_info: WsSessionInfo, idx: int) -> Any:
    payload = {
        "e": "trade",
        "E": 1782864000555 + idx,
        "s": "BTCUSDT",
        "t": 1000 + idx,
        "p": "50000.00",
        "q": "0.1",
        "T": 1782864000554 + idx,
        "m": True,
    }
    return create_raw_ws_envelope(
        exchange="binance",
        market_type="spot",
        instrument_id="ins_382b67a5ff90e4cd6ae4",
        stream_topic=session_info.stream_topic,
        session=session_info,
        payload=payload,
        source_contract_version="binance.spot.ws.trade.v1",
    )


def test_bounded_queue_capacity_and_telemetry():
    async def _test():
        q = BoundedWsEnvelopeQueue(maxsize=2)
        assert q.capacity == 2
        session_info = create_dummy_session_info()

        env1 = create_dummy_envelope(session_info, 1)
        env2 = create_dummy_envelope(session_info, 2)

        await q.put(env1)
        await q.put(env2)
        assert q.qsize == 2
        assert q.get_metrics().high_watermark == 2

        async def delayed_dequeue():
            await asyncio.sleep(0.05)
            item = await q.get()
            q.task_done()
            return item

        task = asyncio.create_task(delayed_dequeue())

        env3 = create_dummy_envelope(session_info, 3)
        await q.put(env3)  # Awaits until delayed_dequeue releases space
        await task

        metrics = q.get_metrics()
        assert metrics.messages_enqueued == 3
        assert metrics.messages_dequeued == 1
        assert metrics.producer_wait_count >= 1
        assert metrics.producer_wait_duration_sec > 0.0

    asyncio.run(_test())


def test_session_state_machine_valid_and_invalid():
    with tempfile.TemporaryDirectory() as tmp_dir:
        session_info = create_dummy_session_info()
        runner = RealtimeSessionLifecycle(
            session_info=session_info,
            symbol="BTCUSDT",
            root=Path(tmp_dir),
        )

        assert runner.meta.state == RealtimeSessionState.CREATED

        runner.transition_to(RealtimeSessionState.CONNECTING)
        assert runner.meta.state == RealtimeSessionState.CONNECTING

        runner.transition_to(RealtimeSessionState.ACTIVE)
        assert runner.meta.state == RealtimeSessionState.ACTIVE

        runner.transition_to(RealtimeSessionState.DRAINING)
        assert runner.meta.state == RealtimeSessionState.DRAINING

        runner.transition_to(RealtimeSessionState.CLOSED)
        assert runner.meta.state == RealtimeSessionState.CLOSED

        # Invalid transition from CLOSED should raise ValueError
        with pytest.raises(ValueError, match="Invalid session state transition"):
            runner.transition_to(RealtimeSessionState.ACTIVE)


def test_graceful_drain_and_conservation():
    async def _test():
        with tempfile.TemporaryDirectory() as tmp_dir:
            session_info = create_dummy_session_info()
            runner = RealtimeSessionLifecycle(
                session_info=session_info,
                symbol="BTCUSDT",
                root=Path(tmp_dir),
                drain_timeout_sec=2.0,
            )

            runner.transition_to(RealtimeSessionState.CONNECTING)
            runner.transition_to(RealtimeSessionState.ACTIVE)
            runner.start_consumer()

            for i in range(5):
                env = create_dummy_envelope(session_info, i)
                await runner.push_envelope(env)

            sealed_file = await runner.close_session(reason="test complete")
            assert sealed_file is not None
            assert sealed_file.exists()
            assert runner.meta.state == RealtimeSessionState.CLOSED

            # Verify conservation: received == enqueued == written == 5
            assert runner.meta.envelopes_received == 5
            assert runner.meta.envelopes_enqueued == 5
            assert runner.meta.envelopes_written == 5
            assert runner.meta.normalized_records == 5

    asyncio.run(_test())


def test_drain_timeout_failure():
    async def _test():
        with tempfile.TemporaryDirectory() as tmp_dir:
            session_info = create_dummy_session_info()
            runner = RealtimeSessionLifecycle(
                session_info=session_info,
                symbol="BTCUSDT",
                root=Path(tmp_dir),
                drain_timeout_sec=0.05,
            )

            runner.transition_to(RealtimeSessionState.CONNECTING)
            runner.transition_to(RealtimeSessionState.ACTIVE)
            runner.start_consumer()

            # Replace consumer task with a never-ending task to trigger drain timeout
            async def never_finish():
                await asyncio.sleep(10.0)

            if runner._consumer_task:
                runner._consumer_task.cancel()
            runner._consumer_task = asyncio.create_task(never_finish())

            env = create_dummy_envelope(session_info, 1)
            await runner.push_envelope(env)

            _ = await runner.close_session(reason="test timeout")
            assert runner.meta.state == RealtimeSessionState.FAILED
            assert "Drain timeout" in (runner.meta.failure_reason or "")

    asyncio.run(_test())


def test_writer_failure_transitions_to_failed():
    async def _test():
        with tempfile.TemporaryDirectory() as tmp_dir:
            session_info = create_dummy_session_info()
            runner = RealtimeSessionLifecycle(
                session_info=session_info,
                symbol="BTCUSDT",
                root=Path(tmp_dir),
            )

            runner.transition_to(RealtimeSessionState.CONNECTING)
            runner.transition_to(RealtimeSessionState.ACTIVE)

            def bad_write(env):
                raise OSError("Disk write failed: No space left")

            runner.writer.write_envelope = bad_write

            runner.start_consumer()
            env = create_dummy_envelope(session_info, 1)
            await runner.push_envelope(env)

            await asyncio.sleep(0.1)
            assert runner.meta.state == RealtimeSessionState.FAILED
            assert "Disk write failed" in (runner.meta.failure_reason or "")

    asyncio.run(_test())


def test_order_preservation():
    async def _test():
        with tempfile.TemporaryDirectory() as tmp_dir:
            session_info = create_dummy_session_info()
            runner = RealtimeSessionLifecycle(
                session_info=session_info,
                symbol="BTCUSDT",
                root=Path(tmp_dir),
            )
            runner.transition_to(RealtimeSessionState.CONNECTING)
            runner.transition_to(RealtimeSessionState.ACTIVE)
            runner.start_consumer()

            pushed_ids = []
            for i in range(10):
                env = create_dummy_envelope(session_info, i)
                pushed_ids.append(env.envelope_id)
                await runner.push_envelope(env)

            sealed_file = await runner.close_session()
            assert sealed_file is not None

            lines = sealed_file.read_text(encoding="utf-8").splitlines()
            read_ids = [json.loads(line)["envelope_id"] for line in lines]
            assert read_ids == pushed_ids

    asyncio.run(_test())
