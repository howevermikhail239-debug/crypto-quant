"""7C Live Reconnect and Gap Recovery Smoke Test with Boundary Proof Audit.

Executes controlled live reconnect & recovery flow:
1. Start live WebSocket stream for Binance Spot BTCUSDT
2. Receive envelopes for 3 seconds
3. Simulate client disconnect
4. Audit candidate gap record created in GapRegistry with pre_gap_last_trade_id
5. Calculate backoff delay with jitter
6. Start new session (verifying distinct session_id lineage)
7. Set post_gap_first_trade_id on candidate gap
8. Perform REST gap recovery and validate boundary proof
9. Resume live stream on new session
10. Print complete 7C metrics report with boundary proof details
"""

from __future__ import annotations

import asyncio
import json
import time
from pathlib import Path

import websockets

from crypto_quant.ingestion.binance.spot_trades import peak_rss_bytes
from crypto_quant.ingestion.realtime_envelope import create_raw_ws_envelope
from crypto_quant.ingestion.realtime_session import RealtimeSessionState
from crypto_quant.ingestion.realtime_supervisor import RealtimeStreamSupervisor


async def run_7c_live_reconnect_smoke():
    root = Path("C:/crypto_quant_data")
    print("=== Starting 7C Live Reconnect & Gap Recovery Smoke Test ===")

    supervisor = RealtimeStreamSupervisor(
        exchange="binance",
        market_type="spot",
        symbol="BTCUSDT",
        stream_topic="btcusdt@trade",
        dataset_class="individual_trade",
        root=root,
    )

    # 1. Connect Session 1
    sess1 = supervisor.create_new_session()
    supervisor.active_session = sess1
    sess1.transition_to(RealtimeSessionState.CONNECTING)

    url = "wss://stream.binance.com:9443/ws/btcusdt@trade"
    ws1 = await websockets.connect(url, open_timeout=10.0)
    sess1.transition_to(RealtimeSessionState.ACTIVE)
    sess1.start_consumer()

    last_trade_id_sess1 = None
    start_t = time.monotonic()
    while time.monotonic() - start_t < 3.0:
        try:
            msg = await asyncio.wait_for(ws1.recv(), timeout=1.0)
            payload = json.loads(msg)
            if payload.get("t"):
                last_trade_id_sess1 = str(payload["t"])
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
        except TimeoutError:
            continue

    # 2. Simulate Disconnect
    await ws1.close()
    gap_record, delay_sec = await supervisor.handle_disconnect("controlled client disconnect test")
    gap_record.pre_gap_last_trade_id = last_trade_id_sess1
    supervisor.registry.update_gap(gap_record)

    print(f"Disconnect Handled. Gap ID: {gap_record.gap_id}, Pre-Gap Last Trade ID: {last_trade_id_sess1}")
    print(f"Calculated Reconnect Delay (Backoff + Jitter): {delay_sec:.4f}s")

    # 3. Reconnect Session 2
    sess2 = supervisor.create_new_session()
    supervisor.active_session = sess2
    sess2.transition_to(RealtimeSessionState.CONNECTING)

    ws2 = await websockets.connect(url, open_timeout=10.0)
    sess2.transition_to(RealtimeSessionState.ACTIVE)
    sess2.start_consumer()

    first_trade_id_sess2 = None
    start_t2 = time.monotonic()
    while time.monotonic() - start_t2 < 3.0:
        try:
            msg = await asyncio.wait_for(ws2.recv(), timeout=1.0)
            payload = json.loads(msg)
            if first_trade_id_sess2 is None and payload.get("t"):
                first_trade_id_sess2 = str(payload["t"])
            env = create_raw_ws_envelope(
                exchange="binance",
                market_type="spot",
                instrument_id="ins_382b67a5ff90e4cd6ae4",
                stream_topic="btcusdt@trade",
                session=sess2.session_info,
                payload=payload,
                source_contract_version="binance.spot.ws.trade.v1",
            )
            await sess2.push_envelope(env)
            supervisor.last_event_time = env.received_at
        except TimeoutError:
            continue

    gap_record.post_gap_first_trade_id = first_trade_id_sess2
    gap_record.session_after = sess2.session_info.session_id
    supervisor.registry.update_gap(gap_record)

    # 4. Perform REST Recovery on Candidate Gap with Boundary Proof
    print("Triggering REST recovery with boundary proof validation...")
    recovered_gap = await supervisor.trigger_gap_recovery(gap_record)
    print(f"Recovery Completed. Status: {recovered_gap.status.value}, Coverage Proven: {recovered_gap.coverage_proven}, Records: {recovered_gap.records_recovered}")

    await ws2.close()
    await sess2.close_session(reason="smoke test completed")

    rss = peak_rss_bytes()
    peak_rss_mb = (rss / (1024 * 1024)) if rss else 0.0

    report = {
        "initial_session_id": sess1.session_info.session_id,
        "reconnected_session_id": sess2.session_info.session_id,
        "session_lineage_distinct": sess1.session_info.session_id != sess2.session_info.session_id,
        "candidate_gap_id": recovered_gap.gap_id,
        "pre_gap_last_trade_id": last_trade_id_sess1,
        "post_gap_first_trade_id": first_trade_id_sess2,
        "recovery_first_trade_id": recovered_gap.recovery_first_trade_id,
        "recovery_last_trade_id": recovered_gap.recovery_last_trade_id,
        "pages_requested": recovered_gap.pages_requested,
        "coverage_proven": recovered_gap.coverage_proven,
        "coverage_method": recovered_gap.coverage_method,
        "records_recovered": recovered_gap.records_recovered,
        "gap_final_status": recovered_gap.status.value,
        "sess1_envelopes_written": sess1.meta.envelopes_written,
        "sess2_envelopes_written": sess2.meta.envelopes_written,
        "peak_ram_mb": round(peak_rss_mb, 2),
    }

    print("=== 7C Live Reconnect & Gap Recovery Smoke Metrics ===")
    print(json.dumps(report, indent=2))
    print("=== 7C Live Smoke Test Complete ===")


if __name__ == "__main__":
    asyncio.run(run_7c_live_reconnect_smoke())
