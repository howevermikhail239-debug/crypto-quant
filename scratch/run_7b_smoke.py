"""7B Realtime Smoke Test for Bounded Queues and Session Lifecycle.

Runs controlled live smoke sessions for:
1. Binance Spot BTCUSDT trade stream
2. Bybit Spot BTCUSDT publicTrade stream
3. Bybit Linear BTCUSDT publicTrade stream

Measures: duration, envelopes received/enqueued/written, normalized records, queue maxsize, queue high-watermark, producer waits, writer lag, peak RAM, clean drain time, unexpected partials.
"""

from __future__ import annotations

import asyncio
import json
import time
from pathlib import Path

import websockets

from crypto_quant.ingestion.binance.spot_trades import peak_rss_bytes
from crypto_quant.ingestion.realtime_envelope import WsSessionInfo, create_raw_ws_envelope
from crypto_quant.ingestion.realtime_session import RealtimeSessionLifecycle, RealtimeSessionState
from crypto_quant.time import utc_now


async def run_7b_session_smoke(
    root: Path,
    exchange: str,
    market_type: str,
    symbol: str,
    url: str,
    topic: str,
    contract_version: str,
    sub_payload: dict | None = None,
    duration_sec: float = 5.0,
    queue_maxsize: int = 100,
) -> dict:
    session_info = WsSessionInfo(
        session_id=f"smoke_7b_{exchange}_{market_type}_001",
        connection_id=f"conn_7b_{exchange[:2]}",
        exchange=exchange,
        market_type=market_type,
        stream_topic=topic,
        connected_at=utc_now(),
    )
    runner = RealtimeSessionLifecycle(
        session_info=session_info,
        symbol=symbol,
        root=root,
        queue_maxsize=queue_maxsize,
        drain_timeout_sec=5.0,
    )
    runner.transition_to(RealtimeSessionState.CONNECTING)

    start_t = time.monotonic()
    async with websockets.connect(url, open_timeout=10.0) as ws:
        runner.transition_to(RealtimeSessionState.ACTIVE)
        runner.start_consumer()

        if sub_payload:
            await ws.send(json.dumps(sub_payload))

        while time.monotonic() - start_t < duration_sec:
            try:
                msg = await asyncio.wait_for(ws.recv(), timeout=1.0)
                raw_payload = json.loads(msg)
                payload = raw_payload.get("data", raw_payload) if exchange == "binance" else raw_payload

                # Filter out subscription responses if necessary
                if exchange == "bybit" and payload.get("topic") != topic:
                    continue

                env = create_raw_ws_envelope(
                    exchange=exchange,
                    market_type=market_type,
                    instrument_id="ins_382b67a5ff90e4cd6ae4" if market_type == "spot" else "ins_98b8c5f600bdff95b23d",
                    stream_topic=topic,
                    session=session_info,
                    payload=payload,
                    source_contract_version=contract_version,
                )
                await runner.push_envelope(env)
            except TimeoutError:
                continue

    drain_start = time.monotonic()
    sealed_file = await runner.close_session(reason="smoke test completed")
    drain_time = time.monotonic() - drain_start

    metrics = runner.queue.get_metrics()
    return {
        "exchange": exchange,
        "market_type": market_type,
        "symbol": symbol,
        "session_state": runner.meta.state.value,
        "duration_sec": time.monotonic() - start_t,
        "drain_time_sec": drain_time,
        "envelopes_received": runner.meta.envelopes_received,
        "envelopes_enqueued": runner.meta.envelopes_enqueued,
        "envelopes_written": runner.meta.envelopes_written,
        "normalized_records": runner.meta.normalized_records,
        "queue_capacity": metrics.capacity,
        "queue_high_watermark": metrics.high_watermark,
        "producer_wait_count": metrics.producer_wait_count,
        "producer_wait_duration_sec": metrics.producer_wait_duration_sec,
        "writer_lag_sec": metrics.writer_lag_sec,
        "sealed_file": str(sealed_file),
    }


async def main():
    root = Path("C:/crypto_quant_data")
    print("=== Starting 7B Realtime Smoke Test ===")

    res1 = await run_7b_session_smoke(
        root,
        exchange="binance",
        market_type="spot",
        symbol="BTCUSDT",
        url="wss://stream.binance.com:9443/ws/btcusdt@trade",
        topic="btcusdt@trade",
        contract_version="binance.spot.ws.trade.v1",
        duration_sec=5.0,
    )
    print("1. Binance Spot BTCUSDT 7B Session:", json.dumps(res1, indent=2))

    res2 = await run_7b_session_smoke(
        root,
        exchange="bybit",
        market_type="spot",
        symbol="BTCUSDT",
        url="wss://stream.bybit.com/v5/public/spot",
        topic="publicTrade.BTCUSDT",
        contract_version="bybit.spot.ws.individual-trade.v1",
        sub_payload={"op": "subscribe", "args": ["publicTrade.BTCUSDT"]},
        duration_sec=5.0,
    )
    print("2. Bybit Spot BTCUSDT 7B Session:", json.dumps(res2, indent=2))

    res3 = await run_7b_session_smoke(
        root,
        exchange="bybit",
        market_type="perpetual",
        symbol="BTCUSDT",
        url="wss://stream.bybit.com/v5/public/linear",
        topic="publicTrade.BTCUSDT",
        contract_version="bybit.linear.ws.individual-trade.v1",
        sub_payload={"op": "subscribe", "args": ["publicTrade.BTCUSDT"]},
        duration_sec=5.0,
    )
    print("3. Bybit Linear BTCUSDT 7B Session:", json.dumps(res3, indent=2))

    rss = peak_rss_bytes()
    peak_rss_mb = (rss / (1024 * 1024)) if rss else 0.0
    print(f"Peak RAM: {peak_rss_mb:.2f} MB")
    print("=== 7B Realtime Smoke Test Complete ===")


if __name__ == "__main__":
    asyncio.run(main())
