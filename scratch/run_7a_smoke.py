"""Realtime Smoke Test for 7A Raw Envelope Capture and Normalization.

Connects to Binance Spot public trade stream and Bybit Spot public trade stream for 5 seconds each,
captures raw envelopes, seals segment files, normalizes to canonical individual trades, and reports metrics.
"""

from __future__ import annotations

import asyncio
import json
import time
from pathlib import Path

import websockets

from crypto_quant.ingestion.realtime_envelope import (
    RawWsSegmentWriter,
    WsSessionInfo,
    create_raw_ws_envelope,
    normalize_ws_envelope_to_individual_trades,
)
from crypto_quant.time import utc_now


async def run_binance_spot_smoke(root: Path, duration_sec: float = 5.0) -> dict:
    url = "wss://stream.binance.com:9443/ws/btcusdt@trade"
    session = WsSessionInfo(
        session_id="smoke_binance_spot_001",
        connection_id="conn_b1",
        exchange="binance",
        market_type="spot",
        stream_topic="btcusdt@trade",
        connected_at=utc_now(),
    )
    writer = RawWsSegmentWriter(root, "binance", "spot", "BTCUSDT")
    start_t = time.monotonic()
    envelope_count = 0
    trade_count = 0
    duplicates = 0
    unknown_sides = 0

    async with websockets.connect(url) as ws:
        while time.monotonic() - start_t < duration_sec:
            try:
                msg = await asyncio.wait_for(ws.recv(), timeout=1.0)
                payload = json.loads(msg)
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
                envelope_count += 1

                rb = normalize_ws_envelope_to_individual_trades(env)
                trade_count += rb.num_rows
                sides = rb.column("taker_side").to_pylist()
                unknown_sides += sum(1 for s in sides if s == "UNKNOWN")
            except TimeoutError:
                continue

    sealed_path = writer.seal()
    elapsed = time.monotonic() - start_t

    return {
        "exchange": "binance",
        "market_type": "spot",
        "symbol": "BTCUSDT",
        "duration_sec": elapsed,
        "envelope_count": envelope_count,
        "trade_count": trade_count,
        "bytes_written": writer.bytes_written,
        "sealed_file": str(sealed_path),
        "unknown_sides": unknown_sides,
        "duplicates": duplicates,
    }


async def run_bybit_spot_smoke(root: Path, duration_sec: float = 5.0) -> dict:
    url = "wss://stream.bybit.com/v5/public/spot"
    session = WsSessionInfo(
        session_id="smoke_bybit_spot_001",
        connection_id="conn_by1",
        exchange="bybit",
        market_type="spot",
        stream_topic="publicTrade.BTCUSDT",
        connected_at=utc_now(),
    )
    writer = RawWsSegmentWriter(root, "bybit", "spot", "BTCUSDT")
    start_t = time.monotonic()
    envelope_count = 0
    trade_count = 0
    duplicates = 0
    unknown_sides = 0

    async with websockets.connect(url) as ws:
        sub_msg = {"op": "subscribe", "args": ["publicTrade.BTCUSDT"]}
        await ws.send(json.dumps(sub_msg))

        while time.monotonic() - start_t < duration_sec:
            try:
                msg = await asyncio.wait_for(ws.recv(), timeout=1.0)
                payload = json.loads(msg)
                if payload.get("topic") == "publicTrade.BTCUSDT":
                    env = create_raw_ws_envelope(
                        exchange="bybit",
                        market_type="spot",
                        instrument_id="ins_382b67a5ff90e4cd6ae4",
                        stream_topic="publicTrade.BTCUSDT",
                        session=session,
                        payload=payload,
                        source_contract_version="bybit.spot.ws.individual-trade.v1",
                    )
                    writer.write_envelope(env)
                    envelope_count += 1

                    rb = normalize_ws_envelope_to_individual_trades(env)
                    trade_count += rb.num_rows
                    sides = rb.column("taker_side").to_pylist()
                    unknown_sides += sum(1 for s in sides if s == "UNKNOWN")
            except TimeoutError:
                continue

    sealed_path = writer.seal()
    elapsed = time.monotonic() - start_t

    return {
        "exchange": "bybit",
        "market_type": "spot",
        "symbol": "BTCUSDT",
        "duration_sec": elapsed,
        "envelope_count": envelope_count,
        "trade_count": trade_count,
        "bytes_written": writer.bytes_written,
        "sealed_file": str(sealed_path),
        "unknown_sides": unknown_sides,
        "duplicates": duplicates,
    }


async def main():
    root = Path("C:/crypto_quant_data")
    print("--- Starting 7A Realtime Smoke Test ---")
    b_res = await run_binance_spot_smoke(root, duration_sec=5.0)
    print("Binance Smoke Result:", json.dumps(b_res, indent=2))

    by_res = await run_bybit_spot_smoke(root, duration_sec=5.0)
    print("Bybit Smoke Result:", json.dumps(by_res, indent=2))

    from crypto_quant.ingestion.binance.spot_trades import peak_rss_bytes
    rss = peak_rss_bytes()
    peak_rss_mb = (rss / (1024 * 1024)) if rss else 0.0
    print(f"Peak RAM: {peak_rss_mb:.2f} MB")
    print("--- 7A Realtime Smoke Test Complete ---")


if __name__ == "__main__":
    asyncio.run(main())
