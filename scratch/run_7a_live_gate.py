"""7A Live-Validation Completion Gate.

Executes 5-second controlled live smoke tests for:
1. Binance USD-M perpetual BTCUSDT `aggTrade` (dataset_class="exchange_aggregate_trade")
2. Bybit Linear perpetual BTCUSDT `publicTrade` (dataset_class="individual_trade")
3. Binance Spot BTCUSDT `aggTrade` (dataset_class="exchange_aggregate_trade")
"""

from __future__ import annotations

import asyncio
import json
import time
from pathlib import Path

import websockets

from crypto_quant.ingestion.binance.spot_trades import peak_rss_bytes
from crypto_quant.ingestion.realtime_envelope import (
    RawWsSegmentWriter,
    WsSessionInfo,
    create_raw_ws_envelope,
    normalize_ws_envelope_to_aggregate_trades,
    normalize_ws_envelope_to_individual_trades,
)
from crypto_quant.time import utc_now


async def run_binance_usdm_aggtrade_smoke(root: Path, duration_sec: float = 5.0) -> dict:
    url = "wss://fstream.binance.com/stream?streams=btcusdt@aggTrade"
    session = WsSessionInfo(
        session_id="smoke_binance_usdm_agg_001",
        connection_id="conn_usdm1",
        exchange="binance",
        market_type="perpetual",
        stream_topic="btcusdt@aggTrade",
        connected_at=utc_now(),
    )
    writer = RawWsSegmentWriter(root, "binance", "perpetual", "BTCUSDT")
    start_t = time.monotonic()
    envelope_count = 0
    record_count = 0

    async with websockets.connect(url, open_timeout=10.0) as ws:
        while time.monotonic() - start_t < duration_sec:
            try:
                msg = await asyncio.wait_for(ws.recv(), timeout=1.0)
                raw_payload = json.loads(msg)
                payload = raw_payload.get("data", raw_payload)
                env = create_raw_ws_envelope(
                    exchange="binance",
                    market_type="perpetual",
                    instrument_id="ins_98b8c5f600bdff95b23d",
                    stream_topic="btcusdt@aggTrade",
                    session=session,
                    payload=payload,
                    source_contract_version="binance.usdm.ws.aggTrade.v1",
                )
                writer.write_envelope(env)
                envelope_count += 1

                rb = normalize_ws_envelope_to_aggregate_trades(env)
                assert rb.column("dataset_class")[0].as_py() == "exchange_aggregate_trade"
                record_count += rb.num_rows
            except TimeoutError:
                continue

    sealed_path = writer.seal()
    elapsed = time.monotonic() - start_t
    return {
        "exchange": "binance",
        "market_type": "perpetual",
        "topic": "btcusdt@aggTrade",
        "dataset_class": "exchange_aggregate_trade",
        "duration_sec": elapsed,
        "envelope_count": envelope_count,
        "record_count": record_count,
        "bytes_written": writer.bytes_written,
        "sealed_file": str(sealed_path),
    }


async def run_bybit_linear_smoke(root: Path, duration_sec: float = 5.0) -> dict:
    url = "wss://stream.bybit.com/v5/public/linear"
    session = WsSessionInfo(
        session_id="smoke_bybit_linear_001",
        connection_id="conn_bylin1",
        exchange="bybit",
        market_type="perpetual",
        stream_topic="publicTrade.BTCUSDT",
        connected_at=utc_now(),
    )
    writer = RawWsSegmentWriter(root, "bybit", "perpetual", "BTCUSDT")
    start_t = time.monotonic()
    envelope_count = 0
    record_count = 0
    unknown_sides = 0

    async with websockets.connect(url, open_timeout=15.0) as ws:
        sub_msg = {"op": "subscribe", "args": ["publicTrade.BTCUSDT"]}
        await ws.send(json.dumps(sub_msg))

        while time.monotonic() - start_t < duration_sec:
            try:
                msg = await asyncio.wait_for(ws.recv(), timeout=1.0)
                payload = json.loads(msg)
                if payload.get("topic") == "publicTrade.BTCUSDT":
                    env = create_raw_ws_envelope(
                        exchange="bybit",
                        market_type="perpetual",
                        instrument_id="ins_98b8c5f600bdff95b23d",
                        stream_topic="publicTrade.BTCUSDT",
                        session=session,
                        payload=payload,
                        source_contract_version="bybit.linear.ws.individual-trade.v1",
                    )
                    writer.write_envelope(env)
                    envelope_count += 1

                    rb = normalize_ws_envelope_to_individual_trades(env)
                    assert rb.column("dataset_class")[0].as_py() == "individual_trade"
                    record_count += rb.num_rows
                    sides = rb.column("taker_side").to_pylist()
                    unknown_sides += sum(1 for s in sides if s == "UNKNOWN")
            except TimeoutError:
                continue

    sealed_path = writer.seal()
    elapsed = time.monotonic() - start_t
    return {
        "exchange": "bybit",
        "market_type": "perpetual",
        "topic": "publicTrade.BTCUSDT",
        "dataset_class": "individual_trade",
        "duration_sec": elapsed,
        "envelope_count": envelope_count,
        "record_count": record_count,
        "bytes_written": writer.bytes_written,
        "unknown_sides": unknown_sides,
        "sealed_file": str(sealed_path),
    }


async def run_binance_spot_aggtrade_smoke(root: Path, duration_sec: float = 5.0) -> dict:
    url = "wss://stream.binance.com:9443/ws/btcusdt@aggTrade"
    session = WsSessionInfo(
        session_id="smoke_binance_spot_agg_001",
        connection_id="conn_bsagg1",
        exchange="binance",
        market_type="spot",
        stream_topic="btcusdt@aggTrade",
        connected_at=utc_now(),
    )
    writer = RawWsSegmentWriter(root, "binance", "spot", "BTCUSDT")
    start_t = time.monotonic()
    envelope_count = 0
    record_count = 0

    async with websockets.connect(url, open_timeout=10.0) as ws:
        while time.monotonic() - start_t < duration_sec:
            try:
                msg = await asyncio.wait_for(ws.recv(), timeout=1.0)
                payload = json.loads(msg)
                env = create_raw_ws_envelope(
                    exchange="binance",
                    market_type="spot",
                    instrument_id="ins_382b67a5ff90e4cd6ae4",
                    stream_topic="btcusdt@aggTrade",
                    session=session,
                    payload=payload,
                    source_contract_version="binance.spot.ws.aggTrade.v1",
                )
                writer.write_envelope(env)
                envelope_count += 1

                rb = normalize_ws_envelope_to_aggregate_trades(env)
                assert rb.column("dataset_class")[0].as_py() == "exchange_aggregate_trade"
                record_count += rb.num_rows
            except TimeoutError:
                continue

    sealed_path = writer.seal()
    elapsed = time.monotonic() - start_t
    return {
        "exchange": "binance",
        "market_type": "spot",
        "topic": "btcusdt@aggTrade",
        "dataset_class": "exchange_aggregate_trade",
        "duration_sec": elapsed,
        "envelope_count": envelope_count,
        "record_count": record_count,
        "bytes_written": writer.bytes_written,
        "sealed_file": str(sealed_path),
    }


async def main():
    root = Path("C:/crypto_quant_data")
    print("=== Starting 7A Live-Validation Completion Gate ===")
    res1 = await run_binance_usdm_aggtrade_smoke(root, duration_sec=5.0)
    print("1. Binance USD-M aggTrade:", json.dumps(res1, indent=2))

    res2 = await run_bybit_linear_smoke(root, duration_sec=5.0)
    print("2. Bybit Linear publicTrade:", json.dumps(res2, indent=2))

    res3 = await run_binance_spot_aggtrade_smoke(root, duration_sec=5.0)
    print("3. Binance Spot aggTrade:", json.dumps(res3, indent=2))

    rss = peak_rss_bytes()
    peak_rss_mb = (rss / (1024 * 1024)) if rss else 0.0
    print(f"Peak RAM: {peak_rss_mb:.2f} MB")
    print("=== 7A Live-Validation Completion Gate PASS ===")


if __name__ == "__main__":
    asyncio.run(main())
