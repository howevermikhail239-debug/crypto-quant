"""Bounded real live-event soak on Bybit Linear BTCUSDT allLiquidation WebSocket."""

import asyncio
import json
import logging
import time
from datetime import UTC, datetime
from pathlib import Path

import websockets

from crypto_quant.ingestion.bybit.liquidations import persist_bybit_liquidation_batch

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger("soak")

WS_URL = "wss://stream.bybit.com/v5/public/linear"
TOPIC = "allLiquidation.BTCUSDT"
DATA_ROOT = Path("C:/crypto_quant_data")


async def run_soak(duration_seconds: float = 60.0):
    logger.info(f"Starting bounded live soak ({duration_seconds}s) on {WS_URL} subscribing to {TOPIC}")
    start_time = time.time()
    last_ping = time.time()
    events_captured = []
    raw_messages = []

    async with websockets.connect(WS_URL, ping_interval=20, ping_timeout=10) as ws:
        # Subscribe
        sub_payload = {"op": "subscribe", "args": [TOPIC]}
        await ws.send(json.dumps(sub_payload))
        ack_str = await ws.recv()
        logger.info(f"Subscription ACK: {ack_str}")

        while time.time() - start_time < duration_seconds:
            try:
                msg_str = await asyncio.wait_for(ws.recv(), timeout=2.0)
                recv_at = datetime.now(UTC)
                msg = json.loads(msg_str)
                logger.info(f"Received message: {msg_str[:200]}")

                if msg.get("topic") == TOPIC and "data" in msg:
                    raw_messages.append((msg, msg_str))
                    for item in msg.get("data", []):
                        events_captured.append((item, recv_at))
            except TimeoutError:
                # Connection liveness check: send Bybit WS ping if idle
                now = time.time()
                if now - last_ping >= 20.0:
                    await ws.send(json.dumps({"op": "ping"}))
                    last_ping = now
                    logger.info("Sent heartbeat ping to Bybit WebSocket")

        # If real events were captured, persist them to data root
        if raw_messages:
            logger.info(f"Persisting {len(events_captured)} real captured events...")
            res = persist_bybit_liquidation_batch(raw_messages, "BTCUSDT", DATA_ROOT)
            logger.info(f"Persistence result: {res}")
            return {
                "event_observation_status": "REAL_EVENT_OBSERVED",
                "events_count": len(events_captured),
                "raw_messages_count": len(raw_messages),
                "duration_seconds": round(time.time() - start_time, 2),
                "raw_messages": raw_messages,
                "persist_result": res,
            }
        else:
            logger.info(f"No real BTCUSDT liquidations observed during {duration_seconds}s soak window.")
            return {
                "event_observation_status": "NO_EVENT_OBSERVED_WITHIN_WINDOW",
                "events_count": 0,
                "raw_messages_count": 0,
                "duration_seconds": round(time.time() - start_time, 2),
            }


if __name__ == "__main__":
    result = asyncio.run(run_soak(duration_seconds=60.0))
    print("\n" + "=" * 60)
    print("SOAK RUN RESULT:", json.dumps({k: v for k, v in result.items() if k != "raw_messages"}, indent=2))
    print("=" * 60)
