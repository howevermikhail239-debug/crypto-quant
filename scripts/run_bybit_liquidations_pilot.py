"""Live pilot runner for Bybit Linear Liquidations Ingestion (Phase 1D.3A)."""

import asyncio
import json
import logging
from pathlib import Path

import pyarrow.parquet as pq

from crypto_quant.ingestion.bybit.liquidations import collect_bybit_liquidations_live

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
root = Path("C:/crypto_quant_data")

print("=" * 70)
print("PHASE 1D.3A: BYBIT LINEAR BTCUSDT REALTIME LIQUIDATION PILOT")
print("=" * 70)

async def main():
    print("\nStarting live collection on Bybit WebSocket for BTCUSDT (30s listen window)...")
    res = await collect_bybit_liquidations_live(
        symbol="BTCUSDT",
        root=root,
        max_duration_seconds=30.0,
        flush_interval_seconds=5.0,
    )
    print(f"\nCollection Result: {res}")

    # Inspect persisted liquidation datasets
    norm_dir = root / "normalized" / "liquidations" / "v1" / "exchange=bybit" / "market_type=perpetual" / "symbol=BTCUSDT"
    pfiles = sorted(norm_dir.rglob("*.parquet")) if norm_dir.exists() else []
    print(f"\nPersisted Parquet Files in {norm_dir}: {len(pfiles)}")
    for pf in pfiles:
        tbl = pq.ParquetFile(pf).read()
        print(f"  - {pf.name}: {len(tbl)} rows, {pf.stat().st_size} bytes")
        if len(tbl) > 0:
            print(f"    Sample row 0: event_time={tbl['event_time'][0].as_py()}, side={tbl['position_side_liquidated'][0].as_py()}, price={tbl['source_price'][0].as_py()}, qty={tbl['source_quantity'][0].as_py()}")

    # Inspect manifest
    mf = root / "control" / "manifests" / "bybit_linear_liquidations.jsonl"
    if mf.exists():
        lines = mf.read_text(encoding="utf-8").strip().splitlines()
        print(f"\nManifest records in {mf.name}: {len(lines)}")
        if lines:
            print(f"  Latest record: {json.dumps(json.loads(lines[-1]), indent=2)}")

if __name__ == "__main__":
    asyncio.run(main())
