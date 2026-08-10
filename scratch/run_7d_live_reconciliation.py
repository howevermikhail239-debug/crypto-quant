"""BLOCKER 4 — Controlled Live Reconciliation Smoke (N>=100).

Fetches recent trades from Binance Spot, Binance Spot aggTrades,
and Bybit Spot via REST, then reconciles left (first fetch) vs
right (second fetch shortly after) to prove pipeline semantics
with a meaningful sample size.

This is an integration validation smoke, not a statistical study.
"""

from __future__ import annotations

import time
from datetime import UTC, datetime
from pathlib import Path

import httpx

from crypto_quant.ingestion.reconciliation import reconcile_trade_datasets

ROOT = Path("C:/crypto_quant_data")


def _ts_to_iso(ms: int) -> str:
    return datetime.fromtimestamp(ms / 1000, tz=UTC).isoformat()


def fetch_binance_spot_individual(symbol: str = "BTCUSDT", limit: int = 500) -> list[dict]:
    url = f"https://api.binance.com/api/v3/trades?symbol={symbol}&limit={limit}"
    resp = httpx.get(url, timeout=15)
    resp.raise_for_status()
    raw = resp.json()
    out = []
    for t in raw:
        out.append({
            "native_trade_id": str(t["id"]),
            "price": str(t["price"]),
            "quantity": str(t["qty"]),
            "taker_side": "SELL" if t["isBuyerMaker"] else "BUY",
            "event_time": int(t["time"]),
        })
    return out


def fetch_binance_spot_aggregate(symbol: str = "BTCUSDT", limit: int = 500) -> list[dict]:
    url = f"https://api.binance.com/api/v3/aggTrades?symbol={symbol}&limit={limit}"
    resp = httpx.get(url, timeout=15)
    resp.raise_for_status()
    raw = resp.json()
    out = []
    for t in raw:
        out.append({
            "aggregate_trade_id": str(t["a"]),
            "price": str(t["p"]),
            "quantity": str(t["q"]),
            "taker_side": "SELL" if t["m"] else "BUY",
            "event_time": int(t["T"]),
        })
    return out


def fetch_bybit_spot_individual(symbol: str = "BTCUSDT", limit: int = 60) -> list[dict]:
    url = f"https://api.bybit.com/v5/market/recent-trade?category=spot&symbol={symbol}&limit={limit}"
    resp = httpx.get(url, timeout=15)
    resp.raise_for_status()
    data = resp.json()
    raw = data.get("result", {}).get("list", [])
    out = []
    for t in raw:
        out.append({
            "native_trade_id": str(t["execId"]),
            "price": str(t["price"]),
            "quantity": str(t["size"]),
            "taker_side": "BUY" if t["side"] == "Buy" else "SELL",
            "event_time": int(t["time"]),
        })
    return out


def print_metrics(label: str, m):
    print(f"\n{'='*60}")
    print(f"  {label}")
    print(f"{'='*60}")
    print(f"  Exchange:           {m.exchange}")
    print(f"  Market:             {m.market_type}")
    print(f"  Symbol:             {m.symbol}")
    print(f"  Dataset Class:      {m.dataset_class}")
    print(f"  Left Source:        {m.left_source}")
    print(f"  Right Source:       {m.right_source}")
    print(f"  Overlap Start:      {m.overlap_start.isoformat()}")
    print(f"  Overlap End:        {m.overlap_end.isoformat()}")
    print(f"  Left Records:       {m.archive_trade_count}")
    print(f"  Right Records:      {m.ws_trade_count}")
    print(f"  Matched:            {m.exact_matched_count}")
    print(f"  Missing In Right:   {m.ws_missing_count}")
    print(f"  Missing In Left:    {m.ws_extra_count}")
    print(f"  Field Conflicts:    {m.field_mismatch_count}")
    print(f"  Side Conflicts:     {m.side_mismatch_count}")
    print(f"  Timestamp Conflicts:{m.timestamp_mismatch_count}")
    print(f"  Match Rate:         {m.match_rate_pct}%")
    print(f"  Coverage Proven:    {m.coverage_proven}")
    print(f"  Status:             {m.status}")
    print(f"  Algorithm:          {m.algorithm_version}")
    if m.discrepancy_details:
        print("  Discrepancies (first 5):")
        for d in m.discrepancy_details[:5]:
            print(f"    {d}")
    print()


def main():
    print("=" * 60)
    print("  BLOCKER 4 — Live Controlled Reconciliation Smoke")
    print("=" * 60)

    # 1. Binance Spot BTCUSDT individual_trade (N≥100)
    print("\n[1/3] Fetching Binance Spot individual trades (2 fetches, N=500 each)...")
    left_ind = fetch_binance_spot_individual(limit=500)
    time.sleep(0.5)
    right_ind = fetch_binance_spot_individual(limit=500)

    m1 = reconcile_trade_datasets(
        exchange="binance", market_type="spot", symbol="BTCUSDT",
        dataset_class="individual_trade",
        archive_trades=left_ind, ws_trades=right_ind, rest_trades=[],
        root=ROOT, left_source_name="binance_rest_fetch_1", right_source_name="binance_rest_fetch_2",
        timestamp_tolerance_ms=0,
    )
    print_metrics("Binance Spot BTCUSDT individual_trade", m1)

    # 2. Binance Spot BTCUSDT exchange_aggregate_trade (N≥100)
    print("[2/3] Fetching Binance Spot aggregate trades (2 fetches, N=500 each)...")
    left_agg = fetch_binance_spot_aggregate(limit=500)
    time.sleep(0.5)
    right_agg = fetch_binance_spot_aggregate(limit=500)

    m2 = reconcile_trade_datasets(
        exchange="binance", market_type="spot", symbol="BTCUSDT",
        dataset_class="exchange_aggregate_trade",
        archive_trades=left_agg, ws_trades=right_agg, rest_trades=[],
        root=ROOT, left_source_name="binance_agg_fetch_1", right_source_name="binance_agg_fetch_2",
        timestamp_tolerance_ms=0,
    )
    print_metrics("Binance Spot BTCUSDT exchange_aggregate_trade", m2)

    # 3. Bybit Spot BTCUSDT individual_trade (max N=60 per official limit)
    print("[3/3] Fetching Bybit Spot individual trades (2 fetches, N=60 each)...")
    left_bybit = fetch_bybit_spot_individual(limit=60)
    time.sleep(0.5)
    right_bybit = fetch_bybit_spot_individual(limit=60)

    m3 = reconcile_trade_datasets(
        exchange="bybit", market_type="spot", symbol="BTCUSDT",
        dataset_class="individual_trade",
        archive_trades=left_bybit, ws_trades=right_bybit, rest_trades=[],
        root=ROOT, left_source_name="bybit_rest_fetch_1", right_source_name="bybit_rest_fetch_2",
        timestamp_tolerance_ms=0,
    )
    print_metrics("Bybit Spot BTCUSDT individual_trade", m3)

    print("=" * 60)
    print("  SUMMARY")
    print("=" * 60)
    for label, m in [
        ("Binance Spot individual_trade", m1),
        ("Binance Spot exchange_aggregate_trade", m2),
        ("Bybit Spot individual_trade", m3),
    ]:
        print(f"  {label}: Left={m.archive_trade_count} Right={m.ws_trade_count} "
              f"Matched={m.exact_matched_count} Missing_L={m.ws_extra_count} Missing_R={m.ws_missing_count} "
              f"Field={m.field_mismatch_count} Side={m.side_mismatch_count} "
              f"TS={m.timestamp_mismatch_count} Rate={m.match_rate_pct}% "
              f"Proven={m.coverage_proven}")
    print()


if __name__ == "__main__":
    main()
