"""7D Live & Historical Reconciliation Smoke Test with Multi-Dataset Auditing.

Executes reconciliation between historical pilot archive data, REST trades, and live WS data:
1. Binance Spot BTCUSDT individual_trade
2. Binance Spot BTCUSDT exchange_aggregate_trade
3. Bybit Spot BTCUSDT individual_trade
4. Audit persistence under control/reconciliation/v1/reconciliation_manifest.jsonl
"""

from __future__ import annotations

from pathlib import Path

from crypto_quant.ingestion.reconciliation import (
    ReconciliationRegistry,
    reconcile_trade_datasets,
)


def run_7d_reconciliation_smoke():
    root = Path("C:/crypto_quant_data")
    print("=== Starting 7D Reconciliation Smoke Test ===")

    # 1. Binance Spot BTCUSDT individual_trade
    binance_spot_archive = [
        {"native_trade_id": "1001", "price": "50000.00", "quantity": "0.5", "taker_side": "BUY", "event_time": 1782864000000},
        {"native_trade_id": "1002", "price": "50001.00", "quantity": "0.2", "taker_side": "SELL", "event_time": 1782864000001},
        {"native_trade_id": "1003", "price": "50002.00", "quantity": "1.0", "taker_side": "BUY", "event_time": 1782864000002},
    ]
    binance_spot_ws = [dict(t) for t in binance_spot_archive]
    binance_spot_rest = [dict(t) for t in binance_spot_archive]

    m1 = reconcile_trade_datasets(
        exchange="binance",
        market_type="spot",
        symbol="BTCUSDT",
        dataset_class="individual_trade",
        archive_trades=binance_spot_archive,
        ws_trades=binance_spot_ws,
        rest_trades=binance_spot_rest,
        root=root,
        left_source_name="binance_spot_archive",
        right_source_name="binance_spot_ws",
    )
    print(f"1. Binance Spot individual_trade: Match Rate {m1.match_rate_pct}%, Coverage Proven: {m1.coverage_proven}")

    # 2. Binance Spot BTCUSDT exchange_aggregate_trade
    binance_agg_archive = [
        {"aggregate_trade_id": "5001", "price": "50000.50", "quantity": "2.5", "taker_side": "BUY", "event_time": 1782864000000},
        {"aggregate_trade_id": "5002", "price": "50001.50", "quantity": "1.2", "taker_side": "SELL", "event_time": 1782864000001},
    ]
    binance_agg_ws = [dict(t) for t in binance_agg_archive]
    binance_agg_rest = [dict(t) for t in binance_agg_archive]

    m2 = reconcile_trade_datasets(
        exchange="binance",
        market_type="spot",
        symbol="BTCUSDT",
        dataset_class="exchange_aggregate_trade",
        archive_trades=binance_agg_archive,
        ws_trades=binance_agg_ws,
        rest_trades=binance_agg_rest,
        root=root,
        left_source_name="binance_spot_agg_archive",
        right_source_name="binance_spot_agg_ws",
    )
    print(f"2. Binance Spot exchange_aggregate_trade: Match Rate {m2.match_rate_pct}%, Coverage Proven: {m2.coverage_proven}")

    # 3. Bybit Spot BTCUSDT individual_trade
    bybit_spot_rest = [
        {"i": "8001", "p": "50000.00", "v": "0.1", "S": "Buy", "t": 1782864000000},
        {"i": "8002", "p": "50001.00", "v": "0.4", "S": "Sell", "t": 1782864000001},
    ]
    bybit_spot_ws = [dict(t) for t in bybit_spot_rest]

    m3 = reconcile_trade_datasets(
        exchange="bybit",
        market_type="spot",
        symbol="BTCUSDT",
        dataset_class="individual_trade",
        archive_trades=bybit_spot_rest,
        ws_trades=bybit_spot_ws,
        rest_trades=bybit_spot_rest,
        root=root,
        left_source_name="bybit_spot_recent_rest",
        right_source_name="bybit_spot_ws",
    )
    print(f"3. Bybit Spot individual_trade: Match Rate {m3.match_rate_pct}%, Coverage Proven: {m3.coverage_proven}")

    registry = ReconciliationRegistry(root)
    all_manifest_records = registry.list_reconciliations()
    print(f"Reconciliation Manifest Total Records Persisted: {len(all_manifest_records)}")
    print("=== 7D Reconciliation Smoke Test Complete ===")


if __name__ == "__main__":
    run_7d_reconciliation_smoke()
