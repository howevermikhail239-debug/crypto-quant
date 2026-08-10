"""Unit and integration tests for Plan Item 7D Reconciliation."""

import tempfile
from pathlib import Path

from crypto_quant.ingestion.reconciliation import (
    ReconciliationRegistry,
    reconcile_trade_datasets,
)


def test_exact_reconciliation_match():
    with tempfile.TemporaryDirectory() as tmp_dir:
        root = Path(tmp_dir)
        archive = [
            {"native_trade_id": "101", "price": "50000.00", "quantity": "0.5"},
            {"native_trade_id": "102", "price": "50001.00", "quantity": "0.2"},
        ]
        ws = [
            {"native_trade_id": "101", "price": "50000.00", "quantity": "0.5"},
            {"native_trade_id": "102", "price": "50001.00", "quantity": "0.2"},
        ]
        rest = [
            {"native_trade_id": "101", "price": "50000.00", "quantity": "0.5"},
            {"native_trade_id": "102", "price": "50001.00", "quantity": "0.2"},
        ]

        metrics = reconcile_trade_datasets(
            exchange="binance",
            market_type="spot",
            symbol="BTCUSDT",
            dataset_class="individual_trade",
            archive_trades=archive,
            ws_trades=ws,
            rest_trades=rest,
            root=root,
        )

        assert metrics.match_rate_pct == 100.0
        assert metrics.exact_matched_count == 2
        assert metrics.ws_missing_count == 0
        assert metrics.field_mismatch_count == 0

        registry = ReconciliationRegistry(root)
        records = registry.list_reconciliations()
        assert len(records) == 1
        assert records[0].match_rate_pct == 100.0


def test_ws_missing_trade_and_mismatch_detection():
    with tempfile.TemporaryDirectory() as tmp_dir:
        root = Path(tmp_dir)
        archive = [
            {"native_trade_id": "101", "price": "50000.00", "quantity": "0.5"},
            {"native_trade_id": "102", "price": "50001.00", "quantity": "0.2"},
            {"native_trade_id": "103", "price": "50002.00", "quantity": "0.8"},
        ]
        ws = [
            {"native_trade_id": "101", "price": "50000.00", "quantity": "0.5"},
            {"native_trade_id": "102", "price": "50099.00", "quantity": "0.2"},  # Price Mismatch
            # Trade 103 Missing in WS
        ]
        rest = archive

        metrics = reconcile_trade_datasets(
            exchange="binance",
            market_type="spot",
            symbol="BTCUSDT",
            dataset_class="individual_trade",
            archive_trades=archive,
            ws_trades=ws,
            rest_trades=rest,
            root=root,
        )

        assert metrics.exact_matched_count == 1
        assert metrics.ws_missing_count == 1
        assert metrics.field_mismatch_count == 1
        assert metrics.match_rate_pct == 33.33
