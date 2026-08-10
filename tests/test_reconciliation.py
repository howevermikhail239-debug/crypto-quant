"""Unit and integration acceptance tests for Item 7D Reconciliation."""

import tempfile
from pathlib import Path

import pytest

from crypto_quant.ingestion.reconciliation import (
    ReconciliationCategory,
    ReconciliationRegistry,
    extract_natural_key,
    reconcile_trade_datasets,
)


def test_natural_key_extraction_by_dataset_class():
    # Individual trade
    ind_item = {"native_trade_id": "1001", "price": "50000", "quantity": "1"}
    assert extract_natural_key(ind_item, "individual_trade") == "1001"

    # Aggregate trade
    agg_item = {"aggregate_trade_id": "5001", "price": "50000", "quantity": "1"}
    assert extract_natural_key(agg_item, "exchange_aggregate_trade") == "5001"

    # Invalid key raises ValueError
    with pytest.raises(ValueError, match="Unable to extract explicit natural key"):
        extract_natural_key({"price": "50000"}, "individual_trade")


def test_reconciliation_dataset_class_mismatch_fails_closed():
    with tempfile.TemporaryDirectory() as tmp_dir:
        root = Path(tmp_dir)
        archive = [{"native_trade_id": "101", "price": "50000", "quantity": "1"}]
        ws = [{"native_trade_id": "101", "price": "50000", "quantity": "1"}]

        with pytest.raises(TypeError, match="DATASET_CLASS_MISMATCH"):
            reconcile_trade_datasets(
                exchange="binance",
                market_type="spot",
                symbol="BTCUSDT",
                dataset_class="individual_trade",
                right_dataset_class="exchange_aggregate_trade",
                archive_trades=archive,
                ws_trades=ws,
                rest_trades=[],
                root=root,
            )


def test_reconciliation_field_and_side_conflict_classification():
    with tempfile.TemporaryDirectory() as tmp_dir:
        root = Path(tmp_dir)
        archive = [
            {"native_trade_id": "101", "price": "50000.00", "quantity": "0.5", "taker_side": "BUY"},
            {"native_trade_id": "102", "price": "50001.00", "quantity": "0.2", "taker_side": "SELL"},
        ]
        ws = [
            {"native_trade_id": "101", "price": "50099.00", "quantity": "0.5", "taker_side": "BUY"},   # Price mismatch
            {"native_trade_id": "102", "price": "50001.00", "quantity": "0.2", "taker_side": "BUY"},   # Side mismatch
        ]

        metrics = reconcile_trade_datasets(
            exchange="binance",
            market_type="spot",
            symbol="BTCUSDT",
            dataset_class="individual_trade",
            archive_trades=archive,
            ws_trades=ws,
            rest_trades=archive,
            root=root,
        )

        assert metrics.field_mismatch_count == 1
        assert metrics.side_mismatch_count == 1
        assert metrics.exact_matched_count == 0
        assert metrics.coverage_proven is False

        cats = [d["category"] for d in metrics.discrepancy_details]
        assert ReconciliationCategory.FIELD_CONFLICT.value in cats
        assert ReconciliationCategory.SIDE_CONFLICT.value in cats


def test_exact_reconciliation_match_and_manifest():
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

        metrics = reconcile_trade_datasets(
            exchange="binance",
            market_type="spot",
            symbol="BTCUSDT",
            dataset_class="individual_trade",
            archive_trades=archive,
            ws_trades=ws,
            rest_trades=archive,
            root=root,
        )

        assert metrics.match_rate_pct == 100.0
        assert metrics.exact_matched_count == 2
        assert metrics.coverage_proven is True
        assert metrics.algorithm_version == "v1.1_natural_key_strict"

        registry = ReconciliationRegistry(root)
        records = registry.list_reconciliations()
        assert len(records) == 1
        assert records[0].match_rate_pct == 100.0


def test_timestamp_conflict_at_zero_tolerance():
    """Same trade ID but 900ms timestamp difference → TIMESTAMP_CONFLICT at default tolerance=0."""
    with tempfile.TemporaryDirectory() as tmp_dir:
        root = Path(tmp_dir)
        archive = [{"native_trade_id": "201", "price": "50000", "quantity": "1", "event_time": 1782864000000}]
        ws = [{"native_trade_id": "201", "price": "50000", "quantity": "1", "event_time": 1782864000900}]

        metrics = reconcile_trade_datasets(
            exchange="binance",
            market_type="spot",
            symbol="BTCUSDT",
            dataset_class="individual_trade",
            archive_trades=archive,
            ws_trades=ws,
            rest_trades=[],
            root=root,
            timestamp_tolerance_ms=0,  # explicit: exact match required
        )

        assert metrics.timestamp_mismatch_count == 1
        assert metrics.coverage_proven is False
        cats = [d["category"] for d in metrics.discrepancy_details]
        assert ReconciliationCategory.TIMESTAMP_CONFLICT.value in cats


def test_timestamp_match_within_explicit_tolerance():
    """Same trade ID, 500ms diff, tolerance=1000ms → MATCH (no conflict)."""
    with tempfile.TemporaryDirectory() as tmp_dir:
        root = Path(tmp_dir)
        archive = [{"native_trade_id": "301", "price": "50000", "quantity": "1", "event_time": 1782864000000}]
        ws = [{"native_trade_id": "301", "price": "50000", "quantity": "1", "event_time": 1782864000500}]

        metrics = reconcile_trade_datasets(
            exchange="binance",
            market_type="spot",
            symbol="BTCUSDT",
            dataset_class="individual_trade",
            archive_trades=archive,
            ws_trades=ws,
            rest_trades=[],
            root=root,
            timestamp_tolerance_ms=1000,  # explicitly justified tolerance
        )

        assert metrics.timestamp_mismatch_count == 0
        assert metrics.exact_matched_count == 1
        assert metrics.coverage_proven is True


def test_dataset_class_isolation_reverse_direction():
    """exchange_aggregate_trade vs individual_trade → rejected (both directions)."""
    with tempfile.TemporaryDirectory() as tmp_dir:
        root = Path(tmp_dir)
        trades = [{"aggregate_trade_id": "5001", "price": "50000", "quantity": "1"}]

        with pytest.raises(TypeError, match="DATASET_CLASS_MISMATCH"):
            reconcile_trade_datasets(
                exchange="binance",
                market_type="spot",
                symbol="BTCUSDT",
                dataset_class="exchange_aggregate_trade",
                right_dataset_class="individual_trade",
                archive_trades=trades,
                ws_trades=trades,
                rest_trades=[],
                root=root,
            )

