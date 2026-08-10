"""Unit tests for Retention Policy and Collector Operational Health (Item 7E)."""

import tempfile
from pathlib import Path

from crypto_quant.ingestion.gap_registry import GapRegistry, GapType
from crypto_quant.ingestion.health import (
    AvailabilityStatus,
    CompletenessStatus,
    compute_collector_health,
)
from crypto_quant.ingestion.retention import (
    RetentionPolicy,
    enforce_retention_policy,
)
from crypto_quant.time import utc_now


def test_retention_policy_dry_run():
    with tempfile.TemporaryDirectory() as tmp_dir:
        root = Path(tmp_dir)
        raw_dir = root / "raw" / "ws"
        raw_dir.mkdir(parents=True, exist_ok=True)
        dummy_file = raw_dir / "sample.jsonl"
        dummy_file.write_text("{}\n", encoding="utf-8")

        import os
        import time
        old_time = time.time() - (10 * 86400)
        os.utime(dummy_file, (old_time, old_time))

        res = enforce_retention_policy(root, RetentionPolicy(raw_ws_envelope_days=5), dry_run=True)
        assert res["raw_ws"] == 1
        assert dummy_file.exists()


def test_collector_health_evaluation():
    with tempfile.TemporaryDirectory() as tmp_dir:
        root = Path(tmp_dir)
        registry = GapRegistry(root)

        now = utc_now()
        _ = registry.register_gap(
            exchange="binance",
            market_type="spot",
            instrument_id="ins_382b67a5ff90e4cd6ae4",
            dataset_class="individual_trade",
            source_stream="btcusdt@trade",
            gap_start=now,
            gap_end=now,
            gap_type=GapType.LOCAL_COLLECTOR_GAP,
        )

        health = compute_collector_health(
            exchange="binance",
            market_type="spot",
            symbol="BTCUSDT",
            root=root,
            current_availability=AvailabilityStatus.HEALTHY,
        )

        assert health.availability == AvailabilityStatus.HEALTHY
        assert health.completeness == CompletenessStatus.GAPPED
        assert health.open_gap_count == 1
