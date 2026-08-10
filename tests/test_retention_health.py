"""Unit and integration acceptance tests for Item 7E Retention & Operational Health."""

import os
import tempfile
import time
from pathlib import Path

from crypto_quant.ingestion.gap_registry import GapRegistry, GapType
from crypto_quant.ingestion.health import (
    AvailabilityStatus,
    CompletenessStatus,
    DiskThresholdStatus,
    compute_collector_health,
)
from crypto_quant.ingestion.retention import (
    HoldRegistry,
    HoldType,
    RetentionPolicy,
    enforce_retention_policy,
)
from crypto_quant.time import utc_now


def test_retention_policy_dry_run_and_deletion_ledger():
    with tempfile.TemporaryDirectory() as tmp_dir:
        root = Path(tmp_dir)
        raw_dir = root / "raw" / "ws"
        raw_dir.mkdir(parents=True, exist_ok=True)
        dummy_file = raw_dir / "sample.jsonl"
        dummy_file.write_text("{}\n", encoding="utf-8")

        old_time = time.time() - (10 * 86400)
        os.utime(dummy_file, (old_time, old_time))

        res_dry = enforce_retention_policy(root, RetentionPolicy(raw_ws_envelope_days=5), dry_run=True)
        assert res_dry["raw_ws"] == 1
        assert dummy_file.exists()

        res_real = enforce_retention_policy(root, RetentionPolicy(raw_ws_envelope_days=5), dry_run=False)
        assert res_real["raw_ws"] == 1
        assert not dummy_file.exists()

        ledger_file = root / "control" / "retention" / "v1" / "deletion_ledger.jsonl"
        assert ledger_file.exists()
        ledger_text = ledger_file.read_text(encoding="utf-8")
        assert "sample.jsonl" in ledger_text


def test_retention_holds_prevent_deletion():
    with tempfile.TemporaryDirectory() as tmp_dir:
        root = Path(tmp_dir)
        raw_dir = root / "raw" / "ws"
        raw_dir.mkdir(parents=True, exist_ok=True)
        held_file = raw_dir / "held_file.jsonl"
        held_file.write_text("{}\n", encoding="utf-8")

        old_time = time.time() - (10 * 86400)
        os.utime(held_file, (old_time, old_time))

        hold_reg = HoldRegistry(root)
        hold_reg.add_hold(HoldType.INCIDENT_HOLD, "held_file.jsonl", "Testing active hold protection")

        res = enforce_retention_policy(root, RetentionPolicy(raw_ws_envelope_days=5), dry_run=False)
        assert res["raw_ws"] == 0
        assert held_file.exists()


def test_retention_permanent_1m_buckets_never_deleted():
    with tempfile.TemporaryDirectory() as tmp_dir:
        root = Path(tmp_dir)
        bucket_dir = root / "derived" / "trade_bucket" / "v1" / "exchange=binance" / "market_type=spot" / "symbol=BTCUSDT" / "granularity=60s"
        bucket_dir.mkdir(parents=True, exist_ok=True)
        perm_file = bucket_dir / "part-00000.parquet"
        perm_file.write_text("parquet_mock", encoding="utf-8")

        ten_years_ago = time.time() - (3650 * 86400)
        os.utime(perm_file, (ten_years_ago, ten_years_ago))

        res = enforce_retention_policy(root, RetentionPolicy(sub_minute_bucket_days=90), dry_run=False)
        assert res["minute_buckets"] == 0
        assert perm_file.exists()


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
        assert health.disk_status == DiskThresholdStatus.OK


def test_health_stale_feed_liveness_transition():
    with tempfile.TemporaryDirectory() as tmp_dir:
        root = Path(tmp_dir)
        health = compute_collector_health(
            exchange="binance",
            market_type="spot",
            symbol="BTCUSDT",
            root=root,
            current_availability=AvailabilityStatus.HEALTHY,
            last_message_age_sec=120.0,  # Exceeds default 60s liveness threshold
            liveness_threshold_sec=60.0,
        )
        assert health.availability == AvailabilityStatus.DEGRADED
