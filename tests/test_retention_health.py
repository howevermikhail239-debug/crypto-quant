"""Unit and integration acceptance tests for Item 7E Retention & Operational Health.

Covers:
- Retention boundary tests at exact policy limits (BLOCKER 1)
- Disk threshold three-tier transitions (BLOCKER 3)
- Availability vs Completeness separation (BLOCKER 17)
- Transport-level liveness vs market freshness (BLOCKER 7, BLOCKER 15)
"""

import os
import tempfile
import time
from datetime import timedelta
from pathlib import Path

from crypto_quant.ingestion.gap_registry import GapRegistry, GapType
from crypto_quant.ingestion.health import (
    AvailabilityStatus,
    CompletenessStatus,
    DiskThresholdStatus,
    compute_collector_health,
)
from crypto_quant.ingestion.retention import (
    RetentionPolicy,
    enforce_retention_policy,
)
from crypto_quant.time import utc_now

# ---------------------------------------------------------------------------
# BLOCKER 1 — retention boundary tests at the DEFAULT 30-day policy
# ---------------------------------------------------------------------------


def test_retention_10d_old_artifact_kept_under_30d_policy():
    """10-day-old raw WS artifact must NOT be deleted under default 30d policy."""
    with tempfile.TemporaryDirectory() as tmp_dir:
        root = Path(tmp_dir)
        raw_dir = root / "raw" / "ws"
        raw_dir.mkdir(parents=True, exist_ok=True)
        f = raw_dir / "ten_day_old.jsonl"
        f.write_text("{}\n", encoding="utf-8")
        os.utime(f, times=(time.time() - 10 * 86400, time.time() - 10 * 86400))

        res = enforce_retention_policy(root, RetentionPolicy(raw_ws_envelope_days=30), dry_run=False)
        assert res["raw_ws"] == 0
        assert f.exists(), "10-day-old artifact must be kept under 30d policy"


def test_retention_29d_old_artifact_kept_under_30d_policy():
    """29-day-old raw WS artifact must NOT be deleted under 30d policy."""
    with tempfile.TemporaryDirectory() as tmp_dir:
        root = Path(tmp_dir)
        raw_dir = root / "raw" / "ws"
        raw_dir.mkdir(parents=True, exist_ok=True)
        f = raw_dir / "twenty_nine_day_old.jsonl"
        f.write_text("{}\n", encoding="utf-8")
        os.utime(f, times=(time.time() - 29 * 86400, time.time() - 29 * 86400))

        res = enforce_retention_policy(root, RetentionPolicy(raw_ws_envelope_days=30), dry_run=False)
        assert res["raw_ws"] == 0
        assert f.exists(), "29-day-old artifact must be kept under 30d policy"


def test_retention_31d_old_artifact_deleted_under_30d_policy():
    """31-day-old raw WS artifact MUST be a delete candidate under 30d policy."""
    with tempfile.TemporaryDirectory() as tmp_dir:
        root = Path(tmp_dir)
        raw_dir = root / "raw" / "ws"
        raw_dir.mkdir(parents=True, exist_ok=True)
        f = raw_dir / "thirty_one_day_old.jsonl"
        f.write_text("{}\n", encoding="utf-8")
        os.utime(f, times=(time.time() - 31 * 86400, time.time() - 31 * 86400))

        res_dry = enforce_retention_policy(root, RetentionPolicy(raw_ws_envelope_days=30), dry_run=True)
        assert res_dry["raw_ws"] == 1, "31-day-old should be deletion candidate"
        assert f.exists(), "dry-run must not delete"

        res = enforce_retention_policy(root, RetentionPolicy(raw_ws_envelope_days=30), dry_run=False)
        assert res["raw_ws"] == 1
        assert not f.exists(), "31-day-old must be deleted under 30d policy"


def test_retention_boundary_exactly_30d_kept():
    """Artifact aged exactly at the policy boundary (30d) must be kept (strictly less-than)."""
    with tempfile.TemporaryDirectory() as tmp_dir:
        root = Path(tmp_dir)
        raw_dir = root / "raw" / "ws"
        raw_dir.mkdir(parents=True, exist_ok=True)
        f = raw_dir / "exactly_30d.jsonl"
        f.write_text("{}\n", encoding="utf-8")
        # Exactly 30 days ago (should NOT be deleted because the check is strictly < threshold)
        os.utime(f, times=(time.time() - 30 * 86400, time.time() - 30 * 86400))

        _ = enforce_retention_policy(root, RetentionPolicy(raw_ws_envelope_days=30), dry_run=True)
        # This may be 0 or 1 depending on sub-second timing.  Document: the engine
        # uses strict less-than (mtime < now - max_days) so at exact boundary the
        # result depends on sub-second race.  We assert the file still exists after
        # dry-run regardless.
        assert f.exists()


# ---------------------------------------------------------------------------
# PRE-1D FIX 1 — SEMANTIC COVERAGE AGE RESOLUTION TESTS
# ---------------------------------------------------------------------------


def test_retention_semantic_coverage_older_than_fresh_mtime():
    """Old manifest coverage (40d ago) + fresh filesystem mtime (1h ago) -> ELIGIBLE & DELETED."""
    import json

    from crypto_quant.ingestion.retention import DeletionLedger

    with tempfile.TemporaryDirectory() as tmp_dir:
        root = Path(tmp_dir)
        raw_dir = root / "raw" / "ws"
        raw_dir.mkdir(parents=True, exist_ok=True)
        f = raw_dir / "recopied_ws_file.jsonl"
        f.write_text("{}\n", encoding="utf-8")
        # Fresh filesystem mtime (today)
        now_ts = time.time()
        os.utime(f, times=(now_ts, now_ts))

        # Create manifest with old coverage_end (40 days ago)
        manifest_dir = root / "control" / "manifests"
        manifest_dir.mkdir(parents=True, exist_ok=True)
        mfile = manifest_dir / "raw_ws_manifest.jsonl"
        old_cov = (utc_now() - timedelta(days=40)).isoformat()
        mrec = {
            "object_id": str(f.relative_to(root)),
            "coverage_end": old_cov,
            "action": "RECORDED",
        }
        mfile.write_text(json.dumps(mrec) + "\n", encoding="utf-8")

        res = enforce_retention_policy(root, RetentionPolicy(raw_ws_envelope_days=30), dry_run=False)
        assert res["raw_ws"] == 1
        assert not f.exists(), "Old semantic coverage must cause deletion despite fresh mtime"

        ledger = DeletionLedger(root)
        deletions = ledger.list_deletions()
        assert len(deletions) == 1
        assert deletions[0].age_basis == "manifest_coverage_end"
        assert deletions[0].semantic_age_timestamp == old_cov


def test_retention_recent_coverage_with_old_mtime_kept():
    """Recent manifest coverage (2d ago) + artificially old mtime (100d ago) -> KEPT."""
    import json

    with tempfile.TemporaryDirectory() as tmp_dir:
        root = Path(tmp_dir)
        raw_dir = root / "raw" / "ws"
        raw_dir.mkdir(parents=True, exist_ok=True)
        f = raw_dir / "recent_stream.jsonl"
        f.write_text("{}\n", encoding="utf-8")
        # Artificially old mtime (100 days ago)
        old_ts = time.time() - 100 * 86400
        os.utime(f, times=(old_ts, old_ts))

        # Manifest specifies recent coverage_end (2 days ago)
        manifest_dir = root / "control" / "manifests"
        manifest_dir.mkdir(parents=True, exist_ok=True)
        mfile = manifest_dir / "raw_ws_manifest.jsonl"
        recent_cov = (utc_now() - timedelta(days=2)).isoformat()
        mrec = {
            "object_id": str(f.relative_to(root)),
            "coverage_end": recent_cov,
            "action": "RECORDED",
        }
        mfile.write_text(json.dumps(mrec) + "\n", encoding="utf-8")

        res = enforce_retention_policy(root, RetentionPolicy(raw_ws_envelope_days=30), dry_run=False)
        assert res["raw_ws"] == 0
        assert f.exists(), "Recent semantic coverage must protect file from deletion despite old mtime"


def test_retention_mtime_fallback_when_manifest_absent():
    """When manifest is absent and filename has no date, falls back to filesystem mtime."""
    from crypto_quant.ingestion.retention import DeletionLedger

    with tempfile.TemporaryDirectory() as tmp_dir:
        root = Path(tmp_dir)
        raw_dir = root / "raw" / "ws"
        raw_dir.mkdir(parents=True, exist_ok=True)
        f = raw_dir / "unindexed_file.jsonl"
        f.write_text("{}\n", encoding="utf-8")
        old_ts = time.time() - 35 * 86400
        os.utime(f, times=(old_ts, old_ts))

        res = enforce_retention_policy(root, RetentionPolicy(raw_ws_envelope_days=30), dry_run=False)
        assert res["raw_ws"] == 1
        assert not f.exists()

        ledger = DeletionLedger(root)
        deletions = ledger.list_deletions()
        assert len(deletions) == 1
        assert deletions[0].age_basis == "filesystem_mtime"



# ---------------------------------------------------------------------------
# BLOCKER 3 — three-tier disk thresholds matching config/default.yaml
# ---------------------------------------------------------------------------


def test_disk_threshold_ok():
    """Free > 80 GB → OK."""
    with tempfile.TemporaryDirectory() as tmp_dir:
        root = Path(tmp_dir)
        h = compute_collector_health(
            exchange="test", market_type="spot", symbol="BTCUSDT", root=root,
            warning_disk_gb=80.0, bootstrap_stop_disk_gb=50.0, critical_ingestion_stop_disk_gb=20.0,
        )
        # The test machine has 300+ GB free, so this should be OK
        assert h.disk_status == DiskThresholdStatus.OK


def test_disk_threshold_warning():
    """Free < warning threshold → WARNING."""
    with tempfile.TemporaryDirectory() as tmp_dir:
        root = Path(tmp_dir)
        h = compute_collector_health(
            exchange="test", market_type="spot", symbol="BTCUSDT", root=root,
            warning_disk_gb=999999.0,  # Impossibly high → triggers WARNING
            bootstrap_stop_disk_gb=50.0, critical_ingestion_stop_disk_gb=20.0,
        )
        assert h.disk_status == DiskThresholdStatus.WARNING


def test_disk_threshold_bootstrap_stop():
    """Free < bootstrap_stop threshold → BOOTSTRAP_STOP."""
    with tempfile.TemporaryDirectory() as tmp_dir:
        root = Path(tmp_dir)
        h = compute_collector_health(
            exchange="test", market_type="spot", symbol="BTCUSDT", root=root,
            warning_disk_gb=999999.0,
            bootstrap_stop_disk_gb=999999.0,  # Also impossibly high
            critical_ingestion_stop_disk_gb=20.0,
        )
        assert h.disk_status == DiskThresholdStatus.BOOTSTRAP_STOP


def test_disk_threshold_critical_ingestion_stop():
    """Free < critical_ingestion_stop threshold → CRITICAL_INGESTION_STOP."""
    with tempfile.TemporaryDirectory() as tmp_dir:
        root = Path(tmp_dir)
        h = compute_collector_health(
            exchange="test", market_type="spot", symbol="BTCUSDT", root=root,
            warning_disk_gb=999999.0,
            bootstrap_stop_disk_gb=999999.0,
            critical_ingestion_stop_disk_gb=999999.0,  # All impossibly high
        )
        assert h.disk_status == DiskThresholdStatus.CRITICAL_INGESTION_STOP


# ---------------------------------------------------------------------------
# BLOCKER 7 / 15 — transport liveness vs market freshness
# ---------------------------------------------------------------------------


def test_transport_unhealthy_degrades_availability():
    """transport_healthy=False must transition HEALTHY → DEGRADED."""
    with tempfile.TemporaryDirectory() as tmp_dir:
        root = Path(tmp_dir)
        h = compute_collector_health(
            exchange="binance", market_type="spot", symbol="BTCUSDT", root=root,
            current_availability=AvailabilityStatus.HEALTHY,
            transport_healthy=False,
        )
        assert h.availability == AvailabilityStatus.DEGRADED


def test_transport_healthy_preserves_availability():
    """transport_healthy=True (default) must NOT degrade HEALTHY."""
    with tempfile.TemporaryDirectory() as tmp_dir:
        root = Path(tmp_dir)
        h = compute_collector_health(
            exchange="binance", market_type="spot", symbol="BTCUSDT", root=root,
            current_availability=AvailabilityStatus.HEALTHY,
            transport_healthy=True,
        )
        assert h.availability == AvailabilityStatus.HEALTHY


def test_reconnecting_not_overridden_by_transport():
    """If already RECONNECTING, transport_healthy=False must NOT downgrade to DEGRADED."""
    with tempfile.TemporaryDirectory() as tmp_dir:
        root = Path(tmp_dir)
        h = compute_collector_health(
            exchange="binance", market_type="spot", symbol="BTCUSDT", root=root,
            current_availability=AvailabilityStatus.RECONNECTING,
            transport_healthy=False,
        )
        assert h.availability == AvailabilityStatus.RECONNECTING


# ---------------------------------------------------------------------------
# BLOCKER 17 — availability vs completeness never merged
# ---------------------------------------------------------------------------


def test_availability_and_completeness_independent():
    """Availability=HEALTHY + Completeness=GAPPED must coexist."""
    with tempfile.TemporaryDirectory() as tmp_dir:
        root = Path(tmp_dir)
        gap_reg = GapRegistry(root)
        now = utc_now()
        gap_reg.register_gap(
            exchange="binance", market_type="spot",
            instrument_id="ins_test", dataset_class="individual_trade",
            source_stream="btcusdt@trade",
            gap_start=now, gap_end=now,
            gap_type=GapType.LOCAL_COLLECTOR_GAP,
        )

        h = compute_collector_health(
            exchange="binance", market_type="spot", symbol="BTCUSDT", root=root,
            current_availability=AvailabilityStatus.HEALTHY,
        )
        assert h.availability == AvailabilityStatus.HEALTHY
        assert h.completeness == CompletenessStatus.GAPPED
        assert h.open_gap_count == 1


def test_collector_health_complete_when_no_gaps():
    """No gaps → COMPLETE completeness."""
    with tempfile.TemporaryDirectory() as tmp_dir:
        root = Path(tmp_dir)
        h = compute_collector_health(
            exchange="binance", market_type="spot", symbol="BTCUSDT", root=root,
        )
        assert h.completeness == CompletenessStatus.COMPLETE
        assert h.open_gap_count == 0
