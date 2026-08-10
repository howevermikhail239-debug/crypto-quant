"""Temporary Sandbox Acceptance Tests for Item 7E Retention Engine (Points 8, 9, 10, 13, 19, 20)."""

import os
import tempfile
import time
from pathlib import Path

from crypto_quant.ingestion.gap_registry import GapRegistry, GapStatus, GapType
from crypto_quant.ingestion.reconciliation import reconcile_trade_datasets
from crypto_quant.ingestion.retention import (
    DeletionLedger,
    HoldRegistry,
    HoldType,
    RetentionPolicy,
    enforce_retention_policy,
)
from crypto_quant.time import utc_now


def test_retention_sandbox_acceptance():
    with tempfile.TemporaryDirectory() as tmp_dir:
        root = Path(tmp_dir)
        old_time = time.time() - (10 * 86400)
        ten_years_ago = time.time() - (3650 * 86400)

        # 1. Create Sandbox Artifacts
        raw_dir = root / "raw" / "ws"
        raw_dir.mkdir(parents=True, exist_ok=True)

        # Artifact A: expired deletable raw artifact
        file_a = raw_dir / "artifact_a_deletable.jsonl"
        file_a.write_text("{}\n", encoding="utf-8")
        os.utime(file_a, (old_time, old_time))

        # Artifact B: recent raw artifact
        file_b = raw_dir / "artifact_b_recent.jsonl"
        file_b.write_text("{}\n", encoding="utf-8")

        # Artifact C: expired artifact with MANUAL_HOLD
        file_c = raw_dir / "artifact_c_held.jsonl"
        file_c.write_text("{}\n", encoding="utf-8")
        os.utime(file_c, (old_time, old_time))
        hold_reg = HoldRegistry(root)
        hold_reg.add_hold(HoldType.MANUAL_HOLD, "artifact_c_held.jsonl", "Manual investigation hold")

        # Artifact D: expired artifact linked to PARTIAL gap
        gap_reg = GapRegistry(root)
        now = utc_now()
        gap_d = gap_reg.register_gap(
            exchange="binance",
            market_type="spot",
            instrument_id="ins_382b67a5ff90e4cd6ae4",
            dataset_class="individual_trade",
            source_stream="btcusdt@trade",
            gap_start=now,
            gap_end=now,
            gap_type=GapType.LOCAL_COLLECTOR_GAP,
        )
        gap_d.status = GapStatus.PARTIAL
        gap_reg.update_gap(gap_d)

        file_d = raw_dir / f"artifact_d_gap_{gap_d.gap_id}.jsonl"
        file_d.write_text("{}\n", encoding="utf-8")
        os.utime(file_d, (old_time, old_time))

        # Artifact E: permanent 1m bucket (10 years old)
        bucket_dir = root / "derived" / "trade_bucket" / "v1" / "exchange=binance" / "market_type=spot" / "symbol=BTCUSDT" / "granularity=60s"
        bucket_dir.mkdir(parents=True, exist_ok=True)
        file_e = bucket_dir / "artifact_e_permanent.parquet"
        file_e.write_text("parquet_mock", encoding="utf-8")
        os.utime(file_e, (ten_years_ago, ten_years_ago))

        # Artifact F: expired artifact linked to unresolved reconciliation conflict
        _ = reconcile_trade_datasets(
            exchange="binance",
            market_type="spot",
            symbol="BTCUSDT",
            dataset_class="individual_trade",
            archive_trades=[{"native_trade_id": "999", "price": "50000"}],
            ws_trades=[{"native_trade_id": "999", "price": "50099"}],  # Price conflict!
            rest_trades=[],
            root=root,
        )
        file_f = raw_dir / "artifact_f_conflict_999.jsonl"
        file_f.write_text("{}\n", encoding="utf-8")
        os.utime(file_f, (old_time, old_time))

        # 2. DRY RUN Test
        dry_res = enforce_retention_policy(root, RetentionPolicy(raw_ws_envelope_days=5), dry_run=True)
        assert dry_res["raw_ws"] == 1
        assert file_a.exists()
        assert file_b.exists()
        assert file_c.exists()
        assert file_d.exists()
        assert file_e.exists()
        assert file_f.exists()

        # 3. ACTUAL RETENTION Test
        real_res = enforce_retention_policy(root, RetentionPolicy(raw_ws_envelope_days=5), dry_run=False)
        assert real_res["raw_ws"] == 1
        assert not file_a.exists()  # A deleted!
        assert file_b.exists()      # B kept (recent)
        assert file_c.exists()      # C kept (hold)
        assert file_d.exists()      # D kept (partial gap)
        assert file_e.exists()      # E kept (permanent 1m)
        assert file_f.exists()      # F kept (conflict evidence)

        # 4. DELETION LEDGER Verification
        ledger = DeletionLedger(root)
        deletions = ledger.list_deletions()
        assert len(deletions) == 1
        assert deletions[0].artifact_ref.endswith("artifact_a_deletable.jsonl")

        # 5. IDEMPOTENCY Test (Second Run)
        second_res = enforce_retention_policy(root, RetentionPolicy(raw_ws_envelope_days=5), dry_run=False)
        assert second_res["raw_ws"] == 0
        deletions_after = ledger.list_deletions()
        assert len(deletions_after) == 1  # No duplicate ledger entries!


def test_hold_registry_event_audit_trail():
    with tempfile.TemporaryDirectory() as tmp_dir:
        root = Path(tmp_dir)
        hold_reg = HoldRegistry(root)

        h1 = hold_reg.add_hold(HoldType.INCIDENT_HOLD, "test_target.jsonl", "Testing hold audit trail")
        assert hold_reg.is_held("test_target.jsonl")

        removed = hold_reg.remove_hold(h1.hold_id, "Incident resolved")
        assert removed is True
        assert not hold_reg.is_held("test_target.jsonl")

        events_file = root / "control" / "retention" / "v1" / "hold_events.jsonl"
        assert events_file.exists()
        lines = events_file.read_text(encoding="utf-8").strip().splitlines()
        assert len(lines) == 2
        assert "CREATED" in lines[0]
        assert "REMOVED" in lines[1]
