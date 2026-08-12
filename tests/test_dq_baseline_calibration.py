from __future__ import annotations

import hashlib
import json
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from crypto_quant.ingestion.dq_baseline import (
    CalibrationState,
    DatasetIdentity,
    FailureImpact,
    MetricEvaluation,
    ThresholdPolicy,
    build_dq_baseline,
    evaluate_metric,
    load_threshold_policies,
    write_dq_profile,
)
from crypto_quant.ingestion.gap_registry import GapRegistry, GapStatus, GapType
from crypto_quant.ingestion.health import DQEligibilityStatus
from crypto_quant.storage.catalog import build_catalog

POLICY_PATH = Path(__file__).parents[1] / "config" / "dq_thresholds.yaml"


def _identity() -> DatasetIdentity:
    return DatasetIdentity(
        "individual_trade", "binance", "spot", "btc", "binance.spot.trade", "v1"
    )


def _write_trade_dataset(root: Path, *, sides: list[str], ids: list[str]) -> None:
    relative = "normalized/individual_trade/trades.parquet"
    path = root / relative
    path.parent.mkdir(parents=True)
    table = pa.table(
        {
            "instrument_id": ["btc"] * len(ids),
            "exchange": ["binance"] * len(ids),
            "market_type": ["spot"] * len(ids),
            "source_dataset_id": ["binance.spot.trade"] * len(ids),
            "data_contract_version": ["v1"] * len(ids),
            "native_trade_id": ids,
            "taker_side": sides,
        }
    )
    pq.write_table(table, path)
    digest = hashlib.sha256(path.read_bytes()).hexdigest()
    manifest = root / "control" / "manifests" / "trades.jsonl"
    manifest.parent.mkdir(parents=True)
    manifest.write_text(
        json.dumps(
            {
                "dataset_class": "individual_trade",
                "object_id": relative,
                "parquet_sha256": digest,
                "exchange": "binance",
                "market_type": "spot",
                "instrument_id": "btc",
                "source_dataset_id": "binance.spot.trade",
            }
        )
        + "\n",
        encoding="utf-8",
    )


def test_threshold_states_and_no_arbitrary_fallback() -> None:
    _version, policies = load_threshold_policies(POLICY_PATH)
    calibrated = evaluate_metric(
        identity=_identity(),
        metric="duplicate_rate",
        observed_value=0.0,
        observations=10,
        policy=policies["semantic_unique_key"],
        reason="test",
    )
    assert calibrated.evaluation == MetricEvaluation.PASS
    uncalibrated = evaluate_metric(
        identity=_identity(),
        metric="freshness_age_seconds",
        observed_value=1.0,
        observations=10,
        policy=policies["event_driven_freshness"],
        reason="quiet event-driven feed",
    )
    assert uncalibrated.threshold is None
    assert uncalibrated.evaluation == MetricEvaluation.UNKNOWN
    assert uncalibrated.eligibility_impact == FailureImpact.DEGRADE
    not_applicable = evaluate_metric(
        identity=_identity(),
        metric="freshness_age_seconds",
        observed_value=None,
        observations=10,
        policy=policies["archive_freshness"],
        reason="archive",
    )
    assert not_applicable.evaluation == MetricEvaluation.NOT_APPLICABLE
    assert not_applicable.eligibility_impact == FailureImpact.NONE
    permitted_unknown = evaluate_metric(
        identity=_identity(),
        metric="unknown_rate.position_side_liquidated",
        observed_value=None,
        observations=1,
        policy=ThresholdPolicy(
            "source_limitation",
            CalibrationState.NOT_APPLICABLE,
            "NONE",
            None,
            FailureImpact.NONE,
            ("source contract permits UNKNOWN",),
        ),
        reason="contract-permitted UNKNOWN",
    )
    assert permitted_unknown.evaluation == MetricEvaluation.NOT_APPLICABLE


def test_policy_rejects_threshold_on_uncalibrated_state(tmp_path: Path) -> None:
    policy = tmp_path / "bad.yaml"
    policy.write_text(
        """policy_version: 1
policies:
  bad:
    calibration_state: UNCALIBRATED
    comparison: MAXIMUM
    threshold: 10
""",
        encoding="utf-8",
    )
    with pytest.raises(ValueError, match="cannot set a threshold"):
        load_threshold_policies(policy)


def test_source_specific_baseline_is_deterministic_and_fail_closed(tmp_path: Path) -> None:
    _write_trade_dataset(tmp_path, sides=["BUY", "UNKNOWN"], ids=["1", "1"])
    catalog = build_catalog(tmp_path).catalog_path
    observed_at = datetime(2026, 8, 12, tzinfo=UTC)
    first = build_dq_baseline(
        catalog_path=catalog,
        data_root=tmp_path,
        policy_path=POLICY_PATH,
        observed_at=observed_at,
    )
    second = build_dq_baseline(
        catalog_path=catalog,
        data_root=tmp_path,
        policy_path=POLICY_PATH,
        observed_at=observed_at,
    )
    assert first.to_dict() == second.to_dict()
    assert first.eligibility == DQEligibilityStatus.UNAVAILABLE
    by_name = {metric.metric: metric for metric in first.metrics}
    assert by_name["duplicate_rate"].observed_value == 0.5
    assert by_name["duplicate_rate"].evaluation == MetricEvaluation.FAIL
    side = by_name["unexpected_unknown_rate.taker_side"]
    assert side.observed_value == 0.5 and side.evaluation == MetricEvaluation.FAIL
    assert by_name["freshness_age_seconds"].calibration_state == CalibrationState.NOT_APPLICABLE
    assert by_name["queue_utilization"].calibration_state == CalibrationState.UNCALIBRATED


def test_gap_status_controls_eligibility_without_inactivity_heuristic(tmp_path: Path) -> None:
    _write_trade_dataset(tmp_path, sides=["BUY"], ids=["1"])
    registry = GapRegistry(tmp_path)
    now = datetime(2026, 8, 12, tzinfo=UTC)
    gap = registry.register_gap(
        exchange="binance",
        market_type="spot",
        instrument_id="btc",
        dataset_class="individual_trade",
        source_stream="trade",
        gap_start=now,
        gap_end=now + timedelta(seconds=1),
        gap_type=GapType.LOCAL_COLLECTOR_GAP,
    )
    gap.status = GapStatus.UNRECOVERABLE
    registry.update_gap(gap)
    profile = build_dq_baseline(
        catalog_path=build_catalog(tmp_path).catalog_path,
        data_root=tmp_path,
        policy_path=POLICY_PATH,
        observed_at=now,
    )
    gap_metric = next(metric for metric in profile.metrics if metric.metric == "gap_exposure")
    assert gap_metric.reason == "unrecoverable_gap_present"
    assert gap_metric.eligibility_impact == FailureImpact.UNAVAILABLE
    assert profile.eligibility == DQEligibilityStatus.UNAVAILABLE


def test_machine_readable_profile_write_is_stable(tmp_path: Path) -> None:
    _write_trade_dataset(tmp_path, sides=["BUY"], ids=["1"])
    profile = build_dq_baseline(
        catalog_path=build_catalog(tmp_path).catalog_path,
        data_root=tmp_path,
        policy_path=POLICY_PATH,
        observed_at=datetime(2026, 8, 12, tzinfo=UTC),
    )
    destination = tmp_path / "reports" / "dq.json"
    write_dq_profile(profile, destination)
    first = destination.read_bytes()
    write_dq_profile(profile, destination)
    assert destination.read_bytes() == first
    payload = json.loads(first)
    assert payload["policy_version"] == "1.0.0"
    assert payload["eligibility"] in {"USABLE", "DEGRADED", "UNAVAILABLE"}


def test_retained_quiet_liquidation_soak_is_source_aware_not_stale(tmp_path: Path) -> None:
    catalog = build_catalog(tmp_path).catalog_path
    soak = (
        tmp_path
        / "control"
        / "ingestion_runs"
        / "liquidation_soak"
        / "v1"
        / "liq_soak_test.json"
    )
    soak.parent.mkdir(parents=True)
    soak.write_text(
        json.dumps(
            {
                "stream_results": {
                    "bybit:BTCUSDT": {
                        "exchange": "bybit",
                        "instrument_id": "btc-perp",
                        "source_dataset_id": "bybit.linear.liquidations.ws",
                        "source_contract_version": "bybit.linear.ws.all-liquidation.v1",
                        "event_observation_status": "NO_EVENT_OBSERVED_WITHIN_WINDOW",
                        "transport_status": "PASS",
                        "source_event_count": 0,
                        "raw_message_count": 0,
                        "duplicate_exact_wire_deliveries": 0,
                        "queue_mode": "NOT_APPLICABLE_SYNCHRONOUS_READ_FLUSH",
                    }
                }
            }
        ),
        encoding="utf-8",
    )
    profile = build_dq_baseline(
        catalog_path=catalog,
        data_root=tmp_path,
        policy_path=POLICY_PATH,
        observed_at=datetime(2026, 8, 12, tzinfo=UTC),
    )
    by_name = {metric.metric: metric for metric in profile.metrics}
    assert by_name["freshness_status"].observed_value == "LOW_ACTIVITY_QUIET"
    assert by_name["freshness_status"].evaluation == MetricEvaluation.UNKNOWN
    assert by_name["duplicate_rate.exact_wire_replay"].observed_value is None
    assert by_name["queue_utilization"].evaluation == MetricEvaluation.NOT_APPLICABLE
    assert not (tmp_path / "control" / "gap_registry" / "v1" / "gap_manifest.jsonl").exists()
