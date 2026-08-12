"""Reproducible source-aware DQ baseline and threshold evaluation (PHASE 1E.2)."""

from __future__ import annotations

import json
import os
import uuid
from dataclasses import asdict, dataclass
from datetime import datetime, timedelta
from enum import StrEnum
from pathlib import Path
from typing import Any

import duckdb
import yaml

from .gap_registry import GapStatus
from .health import DQEligibilityStatus, classify_dq_eligibility, summarize_gap_exposure


class CalibrationState(StrEnum):
    CALIBRATED = "CALIBRATED"
    UNCALIBRATED = "UNCALIBRATED"
    NOT_APPLICABLE = "NOT_APPLICABLE"


class MetricEvaluation(StrEnum):
    PASS = "PASS"
    FAIL = "FAIL"
    UNKNOWN = "UNKNOWN"
    NOT_APPLICABLE = "NOT_APPLICABLE"


class FailureImpact(StrEnum):
    NONE = "NONE"
    DEGRADE = "DEGRADE"
    UNAVAILABLE = "UNAVAILABLE"


@dataclass(frozen=True)
class ThresholdPolicy:
    policy_id: str
    calibration_state: CalibrationState
    comparison: str
    threshold: float | None
    failure_impact: FailureImpact
    evidence: tuple[str, ...]


@dataclass(frozen=True, order=True)
class DatasetIdentity:
    dataset_class: str
    exchange: str
    market_type: str
    instrument_id: str
    source_dataset_id: str
    source_contract_version: str


@dataclass(frozen=True)
class DQMetricResult:
    identity: DatasetIdentity
    metric: str
    observed_value: float | str | dict[str, int] | None
    observations: int | None
    policy_id: str
    calibration_state: CalibrationState
    threshold: float | None
    comparison: str
    evaluation: MetricEvaluation
    reason: str
    evidence: tuple[str, ...]
    eligibility_impact: FailureImpact

    def to_dict(self) -> dict[str, Any]:
        result = asdict(self)
        result["calibration_state"] = self.calibration_state.value
        result["evaluation"] = self.evaluation.value
        result["eligibility_impact"] = self.eligibility_impact.value
        return result


@dataclass(frozen=True)
class DQBaselineProfile:
    profile_version: str
    policy_version: str
    catalog_path: str
    observed_at: str
    metrics: tuple[DQMetricResult, ...]
    eligibility: DQEligibilityStatus
    eligibility_reasons: tuple[str, ...]

    def to_dict(self) -> dict[str, Any]:
        return {
            "profile_version": self.profile_version,
            "policy_version": self.policy_version,
            "catalog_path": self.catalog_path,
            "observed_at": self.observed_at,
            "metrics": [metric.to_dict() for metric in self.metrics],
            "eligibility": self.eligibility.value,
            "eligibility_reasons": list(self.eligibility_reasons),
        }


@dataclass(frozen=True)
class _DatasetSpec:
    dataset_class: str
    view: str
    source_column: str
    contract_column: str
    unique_key: tuple[str, ...] | None
    side_column: str | None = None


_SPECS = (
    _DatasetSpec(
        "ohlcv",
        "market_ohlcv",
        "source_dataset_id",
        "data_contract_version",
        ("instrument_id", "source_dataset_id", "interval", "open_time"),
    ),
    _DatasetSpec(
        "individual_trade",
        "market_individual_trade",
        "source_dataset_id",
        "data_contract_version",
        ("instrument_id", "source_dataset_id", "native_trade_id"),
        "taker_side",
    ),
    _DatasetSpec(
        "derived_trade_bucket",
        "market_derived_trade_bucket",
        "source_dataset_id",
        "aggregation_version",
        (
            "instrument_id",
            "source_dataset_id",
            "source_parquet_sha256",
            "bucket_seconds",
            "bucket_start",
        ),
    ),
    _DatasetSpec(
        "funding_rate",
        "market_funding_rate",
        "source",
        "source_contract_version",
        ("exchange", "instrument_id", "source_contract_version", "funding_time"),
    ),
    _DatasetSpec(
        "open_interest",
        "market_open_interest",
        "source",
        "source_contract_version",
        ("exchange", "instrument_id", "source_contract_version", "observation_time"),
    ),
    _DatasetSpec(
        "liquidations",
        "market_liquidations",
        "source",
        "source_contract_version",
        None,
        "source_side",
    ),
)


def load_threshold_policies(path: Path) -> tuple[str, dict[str, ThresholdPolicy]]:
    payload = yaml.safe_load(path.read_text(encoding="utf-8"))
    if not isinstance(payload, dict) or not isinstance(payload.get("policies"), dict):
        raise ValueError("DQ policy file must contain a policies mapping")
    version = str(payload.get("policy_version", "")).strip()
    if not version:
        raise ValueError("DQ policy_version must be explicit")
    policies: dict[str, ThresholdPolicy] = {}
    for policy_id, raw in payload["policies"].items():
        if not isinstance(raw, dict):
            raise ValueError(f"DQ policy {policy_id} must be a mapping")
        state = CalibrationState(str(raw["calibration_state"]))
        threshold = raw.get("threshold")
        comparison = str(raw.get("comparison", "NONE"))
        if state == CalibrationState.CALIBRATED and comparison in {"MAXIMUM", "MINIMUM"}:
            if threshold is None:
                raise ValueError(f"Calibrated numeric policy {policy_id} requires a threshold")
        elif state != CalibrationState.CALIBRATED and threshold is not None:
            raise ValueError(f"Uncalibrated/not-applicable policy {policy_id} cannot set a threshold")
        policies[str(policy_id)] = ThresholdPolicy(
            policy_id=str(policy_id),
            calibration_state=state,
            comparison=comparison,
            threshold=float(threshold) if threshold is not None else None,
            failure_impact=FailureImpact(str(raw.get("failure_impact", "NONE"))),
            evidence=tuple(str(item) for item in raw.get("evidence", [])),
        )
    return version, policies


def evaluate_metric(
    *,
    identity: DatasetIdentity,
    metric: str,
    observed_value: float | str | dict[str, int] | None,
    observations: int | None,
    policy: ThresholdPolicy,
    reason: str,
) -> DQMetricResult:
    if policy.calibration_state == CalibrationState.NOT_APPLICABLE:
        evaluation = MetricEvaluation.NOT_APPLICABLE
        impact = FailureImpact.NONE
    elif policy.calibration_state == CalibrationState.UNCALIBRATED or observed_value is None:
        evaluation = MetricEvaluation.UNKNOWN
        impact = policy.failure_impact
    elif policy.comparison == "MAXIMUM":
        evaluation = (
            MetricEvaluation.PASS
            if float(observed_value) <= float(policy.threshold)
            else MetricEvaluation.FAIL
        )
        impact = policy.failure_impact if evaluation == MetricEvaluation.FAIL else FailureImpact.NONE
    elif policy.comparison == "MINIMUM":
        evaluation = (
            MetricEvaluation.PASS
            if float(observed_value) >= float(policy.threshold)
            else MetricEvaluation.FAIL
        )
        impact = policy.failure_impact if evaluation == MetricEvaluation.FAIL else FailureImpact.NONE
    elif policy.comparison == "CATEGORICAL":
        evaluation = MetricEvaluation.PASS
        impact = FailureImpact.NONE
    else:
        raise ValueError(f"Unsupported calibrated comparison: {policy.comparison}")
    return DQMetricResult(
        identity=identity,
        metric=metric,
        observed_value=observed_value,
        observations=observations,
        policy_id=policy.policy_id,
        calibration_state=policy.calibration_state,
        threshold=policy.threshold,
        comparison=policy.comparison,
        evaluation=evaluation,
        reason=reason,
        evidence=policy.evidence,
        eligibility_impact=impact,
    )


def _identity_expressions(spec: _DatasetSpec) -> tuple[str, ...]:
    return (
        f"'{spec.dataset_class}'",
        "coalesce(cast(exchange as varchar), 'UNKNOWN')",
        "coalesce(cast(market_type as varchar), 'UNKNOWN')",
        "coalesce(cast(instrument_id as varchar), 'UNKNOWN')",
        f"coalesce(cast({spec.source_column} as varchar), 'UNKNOWN')",
        f"coalesce(cast({spec.contract_column} as varchar), 'UNKNOWN')",
    )


def _metric_groups(
    connection: duckdb.DuckDBPyConnection, spec: _DatasetSpec
) -> list[tuple[Any, ...]]:
    identities = _identity_expressions(spec)
    key_sql = ", ".join(spec.unique_key or ("filename",))
    side_sql = (
        f", sum(case when {spec.side_column} is null or upper(cast({spec.side_column} as varchar)) "
        "not in ('BUY','SELL') then 1 else 0 end)"
        if spec.side_column
        else ", cast(null as hugeint)"
    )
    query = (
        "select "
        + ", ".join(identities)
        + f", count(*), count(*) - count(distinct ({key_sql}))"
        + side_sql
        + f" from {spec.view} group by "
        + ", ".join(identities)
        + " order by 1,2,3,4,5,6"
    )
    return connection.execute(query).fetchall()


def _gap_result(
    root: Path,
    identity: DatasetIdentity,
    policy: ThresholdPolicy,
) -> DQMetricResult:
    summary = summarize_gap_exposure(
        root,
        exchange=identity.exchange,
        market_type=identity.market_type,
        instrument_id=identity.instrument_id,
        dataset_class=identity.dataset_class,
    )
    counts = {status.value: summary.by_status[status] for status in GapStatus}
    if counts[GapStatus.UNRECOVERABLE.value]:
        evaluation = MetricEvaluation.FAIL
        impact = FailureImpact.UNAVAILABLE
        reason = "unrecoverable_gap_present"
    elif any(counts[value] for value in ("OPEN", "PARTIAL", "UNKNOWN")):
        evaluation = MetricEvaluation.FAIL
        impact = FailureImpact.DEGRADE
        reason = "unresolved_or_uncertain_gap_present"
    else:
        evaluation = MetricEvaluation.PASS
        impact = FailureImpact.NONE
        reason = "no_unresolved_gap_for_identity"
    return DQMetricResult(
        identity=identity,
        metric="gap_exposure",
        observed_value=counts,
        observations=summary.total,
        policy_id=policy.policy_id,
        calibration_state=policy.calibration_state,
        threshold=None,
        comparison=policy.comparison,
        evaluation=evaluation,
        reason=reason,
        evidence=policy.evidence,
        eligibility_impact=impact,
    )


def _latest_liquidation_soak(data_root: Path) -> dict[str, Any] | None:
    directory = data_root / "control" / "ingestion_runs" / "liquidation_soak" / "v1"
    candidates = sorted(directory.glob("liq_soak_*.json")) if directory.exists() else []
    if not candidates:
        return None
    payload = json.loads(candidates[-1].read_text(encoding="utf-8"))
    return payload if isinstance(payload, dict) else None


def _liquidation_operational_metrics(
    *,
    data_root: Path,
    policies: dict[str, ThresholdPolicy],
) -> list[DQMetricResult]:
    payload = _latest_liquidation_soak(data_root)
    if not payload or not isinstance(payload.get("stream_results"), dict):
        return []
    results: list[DQMetricResult] = []
    for stream_key, raw in sorted(payload["stream_results"].items()):
        if not isinstance(raw, dict):
            continue
        identity = DatasetIdentity(
            "liquidations",
            str(raw.get("exchange", "UNKNOWN")),
            "perpetual",
            str(raw.get("instrument_id", "UNKNOWN")),
            str(raw.get("source_dataset_id", "UNKNOWN")),
            str(raw.get("source_contract_version", "UNKNOWN")),
        )
        quiet = raw.get("event_observation_status") == "NO_EVENT_OBSERVED_WITHIN_WINDOW"
        transport_pass = raw.get("transport_status") == "PASS"
        freshness_value = "LOW_ACTIVITY_QUIET" if quiet and transport_pass else "UNKNOWN"
        results.append(
            evaluate_metric(
                identity=identity,
                metric="freshness_status",
                observed_value=freshness_value,
                observations=int(raw.get("source_event_count", 0)),
                policy=policies["event_driven_freshness"],
                reason=f"retained soak stream={stream_key}; transport={raw.get('transport_status')}",
            )
        )
        raw_count = int(raw.get("raw_message_count", 0))
        duplicate_count = int(raw.get("duplicate_exact_wire_deliveries", 0))
        results.append(
            evaluate_metric(
                identity=identity,
                metric="duplicate_rate.exact_wire_replay",
                observed_value=duplicate_count / raw_count if raw_count else None,
                observations=raw_count,
                policy=policies["exact_wire_replay"],
                reason=(
                    f"duplicates={duplicate_count}; guarantee=EXACT_WIRE_REPLAY_ONLY; "
                    "cross-envelope economic dedup NOT GUARANTEED"
                ),
            )
        )
        queue_mode = str(raw.get("queue_mode", "UNKNOWN"))
        queue_policy = (
            policies["synchronous_queue"]
            if queue_mode == "NOT_APPLICABLE_SYNCHRONOUS_READ_FLUSH"
            else policies["no_queue_telemetry"]
        )
        for metric_name in ("queue_utilization", "queue_high_watermark", "writer_lag_seconds"):
            results.append(
                evaluate_metric(
                    identity=identity,
                    metric=metric_name,
                    observed_value=None,
                    observations=None,
                    policy=queue_policy,
                    reason=f"retained soak queue_mode={queue_mode}",
                )
            )
        results.append(_gap_result(data_root, identity, policies["gap_status_policy"]))
    return results


def build_dq_baseline(
    *,
    catalog_path: Path,
    data_root: Path,
    policy_path: Path,
    observed_at: datetime,
) -> DQBaselineProfile:
    """Measure active datasets deterministically for a fixed catalog and observation time."""
    if observed_at.tzinfo is None or observed_at.utcoffset() != timedelta(0):
        raise ValueError("DQ observed_at must be timezone-aware UTC")
    policy_version, policies = load_threshold_policies(policy_path)
    metrics: list[DQMetricResult] = []
    connection = duckdb.connect(str(catalog_path), read_only=True)
    try:
        for spec in _SPECS:
            if int(connection.execute(f"select count(*) from {spec.view}").fetchone()[0]) == 0:
                continue
            for row in _metric_groups(connection, spec):
                identity = DatasetIdentity(*(str(value) for value in row[:6]))
                observations, duplicate_count, unknown_side_count = (int(row[6]), int(row[7]), row[8])
                if spec.unique_key is None:
                    duplicate_policy = ThresholdPolicy(
                        "missing_proven_identity_key",
                        CalibrationState.UNCALIBRATED,
                        "NONE",
                        None,
                        FailureImpact.DEGRADE,
                        ("no complete proven identity key is available in this canonical view",),
                    )
                    duplicate_value = None
                else:
                    duplicate_policy = policies["semantic_unique_key"]
                    duplicate_value = duplicate_count / observations if observations else None
                metrics.append(
                    evaluate_metric(
                        identity=identity,
                        metric="duplicate_rate",
                        observed_value=duplicate_value,
                        observations=observations,
                        policy=duplicate_policy,
                        reason=f"duplicates={duplicate_count}; key={spec.unique_key}",
                    )
                )
                if spec.dataset_class != "liquidations":
                    metrics.append(
                        evaluate_metric(
                            identity=identity,
                            metric="freshness_age_seconds",
                            observed_value=None,
                            observations=observations,
                            policy=policies["archive_freshness"],
                            reason="historical immutable dataset",
                        )
                    )
                if spec.side_column:
                    side_policy = policies["required_side"]
                    side_value = int(unknown_side_count) / observations if observations else None
                    metrics.append(
                        evaluate_metric(
                            identity=identity,
                            metric=f"unexpected_unknown_rate.{spec.side_column}",
                            observed_value=side_value,
                            observations=observations,
                            policy=side_policy,
                            reason=f"unknown_or_invalid={int(unknown_side_count)}",
                        )
                    )
                    if spec.dataset_class == "liquidations" and identity.exchange == "binance":
                        metrics.append(
                            evaluate_metric(
                                identity=identity,
                                metric="unknown_rate.position_side_liquidated",
                                observed_value=None,
                                observations=observations,
                                policy=ThresholdPolicy(
                                    "binance_liquidation_position_side_limitation",
                                    CalibrationState.NOT_APPLICABLE,
                                    "NONE",
                                    None,
                                    FailureImpact.NONE,
                                    ("accepted Binance contract cannot infer liquidated position side",),
                                ),
                                reason="contract-permitted UNKNOWN",
                            )
                        )
                if spec.dataset_class != "liquidations":
                    for metric_name in (
                        "queue_utilization",
                        "queue_high_watermark",
                        "writer_lag_seconds",
                    ):
                        metrics.append(
                            evaluate_metric(
                                identity=identity,
                                metric=metric_name,
                                observed_value=None,
                                observations=None,
                                policy=policies["no_queue_telemetry"],
                                reason="retained operational queue distribution unavailable",
                            )
                        )
                    metrics.append(_gap_result(data_root, identity, policies["gap_status_policy"]))
    finally:
        connection.close()
    metrics.extend(_liquidation_operational_metrics(data_root=data_root, policies=policies))
    metrics.sort(key=lambda item: (item.identity, item.metric, item.policy_id))
    hard_reasons = tuple(
        f"{item.identity.dataset_class}:{item.identity.instrument_id}:{item.metric}:{item.reason}"
        for item in metrics
        if item.eligibility_impact == FailureImpact.UNAVAILABLE
    )
    degradation_reasons = tuple(
        f"{item.identity.dataset_class}:{item.identity.instrument_id}:{item.metric}:{item.reason}"
        for item in metrics
        if item.eligibility_impact == FailureImpact.DEGRADE
    )
    eligibility = classify_dq_eligibility(
        hard_fail_reasons=hard_reasons, degradation_reasons=degradation_reasons
    )
    return DQBaselineProfile(
        profile_version="1.0.0",
        policy_version=policy_version,
        catalog_path=str(catalog_path),
        observed_at=observed_at.isoformat(),
        metrics=tuple(metrics),
        eligibility=eligibility.status,
        eligibility_reasons=eligibility.reasons,
    )


def write_dq_profile(profile: DQBaselineProfile, destination: Path) -> Path:
    """Atomically write a stable, machine-readable profile."""
    destination.parent.mkdir(parents=True, exist_ok=True)
    partial = destination.with_name(f".{destination.name}.{uuid.uuid4().hex}.partial")
    try:
        encoded = json.dumps(profile.to_dict(), indent=2, sort_keys=True) + "\n"
        with partial.open("w", encoding="utf-8", newline="\n") as stream:
            stream.write(encoded)
            stream.flush()
            os.fsync(stream.fileno())
        os.replace(partial, destination)
    except BaseException:
        partial.unlink(missing_ok=True)
        raise
    return destination
