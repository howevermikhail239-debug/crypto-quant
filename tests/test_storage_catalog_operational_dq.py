from __future__ import annotations

import hashlib
import json
from datetime import UTC, datetime, timedelta
from pathlib import Path
from types import SimpleNamespace

import duckdb
import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from crypto_quant.ingestion.gap_registry import GapRegistry, GapStatus, GapType
from crypto_quant.ingestion.health import (
    DQEligibilityStatus,
    FeedActivityMode,
    FreshnessStatus,
    SideExpectation,
    classify_dq_eligibility,
    measure_duplicate_rate,
    measure_freshness,
    measure_queue_writer,
    measure_unknown_side_rate,
    summarize_gap_exposure,
)
from crypto_quant.storage.catalog import build_catalog, resolve_active_artifacts


def _write_parquet(root: Path, relative: str, **columns: list[object]) -> tuple[Path, str]:
    path = root / relative
    path.parent.mkdir(parents=True, exist_ok=True)
    pq.write_table(pa.table(columns), path)
    return path, hashlib.sha256(path.read_bytes()).hexdigest()


def _append_manifest(root: Path, name: str, row: dict[str, object]) -> None:
    path = root / "control" / "manifests" / name
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a", encoding="utf-8") as stream:
        stream.write(json.dumps(row, sort_keys=True) + "\n")


def test_catalog_exposes_only_active_immutable_generations(tmp_path: Path) -> None:
    trade_btc, trade_btc_hash = _write_parquet(
        tmp_path,
        "normalized/binance/spot/BTCUSDT/trades.parquet",
        instrument_id=["btc"],
        source_dataset_id=["binance.spot.trade"],
        native_trade_id=["1"],
    )
    trade_eth, trade_eth_hash = _write_parquet(
        tmp_path,
        "normalized/binance/spot/ETHUSDT/trades.parquet",
        instrument_id=["eth"],
        source_dataset_id=["binance.spot.trade"],
        native_trade_id=["2"],
    )
    oi_old, oi_old_hash = _write_parquet(
        tmp_path,
        "normalized/bybit/perpetual/BTCUSDT/oi-old.parquet",
        instrument_id=["btc-perp"],
        source_dataset_id=["bybit.linear.oi"],
        open_interest=[1],
    )
    oi_new, oi_new_hash = _write_parquet(
        tmp_path,
        "normalized/bybit/perpetual/BTCUSDT/oi-new.parquet",
        instrument_id=["btc-perp"],
        source_dataset_id=["bybit.linear.oi"],
        open_interest=[2],
    )
    common = {"exchange": "bybit", "market_type": "perpetual", "symbol": "BTCUSDT"}
    for path, digest, instrument in (
        (trade_btc, trade_btc_hash, "btc"),
        (trade_eth, trade_eth_hash, "eth"),
    ):
        _append_manifest(
            tmp_path,
            "trades.jsonl",
            {
                "action": "NORMALIZED",
                "dataset_class": "individual_trade",
                "object_id": path.relative_to(tmp_path).as_posix(),
                "parquet_sha256": digest,
                "exchange": "binance",
                "market_type": "spot",
                "instrument_id": instrument,
                "source_dataset_id": "binance.spot.trade",
            },
        )
    for path, digest in ((oi_old, oi_old_hash), (oi_new, oi_new_hash)):
        _append_manifest(
            tmp_path,
            "oi.jsonl",
            {
                **common,
                "action": "NORMALIZED",
                "dataset_class": "open_interest",
                "object_id": path.relative_to(tmp_path).as_posix(),
                "parquet_sha256": digest,
                "source_dataset_id": "bybit.linear.oi",
                "instrument_id": "btc-perp",
                "period": "bootstrap",
            },
        )
    metadata = tmp_path / "control" / "instrument_metadata" / "btc.json"
    metadata.parent.mkdir(parents=True)
    metadata.write_text(
        json.dumps({"dataset_id": "bybit.metadata", "symbol": "BTCUSDT"}),
        encoding="utf-8",
    )
    before = {path: hashlib.sha256(path.read_bytes()).hexdigest() for path in (trade_btc, trade_eth, oi_old, oi_new)}

    result = build_catalog(tmp_path)

    assert result.view_row_counts["individual_trade"] == 2
    assert result.view_row_counts["open_interest"] == 1
    assert result.view_row_counts["exchange_aggregate_trade"] == 0
    assert result.metadata_snapshot_count == 1
    assert oi_old.relative_to(tmp_path).as_posix() not in {item.relative_path for item in result.active_artifacts}
    connection = duckdb.connect(str(result.catalog_path), read_only=True)
    try:
        assert connection.execute("SELECT open_interest FROM market_open_interest").fetchone() == (2,)
        assert connection.execute("SELECT count(*) FROM duckdb_tables() WHERE internal=false").fetchone() == (0,)
        assert connection.execute("SELECT count(*) FROM instrument_metadata_snapshots").fetchone() == (1,)
    finally:
        connection.close()
    assert before == {path: hashlib.sha256(path.read_bytes()).hexdigest() for path in before}


def test_catalog_explicit_supersession_and_escape_fail_closed(tmp_path: Path) -> None:
    active, digest = _write_parquet(tmp_path, "normalized/active.parquet", value=[1])
    _append_manifest(
        tmp_path,
        "data.jsonl",
        {"dataset_class": "ohlcv", "object_id": active.relative_to(tmp_path).as_posix(), "parquet_sha256": digest},
    )
    _append_manifest(
        tmp_path,
        "data.jsonl",
        {"dataset_class": "ohlcv", "action": "SUPERSEDED", "object_id": active.relative_to(tmp_path).as_posix()},
    )
    assert resolve_active_artifacts(tmp_path) == ()
    _append_manifest(
        tmp_path,
        "escape.jsonl",
        {"dataset_class": "ohlcv", "object_id": "../outside.parquet"},
    )
    with pytest.raises(ValueError, match="escapes data root"):
        resolve_active_artifacts(tmp_path)


def test_catalog_rejects_active_checksum_mismatch(tmp_path: Path) -> None:
    active, _digest = _write_parquet(tmp_path, "normalized/active.parquet", value=[1])
    _append_manifest(
        tmp_path,
        "data.jsonl",
        {
            "dataset_class": "ohlcv",
            "object_id": active.relative_to(tmp_path).as_posix(),
            "parquet_sha256": "0" * 64,
        },
    )
    with pytest.raises(ValueError, match="checksum mismatch"):
        resolve_active_artifacts(tmp_path)


def test_catalog_uses_latest_manifest_record_for_same_object(tmp_path: Path) -> None:
    active, digest = _write_parquet(tmp_path, "normalized/active.parquet", value=[1])
    reference = active.relative_to(tmp_path).as_posix()
    _append_manifest(
        tmp_path,
        "data.jsonl",
        {"dataset_class": "individual_trade", "object_id": reference, "parquet_sha256": "0" * 64},
    )
    _append_manifest(
        tmp_path,
        "data.jsonl",
        {"dataset_class": "individual_trade", "object_id": reference, "parquet_sha256": digest},
    )
    artifacts = resolve_active_artifacts(tmp_path)
    assert len(artifacts) == 1
    assert artifacts[0].parquet_sha256 == digest
    assert artifacts[0].manifest_record_number == 2


def test_policy_aware_freshness_does_not_invent_event_gap() -> None:
    now = datetime(2026, 8, 12, tzinfo=UTC)
    quiet = measure_freshness(
        last_event_time=None,
        observed_at=now,
        feed_mode=FeedActivityMode.EVENT_DRIVEN,
        policy_name="liquidation-event-driven-v1",
    )
    assert quiet.status == FreshnessStatus.LOW_ACTIVITY_QUIET
    unknown = measure_freshness(
        last_event_time=now - timedelta(seconds=10),
        observed_at=now,
        feed_mode=FeedActivityMode.SCHEDULED,
        policy_name="unfrozen-policy",
    )
    assert unknown.status == FreshnessStatus.UNKNOWN
    stale = measure_freshness(
        last_event_time=now - timedelta(seconds=11),
        observed_at=now,
        feed_mode=FeedActivityMode.SCHEDULED,
        policy_name="explicit-10s-v1",
        stale_after_seconds=10,
    )
    assert stale.status == FreshnessStatus.STALE_BY_POLICY


def test_source_semantic_rates_queue_and_eligibility() -> None:
    duplicates = measure_duplicate_rate(
        observations=10, duplicate_count=1, identity_rule="native_trade_id:v1"
    )
    assert duplicates.rate == 0.1
    with pytest.raises(ValueError, match="identity_rule"):
        measure_duplicate_rate(observations=1, duplicate_count=0, identity_rule="")
    unavailable_side = measure_unknown_side_rate(
        observations=10,
        unknown_count=10,
        expectation=SideExpectation.SOURCE_LIMITATION,
        field_name="position_side_liquidated",
    )
    assert unavailable_side.applicable is False and unavailable_side.rate is None
    required_side = measure_unknown_side_rate(
        observations=10,
        unknown_count=2,
        expectation=SideExpectation.REQUIRED,
        field_name="taker_side",
    )
    assert required_side.rate == 0.2
    queue = measure_queue_writer(
        SimpleNamespace(capacity=100, current_size=25, utilization=0.25, high_watermark=70, writer_lag_sec=1.5)
    )
    assert queue.applicable and queue.high_watermark == 70
    assert classify_dq_eligibility(hard_fail_reasons=("missing_lineage",)).status == DQEligibilityStatus.UNAVAILABLE
    assert classify_dq_eligibility(degradation_reasons=("stale",)).status == DQEligibilityStatus.DEGRADED
    assert classify_dq_eligibility().status == DQEligibilityStatus.USABLE


def test_gap_summary_uses_existing_registry_latest_states(tmp_path: Path) -> None:
    registry = GapRegistry(tmp_path)
    now = datetime(2026, 8, 12, tzinfo=UTC)
    recovered = registry.register_gap(
        exchange="binance",
        market_type="perpetual",
        instrument_id="btc",
        dataset_class="liquidations",
        source_stream="forceOrder",
        gap_start=now,
        gap_end=now + timedelta(seconds=1),
        gap_type=GapType.LOCAL_COLLECTOR_GAP,
    )
    recovered.status = GapStatus.RECOVERED
    registry.update_gap(recovered)
    registry.register_gap(
        exchange="bybit",
        market_type="perpetual",
        instrument_id="eth",
        dataset_class="liquidations",
        source_stream="allLiquidation",
        gap_start=now,
        gap_end=now + timedelta(seconds=1),
        gap_type=GapType.SOURCE_GAP,
    )
    summary = summarize_gap_exposure(tmp_path, dataset_class="liquidations")
    assert summary.total == 2
    assert summary.by_status[GapStatus.RECOVERED] == 1
    assert summary.by_status[GapStatus.OPEN] == 1
    assert summary.by_type[GapType.SOURCE_GAP] == 1
