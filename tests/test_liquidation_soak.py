"""PHASE 1D.3F bounded soak, local-gap, restart, and reconciliation gates."""

from __future__ import annotations

import asyncio
import json
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import pyarrow.parquet as pq
import pytest

from crypto_quant.ingestion.binance import liquidations as binance_liq
from crypto_quant.ingestion.bybit import liquidations as bybit_liq
from crypto_quant.ingestion.gap_registry import GapRegistry, GapStatus, GapType
from crypto_quant.ingestion.liquidation_soak import (
    LOCAL_GAPPED,
    LOCAL_NO_GAP,
    SoakConfig,
    default_streams,
    reconcile_liquidation_run,
    resource_gate,
    run_liquidation_soak,
)
from crypto_quant.ingestion.reconnect import ReconnectConfig


def _outcome(spec, run_id: str, session_id: str, *, messages: int = 0, events: int = 0):
    now = datetime.now(UTC).isoformat()
    return {
        "symbol": spec.symbol,
        "status": "PASS",
        "transport_status": "PASS",
        "subscription_status": "PASS",
        "heartbeat_liveness": "PASS",
        "event_observation_status": (
            "REAL_EVENT_OBSERVED" if events else "NO_EVENT_OBSERVED_WITHIN_WINDOW"
        ),
        "total_messages_received": messages,
        "total_source_events_observed": events,
        "total_records_persisted": events,
        "started_at": now,
        "connected_at": now,
        "subscribed_at": now,
        "ended_at": now,
        "termination_reason": "BOUNDED_SOAK_COMPLETED",
        "ingestion_run_id": run_id,
        "session_id": session_id,
        "dropped_messages": 0,
    }


def _config(attempts: int = 2) -> SoakConfig:
    return SoakConfig(
        duration_seconds=2,
        flush_interval_seconds=0.1,
        min_disk_free_gb=0,
        reconnect=ReconnectConfig(
            initial_delay_sec=0.01,
            max_delay_sec=0.01,
            jitter_ratio=0,
            max_attempts=attempts,
        ),
    )


def test_default_streams_have_four_isolated_canonical_identities():
    streams = default_streams()
    assert {(item.exchange, item.symbol) for item in streams} == {
        ("bybit", "BTCUSDT"),
        ("bybit", "ETHUSDT"),
        ("binance", "BTCUSDT"),
        ("binance", "ETHUSDT"),
    }
    assert len({item.instrument_id for item in streams}) == 4
    assert len({item.key for item in streams}) == 4


def test_quiet_bounded_run_is_not_a_gap_or_failure(tmp_path: Path):
    async def quiet(spec, root, duration, flush, disk, run_id, session_id):
        return _outcome(spec, run_id, session_id)

    report = asyncio.run(
        run_liquidation_soak(
            tmp_path,
            config=_config(),
            streams=default_streams()[:1],
            attempt_runner=quiet,
            run_id="liq_soak_quiet",
        )
    )
    result = report["stream_results"]["bybit:BTCUSDT"]
    assert report["status"] == "PASS"
    assert result["event_observation_status"] == "NO_EVENT_OBSERVED_WITHIN_WINDOW"
    assert result["local_capture_status"] == LOCAL_NO_GAP
    assert result["termination_reason"] == "BOUNDED_SOAK_COMPLETED"
    assert GapRegistry(tmp_path).list_gaps() == []


def test_disconnect_reconnect_records_unrecoverable_local_gap(tmp_path: Path):
    calls = 0

    async def disconnect_once(spec, root, duration, flush, disk, run_id, session_id):
        nonlocal calls
        calls += 1
        if calls == 1:
            raise ConnectionError("controlled websocket disconnect")
        return _outcome(spec, run_id, session_id, messages=1, events=2)

    report = asyncio.run(
        run_liquidation_soak(
            tmp_path,
            config=_config(),
            streams=default_streams()[:1],
            attempt_runner=disconnect_once,
            run_id="liq_soak_reconnect",
        )
    )
    result = report["stream_results"]["bybit:BTCUSDT"]
    assert result["connection_attempts"] == 2
    assert result["disconnects"] == result["reconnects"] == 1
    assert result["local_capture_status"] == LOCAL_GAPPED
    gaps = GapRegistry(tmp_path).list_gaps()
    assert len(gaps) == 1
    assert gaps[0].gap_type == GapType.LOCAL_COLLECTOR_GAP
    assert gaps[0].status == GapStatus.UNRECOVERABLE
    assert gaps[0].coverage_proven is False
    assert gaps[0].recovery_attempted is False
    assert gaps[0].session_before != gaps[0].session_after


def test_wrong_symbol_is_machine_readable_incident_not_interval_gap(tmp_path: Path):
    calls = 0

    async def wrong_then_ok(spec, root, duration, flush, disk, run_id, session_id):
        nonlocal calls
        calls += 1
        if calls == 1:
            raise ValueError("Symbol mismatch for requested stream")
        return _outcome(spec, run_id, session_id)

    report = asyncio.run(
        run_liquidation_soak(
            tmp_path,
            config=_config(),
            streams=default_streams()[:1],
            attempt_runner=wrong_then_ok,
            run_id="liq_soak_wrong_symbol",
        )
    )
    result = report["stream_results"]["bybit:BTCUSDT"]
    assert result["wrong_symbol_rejects"] == result["parser_rejects"] == 1
    assert result["local_capture_status"] == LOCAL_NO_GAP
    assert GapRegistry(tmp_path).list_gaps() == []
    incident = json.loads(
        (tmp_path / "control" / "dq" / "liquidations" / "v1" / "incidents.jsonl").read_text(
            encoding="utf-8"
        )
    )
    assert incident["reason_code"] == "WRONG_SYMBOL"
    assert incident["creates_interval_gap"] is False


def test_one_stream_failure_isolated_from_other_three(tmp_path: Path):
    async def isolated(spec, root, duration, flush, disk, run_id, session_id):
        if spec.exchange == "binance" and spec.symbol == "ETHUSDT":
            raise OSError("controlled network failure")
        return _outcome(spec, run_id, session_id)

    report = asyncio.run(
        run_liquidation_soak(
            tmp_path,
            config=_config(attempts=1),
            attempt_runner=isolated,
            run_id="liq_soak_isolation",
        )
    )
    assert report["status"] == "PARTIAL_SOAK"
    assert report["stream_results"]["binance:ETHUSDT"]["transport_status"] == "FAIL"
    for key in ("bybit:BTCUSDT", "bybit:ETHUSDT", "binance:BTCUSDT"):
        assert report["stream_results"][key]["transport_status"] == "PASS"


def test_confirmed_drop_is_first_class_unrecoverable_local_gap(tmp_path: Path):
    async def dropped(spec, root, duration, flush, disk, run_id, session_id):
        outcome = _outcome(spec, run_id, session_id, messages=3, events=2)
        outcome["dropped_messages"] = 1
        return outcome

    report = asyncio.run(
        run_liquidation_soak(
            tmp_path,
            config=_config(),
            streams=default_streams()[1:2],
            attempt_runner=dropped,
            run_id="liq_soak_drop",
        )
    )
    result = report["stream_results"]["binance:BTCUSDT"]
    assert result["local_capture_status"] == LOCAL_GAPPED
    gaps = GapRegistry(tmp_path).list_gaps()
    assert len(gaps) == 1 and gaps[0].status == GapStatus.UNRECOVERABLE
    assert gaps[0].evidence["dropped_messages"] == 1


def test_resource_gate_is_readable_and_non_synthetic(tmp_path: Path):
    gate = resource_gate(tmp_path, min_disk_free_gb=0)
    assert gate["status"] == "PASS"
    assert gate["free_disk_bytes"] > 0
    assert gate["logical_cpu_count"]
    assert gate["cpu_baseline"] == "NOT_MEASURED"


def _bybit_message() -> tuple[dict[str, Any], str]:
    payload = {
        "topic": "allLiquidation.BTCUSDT",
        "type": "snapshot",
        "ts": 1786434825553,
        "data": [
            {"T": 1786434825501, "s": "BTCUSDT", "S": "Buy", "v": "0.01", "p": "60000"},
            {"T": 1786434825502, "s": "BTCUSDT", "S": "Sell", "v": "0.02", "p": "60001"},
        ],
    }
    return payload, json.dumps(payload, separators=(",", ":"))


def test_raw_survives_normalization_failure_and_retry_is_idempotent(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
):
    payload, wire = _bybit_message()
    original = bybit_liq.merge_and_write_liquidation_parquet

    def fail_normalization(*args, **kwargs):
        raise RuntimeError("controlled normalization failure")

    monkeypatch.setattr(bybit_liq, "merge_and_write_liquidation_parquet", fail_normalization)
    with pytest.raises(RuntimeError, match="controlled normalization"):
        bybit_liq.persist_bybit_liquidation_batch(
            [(payload, wire)], "BTCUSDT", tmp_path, received_at=datetime.now(UTC)
        )
    assert len(list((tmp_path / "raw").rglob("*.jsonl"))) == 1
    assert not (tmp_path / "control" / "checkpoints").exists()
    monkeypatch.setattr(bybit_liq, "merge_and_write_liquidation_parquet", original)
    first = bybit_liq.persist_bybit_liquidation_batch(
        [(payload, wire)], "BTCUSDT", tmp_path, received_at=datetime.now(UTC)
    )
    replay = bybit_liq.persist_bybit_liquidation_batch(
        [(payload, wire)], "BTCUSDT", tmp_path, received_at=datetime.now(UTC)
    )
    assert first["total_accumulated_rows"] == replay["total_accumulated_rows"] == 2


@pytest.mark.parametrize("venue", ["bybit", "binance"])
def test_received_rejected_wire_is_durably_quarantined(tmp_path: Path, venue: str):
    payload, wire = _bybit_message()
    error = ValueError("controlled parser rejection")
    if venue == "bybit":
        target = bybit_liq._quarantine_failed_live_buffer(
            tmp_path, "BTCUSDT", [(payload, wire)], error
        )
    else:
        target = binance_liq._quarantine_failed_live_buffer(
            tmp_path, "BTCUSDT", [(payload, wire, datetime.now(UTC))], error
        )
    assert target.read_text(encoding="utf-8") == wire + "\n"
    reason = json.loads(target.with_suffix(".reason.json").read_text(encoding="utf-8"))
    assert reason["raw_message_count"] == 1
    assert reason["error_type"] == "ValueError"
    assert reason["reason_code"] == "PERSISTENCE_OR_NORMALIZATION_REJECTED"


def test_exact_replay_across_sessions_dedups_but_different_envelope_survives(tmp_path: Path):
    payload, wire = _bybit_message()
    first = bybit_liq.persist_bybit_liquidation_batch(
        [(payload, wire)],
        "BTCUSDT",
        tmp_path,
        received_at=datetime.now(UTC),
        ingestion_run_id="run",
        session_id="before_disconnect",
    )
    replay = bybit_liq.persist_bybit_liquidation_batch(
        [(payload, wire)],
        "BTCUSDT",
        tmp_path,
        received_at=datetime.now(UTC),
        ingestion_run_id="run",
        session_id="after_reconnect",
    )
    changed = dict(payload)
    changed["ts"] += 1
    changed_wire = json.dumps(changed, separators=(",", ":"))
    second = bybit_liq.persist_bybit_liquidation_batch(
        [(changed, changed_wire)],
        "BTCUSDT",
        tmp_path,
        received_at=datetime.now(UTC),
        ingestion_run_id="run",
        session_id="after_reconnect",
    )
    assert first["new_rows_persisted"] == 2
    assert replay["new_rows_persisted"] == 0
    assert replay["total_accumulated_rows"] == 2
    assert second["new_rows_persisted"] == 2
    assert second["total_accumulated_rows"] == 4


def test_normalized_before_checkpoint_failure_is_retry_safe(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
):
    payload = json.loads(
        Path("tests/fixtures/binance/ws_force_order_usdm_btc_sell.json").read_text(encoding="utf-8")
    )
    wire = json.dumps(payload, separators=(",", ":"))
    original = binance_liq._write_json_atomic

    def fail_checkpoint(path: Path, content: dict[str, Any]) -> None:
        raise RuntimeError("controlled checkpoint failure")

    monkeypatch.setattr(binance_liq, "_write_json_atomic", fail_checkpoint)
    with pytest.raises(RuntimeError, match="controlled checkpoint"):
        binance_liq.persist_binance_liquidation_batch(
            [(payload, wire, datetime.now(UTC))], tmp_path
        )
    assert len(list((tmp_path / "raw").rglob("*.jsonl"))) == 1
    parquet = list((tmp_path / "normalized").rglob("*.parquet"))
    assert len(parquet) == 1 and pq.ParquetFile(parquet[0]).metadata.num_rows == 1
    assert not (tmp_path / "control" / "checkpoints").exists()
    monkeypatch.setattr(binance_liq, "_write_json_atomic", original)
    result = binance_liq.persist_binance_liquidation_batch(
        [(payload, wire, datetime.now(UTC))], tmp_path
    )
    assert result["total_accumulated_rows"] == 1
    assert len(list((tmp_path / "normalized").rglob("*.parquet"))) == 1


def test_run_manifest_reconciliation_verifies_refs_and_hashes(tmp_path: Path):
    payload, wire = _bybit_message()
    bybit_liq.persist_bybit_liquidation_batch(
        [(payload, wire)],
        "BTCUSDT",
        tmp_path,
        received_at=datetime.now(UTC),
        ingestion_run_id="liq_soak_reconcile",
        session_id="session_one",
    )
    result = reconcile_liquidation_run(tmp_path, "liq_soak_reconcile")
    assert result["status"] == "PASS"
    assert result["manifest_records"] == 1
    assert result["raw_message_count"] == 1
    assert result["expected_canonical_observations"] == 2
    assert result["broken_refs"] == []
