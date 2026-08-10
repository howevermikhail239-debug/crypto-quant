from datetime import UTC, datetime, timedelta, timezone

import pytest

from crypto_quant.contracts import (
    Checkpoint,
    DeletionRecord,
    GapKind,
    GapRecord,
    KnowledgeRecord,
    ManifestAction,
    ManifestEvent,
)
from crypto_quant.hashing import stable_id
from crypto_quant.identity import InstrumentIdentity
from crypto_quant.time import knowledge_available, parse_epoch, require_utc
from crypto_quant.versioning import require_compatible_major


def test_identity_distinguishes_spot_and_perpetual() -> None:
    common = dict(
        exchange="binance",
        native_symbol="BTCUSDT",
        base_asset="BTC",
        quote_asset="USDT",
        quantity_unit="BTC",
        notional_unit="USDT",
    )
    spot = InstrumentIdentity(**common, market_type="spot", contract_type="spot")
    perpetual = InstrumentIdentity(
        **common,
        market_type="derivative",
        contract_type="linear_perpetual",
        settle_asset="USDT",
    )
    assert spot.instrument_id != perpetual.instrument_id
    assert stable_id("x", {"b": 2, "a": 1}) == stable_id("x", {"a": 1, "b": 2})


def test_knowledge_time_rejects_future_observation() -> None:
    now = datetime.now(UTC)
    record = KnowledgeRecord(
        event_time=now - timedelta(seconds=2),
        knowledge_time=now,
        knowledge_time_basis="received_at",
    )
    assert record.eligible_at(now)
    assert not knowledge_available(
        knowledge_time=now,
        decision_time=now - timedelta(microseconds=1),
    )


def test_version_major_must_match() -> None:
    require_compatible_major("1.3.0", "1.0.0")
    with pytest.raises(ValueError):
        require_compatible_major("1.0.0", "2.0.0")


def test_time_normalizes_aware_timestamp_and_requires_epoch_unit() -> None:
    aware = datetime(2026, 1, 1, tzinfo=timezone(timedelta(hours=3)))
    assert require_utc(aware) == datetime(2025, 12, 31, 21, tzinfo=UTC)
    assert parse_epoch(1_000, unit="ms") == datetime(1970, 1, 1, 0, 0, 1, tzinfo=UTC)
    assert parse_epoch(1_000_000, unit="us") == datetime(1970, 1, 1, 0, 0, 1, tzinfo=UTC)
    with pytest.raises(ValueError):
        require_utc(datetime(2026, 1, 1))


def test_identity_normalizes_natural_key_and_excludes_mutable_rules() -> None:
    base = dict(
        exchange=" Binance ",
        native_symbol=" btcusdt ",
        market_type=" SPOT ",
        contract_type=" spot ",
        base_asset=" btc ",
        quote_asset=" usdt ",
        quantity_unit="BTC",
        notional_unit="USDT",
    )
    left = InstrumentIdentity(**base, price_tick="0.01")
    right = InstrumentIdentity(**base, price_tick="1.00", quantity_step="0.5")
    assert left.instrument_id == right.instrument_id
    assert left.native_symbol == "BTCUSDT"
    with pytest.raises(ValueError):
        InstrumentIdentity(**(base | {"market_type": "option"}))


def test_control_contract_temporal_invariants() -> None:
    now = datetime.now(UTC)
    with pytest.raises(ValueError):
        ManifestEvent(
            event_id="a",
            action=ManifestAction.INGESTED,
            object_id="b",
            occurred_at=now,
            coverage_start=now,
            coverage_end=now - timedelta(seconds=1),
            schema_version="1.0.0",
            collector_version="0.0.0",
            normalization_version="0.0.0",
        )
    with pytest.raises(ValueError):
        Checkpoint(
            checkpoint_id="a",
            source_dataset_id="b",
            instrument_id="c",
            committed_at=now,
            last_knowledge_time=now + timedelta(seconds=1),
        )
    with pytest.raises(ValueError):
        GapRecord(
            gap_id="a",
            source_dataset_id="b",
            instrument_id="c",
            kind=GapKind.UNKNOWN_GAP,
            started_at=now,
            ended_at=now,
            detected_at=now,
            status="open",
        )
    with pytest.raises(ValueError):
        DeletionRecord(
            deletion_id="a", object_id="b", planned_at=now, deleted_at=now, reason="retention"
        )
