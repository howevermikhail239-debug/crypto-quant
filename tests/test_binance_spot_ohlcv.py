from __future__ import annotations

import hashlib
import io
import json
import zipfile
from datetime import UTC, datetime
from pathlib import Path

import pyarrow.parquet as pq
import pytest
import yaml

from crypto_quant.ingestion.binance.spot_ohlcv import (
    ARROW_SCHEMA,
    USDM,
    archive_epoch_unit,
    archive_rows,
    binance_identity,
    btcusdt_spot_identity,
    commit_month,
    commit_rest_tail,
    commit_rest_tail_v2,
    fetch_instrument_metadata,
    fetch_rest_final,
    normalize_kline,
    reconcile_overlap,
    recover_stale_partials,
    save_metadata_snapshot,
)


def _zip_fixture() -> bytes:
    fixture = Path(__file__).parent / "fixtures" / "binance" / "spot_btcusdt_1m.csv"
    stream = io.BytesIO()
    with zipfile.ZipFile(stream, "w", zipfile.ZIP_DEFLATED) as archive:
        archive.writestr("BTCUSDT-1m-2024-01.csv", fixture.read_bytes())
    return stream.getvalue()


def test_fixture_contract_normalizes_exact_binance_tuple() -> None:
    row = archive_rows(_zip_fixture())[0]
    normalized = normalize_kline(
        row,
        identity=btcusdt_spot_identity(),
        source_method="fixture",
        source_uri="fixture://binance",
        raw_object_ref="fixture.csv",
        epoch_unit="ms",
        source_sha256="a" * 64,
        retrieved_at=datetime(2024, 1, 1, tzinfo=UTC),
    )
    assert normalized["close_time"].isoformat() == "2024-01-01T00:00:59.999000+00:00"
    assert normalized["knowledge_time"] is None
    assert normalized["knowledge_time_basis"] == "unknown_historical"


@pytest.mark.parametrize("field,value", [(0, "1704067200001"), (6, "1704067260000")])
def test_fixture_gate_rejects_invalid_boundaries(field: int, value: str) -> None:
    row = archive_rows(_zip_fixture())[0]
    row[field] = value
    with pytest.raises(ValueError, match="boundaries"):
        normalize_kline(
            row,
            identity=btcusdt_spot_identity(),
            source_method="fixture",
            source_uri="fixture://binance",
            raw_object_ref="fixture.csv",
            epoch_unit="ms",
            source_sha256="a" * 64,
            retrieved_at=datetime(2024, 1, 1, tzinfo=UTC),
        )


def test_commit_is_atomic_and_idempotent(tmp_path: Path) -> None:
    payload = _zip_fixture()
    identity = btcusdt_spot_identity()
    first = commit_month(
        root=tmp_path,
        identity=identity,
        month="2024-01",
        zip_bytes=payload,
        retrieved_at=datetime(2024, 2, 1, tzinfo=UTC),
    )
    second = commit_month(
        root=tmp_path,
        identity=identity,
        month="2024-01",
        zip_bytes=payload,
        retrieved_at=datetime(2024, 2, 1, tzinfo=UTC),
    )
    assert first.rows == second.rows == 2
    files = list((tmp_path / "normalized").rglob("*.parquet"))
    assert len(files) == 1
    table = pq.ParquetFile(files[0]).read()
    assert table.schema == ARROW_SCHEMA
    assert table.num_rows == 2
    manifest = (tmp_path / "control" / "manifests" / "binance_spot_ohlcv.jsonl").read_text()
    assert len(manifest.splitlines()) == 1
    checkpoint = json.loads(
        (tmp_path / "control" / "checkpoints" / "binance_spot_btcusdt_1m.json").read_text()
    )
    assert checkpoint["cursor"] == "2024-01"


def test_gap_is_recorded_not_deleted(tmp_path: Path) -> None:
    rows = archive_rows(_zip_fixture())
    third = rows[-1].copy()
    third[0], third[6] = "1704067320000", "1704067379999"
    rows.append(third)
    rows.pop(1)
    stream = io.BytesIO()
    with zipfile.ZipFile(stream, "w") as archive:
        archive.writestr("BTCUSDT-1m-2024-01.csv", "\n".join(",".join(row) for row in rows))
    commit_month(
        root=tmp_path,
        identity=btcusdt_spot_identity(),
        month="2024-01",
        zip_bytes=stream.getvalue(),
        retrieved_at=datetime(2024, 2, 1, tzinfo=UTC),
    )
    records = list((tmp_path / "control" / "gap_registry").glob("*.json"))
    assert len(records) == 1


def test_archive_timestamp_policy_is_explicit_pre_and_post_transition() -> None:
    assert archive_epoch_unit("2024-12") == "ms"
    assert archive_epoch_unit("2025-01") == "us"
    post = archive_rows(_zip_fixture())[0]
    post[0], post[6] = "1735689600000000", "1735689659999999"
    item = normalize_kline(
        post,
        identity=btcusdt_spot_identity(),
        source_method="fixture",
        source_uri="fixture://",
        raw_object_ref="post.csv",
        epoch_unit="us",
        source_sha256="b" * 64,
        retrieved_at=datetime(2025, 1, 1, tzinfo=UTC),
    )
    assert item["open_time"].year == 2025


class _Response:
    def __init__(self, payload: object):
        self.payload = payload

    def raise_for_status(self) -> None:
        pass

    def json(self) -> object:
        return self.payload


class _MetadataClient:
    def get(self, *_: object, **__: object) -> _Response:
        path = Path(__file__).parent / "fixtures" / "binance" / "exchangeInfo_BTCUSDT_spot.json"
        return _Response(json.loads(path.read_text()))


def test_metadata_fixture_snapshot_has_tick_step_and_status(tmp_path: Path) -> None:
    value = fetch_instrument_metadata("BTCUSDT", client=_MetadataClient())  # type: ignore[arg-type]
    assert value["status"] == "TRADING" and value["price_tick"] == "0.01000000"
    assert value["quantity_step"] == "0.00001000"
    assert save_metadata_snapshot(tmp_path, value).exists()


def test_rest_tail_persists_and_reconciliation_rejects_conflict(tmp_path: Path) -> None:
    rows = archive_rows(_zip_fixture())
    result = commit_rest_tail(
        root=tmp_path,
        identity=btcusdt_spot_identity(),
        rows=rows,
        conservative_cutoff_ms=1704067400000,
        retrieved_at=datetime(2024, 1, 1, tzinfo=UTC),
    )
    assert result.rows == 2
    rest = [
        normalize_kline(
            row,
            identity=btcusdt_spot_identity(),
            source_method="rest",
            source_uri="fixture://",
            raw_object_ref="x",
            epoch_unit="ms",
            source_sha256="c" * 64,
            retrieved_at=datetime(2024, 1, 1, tzinfo=UTC),
        )
        for row in rows
    ]
    archive = [dict(row) for row in rest]
    reconcile_overlap(archive, rest)
    archive[0]["close"] += 1
    with pytest.raises(ValueError, match="conflict"):
        reconcile_overlap(archive, rest)


class _RestResponse:
    def __init__(self, status: int, payload: object):
        self.status_code, self.payload, self.headers = status, payload, {"Retry-After": "0"}

    def raise_for_status(self) -> None:
        if self.status_code >= 400:
            raise pytest.fail.Exception("unexpected HTTP failure")

    def json(self) -> object:
        return self.payload


class _RetryClient:
    def __init__(self, rows: list[list[str]], status: int = 429):
        self.values = [_RestResponse(status, []), _RestResponse(200, rows)]

    def get(self, *_: object, **__: object) -> _RestResponse:
        return self.values.pop(0)


def test_rest_retries_429_and_filters_open_candle() -> None:
    rows = archive_rows(_zip_fixture())
    result = fetch_rest_final(
        "BTCUSDT", start_ms=1704067200000, end_ms=1704067260000, client=_RetryClient(rows)
    )  # type: ignore[arg-type]
    assert len(result) == 1


def test_rest_retries_418() -> None:
    rows = archive_rows(_zip_fixture())
    assert (
        len(
            fetch_rest_final(
                "BTCUSDT",
                start_ms=1704067200000,
                end_ms=1704067260000,
                client=_RetryClient(rows, 418),
            )
        )
        == 1
    )  # type: ignore[arg-type]


@pytest.mark.parametrize(
    "name", ["binance_spot_rest_klines_1m_v1.yaml", "binance_spot_archive_klines_1m_v1.yaml"]
)
def test_separate_source_contracts_enumerate_twelve_raw_fields(name: str) -> None:
    path = Path(__file__).parents[1] / "schemas" / "contracts" / name
    contract = yaml.safe_load(path.read_text(encoding="utf-8"))
    assert len(contract["fields"]) == 12
    assert [field["source_field"] for field in contract["fields"]] == [
        f"[{item}]" for item in range(12)
    ]


def test_recovery_quarantines_stale_partial_without_touching_final(tmp_path: Path) -> None:
    partial = tmp_path / "normalized" / "x.parquet.partial"
    partial.parent.mkdir(parents=True)
    partial.write_bytes(b"interrupted")
    final = tmp_path / "normalized" / "safe.parquet"
    final.write_bytes(b"final")
    moved = recover_stale_partials(tmp_path)
    assert len(moved) == 1 and not partial.exists() and final.read_bytes() == b"final"


def test_resume_final_parquet_without_manifest_creates_manifest_and_checkpoint(
    tmp_path: Path,
) -> None:
    payload = _zip_fixture()
    stamp = datetime(2024, 2, 1, tzinfo=UTC)
    commit_month(
        root=tmp_path,
        identity=btcusdt_spot_identity(),
        month="2024-01",
        zip_bytes=payload,
        retrieved_at=stamp,
    )
    (tmp_path / "control" / "manifests" / "binance_spot_ohlcv.jsonl").unlink()
    (tmp_path / "control" / "checkpoints" / "binance_spot_btcusdt_1m.json").unlink()
    commit_month(
        root=tmp_path,
        identity=btcusdt_spot_identity(),
        month="2024-01",
        zip_bytes=payload,
        retrieved_at=stamp,
    )
    assert (
        len(
            (tmp_path / "control" / "manifests" / "binance_spot_ohlcv.jsonl")
            .read_text()
            .splitlines()
        )
        == 1
    )
    assert (tmp_path / "control" / "checkpoints" / "binance_spot_btcusdt_1m.json").exists()


def test_resume_manifest_without_checkpoint_does_not_duplicate_manifest(tmp_path: Path) -> None:
    payload = _zip_fixture()
    stamp = datetime(2024, 2, 1, tzinfo=UTC)
    commit_month(
        root=tmp_path,
        identity=btcusdt_spot_identity(),
        month="2024-01",
        zip_bytes=payload,
        retrieved_at=stamp,
    )
    (tmp_path / "control" / "checkpoints" / "binance_spot_btcusdt_1m.json").unlink()
    commit_month(
        root=tmp_path,
        identity=btcusdt_spot_identity(),
        month="2024-01",
        zip_bytes=payload,
        retrieved_at=stamp,
    )
    assert (
        len(
            (tmp_path / "control" / "manifests" / "binance_spot_ohlcv.jsonl")
            .read_text()
            .splitlines()
        )
        == 1
    )


def test_write_failure_never_advances_checkpoint(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    import crypto_quant.ingestion.binance.spot_ohlcv as module

    monkeypatch.setattr(
        module.pq, "write_table", lambda *_a, **_k: (_ for _ in ()).throw(OSError("disk"))
    )
    with pytest.raises(OSError, match="disk"):
        commit_month(
            root=tmp_path,
            identity=btcusdt_spot_identity(),
            month="2024-01",
            zip_bytes=_zip_fixture(),
        )
    assert not list((tmp_path / "control" / "checkpoints").glob("*.json"))


def test_fixed_historical_input_has_same_canonical_table_hash(tmp_path: Path) -> None:
    stamp = datetime(2024, 2, 1, tzinfo=UTC)
    payload = _zip_fixture()
    hashes = []
    for root in (tmp_path / "one", tmp_path / "two"):
        commit_month(
            root=root,
            identity=btcusdt_spot_identity(),
            month="2024-01",
            zip_bytes=payload,
            retrieved_at=stamp,
        )
        file = next((root / "normalized").rglob("*.parquet"))
        hashes.append(hashlib.sha256(file.read_bytes()).hexdigest())
    assert hashes[0] == hashes[1]


def test_usdm_archive_uses_derivative_manifest_only(tmp_path: Path) -> None:
    commit_month(
        root=tmp_path,
        identity=binance_identity("BTCUSDT", USDM),
        month="2024-01",
        zip_bytes=_zip_fixture(),
        retrieved_at=datetime(2024, 2, 1, tzinfo=UTC),
        market=USDM,
    )
    manifests = tmp_path / "control" / "manifests"
    assert (manifests / "binance_derivative_ohlcv.jsonl").exists()
    assert not (manifests / "binance_spot_ohlcv.jsonl").exists()


def test_future_binance_tail_can_use_v2_without_rewriting_v1(tmp_path: Path) -> None:
    output = commit_rest_tail_v2(root=tmp_path, identity=btcusdt_spot_identity(), rows=archive_rows(_zip_fixture()), conservative_cutoff_ms=1704067400000, retrieved_at=datetime(2024, 1, 1, tzinfo=UTC))
    assert "\\v2\\" in str(output) and pq.ParquetFile(output).read().num_rows == 2


def test_future_binance_usdm_v2_uses_canonical_perpetual_identity(tmp_path: Path) -> None:
    output = commit_rest_tail_v2(
        root=tmp_path,
        identity=binance_identity("BTCUSDT", USDM),
        rows=archive_rows(_zip_fixture()),
        conservative_cutoff_ms=1704067400000,
        retrieved_at=datetime(2024, 1, 1, tzinfo=UTC),
        market=USDM,
    )
    table = pq.read_table(output)
    assert set(table.column("market_type").to_pylist()) == {"perpetual"}
    assert "binance_perpetual_linear_perpetual" in next(
        (tmp_path / "control" / "manifests").glob("*.jsonl")
    ).name
