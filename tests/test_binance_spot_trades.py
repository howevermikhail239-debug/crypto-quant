from __future__ import annotations

import hashlib
import io
import json
import os
import zipfile
from datetime import UTC, date, datetime
from pathlib import Path

import pyarrow.parquet as pq
import pytest

from crypto_quant.ingestion.binance.spot_trades import (
    INDIVIDUAL_TRADE_SCHEMA,
    acquire_writer_lease,
    archive_rows,
    binance_spot_identity,
    btcusdt_spot_identity,
    commit_day,
    normalize_trade,
    quarantine_stale_trade_partials,
    source_timestamp_unit,
)


def _zip_fixture(*, with_header: bool = True) -> bytes:
    stream = io.BytesIO()
    with zipfile.ZipFile(stream, "w") as archive:
        archive.writestr(
            "BTCUSDT-trades-2026-07-01.csv",
            ("id,price,qty,quoteQty,time,isBuyerMaker,isBestMatch\n" if with_header else "")
            + "10,100000.0,0.01,1000.0,1782864000000000,True,True\n11,100001.0,0.02,2000.02,1782864000000000,False,False\n",
        )
    return stream.getvalue()


@pytest.mark.parametrize(
    ("buyer_maker", "expected_side", "expected_sign"), [("True", "SELL", -1), ("False", "BUY", 1)]
)
def test_buyer_maker_fixture_truth_table(
    buyer_maker: str, expected_side: str, expected_sign: int
) -> None:
    row = {
        "id": "1",
        "price": "100",
        "qty": "2",
        "quoteQty": "200",
        "time": "1782864000000000",
        "isBuyerMaker": buyer_maker,
        "isBestMatch": "True",
    }
    result = normalize_trade(
        row,
        source_ordinal=0,
        identity=btcusdt_spot_identity(),
        trading_date=date(2026, 7, 1),
        source_uri="fixture://",
        raw_object_ref="fixture.zip",
        source_sha256="a" * 64,
        retrieved_at=datetime(2026, 7, 2, tzinfo=UTC),
    )
    assert result["taker_side"] == expected_side
    assert result["signed_quantity"] == expected_sign * 2
    assert result["source_timestamp_unit"] == "us" and result["knowledge_time"] is None


def test_daily_archive_commit_preserves_ids_and_same_timestamp(tmp_path: Path) -> None:
    payload = _zip_fixture()
    archive = tmp_path / "fixture.zip"
    archive.write_bytes(payload)
    measurement = commit_day(
        root=tmp_path,
        identity=btcusdt_spot_identity(),
        trading_date=date(2026, 7, 1),
        archive_path=archive,
        expected_raw_sha256=hashlib.sha256(payload).hexdigest(),
        retrieved_at=datetime(2026, 7, 2, tzinfo=UTC),
    )
    output = next((tmp_path / "normalized").rglob("*.parquet"))
    table = pq.ParquetFile(output).read()
    assert table.schema == INDIVIDUAL_TRADE_SCHEMA and table.num_rows == 2
    assert table.column("native_trade_id").to_pylist() == ["10", "11"]
    assert len(set(table.column("source_timestamp").to_pylist())) == 1
    assert measurement.rows == 2 and measurement.resource_gate_passed
    event = json.loads(
        (tmp_path / "control" / "manifests" / "binance_spot_individual_trade.jsonl").read_text()
    )
    assert (
        event["dataset_class"] == "individual_trade"
        and event["raw_sha256"] == measurement.archive_sha256
    )


def test_source_contract_is_exactly_seven_columns(tmp_path: Path) -> None:
    archive = tmp_path / "fixture.zip"
    archive.write_bytes(_zip_fixture())
    assert len(archive_rows(archive)[0]) == 7
    assert source_timestamp_unit(date(2024, 12, 31)) == "ms"
    assert source_timestamp_unit(date(2025, 1, 1)) == "us"


def test_headerless_official_archive_layout_is_supported(tmp_path: Path) -> None:
    archive = tmp_path / "fixture.zip"
    archive.write_bytes(_zip_fixture(with_header=False))
    assert archive_rows(archive)[0]["id"] == "10"


def test_aggregate_columns_are_rejected(tmp_path: Path) -> None:
    archive = tmp_path / "bad.zip"
    archive.write_bytes(b"not-a-zip")
    with pytest.raises(ValueError, match="archive"):
        archive_rows(archive)


def test_second_writer_lease_is_rejected_and_release_is_recoverable(tmp_path: Path) -> None:
    lease = acquire_writer_lease(tmp_path, lease_id="one")
    with pytest.raises(RuntimeError, match="lease"):
        acquire_writer_lease(tmp_path, lease_id="two")
    lease.unlink()
    assert acquire_writer_lease(tmp_path, lease_id="two").exists()


def test_dead_stale_lease_and_partial_are_quarantined(tmp_path: Path) -> None:
    lease = tmp_path / "control" / "leases" / "binance_spot_individual_trade.lock"
    lease.parent.mkdir(parents=True)
    lease.write_text(
        json.dumps(
            {
                "run_id": "dead",
                "pid": 99999999,
                "created_at": "2020-01-01T00:00:00+00:00",
                "heartbeat": "2020-01-01T00:00:00+00:00",
            }
        )
    )
    assert acquire_writer_lease(tmp_path, lease_id="new").exists()
    assert list((tmp_path / "quarantine" / "stale_trade_writer").glob("*.json"))
    active = tmp_path / "active.partial"
    stale = tmp_path / "stale.partial"
    active.write_bytes(b"a")
    stale.write_bytes(b"s")
    stamp = stale.stat().st_mtime + 20
    os.utime(active, (stamp, stamp))
    moved = quarantine_stale_trade_partials(tmp_path, stale_after_seconds=10, now=stamp)
    assert active.exists() and len(moved) == 1 and not stale.exists()


def test_exact_archive_member_symbol_date_is_enforced(tmp_path: Path) -> None:
    archive = tmp_path / "wrong.zip"
    stream = io.BytesIO()
    with zipfile.ZipFile(stream, "w") as value:
        value.writestr(
            "ETHUSDT-trades-2026-07-01.csv", "id,price,qty,quoteQty,time,isBuyerMaker,isBestMatch\n"
        )
    archive.write_bytes(stream.getvalue())
    from crypto_quant.ingestion.binance.spot_trades import iter_archive_rows

    with pytest.raises(ValueError, match="symbol/date"):
        iter_archive_rows(archive, expected_symbol="BTCUSDT", expected_date=date(2026, 7, 1))


def test_eth_identity_reuses_same_strict_spot_route() -> None:
    eth = binance_spot_identity("ETHUSDT")
    assert eth.exchange == "binance" and eth.market_type == eth.contract_type == "spot"
    assert eth.base_asset == eth.quantity_unit == "ETH" and eth.notional_unit == "USDT"
    assert eth.instrument_id != btcusdt_spot_identity().instrument_id
    with pytest.raises(ValueError, match="only BTCUSDT and ETHUSDT"):
        binance_spot_identity("SOLUSDT")
