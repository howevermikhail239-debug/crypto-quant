from __future__ import annotations

import json
import os
from datetime import UTC, datetime
from pathlib import Path

import pytest
import yaml

from crypto_quant.ingestion.bybit.ohlcv import (
    LINEAR,
    SPOT,
    fetch_final,
    fetch_metadata,
    identity,
    normalize,
    save_metadata_snapshot,
)
from crypto_quant.ingestion.ohlcv_v2 import (
    OhlcvSourceDescriptor,
    commit_rows,
    recover_stale_partials,
)

FIX = Path(__file__).parent / "fixtures" / "bybit" / "kline_spot_BTCUSDT.json"
SPOT_META = Path(__file__).parent / "fixtures" / "bybit" / "instruments_spot_BTCUSDT.json"
LINEAR_META = Path(__file__).parent / "fixtures" / "bybit" / "instruments_linear_BTCUSDT.json"


def test_spot_fixture_contract_and_reverse_order_normalize():
    payload = json.loads(FIX.read_text())
    row = payload["result"]["list"][1]
    x = normalize(
        row,
        ident=identity("BTCUSDT", "spot"),
        category="spot",
        source_uri="fixture://bybit",
        retrieved_at=datetime(2026, 7, 1, tzinfo=UTC),
    )
    assert (
        x["interval"] == "PT1M" and x["base_volume"] == 2 - 1 and x["source_volume_unit"] == "BTC"
    )
    assert x["trade_count"] is None and x["knowledge_time"] is None


@pytest.mark.parametrize("category", ["spot", "linear"])
def test_volume_units_and_source_unsupported_fields_are_explicit(category: str):
    value = normalize(
        _row(1_800_000_000_000),
        ident=identity("BTCUSDT", category),
        category=category,
        source_uri="fixture://bybit",
        retrieved_at=datetime(2026, 8, 10, tzinfo=UTC),
    )
    assert value["base_volume"] == value["source_volume"] == 2
    assert value["quote_volume"] == value["source_turnover"] == 200
    assert value["source_volume_unit"] == "BTC"
    assert value["source_turnover_unit"] == "USDT"
    assert value["trade_count"] is None
    assert value["taker_buy_base_volume"] is None
    assert value["taker_buy_quote_volume"] is None


@pytest.mark.parametrize(
    "name", ["bybit_spot_rest_klines_1m_v1.yaml", "bybit_linear_rest_klines_1m_v1.yaml"]
)
def test_contract_has_exact_seven_tuple_fields(name):
    data = yaml.safe_load((Path(__file__).parents[1] / "schemas" / "contracts" / name).read_text())
    assert [x["source_field"] for x in data["fields"]] == [f"[{i}]" for i in range(7)]
    assert {x["source_field"] for x in data["envelope_fields"]} == {
        "retCode",
        "retMsg",
        "retExtInfo",
        "time",
        "result.category",
        "result.symbol",
        "result.list",
    }


@pytest.mark.parametrize(
    ("name", "required"),
    [
        (
            "bybit_spot_instruments_info_v1.yaml",
            {"symbol", "baseCoin", "quoteCoin", "priceFilter.tickSize", "lotSizeFilter.basePrecision"},
        ),
        (
            "bybit_linear_instruments_info_v1.yaml",
            {"symbol", "contractType", "settleCoin", "launchTime", "lotSizeFilter.qtyStep"},
        ),
    ],
)
def test_metadata_contracts_are_field_level(name, required):
    data = yaml.safe_load((Path(__file__).parents[1] / "schemas" / "contracts" / name).read_text())
    assert required <= {item["source_field"] for item in data["fields"]}
    assert all({"source_field", "unit", "nullable"} <= item.keys() for item in data["fields"])
    assert {"retCode", "result.category", "time"} <= {
        item["source_field"] for item in data["envelope_fields"]
    }


class Response:
    status_code = 200
    headers = {}

    def __init__(self, p):
        self.p = p

    def raise_for_status(self):
        pass

    def json(self):
        return self.p


class SequenceClient:
    def __init__(self, payloads):
        self.payloads = list(payloads)
        self.calls = []

    def get(self, *_, **kwargs):
        self.calls.append(kwargs.get("params"))
        if not self.payloads:
            raise AssertionError("unexpected request")
        value = self.payloads.pop(0)
        return value if isinstance(value, Response) else Response(value)


def _row(open_ms: int, close: str = "100") -> list[str]:
    return [str(open_ms), "100", "101", "99", close, "2", "200"]


def _envelope(rows, *, category="spot", symbol="BTCUSDT", ret_code=0):
    return {
        "retCode": ret_code,
        "retMsg": "OK" if ret_code == 0 else "error",
        "result": {"category": category, "symbol": symbol, "list": rows},
        "retExtInfo": {},
        "time": 1782865000000,
    }


class Client:
    def __init__(self):
        self.calls = []

    def get(self, *_, **kw):
        self.calls.append(kw["params"])
        return Response(json.loads(FIX.read_text()))


def test_reverse_pagination_uses_exclusive_internal_end_and_dedups():
    c = Client()
    rows, _ = fetch_final(
        "BTCUSDT",
        category="spot",
        start_ms=1782863940000,
        end_exclusive_ms=1782864060000,
        conservative_cutoff_ms=1782865000000,
        client=c,
    )
    assert [int(x[0]) for x in rows] == [1782863940000, 1782864000000]
    assert c.calls[0]["end"] == 1782864059999


def test_rejects_bad_tuple():
    with pytest.raises(ValueError):
        normalize(
            ["1"] * 6,
            ident=identity("BTCUSDT", "spot"),
            category="spot",
            source_uri="x",
            retrieved_at=datetime.now(UTC),
        )


def test_active_partial_is_not_recovered_but_stale_one_is(tmp_path: Path):
    active = tmp_path / "active.partial"
    stale = tmp_path / "stale.partial"
    active.write_text("a")
    stale.write_text("b")
    stale.touch()
    now = stale.stat().st_mtime + 2
    os.utime(active, (now, now))
    moved = recover_stale_partials(tmp_path, stale_after_seconds=1, now=now)
    assert active.exists() and len(moved) == 1 and not stale.exists()


@pytest.mark.parametrize("count", [180, 1000])
def test_half_open_page_boundaries_are_complete_and_ascending(count: int):
    start = 1_800_000_000_000
    rows = [_row(start + index * 60_000) for index in reversed(range(count))]
    client = SequenceClient([_envelope(rows)])
    actual, _ = fetch_final(
        "BTCUSDT",
        category="spot",
        start_ms=start,
        end_exclusive_ms=start + count * 60_000,
        conservative_cutoff_ms=start + (count + 1) * 60_000,
        client=client,
    )
    assert len(actual) == count
    assert [int(row[0]) for row in actual] == [start + index * 60_000 for index in range(count)]
    assert client.calls[0]["category"] == "spot"
    assert client.calls[0]["end"] == start + count * 60_000 - 1


def test_empty_page_is_an_explicit_empty_result():
    client = SequenceClient([_envelope([])])
    actual, pages = fetch_final(
        "BTCUSDT",
        category="spot",
        start_ms=1_800_000_000_000,
        end_exclusive_ms=1_800_000_060_000,
        conservative_cutoff_ms=1_900_000_000_000,
        client=client,
    )
    assert actual == [] and len(pages) == 1


def test_repeated_or_out_of_range_page_is_rejected():
    start = 1_800_000_000_000
    first = [_row(start + index * 60_000) for index in reversed(range(1, 1001))]
    client = SequenceClient([_envelope(first), _envelope(first)])
    with pytest.raises(ValueError, match="outside"):
        fetch_final(
            "BTCUSDT",
            category="spot",
            start_ms=start,
            end_exclusive_ms=start + 1001 * 60_000,
            conservative_cutoff_ms=1_900_000_000_000,
            client=client,
        )


def test_conflicting_duplicate_is_rejected():
    start = 1_800_000_000_000
    client = SequenceClient([_envelope([_row(start), _row(start, close="100.5")])])
    with pytest.raises(ValueError, match="conflicting"):
        fetch_final(
            "BTCUSDT",
            category="spot",
            start_ms=start,
            end_exclusive_ms=start + 60_000,
            conservative_cutoff_ms=1_900_000_000_000,
            client=client,
        )


@pytest.mark.parametrize(
    ("payload", "match"),
    [
        (_envelope([], ret_code=1), "identity"),
        (_envelope([], category="linear"), "identity"),
        (_envelope([], symbol="ETHUSDT"), "identity"),
    ],
)
def test_application_and_envelope_identity_errors_are_rejected(payload, match):
    with pytest.raises(ValueError, match=match):
        fetch_final(
            "BTCUSDT",
            category="spot",
            start_ms=1_800_000_000_000,
            end_exclusive_ms=1_800_000_060_000,
            conservative_cutoff_ms=1_900_000_000_000,
            client=SequenceClient([payload]),
        )


def test_current_candle_is_not_promoted_to_final():
    start = 1_800_000_000_000
    actual, _ = fetch_final(
        "BTCUSDT",
        category="spot",
        start_ms=start,
        end_exclusive_ms=start + 120_000,
        conservative_cutoff_ms=start + 90_000,
        client=SequenceClient([_envelope([_row(start + 60_000), _row(start)])]),
    )
    assert [int(row[0]) for row in actual] == [start]


def test_403_fails_closed():
    forbidden = Response(_envelope([]))
    forbidden.status_code = 403
    with pytest.raises(RuntimeError, match="600s"):
        fetch_final(
            "BTCUSDT", category="spot", start_ms=1_800_000_000_000,
            end_exclusive_ms=1_800_000_060_000, conservative_cutoff_ms=1_900_000_000_000,
            client=SequenceClient([forbidden]),
        )


def test_429_retry_and_application_rate_limit(monkeypatch):
    throttled = Response(_envelope([]))
    throttled.status_code = 429
    sleeps = []
    monkeypatch.setattr("crypto_quant.ingestion.bybit.ohlcv.time.sleep", sleeps.append)
    actual, _ = fetch_final(
        "BTCUSDT",
        category="spot",
        start_ms=1_800_000_000_000,
        end_exclusive_ms=1_800_000_060_000,
        conservative_cutoff_ms=1_900_000_000_000,
        client=SequenceClient([throttled, _envelope([])]),
    )
    assert actual == [] and sleeps == [1]
    limited = Response(_envelope([], ret_code=10006))
    limited.headers = {"X-Bapi-Limit-Reset-Timestamp": "1800000000123"}
    with pytest.raises(RuntimeError, match="1800000000123"):
        fetch_final(
            "BTCUSDT", category="spot", start_ms=1_800_000_000_000,
            end_exclusive_ms=1_800_000_060_000, conservative_cutoff_ms=1_900_000_000_000,
            client=SequenceClient([limited]),
        )


@pytest.mark.parametrize(
    ("category", "fixture", "expected_step", "expected_launch"),
    [
        ("spot", SPOT_META, None, None),
        ("linear", LINEAR_META, "0.001", "1584230400000"),
    ],
)
def test_official_shaped_metadata_fixture_and_snapshot(
    tmp_path: Path, category: str, fixture: Path, expected_step, expected_launch
):
    payload = json.loads(fixture.read_text(encoding="utf-8"))
    snapshot = fetch_metadata("BTCUSDT", category, client=SequenceClient([payload]))
    assert snapshot["quantity_step"] == expected_step
    assert snapshot["launch_time"] == expected_launch
    assert snapshot["source_envelope"]["result"]["category"] == category
    if category == "spot":
        assert snapshot["raw"]["lotSizeFilter"]["basePrecision"] == "0.000001"
    stored = json.loads(save_metadata_snapshot(tmp_path, snapshot).read_text(encoding="utf-8"))
    assert stored["source_envelope"] == payload


def _canonical_row(category: str = "spot"):
    return normalize(
        _row(1_800_000_000_000),
        ident=identity("BTCUSDT", category),
        category=category,
        source_uri="fixture://bybit",
        retrieved_at=datetime(2026, 8, 10, tzinfo=UTC),
    )


def test_descriptor_routing_matrix_is_unique_and_mismatch_writes_nothing(tmp_path: Path):
    descriptors = [
        SPOT,
        LINEAR,
        OhlcvSourceDescriptor("binance", "spot", "spot", "binance.spot.klines.1m"),
        OhlcvSourceDescriptor(
            "binance", "perpetual", "linear_perpetual", "binance.usdm.klines.1m"
        ),
    ]
    assert len({value.namespace() for value in descriptors}) == 4
    row = _canonical_row("spot")
    with pytest.raises(ValueError, match="identity"):
        commit_rows(
            root=tmp_path,
            descriptor=descriptors[2],
            identity=identity("BTCUSDT", "spot"),
            rows=[row],
            raw_bytes=b"[]",
            source_uri="fixture://mismatch",
            request={},
            generation="test",
        )
    assert list(tmp_path.iterdir()) == []

    binance_row = dict(row)
    binance_row.update(
        exchange="binance",
        source_dataset_id="binance.spot.klines.1m",
    )
    with pytest.raises(ValueError, match="instrument identity"):
        commit_rows(
            root=tmp_path,
            descriptor=descriptors[2],
            identity=identity("BTCUSDT", "spot"),
            rows=[binance_row],
            raw_bytes=b"[]",
            source_uri="fixture://identity-mismatch",
            request={},
            generation="test",
        )
    assert list(tmp_path.iterdir()) == []


def test_v2_commit_recovers_manifest_and_checkpoint_without_duplication(tmp_path: Path):
    row = _canonical_row("spot")
    kwargs = dict(
        root=tmp_path,
        descriptor=SPOT,
        identity=identity("BTCUSDT", "spot"),
        rows=[row],
        raw_bytes=b"fixture-envelope",
        source_uri="fixture://bybit",
        request={"category": "spot"},
        generation="recovery-test",
    )
    output = commit_rows(**kwargs)
    manifest = next((tmp_path / "control" / "manifests").glob("*.jsonl"))
    checkpoint = next((tmp_path / "control" / "checkpoints").glob("*.json"))
    manifest.unlink()
    checkpoint.unlink()
    assert commit_rows(**kwargs) == output
    assert manifest.exists() and checkpoint.exists()
    event = json.loads(manifest.read_text(encoding="utf-8"))
    assert event["exchange"] == "bybit"
    assert event["market_type"] == "spot"
    assert event["instrument_id"] == identity("BTCUSDT", "spot").instrument_id
    assert event["raw_bytes"] == len(b"fixture-envelope")
    assert event["parquet_bytes"] == output.stat().st_size
    assert event["retrieved_at"] == "2026-08-10T00:00:00+00:00"
    before = manifest.read_text(encoding="utf-8")
    checkpoint.unlink()
    commit_rows(**kwargs)
    assert manifest.read_text(encoding="utf-8") == before and checkpoint.exists()
