"""Unit and integration acceptance tests for Bybit Linear Open Interest Ingestion (Phase 1D.2B)."""

import json
import tempfile
from pathlib import Path
from unittest.mock import MagicMock

import yaml

from crypto_quant.ingestion.binance.funding import funding_identity
from crypto_quant.ingestion.bybit.open_interest import (
    fetch_bybit_open_interest_history,
    ingest_bybit_open_interest,
    parse_bybit_open_interest_item,
)


def test_parse_bybit_open_interest_preserves_decimal_and_fields():
    ident = funding_identity("BTCUSDT")
    raw = {
        "symbol": "BTCUSDT",
        "openInterest": "58824.82200000",
        "singleOpenInterest": "29412.411",
        "timestamp": "1786429200000",
    }
    rec = parse_bybit_open_interest_item(raw, ident, period="5m")

    assert rec.exchange == "bybit"
    assert rec.symbol == "BTCUSDT"
    assert rec.venue_product_type == "linear"
    assert rec.period == "5m"
    assert rec.oi_base == "58824.82200000"
    assert rec.single_side_oi_base == "29412.411"
    assert rec.oi_notional is None  # Bybit history does not provide notional value
    assert rec.oi_semantic == "SUM_BOTH_SIDES_BASE"
    assert rec.knowledge_time is None  # UNKNOWN for historical bootstrap


def test_bybit_open_interest_source_contract_loads_and_validates():
    """Validates frozen YAML contract for Bybit Linear Open Interest."""
    contracts_dir = Path("schemas/contracts")
    contract_path = contracts_dir / "bybit_linear_open_interest_rest_v1.yaml"
    assert contract_path.exists(), "Bybit OI contract must exist"

    data = yaml.safe_load(contract_path.read_text(encoding="utf-8"))
    assert data["contract_id"] == "bybit.linear.rest.open-interest.v1"
    assert data["exchange"] == "bybit"
    assert data["market_type"] == "perpetual"
    assert len(data["fields"]) == 4


def test_bybit_oi_notional_nullability_provenance():
    ident = funding_identity("ETHUSDT")
    raw = {
        "symbol": "ETHUSDT",
        "openInterest": "250000.5",
        "singleOpenInterest": "125000.25",
        "timestamp": "1786429200000",
    }
    rec = parse_bybit_open_interest_item(raw, ident, period="5m")
    assert rec.oi_notional is None, "Must preserve null provenance without synthetic conversion"


def test_fetch_bybit_open_interest_history_cursor_pagination():
    mock_client = MagicMock()

    resp1 = MagicMock()
    resp1.json.return_value = {
        "retCode": 0,
        "retMsg": "OK",
        "result": {
            "list": [
                {"symbol": "BTCUSDT", "openInterest": "200", "singleOpenInterest": "100", "timestamp": "3000"},
                {"symbol": "BTCUSDT", "openInterest": "150", "singleOpenInterest": "75", "timestamp": "2000"},
            ],
            "nextPageCursor": "cursor_page_2",
        },
    }
    resp2 = MagicMock()
    resp2.json.return_value = {
        "retCode": 0,
        "retMsg": "OK",
        "result": {
            "list": [
                {"symbol": "BTCUSDT", "openInterest": "100", "singleOpenInterest": "50", "timestamp": "1000"},
            ],
            "nextPageCursor": "",
        },
    }
    mock_client.get.side_effect = [resp1, resp2]

    res, term = fetch_bybit_open_interest_history("BTCUSDT", period="5m", limit=2, client=mock_client)
    assert len(res) == 3
    assert res[0]["timestamp"] == "1000"
    assert res[1]["timestamp"] == "2000"
    assert res[2]["timestamp"] == "3000"
    assert term == "CURSOR_EMPTY"
    assert mock_client.get.call_count == 2


def test_bybit_pagination_termination_distinguishes_source_exhaustion_vs_page_limit():
    """Proves that Bybit pagination explicitly reports PAGE_LIMIT_REACHED when stopped by max_pages."""
    mock_client = MagicMock()

    resp = MagicMock()
    resp.json.return_value = {
        "retCode": 0,
        "retMsg": "OK",
        "result": {
            "list": [
                {"symbol": "BTCUSDT", "openInterest": "200", "singleOpenInterest": "100", "timestamp": "2000"},
                {"symbol": "BTCUSDT", "openInterest": "150", "singleOpenInterest": "75", "timestamp": "1000"},
            ],
            "nextPageCursor": "cursor_page_2_available",
        },
    }
    mock_client.get.return_value = resp

    res, term = fetch_bybit_open_interest_history("BTCUSDT", period="5m", limit=2, max_pages=1, client=mock_client)
    assert len(res) == 2
    assert term == "PAGE_LIMIT_REACHED", "Must explicitly indicate page limit termination"


def test_bybit_accumulation_preserves_earlier_partition_data():
    """Proves that local accumulated history is preserved when new Bybit records arrive."""
    with tempfile.TemporaryDirectory() as tmp_dir:
        root = Path(tmp_dir)
        ident = funding_identity("BTCUSDT")

        # 1. Initial State: Old observation T_old (in 2026)
        old_rec = parse_bybit_open_interest_item(
            {"symbol": "BTCUSDT", "openInterest": "50000", "singleOpenInterest": "25000", "timestamp": "1780000000000"},
            ident,
            period="5m",
        )
        target_parquet = (
            root
            / "normalized"
            / "open_interest"
            / "v1"
            / "exchange=bybit"
            / "market_type=perpetual"
            / "symbol=BTCUSDT"
            / "period=5m"
            / f"year={old_rec.observation_time.year}"
            / f"part-btcusdt_5m_{old_rec.observation_time.year}.parquet"
        )
        from crypto_quant.ingestion.binance.open_interest import merge_and_write_oi_parquet

        merge_and_write_oi_parquet(target_parquet, [old_rec])
        assert target_parquet.exists()

        # 2. New Ingestion: returns newer observations (timestamps > 1785000000000)
        mock_client = MagicMock()
        resp = MagicMock()
        resp.json.return_value = {
            "retCode": 0,
            "retMsg": "OK",
            "result": {
                "list": [
                    {"symbol": "BTCUSDT", "openInterest": "100000", "singleOpenInterest": "50000", "timestamp": "1786429200000"},
                    {"symbol": "BTCUSDT", "openInterest": "101000", "singleOpenInterest": "50500", "timestamp": "1786428900000"},
                ],
                "nextPageCursor": "",
            },
        }
        mock_client.get.return_value = resp

        res = ingest_bybit_open_interest("BTCUSDT", root, period="5m", client=mock_client)
        assert res["status"] == "PASS"
        assert res["records_count"] == 2
        assert res["total_accumulated_rows"] == 3  # T_old + 2 new records

        import pyarrow.parquet as pq

        tbl = pq.ParquetFile(target_parquet).read()
        assert len(tbl) == 3
        timestamps = tbl["observation_time"].to_pylist()
        assert timestamps[0] < timestamps[1] < timestamps[2]
        assert tbl["oi_base"][0].as_py() == "50000"


def test_bybit_historical_knowledge_time_must_be_null():
    """Proves that historical bootstrap records have knowledge_time=None."""
    ident = funding_identity("BTCUSDT")
    raw = {
        "symbol": "BTCUSDT",
        "openInterest": "58824.82200000",
        "singleOpenInterest": "29412.411",
        "timestamp": "1786429200000",
    }
    rec = parse_bybit_open_interest_item(raw, ident, period="5m")
    assert rec.knowledge_time is None, "Historical knowledge_time must be null / UNKNOWN"


def test_ingest_bybit_open_interest_end_to_end():
    with tempfile.TemporaryDirectory() as tmp_dir:
        root = Path(tmp_dir)
        mock_client = MagicMock()

        resp = MagicMock()
        resp.json.return_value = {
            "retCode": 0,
            "retMsg": "OK",
            "result": {
                "list": [
                    {"symbol": "BTCUSDT", "openInterest": "200", "singleOpenInterest": "100", "timestamp": "1786429200000"},
                    {"symbol": "BTCUSDT", "openInterest": "150", "singleOpenInterest": "75", "timestamp": "1786428900000"},
                ],
                "nextPageCursor": "",
            },
        }
        mock_client.get.return_value = resp

        result = ingest_bybit_open_interest("BTCUSDT", root, period="5m", client=mock_client)
        assert result["status"] == "PASS"
        assert result["records_count"] == 2
        assert result["coverage_status"] == "COMPLETE"

        # Verify raw file
        raw_files = list((root / "raw" / "bybit" / "perpetual" / "open_interest" / "BTCUSDT" / "5m").glob("*.jsonl"))
        assert len(raw_files) == 1

        # Verify Parquet files
        norm_dir = root / "normalized" / "open_interest" / "v1" / "exchange=bybit" / "market_type=perpetual" / "symbol=BTCUSDT" / "period=5m"
        parquet_files = list(norm_dir.rglob("*.parquet"))
        assert len(parquet_files) == 1

        # Verify manifest
        manifest_file = root / "control" / "manifests" / "bybit_linear_open_interest.jsonl"
        assert manifest_file.exists()
        manifest_lines = manifest_file.read_text(encoding="utf-8").strip().splitlines()
        assert len(manifest_lines) == 1
        mdata = json.loads(manifest_lines[0])
        assert mdata["exchange"] == "bybit"
        assert mdata["symbol"] == "BTCUSDT"
        assert mdata["period"] == "5m"
        assert mdata["row_count"] == 2

        # Verify checkpoint
        chk_file = root / "control" / "checkpoints" / "bybit_linear_open_interest_BTCUSDT_5m.json"
        assert chk_file.exists()
        chk = json.loads(chk_file.read_text(encoding="utf-8"))
        assert chk["total_records"] == 2


def test_bybit_oi_rerun_bootstrap_idempotent_without_rmtree():
    with tempfile.TemporaryDirectory() as tmp_dir:
        root = Path(tmp_dir)
        mock_client = MagicMock()

        resp = MagicMock()
        resp.json.return_value = {
            "retCode": 0,
            "retMsg": "OK",
            "result": {
                "list": [
                    {"symbol": "BTCUSDT", "openInterest": "200", "singleOpenInterest": "100", "timestamp": "1786429200000"},
                    {"symbol": "BTCUSDT", "openInterest": "150", "singleOpenInterest": "75", "timestamp": "1786428900000"},
                ],
                "nextPageCursor": "",
            },
        }
        mock_client.get.side_effect = [resp, resp]

        res1 = ingest_bybit_open_interest("BTCUSDT", root, period="5m", client=mock_client)
        assert res1["status"] == "PASS"

        res2 = ingest_bybit_open_interest("BTCUSDT", root, period="5m", client=mock_client)
        assert res2["status"] == "PASS"

        norm_dir = root / "normalized" / "open_interest" / "v1" / "exchange=bybit" / "market_type=perpetual" / "symbol=BTCUSDT" / "period=5m"
        parquet_files = list(norm_dir.rglob("*.parquet"))
        assert len(parquet_files) == 1, "Must not duplicate parquet files per year"
