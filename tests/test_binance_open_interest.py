"""Unit and integration acceptance tests for Binance USD-M Open Interest Ingestion (Phase 1D.2A)."""

import json
import tempfile
from pathlib import Path
from unittest.mock import MagicMock

import pytest
import yaml

from crypto_quant.ingestion.binance.funding import funding_identity
from crypto_quant.ingestion.binance.open_interest import (
    CANONICAL_OI_SCHEMA,
    fetch_binance_open_interest_current,
    fetch_binance_open_interest_history,
    ingest_binance_open_interest,
    parse_binance_open_interest_item,
    records_to_pyarrow_oi_table,
    validate_open_interest_records_dq,
)


def test_parse_binance_open_interest_preserves_decimal_and_fields():
    ident = funding_identity("BTCUSDT")
    raw = {
        "symbol": "BTCUSDT",
        "sumOpenInterest": "106568.86800000",
        "sumOpenInterestValue": "6816677641.62000000",
        "CMCCirculatingSupply": "20068534.00000000",
        "timestamp": 1786428000000,
    }
    rec = parse_binance_open_interest_item(raw, ident, period="5m")

    assert rec.exchange == "binance"
    assert rec.symbol == "BTCUSDT"
    assert rec.venue_product_type == "usdm"
    assert rec.period == "5m"
    assert rec.oi_base == "106568.86800000"
    assert rec.oi_notional == "6816677641.62000000"
    assert rec.single_side_oi_base is None  # Binance does not provide single-side
    assert rec.oi_semantic == "SUM_TOTAL_BASE_AND_NOTIONAL"
    assert rec.knowledge_time is None  # UNKNOWN for historical bootstrap


def test_binance_open_interest_source_contracts_load_and_validate():
    """Validates frozen YAML contracts for Binance Open Interest History and Current Snapshot."""
    contracts_dir = Path("schemas/contracts")
    hist_contract = contracts_dir / "binance_usdm_open_interest_hist_rest_v1.yaml"
    curr_contract = contracts_dir / "binance_usdm_open_interest_current_rest_v1.yaml"

    assert hist_contract.exists(), "Hist contract must exist"
    assert curr_contract.exists(), "Current contract must exist"

    hist_data = yaml.safe_load(hist_contract.read_text(encoding="utf-8"))
    assert hist_data["contract_id"] == "binance.usdm.rest.open-interest-hist.v1"
    assert hist_data["exchange"] == "binance"
    assert hist_data["market_type"] == "perpetual"
    assert len(hist_data["fields"]) == 5

    curr_data = yaml.safe_load(curr_contract.read_text(encoding="utf-8"))
    assert curr_data["contract_id"] == "binance.usdm.rest.open-interest-current.v1"
    assert curr_data["exchange"] == "binance"
    assert len(curr_data["fields"]) == 3


def test_records_to_pyarrow_oi_table():
    ident = funding_identity("BTCUSDT")
    raw = {
        "symbol": "BTCUSDT",
        "sumOpenInterest": "106568.86800000",
        "sumOpenInterestValue": "6816677641.62000000",
        "timestamp": 1786428000000,
    }
    rec = parse_binance_open_interest_item(raw, ident, period="5m")
    tbl = records_to_pyarrow_oi_table([rec])
    assert tbl.schema.names == CANONICAL_OI_SCHEMA.names
    assert len(tbl) == 1
    assert tbl["oi_base"][0].as_py() == "106568.86800000"
    assert tbl["oi_notional"][0].as_py() == "6816677641.62000000"


def test_validate_open_interest_records_dq_catches_errors():
    ident = funding_identity("BTCUSDT")
    raw1 = {"symbol": "BTCUSDT", "sumOpenInterest": "100", "sumOpenInterestValue": "1000", "timestamp": 1000}
    raw2 = {"symbol": "BTCUSDT", "sumOpenInterest": "100", "sumOpenInterestValue": "1000", "timestamp": 2000}
    raw_dup = {"symbol": "BTCUSDT", "sumOpenInterest": "100", "sumOpenInterestValue": "1000", "timestamp": 2000}
    raw_neg = {"symbol": "BTCUSDT", "sumOpenInterest": "-10", "sumOpenInterestValue": "1000", "timestamp": 3000}

    rec1 = parse_binance_open_interest_item(raw1, ident, period="5m")
    rec2 = parse_binance_open_interest_item(raw2, ident, period="5m")
    rec_dup = parse_binance_open_interest_item(raw_dup, ident, period="5m")
    rec_neg = parse_binance_open_interest_item(raw_neg, ident, period="5m")

    issues = validate_open_interest_records_dq([rec1, rec2, rec_dup, rec_neg])
    assert any("Duplicate natural key" in i for i in issues)
    assert any("Non-monotonic timestamp" in i for i in issues)
    assert any("Negative oi_base" in i for i in issues)


def test_fetch_binance_open_interest_history_pagination_ascending():
    mock_client = MagicMock()

    # Batch 1 (latest): items from ts=2000 to ts=3000 (limit=2, so continues with endTime=1999)
    resp1 = MagicMock()
    resp1.status_code = 200
    resp1.json.return_value = [
        {"symbol": "BTCUSDT", "sumOpenInterest": "101", "sumOpenInterestValue": "1010", "timestamp": 2000},
        {"symbol": "BTCUSDT", "sumOpenInterest": "102", "sumOpenInterestValue": "1020", "timestamp": 3000},
    ]
    # Batch 2 (older): items from ts=1000 (1 item < limit, terminates)
    resp2 = MagicMock()
    resp2.status_code = 200
    resp2.json.return_value = [
        {"symbol": "BTCUSDT", "sumOpenInterest": "100", "sumOpenInterestValue": "1000", "timestamp": 1000},
    ]
    mock_client.get.side_effect = [resp1, resp2]

    res = fetch_binance_open_interest_history("BTCUSDT", period="5m", limit=2, client=mock_client)
    assert len(res) == 3
    assert res[0]["timestamp"] == 1000
    assert res[1]["timestamp"] == 2000
    assert res[2]["timestamp"] == 3000
    assert mock_client.get.call_count == 2



def test_fetch_binance_open_interest_current_snapshot():
    with tempfile.TemporaryDirectory() as tmp_dir:
        root = Path(tmp_dir)
        mock_client = MagicMock()
        resp = MagicMock()
        resp.status_code = 200
        resp.json.return_value = {"symbol": "BTCUSDT", "openInterest": "106617.807", "time": 1786429377185}
        mock_client.get.return_value = resp

        curr = fetch_binance_open_interest_current("BTCUSDT", root, client=mock_client)
        assert curr["symbol"] == "BTCUSDT"
        assert curr["openInterest"] == "106617.807"

        meta_dir = root / "control" / "instrument_metadata"
        snapshot_files = list(meta_dir.glob("binance_usdm_open_interest_current_BTCUSDT_*.json"))
        assert len(snapshot_files) == 1
        saved = json.loads(snapshot_files[0].read_text(encoding="utf-8"))
        assert saved["data"]["openInterest"] == "106617.807"


def test_ingest_binance_open_interest_end_to_end():
    with tempfile.TemporaryDirectory() as tmp_dir:
        root = Path(tmp_dir)
        mock_client = MagicMock()

        resp_curr = MagicMock()
        resp_curr.status_code = 200
        resp_curr.json.return_value = {"symbol": "BTCUSDT", "openInterest": "106617.807", "time": 1786429377185}

        resp_hist = MagicMock()
        resp_hist.status_code = 200
        resp_hist.json.return_value = [
            {"symbol": "BTCUSDT", "sumOpenInterest": "100", "sumOpenInterestValue": "1000", "timestamp": 1786428000000},
            {"symbol": "BTCUSDT", "sumOpenInterest": "101", "sumOpenInterestValue": "1010", "timestamp": 1786428300000},
        ]
        mock_client.get.side_effect = [resp_curr, resp_hist]

        result = ingest_binance_open_interest("BTCUSDT", root, period="5m", start_time_ms=1786428000000, client=mock_client)
        assert result["status"] == "PASS"
        assert result["records_count"] == 2

        # Verify raw file
        raw_files = list((root / "raw" / "binance" / "perpetual" / "open_interest" / "BTCUSDT" / "5m").glob("*.jsonl"))
        assert len(raw_files) == 1

        # Verify Parquet files
        norm_dir = root / "normalized" / "open_interest" / "v1" / "exchange=binance" / "market_type=perpetual" / "symbol=BTCUSDT" / "period=5m"
        parquet_files = list(norm_dir.rglob("*.parquet"))
        assert len(parquet_files) == 1

        # Verify manifest
        manifest_file = root / "control" / "manifests" / "binance_usdm_open_interest.jsonl"
        assert manifest_file.exists()
        manifest_lines = manifest_file.read_text(encoding="utf-8").strip().splitlines()
        assert len(manifest_lines) == 1
        mdata = json.loads(manifest_lines[0])
        assert mdata["exchange"] == "binance"
        assert mdata["symbol"] == "BTCUSDT"
        assert mdata["period"] == "5m"
        assert mdata["row_count"] == 2

        # Verify checkpoint
        chk_file = root / "control" / "checkpoints" / "binance_usdm_open_interest_BTCUSDT_5m.json"
        assert chk_file.exists()
        chk = json.loads(chk_file.read_text(encoding="utf-8"))
        assert chk["total_records"] == 2


def test_binance_oi_rerun_bootstrap_idempotent_without_rmtree():
    with tempfile.TemporaryDirectory() as tmp_dir:
        root = Path(tmp_dir)
        mock_client = MagicMock()

        resp_curr = MagicMock()
        resp_curr.status_code = 200
        resp_curr.json.return_value = {"symbol": "BTCUSDT", "openInterest": "106617.807", "time": 1786429377185}

        resp_hist = MagicMock()
        resp_hist.status_code = 200
        resp_hist.json.return_value = [
            {"symbol": "BTCUSDT", "sumOpenInterest": "100", "sumOpenInterestValue": "1000", "timestamp": 1786428000000},
            {"symbol": "BTCUSDT", "sumOpenInterest": "101", "sumOpenInterestValue": "1010", "timestamp": 1786428300000},
        ]
        mock_client.get.side_effect = [resp_curr, resp_hist, resp_curr, resp_hist]

        res1 = ingest_binance_open_interest("BTCUSDT", root, period="5m", start_time_ms=1786428000000, client=mock_client)
        assert res1["status"] == "PASS"

        res2 = ingest_binance_open_interest("BTCUSDT", root, period="5m", start_time_ms=1786428000000, client=mock_client)
        assert res2["status"] == "PASS"

        norm_dir = root / "normalized" / "open_interest" / "v1" / "exchange=binance" / "market_type=perpetual" / "symbol=BTCUSDT" / "period=5m"
        parquet_files = list(norm_dir.rglob("*.parquet"))
        assert len(parquet_files) == 1, "Must not duplicate parquet files per year"


def test_binance_oi_invalid_period_rejected():
    with pytest.raises(ValueError, match="Invalid period"):
        fetch_binance_open_interest_history("BTCUSDT", period="10m")


def test_binance_rolling_window_accumulation_preserves_old_history_outside_window():
    """Proves that local accumulated history is preserved when new rolling 30-day window arrives.

    Scenario:
    - Partition year=2026 initially contains historical observation T_old (e.g. 60 days ago, no longer in 30d API window).
    - New ingestion returns only current rolling window (T_new_1, T_new_2).
    - Merged partition must contain: T_old + T_new_1 + T_new_2, sorted ascending, 0 duplicate keys.
    """
    with tempfile.TemporaryDirectory() as tmp_dir:
        root = Path(tmp_dir)
        ident = funding_identity("BTCUSDT")

        # 1. Initial State: Old observation T_old (60 days ago in 2026)
        old_rec = parse_binance_open_interest_item(
            {"symbol": "BTCUSDT", "sumOpenInterest": "50000", "sumOpenInterestValue": "3000000000", "timestamp": 1780000000000},
            ident,
            period="5m",
        )
        target_parquet = (
            root
            / "normalized"
            / "open_interest"
            / "v1"
            / "exchange=binance"
            / "market_type=perpetual"
            / "symbol=BTCUSDT"
            / "period=5m"
            / f"year={old_rec.observation_time.year}"
            / f"part-btcusdt_5m_{old_rec.observation_time.year}.parquet"
        )
        from crypto_quant.ingestion.binance.open_interest import merge_and_write_oi_parquet

        merge_and_write_oi_parquet(target_parquet, [old_rec])
        assert target_parquet.exists()

        # 2. New Ingestion: returns only current window (timestamps > 1785000000000)
        mock_client = MagicMock()
        resp_curr = MagicMock()
        resp_curr.status_code = 200
        resp_curr.json.return_value = {"symbol": "BTCUSDT", "openInterest": "106617.807", "time": 1786429377185}

        resp_hist = MagicMock()
        resp_hist.status_code = 200
        resp_hist.json.return_value = [
            {"symbol": "BTCUSDT", "sumOpenInterest": "100000", "sumOpenInterestValue": "6000000000", "timestamp": 1786428000000},
            {"symbol": "BTCUSDT", "sumOpenInterest": "101000", "sumOpenInterestValue": "6100000000", "timestamp": 1786428300000},
        ]
        mock_client.get.side_effect = [resp_curr, resp_hist]

        res = ingest_binance_open_interest("BTCUSDT", root, period="5m", client=mock_client)
        assert res["status"] == "PASS"
        assert res["records_count"] == 2  # batch count
        assert res["total_accumulated_rows"] == 3  # T_old + 2 new records

        # 3. Verify on disk parquet
        import pyarrow.parquet as pq

        tbl = pq.ParquetFile(target_parquet).read()
        assert len(tbl) == 3
        timestamps = tbl["observation_time"].to_pylist()
        assert timestamps[0] < timestamps[1] < timestamps[2]
        assert tbl["oi_base"][0].as_py() == "50000"
        assert tbl["oi_base"][1].as_py() == "100000"
        assert tbl["oi_base"][2].as_py() == "101000"


def test_binance_historical_knowledge_time_must_be_null():
    """Proves that historical bootstrap records have knowledge_time=None to prevent look-ahead bias."""
    ident = funding_identity("BTCUSDT")
    raw = {
        "symbol": "BTCUSDT",
        "sumOpenInterest": "106568.86800000",
        "sumOpenInterestValue": "6816677641.62000000",
        "timestamp": 1786428000000,
    }
    rec = parse_binance_open_interest_item(raw, ident, period="5m")
    assert rec.knowledge_time is None, "Historical knowledge_time must be null / UNKNOWN"
