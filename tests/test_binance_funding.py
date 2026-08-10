"""Unit and integration acceptance tests for Binance USD-M Funding Rate Ingestion (Phase 1D.1A)."""

import json
import tempfile
from datetime import UTC, datetime
from pathlib import Path
from unittest.mock import MagicMock

import pytest

from crypto_quant.ingestion.binance.funding import (
    CANONICAL_FUNDING_SCHEMA,
    fetch_binance_funding_history,
    fetch_binance_funding_info,
    funding_identity,
    ingest_binance_funding_rate,
    parse_binance_funding_rate_item,
    records_to_pyarrow_table,
    validate_funding_records_dq,
)


def test_funding_identity_binance():
    ident = funding_identity("BTCUSDT")
    assert ident.exchange == "binance"
    assert ident.native_symbol == "BTCUSDT"
    assert ident.market_type == "perpetual"
    assert ident.contract_type == "linear_perpetual"
    assert ident.base_asset == "BTC"
    assert ident.quote_asset == "USDT"
    assert ident.settle_asset == "USDT"
    assert ident.instrument_id.startswith("ins_")

    with pytest.raises(ValueError, match="BTCUSDT/ETHUSDT only"):
        funding_identity("SOLUSDT")


def test_parse_binance_funding_rate_preserves_decimal_and_fields():
    ident = funding_identity("BTCUSDT")
    raw = {
        "symbol": "BTCUSDT",
        "fundingTime": 1786291200000,
        "fundingRate": "0.00007054",
        "markPrice": "65207.70000000",
        "rateType": "Regular",
    }
    rec = parse_binance_funding_rate_item(raw, ident)

    assert rec.exchange == "binance"
    assert rec.symbol == "BTCUSDT"
    assert rec.venue_product_type == "usdm"
    assert rec.funding_rate == "0.00007054"  # Raw Decimal fraction preserved
    assert rec.source_rate_type == "Regular"
    assert rec.canonical_rate_type == "REGULAR"
    assert rec.mark_price == "65207.70000000"
    assert rec.configured_interval_minutes is None  # Never retrofitted
    assert rec.observed_interval_minutes is None  # First record has no delta
    assert rec.interval_source == "UNKNOWN"
    assert rec.knowledge_time is None  # UNKNOWN for historical bootstrap


def test_parse_binance_funding_rate_special_rate_type():
    ident = funding_identity("BTCUSDT")
    raw = {
        "symbol": "BTCUSDT",
        "fundingTime": 1786291200000,
        "fundingRate": "-0.00025000",
        "markPrice": "",
        "rateType": "Special",
    }
    rec = parse_binance_funding_rate_item(raw, ident)
    assert rec.source_rate_type == "Special"
    assert rec.canonical_rate_type == "SPECIAL"
    assert rec.funding_rate == "-0.00025000"
    assert rec.mark_price is None  # Empty string maps to None


def test_observed_interval_calculation_and_configured_isolation():
    ident = funding_identity("BTCUSDT")
    t1 = datetime(2026, 8, 1, 0, 0, tzinfo=UTC)
    raw2 = {
        "symbol": "BTCUSDT",
        "fundingTime": int(datetime(2026, 8, 1, 8, 0, tzinfo=UTC).timestamp() * 1000),
        "fundingRate": "0.00010000",
        "markPrice": "60000",
        "rateType": "Regular",
    }
    rec2 = parse_binance_funding_rate_item(raw2, ident, prev_funding_time=t1)

    assert rec2.observed_interval_minutes == 480  # 8 hours = 480 minutes
    assert rec2.interval_source == "OBSERVED_EVENT_DELTA"
    assert rec2.configured_interval_minutes is None


def test_records_to_pyarrow_table():
    ident = funding_identity("BTCUSDT")
    raw = {
        "symbol": "BTCUSDT",
        "fundingTime": 1786291200000,
        "fundingRate": "0.00007054",
        "markPrice": "65207.70",
        "rateType": "Regular",
    }
    rec = parse_binance_funding_rate_item(raw, ident)
    table = records_to_pyarrow_table([rec])
    assert table.schema.names == CANONICAL_FUNDING_SCHEMA.names
    assert len(table) == 1
    assert table["funding_rate"][0].as_py() == "0.00007054"


def test_validate_funding_records_dq_catches_duplicates_and_ordering():
    ident = funding_identity("BTCUSDT")
    raw1 = {"symbol": "BTCUSDT", "fundingTime": 1000000, "fundingRate": "0.0001", "rateType": "Regular"}
    raw2 = {"symbol": "BTCUSDT", "fundingTime": 2000000, "fundingRate": "0.0001", "rateType": "Regular"}
    # Duplicate of raw2
    raw3 = {"symbol": "BTCUSDT", "fundingTime": 2000000, "fundingRate": "0.0002", "rateType": "Regular"}

    rec1 = parse_binance_funding_rate_item(raw1, ident)
    rec2 = parse_binance_funding_rate_item(raw2, ident)
    rec3 = parse_binance_funding_rate_item(raw3, ident)

    issues = validate_funding_records_dq([rec1, rec2, rec3])
    assert any("Duplicate natural key" in i for i in issues)
    assert any("Non-monotonic timestamp" in i for i in issues)


def test_fetch_binance_funding_history_pagination():
    mock_client = MagicMock()

    # Batch 1 returns 2 items with limit=2 (so continues)
    resp1 = MagicMock()
    resp1.json.return_value = [
        {"symbol": "BTCUSDT", "fundingTime": 1000, "fundingRate": "0.0001"},
        {"symbol": "BTCUSDT", "fundingTime": 2000, "fundingRate": "0.0001"},
    ]
    # Batch 2 returns 1 item (so terminates)
    resp2 = MagicMock()
    resp2.json.return_value = [
        {"symbol": "BTCUSDT", "fundingTime": 3000, "fundingRate": "0.0001"},
    ]
    mock_client.get.side_effect = [resp1, resp2]

    res = fetch_binance_funding_history("BTCUSDT", limit=2, client=mock_client)
    assert len(res) == 3
    assert res[0]["fundingTime"] == 1000
    assert res[2]["fundingTime"] == 3000
    assert mock_client.get.call_count == 2


def test_fetch_binance_funding_info_snapshot():
    with tempfile.TemporaryDirectory() as tmp_dir:
        root = Path(tmp_dir)
        mock_client = MagicMock()
        resp = MagicMock()
        resp.json.return_value = [
            {
                "symbol": "BTCUSDT",
                "adjustedFundingRateCap": "0.00300",
                "adjustedFundingRateFloor": "-0.00300",
                "fundingIntervalHours": 8,
            }
        ]
        mock_client.get.return_value = resp

        info = fetch_binance_funding_info(root, client=mock_client)
        assert len(info) == 1
        assert info[0]["symbol"] == "BTCUSDT"

        meta_dir = root / "control" / "instrument_metadata"
        snapshot_files = list(meta_dir.glob("binance_usdm_funding_info_*.json"))
        assert len(snapshot_files) == 1
        saved = json.loads(snapshot_files[0].read_text(encoding="utf-8"))
        assert saved["dataset_id"] == "binance.usdm.funding_info.rest"
        assert saved["items"][0]["fundingIntervalHours"] == 8


def test_ingest_binance_funding_rate_end_to_end():
    with tempfile.TemporaryDirectory() as tmp_dir:
        root = Path(tmp_dir)
        mock_client = MagicMock()

        # Mock fundingInfo
        resp_info = MagicMock()
        resp_info.json.return_value = [{"symbol": "BTCUSDT", "fundingIntervalHours": 8}]

        # Mock fundingRate
        resp_rates = MagicMock()
        resp_rates.json.return_value = [
            {"symbol": "BTCUSDT", "fundingTime": 1786204800000, "fundingRate": "0.0001", "markPrice": "64000", "rateType": "Regular"},
            {"symbol": "BTCUSDT", "fundingTime": 1786233600000, "fundingRate": "0.00015", "markPrice": "64200", "rateType": "Regular"},
        ]
        mock_client.get.side_effect = [resp_info, resp_rates]

        result = ingest_binance_funding_rate("BTCUSDT", root, client=mock_client)
        assert result["status"] == "PASS"
        assert result["records_count"] == 2

        # Verify raw file
        raw_files = list((root / "raw" / "binance" / "perpetual" / "funding_rate" / "BTCUSDT").glob("*.jsonl"))
        assert len(raw_files) == 1

        # Verify Parquet files
        norm_dir = root / "normalized" / "funding" / "v1" / "exchange=binance" / "market_type=perpetual" / "symbol=BTCUSDT"
        parquet_files = list(norm_dir.rglob("*.parquet"))
        assert len(parquet_files) == 1

        # Verify manifest
        manifest_file = root / "control" / "manifests" / "binance_usdm_funding_rate.jsonl"
        assert manifest_file.exists()
        manifest_lines = manifest_file.read_text(encoding="utf-8").strip().splitlines()
        assert len(manifest_lines) == 1
        mdata = json.loads(manifest_lines[0])
        assert mdata["exchange"] == "binance"
        assert mdata["symbol"] == "BTCUSDT"
        assert mdata["row_count"] == 2

        # Verify checkpoint
        chk_file = root / "control" / "checkpoints" / "binance_usdm_funding_rate_BTCUSDT.json"
        assert chk_file.exists()
        chk = json.loads(chk_file.read_text(encoding="utf-8"))
        assert chk["total_records"] == 2
