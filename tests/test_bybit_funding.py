"""Unit and integration acceptance tests for Bybit Linear Funding Rate Ingestion (Phase 1D.1B)."""

import json
import tempfile
from datetime import UTC, datetime
from pathlib import Path
from unittest.mock import MagicMock

import pytest
import yaml

from crypto_quant.ingestion.bybit.funding import (
    fetch_bybit_funding_history,
    fetch_bybit_instruments_info_snapshot,
    funding_identity,
    ingest_bybit_funding_rate,
    parse_bybit_funding_rate_item,
)


def test_funding_identity_bybit():
    ident = funding_identity("BTCUSDT")
    assert ident.exchange == "bybit"
    assert ident.native_symbol == "BTCUSDT"
    assert ident.market_type == "perpetual"
    assert ident.contract_type == "linear_perpetual"
    assert ident.base_asset == "BTC"
    assert ident.quote_asset == "USDT"
    assert ident.settle_asset == "USDT"
    assert ident.instrument_id.startswith("ins_")

    with pytest.raises(ValueError, match="BTCUSDT/ETHUSDT only"):
        funding_identity("SOLUSDT")


def test_parse_bybit_funding_rate_preserves_decimal_and_fields():
    ident = funding_identity("BTCUSDT")
    raw = {
        "symbol": "BTCUSDT",
        "fundingRate": "0.00005639",
        "fundingRateTimestamp": "1786348800000",
    }
    rec = parse_bybit_funding_rate_item(raw, ident)

    assert rec.exchange == "bybit"
    assert rec.symbol == "BTCUSDT"
    assert rec.venue_product_type == "linear"
    assert rec.funding_rate == "0.00005639"  # Raw Decimal fraction preserved
    assert rec.source_rate_type is None  # Bybit does not provide rateType
    assert rec.canonical_rate_type == "NOT_PROVIDED"
    assert rec.mark_price is None  # Bybit does not provide markPrice
    assert rec.configured_interval_minutes is None  # Never retrofitted
    assert rec.observed_interval_minutes is None  # First record
    assert rec.interval_source == "UNKNOWN"
    assert rec.knowledge_time is None  # UNKNOWN for historical bootstrap


def test_bybit_rate_type_and_mark_price_nullability_provenance():
    """Confirms Bybit records maintain NULL provenance without silent enrichment."""
    ident = funding_identity("ETHUSDT")
    raw = {
        "symbol": "ETHUSDT",
        "fundingRate": "-0.00012000",
        "fundingRateTimestamp": "1786348800000",
    }
    rec = parse_bybit_funding_rate_item(raw, ident)
    assert rec.mark_price is None
    assert rec.source_rate_type is None
    assert rec.canonical_rate_type == "NOT_PROVIDED"


def test_bybit_funding_source_contract_loads_and_validates():
    """Validates frozen YAML contract for Bybit Linear Funding Rate."""
    contracts_dir = Path("schemas/contracts")
    contract_path = contracts_dir / "bybit_linear_funding_rate_rest_v1.yaml"
    assert contract_path.exists(), "Bybit funding contract must exist"

    data = yaml.safe_load(contract_path.read_text(encoding="utf-8"))
    assert data["contract_id"] == "bybit.linear.rest.funding-rate.v1"
    assert data["exchange"] == "bybit"
    assert data["market_type"] == "perpetual"
    assert len(data["fields"]) == 3


def test_bybit_observed_interval_calculation_and_configured_isolation():
    ident = funding_identity("BTCUSDT")
    t1 = datetime(2026, 8, 1, 0, 0, tzinfo=UTC)
    raw2 = {
        "symbol": "BTCUSDT",
        "fundingRate": "0.00010000",
        "fundingRateTimestamp": str(int(datetime(2026, 8, 1, 8, 0, tzinfo=UTC).timestamp() * 1000)),
    }
    rec2 = parse_bybit_funding_rate_item(raw2, ident, prev_funding_time=t1)

    assert rec2.observed_interval_minutes == 480  # 8 hours = 480 minutes
    assert rec2.interval_source == "OBSERVED_EVENT_DELTA"
    assert rec2.configured_interval_minutes is None


def test_fetch_bybit_funding_history_pagination_backwards_and_sorted():
    """Tests Bybit pagination traversing backwards and returning records in strictly ascending order."""
    mock_client = MagicMock()

    # Batch 1 (latest) returns 2 items in descending order (with limit=2 so continues)
    resp1 = MagicMock()
    resp1.json.return_value = {
        "retCode": 0,
        "retMsg": "OK",
        "result": {
            "list": [
                {"symbol": "BTCUSDT", "fundingRate": "0.0002", "fundingRateTimestamp": "3000"},
                {"symbol": "BTCUSDT", "fundingRate": "0.0001", "fundingRateTimestamp": "2000"},
            ]
        },
    }
    # Batch 2 (older) returns 1 item in descending order (terminates)
    resp2 = MagicMock()
    resp2.json.return_value = {
        "retCode": 0,
        "retMsg": "OK",
        "result": {
            "list": [
                {"symbol": "BTCUSDT", "fundingRate": "0.0001", "fundingRateTimestamp": "1000"},
            ]
        },
    }
    mock_client.get.side_effect = [resp1, resp2]

    res = fetch_bybit_funding_history("BTCUSDT", limit=2, client=mock_client)
    assert len(res) == 3
    # Verify result is sorted strictly ascending
    assert res[0]["fundingRateTimestamp"] == "1000"
    assert res[1]["fundingRateTimestamp"] == "2000"
    assert res[2]["fundingRateTimestamp"] == "3000"
    assert mock_client.get.call_count == 2


def test_fetch_bybit_instruments_info_snapshot():
    with tempfile.TemporaryDirectory() as tmp_dir:
        root = Path(tmp_dir)
        mock_client = MagicMock()
        resp = MagicMock()
        resp.json.return_value = {
            "retCode": 0,
            "retMsg": "OK",
            "result": {
                "list": [
                    {
                        "symbol": "BTCUSDT",
                        "contractType": "LinearPerpetual",
                        "status": "Trading",
                        "fundingInterval": 480,
                    }
                ]
            },
        }
        mock_client.get.return_value = resp

        info = fetch_bybit_instruments_info_snapshot("BTCUSDT", root, client=mock_client)
        assert info["symbol"] == "BTCUSDT"
        assert info["fundingInterval"] == 480

        meta_dir = root / "control" / "instrument_metadata"
        snapshot_files = list(meta_dir.glob("bybit_linear_instruments_info_BTCUSDT_*.json"))
        assert len(snapshot_files) == 1
        saved = json.loads(snapshot_files[0].read_text(encoding="utf-8"))
        assert saved["dataset_id"] == "bybit.linear.instruments-info.rest"
        assert saved["item"]["fundingInterval"] == 480


def test_ingest_bybit_funding_rate_end_to_end():
    with tempfile.TemporaryDirectory() as tmp_dir:
        root = Path(tmp_dir)
        mock_client = MagicMock()

        # Mock instruments-info
        resp_info = MagicMock()
        resp_info.json.return_value = {
            "retCode": 0,
            "retMsg": "OK",
            "result": {"list": [{"symbol": "BTCUSDT", "fundingInterval": 480}]},
        }

        # Mock funding history
        resp_rates = MagicMock()
        resp_rates.json.return_value = {
            "retCode": 0,
            "retMsg": "OK",
            "result": {
                "list": [
                    {"symbol": "BTCUSDT", "fundingRate": "0.00015", "fundingRateTimestamp": "1786233600000"},
                    {"symbol": "BTCUSDT", "fundingRate": "0.00010", "fundingRateTimestamp": "1786204800000"},
                ]
            },
        }
        mock_client.get.side_effect = [resp_info, resp_rates]

        result = ingest_bybit_funding_rate("BTCUSDT", root, client=mock_client)
        assert result["status"] == "PASS"
        assert result["records_count"] == 2

        # Verify raw file
        raw_files = list((root / "raw" / "bybit" / "perpetual" / "funding_rate" / "BTCUSDT").glob("*.jsonl"))
        assert len(raw_files) == 1

        # Verify Parquet files
        norm_dir = root / "normalized" / "funding" / "v1" / "exchange=bybit" / "market_type=perpetual" / "symbol=BTCUSDT"
        parquet_files = list(norm_dir.rglob("*.parquet"))
        assert len(parquet_files) == 1

        # Verify manifest
        manifest_file = root / "control" / "manifests" / "bybit_linear_funding_rate.jsonl"
        assert manifest_file.exists()
        manifest_lines = manifest_file.read_text(encoding="utf-8").strip().splitlines()
        assert len(manifest_lines) == 1
        mdata = json.loads(manifest_lines[0])
        assert mdata["exchange"] == "bybit"
        assert mdata["venue_product_type"] == "linear"
        assert mdata["symbol"] == "BTCUSDT"
        assert mdata["row_count"] == 2

        # Verify checkpoint
        chk_file = root / "control" / "checkpoints" / "bybit_linear_funding_rate_BTCUSDT.json"
        assert chk_file.exists()
        chk = json.loads(chk_file.read_text(encoding="utf-8"))
        assert chk["total_records"] == 2


def test_bybit_rerun_bootstrap_idempotent_without_rmtree():
    """Proves re-running Bybit ingestion over existing dataset is idempotent without any rmtree."""
    with tempfile.TemporaryDirectory() as tmp_dir:
        root = Path(tmp_dir)
        mock_client = MagicMock()

        resp_info = MagicMock()
        resp_info.json.return_value = {
            "retCode": 0,
            "retMsg": "OK",
            "result": {"list": [{"symbol": "BTCUSDT", "fundingInterval": 480}]},
        }

        resp_rates = MagicMock()
        resp_rates.json.return_value = {
            "retCode": 0,
            "retMsg": "OK",
            "result": {
                "list": [
                    {"symbol": "BTCUSDT", "fundingRate": "0.00015", "fundingRateTimestamp": "1786233600000"},
                    {"symbol": "BTCUSDT", "fundingRate": "0.00010", "fundingRateTimestamp": "1786204800000"},
                ]
            },
        }
        mock_client.get.side_effect = [resp_info, resp_rates, resp_info, resp_rates]

        # 1. First Run
        res1 = ingest_bybit_funding_rate("BTCUSDT", root, client=mock_client)
        assert res1["status"] == "PASS"
        assert res1["records_count"] == 2

        # 2. Second Run
        res2 = ingest_bybit_funding_rate("BTCUSDT", root, client=mock_client)
        assert res2["status"] == "PASS"
        assert res2["records_count"] == 2

        # Verify Parquet files count remains exactly 1 for the year
        norm_dir = root / "normalized" / "funding" / "v1" / "exchange=bybit" / "market_type=perpetual" / "symbol=BTCUSDT"
        parquet_files = list(norm_dir.rglob("*.parquet"))
        assert len(parquet_files) == 1, "Must not create duplicate files per year partition"

        # Verify manifest has 2 records
        manifest_file = root / "control" / "manifests" / "bybit_linear_funding_rate.jsonl"
        lines = [json.loads(line) for line in manifest_file.read_text(encoding="utf-8").splitlines() if line.strip()]
        assert len(lines) == 2
        assert lines[0]["row_count"] == 2
        assert lines[1]["row_count"] == 2
