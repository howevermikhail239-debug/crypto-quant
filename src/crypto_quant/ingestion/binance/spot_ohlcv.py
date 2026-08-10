"""Parameterized Binance Spot and USD-M 1m OHLCV adapter.

The adapter deliberately accepts only final candles and writes immutable monthly
Parquet partitions.  It is not a trading feed and does not infer historical
knowledge time from download time.
"""

from __future__ import annotations

import csv
import hashlib
import json
import os
import tempfile
import time
import zipfile
from collections.abc import Sequence
from dataclasses import asdict, dataclass
from datetime import UTC, datetime
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Any

import httpx
import pyarrow as pa
import pyarrow.parquet as pq

from ...contracts import GapKind, GapRecord
from ...identity import InstrumentIdentity
from ...time import parse_epoch, utc_now
from ..ohlcv_v2 import OhlcvSourceDescriptor, commit_rows

INTERVAL_MS = 60_000
CONTRACT_ID = "binance.spot.ohlcv.1m.v1"
DATASET_ID = "binance.spot.klines.1m"
ARCHIVE_BASE = "https://data.binance.vision/data/spot/monthly/klines"
REST_BASE = "https://api.binance.com"


@dataclass(frozen=True)
class BinanceKlineMarket:
    market_type: str
    contract_type: str
    dataset_id: str
    archive_base: str
    rest_base: str
    rest_path: str
    epoch_unit: str


SPOT = BinanceKlineMarket(
    "spot", "spot", "binance.spot.klines.1m", ARCHIVE_BASE, REST_BASE, "/api/v3/klines", "policy"
)
USDM = BinanceKlineMarket(
    "derivative",
    "linear_perpetual",
    "binance.usdm.klines.1m",
    "https://data.binance.vision/data/futures/um/monthly/klines",
    "https://fapi.binance.com",
    "/fapi/v1/klines",
    "ms",
)

ARROW_SCHEMA = pa.schema(
    [
        pa.field("instrument_id", pa.string(), nullable=False),
        pa.field("exchange", pa.string(), nullable=False),
        pa.field("market_type", pa.string(), nullable=False),
        pa.field("contract_type", pa.string(), nullable=False),
        pa.field("native_symbol", pa.string(), nullable=False),
        pa.field("interval", pa.string(), nullable=False),
        pa.field("is_closed", pa.bool_(), nullable=False),
        pa.field("open_time", pa.timestamp("ms", tz="UTC"), nullable=False),
        pa.field("close_time", pa.timestamp("ms", tz="UTC"), nullable=False),
        pa.field("open", pa.decimal128(38, 18), nullable=False),
        pa.field("high", pa.decimal128(38, 18), nullable=False),
        pa.field("low", pa.decimal128(38, 18), nullable=False),
        pa.field("close", pa.decimal128(38, 18), nullable=False),
        pa.field("base_volume", pa.decimal128(38, 18), nullable=False),
        pa.field("quote_volume", pa.decimal128(38, 18), nullable=False),
        pa.field("trade_count", pa.int64(), nullable=False),
        pa.field("taker_buy_base_volume", pa.decimal128(38, 18), nullable=False),
        pa.field("taker_buy_quote_volume", pa.decimal128(38, 18), nullable=False),
        pa.field("source_method", pa.string(), nullable=False),
        pa.field("source_dataset_id", pa.string(), nullable=False),
        pa.field("source_uri", pa.string(), nullable=False),
        pa.field("observation_id", pa.string(), nullable=False),
        pa.field("raw_object_ref", pa.string(), nullable=False),
        pa.field("source_object_sha256", pa.string(), nullable=False),
        pa.field("retrieved_at", pa.timestamp("ms", tz="UTC"), nullable=False),
        pa.field("processed_at", pa.timestamp("ms", tz="UTC"), nullable=False),
        pa.field("knowledge_time", pa.timestamp("ms", tz="UTC"), nullable=True),
        pa.field("knowledge_time_basis", pa.string(), nullable=False),
        pa.field("schema_version", pa.string(), nullable=False),
        pa.field("collector_version", pa.string(), nullable=False),
        pa.field("normalization_version", pa.string(), nullable=False),
        pa.field("data_contract_version", pa.string(), nullable=False),
    ]
)


@dataclass(frozen=True)
class PilotMeasurement:
    rows: int
    elapsed_seconds: float
    parquet_bytes: int
    input_bytes: int


@dataclass(frozen=True)
class DataObjectManifest:
    """OHLCV-specific provenance; object checksum never aliases source checksum."""

    object_id: str
    parquet_sha256: str
    raw_sha256: str
    raw_object_ref: str
    source_uri: str
    source_kind: str
    source_dataset_id: str
    coverage_start: str
    coverage_end: str
    row_count: int
    retrieved_at: str
    processed_at: str
    schema_version: str = "1.0.0"
    contract_version: str = "1.0.0"
    collector_version: str = "0.1.0"
    normalization_version: str = "1.0.0"


@dataclass(frozen=True)
class KlineCheckpoint:
    source_dataset_id: str
    instrument_id: str
    cursor: str
    last_event_time: str
    last_knowledge_time: None
    committed_at: str


def btcusdt_spot_identity() -> InstrumentIdentity:
    return InstrumentIdentity(
        exchange="binance",
        native_symbol="BTCUSDT",
        market_type="spot",
        contract_type="spot",
        base_asset="BTC",
        quote_asset="USDT",
        settle_asset=None,
        quantity_unit="BTC",
        notional_unit="USDT",
    )


def binance_identity(symbol: str, market: BinanceKlineMarket = SPOT) -> InstrumentIdentity:
    base = symbol.removesuffix("USDT")
    return InstrumentIdentity(
        exchange="binance",
        native_symbol=symbol,
        market_type=market.market_type,
        contract_type=market.contract_type,
        base_asset=base,
        quote_asset="USDT",
        settle_asset="USDT" if market is USDM else None,
        quantity_unit=base,
        notional_unit="USDT",
    )


def fetch_instrument_metadata(
    symbol: str, client: httpx.Client | None = None, market: BinanceKlineMarket = SPOT
) -> dict[str, Any]:
    """Read official exchangeInfo and return a versionable trading-rule snapshot."""
    own_client = client is None
    client = client or httpx.Client(timeout=30)
    try:
        path = "/api/v3/exchangeInfo" if market is SPOT else "/fapi/v1/exchangeInfo"
        response = client.get(f"{market.rest_base}{path}", params={"symbol": symbol})
        response.raise_for_status()
        payload = response.json()
        symbols = [item for item in payload.get("symbols", []) if item.get("symbol") == symbol]
        if len(symbols) != 1:
            raise ValueError("exchangeInfo did not return exactly the requested symbol")
        item = symbols[0]
        filters = {entry["filterType"]: entry for entry in item.get("filters", [])}
        return {
            "exchange": "binance",
            "market_type": market.market_type,
            "contract_type": market.contract_type,
            "native_symbol": symbol,
            "base_asset": item["baseAsset"],
            "quote_asset": item["quoteAsset"],
            "status": item["status"],
            "price_tick": filters["PRICE_FILTER"]["tickSize"],
            "quantity_step": filters["LOT_SIZE"]["stepSize"],
            "retrieved_at": utc_now().isoformat(),
            "source_uri": f"{market.rest_base}{path}?symbol={symbol}",
        }
    finally:
        if own_client:
            client.close()


def save_metadata_snapshot(root: Path, snapshot: dict[str, Any]) -> Path:
    digest = _sha256(json.dumps(snapshot, sort_keys=True).encode())
    path = (
        root
        / "control"
        / "instrument_metadata"
        / f"binance_{snapshot['market_type']}_{snapshot['native_symbol']}_{digest}.json"
    )
    if not path.exists():
        _atomic_json(path, snapshot)
    return path


def _sha256(content: bytes) -> str:
    return hashlib.sha256(content).hexdigest()


def _decimal(value: object, field: str) -> Decimal:
    try:
        number = Decimal(str(value))
    except (InvalidOperation, ValueError) as error:
        raise ValueError(f"invalid decimal {field}: {value!r}") from error
    if not number.is_finite():
        raise ValueError(f"non-finite decimal {field}")
    return number


def normalize_kline(
    row: Sequence[object],
    *,
    identity: InstrumentIdentity,
    source_method: str,
    source_uri: str,
    raw_object_ref: str,
    epoch_unit: str,
    market: BinanceKlineMarket = SPOT,
    source_sha256: str,
    retrieved_at: datetime,
    schema_version: str = "1.0.0",
    collector_version: str = "0.1.0",
    normalization_version: str = "1.0.0",
) -> dict[str, Any]:
    """Validate exactly the documented 12-field Binance kline tuple."""
    if len(row) != 12:
        raise ValueError(f"Binance kline requires 12 fields, got {len(row)}")
    open_epoch, close_epoch = int(row[0]), int(row[6])
    if epoch_unit not in {"ms", "us"}:
        raise ValueError("epoch_unit must be an explicit source-contract value")
    unit = epoch_unit
    interval = INTERVAL_MS * (1_000 if unit == "us" else 1)
    if open_epoch % interval or close_epoch != open_epoch + interval - 1:
        raise ValueError("invalid 1m kline time boundaries")
    values = {
        name: _decimal(raw, name)
        for name, raw in zip(
            (
                "open",
                "high",
                "low",
                "close",
                "base_volume",
                "quote_volume",
                "taker_buy_base_volume",
                "taker_buy_quote_volume",
            ),
            (row[1], row[2], row[3], row[4], row[5], row[7], row[9], row[10]),
            strict=True,
        )
    }
    if any(values[name] <= 0 for name in ("open", "high", "low", "close")):
        raise ValueError("prices must be positive")
    if (
        not values["low"] <= min(values["open"], values["close"])
        or not max(values["open"], values["close"]) <= values["high"]
    ):
        raise ValueError("OHLC bounds invalid")
    if any(
        values[name] < 0
        for name in (
            "base_volume",
            "quote_volume",
            "taker_buy_base_volume",
            "taker_buy_quote_volume",
        )
    ):
        raise ValueError("volume must be nonnegative")
    if (
        values["taker_buy_base_volume"] > values["base_volume"]
        or values["taker_buy_quote_volume"] > values["quote_volume"]
    ):
        raise ValueError("taker volume exceeds total volume")
    trades = int(row[8])
    if trades < 0:
        raise ValueError("trade count must be nonnegative")
    return {
        "instrument_id": identity.instrument_id,
        "exchange": "binance",
        "market_type": market.market_type,
        "contract_type": market.contract_type,
        "native_symbol": identity.native_symbol,
        "interval": "1m",
        "is_closed": True,
        "open_time": parse_epoch(open_epoch, unit=unit),
        "close_time": parse_epoch(close_epoch, unit=unit),
        **values,
        "trade_count": trades,
        "source_method": source_method,
        "source_dataset_id": market.dataset_id,
        "source_uri": source_uri,
        "observation_id": f"{identity.instrument_id}:1m:{open_epoch}:{source_method}",
        "raw_object_ref": raw_object_ref,
        "source_object_sha256": source_sha256,
        "retrieved_at": retrieved_at,
        # Deterministic historical normalization: processing wall clock belongs
        # in the manifest, not in canonical candle content.
        "processed_at": retrieved_at,
        "knowledge_time": None,
        "knowledge_time_basis": "unknown_historical",
        "schema_version": schema_version,
        "collector_version": collector_version,
        "normalization_version": normalization_version,
        "data_contract_version": "1.0.0",
    }


def archive_url(symbol: str, month: str, market: BinanceKlineMarket = SPOT) -> str:
    return f"{market.archive_base}/{symbol}/1m/{symbol}-1m-{month}.zip"


def archive_epoch_unit(month: str, market: BinanceKlineMarket = SPOT) -> str:
    """Explicit Binance archive policy, documented by Binance public-data README."""
    try:
        year, month_number = (int(item) for item in month.split("-"))
    except ValueError as error:
        raise ValueError("month must be YYYY-MM") from error
    if not 1 <= month_number <= 12:
        raise ValueError("month must be YYYY-MM")
    return (
        market.epoch_unit
        if market.epoch_unit != "policy"
        else ("us" if (year, month_number) >= (2025, 1) else "ms")
    )


def _expected_checksum(text: str) -> str:
    token = text.strip().split()[0].lower()
    if len(token) != 64 or any(character not in "0123456789abcdef" for character in token):
        raise ValueError("invalid Binance CHECKSUM object")
    return token


def fetch_archive(
    symbol: str, month: str, client: httpx.Client | None = None, market: BinanceKlineMarket = SPOT
) -> tuple[bytes, str]:
    own_client = client is None
    client = client or httpx.Client(timeout=60, follow_redirects=True)
    try:
        url = archive_url(symbol, month, market)
        response = client.get(url)
        response.raise_for_status()
        checksum_response = client.get(f"{url}.CHECKSUM")
        checksum_response.raise_for_status()
        expected = _expected_checksum(checksum_response.text)
        if _sha256(response.content) != expected:
            raise ValueError("Binance archive checksum mismatch")
        return response.content, expected
    finally:
        if own_client:
            client.close()


def archive_rows(zip_bytes: bytes) -> list[list[str]]:
    with zipfile.ZipFile(__import__("io").BytesIO(zip_bytes)) as archive:
        csv_names = [name for name in archive.namelist() if name.endswith(".csv")]
        if len(csv_names) != 1:
            raise ValueError("archive must contain exactly one CSV")
        with archive.open(csv_names[0]) as source:
            rows = list(csv.reader(line.decode("utf-8") for line in source))
            return rows[1:] if rows and rows[0] and rows[0][0] == "open_time" else rows


def find_gaps(
    rows: Sequence[dict[str, Any]], *, detected_at: datetime, market: BinanceKlineMarket = SPOT
) -> list[GapRecord]:
    result: list[GapRecord] = []
    for previous, current in zip(rows, rows[1:], strict=False):
        expected = previous["open_time"].timestamp() * 1000 + INTERVAL_MS
        actual = current["open_time"].timestamp() * 1000
        if actual > expected:
            result.append(
                GapRecord(
                    gap_id=f"gap_{previous['instrument_id']}_{int(expected)}",
                    source_dataset_id=market.dataset_id,
                    instrument_id=previous["instrument_id"],
                    kind=GapKind.EXCHANGE_GAP,
                    started_at=datetime.fromtimestamp(expected / 1000, tz=UTC),
                    ended_at=current["open_time"],
                    detected_at=detected_at,
                    status="open",
                    reason="missing_in_completed_source_object",
                )
            )
    return result


def _atomic_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.NamedTemporaryFile(
        "w", encoding="utf-8", dir=path.parent, delete=False, suffix=".partial"
    ) as handle:
        json.dump(payload, handle, sort_keys=True, default=str)
        handle.flush()
        os.fsync(handle.fileno())
        temporary = Path(handle.name)
    os.replace(temporary, path)


def _append_jsonl(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a", encoding="utf-8") as handle:
        handle.write(json.dumps(payload, sort_keys=True, default=str) + "\n")
        handle.flush()
        os.fsync(handle.fileno())


def manifest_path(root: Path, market: BinanceKlineMarket) -> Path:
    return root / "control" / "manifests" / f"binance_{market.market_type}_ohlcv.jsonl"


def recover_stale_partials(root: Path) -> list[Path]:
    """Quarantine interrupted partials; they are never mistaken for committed data."""
    recovered: list[Path] = []
    quarantine = root / "quarantine" / "stale_partials"
    for partial in root.rglob("*.partial"):
        if quarantine in partial.parents:
            continue
        quarantine.mkdir(parents=True, exist_ok=True)
        target = quarantine / f"{partial.name}.{_sha256(str(partial).encode())[:12]}"
        os.replace(partial, target)
        recovered.append(target)
    return recovered


def commit_month(
    *,
    root: Path,
    identity: InstrumentIdentity,
    month: str,
    zip_bytes: bytes,
    expected_raw_sha256: str | None = None,
    retrieved_at: datetime | None = None,
    market: BinanceKlineMarket = SPOT,
) -> PilotMeasurement:
    """Normalize one final archive object and durably checkpoint it; reruns are idempotent."""
    retrieved_at = retrieved_at or utc_now()
    checksum = _sha256(zip_bytes)
    if expected_raw_sha256 is not None and checksum != expected_raw_sha256:
        raise ValueError("archive checksum mismatch before persistence")
    raw_path = (
        root
        / "raw"
        / "binance"
        / market.market_type
        / "klines_1m"
        / identity.native_symbol
        / f"{month}-{checksum}.zip"
    )
    raw_path.parent.mkdir(parents=True, exist_ok=True)
    if not raw_path.exists():
        with tempfile.NamedTemporaryFile(
            "wb", dir=raw_path.parent, delete=False, suffix=".partial"
        ) as handle:
            handle.write(zip_bytes)
            handle.flush()
            os.fsync(handle.fileno())
            temporary = Path(handle.name)
        os.replace(temporary, raw_path)
    started = time.perf_counter()
    normalized = [
        normalize_kline(
            row,
            identity=identity,
            source_method="binance_archive_monthly",
            source_uri=archive_url(identity.native_symbol, month, market),
            raw_object_ref=str(raw_path.relative_to(root)),
            epoch_unit=archive_epoch_unit(month, market),
            market=market,
            source_sha256=checksum,
            retrieved_at=retrieved_at,
        )
        for row in archive_rows(zip_bytes)
    ]
    normalized.sort(key=lambda item: item["open_time"])
    if len({row["open_time"] for row in normalized}) != len(normalized):
        raise ValueError("duplicate candle open time in archive")
    gaps = find_gaps(normalized, detected_at=retrieved_at, market=market)
    output = (
        root
        / "normalized"
        / "ohlcv"
        / "v1"
        / "exchange=binance"
        / f"market_type={market.market_type}"
        / f"instrument_id={identity.instrument_id}"
        / "interval=1m"
        / f"month={month}"
        # Full checksum is retained in the manifest and raw object; a short filename
        # keeps the Windows path comfortably below legacy MAX_PATH in nested data roots.
        / f"part-{checksum[:16]}.parquet"
    )
    output.parent.mkdir(parents=True, exist_ok=True)
    if not output.exists():
        table = pa.Table.from_pylist(normalized, schema=ARROW_SCHEMA)
        partial = output.parent / f".{output.name}.partial"
        pq.write_table(table, partial, compression="zstd", use_dictionary=True)
        # Reject a corrupt partial before it becomes visible to readers or manifests.
        if pq.ParquetFile(partial).metadata.num_rows != len(normalized):
            raise ValueError("Parquet validation failed")
        os.replace(partial, output)
    coverage_start, coverage_end = normalized[0]["open_time"], normalized[-1]["close_time"]
    parquet_checksum = _sha256(output.read_bytes())
    event = DataObjectManifest(
        object_id=str(output.relative_to(root)),
        parquet_sha256=parquet_checksum,
        raw_sha256=checksum,
        raw_object_ref=str(raw_path.relative_to(root)),
        source_uri=archive_url(identity.native_symbol, month, market),
        source_kind="archive_monthly",
        source_dataset_id=market.dataset_id,
        coverage_start=coverage_start.isoformat(),
        coverage_end=coverage_end.isoformat(),
        row_count=len(normalized),
        retrieved_at=retrieved_at.isoformat(),
        processed_at=utc_now().isoformat(),
    )
    manifest = manifest_path(root, market)
    # Event id makes append-only manifest idempotent under a normal restart.
    existing = manifest.read_text(encoding="utf-8") if manifest.exists() else ""
    if event.parquet_sha256 not in existing:
        _append_jsonl(manifest, asdict(event))
    for gap in gaps:
        gap_path = root / "control" / "gap_registry" / f"{gap.gap_id}.json"
        if not gap_path.exists():
            _atomic_json(gap_path, gap.model_dump(mode="json"))
    checkpoint = KlineCheckpoint(
        source_dataset_id=market.dataset_id,
        instrument_id=identity.instrument_id,
        cursor=month,
        last_event_time=coverage_end.isoformat(),
        last_knowledge_time=None,
        committed_at=retrieved_at.isoformat(),
    )
    _atomic_json(
        root
        / "control"
        / "checkpoints"
        / f"binance_{market.market_type}_{identity.native_symbol.lower()}_1m.json",
        asdict(checkpoint),
    )
    return PilotMeasurement(
        len(normalized), time.perf_counter() - started, output.stat().st_size, len(zip_bytes)
    )


def fetch_rest_final(
    symbol: str,
    *,
    start_ms: int,
    end_ms: int,
    client: httpx.Client | None = None,
    market: BinanceKlineMarket = SPOT,
) -> list[list[object]]:
    """Paginate a completed REST range; retries bounded 429/timeouts with backoff."""
    own_client = client is None
    client = client or httpx.Client(timeout=30)
    try:
        cursor, by_open = start_ms, {}
        for _page in range(10_000):
            for attempt in range(4):
                try:
                    response = client.get(
                        f"{market.rest_base}{market.rest_path}",
                        params={
                            "symbol": symbol,
                            "interval": "1m",
                            "startTime": cursor,
                            "endTime": end_ms,
                            "limit": 1000,
                        },
                    )
                    if response.status_code in {418, 429}:
                        time.sleep(float(response.headers.get("Retry-After", 2**attempt)))
                        continue
                    response.raise_for_status()
                    payload = response.json()
                    break
                except (httpx.TimeoutException, httpx.TransportError):
                    if attempt == 3:
                        raise
                    time.sleep(2**attempt)
            else:
                raise RuntimeError("REST retry budget exhausted")
            if not isinstance(payload, list):
                raise ValueError("unexpected Binance REST kline response")
            final_rows = [row for row in payload if int(row[6]) < end_ms]
            for row in final_rows:
                previous = by_open.get(int(row[0]))
                if previous is not None and previous != row:
                    raise ValueError("conflicting REST candle for the same open time")
                by_open[int(row[0])] = row
            if not payload or len(payload) < 1000:
                return [by_open[key] for key in sorted(by_open)]
            next_cursor = int(payload[-1][0]) + INTERVAL_MS
            if next_cursor <= cursor or next_cursor >= end_ms:
                return [by_open[key] for key in sorted(by_open)]
            cursor = next_cursor
        raise RuntimeError("REST pagination safety limit reached")
    finally:
        if own_client:
            client.close()


def commit_rest_tail(
    *,
    root: Path,
    identity: InstrumentIdentity,
    rows: Sequence[Sequence[object]],
    conservative_cutoff_ms: int,
    retrieved_at: datetime | None = None,
    market: BinanceKlineMarket = SPOT,
) -> PilotMeasurement:
    """Persist final REST candles as an immutable micro-generation.

    This is intentionally separate from archive partitions: later archive overlap
    is reconciled by key/value equality, never overwritten silently.
    """
    retrieved_at = retrieved_at or utc_now()
    final = [list(row) for row in rows if int(row[6]) < conservative_cutoff_ms]
    if not final:
        raise ValueError("REST tail contains no final candles")
    raw_bytes = json.dumps(final, separators=(",", ":")).encode()
    raw_hash = _sha256(raw_bytes)
    raw_path = (
        root
        / "raw"
        / "binance"
        / market.market_type
        / "klines_1m_rest"
        / identity.native_symbol
        / f"{raw_hash}.json"
    )
    raw_path.parent.mkdir(parents=True, exist_ok=True)
    if not raw_path.exists():
        with tempfile.NamedTemporaryFile(
            "wb", dir=raw_path.parent, delete=False, suffix=".partial"
        ) as h:
            h.write(raw_bytes)
            h.flush()
            os.fsync(h.fileno())
            temporary = Path(h.name)
        os.replace(temporary, raw_path)
    normalized = [
        normalize_kline(
            row,
            identity=identity,
            source_method="binance_rest_tail",
            source_uri=f"{market.rest_base}{market.rest_path}",
            raw_object_ref=str(raw_path.relative_to(root)),
            epoch_unit="ms",
            source_sha256=raw_hash,
            retrieved_at=retrieved_at,
            market=market,
        )
        for row in final
    ]
    normalized.sort(key=lambda row: row["open_time"])
    by_time = {row["open_time"]: row for row in normalized}
    if len(by_time) != len(normalized):
        raise ValueError("duplicate REST candle open time")
    started = time.perf_counter()
    generation = raw_hash[:16]
    output = (
        root
        / "normalized"
        / "ohlcv"
        / "v1"
        / "exchange=binance"
        / f"market_type={market.market_type}"
        / f"instrument_id={identity.instrument_id}"
        / "interval=1m"
        / "tail_generations"
        / f"part-{generation}.parquet"
    )
    output.parent.mkdir(parents=True, exist_ok=True)
    if not output.exists():
        partial = output.parent / f".{output.name}.partial"
        pq.write_table(
            pa.Table.from_pylist(normalized, schema=ARROW_SCHEMA), partial, compression="zstd"
        )
        if pq.ParquetFile(partial).metadata.num_rows != len(normalized):
            raise ValueError("Parquet validation failed")
        os.replace(partial, output)
    parquet_hash = _sha256(output.read_bytes())
    manifest = DataObjectManifest(
        object_id=str(output.relative_to(root)),
        parquet_sha256=parquet_hash,
        raw_sha256=raw_hash,
        raw_object_ref=str(raw_path.relative_to(root)),
        source_uri=f"{market.rest_base}{market.rest_path}",
        source_kind="rest_tail",
        source_dataset_id=market.dataset_id,
        coverage_start=normalized[0]["open_time"].isoformat(),
        coverage_end=normalized[-1]["close_time"].isoformat(),
        row_count=len(normalized),
        retrieved_at=retrieved_at.isoformat(),
        processed_at=utc_now().isoformat(),
    )
    manifest_file = manifest_path(root, market)
    existing = manifest_file.read_text(encoding="utf-8") if manifest_file.exists() else ""
    if parquet_hash not in existing:
        _append_jsonl(manifest_file, asdict(manifest))
    checkpoint = KlineCheckpoint(
        source_dataset_id=market.dataset_id,
        instrument_id=identity.instrument_id,
        cursor=f"tail:{generation}",
        last_event_time=normalized[-1]["close_time"].isoformat(),
        last_knowledge_time=None,
        committed_at=retrieved_at.isoformat(),
    )
    _atomic_json(
        root
        / "control"
        / "checkpoints"
        / f"binance_{market.market_type}_{identity.native_symbol.lower()}_1m_rest_tail.json",
        asdict(checkpoint),
    )
    return PilotMeasurement(
        len(normalized), time.perf_counter() - started, output.stat().st_size, len(raw_bytes)
    )


def reconcile_overlap(archive: Sequence[dict[str, Any]], rest: Sequence[dict[str, Any]]) -> None:
    """Raise on any same-key source disagreement; caller may then quarantine both."""
    archive_by_key = {row["open_time"]: row for row in archive}
    fields = ("open", "high", "low", "close", "base_volume", "quote_volume", "trade_count")
    for row in rest:
        archive_row = archive_by_key.get(row["open_time"])
        if archive_row and any(archive_row[field] != row[field] for field in fields):
            raise ValueError(f"archive/REST conflict at {row['open_time'].isoformat()}")


def commit_rest_tail_v2(
    *, root: Path, identity: InstrumentIdentity, rows: Sequence[Sequence[object]],
    conservative_cutoff_ms: int, retrieved_at: datetime | None = None,
    market: BinanceKlineMarket = SPOT,
) -> Path:
    """Write future Binance REST generations in the exchange-neutral V2 schema.

    This deliberately does not rewrite the immutable V1 archive/tail outputs.
    """
    retrieved_at = retrieved_at or utc_now()
    final = [list(row) for row in rows if int(row[6]) < conservative_cutoff_ms]
    if not final:
        raise ValueError("REST tail contains no final candles")
    raw = json.dumps(final, separators=(",", ":")).encode()
    v2_identity = identity
    if market is USDM:
        v2_identity = InstrumentIdentity(
            exchange=identity.exchange,
            native_symbol=identity.native_symbol,
            market_type="perpetual",
            contract_type=identity.contract_type,
            base_asset=identity.base_asset,
            quote_asset=identity.quote_asset,
            settle_asset=identity.settle_asset,
            quantity_unit=identity.quantity_unit,
            notional_unit=identity.notional_unit,
        )
    normalized = [
        normalize_kline(
            row, identity=v2_identity, source_method="binance_rest_tail_v2",
            source_uri=f"{market.rest_base}{market.rest_path}", raw_object_ref="",
            epoch_unit="ms", source_sha256="", retrieved_at=retrieved_at, market=market,
            schema_version="2.1.0", collector_version="0.2.0", normalization_version="2.0.0",
        ) for row in final
    ]
    # V2 records source fields separately; Binance fields are still genuinely supplied.
    canonical_market_type = "perpetual" if market is USDM else "spot"
    for row in normalized:
        row["market_type"] = canonical_market_type
        row["interval"] = "PT1M"
        row["source_volume"] = row["base_volume"]
        row["source_volume_unit"] = v2_identity.base_asset
        row["source_turnover"] = row["quote_volume"]
        row["source_turnover_unit"] = v2_identity.quote_asset
        row.update({"candle_source":"exchange", "aggregation_version":None, "source_revision_id":None,
                    "exchange_timestamp":None, "source_published_at":None, "received_at":retrieved_at,
                    "clock_offset_ms":None, "clock_uncertainty_ms":None,
                    "ingestion_run_id":None, "dq_flags":[]})
    desc = OhlcvSourceDescriptor("binance", canonical_market_type, market.contract_type, market.dataset_id)
    return commit_rows(
        root=root, descriptor=desc, identity=v2_identity, rows=normalized, raw_bytes=raw,
        source_uri=f"{market.rest_base}{market.rest_path}",
        request={"symbol": v2_identity.native_symbol, "interval": "1m", "closed_cutoff": conservative_cutoff_ms},
        generation=f"tail-{_sha256(raw)[:16]}",
    )
