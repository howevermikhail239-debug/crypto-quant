"""Bybit V5 REST 1-minute OHLCV bootstrap (closed candles only)."""

from __future__ import annotations

import json
import time
from collections.abc import Sequence
from datetime import datetime
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Any

import httpx

from ...identity import InstrumentIdentity
from ...time import parse_epoch, utc_now
from ..ohlcv_v2 import OhlcvSourceDescriptor, atomic_json, commit_rows, sha256

BASE = "https://api.bybit.com"
INTERVAL_MS = 60_000
IP_BAN_COOLDOWN_SECONDS = 600
SPOT = OhlcvSourceDescriptor("bybit", "spot", "spot", "bybit.spot.klines.1m")
LINEAR = OhlcvSourceDescriptor("bybit", "perpetual", "linear_perpetual", "bybit.linear.klines.1m")


def identity(symbol: str, category: str) -> InstrumentIdentity:
    base = symbol.removesuffix("USDT")
    if category == "spot":
        return InstrumentIdentity(
            exchange="bybit",
            native_symbol=symbol,
            market_type="spot",
            contract_type="spot",
            base_asset=base,
            quote_asset="USDT",
            quantity_unit=base,
            notional_unit="USDT",
        )
    if category == "linear":
        return InstrumentIdentity(
            exchange="bybit",
            native_symbol=symbol,
            market_type="perpetual",
            contract_type="linear_perpetual",
            base_asset=base,
            quote_asset="USDT",
            settle_asset="USDT",
            quantity_unit=base,
            notional_unit="USDT",
        )
    raise ValueError("category must be spot or linear")


def descriptor(category: str) -> OhlcvSourceDescriptor:
    return (
        SPOT
        if category == "spot"
        else LINEAR
        if category == "linear"
        else (_ for _ in ()).throw(ValueError("unsupported category"))
    )


def _d(x: object, name: str) -> Decimal:
    try:
        value = Decimal(str(x))
    except (InvalidOperation, ValueError) as e:
        raise ValueError(f"invalid {name}") from e
    if not value.is_finite():
        raise ValueError(f"invalid {name}")
    return value


def normalize(
    row: Sequence[object],
    *,
    ident: InstrumentIdentity,
    category: str,
    source_uri: str,
    retrieved_at: datetime,
    source_method: str = "bybit_rest_bootstrap",
) -> dict[str, Any]:
    if len(row) != 7:
        raise ValueError("Bybit kline requires exactly 7 fields")
    epoch = int(row[0])
    if epoch % INTERVAL_MS:
        raise ValueError("Bybit kline open time must align to one minute")
    o, h, low, c, volume, turnover = (
        _d(x, n)
        for x, n in zip(
            row[1:], ("open", "high", "low", "close", "volume", "turnover"), strict=True
        )
    )
    if min(o, h, low, c) <= 0 or volume < 0 or turnover < 0 or low > min(o, c) or h < max(o, c):
        raise ValueError("invalid OHLCV bounds")
    desc = descriptor(category)
    # Linear mapping is documented. Spot mapping is fixture-gated and recorded
    # explicitly in its contract; the raw source fields remain preserved either way.
    return {
        "instrument_id": ident.instrument_id,
        "exchange": "bybit",
        "market_type": desc.market_type,
        "contract_type": desc.contract_type,
        "native_symbol": ident.native_symbol,
        "interval": "PT1M",
        "is_closed": True,
        "open_time": parse_epoch(epoch, unit="ms"),
        "close_time": parse_epoch(epoch + INTERVAL_MS - 1, unit="ms"),
        "open": o,
        "high": h,
        "low": low,
        "close": c,
        "base_volume": volume,
        "quote_volume": turnover,
        "source_volume": volume,
        "source_volume_unit": ident.base_asset,
        "source_turnover": turnover,
        "source_turnover_unit": ident.quote_asset,
        "trade_count": None,
        "taker_buy_base_volume": None,
        "taker_buy_quote_volume": None,
        "source_method": source_method,
        "source_dataset_id": desc.dataset_id,
        "source_uri": source_uri,
        "observation_id": f"{ident.instrument_id}:PT1M:{epoch}:{source_method}",
        "raw_object_ref": "",
        "source_object_sha256": "",
        "retrieved_at": retrieved_at,
        "processed_at": retrieved_at,
        "knowledge_time": None,
        "knowledge_time_basis": "unknown_historical",
        "schema_version": "2.1.0",
        "collector_version": "0.2.0",
        "normalization_version": "2.0.0",
        "data_contract_version": "1.0.0",
        "candle_source": "exchange",
        "aggregation_version": None,
        "source_revision_id": None,
        "exchange_timestamp": None,
        "source_published_at": None,
        "received_at": retrieved_at,
        "clock_offset_ms": None,
        "clock_uncertainty_ms": None,
        "ingestion_run_id": None,
        "dq_flags": [],
    }


def fetch_server_time(client: httpx.Client | None = None) -> int:
    own = client is None
    client = client or httpx.Client(timeout=30)
    try:
        response = client.get(f"{BASE}/v5/market/time")
        response.raise_for_status()
        p = response.json()
        if p.get("retCode") != 0:
            raise ValueError("Bybit time retCode")
        return int(p["result"]["timeNano"]) // 1_000_000
    finally:
        if own:
            client.close()


def fetch_metadata(
    symbol: str, category: str, client: httpx.Client | None = None
) -> dict[str, Any]:
    own = client is None
    client = client or httpx.Client(timeout=30)
    try:
        response = client.get(
            f"{BASE}/v5/market/instruments-info", params={"category": category, "symbol": symbol}
        )
        response.raise_for_status()
        p = response.json()
        if p.get("retCode") != 0 or p.get("result", {}).get("category") != category:
            raise ValueError("Bybit metadata category mismatch")
        items = [x for x in p["result"].get("list", []) if x.get("symbol") == symbol]
        if len(items) != 1:
            raise ValueError("Bybit instruments response did not return exactly requested symbol")
        x = items[0]
        lot = x.get("lotSizeFilter", {})
        price = x.get("priceFilter", {})
        return {
            "exchange": "bybit",
            "market_type": descriptor(category).market_type,
            "contract_type": descriptor(category).contract_type,
            "native_symbol": symbol,
            "base_asset": x["baseCoin"],
            "quote_asset": x["quoteCoin"],
            "settle_asset": x.get("settleCoin"),
            "status": x["status"],
            "price_tick": price.get("tickSize"),
            "quantity_step": lot.get("qtyStep"),
            "launch_time": x.get("launchTime"),
            "retrieved_at": utc_now().isoformat(),
            "source_uri": f"{BASE}/v5/market/instruments-info?category={category}&symbol={symbol}",
            "source_envelope": p,
            "raw": x,
        }
    finally:
        if own:
            client.close()


def save_metadata_snapshot(root: Path, snapshot: dict[str, Any]) -> Path:
    digest = sha256(json.dumps(snapshot, sort_keys=True, default=str).encode())
    p = (
        root
        / "control"
        / "instrument_metadata"
        / f"bybit_{snapshot['market_type']}_{snapshot['native_symbol']}_{digest}.json"
    )
    if not p.exists():
        atomic_json(p, snapshot)
    return p


def fetch_final(
    symbol: str,
    *,
    category: str,
    start_ms: int,
    end_exclusive_ms: int,
    conservative_cutoff_ms: int,
    client: httpx.Client | None = None,
) -> tuple[list[list[object]], list[dict[str, Any]]]:
    """Reverse-order V5 pagination using a half-open internal range.
    End is sent as end_exclusive-1 because live API observed an inclusive end.
    """
    own = client is None
    client = client or httpx.Client(timeout=30)
    cursor = end_exclusive_ms
    by_open = {}
    pages = []
    try:
        for _ in range(10000):
            req = {
                "category": category,
                "symbol": symbol,
                "interval": "1",
                "start": start_ms,
                "end": cursor - 1,
                "limit": 1000,
            }
            for attempt in range(4):
                try:
                    r = client.get(f"{BASE}/v5/market/kline", params=req)
                    if r.status_code == 403:
                        raise RuntimeError(
                            f"Bybit IP ban/cooldown: stop requests for {IP_BAN_COOLDOWN_SECONDS}s"
                        )
                    if r.status_code == 429:
                        time.sleep(2**attempt)
                        continue
                    r.raise_for_status()
                    p = r.json()
                    break
                except (httpx.TransportError, httpx.TimeoutException):
                    if attempt == 3:
                        raise
                    time.sleep(2**attempt)
            else:
                raise RuntimeError("Bybit REST retry budget exhausted")
            if p.get("retCode") == 10006:
                reset = r.headers.get("X-Bapi-Limit-Reset-Timestamp")
                raise RuntimeError(f"Bybit API rate limit retCode=10006; reset={reset}")
            if (
                p.get("retCode") != 0
                or p.get("result", {}).get("category") != category
                or p["result"].get("symbol") != symbol
            ):
                raise ValueError("Bybit kline envelope identity mismatch")
            values = p["result"].get("list", [])
            pages.append({"request": req, "response": p})
            if not values:
                break
            oldest = None
            for row in values:
                open_ms = int(row[0])
                if not start_ms <= open_ms < cursor:
                    raise ValueError("Bybit returned a candle outside the requested half-open page")
                oldest = open_ms if oldest is None else min(oldest, open_ms)
                if (
                    start_ms <= open_ms < end_exclusive_ms
                    and open_ms + INTERVAL_MS <= conservative_cutoff_ms
                ):
                    prior = by_open.get(open_ms)
                    if prior is not None and prior != row:
                        raise ValueError("conflicting Bybit candle")
                    by_open[open_ms] = row
            if oldest is None or oldest <= start_ms:
                break
            if oldest >= cursor:
                raise RuntimeError("Bybit reverse pagination did not advance")
            cursor = oldest
        return [by_open[k] for k in sorted(by_open)], pages
    finally:
        if own:
            client.close()


def commit_bootstrap(
    *,
    root: Path,
    ident: InstrumentIdentity,
    category: str,
    start_ms: int,
    end_exclusive_ms: int,
    client: httpx.Client | None = None,
    retrieved_at: datetime | None = None,
) -> Path:
    retrieved_at = retrieved_at or utc_now()
    cutoff = fetch_server_time(client)
    rows, pages = fetch_final(
        ident.native_symbol,
        category=category,
        start_ms=start_ms,
        end_exclusive_ms=end_exclusive_ms,
        conservative_cutoff_ms=cutoff,
        client=client,
    )
    normalized = [
        normalize(
            x,
            ident=ident,
            category=category,
            source_uri=f"{BASE}/v5/market/kline",
            retrieved_at=retrieved_at,
        )
        for x in rows
    ]
    raw = json.dumps(pages, separators=(",", ":"), sort_keys=True).encode()
    generation = f"{start_ms}-{end_exclusive_ms}"
    return commit_rows(
        root=root,
        descriptor=descriptor(category),
        identity=ident,
        rows=normalized,
        raw_bytes=raw,
        source_uri=f"{BASE}/v5/market/kline",
        request={
            "category": category,
            "symbol": ident.native_symbol,
            "interval": "1",
            "start": start_ms,
            "end_exclusive": end_exclusive_ms,
            "closed_cutoff": cutoff,
        },
        generation=generation,
    )
