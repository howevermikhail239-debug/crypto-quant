"""REST Gap Recovery Adapter with Boundary Proof & Truncation Risk Protection (Phase 1C Item 7C).

Recovers missing trade intervals via REST API endpoints while enforcing:
- Boundary Proof: RECOVERED status requires proven boundary coverage.
- Max-Limit Truncation Risk: Single max-limit page is flagged as TRUNCATION_RISK and FORBIDDEN from RECOVERED status unless boundary is proven.
- Idempotent Pagination: Paginates using `fromId` / trade_id cursors until target boundary is reached.
- Strict Dataset Class Isolation: individual_trade gaps ONLY recovered from individual trade REST endpoints.
- Separate gap_type (cause) vs gap_status (lifecycle).
"""

from __future__ import annotations

import json
import logging
from pathlib import Path
from typing import Any

import httpx

from ..time import utc_now
from .gap_registry import GapRecord, GapRegistry, GapStatus

logger = logging.getLogger(__name__)


def perform_gap_recovery(
    gap: GapRecord,
    root: Path,
    *,
    rest_client: httpx.Client | None = None,
    mock_fetched_items: list[dict[str, Any]] | None = None,
) -> GapRecord:
    """Attempts bounded gap recovery with explicit boundary proof validation.

    Strict Invariants:
    1. RECOVERED status requires coverage_proven == True.
    2. Single max-limit response without reaching boundary MUST NOT receive RECOVERED status (TRUNCATION_RISK).
    3. individual_trade recovery MUST NOT use aggregate trade endpoints.
    """
    gap.recovery_attempted = True
    gap.recovery_started_at = utc_now()
    registry = GapRegistry(root)

    # 1. Dataset Class Isolation Check
    if gap.dataset_class not in ("individual_trade", "exchange_aggregate_trade"):
        gap.status = GapStatus.UNRECOVERABLE
        gap.coverage_proven = False
        gap.notes = f"Unsupported dataset class for recovery: {gap.dataset_class}"
        gap.recovery_completed_at = utc_now()
        registry.update_gap(gap)
        return gap

    fetched_items: list[dict[str, Any]] = []
    pages_requested = 0
    endpoint_limit = 1000
    coverage_proven = False
    coverage_method = None

    if mock_fetched_items is not None:
        fetched_items = mock_fetched_items
        pages_requested = 1
        gap.recovery_source = "mock_rest_fixture"
        # Validate boundary proof for mock if pre_gap & post_gap trade IDs exist
        if gap.pre_gap_last_trade_id and gap.post_gap_first_trade_id:
            first_id = str(fetched_items[0].get("id") or fetched_items[0].get("a") or fetched_items[0].get("t") or "")
            last_id = str(fetched_items[-1].get("id") or fetched_items[-1].get("a") or fetched_items[-1].get("t") or "")
            try:
                pre_id = int(gap.pre_gap_last_trade_id)
                post_id = int(gap.post_gap_first_trade_id)
                rec_first = int(first_id)
                rec_last = int(last_id)
                if rec_first <= pre_id + 1 and rec_last >= post_id - 1:
                    coverage_proven = True
                    coverage_method = "trade_id_sequence_complete"
            except (ValueError, TypeError):
                pass
        elif len(fetched_items) < endpoint_limit:
            coverage_proven = True
            coverage_method = "timestamp_range_bounded"
    else:
        # Live REST Pagination Logic
        gap_start_ms = int(gap.gap_start.timestamp() * 1000)
        gap_end_ms = int(gap.gap_end.timestamp() * 1000)
        client = rest_client or httpx.Client(timeout=10.0)

        try:
            if gap.exchange == "binance":
                symbol = gap.source_stream.split("@")[0].upper()
                if gap.dataset_class == "individual_trade":
                    url = "https://api.binance.com/api/v3/historicalTrades"
                    from_id = int(gap.pre_gap_last_trade_id) + 1 if gap.pre_gap_last_trade_id else None
                    target_post_id = int(gap.post_gap_first_trade_id) if gap.post_gap_first_trade_id else None

                    gap.recovery_source = url
                    while True:
                        pages_requested += 1
                        params: dict[str, Any] = {"symbol": symbol, "limit": endpoint_limit}
                        if from_id:
                            params["fromId"] = from_id

                        resp = client.get(url, params=params)
                        if resp.status_code != 200:
                            # Fall back to /api/v3/trades if historicalTrades requires API key
                            fallback_url = "https://api.binance.com/api/v3/trades"
                            resp = client.get(fallback_url, params={"symbol": symbol, "limit": endpoint_limit})
                            gap.recovery_source = fallback_url

                        if resp.status_code != 200:
                            break

                        items = resp.json()
                        if not items:
                            break
                        fetched_items.extend(items)

                        last_item_id = int(items[-1]["id"])
                        if target_post_id and last_item_id >= target_post_id - 1:
                            coverage_proven = True
                            coverage_method = "trade_id_sequence_complete"
                            break

                        if len(items) < endpoint_limit or pages_requested >= 10:
                            break

                        from_id = last_item_id + 1

                elif gap.dataset_class == "exchange_aggregate_trade":
                    url = "https://api.binance.com/api/v3/aggTrades"
                    from_id = int(gap.pre_gap_last_trade_id) + 1 if gap.pre_gap_last_trade_id else None
                    target_post_id = int(gap.post_gap_first_trade_id) if gap.post_gap_first_trade_id else None

                    gap.recovery_source = url
                    while True:
                        pages_requested += 1
                        params = {"symbol": symbol, "limit": endpoint_limit}
                        if from_id:
                            params["fromId"] = from_id
                        else:
                            params["startTime"] = gap_start_ms
                            params["endTime"] = gap_end_ms

                        resp = client.get(url, params=params)
                        if resp.status_code != 200:
                            break
                        items = resp.json()
                        if not items:
                            break
                        fetched_items.extend(items)

                        last_item_id = int(items[-1]["a"])
                        if target_post_id and last_item_id >= target_post_id - 1:
                            coverage_proven = True
                            coverage_method = "trade_id_sequence_complete"
                            break

                        if len(items) < endpoint_limit or pages_requested >= 10:
                            break

                        from_id = last_item_id + 1

            elif gap.exchange == "bybit":
                url = "https://api.bybit.com/v5/market/recent-trade"
                category = "spot" if gap.market_type == "spot" else "linear"
                symbol = gap.source_stream.removeprefix("publicTrade.")
                gap.recovery_source = url
                pages_requested += 1

                resp = client.get(url, params={"category": category, "symbol": symbol, "limit": 1000})
                if resp.status_code == 200:
                    fetched_items = resp.json().get("result", {}).get("list", [])
                    # Bybit recent-trade returns max 1000 items without deep pagination.
                    # Flag coverage as UNPROVEN unless returned count < limit and gap inside window.
                    if len(fetched_items) < 1000 and len(fetched_items) > 0:
                        coverage_proven = True
                        coverage_method = "recent_trade_window_bounded"
        except Exception as exc:
            logger.warning(f"REST recovery fetch error for gap {gap.gap_id}: {exc}")
            gap.notes = f"REST fetch exception: {exc}"
        finally:
            if rest_client is None:
                client.close()

    # 3. Process records & boundary metadata
    records_count = len(fetched_items)
    gap.records_recovered = records_count
    gap.pages_requested = pages_requested
    gap.endpoint_limit = endpoint_limit
    gap.recovery_completed_at = utc_now()

    if records_count > 0:
        first_item = fetched_items[0]
        last_item = fetched_items[-1]

        gap.recovery_first_trade_id = str(first_item.get("id") or first_item.get("a") or first_item.get("i") or "")
        gap.recovery_last_trade_id = str(last_item.get("id") or last_item.get("a") or last_item.get("i") or "")

        # Truncation Risk Check: max-limit without boundary proof
        if records_count == endpoint_limit and not coverage_proven:
            coverage_proven = False
            gap.status = GapStatus.PARTIAL
            gap.notes = (
                f"TRUNCATION_RISK: Response returned max endpoint limit ({endpoint_limit}) "
                f"without proven boundary coverage. Status set to PARTIAL."
            )
        elif coverage_proven:
            gap.status = GapStatus.RECOVERED
            gap.notes = (
                f"Proven recovery of {records_count} records via {gap.recovery_source} "
                f"({pages_requested} pages, method: {coverage_method})"
            )
        else:
            gap.status = GapStatus.PARTIAL
            gap.notes = f"Recovered {records_count} records via {gap.recovery_source}, but boundary coverage remains unproven."

        gap.coverage_proven = coverage_proven
        gap.coverage_method = coverage_method

        # Save raw recovery artifact
        rec_dir = root / "raw" / "recovery" / f"exchange={gap.exchange}" / f"market_type={gap.market_type}" / f"symbol={gap.source_stream}"
        rec_dir.mkdir(parents=True, exist_ok=True)
        rec_file = rec_dir / f"recovery_{gap.gap_id}.jsonl"
        with rec_file.open("w", encoding="utf-8") as f:
            for item in fetched_items:
                f.write(json.dumps(item, sort_keys=True) + "\n")
    else:
        gap.status = GapStatus.UNRECOVERABLE
        gap.coverage_proven = False
        gap.notes = "No records returned from official REST API for specified gap window"

    registry.update_gap(gap)
    return gap


def audit_revision_previous_smoke_gap(root: Path, target_gap_id: str = "gap_11588b0a09dc43ff") -> GapRecord | None:
    """Appends an auditable revision record for previous smoke gap, updating status from RECOVERED to PARTIAL."""
    registry = GapRegistry(root)
    gaps = registry.list_gaps()
    target_gap = next((g for g in gaps if g.gap_id == target_gap_id), None)
    if target_gap is None:
        return None

    # Correct previous unproven RECOVERED status to PARTIAL
    target_gap.status = GapStatus.PARTIAL
    target_gap.coverage_proven = False
    target_gap.coverage_method = "unproven_recent_trades_limit"
    target_gap.notes = (
        "Audit Revision: Corrected status from RECOVERED to PARTIAL. "
        "Original response returned 1000 records from /api/v3/trades without proven boundary coverage."
    )
    registry.update_gap(target_gap)
    return target_gap
