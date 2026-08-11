# PHASE 1D.3C — Independent Acceptance Audit

**Final status:** FINAL DONE / ACCEPTED

**Audit date:** 2026-08-11

**Implementation under audit:** `5abcfae5acec1dcea8da6d752c46cee7367285c0`

**Scope:** Binance USD-M BTCUSDT `<symbol>@forceOrder` only. No ETHUSDT or later phase was started.

## Acceptance checklist

| # | Audit item | Status | Evidence / outcome |
|---:|---|---|---|
| 1 | Current Git state | CONFIRMED_OK | `HEAD=5abcfae` at audit start, branch `master`, clean tree/index. |
| 2 | Actual WebSocket URL/mode | CONFIRMED_OK | Documented `REQUEST_SUBSCRIBE`: connect to `/market/ws`, send JSON `SUBSCRIBE`. Mock regression and genuine ACK passed. |
| 3 | Source-contract consistency | CONFIRMED_DEFECT_FIXED | YAML/code/tests/live row were consistent; stale higher-level `latest` wording was aligned to the frozen conflict contract. |
| 4 | Latest/largest conflict | CONFIRMED_DEFECT_FIXED | Remains `DOC_CONFLICT_LATEST_VS_LARGEST`; no algorithm inferred from one observation. |
| 5 | Source incompleteness versus bad row | FALSE_POSITIVE | `source_claimed_completeness` is first-class; Technical Design says flags do not automatically delete rows; no generic consumer invalidates every non-empty `dq_flags`. Regression proves the flagged row is structurally valid. |
| 6 | Genuine byte-to-row reconciliation | CONFIRMED_OK | All requested raw fields exactly matched the canonical row. |
| 7 | Raw hash/message ID | CONFIRMED_OK | Raw file hash equals manifest and report; `message_id` equals SHA-256 of the exact wire frame without JSON reserialization. |
| 8 | Parquet/manifest hash | CONFIRMED_OK | File exists; disk hash equals manifest and report; identity matches. |
| 9 | Immutable generations | CONFIRMED_OK | Fixture G1 remains byte-identical after G2; production G1 remained byte-identical through idempotent recovery. |
| 10 | Checkpoint semantics | CONFIRMED_DEFECT_FIXED | Added exact last raw/Parquet refs and hashes; no completeness claim. Production checkpoint refreshed without changing G1 or manifest. |
| 11 | Knowledge time / clock | CONFIRMED_OK | `knowledge_time=received_at`; E/T/local time remain separate. Source E was 1.598 s and T 0.590 s ahead of local receive clock; recorded as a clock-quality observation, not latency. Long-window clock characterization is deferred to soak. |
| 12 | Event-time mapping | CONFIRMED_OK | `event_time=o.T`; `exchange_timestamp=E`; both source integers preserved. |
| 13 | Quantity unit | CONFIRMED_OK | BTCUSDT USD-M identity/metadata resolves q/l/z to base BTC; no multiplier or notional guessed. |
| 14 | q/l/z separation | CONFIRMED_OK | Original, last-filled and accumulated-filled quantities remain independently recoverable. |
| 15 | p/ap separation | CONFIRMED_OK | Order price and average price remain distinct; neither is called bankruptcy price. |
| 16 | Side semantics | CONFIRMED_OK | BUY/SELL source side preserved; `position_side_liquidated=UNKNOWN`. |
| 17 | Snapshot append semantics | CONFIRMED_OK | Different observations append; exact replay deduplicates; previous generation is not replaced. |
| 18 | Dedup boundary | CONFIRMED_OK | Exact-wire replay only; different envelopes are not heuristic-collapsed. |
| 19 | Native event/order ID | CONFIRMED_OK | Absent in public payload; no USER_DATA order ID borrowed. |
| 20 | USER_DATA REST | CONFIRMED_OK | `/fapi/v1/forceOrders` appears only in contract/docs/tests as unsuitable private history; collector has no key/signature/backfill use. |
| 21 | Historical evidence | CONFIRMED_DEFECT_FIXED | Two empty WAF-challenge files were reclassified invalid and preserved; hashed excerpts derived from the existing official `llms-full` capture were appended to the same evidence index. |
| 22 | Synthetic contamination | CONFIRMED_OK | Active synthetic rows 0; active genuine BTC rows 1. Fixtures use temporary roots. |
| 23 | Genuine row source limitation | CONFIRMED_OK | Row stores incomplete source class, 1000 ms window and conflict rule. |
| 24 | Future feature safety | CONFIRMED_OK | Contract/completeness metadata machine-readably forbids complete-tape interpretations. No feature code added. |
| 25 | Health semantics | DEFERRED_BY_DESIGN | Generic `health` PASS is not liquidation completeness evidence. Liquidation-specific availability/local-gap health belongs to the later soak; current row and manifest already preserve source/local completeness separately. |
| 26 | HANDOFF | CONFIRMED_DEFECT_FIXED | Updated only after acceptance; existing file retained. |
| 27 | Validation | CONFIRMED_OK | Focused/full/Ruff/config/health/lock/diff checks passed. |
| 28 | Commit policy | CONFIRMED_OK | Narrow acceptance commit; no ETH or unrelated refactor. |
| 29 | Final report | CONFIRMED_OK | This report contains the required explicit sections and classifications. |
| 30 | Acceptance | CONFIRMED_OK | All acceptance invariants satisfied; PHASE 1D.3C closes as FINAL DONE / ACCEPTED. |

## WebSocket

```text
mode: REQUEST_SUBSCRIBE
effective URL: wss://fstream.binance.com/market/ws
subscription request: {"method":"SUBSCRIBE","params":["btcusdt@forceOrder"],"id":1}
topic: btcusdt@forceOrder
```

The current Binance migration notice explicitly supports JSON `SUBSCRIBE`, maps `<symbol>@forceOrder` to Market, and documents the routed Market base. Path-based raw mode (`/market/ws/<streamName>`) is also supported but is not the mode used by this collector.

## Source completeness

```text
selection window: 1000 ms
selection rule: DOC_CONFLICT_LATEST_VS_LARGEST
source event completeness: INCOMPLETE_THROTTLED_SNAPSHOT
row structural validity: PASS
source incompleteness incorrectly invalidates row: NO
```

## Genuine event reconciliation

| Field | Raw | Normalized |
|---|---:|---:|
| `s` | `BTCUSDT` | `symbol=BTCUSDT` |
| `S` | `SELL` | `source_side=SELL` |
| `q` | `0.002` | `source_quantity=0.002` |
| `l` | `0.002` | `last_filled_quantity=0.002` |
| `z` | `0.002` | `accumulated_filled_quantity=0.002` |
| `p` | `64048.20` | `source_price=64048.20` |
| `ap` | `64301.90` | `average_fill_price=64301.90` |
| `X` | `FILLED` | `order_status=FILLED` |
| `o` | `LIMIT` | `order_type=LIMIT` |
| `f` | `IOC` | `time_in_force=IOC` |
| `E` | `1786451007391` | `source_event_time_ms=1786451007391` |
| `T` | `1786451006384` | `source_order_trade_time_ms=1786451006384` |

```text
raw hash: PASS
ec277af4f4238c71fd347b258df64c7e4537a454c34d010841ab65f88f483ad8

Parquet hash: PASS
5cdc49ae06f6437807b0ad4ba5aade722804b3a805b490130a8cff6a418b3cc7

raw → normalized fields: PASS
manifest: PASS
checkpoint: PASS
instrument_id: ins_dae8124762a847d14263
```

Decimal source lexemes are preserved as strings; no float/scientific conversion or precision loss occurred.

## Time, quantity, price, and side

```text
E semantic: exchange push/event time
T semantic: order trade time
canonical event_time: T
canonical exchange_timestamp: E
knowledge_time: received_at

q: original order quantity, BTC
l: last filled quantity, BTC
z: accumulated filled quantity, BTC
precision preservation: PASS

p: order price, USDT per BTC
ap: average fill price, USDT per BTC
kept distinct: PASS

source side: SELL preserved
position_side_liquidated: UNKNOWN
side status: PASS
```

## Dedup and data root

```text
exact-wire replay: PASS
cross-envelope economic dedup: NOT GUARANTEED
native event ID: absent

active synthetic rows: 0
active genuine BTC rows: 1
```

Production G1, its raw object, and manifest remained byte-identical during acceptance recovery. Only the mutable checkpoint was advanced to include exact generation references.

Evidence index after remediation:

```text
27 entries
27/27 artifact hashes valid
25 accepted evidence artifacts
2 preserved INVALID_EMPTY_WAF_CHALLENGE artifacts
```

## Portability

```text
Antigravity repository files: 5/5 PASS
backup copies: 5/5 PASS
integrity manifest: PASS
HANDOFF.md: present
```

## Known limitation carried forward

- The source is incomplete by design and cannot establish complete event counts, cascade reconstruction, or complete liquidation volume.
- The official `latest`/`largest` conflict remains unresolved.
- Public historical recovery is not verified; missed observations are unrecoverable or unknown.
- The short pilot does not establish long-window local completeness, disconnect gaps, clock quality, or cross-source operational behavior. Those belong to the later soak, not this acceptance slice.

## Stop point

PHASE 1D.3C is accepted. No ETHUSDT implementation was started. The next separate authorized vertical slice is PHASE 1D.3D — Binance USD-M ETHUSDT Liquidations Parity.
