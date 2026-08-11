# PHASE 1D.3C — Binance USD-M BTCUSDT Liquidation Pilot

**Status:** IMPLEMENTED / READY FOR INDEPENDENT ACCEPTANCE

**Verified:** 2026-08-11

**Scope:** Binance USD-M BTCUSDT `<symbol>@forceOrder` only

## Source contract

- [Binance USD-M WebSocket connection](https://developers.binance.com/en/docs/products/derivatives-trading-usds-futures/websocket-market-streams/Connect) defines `wss://fstream.binance.com`, the `/market` route, raw `/ws/<streamName>` and JSON subscription behavior.
- [Binance WebSocket migration notice](https://developers.binance.com/en/docs/products/derivatives-trading-usds-futures/websocket-market-streams/Important-WebSocket-Change-Notice) maps `<symbol>@forceOrder` to the Market service; legacy unrouted URLs are not used.
- The current API-reference corpus says the stream emits the **latest** liquidation order within 1000 ms, while the [2026-04-10 Binance changelog](https://developers.binance.com/en/docs/products/derivatives-trading-usds-futures/change-log) says that wording changed from **latest** to **largest**. The contract therefore freezes `selection_rule=DOC_CONFLICT_LATEST_VS_LARGEST`; it does not choose either interpretation without stronger evidence.
- The independently supported invariant is at most one selected observation per symbol per 1000 ms. The stream is classified `INCOMPLETE_THROTTLED_SNAPSHOT`, never a complete event stream.
- `GET /fapi/v1/forceOrders` is USER_DATA account history, not public market backfill. No verified official public historical USD-M liquidation source was found in the tested documented/archive locations. Missed public WebSocket observations remain `UNRECOVERABLE_OR_UNKNOWN`.

The frozen field-level contract is `schemas/contracts/binance_usdm_liquidation_ws_v1.yaml`. The external evidence index remains the existing single framework:

```text
C:\crypto_quant_data\evidence\phase1d3_audit\phase1d3_audit_evidence_index.json
25 entries; 25/25 content SHA-256 values verified; 0 broken
```

## Fields and identity

- `o.q`, `o.l`, and `o.z` remain separate original, last-filled, and accumulated-filled quantity fields. None is renamed to “true liquidation volume”.
- For the current BTCUSDT USD-M linear perpetual instrument, source quantities normalize to BTC while their source semantics remain explicit.
- `o.p` is retained as order price and `o.ap` as average fill price. `o.p` is not treated as bankruptcy price or execution ground truth.
- `o.S` is retained as the forced liquidation order side. `position_side_liquidated` remains `UNKNOWN`; no Bybit side convention is transferred.
- `E` is the exchange push/event timestamp; `o.T` is the order trade/event timestamp. Realtime `knowledge_time=received_at`.
- The canonical identity is the existing Binance USD-M BTCUSDT identity `ins_dae8124762a847d14263`, distinct from Bybit and Spot identities.

## Deduplication, storage, and DQ

The source exposes neither a native event ID nor a sequence ID. `message_id` is SHA-256 of the exact received text frame, and only exact-wire replay deduplication is guaranteed. Economically similar but byte-different envelopes are retained as separate observations.

Manifest counters are explicit:

- `raw_message_count`: received wire envelopes in the persisted batch;
- `observation_count`: parsed source observations before exact replay deduplication;
- `event_count` and `row_count`: canonical observations/rows after exact replay deduplication.

Raw JSONL and normalized Parquet use immutable content-addressed objects/generations. Manifest events are append-only; checkpoint publication follows durable raw/normalized publication. Every row carries `SOURCE_SELECTION_INCOMPLETENESS`. Local disconnect/connection failures are distinct from venue selection incompleteness.

## Bounded live pilot

The production Market WebSocket was observed for 43.97 seconds on `btcusdt@forceOrder`:

```text
ws_endpoint = wss://fstream.binance.com/market/ws
transport_status = PASS
subscription_status = PASS
heartbeat_liveness = PASS
event_observation_status = REAL_EVENT_OBSERVED
total_messages_received = 1
total_records_persisted = 1
flush_count = 1
```

The genuine observation was `SELL`, with `q=l=z=0.002 BTC`, order price `64048.20`, average fill price `64301.90`, and `position_side_liquidated=UNKNOWN`. These values are reported as source fields, not interpreted as complete liquidation volume or liquidated-position direction.

Persisted active artifacts:

```text
raw bytes:       210
raw SHA-256:     ec277af4f4238c71fd347b258df64c7e4537a454c34d010841ab65f88f483ad8
normalized rows: 1
Parquet bytes:   15,905
Parquet SHA-256: 5cdc49ae06f6437807b0ad4ba5aade722804b3a805b490130a8cff6a418b3cc7
manifest rows:   1
checkpoint:      present and identity/lineage consistent
active synthetic rows: 0
```

The local receive clock was about 0.59 seconds behind the source event timestamp for this observation, below the conservative five-minute anomaly threshold. This single sample does not establish latency or clock-quality statistics.

## Validation and review

Pre-live and post-live validation:

```text
.venv\Scripts\python.exe -m pytest tests\test_binance_liquidations.py -q
16 passed

.venv\Scripts\python.exe -m pytest -q
231 passed

.venv\Scripts\python.exe -m ruff check .
PASS

.venv\Scripts\python.exe -m crypto_quant config-check
PASS

.venv\Scripts\python.exe -m crypto_quant health
PASS (growth_projections=UNKNOWN remains informational)

uv lock --check
PASS
```

Q3 post-gate review found no BLOCKER or HIGH issue. The deliberate source incompleteness, unresolved latest/largest wording, absence of public historical recovery, lack of a native event ID, and short-pilot limitations remain accepted and explicit.

## Not implemented

- ETHUSDT, all-market streams, or another venue;
- long soak, reconnect/recovery acceptance, or historical reconciliation;
- liquidation-derived features, signals, strategies, risk, Telegram, or execution;
- inference of liquidated position side, complete cascade count, or complete liquidation volume.

The next action is an independent PHASE 1D.3C acceptance audit. No next implementation slice is authorized by this report.
