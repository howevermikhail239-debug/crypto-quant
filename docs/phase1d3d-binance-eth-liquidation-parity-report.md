# PHASE 1D.3D — Binance USD-M ETHUSDT Liquidation Parity

**Status:** IMPLEMENTED / READY FOR INDEPENDENT ACCEPTANCE

**Verified:** 2026-08-12

**Scope:** Binance USD-M ETHUSDT `<symbol>@forceOrder` parity on the accepted shared Binance liquidation adapter only.

## Source contract and metadata evidence

- The accepted source remains the Binance USD-M Market WebSocket `<symbol>@forceOrder` stream. No separate ETH collector, source dataset, schema, or YAML contract was introduced.
- [Binance USD-M common definitions](https://developers.binance.com/zh-CN/docs/products/derivatives-trading-usds-futures/common-definition) define the base asset as the traded object and `LOT_SIZE` / `MARKET_LOT_SIZE` as constraints on order `quantity`.
- The preserved official `GET /fapi/v1/exchangeInfo` response identifies ETHUSDT as a trading `PERPETUAL` with `baseAsset=ETH`, `quoteAsset=USDT`, `marginAsset=USDT`, and quantity step `0.001`.
- Therefore `o.q`, `o.l`, and `o.z` retain their distinct source meanings and normalize to the canonical base-asset unit ETH. They are not reinterpreted as complete liquidation volume.
- `o.p` remains order price and `o.ap` remains average fill price in USDT per ETH. `E` and `o.T` remain separate exchange push and order trade timestamps.

The existing field-level source contract was generalized without changing its source semantics:

```text
schemas/contracts/binance_usdm_liquidation_ws_v1.yaml
contract_id = binance.usdm.ws.liquidation-order.v1
source_dataset_id = binance.usdm.liquidations.ws
```

Auditable external evidence remains in the existing framework:

```text
C:\crypto_quant_data\evidence\phase1d3_audit\phase1d3_audit_evidence_index.json
28 entries; 28/28 referenced content SHA-256 values verified
ETH derived metadata artifact SHA-256:
8a107e8e9882b062291c95ab6d3a7eaa0e6ddf92db6d74c04b1ddf663653ac57
parent official exchangeInfo SHA-256:
8e57decc0429767834151a6b9174d01187d5aa4f01e087d93c4bb71093fa3779
```

## Shared adapter and identity isolation

One adapter now accepts only the explicit set `BTCUSDT` and `ETHUSDT`. It derives the complete canonical identity and units from the requested symbol and rejects Spot identities, unsupported symbols, and requested-symbol/payload mismatches before authoritative writes.

Canonical identities remain distinct:

```text
Binance USD-M BTCUSDT: ins_dae8124762a847d14263
Binance USD-M ETHUSDT: ins_13dce2c0972bec4044d9
Bybit linear BTCUSDT:   ins_843e0aeb9de581e61b56
Bybit linear ETHUSDT:   ins_c4c118aced7726321b3c
Binance Spot ETHUSDT:   ins_fabbbb813bf3250a0488
```

Raw paths, normalized partitions, immutable generation names, checkpoints, and manifest rows remain symbol-scoped. An economically identical BTC/ETH payload produces different instrument IDs, exact-wire message IDs, paths, and content hashes. Exact replay deduplication remains isolated per symbol.

## Fixture and failure-path coverage

Official-shaped ETH SELL and BUY fixtures independently cover:

- distinct `q`, `l`, and `z` values;
- distinct `p` and `ap` values;
- distinct `E` and `T` timestamps;
- both source order sides;
- canonical ETH units and identity;
- malformed required fields;
- Spot identity rejection;
- BTC payload on an ETH request and the inverse;
- mocked ETH topic receiving a BTC payload;
- replay, manifest counter, checkpoint, path, and hash isolation.

No fixture data was written to production raw or normalized namespaces.

## Bounded live pilot

One bounded production pilot subscribed to `ethusdt@forceOrder` through the accepted Market WebSocket route:

```text
requested window = 35 seconds
observed duration = 45.2 seconds (connection/subscription overhead included)
transport_status = PASS
subscription_status = PASS
heartbeat_liveness = PASS
event_observation_status = NO_EVENT_OBSERVED_WITHIN_WINDOW
total_messages_received = 0
total_records_persisted = 0
flush_count = 0
```

Zero events are a valid bounded observation. The collector correctly created no ETH raw object, normalized Parquet, checkpoint, or manifest event. This does not establish source completeness or long-run availability.

## BTC immutability regression

The accepted genuine BTCUSDT production artifacts are unchanged after ETH parity work and the live pilot:

```text
raw SHA-256:     ec277af4f4238c71fd347b258df64c7e4537a454c34d010841ab65f88f483ad8
Parquet SHA-256: 5cdc49ae06f6437807b0ad4ba5aade722804b3a805b490130a8cff6a418b3cc7
BTC manifest rows: 1
BTC checkpoint: present
active synthetic rows: 0
```

## Validation

```text
.venv\Scripts\python.exe -m pytest tests\test_binance_liquidations.py -q
22 passed

.venv\Scripts\python.exe -m pytest -q
237 passed

.venv\Scripts\python.exe -m ruff check .
PASS

.venv\Scripts\python.exe -m crypto_quant config-check
PASS

uv lock --check
PASS

git diff --check
PASS
```

Q3 post-gate review found no BLOCKER or HIGH issue. Canonical instrument identity, source semantics, quantity units, event/knowledge timestamps, exact replay deduplication, immutable lineage, fail-before-write behavior, and BTC artifact immutability remain intact.

## Known limitations / not implemented

- The source remains `INCOMPLETE_THROTTLED_SNAPSHOT` with the accepted unresolved `DOC_CONFLICT_LATEST_VS_LARGEST` selection wording.
- No public historical recovery was added; missed public observations remain unrecoverable or unknown.
- A zero-event pilot does not provide a genuine ETH row or measure ETH event frequency.
- No all-market stream, long soak, reconnect acceptance, PHASE 1D.3E+, liquidation features, models, strategies, risk, Telegram, or execution was started.

The next action is an independent PHASE 1D.3D acceptance audit. This report does not authorize the next implementation slice.
