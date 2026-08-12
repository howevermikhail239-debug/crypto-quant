# PHASE 1D.3D — Independent Acceptance Audit

**Final status:** FINAL DONE / ACCEPTED

**Audit date:** 2026-08-12

**Implementation under audit:** `b57e039d2a40a8d3f1fd772e80f6dff8e5df07cc`

**Scope:** Binance USD-M ETHUSDT liquidation parity only. No later liquidation phase was started.

## Acceptance checklist

| Concern | Status | Independent outcome |
|---|---|---|
| Git state and implementation commit | CONFIRMED_OK | Audit started at the reported clean `master` HEAD `b57e039`. |
| Canonical ETH identity | CONFIRMED_OK | Existing builder recomputed `ins_13dce2c0972bec4044d9` with Binance/perpetual/linear-perpetual/ETH/USDT/USDT dimensions. |
| Identity regression matrix | CONFIRMED_OK | Binance BTC/ETH perpetual and Bybit BTC/ETH perpetual IDs are four distinct values; Binance ETH Spot is also distinct. |
| ETH q/l/z units | CONFIRMED_OK | Exact ETHUSDT record from hashed official `exchangeInfo` plus official USD-M common definitions resolves order quantity to base ETH. |
| Shared adapter | CONFIRMED_OK | One parser/normalizer/storage path and one frozen source contract serve BTC/ETH. No ETH-specific collector or contract exists. |
| BTC/ETH allowlist | CONFIRMED_OK | Module-level `SUPPORTED_SYMBOLS` is the explicit Core-scope policy gate; all source semantics remain instrument-generic. Future extension requires metadata verification and policy-set extension, not collector duplication. |
| BTC-specific semantic hardcodes | CONFIRMED_OK | Zero. Remaining BTC occurrences are documentation, supported-scope membership, or backward-compatible pilot defaults. |
| Wrong-symbol and topic/payload mismatch | CONFIRMED_OK | Both directions reject before raw, normalized, manifest, or checkpoint publication. Source symbol is never rewritten. |
| Cross-symbol identity and dedup | CONFIRMED_OK | Economically identical temp BTC/ETH messages produce distinct identities, exact-wire message IDs, raw/Parquet hashes and control lineage; per-symbol replay deduplicates. |
| q/l/z and p/ap separation | CONFIRMED_OK | All five fields remain independently recoverable Decimal lexemes; no liquidation-volume, bankruptcy-price, or mark-price inference exists. |
| Side semantics | CONFIRMED_OK | Source BUY/SELL is preserved; `position_side_liquidated=UNKNOWN`; no LONG/SHORT inference was introduced. |
| Source incompleteness versus row validity | CONFIRMED_OK | Valid rows retain `SOURCE_SELECTION_INCOMPLETENESS` and `INCOMPLETE_THROTTLED_SNAPSHOT` without being classified as malformed. |
| Raw/normalized/checkpoint/manifest isolation | CONFIRMED_OK | Temp persistence proves symbol-scoped immutable objects, partitions, checkpoints and consistent manifest identities/counts. |
| Genuine BTC artifact immutability | CONFIRMED_OK | Accepted raw and Parquet hashes matched before and after all acceptance tests. |
| Bounded ETH transport | CONFIRMED_OK | Recorded 45.2 s run proves Market URL, JSON SUBSCRIBE ACK, `ethusdt@forceOrder`, ping/pong and bounded termination. Zero events is valid. |
| Fake market state | CONFIRMED_OK | No ETH raw/normalized/checkpoint/manifest was created by the zero-event run; production fixture frames are absent. |
| Historical bootstrap status | DEFERRED_BY_DESIGN | Accepted `NO_VERIFIED_PUBLIC_SOURCE`; missed observations remain unrecoverable/unknown. USER_DATA history is unused. |
| ETH evidence index | CONFIRMED_OK | One new derived artifact is an exact ETHUSDT record from the hashed parent; recorded artifact and parent hashes match. |
| Portability | CONFIRMED_OK | Repository and external backup copies both match the 5/5 integrity manifest; HANDOFF remains present. |
| Test quality and validation | CONFIRMED_OK | Required semantics are covered behaviorally; focused, full, Ruff, config, health, lock and diff gates passed. |

## Identity and quantity

```text
exchange: binance
native_symbol: ETHUSDT
market_type: perpetual
contract_type: linear_perpetual
base_asset: ETH
quote_asset: USDT
settle_asset: USDT
instrument_id: ins_13dce2c0972bec4044d9

q: Original Quantity, ETH
l: Last Filled Quantity, ETH
z: Accumulated Filled Quantity, ETH
mapping source: canonical instrument base asset
classification: VERIFIED
```

The adapter assigns `source_quantity_unit=identity.quantity_unit`; it contains no BTC/ETH unit branch.

## Source semantics and isolation

```text
p/ap distinct: PASS
source side preserved: PASS
position_side_liquidated: UNKNOWN
selection window: 1000 ms
selection rule: DOC_CONFLICT_LATEST_VS_LARGEST
source completeness: INCOMPLETE_THROTTLED_SNAPSHOT
row structural validity independent: PASS

wrong-symbol: PASS
topic/payload mismatch: PASS
cross-symbol dedup isolation: PASS
raw isolation: PASS
normalized isolation: PASS
checkpoint isolation: PASS
manifest consistency: PASS
```

## BTC immutability

```text
accepted raw before/after:
ec277af4f4238c71fd347b258df64c7e4537a454c34d010841ab65f88f483ad8
unchanged: PASS

accepted Parquet before/after:
5cdc49ae06f6437807b0ad4ba5aade722804b3a805b490130a8cff6a418b3cc7
unchanged: PASS
```

## Live and production data root

```text
ETH connection: PASS
SUBSCRIBE ACK: PASS
heartbeat: PASS
genuine ETH event: NO (NOT A BLOCKER)
fake ETH market state created: NO

active BTC synthetic: 0
active ETH synthetic: 0
active BTC genuine: 1
active ETH genuine: 0
BTC checkpoint: present
ETH checkpoint: absent
```

## Evidence and portability

```text
new ETH evidence: 1
new ETH evidence hash-valid: 1/1
artifact SHA-256: 8a107e8e9882b062291c95ab6d3a7eaa0e6ddf92db6d74c04b1ddf663653ac57
parent exchangeInfo SHA-256: 8e57decc0429767834151a6b9174d01187d5aa4f01e087d93c4bb71093fa3779

repository agent/rule files: 5/5 PASS
external backup: 5/5 PASS
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

.venv\Scripts\python.exe -m crypto_quant health
PASS; growth_projections=UNKNOWN is an existing informational result

uv lock --check
PASS

git diff --check
PASS
```

## Stop point

PHASE 1D.3D is FINAL DONE / ACCEPTED. The next single vertical slice named by the current roadmap is PHASE 1D.3F — liquidation soak, gap tracking and source/local completeness DQ. It was not started by this audit.
