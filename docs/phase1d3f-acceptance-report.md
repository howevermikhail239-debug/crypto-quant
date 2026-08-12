# PHASE 1D.3F Independent Acceptance Report

## Verdict

**PHASE 1D.3F — FINAL DONE / ACCEPTED**

- Candidate commit: `12162e417ae63bb30d4d38204b54686ac24534e4`
- Branch: `master`
- Audit commit: the commit containing this report
- Scope: four-stream liquidation soak, source-local gap semantics, recovery,
  persistence reconciliation, and authoritative data-root integrity
- No later phase was started.

The candidate was not accepted from its implementation report alone. The code,
tests, live run record, sessions, manifests, raw and Parquet objects, checkpoints,
quarantine, hashes, identities, and process state were checked independently.

## Governing contracts and invariants

- `schemas/contracts/bybit_linear_all_liquidation_ws_v1.yaml`
- `schemas/contracts/binance_usdm_liquidation_ws_v1.yaml`
- explicit venue/market/instrument identity;
- exact-wire replay dedup only where the venue supplies no native event ID;
- event, exchange, receive, and knowledge timestamps remain distinct;
- raw-first durable persistence, immutable normalized generations, manifests,
  checkpoints, and quarantine;
- quiet event time is not a gap;
- confirmed local disconnect/drop is an explicit unrecoverable local gap because
  no accepted public historical liquidation backfill exists;
- Binance USD-M remains an incomplete throttled snapshot source, not a complete
  liquidation tape.

## Four-stream matrix

| Venue | Market | Symbol | Canonical instrument | Endpoint | Topic / subscription | Persistence route |
|---|---|---|---|---|---|---|
| Bybit | perpetual / linear | BTCUSDT | `ins_843e0aeb9de581e61b56` | `wss://stream.bybit.com/v5/public/linear` | `allLiquidation.BTCUSDT` | Bybit parser → canonical v1 → `raw/bybit/perpetual/liquidations/BTCUSDT` |
| Bybit | perpetual / linear | ETHUSDT | `ins_c4c118aced7726321b3c` | same isolated endpoint | `allLiquidation.ETHUSDT` | same adapter with ETH identity and symbol-scoped paths |
| Binance | perpetual / linear | BTCUSDT | `ins_dae8124762a847d14263` | `wss://fstream.binance.com/market/ws` | JSON `SUBSCRIBE` to `btcusdt@forceOrder` | Binance parser → canonical v1 → `raw/binance/perpetual/liquidations/BTCUSDT` |
| Binance | perpetual / linear | ETHUSDT | `ins_13dce2c0972bec4044d9` | same isolated endpoint | JSON `SUBSCRIBE` to `ethusdt@forceOrder` | same adapter with ETH identity and symbol-scoped paths |

`default_streams()` produces four distinct stream keys and four distinct canonical
instrument IDs. Each task has its own session ID, retry loop, topic, identity, and
symbol-scoped raw, normalized, manifest, and checkpoint routes. No Spot identity
participates in these streams.

## Live soak evidence

Authoritative run record:
`C:\crypto_quant_data\control\ingestion_runs\liquidation_soak\v1\liq_soak_96d04877a3d64202807ac752e8976208.json`.

- interval: `2026-08-12T07:13:44.503696Z` through
  `2026-08-12T07:23:54.601516Z`;
- four successful connections and four successful subscription handshakes;
- one session per stream, approximately 600–610 seconds;
- events observed and rows persisted: 0;
- disconnects, reconnects, drops, parser rejects, wrong-symbol rejects, and exact
  wire duplicates: 0;
- all streams: `NO_EVENT_OBSERVED_WITHIN_WINDOW` and
  `NO_DETECTED_LOCAL_GAP`;
- bounded termination: `BOUNDED_SOAK_COMPLETED`.

For Bybit, the historical session rows contained `connected_at` and
`subscribed_at`; the collector can set `subscribed_at` only after a positive venue
ACK. The audit nevertheless found that the candidate outcome did not persist an
explicit `subscription_status`, while orchestration defaulted a missing field to
PASS. This was corrected during acceptance. Future runs now persist explicit ACK,
endpoint, and topic evidence, and a missing ACK cannot produce an overall PASS.

No liquidation event during a healthy bounded connection is a valid observation
state. It is not a zero-valued event, synthetic observation, completeness proof, or
gap.

## Disconnect, reconnect, crash, and quarantine evidence

The audit added an end-to-end deterministic Bybit test using the production
collector path: ACK → valid frame → disconnect before scheduled flush → durable
raw/Parquet persistence → retry → second connection → resubscription to the same
topic. The stream retains its identity and records an unrecoverable local gap.
Focused venue tests also prove that both Bybit and Binance persist a received buffer
before propagating an unexpected disconnect.

Existing and repeated tests prove:

- one failing stream does not terminate the other streams;
- exact-wire replay across sessions is idempotent;
- distinct envelopes/events, venues, and instruments remain distinct;
- crash before/during/after normalized publication can be retried without corrupting
  authoritative state;
- rejected received wire payloads are durably quarantined;
- wrong symbol/topic fails before authoritative write;
- invalid required fields and unsupported Bybit side values fail closed;
- quiet time creates no false gap; confirmed disconnect/drop does.

## Dedup boundary

Neither accepted feed supplies a reliable native per-event ID suitable for broad
economic-event dedup. The rule is exact-wire replay identity plus event index where
batching exists. Cross-envelope look-alike events are preserved. No heuristic
cross-venue or cross-instrument dedup was introduced.

## Independent data-root audit

The audit parsed both liquidation manifests, recomputed SHA-256 for every active
referenced object, read Parquet identity/version columns, checked checkpoints,
enumerated partials and gaps, and compared active liquidation raw hashes against
repository fixture hashes.

- manifest records total: 3;
- active artifact manifest records: 1;
- quarantine records: 1;
- referenced active artifacts hash-valid: 2/2;
- broken refs: 0;
- identity mismatches: 0;
- checkpoint inconsistencies: 0;
- active synthetic liquidation observations: 0;
- active production liquidation rows: one Binance BTCUSDT G1 row;
- production liquidation gaps created by the quiet soak: 0.

The early synthetic Bybit BTC batch remains recoverably isolated under
`C:\crypto_quant_data\quarantine\synthetic_phase1d3_test_data`; it is not active
production data. Its quarantine manifest and bytes remain intact.

### Binance BTC G1 immutability

- raw SHA-256:
  `ec277af4f4238c71fd347b258df64c7e4537a454c34d010841ab65f88f483ad8`;
- Parquet SHA-256:
  `5cdc49ae06f6437807b0ad4ba5aade722804b3a805b490130a8cff6a418b3cc7`;
- row count: 1;
- canonical identity: Binance / perpetual / linear perpetual / BTCUSDT /
  `ins_dae8124762a847d14263`;
- schema / collector / normalization: `1.0.0` / `0.1.0` / `1.0.0`.

Both hashes match accepted pre-1D.3F evidence in repository history. No overwrite,
deletion, identity change, partition change, or manifest mutation was found.

## Defects found and production changes

1. **HIGH — received-buffer loss at disconnect boundary.** A WebSocket exception
   after receiving a frame but before periodic flush could discard the in-memory
   buffer. Both venue collectors now persist a non-empty accepted buffer before
   re-raising; persistence failure quarantines the exact buffer.
2. **MEDIUM — false subscription-ACK evidence.** Orchestration treated a missing
   `subscription_status` as PASS and Bybit omitted the field. Bybit now returns
   explicit status/endpoint/topic; missing ACK is FAIL and overall PASS requires
   both transport and ACK PASS.
3. **MEDIUM — unsupported Bybit side accepted as UNKNOWN.** The contract permits
   only `Buy` and `Sell`; another value is now an explicit reject handled by the
   quarantine path.
4. **MEDIUM — nonexistent run reconciliation could report PASS.** Reconciliation
   now requires the immutable run report and returns `RUN_REPORT_NOT_FOUND`
   otherwise.

Changes are limited to independently reproduced defects and regression coverage.
No feature, strategy, signal, or later-phase code was added.

## Validation

- baseline focused suite before audit edits: 54 passed;
- final focused liquidation suite: 59 passed;
- final full suite: 255 passed (candidate 250 plus five audit regressions), 80%
  aggregate coverage;
- `python -m ruff check .`: PASS;
- `python -m crypto_quant config-check`: PASS;
- `python -m crypto_quant health`: operational checks PASS; global growth
  projection remains `UNKNOWN` and unrelated Spot completeness remains `PARTIAL`;
- `uv lock --check`: PASS, 26 packages resolved;
- `git diff --check`: PASS;
- independent and built-in data-root audits: PASS;
- no liquidation collector or soak process remained. Two unrelated Python
  processes belonging to another user project were not touched.

## Residual source limitations

- A healthy connection cannot prove silent-loss absence because neither source
  exposes a reliable complete event sequence.
- Binance is explicitly an incomplete throttled snapshot source.
- No accepted public historical liquidation backfill exists for these contracts;
  disconnect intervals remain unrecoverable/unknown.
- `NO_EVENT_OBSERVED_WITHIN_WINDOW` is not proof that the market had no liquidation
  activity outside what the source and local connection could reveal.

These are explicit source limitations, not hidden implementation claims, and do not
block acceptance of this bounded source-local DQ phase.

## Final decision

All independently found defects were corrected and reverified. Critical identity,
persistence, ACK, reconnect, dedup, gap, quarantine, hash, and synthetic-isolation
invariants are supported by code, tests, and authoritative artifact readback.
Therefore PHASE 1D.3F is **FINAL DONE / ACCEPTED**.
