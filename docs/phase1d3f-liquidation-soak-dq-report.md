# PHASE 1D.3F — Liquidation Soak / Gaps / Source-Local Completeness DQ

Status: **IMPLEMENTED / READY FOR INDEPENDENT ACCEPTANCE**

Run date: 2026-08-12

Live run ID: `liq_soak_96d04877a3d64202807ac752e8976208`

HEAD before implementation: `db01156732b9846d1091ea02c218c6b494fe3076`

This is an implementation report, not independent acceptance. No signal, feature,
strategy, backtest, Phase 1D.3E, or Phase 1E work was started.

## Scope and governing invariants

The implementation follows the liquidation, timestamp/knowledge-time, gap registry,
recovery, provenance, and immutable-generation requirements in:

- `crypto_quant_master_spec.md`;
- `crypto_quant_revised_technical_design.md`, especially sections 4.5, 5, 10, 11.1, and 13;
- `crypto_quant_phase1_data_contracts.md`, sections 3.8 and 5.6;
- the accepted source contracts
  `bybit_linear_all_liquidation_ws_v1.yaml` and
  `binance_usdm_liquidation_ws_v1.yaml`;
- the PHASE 1D.3F execution prompt.

The following dimensions remain separate and machine-readable:

1. source claim/design;
2. transport and subscription availability;
3. source-local capture status;
4. normalized row structural quality.

Quiet market time does not create a gap. A healthy connection without a reliable
source sequence is classified as `NO_DETECTED_LOCAL_GAP`, never as proof of complete
capture. A proven local disconnect/drop produces `LOCAL_COLLECTOR_GAP` with
`UNRECOVERABLE`; reconnect does not turn the missing interval into recovered history.

## Implementation

`src/crypto_quant/ingestion/liquidation_soak.py` adds a bounded orchestration and
control layer over the already accepted source adapters. It does not create new
source contracts, a new manifest format, a new gap framework, or a new DQ engine.

Implemented behavior:

- one unique ingestion run ID and one unique session ID per connection attempt;
- four independent stream tasks, so one stream failure cannot restart the others;
- configurable duration, flush interval, disk floor, and bounded reconnect attempts;
- existing exponential backoff policy;
- append-only session events and DQ incidents under the existing external control root;
- existing `GapRegistry` records for observed disconnects and confirmed local drops;
- explicit `NO_VERIFIED_PUBLIC_LIQUIDATION_BACKFILL` recovery result;
- machine-readable parser/routing classifications (`INVALID_SOURCE_MESSAGE`,
  `UNKNOWN_SCHEMA`, `WRONG_SYMBOL`, `PROCESS_ERROR`, and
  transport failures);
- raw message, source event, newly persisted row, duplicate, rejection, and drop counts;
- raw/parquet manifest hash reconciliation for one run;
- deterministic full liquidation data-root audit, including quarantine-aware history.

The accepted collectors now expose run/session/timing lineage, source-event counts,
first/last event times, exact-wire hashes, and actual newly persisted row counts.
Canonical source semantics and schemas did not change.

The collector remains a simple synchronous read/flush loop. Therefore queue capacity,
queue high-water mark, and writer-lag queue metrics are `N/A`, not fabricated zeroes.
No in-process queue can overflow. Any known dropped-message count supplied by a source
or future buffering boundary is nevertheless persisted as a first-class local gap.

## Deterministic operational tests

Focused tests prove:

- quiet bounded completion creates no failure and no gap;
- controlled disconnect creates an unrecoverable local gap;
- reconnect uses a new session and does not mark history recovered;
- exact raw replay across sessions adds no canonical rows;
- a different envelope with the same economic content remains a distinct observation;
- Bybit message multiplicity survives replay/reconnect semantics;
- Binance selected observations append and never replace old snapshots (existing
  accepted source-adapter tests);
- a wrong-symbol rejection is a machine-readable incident, not an interval gap;
- rejected received wire frames are durably quarantined before the original parser or
  normalization error is re-raised;
- a confirmed local drop is an unrecoverable local completeness failure;
- raw evidence survives a controlled normalization failure and retry is idempotent;
- a failure after normalized publication but before checkpoint advancement is retry-safe;
- one failed stream does not damage the other three;
- BTC/ETH and venue identities remain isolated;
- Binance source incompleteness is not row corruption, and Bybit's source claim is not
  local proof of complete capture.

All controlled/synthetic events were written only to pytest temporary roots. They were
never written to `C:\crypto_quant_data`.

## Live four-stream soak

Command:

```powershell
.\.venv\Scripts\python.exe -m crypto_quant.ingestion.liquidation_soak `
  --data-root C:\crypto_quant_data `
  --duration 600 `
  --flush-interval 5 `
  --min-disk-free-gb 20 `
  --max-attempts 3
```

Run window: `2026-08-12T07:13:44.503696Z` to
`2026-08-12T07:23:54.601516Z`. Overall status: `PASS`.

| Stream | Connected | Subscribed | Duration | Messages | Events | New rows | Disconnect / reconnect | Reject / drop / duplicate | Result |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---|
| Bybit BTCUSDT | PASS | PASS | 600.86 s | 0 | 0 | 0 | 0 / 0 | 0 / 0 / 0 | `NO_DETECTED_LOCAL_GAP` |
| Bybit ETHUSDT | PASS | PASS | 600.84 s | 0 | 0 | 0 | 0 / 0 | 0 / 0 / 0 | `NO_DETECTED_LOCAL_GAP` |
| Binance BTCUSDT | PASS | PASS | 610.03 s | 0 | 0 | 0 | 0 / 0 | 0 / 0 / 0 | `NO_DETECTED_LOCAL_GAP` |
| Binance ETHUSDT | PASS | PASS | 608.86 s | 0 | 0 | 0 | 0 / 0 | 0 / 0 / 0 | `NO_DETECTED_LOCAL_GAP` |

All four terminations were `BOUNDED_SOAK_COMPLETED`. The small duration spread comes
from independent connection/subscription and WebSocket close handshakes. No orphan
`liquidation_soak` Python process remained after completion.

No genuine liquidation was observed during this bounded window. This is explicitly
`NO_EVENT_OBSERVED_WITHIN_WINDOW`, not a source gap, not proof that no market
liquidations occurred, and not an acceptance blocker.

### Source completeness matrix

| Source | Source claim/design | Delivery | Local capture result | Silent-loss detectability | Historical recovery |
|---|---|---|---|---|---|
| Bybit Linear | `ALL_LIQUIDATIONS` | `BATCHED_500MS_PUSH` | `NO_DETECTED_LOCAL_GAP` | not provable: no reliable sequence ID | no verified public backfill |
| Binance USD-M | `INCOMPLETE_THROTTLED_SNAPSHOT` | max one selected observation/symbol/1000 ms | `NO_DETECTED_LOCAL_GAP` | not provable: no reliable sequence ID | no verified public backfill |

Binance retains `DOC_CONFLICT_LATEST_VS_LARGEST`. A perfect local Binance connection
cannot produce a complete market liquidation tape. Bybit's source claim remains a
versioned source claim and does not become a locally proven completeness assertion.

## Gaps and recovery

- controlled disconnect gaps created in temporary tests: 1;
- controlled confirmed-drop gaps created in temporary tests: 1;
- live unexpected gaps: 0;
- quiet-market false gaps: 0;
- gaps incorrectly marked recovered: 0;
- production liquidation gap records created by this soak: 0.

Every controlled disconnect/drop record has canonical instrument identity, source
stream, run/session evidence, `LOCAL_COLLECTOR_GAP`, `UNRECOVERABLE`,
`coverage_proven=false`, and the explicit no-history limitation. No REST or archive
recovery is attempted because no verified public liquidation history is available for
these streams.

## Reconciliation and data-root audit

The live run had zero messages, therefore live reconciliation is exactly:

```text
raw messages:                     0
expected canonical observations: 0
persisted observations:           0
exact-wire duplicates removed:    0
invalid/rejected:                 0
unexplained difference:           0
manifest records for this run:    0 (valid zero-event result)
```

Full external data-root readback after the soak:

| Stream | Active raw objects | Active generations | Active canonical rows | Checkpoint | Gaps |
|---|---:|---:|---:|---|---:|
| Bybit BTCUSDT | 0 | 0 | 0 | none (no active genuine rows) | 0 |
| Bybit ETHUSDT | 0 | 0 | 0 | none | 0 |
| Binance BTCUSDT | 1 | 1 | 1 | present and identity-consistent | 0 |
| Binance ETHUSDT | 0 | 0 | 0 | none | 0 |

Manifest/control audit:

- total liquidation manifest events: 3;
- artifact-bearing `NORMALIZED` records: 2/2 hash-valid;
- quarantine records: 1, with relocated raw/parquet artifacts hash-valid;
- active artifact records: 1;
- broken references: 0;
- canonical identity mismatches: 0;
- checkpoint inconsistencies: 0;
- active synthetic observations: 0;
- accepted Binance BTCUSDT G1 raw hash remains
  `ec277af4f4238c71fd347b258df64c7e4537a454c34d010841ab65f88f483ad8`;
- accepted Binance BTCUSDT G1 parquet hash remains
  `5cdc49ae06f6437807b0ad4ba5aade722804b3a805b490130a8cff6a418b3cc7`.

The historical quarantined Bybit synthetic batch remains recoverable under
`quarantine/synthetic_phase1d3_test_data`; it is not authoritative data and was not
deleted or restored.

## Time and latency DQ

Realtime normalized rows continue to use `knowledge_time = received_at`; no historical
knowledge time is invented. The live soak had no events, so per-stream event latency is
`N/A`.

The one previously accepted genuine Binance BTC row shows local receive time 1,597.138
ms before exchange push timestamp and 590.138 ms before order event timestamp. This is
recorded as clock-skew uncertainty, not negative network latency. Processing followed
receive by 1,008.108 ms. Until exchange/local clock offset is measured, latency claims
remain unavailable and future feature eligibility must retain this uncertainty.

## Resources and growth

Resource gate before the run:

- free disk: 340,234,948,608 bytes (316.868 GiB), PASS versus 20 GiB floor;
- available RAM: 4,021,280,768 bytes;
- logical CPUs: 16;
- existing liquidation data footprint: 515,280,577 bytes;
- expected four-stream load: trivial to moderate.

After the run:

- free disk: 340,246,265,856 bytes (316.879 GiB);
- available RAM: 3,837,698,048 bytes;
- liquidation data footprint: unchanged at 515,280,577 bytes;
- new raw and normalized event bytes: 0;
- peak RAM and CPU utilization: not measured; no value is fabricated;
- growth projection: `UNKNOWN` because this zero-event 10-minute sample cannot support
  a useful extrapolation.

## Portability and production safety

- repository Antigravity agent files: 5/5 present;
- external backup `C:\crypto_quant_data\migration_backup\antigravity_agents\pre_codex_20260811`: 5/5 agent files plus integrity manifest present;
- `HANDOFF.md`: preserved and updated only for phase status;
- production synthetic contamination: 0;
- no background collector/service remains;
- no accepted raw, normalized generation, manifest, or checkpoint was overwritten.

## Known limitations and risks

- Neither stream family provides a reliable liquidation sequence ID, so silent loss
  during an apparently healthy connection may be undetectable.
- No verified public historical liquidation backfill exists in the accepted source
  baseline; observed local gaps are unrecoverable/unknown rather than repaired.
- The existing accepted persistence format uses cumulative immutable yearly Parquet
  generations. This phase did not redesign it.
- The collectors use synchronous read/flush rather than a separate bounded queue; queue
  metrics are therefore not applicable.
- The live window contained no events, so real four-stream event lineage and empirical
  throughput/growth remain unobserved for this run. Accepted Binance BTC evidence remains
  the only active genuine production liquidation row.
- Windows lacks the optional `tzdata` package in this environment; audits use Arrow
  integer timestamp values and do not alter dependency scope.
- Source selection/completeness classes are not cross-venue volume comparability claims.

## Validation

Final pre-commit gates:

- `python -m pytest -q tests/test_liquidation_soak.py tests/test_bybit_liquidations.py tests/test_binance_liquidations.py`: PASS;
- `python -m pytest -q`: 250 tests PASS (final count after rejected-frame tests);
- `python -m ruff check .`: PASS;
- `python -m crypto_quant config-check`: PASS;
- `python -m crypto_quant health`: operational checks PASS; legacy global growth projection remains `UNKNOWN` and existing unrelated Spot health is `PARTIAL`;
- `uv lock --check`: PASS, 26 packages resolved;
- `git diff --check`: PASS;
- liquidation data-root audit: PASS;
- portability readback: PASS.

## Stop point

PHASE 1D.3F is **IMPLEMENTED / READY FOR INDEPENDENT ACCEPTANCE**. It is not marked
FINAL DONE/ACCEPTED. Work stops here; PHASE 1D.3E, PHASE 1E, features, signals, models,
strategies, and backtests are not started.
