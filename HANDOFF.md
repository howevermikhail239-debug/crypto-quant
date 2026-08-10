# CHECKPOINT / HANDOFF

## Project / Phase

- Project: local-first crypto quant research/data platform for finding reproducible statistical edge, not live trading.
- Current phase: **PHASE 1C — TRADES**.
- Accepted prior state: **PHASE 0 DONE, PHASE 1A DONE, PHASE 1B DONE**.
- PHASE 1C goal: build exchange-neutral but source-faithful infrastructure for `individual_trade`, `exchange_aggregate_trade`, and deterministic `derived_trade_bucket` datasets, with proven taker-side semantics before any signed-volume/CVD research.
- Live data root: `C:\crypto_quant_data` (outside Git). Do not move it into the repository.
- Checkpoint date: 2026-08-10 (Europe/Moscow).

Original 8-point working plan:

1. Verify governing contracts, baseline, and current official sources.
2. Freeze canonical trade schemas, source contracts, and fixture gate.
3. Implement Binance Spot BTCUSDT individual-trade one-day vertical slice and resource gate.
4. Add deterministic 1s/5s/1m buckets and retention/recovery controls.
5. Expand individual pipelines to ETH, USD-M, and Bybit one-day pilots.
6. Add explicit exchange-aggregate datasets only for confirmed Binance sources.
7. Add realtime recovery/reconciliation, DQ, and disk guards.
8. Produce `docs/phase1c-report.md` and pass the full PHASE 1C DoD gate.

## Progress

| # | Work item | Status | Evidence / boundary |
|---|---|---|---|
| 1 | Governing contracts, baseline, official-source verification | **DONE** | Current Binance/Bybit source matrix was checked against official documentation. Source gaps are recorded below. |
| 2 | Canonical schemas, source contracts, fixture gate | **IN_PROGRESS** | Canonical individual schema and strict archive contracts/fixtures exist for Binance Spot and USD-M only. Bybit archive/REST/WS and Binance aggregate contracts are not implemented. The canonical schema still lives in `spot_trades.py`, not a final exchange-neutral package. |
| 3 | Binance Spot BTCUSDT one-day vertical slice/resource gate | **DONE** | 2026-07-01 archive, checksum, immutable raw/Parquet, manifest/checkpoint, resource measurements, recovery tests. |
| 4 | Deterministic buckets and retention/recovery controls | **DONE** for completed Binance archive slices | Correct immutable 1s/5s/60s pipeline, conservation gates, manifest/checkpoints, writer lease/stale recovery, disk estimation and retention dry-run/audit exist. This does not mean realtime recovery is done. |
| 5 | Expand individual pipelines to ETH, USD-M, Bybit | **IN_PROGRESS** | Binance Spot ETHUSDT and Binance USD-M BTCUSDT/ETHUSDT are complete for one day plus buckets. **Stopped immediately before Bybit trade implementation. No Bybit trade source file or contract was created.** |
| 6 | Exchange aggregate datasets | **NOT_STARTED** | No `exchange_aggregate_trade` adapter/schema/runtime dataset exists. |
| 7 | Realtime recovery/reconciliation, DQ, disk guards | **NOT_STARTED** as a plan item | Historical disk/recovery/DQ primitives exist, but no trades WS collector, reconnect loop, archive/REST/WS reconciliation, session gap registry, or realtime soak exists. |
| 8 | PHASE 1C report and final DoD | **NOT_STARTED** | `docs/phase1c-report.md` does not exist. PHASE 1C must not be called DONE. |

## Completed work

### Architecture and invariants

- Physical dataset classes are distinct:
  - `individual_trade`
  - `exchange_aggregate_trade` (reserved, not implemented)
  - `derived_trade_bucket`
- There is no fallback from individual to exchange aggregate semantics.
- Binance `isBuyerMaker=true` maps to taker `SELL`; `false` maps to taker `BUY`. Fixture tests prove both branches.
- Signed quantity convention is reserved and tested: taker BUY positive, taker SELL negative. Delta exists only per deterministic bucket; CVD is not implemented.
- Trade identity uses exchange/native trade ID plus instrument/source identity, never timestamp-price-quantity.
- Multiple executions at the same timestamp remain distinct.
- Historical `knowledge_time` is null/retrieval-only; download time is not historical market availability.
- Binance Spot archive timestamp unit is selected by dated contract (ms before 2025-01-01, us from 2025-01-01), never inferred from digit length.
- Binance USD-M archive timestamps are explicitly milliseconds for the verified six-column archive contract.
- Optional classifications not supplied by a source remain null; they are not fabricated as false.
- Derived buckets are half-open and deterministically ordered by event time, source ordinal, and native trade ID. They contain counts, base/quote buy/sell/total/delta, OHLC, average/median/max size, source lineage and versions. No CVD.
- Count, base-volume and quote-volume conservation are mandatory readback gates.
- Runtime files use immutable/content-addressed naming, atomic partial-to-final publication, append-only manifests, and checkpoints only after durable output.
- Structured single-writer lease contains run ID/PID/timestamps; active owners are rejected and stale/dead owners plus stale partials are recoverably quarantined.
- Disk gate uses measured components and preserves the configured 50 GiB bootstrap floor. It no longer uses the rejected `archive_size * 4` estimate.
- Retention planning uses logical coverage and refuses source deletion unless a complete verified permanent 60s aggregate lineage exists. Live pilot data was not deleted.

### Source matrix frozen so far

- Binance Spot:
  - individual archive `trades`, recent/historical REST, and WS `@trade` officially exist;
  - exchange aggregate archive/REST/WS exist but are not implemented yet.
- Binance USD-M:
  - individual daily/monthly archive and recent/historical REST exist;
  - no officially verified public individual-trade WS was found; do not substitute `@aggTrade`;
  - aggregate archive/REST/WS exist but are not implemented yet.
- Bybit Spot/Linear:
  - official individual trade archives and recent REST/publicTrade WS exist;
  - REST/WS `side`/`S` is explicitly taker side;
  - archive `side` taker semantics and archive-ID namespace parity with REST/WS remain unproved;
  - no separate Bybit exchange-aggregate dataset was verified.

### Code/contracts/components

- `src/crypto_quant/ingestion/binance/spot_trades.py`
  - Binance Spot individual archive streaming download/parse/normalize/commit;
  - official SHA-256 sidecar verification plus local hash;
  - strict member symbol/date and seven-column contract;
  - dated ms/us contract selection;
  - measured disk gate, structured lease, stale recovery, manifest/checkpoint;
  - retention planner/dry-run audit.
- `src/crypto_quant/ingestion/binance/usdm_trades.py`
  - separate Binance USD-M six-column millisecond archive adapter;
  - linear perpetual identity and strict route;
  - no individual WS and no aggregate fallback.
- `src/crypto_quant/ingestion/trade_buckets.py`
  - typed Spot/USD-M source descriptors;
  - immutable 1s/5s/60s derived buckets;
  - descriptor/manifest/hash/date validation and conservation gates.
- Contracts:
  - `schemas/contracts/binance_spot_archive_individual_trade_v1.yaml`
  - `schemas/contracts/binance_usdm_archive_individual_trade_v1.yaml`
- Tests:
  - `tests/test_binance_spot_trades.py`
  - `tests/test_binance_usdm_trades.py`
  - `tests/test_trade_buckets.py`
- Existing PHASE 0/1A/1B code and documentation remain in place and passed regression tests.

### Completed live one-day pilots (2026-07-01)

All runtime artifacts below are under `C:\crypto_quant_data`, not Git.

| Venue/market | Instrument | Trades | ZIP bytes | Extracted bytes | Parquet bytes | Normalize time | Coverage UTC |
|---|---:|---:|---:|---:|---:|---:|---|
| Binance Spot | BTCUSDT | 4,166,849 | 29,578,890 | 316,442,916 | 60,991,872 | ~330 s | 00:00:00.054233–23:59:59.831689 |
| Binance Spot | ETHUSDT | 2,678,598 | 21,542,474 | 200,599,851 | 43,289,338 | 197.542 s | 00:00:00.021627–23:59:59.179724 |
| Binance USD-M | BTCUSDT | 5,246,380 | 39,048,755 | 277,475,402 | 95,094,478 | 380.283 s | 00:00:00.058–23:59:59.987 |
| Binance USD-M | ETHUSDT | 6,072,541 | 49,485,019 | 323,448,862 | 112,959,581 | 479.029 s | 00:00:00.034–23:59:59.892 |

Side DQ:

- Spot BTC: fixture/provenance gate passed; unknown side 0%.
- Spot ETH: BUY 1,355,239 (50.5951%), SELL 1,323,359 (49.4049%), DQ flags 0.
- USD-M BTC: BUY 2,605,879 (49.670039%), SELL 2,640,501 (50.329961%), DQ flags 0.
- USD-M ETH: BUY 3,015,972 (49.665733%), SELL 3,056,569 (50.334267%), DQ flags 0.

Derived output sizes:

| Market/instrument | 60s | 5s | 1s |
|---|---:|---:|---:|
| Spot BTCUSDT | 226,776 B / 1,440 rows | 2,315,952 B / 17,277 rows | 7,414,682 B / 75,946 rows |
| Spot ETHUSDT | 221,844 B / 1,440 rows | 2,205,797 B / 17,166 rows | 6,231,501 B / 64,800 rows |
| USD-M BTCUSDT | 228,509 B / 1,440 rows | 2,185,785 B / 17,280 rows | 7,828,004 B / 85,342 rows |
| USD-M ETHUSDT | 226,985 B / 1,440 rows | 2,368,582 B / 17,280 rows | 8,902,797 B / 84,745 rows |

Every grain passed exact trade-count, base-volume and quote-volume conservation readback. Twelve derived manifest records and twelve checkpoints exist.

## Current work

Current point: **plan item 5, immediately before Bybit trade implementation**.

What was already done for item 5:

- Binance Spot ETHUSDT one-day individual archive plus 1s/5s/60s buckets and DQ.
- Binance USD-M BTCUSDT and ETHUSDT one-day individual archives plus buckets and DQ.
- Separate Spot vs USD-M contracts, identities, timestamp units and routing.

Where work stopped:

- Official-source reasoning for Bybit was completed, but no Bybit trades file, schema, fixture, test, or live archive pilot was written.
- The interrupted worker was told to implement Bybit Spot/Linear BTCUSDT/ETHUSDT one-day pilots, but it was stopped before modifying the repository.
- No project Python process remains active.

What remains in item 5:

1. Create separate Bybit Spot and Linear archive contracts from the observed exact CSV layouts.
2. Create official-shaped REST and WS fixtures proving `side`/`S` is taker side and proving that `seq` is not a unique trade/message key.
3. Resolve archive-side and ID namespace parity using current official documentation or a real overlap comparison. Do not guess.
4. If parity remains unproved, preserve archive `source_side`, set canonical `taker_side=UNKNOWN`, block signed buckets, and document the limitation.
5. Add archive adapters with local SHA-256 and HTTP metadata (no external checksum sidecar was verified).
6. Run exactly one day (2026-07-01) for Bybit Spot BTCUSDT/ETHUSDT, then Linear BTCUSDT/ETHUSDT; measure rows/ZIP or gzip/raw/Parquet/runtime/RSS/DQ.
7. Only build signed buckets for sources whose archive taker-side semantics pass the gate.

Files to create/continue for item 5 (recommended; names not yet committed by code):

- `src/crypto_quant/ingestion/bybit/trades.py` or separate `spot_trades.py` / `linear_trades.py` if source layouts justify it.
- `schemas/contracts/bybit_spot_archive_individual_trade_v1.yaml`
- `schemas/contracts/bybit_spot_rest_individual_trade_v1.yaml`
- `schemas/contracts/bybit_spot_ws_individual_trade_v1.yaml`
- corresponding Linear contracts.
- `tests/test_bybit_trades.py` plus sanitized official-shaped fixtures.
- Extend typed descriptors in `src/crypto_quant/ingestion/trade_buckets.py` only after side/unit contracts are proven.

## Validation

Checkpoint validation run on 2026-08-10:

```powershell
.\.venv\Scripts\python.exe -m pytest -q
.\.venv\Scripts\python.exe -m ruff check .
.\.venv\Scripts\python.exe -m crypto_quant config-check
& 'C:\Users\Admin\AppData\Local\Microsoft\WinGet\Packages\astral-sh.uv_Microsoft.Winget.Source_8wekyb3d8bbwe\uv.exe' lock --check
```

Results:

- pytest: **91 passed**, total coverage **77%**.
- Ruff: **PASS**.
- config-check: **PASS**, data root `C:\crypto_quant_data`.
- `uv lock --check`: **PASS** (`Resolved 25 packages`).
- `uv` is not currently on this PowerShell session's PATH; use the absolute executable above or refresh PATH.

Not yet tested/implemented:

- Bybit trade adapters/contracts/fixtures/pilots.
- Binance/Bybit trade REST/WS live collectors and reconnect behavior.
- Archive/REST/WS overlap reconciliation.
- Exchange aggregate datasets.
- Full PHASE 1C soak, report and DoD.
- Binance USD-M REST reconciliation helper was intentionally deferred because a current field-level contract was not frozen in code.

## Git state at checkpoint

- Before this checkpoint the repository was a valid Git worktree with **no commits at all** (`No commits yet on master`).
- Therefore every project file appeared as untracked (`??`), while `git diff` and `git diff --stat` were empty because Git had no tracked baseline.
- The tree appeared to contain only this project/foundation/phase work; no unrelated tracked changes were available to separate.
- The checkpoint commit should therefore be the initial repository snapshot containing all non-ignored project files plus this handoff.

## Runtime artifact state

- No active trade writer lease or live `.partial` exists in normalized/derived paths.
- Intentionally retained quarantine evidence:
  - `C:\crypto_quant_data\quarantine\phase1c_prototype_buckets\` contains three rejected early prototype bucket files and `reason.json`.
  - `C:\crypto_quant_data\quarantine\stale_trade_partials\eth-run2-partial-20260810.parquet.partial` is a recoverably preserved stale ETH Spot partial (26,215,033 B).
- Do not treat quarantine files as valid datasets and do not silently delete them.

## Known issues / risks

1. **PHASE 1C is not complete.** Bybit, exchange aggregates, realtime/reconciliation, final report and soak are missing.
2. The approved design/config still contain older names such as `raw_trade` / `aggregate_trade`. Current implementation uses `individual_trade`, `exchange_aggregate_trade`, and `derived_trade_bucket`. This documentation/config migration was intentionally not done during checkpoint mode.
3. `config/default.yaml` still lists `raw_trade` and `aggregate_trade` dataset names and an earlier `initial_90_days` bootstrap statement. Do not allow that stale text to trigger a 90-day download; the measured one-day resource gate and current phase instructions govern.
4. The canonical individual Arrow schema is defined in `binance/spot_trades.py` and reused by USD-M; it has not yet been extracted into a final exchange-neutral trades module.
5. `usdm_trades.py` test coverage is low (~29%) despite live pilots; more failure/recovery tests are needed before relying on it operationally.
6. Binance USD-M archive trade IDs have non-unit transitions (BTC 7,948 boundaries, max step 4; ETH 11,168, max step 5). These are observations only, not confirmed missing trades because ID-contiguity semantics are unverified.
7. Bybit archive `side` semantics and archive ID namespace parity with REST/WS remain unverified. REST/WS taker-side documentation does not automatically prove archive semantics.
8. Bybit archive checksum sidecars were not verified. Use a locally computed SHA-256 and retain HTTP metadata; never label ETag as a checksum.
9. No public Binance USD-M individual-trade WS was verified. Do not substitute `@aggTrade`.
10. No Bybit exchange-aggregate source was verified. Do not invent one.
11. Historical publication/knowledge times remain unknown for archives. Retrieval timestamps must never become historical knowledge times.
12. Internal peak RSS reporting sometimes returns null; external observed Windows working-set measurements were used in pilot notes.
13. A rejected prototype once reached ~9.9 GB because it loaded the whole Parquet to hash it. That defect is fixed with streaming file hashing. Do not reintroduce `Path.read_bytes()` for large data hashing.
14. Do not compare or deduplicate Bybit `seq` as a unique trade/message ID; multiple WS messages may share a sequence.
15. The Git checkpoint is an initial snapshot, not a small historical diff, because the repository had no previous commit.

## Do not redo

- Do not reinitialize PHASE 0 or rewrite its config/logging/path/version/control-plane foundation.
- Do not redo PHASE 1A or PHASE 1B OHLCV pipelines/pilots.
- Do not redownload or renormalize the four completed Binance trade archives unless checksum/conflict evidence proves it necessary.
- Do not rebuild the twelve correct derived bucket artifacts merely for convenience; they are manifest/checkpoint backed and passed conservation.
- Do not restore the quarantined prototype bucket artifacts into valid derived paths.
- Do not replace streaming ZIP/Parquet hashing with whole-file in-memory reads.
- Do not collapse Spot and perpetual identity, source semantics, units, or paths.
- Do not alias aggregate trades to individual trades or locally derived buckets to exchange aggregates.
- Do not infer gaps from idle seconds or unproved ID continuity.
- Do not calculate CVD or move into feature/ML/trading work.
- Do not start OI/funding/liquidations/order book/Telegram/Polymarket/Risk Engine/Trade Lifecycle runtime.

## Exact next actions

Start exactly here:

### Finish item 5 — Bybit individual trades

1. Re-open current official Bybit archive, recent-trade REST, and publicTrade WS documentation; save exact URLs and verification date in the PHASE 1C report/evidence.
2. Download only small header/sample fixtures first. Freeze Spot and Linear archive contracts separately; preserve unknown units/semantics as unknown.
3. Add REST/WS official-shaped fixtures and tests for taker Buy/Sell, block/RPI flags, timestamp ms, execution IDs, and non-unique `seq` behavior.
4. Attempt a bounded archive-vs-REST/WS overlap proof for side and ID namespace. If not provable, fail closed (`taker_side=UNKNOWN`) and skip signed buckets.
5. Implement bounded-memory, local-SHA, atomic Bybit Spot adapter and run BTCUSDT one day first; pass disk/resource/DQ gate, then ETHUSDT.
6. Implement Linear separately and repeat BTCUSDT then ETHUSDT. Do not carry Spot units/layout into Linear.
7. Add unsigned resource statistics for any source blocked on side semantics; build 1s/5s/60s signed buckets only after the fixture/overlap gate.
8. Run the full test/Ruff/lock/config gate and record all pilot metrics.

### Item 6 — explicit exchange aggregate datasets

9. Implement a separate canonical `exchange_aggregate_trade` schema and read path.
10. Freeze Binance Spot archive/REST/WS aggregate contracts and Binance USD-M archive/REST/WS aggregate contracts separately.
11. Preserve USD-M `q` vs `nq` where actually provided; never fabricate archive `nq` if absent.
12. Do not create Bybit aggregate contracts without an official source.
13. Add class-separation tests that fail closed when an experiment requires individual semantics but receives aggregate data.

### Item 7 — realtime/reconciliation/DQ

14. Implement Binance Spot `@trade` and Bybit publicTrade WS source-faithful envelope capture, bounded queues, checkpoints, session IDs, reconnect and gap records.
15. Do not implement a Binance USD-M individual WS unless an official source is newly verified; use archive/REST accumulation and explicit availability limits.
16. Add REST/archive overlap reconciliation by verified native IDs and compare price/quantity/side/time without requiring byte-identical representations.
17. Add sleep/network/process-crash tests and register unrecoverable periods as local/unknown gaps.
18. Add 30-day source/normalized and 90-day 1s/5s retention operational tests; keep 1m permanent and retain deletion ledger audit.

### Item 8 — report and final gate

19. Create `docs/phase1c-report.md` with official sources, matrix, semantics, coverage, DQ/gaps, resource table per exchange/market, projections, recovery limits, defects, tests and technical debt.
20. Update stale `raw_trade` terminology/status in design/config/docs through an explicit versioned migration, not a silent rename.
21. Run full pytest, Ruff, lock/config/health, deterministic replay, manifests/checkpoints readback, and a bounded realtime soak.
22. Perform the mandatory Q3 `quant-critical-review` post-gate. Mark PHASE 1C DONE only if every DoD condition is evidenced; then stop before the next market-data family.

## Commands

Environment and project:

```powershell
Set-Location 'C:\Users\Admin\Documents\ChatGPT\анализ крипты'
.\.venv\Scripts\Activate.ps1
python -m crypto_quant --help
python -m crypto_quant config-check
python -m crypto_quant health
```

Locked environment (absolute `uv` path until PATH refresh):

```powershell
$uvExe = 'C:\Users\Admin\AppData\Local\Microsoft\WinGet\Packages\astral-sh.uv_Microsoft.Winget.Source_8wekyb3d8bbwe\uv.exe'
& $uvExe lock --check
& $uvExe sync --locked --group dev
```

Validation:

```powershell
.\.venv\Scripts\python.exe -m pytest -q
.\.venv\Scripts\python.exe -m pytest tests\test_binance_spot_trades.py tests\test_binance_usdm_trades.py tests\test_trade_buckets.py -q
.\.venv\Scripts\python.exe -m ruff check .
.\.venv\Scripts\python.exe -m crypto_quant config-check
git status --short --branch
git diff
git diff --stat
```

Useful inspection (read-only):

```powershell
Get-ChildItem 'C:\crypto_quant_data\normalized\individual_trade' -Recurse -Filter *.parquet
Get-ChildItem 'C:\crypto_quant_data\derived\trade_bucket' -Recurse -Filter *.parquet
Get-ChildItem 'C:\crypto_quant_data\control\manifests' -File
Get-ChildItem 'C:\crypto_quant_data\control\checkpoints' -File
Get-ChildItem 'C:\crypto_quant_data\quarantine' -Recurse -Force
```
