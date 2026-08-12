# PHASE 1E.1 — Storage & Operational DQ Foundation

**Status:** FINAL DONE / ACCEPTED

**Accepted parent:** `0857da87c161e67a16ecd37fe69e40998ae58043`

**Date:** 2026-08-12

## Scope and governing requirements

This slice implements only the storage/catalog and operational-DQ foundation required by PHASE 1E. It follows the immutable Parquet, manifest lineage, source-semantic DQ, explicit gap, and fail-closed eligibility requirements in `crypto_quant_master_spec.md`, `crypto_quant_revised_technical_design.md`, and `crypto_quant_phased_development_prompt.md`.

`crypto_quant_architecture_v2.md` and `phase1c_closure_gate.md`, named in the phase request, are not present in the accepted repository and were therefore not invented or treated as governing evidence.

## Implemented

- Added DuckDB as a locked runtime dependency.
- Added a persistent, atomically rebuilt DuckDB catalog containing views only. Authoritative market observations remain in immutable Parquet; no market-data rows are copied into mutable DuckDB tables.
- Added manifest-aware active-generation resolution for OHLCV, individual trades, exchange aggregate trades, derived trade buckets, funding, OI, and liquidations.
- Added explicit removal/quarantine handling and latest-generation selection for cumulative funding/OI/liquidation datasets. Additive datasets retain all independently published active objects.
- Added `catalog_active_artifacts` lineage view and `instrument_metadata_snapshots` inventory view.
- Optional missing datasets produce queryable empty views; the currently absent exchange-aggregate-trade dataset does not break catalog construction.
- Extended the existing health/DQ module with policy-aware freshness, source-key-aware duplicate metrics, source-aware unknown-side metrics, accepted queue/writer telemetry projection, GapRegistry summaries, and a fail-closed eligibility decision interface.
- No final DQ thresholds were introduced. Freshness without an explicit threshold remains `UNKNOWN`; a quiet event-driven feed is `LOW_ACTIVITY_QUIET`, not an invented gap.

## Active-generation semantics

- `NORMALIZED`, `INGESTED`, `COMPACTED`, and legacy records without an action are publish events.
- `SUPERSEDED` and `DELETED` remove the explicitly referenced object.
- `QUARANTINED` excludes the recorded quarantined Parquet object.
- If an append-only manifest republishes the same object reference, only its latest record is active; obsolete hashes are not treated as simultaneous generations.
- Funding, OI, and liquidation manifests are cumulative-generation sources: only the latest accepted generation for the same manifest/source/exchange/market/instrument/period is active.
- OHLCV, individual trades, exchange aggregates, and derived buckets are additive immutable objects unless explicitly removed.
- An active manifest reference that escapes `data_root` or points to a missing file fails closed.

## Real data-root validation

Catalog built at `C:\crypto_quant_data\control\catalog\phase1e1.duckdb` from 96 active Parquet artifacts. SHA-256 hashes of every selected Parquet artifact were recomputed before and after catalog construction and were identical.

| View dataset | Rows |
| --- | ---: |
| OHLCV | 357,165 |
| Individual trades | 25,579,306 |
| Exchange aggregate trades | 0 |
| Derived trade buckets | 950,513 |
| Funding | 28,273 |
| Open interest | 417,896 |
| Liquidations | 1 |
| Instrument metadata snapshots | 25 files |

DuckDB introspection confirmed zero non-internal user tables.

## Validation

```powershell
& 'C:\Users\Admin\AppData\Local\Microsoft\WinGet\Packages\astral-sh.uv_Microsoft.Winget.Source_8wekyb3d8bbwe\uv.exe' lock --check
.\.venv\Scripts\python.exe -m ruff check .
.\.venv\Scripts\python.exe -m pytest -q
.\.venv\Scripts\python.exe -m crypto_quant config-check
.\.venv\Scripts\python.exe -m crypto_quant health
git diff --check
```

Results: uv lock PASS; Ruff PASS; 262 tests PASS; config-check PASS; operational health PASS with the pre-existing PHASE-0 `growth_projections=UNKNOWN`; diff check PASS. New focused tests cover active/latest/superseded generation selection, corrected same-reference manifest records, checksum mismatch rejection, dataset isolation, optional empty views, view-only catalog behavior, metadata inventory, Parquet immutability, root traversal rejection, freshness policy states, semantic duplicate/side metrics, queue telemetry, gap summaries, and eligibility boundaries.

## Limitations and deferred work

- This is not the final `data-health` CLI/report and does not establish final alert/hard-fail thresholds.
- No short operational soak, final data-quality report, point-in-time feature builder, feature generation, modeling, backtest, paper trading, Telegram, or execution was started.
- Phase-8 service orchestration remains explicitly deferred: no service runner, scheduler, alert engine, global reconnect-health automation, write-lease registry, or new stale-partial recovery framework was added.
- Instrument metadata is exposed as an immutable-file inventory because accepted metadata payloads are heterogeneous; field-level normalization remains a separately governed task.
- Dataset-class inference supports accepted legacy manifests. New writers should continue emitting explicit `dataset_class`; no silent fallback is permitted when classification is unknown.
- Eligibility consumes explicit reasons from future versioned policies. It does not infer research usability from arbitrary numerical cutoffs.

## Gate conclusion

Short independent acceptance on 2026-08-12 confirmed 0 mutable user tables, 96 unique active artifact references, preserved venue/market/instrument/dataset identity columns, 7 focused tests, and 262 full-suite tests. No defect was found and production code was not changed during acceptance.

PHASE 1E.1 is **FINAL DONE / ACCEPTED**. Threshold calibration, longer operational observation if required, the final Data Quality Report, and all remaining PHASE 1E gate work remain separate future packages. Stop here; do not begin another PHASE 1E sub-slice without explicit authorization.
