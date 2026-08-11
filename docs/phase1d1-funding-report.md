# Phase 1D.1 — Funding Rate Ingestion & Normalization Report

**Phase**: Phase 1D.1 (Sub-phases 1D.1A, 1D.1B, 1D.1C)
**Date**: 2026-08-11
**Status**: COMPLETE / SUBMITTED FOR DoD ACCEPTANCE

---

## 1. Executive Summary

Phase 1D.1 delivers the complete historical funding rate ingestion and canonical normalization pipeline for **Binance USDⓈ-M** and **Bybit Linear** perpetual contracts (`BTCUSDT` and `ETHUSDT`).

- **Binance USDⓈ-M Inception**: Full bootstrap from 2019 to 2026 (7,579 BTC records, 7,345 ETH records).
- **Bybit Linear Inception**: Full bootstrap from 2020 to 2026 (6,989 BTC records, 6,360 ETH records).
- **Storage**: Highly compact partitioned Parquet (`year=YYYY/part-{symbol}_{year}.parquet`), total storage across all 4 datasets is **under 510 KB**.
- **Data Quality**: 0 duplicate keys, 0 timestamp non-monotonicities, 0 interval anomalies across all 28,273 historical events.

---

## 2. Ingested Dataset Summary & Statistics

| Metric | Binance BTCUSDT | Binance ETHUSDT | Bybit BTCUSDT | Bybit ETHUSDT |
|---|---|---|---|---|
| **Exchange** | `binance` | `binance` | `bybit` | `bybit` |
| **Market Type (Canonical)** | `perpetual` | `perpetual` | `perpetual` | `perpetual` |
| **Contract Type** | `linear_perpetual` | `linear_perpetual` | `linear_perpetual` | `linear_perpetual` |
| **Venue Product Type** | `usdm` | `usdm` | `linear` | `linear` |
| **Instrument ID** | `ins_dae8124762a847d14263` | `ins_ba2d02951dcbe0be4f3c` | `ins_843e0aeb9de581e61b56` | `ins_7951a3beea6eb9eb8df3` |
| **Observed Coverage Start** | `2019-09-10 08:00:00 UTC` | `2019-11-27 08:00:00 UTC` | `2020-03-25 16:00:00 UTC` | `2020-10-21 08:00:00 UTC` |
| **Observed Coverage End** | `2026-08-10 08:00:00 UTC` | `2026-08-10 08:00:00 UTC` | `2026-08-11 00:00:00 UTC` | `2026-08-11 00:00:00 UTC` |
| **Total Ingested Records** | **7,579** | **7,345** | **6,989** | **6,360** |
| **Parquet Partitions (Years)** | 8 (`2019`–`2026`) | 8 (`2019`–`2026`) | 7 (`2020`–`2026`) | 7 (`2020`–`2026`) |
| **Total Parquet Size** | **157.99 KB** | **155.41 KB** | **97.37 KB** | **93.75 KB** |
| **Observed Interval (Delta)** | 7,578 $\times$ **480 min** | 7,344 $\times$ **480 min** | 6,988 $\times$ **480 min** | 6,359 $\times$ **480 min** |
| **Interval Gaps / Anomalies** | **0** | **0** | **0** | **0** |
| **Source rateType** | `Regular`: 100% | `Regular`: 100% | `None` (not provided) | `None` (not provided) |
| **Canonical rate_type** | `REGULAR` | `REGULAR` | `NOT_PROVIDED` | `NOT_PROVIDED` |
| **Mark Price Provenance** | Present (Binance REST) | Present (Binance REST) | `None` (NULL, no enrichment) | `None` (NULL, no enrichment) |
| **Min Funding Rate** | `-0.00300000` (-0.30%) | `-0.00356332` (-0.356%) | `-0.00375000` (-0.375%) | `-0.00345950` (-0.346%) |
| **Max Funding Rate** | `+0.00300000` (+0.30%) | `+0.00375000` (+0.375%) | `+0.00375000` (+0.375%) | `+0.00375000` (+0.375%) |
| **Positive / Negative Counts** | 6,486 pos / 1,093 neg | 6,333 pos / 1,012 neg | 5,846 pos / 1,142 neg | 5,267 pos / 1,093 neg |
| **Duplicate Natural Keys** | **0** | **0** | **0** | **0** |
| **DQ Validation** | **PASS** | **PASS** | **PASS** | **PASS** |

---

## 3. Cross-Venue Sanity Check (Binance USDⓈ-M vs Bybit Linear)

| Dimension | BTCUSDT Sanity Check | ETHUSDT Sanity Check |
|---|---|---|
| **Common Settlement Timestamps** | 3,798 timestamps | 3,382 timestamps |
| **Common Date Range** | `2020-03-26 16:00:00` to `2026-08-09 16:00:00` | `2020-10-21 16:00:00` to `2026-08-09 16:00:00` |
| **Rate Scale & Units** | Strict decimal fractions on both venues (e.g. `0.0001` = 0.01%) | Strict decimal fractions on both venues (e.g. `0.0001` = 0.01%) |
| **Sign Agreement Rate** | **82.81%** (3,145 same sign, 653 diff) | **84.89%** (2,871 same sign, 511 diff) |
| **Unit / Scale Defect** | **None** | **None** |

*Note*: Rates naturally diverge during idiosyncratic venue order flows; no arbitrage or spread features are calculated during this ingestion phase.

---

## 4. Architectural & Provenance Invariants Satisfied

1. **Decimal Preservation**: All funding rates are stored as raw Decimal fractions (e.g. `0.00007054`), never multiplied by 100 in canonical storage.
2. **Natural Key Isolation**:
   $$\text{Natural Key} = (\text{exchange}, \text{instrument\_id}, \text{funding\_time}, \text{rate\_type})$$
   Proved that `Regular` and `Special` events at identical timestamps produce separate non-colliding records.
3. **Nullability Provenance**:
   - Bybit funding history does not provide `markPrice` $\rightarrow$ `mark_price = None` (no silent enrichment from external candles).
   - Bybit funding history does not provide `rateType` $\rightarrow$ `source_rate_type = None`, `canonical_rate_type = "NOT_PROVIDED"`.
4. **Interval Separation**:
   - `observed_interval_minutes`: derived purely from historical event timestamps (`current - previous`).
   - `configured_interval_minutes`: stored in point-in-time metadata snapshots (`fundingInfo`, `instruments-info`), **never backfilled historically**.
5. **Conservative Knowledge-Time**: `knowledge_time = None` (UNKNOWN) for all historical bootstrap rows to prevent look-ahead bias in future ML pipelines.
6. **Frozen YAML Contracts**:
   - `schemas/contracts/binance_usdm_funding_rate_rest_v1.yaml`
   - `schemas/contracts/binance_usdm_funding_info_rest_v1.yaml`
   - `schemas/contracts/bybit_linear_funding_rate_rest_v1.yaml`
   - `schemas/contracts/bybit_linear_instruments_info_v1.yaml`
7. **Idempotency & Manifest Integrity**: Repeated bootstrap execution writes into deterministic yearly files (`part-{symbol}_{year}.parquet`) without requiring `rmtree` or creating duplicate entries.

---

## 5. Verification & Test Suite Summary

- **Total Test Count**: **173 passed** in 4.30s
- **Coverage**: **80%** overall
  - `src/crypto_quant/ingestion/binance/funding.py`: 86%
  - `src/crypto_quant/ingestion/bybit/funding.py`: 85%
- **Linter**: `ruff check .` — **0 errors**
- **System Health**: `crypto_quant config-check` — **PASS**, `crypto_quant health` — **PASS**
- **Lock Check**: `uv lock --check` — **PASS**

---

## 6. Definition of Done Checklist — PHASE 1D.1

| Requirement | Status | Evidence |
|---|---|---|
| Binance USDⓈ-M BTC/ETH Complete | ✅ DONE | 7,579 BTC + 7,345 ETH records in Parquet |
| Bybit Linear BTC/ETH Complete | ✅ DONE | 6,989 BTC + 6,360 ETH records in Parquet |
| Official Contracts Frozen in YAML | ✅ DONE | 4 YAML contracts in `schemas/contracts/` |
| Funding Rate Decimal Fraction Units | ✅ DONE | Verified Decimal strings (no % multiplication) |
| rateType Semantics Preserved | ✅ DONE | Regular/Special/NOT_PROVIDED tested |
| Mark Price Provenance Preserved | ✅ DONE | Binance has markPrice, Bybit mark_price=None |
| Observed vs Configured Interval Decoupled | ✅ DONE | Delta calculated, snapshot not backfilled |
| Conservative Knowledge-Time | ✅ DONE | `knowledge_time = None` in historical storage |
| Pagination & Idempotent Restart Tested | ✅ DONE | Multi-page, boundary deduplication tested |
| Manifests & Checkpoints Recorded | ✅ DONE | JSONL manifests & JSON checkpoints active |
| DQ Validation Complete | ✅ DONE | 0 duplicates, strict monotonicity, valid values |
| Full Repository Tests PASS | ✅ DONE | 173 tests pass, ruff clean, health pass |

```text
PHASE 1D.1 — FINAL DONE
```
