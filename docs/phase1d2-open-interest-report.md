# Phase 1D.2 — Open Interest (OI) Ingestion & Normalization Report

**Phase**: Phase 1D.2 (Derivatives Open Interest)
**Status**: COMPLETE / AUDITED
**Primary Granularity**: `5m` (300s baseline)
**Instruments**: Binance USDⓈ-M (`BTCUSDT`, `ETHUSDT`) and Bybit Linear (`BTCUSDT`, `ETHUSDT`)

---

## 1. Governing Specifications & Source Contracts

| Contract ID | Endpoint | Exchange | Product | Semantic Mapping | Status |
|---|---|---|---|---|---|
| `binance.usdm.rest.open-interest-hist.v1` | `GET /futures/data/openInterestHist` | Binance | USDⓈ-M Perpetual | `sumOpenInterest` (base) + `sumOpenInterestValue` (USDT notional) | `VERIFIED` |
| `binance.usdm.rest.open-interest-current.v1` | `GET /fapi/v1/openInterest` | Binance | USDⓈ-M Perpetual | `openInterest` (base) point snapshot | `VERIFIED` |
| `bybit.linear.rest.open-interest.v1` | `GET /v5/market/open-interest` | Bybit | Linear Perpetual | `openInterest` (both sides, base) + `singleOpenInterest` (single side, base) | `VERIFIED` |

---

## 2. Invariants & Data Semantics

1. **Natural Key**:
   $$\text{Natural Key} = (\text{exchange}, \text{instrument\_id}, \text{period}, \text{observation\_time})$$
2. **Canonical Identity**:
   * Binance: `exchange="binance"`, `market_type="perpetual"`, `contract_type="linear_perpetual"`, `venue_product_type="usdm"`, `settle_asset="USDT"`
   * Bybit: `exchange="bybit"`, `market_type="perpetual"`, `contract_type="linear_perpetual"`, `venue_product_type="linear"`, `settle_asset="USDT"`
3. **Decimal Representation & Unit Preservation**:
   * Raw string decimals preserved without precision loss (e.g. `"106568.86800000"`).
   * **Binance**: Provides total base asset contracts (`sumOpenInterest`) and total USDT notional (`sumOpenInterestValue`).
   * **Bybit**: Provides total open interest for both sides (`openInterest`) and single side (`singleOpenInterest`). Bybit does **not** provide notional value in history $\rightarrow$ stored as `oi_notional = None` (no synthetic conversions in storage layer).
4. **Time & Knowledge Semantics**:
   * `observation_time`: UTC bucket timestamp (`timestamp[us, tz=UTC]`).
   * `event_time`: Identical to `observation_time`.
   * `knowledge_time`: `None` (UNKNOWN for historical bootstrap to eliminate look-ahead bias). Proven $0 / N$ non-null in all datasets.
5. **Continuous Local Accumulation & Safe Merging**:
   * Yearly partitions updated via `merge_and_write_oi_parquet`, which deduplicates by natural key and preserves historical observations outside the current 30-day API rolling window.

---

## 3. Dataset Summary Statistics & Horizon Analysis

| Series | Exchange | Period | Rows | Partitions | Size | Coverage Start (UTC) | Coverage End (UTC) | Source Window Status | Termination Reason |
|---|---|---|---|---|---|---|---|---|---|
| **BTCUSDT** | Binance | `5m` | **8,928** | 1 (`2026`) | 181.22 KB | 2026-07-11 06:30 | 2026-08-11 06:25 | `COMPLETE_OFFICIAL_WINDOW` | Official 30-day REST limit |
| **ETHUSDT** | Binance | `5m` | **8,928** | 1 (`2026`) | 188.83 KB | 2026-07-11 06:35 | 2026-08-11 06:30 | `COMPLETE_OFFICIAL_WINDOW` | Official 30-day REST limit |
| **BTCUSDT** | Bybit | `5m` | **200,000** | 3 (`2024-2026`) | 3.18 MB | 2024-09-15 19:55 | 2026-08-11 06:30 | `PARTIAL_TRUNCATED_BY_PAGE_LIMIT` | `max_pages=1000` (safety cap) |
| **ETHUSDT** | Bybit | `5m` | **200,000** | 3 (`2024-2026`) | 3.18 MB | 2024-09-15 20:00 | 2026-08-11 06:35 | `PARTIAL_TRUNCATED_BY_PAGE_LIMIT` | `max_pages=1000` (safety cap) |

> [!NOTE]
> **Source Horizon Discovery**: Independent API probe verified that Bybit Linear OI goes back to contract launch (**August 2020**, $\sim 630,000$ rows). The 200,000 rows dataset covers $\sim 2$ full years of 5m history and is explicitly marked as `PARTIAL_TRUNCATED_BY_PAGE_LIMIT`. Forward accumulation and deeper backfill are fully supported via `merge_and_write_oi_parquet`.

---

## 4. Exhaustive Data Quality Audit (Exact Denominators)

| Metric | Binance BTCUSDT | Binance ETHUSDT | Bybit BTCUSDT | Bybit ETHUSDT |
|---|---|---|---|---|
| **Total Rows ($N$)** | **8,928** | **8,928** | **200,000** | **200,000** |
| **Duplicate Natural Keys** | **0 / 8,928** | **0 / 8,928** | **0 / 200,000** | **0 / 200,000** |
| **Non-Monotonic Timestamps** | **0 / 8,928** | **0 / 8,928** | **0 / 200,000** | **0 / 200,000** |
| **Negative OI Values** | **0 / 8,928** | **0 / 8,928** | **0 / 200,000** | **0 / 200,000** |
| **Null OI Base Values** | **0 / 8,928** | **0 / 8,928** | **0 / 200,000** | **0 / 200,000** |
| **Misaligned 5m Cadence** | **0 / 8,928** | **0 / 8,928** | **0 / 200,000** | **0 / 200,000** |
| **Wrong Identity / Period** | **0 / 8,928** | **0 / 8,928** | **0 / 200,000** | **0 / 200,000** |
| **Non-Null Knowledge Time** | **0 / 8,928** | **0 / 8,928** | **0 / 200,000** | **0 / 200,000** |

---

## 5. Cross-Venue Sanity Check (1D.2C)

* Overlapping window: `2026-07-11 06:30` to `2026-08-11 06:25` (**8,928 common 5m timestamps**).
* **BTCUSDT OI Ratio (Binance / Bybit Both-Sides)**: Min = `1.6845`, Median = `1.8567`, Max = `1.9825`.
* **BTCUSDT OI Ratio (Binance / Bybit Single-Side)**: Min = `3.3691`, Median = `3.7133`, Max = `3.9650`.
* **ETHUSDT OI Ratio (Binance / Bybit Both-Sides)**: Min = `2.6990`, Median = `3.0136`, Max = `3.2863`.
* **ETHUSDT OI Ratio (Binance / Bybit Single-Side)**: Min = `5.3979`, Median = `6.0273`, Max = `6.5726`.
* Sanity Verdict: **PASS** (stable economic ratios, 0 scale/offset defects, 0 timezone shifts).

---

## 6. Manifest & Control Plane Reconciliation

* **Binance Manifest**: 8,928 rows (BTC) / 8,928 rows (ETH) $\leftrightarrow$ Parquet 8,928 rows $\leftrightarrow$ Checkpoint 8,928 rows.
* **Bybit Manifest**: 200,000 rows (BTC) / 200,000 rows (ETH) $\leftrightarrow$ Parquet (3 partitions) 200,000 rows $\leftrightarrow$ Checkpoint 200,000 rows.
* All raw objects (`.jsonl`) and normalized objects (`.parquet`) exist and match checksums.
