# Phase 1D.2 — Open Interest (OI) Ingestion & Normalization Report

**Phase**: Phase 1D.2 (Derivatives Open Interest)
**Status**: COMPLETE / AUDITED (Round 2 Acceptance)
**Primary Granularity**: `5m` (300s baseline)
**Instruments**: Binance USDⓈ-M (`BTCUSDT`, `ETHUSDT`) and Bybit Linear (`BTCUSDT`, `ETHUSDT`)

---

## 1. Governing Specifications & Source Contracts

| Contract ID | Endpoint | Exchange | Product | Semantic Mapping | Status |
|---|---|---|---|---|---|
| `binance.usdm.rest.open-interest-hist.v1` | `GET /futures/data/openInterestHist` | Binance | USDⓈ-M Perpetual | `sumOpenInterest` (base) + `sumOpenInterestValue` (USD/USDT notional) | `VERIFIED` |
| `binance.usdm.rest.open-interest-current.v1` | `GET /fapi/v1/openInterest` | Binance | USDⓈ-M Perpetual | `openInterest` (base) point snapshot | `VERIFIED` |
| `bybit.linear.rest.open-interest.v1` | `GET /v5/market/open-interest` | Bybit | Linear Perpetual | `openInterest` (both sides, base) + `singleOpenInterest` (single side, base) | `VERIFIED` |

---

## 2. Invariants, Provenance & Storage Generations

1. **Natural Key**:
   $$\text{Natural Key} = (\text{exchange}, \text{instrument\_id}, \text{period}, \text{observation\_time})$$
2. **Canonical Identity**:
   * Binance: `exchange="binance"`, `market_type="perpetual"`, `contract_type="linear_perpetual"`, `venue_product_type="usdm"`, `settle_asset="USDT"`
   * Bybit: `exchange="bybit"`, `market_type="perpetual"`, `contract_type="linear_perpetual"`, `venue_product_type="linear"`, `settle_asset="USDT"`
3. **Immutable Parquet Generations & Content-Addressed Storage**:
   * Yearly partitions are stored as immutable generation files: `part-{symbol}_{period}_{yr}_{gen_hash}.parquet`.
   * When new incoming observations arrive, `merge_and_write_oi_parquet` reads existing generations in `year=YYYY/`, combines them with new rows by natural key, deduplicates, sorts strictly ascending, and publishes a new immutable generation $G_2$.
   * Prior accepted generations ($G_1$) remain **byte-identical on disk forever**.
   * Historical manifest entries preserve their exact `raw_sha256`, `parquet_sha256`, and object references without mutation.
4. **Raw Object Immutability**:
   * Raw JSONL files are content-addressed: `oi_{min_ts}_{max_ts}_{raw_hash[:8]}.jsonl`.
   * Written using atomic temporary files with explicit `\n` newlines, matching `raw_sha256` deterministically across platforms.
5. **Decimal Representation & Unit Preservation**:
   * Raw string decimals preserved without precision loss.
   * **Binance**: Provides total base asset contracts (`sumOpenInterest`) and total USDT notional (`sumOpenInterestValue`).
   * **Bybit**: Provides total open interest for both sides (`openInterest`) and single side (`singleOpenInterest`). Bybit does **not** provide notional value in history $\rightarrow$ stored as `oi_notional = None` (zero synthetic enrichment). `single_side_oi_base` is parsed directly from source without synthetic division.
6. **Time & Knowledge Semantics**:
   * `observation_time`: UTC bucket timestamp (`timestamp[us, tz=UTC]`).
   * `event_time`: Identical to `observation_time`.
   * `knowledge_time`: `None` (UNKNOWN for historical bootstrap to eliminate look-ahead leakage).
   * Known limitation recorded in manifest: `"historical knowledge_time unknown; retrieval time is not market availability"`.
7. **Continuous Accumulation vs Scheduling**:
   * Incremental accumulation capability is **IMPLEMENTED** (outside-window history preserved across runs).
   * Automatic background scheduling / service daemons are **DEFERRED** to operations phase.

---

## 3. Source Horizon vs Configured Bootstrap Scope

| Instrument | Exchange | Official Contract `launchTime` (`instruments-info`) | Earliest Queryable 5m OI on API Endpoint | Active Ingested Rows | Active Partition Generations | Coverage Range (UTC) | Ingestion Scope Classification | Termination Reason |
|---|---|---|---|---|---|---|---|---|
| **BTCUSDT** | Binance | 2019-09-08 | Rolling 30 days | **8,938** | `year=2026` (1 gen) | 2026-07-11 06:30 $\rightarrow$ 2026-08-11 07:15 | `COMPLETE_OFFICIAL_WINDOW` | `SOURCE_WINDOW_LIMIT` (30d REST limit) |
| **ETHUSDT** | Binance | 2019-11-27 | Rolling 30 days | **8,936** | `year=2026` (1 gen) | 2026-07-11 06:35 $\rightarrow$ 2026-08-11 07:15 | `COMPLETE_OFFICIAL_WINDOW` | `SOURCE_WINDOW_LIMIT` (30d REST limit) |
| **BTCUSDT** | Bybit | **2020-03-15** (`1584230400000`) | 2020-08-05 | **200,000** | `year=2024`, `2025`, `2026` | 2024-09-15 20:40 $\rightarrow$ 2026-08-11 07:15 | `PARTIAL_CONFIGURED_BOOTSTRAP` | `PAGE_LIMIT_REACHED` (`max_pages=1000`) |
| **ETHUSDT** | Bybit | **2021-03-15** (`1615766400000`) | 2020-12-31 / 2021-01-01 | **200,000** | `year=2024`, `2025`, `2026` | 2024-09-15 20:40 $\rightarrow$ 2026-08-11 07:15 | `PARTIAL_CONFIGURED_BOOTSTRAP` | `PAGE_LIMIT_REACHED` (`max_pages=1000`) |

> [!IMPORTANT]
> **Source Horizon Specifics**:
> * Bybit `BTCUSDT` Linear launched on **2020-03-15**; Bybit `ETHUSDT` Linear launched **one full year later** on **2021-03-15**.
> * The current 200,000-row bootstrap provides ~2 full years of 5-minute data (400,000 observations across BTC and ETH), fully covering the required multi-year research window at 6.36 MB total storage.
> * Full backfill to inception is estimated at ~3,150 API pages ($\sim 2.5$ min runtime, $\sim 19$ MB disk) and can be executed on demand using the same merge-safe generation writer.

---

## 4. Exhaustive Data Quality Audit (Exact Denominators)

| Metric | Binance BTCUSDT | Binance ETHUSDT | Bybit BTCUSDT | Bybit ETHUSDT |
|---|---|---|---|---|
| **Total Rows ($N$)** | **8,938** | **8,936** | **200,000** | **200,000** |
| **Duplicate Natural Keys** | **0 / 8,938** | **0 / 8,936** | **0 / 200,000** | **0 / 200,000** |
| **Non-Monotonic Timestamps** | **0 / 8,938** | **0 / 8,936** | **0 / 200,000** | **0 / 200,000** |
| **Negative OI Values** | **0 / 8,938** | **0 / 8,936** | **0 / 200,000** | **0 / 200,000** |
| **Null OI Base Values** | **0 / 8,938** | **0 / 8,936** | **0 / 200,000** | **0 / 200,000** |
| **Misaligned 5m Cadence** | **0 / 8,938** | **0 / 8,936** | **0 / 200,000** | **0 / 200,000** |
| **Wrong Identity / Period** | **0 / 8,938** | **0 / 8,936** | **0 / 200,000** | **0 / 200,000** |
| **Non-Null Knowledge Time** | **0 / 8,938** | **0 / 8,936** | **0 / 200,000** | **0 / 200,000** |

---

## 5. Cross-Venue Sanity Check (1D.2C)

* Overlapping window: `2026-07-11 06:30` to `2026-08-11 07:15` (**8,936 common 5m timestamps**).
* **BTCUSDT OI Ratio (Binance / Bybit Both-Sides)**: Min = `1.6845`, Median = `1.8567`, Max = `1.9825`.
* **ETHUSDT OI Ratio (Binance / Bybit Both-Sides)**: Min = `2.6990`, Median = `3.0136`, Max = `3.2863`.
* **Verdict**: **PASS** — No gross cross-venue scaling or timestamp anomalies detected between Binance and Bybit. Ratios remain stable across the entire overlapping window.

---

## 6. Manifest & Lineage Historical Integrity Audit

Exhaustive verification of **100% of all historical manifest records ever written**:

* **Binance Manifest (`binance_usdm_open_interest.jsonl`)**: **8 / 8 records PASS** (0 broken).
* **Bybit Manifest (`bybit_linear_open_interest.jsonl`)**: **6 / 6 records PASS** (0 broken).
* **Total Records Audited**: **14 / 14 PASS** (0 missing raw files, 0 raw hash mismatches, 0 missing parquets, 0 parquet hash mismatches).
