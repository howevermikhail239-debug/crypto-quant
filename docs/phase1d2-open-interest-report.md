# Phase 1D.2 — Open Interest (OI) Ingestion & Normalization Report

**Phase**: Phase 1D.2 (Derivatives Open Interest)  
**Status**: COMPLETE  
**Primary Granularity**: `5m` (300s baseline)  
**Instruments**: Binance USDⓈ-M (`BTCUSDT`, `ETHUSDT`) and Bybit Linear (`BTCUSDT`, `ETHUSDT`)  

---

## 1. Governing Specifications & Source Contracts

| Contract ID | Endpoint | Exchange | Product | Semantic Mapping |
|---|---|---|---|---|
| `binance.usdm.rest.open-interest-hist.v1` | `GET /futures/data/openInterestHist` | Binance | USDⓈ-M Perpetual | `sumOpenInterest` (base) + `sumOpenInterestValue` (USDT notional) |
| `binance.usdm.rest.open-interest-current.v1` | `GET /fapi/v1/openInterest` | Binance | USDⓈ-M Perpetual | `openInterest` point snapshot |
| `bybit.linear.rest.open-interest.v1` | `GET /v5/market/open-interest` | Bybit | Linear Perpetual | `openInterest` (both sides, base) + `singleOpenInterest` (single side, base) |

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
   * `knowledge_time`: `None` (UNKNOWN for historical bootstrap to eliminate look-ahead bias).
5. **Deterministic Storage Partitioning**:
   `normalized/open_interest/v1/exchange={exchange}/market_type=perpetual/symbol={symbol}/period={period}/year={YYYY}/part-{symbol}_{period}_{year}.parquet`

---

## 3. Dataset Summary Statistics

| Series | Exchange | Period | Row Count | Parquet Size | Coverage Start (UTC) | Coverage End (UTC) | Min OI Base | Max OI Base | Avg OI Base | Notional / Single-Side Details |
|---|---|---|---|---|---|---|---|---|---|---|
| **BTCUSDT** | Binance | `5m` | **8,928** | 181.23 KB | 2026-07-11 06:30 | 2026-08-11 06:25 | 99,359.9070 | 111,625.0080 | 104,332.9644 | Notional: $6.65B – $7.36B |
| **ETHUSDT** | Binance | `5m` | **8,928** | 188.82 KB | 2026-07-11 06:30 | 2026-08-11 06:25 | 2,208,837.8760 | 2,473,870.4960 | 2,353,723.1119 | Notional: $6.46B – $7.30B |
| **BTCUSDT** | Bybit | `5m` | **8,641** | 147.57 KB | 2026-07-12 06:30 | 2026-08-11 06:30 | 54,238.1970 | 60,773.0760 | 57,849.5298 | Single-side: 27,119.1 – 30,386.5 |
| **ETHUSDT** | Bybit | `5m` | **8,641** | 147.63 KB | 2026-07-12 06:30 | 2026-08-11 06:30 | 1,236,594.1300 | 1,432,242.0600 | 1,334,994.4920 | Single-side: 618,297.1 – 716,121.0 |

---

## 4. Cross-Venue Sanity Check & Verification

1. **Market Share / Scale Alignment**:
   * BTC Open Interest: Binance (~104k BTC) vs Bybit Total Both-Sides (~58k BTC $\rightarrow$ single-side ~29k BTC).
   * ETH Open Interest: Binance (~2.35M ETH) vs Bybit Total Both-Sides (~1.33M ETH $\rightarrow$ single-side ~667k ETH).
   * The relative market shares across BTC and ETH are consistent (Binance holds ~60-65% market share, Bybit holds ~35-40%).
2. **Deterministic Single-Side Invariant (Bybit)**:
   * Tested and proved: $\text{singleOpenInterest} = \frac{1}{2} \times \text{openInterest}$ across all 17,282 Bybit rows.
3. **Restarts & Idempotency**:
   * Rerunning bootstrap overwrites deterministic yearly parquet files atomically (`.parquet.partial` $\rightarrow$ `.parquet`) without data corruption or row multiplication.

---

## 5. Definition of Done Checklist

- [x] Official frozen YAML contracts created for Binance and Bybit OI endpoints.
- [x] Canonical PyArrow schema `CANONICAL_OI_SCHEMA` enforced with `timestamp[us, tz=UTC]`.
- [x] Binance USDⓈ-M `5m` Open Interest historical bootstrap (8,928 rows per symbol) + current point snapshot.
- [x] Bybit Linear `5m` Open Interest historical bootstrap (8,641 rows per symbol) via cursor traversal.
- [x] Provenance isolation: `oi_notional = None` preserved for Bybit without synthetic conversion.
- [x] All 190 tests pass (`pytest -v`), `ruff` clean, `config-check` & `health` PASS.
- [x] Immutable raw artifacts, Parquet partitions, manifests, and checkpoints logged to `C:/crypto_quant_data`.
