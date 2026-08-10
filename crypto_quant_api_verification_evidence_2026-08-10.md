# API VERIFICATION EVIDENCE — 2026-08-10

This is a dated audit note supporting `crypto_quant_revised_technical_design.md`, not independently reproducible evidence: raw XML/DOM/screenshots were not preserved in this pre-implementation pass. It does not prove that an alternative, undisclosed or future archive cannot exist. PHASE 0 must repeat the audit and publish immutable raw evidence before any liquidation-bootstrap decision.

## Binance USDⓈ-M liquidation archive inventory

Official bucket endpoint:

```text
https://s3-ap-northeast-1.amazonaws.com/data.binance.vision/
```

Query 1:

```text
retrieved_at: 2026-08-10T06:38:13.0065002Z
prefix: data/futures/um/daily/liquidationSnapshot/BTCUSDT/
max-keys: 1000
KeyCount: 0
response_bytes_utf8: 300
response_sha256: b61fba80cfaceedf3613b896cdb944fa37c159a1048b98953db023b709bdd222
```

Query 2:

```text
retrieved_at: 2026-08-10T06:38:13.3258205Z
prefix: data/futures/um/monthly/liquidationSnapshot/BTCUSDT/
max-keys: 1000
KeyCount: 0
response_bytes_utf8: 302
response_sha256: 2a8e9bcde184916ec9da358af136635ba2d48c8ed6458b5bfaf7ab8cd3449722
```

Unpreserved control observation: the same official bucket appeared to list old objects under `data/futures/cm/daily/liquidationSnapshot/BTCUSD_PERP/`. Treat this only as a lead until the full query URL, raw XML, returned keys, retrieval timestamp and response hash are captured. COIN-M coverage would not prove USDⓈ-M coverage in any case.

Official archive documentation: [Binance Public Data](https://github.com/binance/binance-public-data).

## Bybit advertised historical products

Official page: [Historical data download](https://www.bybit.com/en/derivative-activity/history-data)

```text
observed_at: 2026-08-10T06:25:15Z
product_line_filters: Spot, Contract, Option
data_category_filters: Quote Data, OB Data, Trade Data
visible_products:
  - Public Trading History
  - Premium Index Price Kline
  - Index Price Kline
  - OrderBook
  - Mark Price Kline
canonical_visible_product_list:
  Public Trading History|Premium Index Price Kline|Index Price Kline|OrderBook|Mark Price Kline
canonical_visible_product_list_sha256:
  6dd8b2f8a785d514591e596202442042c87e8cde56b6a3ad77d511996bfb73a8
```

No liquidation product was visible in the current advertised catalog. The public directory root `https://public.bybit.com/` exposed `kline_for_metatrader4/`, `premium_index/`, `spot_index/`, `trading/`, and `spot/` at the time of inspection. Because no raw DOM/screenshot was preserved, this is an audit observation only and must be rechecked before PHASE 1D.3.

## Required re-verification

- Repeat both exchange inventory checks immediately before liquidation implementation.
- Expand from BTCUSDT to ETHUSDT and any alternate officially documented bucket/prefix.
- Store raw inventory responses as immutable raw objects once PHASE 0 manifest storage exists.
- Record terms, coverage, granularity, completeness and schema for any archive discovered later.
