# PHASE 1B — Bybit V5 OHLCV

## Scope and gate sequence

Only regular traded-price 1-minute OHLCV was added: Bybit Spot BTCUSDT/ETHUSDT
and Bybit USDT linear perpetual BTCUSDT/ETHUSDT. No trades, CVD, OI, funding,
liquidations, WebSocket runtime, features, models, or execution were added.

The first vertical-slice gate was **Bybit Spot BTCUSDT, 2026-07-01T00:00Z to
2026-08-01T00:00Z**. It passed before the other three instruments were written:
44,640 expected rows, 44,640 unique rows, no detected one-minute discontinuity,
45 reverse-order REST pages, 17.8 s wall time, 3,799,357-byte Zstd Parquet, and
one immutable raw JSON envelope object. The manifest records the half-open
request, checksum, coverage, versions and source contract; its checkpoint is
written only after the Parquet file is atomically visible. The second same
request is idempotent by object checksum.

The completed expansion subsequently produced 44,640 rows for each of Spot
ETHUSDT, linear BTCUSDT and linear ETHUSDT. The four July series have identical
minute keys (sanity check only; prices/volumes are never joined by equality).

| Instrument | Rows | Raw JSON | Parquet | Runtime | Peak working set |
|---|---:|---:|---:|---:|---:|
| BTCUSDT Spot | 44,640 | 3,800,149 B | 3,799,357 B | 19.05 s | 272,666,624 B |
| ETHUSDT Spot | 44,640 | 3,775,089 B | 3,518,378 B | 18.59 s | 273,543,168 B |
| BTCUSDT Linear | 44,640 | 3,578,186 B | 3,575,014 B | 19.59 s | 272,875,520 B |
| ETHUSDT Linear | 44,640 | 3,595,902 B | 3,377,772 B | 19.66 s | 272,273,408 B |

These are fresh single-process Windows measurements, not future upper bounds.

## Data semantics

The implementation always sends explicit `category`, uses an internal
half-open range and sends `end_exclusive - 1`, because the live V5 probe
returned the candle with open time equal to `end`. Each response is reverse
ordered; the adapter deduplicates/conflict-checks then writes ascending data.
REST historical candles have `knowledge_time = null` and
`knowledge_time_basis = unknown_historical`. A candle is accepted only when its
end is before Bybit server time. WebSocket `confirm=true` is documented for a
future realtime collector but is not implemented here.

Linear volume/turnover are mapped to base/quote per the official kline
documentation. Spot keeps both source values and maps them under the frozen
fixture contract with a documented limitation: the published REST kline page
does not explicitly state the Spot units; this must not be silently reused for
a different Bybit source.

## Architecture and compatibility

`normalized/ohlcv/v2` is an exchange-neutral schema. Nullable source-specific
fields prevent a venue from fabricating Binance-only trade-count/taker fields.
New V2 datasets use explicit `market_type=perpetual` for linear perpetuals;
the completed V1 Binance derivative namespace is retained exactly as-is. A
Binance V2 REST-tail bridge is regression-tested, but does not migrate or
rewrite V1 data. Typed descriptors validate exchange, market type, contract
type and dataset ID before routing.

## Metadata and coverage

Instrument-info snapshots are immutable and retain the raw item plus retrieval
provenance. Spot `basePrecision` is retained as metadata and is not treated as
`quantity_step`; current Spot responses do not provide `launchTime`. Linear
responses provide `launchTime` (BTCUSDT: 2020-03-15; ETHUSDT: 2021-03-15).
These dates are listing metadata, not a claim of complete REST history. A
bounded REST probe observed first non-empty availability at: Spot BTCUSDT and
ETHUSDT 2021-07-05T12:00Z, linear BTCUSDT 2020-03-25T10:36Z, and linear ETHUSDT
2021-03-15T00:00Z. This is observed endpoint coverage only, not a completeness
or listing-date claim.

## Verified sources and limitations

Verified on 2026-08-10: [V5 kline](https://bybit-exchange.github.io/docs/v5/market/kline),
[V5 instruments info](https://bybit-exchange.github.io/docs/v5/market/instrument),
[public kline WebSocket](https://bybit-exchange.github.io/docs/v5/websocket/public/kline),
[server time](https://bybit-exchange.github.io/docs/v5/market/time), and
[rate limits](https://bybit-exchange.github.io/docs/v5/rate-limit).
The official historical-data portal was inspected; its exposed catalog did not
verify a regular traded-price OHLCV archive equivalent to the V5 endpoint, so
this phase uses REST bootstrap and makes no archive-completeness claim.

## Cross-source DQ sanity

The July Binance and Bybit partitions align on all 44,640 minute keys for each
matching instrument/market. Mean-close ratios Bybit/Binance were 1.000004
(BTC Spot), 1.000013 (ETH Spot), 0.999984 (BTC perpetual), and 0.999999 (ETH
perpetual). This rejects obvious timezone, ordering, and unit-scale defects; it
is not an exact-price assertion and is not a cross-exchange feature.

Known remaining limitations: gap causes are `UNKNOWN` unless independently
proven; no realtime WebSocket collector/backfill is in this phase; and
historical availability/knowledge time is not inferred. REST partial-file
recovery is run-owned/stale-age safe: active partials are untouched and stale
ones are quarantined. A 403 is a 600-second explicit cooldown error, rather
than a short retry.

## Gate evidence

The full gate covers source-envelope identity and rate-limit failures,
180/1000-row half-open boundaries, empty/repeated/out-of-range pages,
conflicting duplicates, current-candle exclusion, Spot/Linear units,
official-shaped metadata fixtures, immutable restart recovery, stale partials,
UNKNOWN gaps, four-way routing, and Binance V2 regression. Final command
results are recorded at handoff.
The next Companion roadmap phase is **PHASE 1C**, not any trading or model work.

Final handoff gate: `uv lock --check` passed; `ruff check .` passed; full
`pytest -q` passed with 69 tests and 81% coverage; `config-check` passed;
`health` exited 0 with all operational checks PASS and the intentionally
informational `growth_projections=UNKNOWN` from the PHASE 0 health model.

## Measured storage and projections

The four live July objects total 14,749,326 B raw JSON and 14,270,521 B
Parquet (29,019,847 B combined; compression ratio 1.03 raw/Parquet). Individual
Parquet sizes are Spot BTC 3,799,357 B, Spot ETH 3,518,381 B, linear BTC
3,575,010 B and linear ETH 3,377,773 B. The Phase 1A measured combined Binance
monthly baseline is 22,650,089 B. Therefore the current two-venue OHLCV-only
projection is 51,669,936 B/month: 51.7 MB / 155.0 MB / 620.0 MB for 1 / 3 / 12
months, excluding metadata, manifests, gaps, temporary atomic files and future
sources. Fresh Bybit process peaks were 272.3-273.5 MB (decimal bytes); the
earlier Binance process peak was 219,140,096 B. Sequential operation remains
the laptop-safe default.

For a rough full bootstrap, multiplying the Bybit July footprint by the mean
observed coverage months (61, 61, 77, 65) gives about 1.93 GB for Bybit; with
the Phase 1A 2.12 GB baseline it is about 4.05 GB before safety headroom.
