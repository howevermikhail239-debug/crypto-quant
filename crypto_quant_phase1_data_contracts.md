# PHASE 1 FIELD-LEVEL DATA CONTRACTS

**Status:** design contract, must be frozen with captured sanitized fixtures before adapter implementation  
**Contract version:** 0.1.0  
**Official documentation verified:** 2026-08-10

This registry is normative for PHASE 1, but remains **DRAFT / NOT FROZEN** until the fixture gates in section 6 pass. A collector may parse only a source whose complete accepted payload shape is represented here and in a fixture. Unknown fields remain in the byte-faithful raw object, raise a schema-drift observation, and are not normalized until the contract is versioned.

Normative inheritance is allowed only where this document says `inherits`. An inherited contract means every individually listed parent field, requirement, unit, timestamp rule, validation and limitation applies unchanged; the child section lists every override. This is not an informal cross-reference. REST request parameters and response envelopes are part of the source contract, even though they are not market-data rows. Archive formats never inherit REST/WS contracts until their own header/schema fixture is frozen.

Canonical numerics are Decimal; canonical timestamps are UTC. `required` below means required by the documented payload variant, not globally non-null across all exchanges.

## 1. Common transport envelope

### REST/archive

| Raw field | Meaning / unit / timestamp | Required | Canonical handling | Validation / limitation |
|---|---|---:|---|---|
| source URL/object key | Exact endpoint/archive object | yes | `source_uri`, manifest | HTTPS; expected official host |
| request parameters | Exact query | REST | manifest request | Canonical serialization + hash |
| HTTP status | Response status | REST | ingestion event | 2xx for data; errors preserved |
| rate-limit/time headers | Exchange headers | no | raw headers + health metrics | Never treated as market observation |
| response/archive bytes | Byte-faithful payload | yes | immutable raw object | Content SHA-256; source checksum when present |
| `retrieved_at` | Local UTC download time | yes | envelope/manifest | Audit only; not historical knowledge time |
| `received_at` | Final-byte local receive time | REST | envelope | Clock-quality metadata required |
| source checksum | Archive checksum | no | manifest checksum | Verify before parse; absence explicit |

### Binance REST response envelope

Successful market-data endpoints return either a documented object or array as their top-level payload; they do not use a stable application-level success envelope. Request fields are stored individually as `symbol`, interval/period, `startTime`, `endTime`, `limit`, `fromId` and any endpoint-specific cursor exactly as sent. Absence is distinct from an explicit value. Error payload fields (`code`, `msg`) and HTTP/rate-limit headers are raw control-plane records and never normalized as market observations.

### Bybit V5 REST response envelope

| Source field | Meaning / unit / timestamp | Required | Canonical handling | Validation / limitation |
|---|---|---:|---|---|
| `retCode` | Application result code | yes | ingestion status | Must be `0` before data normalization |
| `retMsg` | Application result message | yes | raw control record | Preserve even on success |
| `retExtInfo` | Additional result information/object | yes/current | raw control record | Preserve all keys; never market data |
| `time` | Bybit response generation/server time, ms | yes/current | exchange clock/control sample | Not event or knowledge time of every row |
| `result.category` | Product category | endpoint-dependent | identity/routing check | Request category must always be explicit |
| `result.symbol` | Requested/result symbol | endpoint-dependent | identity check | Exact native symbol |
| `result.list[]` | Endpoint records | yes | parsed by endpoint contract | Preserve original ordering before normalization |
| `result.nextPageCursor` | Pagination cursor | endpoint-dependent | checkpoint/request state | Empty terminates traversal; never a market field |

Every REST request manifest stores the exact endpoint plus each documented parameter (`category`, `symbol`, interval/`intervalTime`, `start`/`startTime`, `end`/`endTime`, `limit`, `cursor`) as a typed nullable field and the canonical query hash. Endpoint sections below declare which subset is legal and required.

### PHASE 1 REST request and pagination contract matrix

| Source endpoint | Required request fields | Optional request fields | Pagination / ordering rule | Known limitation |
|---|---|---|---|---|
| Binance Spot `/api/v3/exchangeInfo` | none globally | `symbol` or `symbols`, permissions/status filters when documented | Snapshot response; no cursor | Query shape versioned because optional filters evolve |
| Binance Spot `/api/v3/klines` | `symbol`, `interval` | `startTime`, `endTime`, `timeZone`, `limit` | Advance by last open time + one interval; detect inclusive-boundary duplicates | max 1000 current docs |
| Binance Spot `/api/v3/historicalTrades` | `symbol` | `limit`, `fromId` | Advance stable trade ID with overlap/dedup | API key requirement and depth checked before use |
| Binance Spot `/api/v3/aggTrades` | `symbol` | `fromId`, `startTime`, `endTime`, `limit` | ID or bounded-time pagination, never mix modes silently | max 1000 current docs |
| Binance USDⓈ-M `/fapi/v1/exchangeInfo` | none | none in baseline | Snapshot; no cursor | Full payload fixture required |
| Binance USDⓈ-M `/fapi/v1/klines` | `symbol`, `interval` | `startTime`, `endTime`, `limit` | Advance by last open time + interval with overlap check | max 1500; weight depends on limit |
| Binance USDⓈ-M `/fapi/v1/aggTrades` | `symbol` | `fromId`, `startTime`, `endTime`, `limit` | Documented ID/time pagination | REST only last 24h; max one-hour query range |
| Binance USDⓈ-M `/fapi/v1/fundingRate` | none globally | `symbol`, `startTime`, `endTime`, `limit` | Ascending; continue from last `fundingTime` + 1ms with dedup | max 1000; shared limiter |
| Binance USDⓈ-M `/fapi/v1/openInterest` | `symbol` | none | Point observation, no pagination | Not history |
| Binance USDⓈ-M `/futures/data/openInterestHist` | `symbol`, `period` | `limit`, `startTime`, `endTime` | Bounded chronological windows with key dedup | latest one month only |
| Bybit `/v5/market/instruments-info` | explicit `category` | `symbol`, `status`, `baseCoin`, `limit`, `cursor` as category allows | Follow `nextPageCursor` until empty | Linear universe requires pagination |
| Bybit `/v5/market/kline` | explicit `category`, `symbol`, `interval` | `start`, `end`, `limit` | Response reverse-sorted; move earlier/later bound explicitly, normalize ascending, dedup open time | max 1000; latest candle may be open |
| Bybit `/v5/market/recent-trade` | explicit `category` | `symbol`, `baseCoin`, `optionType`, `limit` as category allows | Recent snapshot, no historical cursor | spot max 60; non-spot max 1000 |
| Bybit `/v5/market/funding/history` | explicit `category`, `symbol` | `startTime`, `endTime`, `limit` | Reverse/source order normalized; bounded windows, timestamp dedup | `startTime` alone invalid; max 200 |
| Bybit `/v5/market/open-interest` | explicit `category`, `symbol`, `intervalTime` | `startTime`, `endTime`, `limit`, `cursor` | Follow `nextPageCursor`; normalize observation order | max 200 |

Bulk/archive object-listing requests additionally store bucket/host, full prefix/key, continuation token, requested checksum object and raw listing response. WebSocket subscription requests store exact topic list, arguments, connection URL and session ID.

### WebSocket

| Raw field | Meaning | Required | Canonical handling | Validation / limitation |
|---|---|---:|---|---|
| connection/session ID | Local WS session identity | yes | raw envelope | New ID after reconnect |
| topic/stream | Native subscription | yes | source dataset routing | Must match configured instrument |
| payload bytes/text | Exact message | yes | immutable raw envelope | Hash before normalization |
| `received_at` | Local receipt time | yes | common envelope | Clock offset/uncertainty required |
| source sequence/message ID | Native continuity evidence | no | source-specific fields | Absence means gaps may be unprovable |
| ping/pong/reconnect metadata | Connection health | yes | health/checkpoint registry | Not market data |

---

# 2. Binance Spot contracts

Official sources: [Spot REST market data](https://developers.binance.com/en/docs/catalog/core-trading-spot-trading/api/rest-api/market), [Spot WebSocket streams](https://github.com/binance/binance-spot-api-docs/blob/master/web-socket-streams.md), [bulk archive](https://github.com/binance/binance-public-data).

## 2.1 Instrument metadata — `/api/v3/exchangeInfo`

| Source field | Meaning / source unit | Required | Canonical field / transformation | Validation / known limitation |
|---|---|---:|---|---|
| `timezone` | Exchange timezone label | response | raw metadata | Expect UTC but do not derive event times from label |
| `serverTime` | Exchange server time, ms | response variant | clock sample | Some documentation says ignore in favor of time endpoint |
| `rateLimits[]` | Current limiter definitions | yes | health/config snapshot | Preserve all nested fields |
| `exchangeFilters[]` | Exchange-wide rules | yes | raw metadata | Preserve opaque if unsupported |
| `symbols[]` | Instrument list | yes | one metadata snapshot per symbol | Filter exact BTCUSDT/ETHUSDT + SPOT identity |
| `symbols[].symbol` | Native symbol | yes | `native_symbol` | Uppercase non-empty |
| `symbols[].status` | Trading status | yes | `status` | Preserve source enum |
| `symbols[].baseAsset` | Base asset | yes | `base_asset` | Must match identity |
| `symbols[].quoteAsset` | Quote asset | yes | `quote_asset`, spot `settle_asset` default | Must match identity |
| `symbols[].baseAssetPrecision` | Base display/account precision | yes | raw metadata | Not quantity step |
| `symbols[].quotePrecision` | Quote precision | yes | raw metadata | Not price tick |
| `symbols[].quoteAssetPrecision` | Quote asset precision | yes | raw metadata | Preserve separately |
| `symbols[].orderTypes[]` | Supported order types | yes | raw metadata | Snapshot only |
| `symbols[].icebergAllowed` | Iceberg support | yes | raw metadata | Boolean |
| `symbols[].ocoAllowed` | OCO support | yes | raw metadata | Boolean |
| `symbols[].otoAllowed` | OTO support | no/version-dependent | raw metadata | Nullable; schema drift tracked |
| `symbols[].quoteOrderQtyMarketAllowed` | Quote-sized market support | yes | raw metadata | Does not change trade quantity unit |
| `symbols[].allowTrailingStop` | Trailing stop support | yes | raw metadata | Boolean |
| `symbols[].cancelReplaceAllowed` | Cancel/replace support | no/version-dependent | raw metadata | Nullable |
| `symbols[].amendAllowed` | Amend support | no/version-dependent | raw metadata | Nullable |
| `symbols[].isSpotTradingAllowed` | Spot enabled | yes | identity/status check | Must be true for active spot scope |
| `symbols[].isMarginTradingAllowed` | Margin enabled | yes | raw metadata | Margin is outside PHASE 1 |
| `symbols[].filters[]` | Typed trading filters | yes | raw JSON + typed extraction by `filterType` | Preserve every nested key; unknown filterType blocks silent normalization |
| `PRICE_FILTER.tickSize` | Price increment, quote/base | conditional | `price_tick=Decimal` | >0 for trading symbols |
| `LOT_SIZE.stepSize` | Base quantity increment | conditional | `quantity_step=Decimal`; unit base asset | >0 |
| `LOT_SIZE.minQty/maxQty` | Base quantity bounds | conditional | raw/typed metadata | min≤max |
| `MIN_NOTIONAL` or `NOTIONAL` fields | Quote notional constraints | conditional | raw/typed metadata | Filter-version-specific |
| `symbols[].permissions[]` | Legacy permissions | no | raw metadata | May be replaced by permission sets |
| `symbols[].permissionSets[][]` | Permission combinations | no | raw metadata | Preserve nested structure |
| `symbols[].defaultSelfTradePreventionMode` | Default STP | no | raw metadata | Trading outside MVP |
| `symbols[].allowedSelfTradePreventionModes[]` | Allowed STP | no | raw metadata | Trading outside MVP |

Canonical fixed values: `exchange=binance`, `market_type=spot`, `contract_type=spot`, `quantity_unit=base_asset`, `notional_unit=quote_asset`, `contract_size=null`.

## 2.2 OHLCV — `/api/v3/klines`, bulk `klines`, WS kline

| Source field | Meaning / unit / timestamp | Required | Canonical field / transformation | Validation / limitation |
|---|---|---:|---|---|
| array `[0]` / WS `k.t` | Candle open time; REST ms, bulk may be µs from 2025-01-01 | yes | `open_time`; explicit source time-unit parser | Aligned to interval; never magnitude-guess without source contract |
| `[1]` / `k.o` | Open price, quote/base | yes | `open` Decimal | >0 |
| `[2]` / `k.h` | High price | yes | `high` | high≥max(open,close,low) |
| `[3]` / `k.l` | Low price | yes | `low` | low≤min(open,close,high) |
| `[4]` / `k.c` | Close/latest price | yes | `close` | Final dataset only if candle closed |
| `[5]` / `k.v` | Base asset volume | yes | `base_volume` | ≥0 |
| `[6]` / `k.T` | Candle close time | yes | `close_time` with documented inclusive end | Must match interval boundary |
| `[7]` / `k.q` | Quote asset volume | yes | `quote_volume` | ≥0 |
| `[8]` / `k.n` | Trade count | yes | `trade_count` integer | ≥0 |
| `[9]` / `k.V` | Taker-buy base volume | yes | `taker_buy_base_volume` | 0≤value≤base_volume |
| `[10]` / `k.Q` | Taker-buy quote volume | yes | `taker_buy_quote_volume` | 0≤value≤quote_volume |
| `[11]` / `k.B` | Ignore field | yes | raw only | Never used as feature |
| WS `e` | Event type | WS | raw envelope | Must be `kline` |
| WS `E` | Event generation time, ms | WS | `exchange_timestamp` | Not candle close time |
| WS `s` / `k.s` | Symbol | WS | identity check | Must match subscription |
| WS `k.i` | Interval | WS | `interval` | Must equal configured interval |
| WS `k.f`, `k.L` | First/last trade IDs | WS | source revision evidence | Ordering sanity only |
| WS `k.x` | Closed flag | WS | `is_closed` | Only `true` finalized |

Historical `knowledge_time_basis` remains `documented_publication_time`, `conservative_inferred`, or `retrieval_only_unknown`; `retrieved_at` is never substituted.

## 2.3 Raw trade — bulk `trades` / `/api/v3/historicalTrades`

| Source field | Meaning / unit | Required | Canonical mapping | Validation / limitation |
|---|---|---:|---|---|
| `id` | Native trade ID | yes | `native_trade_id` | Unique within instrument/source |
| `price` | Execution price | yes | `price` | >0 |
| `qty` | Base quantity | yes | `source_quantity`, `quantity_base` | >0 |
| `quoteQty` | Quote notional | yes | `notional_quote` direct | Compare to price×qty within rounding tolerance |
| `time` | Trade event time; API ms, archive time unit per archive contract | yes | `event_time`, `exchange_timestamp` | Monotonic non-decreasing after sort |
| `isBuyerMaker` | Buyer resting/maker flag | yes | `buyer_is_maker`; taker SELL if true, BUY if false after fixture gate | Fixture gate mandatory |
| `isBestMatch` | Best-match flag | yes | raw/source field | No trading interpretation without hypothesis |

## 2.4 Aggregate trade — `/api/v3/aggTrades`, bulk `aggTrades`, WS `aggTrade`

| Source field | Meaning | Required | Canonical mapping | Validation / limitation |
|---|---|---:|---|---|
| `a` | Aggregate trade ID | yes | `aggregate_trade_id` | Unique within instrument/source |
| `p` | Price | yes | `price` | >0 |
| `q` | Aggregated base quantity | yes | `source_quantity`, `quantity_base` | >0 |
| `f` | First constituent trade ID | yes | `first_trade_id` | f≤l |
| `l` | Last constituent trade ID | yes | `last_trade_id` | l≥f; count not assumed equal l-f+1 without fixture |
| `T` | Aggregate event time, ms | yes | `event_time`, `exchange_timestamp` | UTC conversion |
| `m` | Buyer-maker flag | yes | same verified taker-side rule as raw contract | Separate fixture from raw dataset |
| `M` | Best-price-match flag | yes | raw/source field | No feature by default |
| WS `e`, `E`, `s` | Event type/generation time/symbol | WS | envelope + identity | Must match stream |

Aggregation semantics: fills from the same taker order at the same price/time grouping per current official docs. Dataset ID must include `aggregate_trade`.

---

# 3. Binance USDⓈ-M linear perpetual contracts

Official sources: [USDⓈ-M market-data REST](https://developers.binance.com/en/docs/catalog/core-trading-derivatives-trading-usd-s-m-futures/api/rest-api/market-data), [liquidation stream](https://developers.binance.com/en/docs/products/derivatives-trading-usds-futures/websocket-market-streams/Liquidation-Order-Streams), [bulk archive](https://github.com/binance/binance-public-data).

## 3.1 Instrument metadata — `/fapi/v1/exchangeInfo`

| Source field | Meaning | Required | Canonical handling | Validation / limitation |
|---|---|---:|---|---|
| `timezone` | Exchange timezone | yes | raw metadata | UTC expected |
| `serverTime` | Server time | yes | raw/clock observation | Time endpoint preferred for clock samples |
| `rateLimits[]` | Current API limits | yes | limiter snapshot | Preserve nested fields |
| `exchangeFilters[]` | Exchange rules | yes | raw metadata | Preserve |
| `assets[]` | Margin asset metadata | yes | raw metadata | Only USDT relevant; trading outside MVP |
| `symbols[].symbol` | Native contract symbol | yes | `native_symbol` | BTCUSDT/ETHUSDT |
| `pair` | Underlying pair | yes | raw metadata | Identity cross-check |
| `contractType` | Contract type | yes | `contract_type=linear_perpetual` only after exact enum match | Reject dated/inverse mismatch |
| `deliveryDate` | Delivery/delist time, ms | yes | `delivery_or_delist_time` nullable/semantic | Perpetual sentinel must be documented |
| `onboardDate` | Listing/onboard time, ms | yes | `listing_time` | UTC |
| `status` | Contract status | yes | `status` | Preserve source enum |
| `maintMarginPercent`, `requiredMarginPercent` | Margin metadata | yes/current payload | raw metadata | Docs may mark ignore; not feature |
| `baseAsset` | Base | yes | `base_asset` | Identity check |
| `quoteAsset` | Quote | yes | `quote_asset` | Identity check |
| `marginAsset` | Settlement/margin asset | yes | `settle_asset` | USDT for scope |
| `pricePrecision`, `quantityPrecision`, `baseAssetPrecision`, `quotePrecision` | Display/format precision | yes | raw metadata | Not substitutes for tick/step |
| `underlyingType`, `underlyingSubType[]` | Underlying classification | yes | raw metadata | Preserve |
| `settlePlan` | Settlement metadata | yes | raw metadata | Preserve |
| `triggerProtect` | Trigger protection | yes | raw metadata | Trading outside MVP |
| `filters[]` | Typed filters | yes | raw + typed extraction | Unknown filter blocks silent normalization |
| `PRICE_FILTER.tickSize` | Price tick | conditional | `price_tick` | >0 |
| `LOT_SIZE.stepSize` | Quantity increment | conditional | `quantity_step`; unit resolved by contract | >0 |
| `orderTypes[]`, `timeInForce[]` | Supported execution settings | yes | raw metadata | Trading outside MVP |
| `liquidationFee` | Liquidation fee rate | yes | raw metadata | Not liquidation event size |
| `marketTakeBound` | Market-order bound | yes | raw metadata | Trading outside MVP |

Canonical fixed fields: `exchange=binance`, `market_type=perpetual`, `settle_asset=USDT`. `quantity_unit`, `contract_size` and any multiplier remain `UNVERIFIED` until official response/rules fixture is frozen; adapters may not invent a multiplier.

## 3.2 OHLCV — `/fapi/v1/klines`, bulk/WS kline

**Normative inheritance:** inherits every individually listed field in section 2.2: array `[0]` through `[11]`, WS `e`, `E`, `s`, `k.t/o/h/l/c/v/T/q/n/V/Q/B/i/f/L/x`, including timestamp, closure and validation rules. Overrides: identity is `binance/usdt_linear_perpetual`; source time units are frozen independently; volume/taker-buy volume and quote-asset volume keep separate source units; the fixture must confirm base/quote semantics before canonical assignment. No archive inherits this mapping until its header/schema contract is frozen.

## 3.3 Raw trade — futures bulk `trades` / documented raw trade endpoint

| Source field | Meaning | Required | Canonical mapping | Validation / limitation |
|---|---|---:|---|---|
| `id` | Native trade ID | yes | `native_trade_id` | Unique per instrument/source |
| `price` | Execution price | yes | `price` | >0 |
| `qty` | Source quantity | yes | `source_quantity`; normalized unit from metadata contract | >0 |
| `quoteQty` | Quote notional where supplied | conditional | `notional_quote` direct | Preserve null if archive variant lacks it |
| `time` | Trade time | yes | `event_time`, `exchange_timestamp` | Explicit source time unit |
| `isBuyerMaker` | Buyer-maker flag | yes | taker-side derivation only after futures fixture gate | No reuse of spot fixture as sole proof |
| `isRPITrade` | RPI trade flag in current response variants | conditional | `is_rpi_trade` | Nullable across archive versions |

## 3.4 Aggregate trade — `/fapi/v1/aggTrades`, bulk/WS

| Source field | Meaning | Required | Canonical mapping | Validation / limitation |
|---|---|---:|---|---|
| `a` | Aggregate ID | yes | `aggregate_trade_id` | Unique |
| `p` | Price | yes | `price` | >0 |
| `q` | Aggregate source quantity | yes | `source_quantity`; unit via metadata contract | >0 |
| `nq` | Normal quantity excluding RPI trades | current REST variant | source-specific field | Must not replace `q`; nullable for old archive/WS variants |
| `f` | First constituent trade ID | yes | `first_trade_id` | Must be ≤ `l` |
| `l` | Last constituent trade ID | yes | `last_trade_id` | Must be ≥ `f` |
| `T` | Aggregate trade timestamp | yes | `event_time`, `exchange_timestamp` | UTC |
| `m` | Buyer-maker flag | yes | taker-side only after fixture gate | Separate aggregation semantics |

Current REST limitation: futures aggTrades history is not older than 24 hours; bulk archive is preferred for deeper bootstrap.

## 3.5 Funding — `/fapi/v1/fundingRate`

| Source field | Meaning / unit | Required | Canonical mapping | Validation / limitation |
|---|---|---:|---|---|
| `symbol` | Contract symbol | yes | identity check | Perpetual only |
| `fundingRate` | Realized decimal funding rate | yes | `funding_rate` Decimal | Plausibility bounds, no percent multiplication |
| `fundingTime` | Settlement time, ms | yes | `funding_time`, `event_time` | Interval/order checks |
| `markPrice` | Mark price associated with charge | yes/current | `mark_price` | >0 |
| `rateType` | Regular/Special classification | current | source rate subtype | Preserve; canonical `rate_kind=realized_settlement` |

Funding interval/caps come from current funding info/metadata snapshot. Knowledge time is settlement publication, not automatically `fundingTime`.

## 3.6 OI current — `/fapi/v1/openInterest`

| Source field | Meaning | Required | Canonical mapping | Validation / limitation |
|---|---|---:|---|---|
| `openInterest` | Current source OI value | yes | `oi_source_value`; `oi_semantic=current_total` | Source unit must be frozen from official contract fixture |
| `symbol` | Contract symbol | yes | identity check | Exact perpetual identity |
| `time` | Transaction/observation time, ms | yes | `observation_time`, `exchange_timestamp` | Does not prove publication delay |

## 3.7 OI history — `/futures/data/openInterestHist`

| Source field | Meaning | Required | Canonical mapping | Validation / limitation |
|---|---|---:|---|---|
| `symbol` | Contract symbol | yes | identity check | Exact identity |
| `sumOpenInterest` | Total source OI | yes | `oi_source_value`; unit contract required | Do not label simply `oi` |
| `sumOpenInterestValue` | Total OI value | yes | direct source quote/notional representation after unit fixture | Keep separate from converted values |
| `CMCCirculatingSupply` | External circulating supply included by source | current | raw only | Not used as OI or feature in PHASE 1 |
| `timestamp` | Period end, ms | yes | `observation_time` | Latest one month only per current docs |

## 3.8 Liquidation — `<symbol>@forceOrder`

| Source field | Meaning | Required | Canonical mapping | Validation / limitation |
|---|---|---:|---|---|
| `e` | Event type | yes | raw envelope | `forceOrder` |
| `E` | Event generation time, ms | yes | `exchange_timestamp` | Not order trade time |
| `o.s` | Symbol | yes | identity check | Exact contract |
| `o.S` | Forced order side BUY/SELL | yes | `source_side`, semantic `order_side` | Liquidated position side derivation remains unverified until fixture/official rule |
| `o.o` | Order type | yes | `order_type` | Preserve |
| `o.f` | Time in force | yes | `time_in_force` | Preserve |
| `o.q` | Original source quantity | yes | `source_quantity` | Unit from metadata contract |
| `o.p` | Order price | yes | `source_price`, `price_semantic=order_price` | May differ from execution average |
| `o.ap` | Average price | yes | canonical average fill price | >0 when filled |
| `o.X` | Order status | yes | `order_status` | Preserve |
| `o.l` | Last filled quantity | yes | `last_filled_quantity` | ≤ accumulated/original under documented semantics |
| `o.z` | Accumulated filled quantity | yes | `accumulated_filled_quantity` | ≥0 |
| `o.T` | Order trade time, ms | yes | `event_time` | UTC |

Completeness is always `incomplete_throttled_snapshot`: at most one selected liquidation order per symbol within each 1000ms interval is pushed. Current official sources conflict on whether the selected order is the `latest` or the `largest`; freeze this as `DOC_CONFLICT_LATEST_VS_LARGEST` until Binance resolves the inconsistency. No event-count completeness is inferred.

---

# 4. Bybit Spot contracts

Official sources: [instruments info](https://bybit-exchange.github.io/docs/v5/market/instrument), [kline](https://bybit-exchange.github.io/docs/v5/market/kline), [recent trade](https://bybit-exchange.github.io/docs/v5/market/recent-trade), [trade WS](https://bybit-exchange.github.io/docs/v5/websocket/public/trade), [historical portal](https://www.bybit.com/en/derivative-activity/history-data).

## 4.1 Instrument metadata — `/v5/market/instruments-info?category=spot`

| Source field | Meaning | Required | Canonical mapping | Validation / limitation |
|---|---|---:|---|---|
| envelope `category` | Product category | yes | must be spot | Reject default/linear mismatch |
| `list[].symbolId` | Native numeric symbol ID | yes/current | raw metadata | Not canonical identity |
| `symbol` | Native symbol | yes | `native_symbol` | BTCUSDT/ETHUSDT |
| `baseCoin` | Base | yes | `base_asset` | Identity check |
| `quoteCoin` | Quote | yes | `quote_asset`, spot settle default | Identity check |
| `innovation` | Deprecated flag | no | raw only | Use symbolType instead |
| `symbolType` | Region/type classification | no | raw metadata | Preserve |
| `xstockMultiplier` | Xstock multiplier | no | raw metadata | Out of scope; must not alter crypto identity silently |
| `status` | Trading status | yes | `status` | Spot docs currently Trading only |
| `marginTrading` | Margin support | yes | raw metadata | Margin outside scope |
| `stTag` | Special-treatment flag | yes/current | raw metadata/DQ context | Preserve source enum |
| `lotSizeFilter.basePrecision` | Base precision | yes | raw metadata | Not necessarily quantity step |
| `lotSizeFilter.quotePrecision` | Quote precision | yes | raw metadata | Not price tick |
| `minOrderQty/maxOrderQty` | Deprecated quantity limits | no | raw metadata | Do not use as current rule if docs deprecate |
| `minOrderAmt/maxOrderAmt` | Quote amount limits/deprecated max | no | raw metadata | Respect current docs |
| `maxLimitOrderQty` | Max limit quantity | yes/current | raw metadata | Mutable |
| `maxMarketOrderQty` | Max market quantity | yes/current | raw metadata | Mutable |
| `postOnlyMaxLimitOrderSize` | Post-only/RPI max | no/current | raw metadata | Mutable |
| `priceFilter.tickSize` | Price increment | yes | `price_tick` | >0 |
| `riskParameters.priceLimitRatioX/Y` | Price risk ratios | no/current | raw metadata | Formula/version may change |

`quantity_step` for Bybit Spot is not set from `basePrecision` without a verified rule/fixture. If no explicit quantity step is available, canonical field remains unknown and the contract blocks claims that precision equals step.

## 4.2 OHLCV — `/v5/market/kline?category=spot`, WS kline

| Source field | Meaning | Required | Canonical mapping | Validation / limitation |
|---|---|---:|---|---|
| envelope `category`, `symbol` | Category/symbol | yes | identity check | Category must be explicit; API defaults linear if omitted |
| `list[0]` | Candle start time, ms | yes | `open_time` | Response sorted reverse; normalize ascending |
| `list[1]` | Open price | yes | `open` Decimal | >0 |
| `list[2]` | High price | yes | `high` Decimal | ≥max(open,close,low) |
| `list[3]` | Low price | yes | `low` Decimal | ≤min(open,close,high) |
| `list[4]` | Close/latest price | yes | `close` Decimal | Current unfinished close is latest price; finalize only closed candle |
| `list[5]` | Trade volume | yes | `source_volume` | Spot unit is marked `REQUIRES_FIXTURE`; do not inherit linear rule |
| `list[6]` | Turnover | yes | source turnover | Spot unit is marked `REQUIRES_FIXTURE` |
| WS `topic`, `type`, `ts` | Topic/message type/system generation time | WS | envelope/exchange timestamp | `type=snapshot` is message type |
| WS `data[].start` | Candle start, ms | WS | `open_time` | Interval-aligned |
| WS `data[].end` | Candle end, ms | WS | `close_time` | Boundary sanity |
| WS `data[].open` | Open price | WS | `open` | Decimal >0 |
| WS `data[].high` | High price | WS | `high` | OHLC invariant |
| WS `data[].low` | Low price | WS | `low` | OHLC invariant |
| WS `data[].close` | Close/latest price | WS | `close` | Finalize only if `confirm=true` |
| WS `data[].volume` | Source volume | WS | source value pending unit fixture | Explicit unit required before normalization |
| WS `data[].turnover` | Source turnover | WS | source value pending unit fixture | Explicit unit required before normalization |
| WS `confirm` | Closed flag | WS | `is_closed` | Only true finalized |
| WS `timestamp` | Last matched order time in candle | WS | source field | Not candle publication time |

## 4.3 Raw trade — REST/WS/history portal

| Source field | Meaning | Required | Canonical mapping | Validation / limitation |
|---|---|---:|---|---|
| `execId` / WS `i` | Trade ID | yes | `native_trade_id` | Unique per source/instrument |
| `symbol` / `s` | Symbol | yes | identity check | Exact spot identity |
| `price` / `p` | Trade price | yes | `price` | >0 |
| `size` / `v` | Trade size | yes | `source_quantity` | Unit `REQUIRES_FIXTURE` for spot archive/REST/WS parity |
| `side` / `S` | Taker side Buy/Sell | yes | `taker_side` direct | Official docs state taker side |
| `time` / `T` | Fill time, ms | yes | `event_time`, `exchange_timestamp` | UTC |
| `isBlockTrade` / `BT` | Block-trade flag | yes/current | `is_block_trade` | Boolean |
| `isRPITrade` / `RPI` | RPI flag | yes/current | `is_rpi_trade` | Boolean/nullable across archives |
| `seq` | Cross sequence | current | `sequence_id` | Multiple messages may share seq; not globally unique |
| WS `topic`, `type`, `ts` | Topic/snapshot/system time | WS | envelope | One message can hold many trades |

Historical portal file columns, timestamp resolution and unit parity must be captured as a separate archive contract version before import.

---

# 5. Bybit Linear USDT perpetual contracts

Official sources: same Bybit market docs plus [OI](https://bybit-exchange.github.io/docs/v5/market/open-interest), [funding](https://bybit-exchange.github.io/docs/v5/market/history-fund-rate), [all liquidation](https://bybit-exchange.github.io/docs/v5/websocket/public/all-liquidation).

## 5.1 Instrument metadata — `category=linear`

| Source field | Meaning | Required | Canonical mapping | Validation / limitation |
|---|---|---:|---|---|
| envelope `category`, `nextPageCursor` | Category/pagination | yes | category check + ingestion state | Cursor required for full universe |
| `symbol`, `symbolId` | Native symbol/ID | yes | native symbol; ID raw | Exact identity |
| `contractType` | Native contract type | yes | `linear_perpetual` only for exact matching enum | Reject other contracts |
| `status` | Instrument status | yes | `status` | Snapshot |
| `baseCoin`, `quoteCoin`, `settleCoin` | Assets | yes | base/quote/settle | Settle must be USDT for scope |
| `symbolType` | Region/type | no | raw metadata | Preserve |
| `launchTime`, `deliveryTime` | Launch/delivery/delist times, ms | yes | listing/delivery fields | Perpetual delivery sentinel treated contractually |
| `deliveryFeeRate` | Delivery fee | yes/current | raw metadata | Usually empty for perpetual |
| `priceScale` | Display scale | yes | raw metadata | Not price tick |
| `leverageFilter.minLeverage/maxLeverage/leverageStep` | Leverage rules | yes | metadata snapshot | Mutable; not market feature |
| `priceFilter.minPrice/maxPrice/tickSize` | Price bounds/increment | yes | tick→`price_tick`; bounds raw | >0, min≤max |
| `lotSizeFilter.minNotionalValue` | Minimum quote notional | yes | raw/typed metadata | Unit quote/settle per contract |
| `maxOrderQty/maxMktOrderQty/minOrderQty/qtyStep` | Quantity rules | yes | qtyStep→`quantity_step`; other fields raw | Source quantity unit resolved by contract fixture |
| `postOnlyMaxOrderQty` | Deprecated post-only max | no | raw metadata | Do not prefer over current field |
| `unifiedMarginTrade` | Unified margin support | yes | raw metadata | Trading outside scope |
| `fundingInterval` | Funding interval minutes | yes | `funding_interval_minutes` | >0 |
| `upperFundingRate/lowerFundingRate` | Funding caps | yes/current | metadata snapshot | lower≤upper |
| `copyTrading` | Copy-trading flag | yes | raw metadata | Out of scope |
| `displayName` | Display name | no | raw metadata | Not identity |
| `forbidUplWithdrawal` | Withdrawal rule | no | raw metadata | Out of scope |
| `riskParameters.priceLimitRatioX/Y` | Price-limit formula inputs | yes/current | raw metadata | Formula version may change |
| `isPreListing`, `preListingInfo` | Prelaunch state/details | yes/current | raw metadata/status | Reject prelaunch for production dataset until Trading |

Bybit docs do not expose one universal `contract_size` field for this response. For in-scope linear BTC/ETH, contract multiplier/quantity representation must be verified; no silent default beyond a versioned explicit decision.

## 5.2 OHLCV — `/v5/market/kline?category=linear`

**Normative inheritance:** inherits every individually listed REST and WS field in section 4.2 (`category`, `symbol`, list indices `[0]`…`[6]`, WS topic/type/ts, start/end, each OHLC price, volume, turnover, confirm and timestamp), with the following complete overrides:

- `list[5] volume` → base coin quantity → `base_volume`;
- `list[6] turnover` → quote coin amount → `quote_volume`.

The response remains reverse sorted and can contain an unfinished latest candle.

## 5.3 Raw public trade — REST/WS/archive

**Normative inheritance:** REST and WS inherit every individually listed field in section 4.3 (`execId/i`, `symbol/s`, `price/p`, `size/v`, `side/S`, `time/T`, `isBlockTrade/BT`, `isRPITrade/RPI`, `seq`, WS topic/type/ts), including validation and nullability. Overrides: identity is Bybit USDT linear perpetual; `side` remains taker side; `size` maps to base quantity only after the linear quantity-unit fixture is frozen. The historical archive does not inherit: its header, fields, timestamp precision, nullability and units require a separate contract version before import.

## 5.4 Funding — `/v5/market/funding/history`

| Source field | Meaning | Required | Canonical mapping | Validation / limitation |
|---|---|---:|---|---|
| envelope `category` | linear/inverse | yes | must be linear | Reject wrong category |
| `list[].symbol` | Symbol | yes | identity check | Exact perpetual |
| `fundingRate` | Realized funding rate, decimal | yes | `funding_rate` | Plausibility/cap checks against contemporaneous metadata |
| `fundingRateTimestamp` | Settlement timestamp, ms | yes | `funding_time`, `event_time` | Knowledge time not automatically identical |

Request contract: `startTime` alone is invalid; max/default 200; per-symbol interval from metadata.

## 5.5 OI — `/v5/market/open-interest`

| Source field | Meaning / unit | Required | Canonical mapping | Validation / limitation |
|---|---|---:|---|---|
| envelope `category`, `symbol` | Product/symbol | yes | identity check | Linear only |
| `list[].openInterest` | Sum of both sides; for BTCUSDT linear unit BTC | yes | `oi_source_value`, `oi_source_unit=base_asset`, `oi_semantic=sum_both_sides`; direct `oi_base` | Keep exact source string |
| `singleOpenInterest` | Single-side OI in same unit | current | separate `oi_semantic=single_side` observation/field | Never overwrite both-sides OI |
| `timestamp` | Observation time, ms | yes | `observation_time` | Does not prove publication latency |
| `nextPageCursor` | Pagination cursor | yes/current | ingestion state | Follow until empty |

Supported intervals are 5min, 15min, 30min, 1h, 4h, 1d; PHASE 1 baseline is 5min. Docs state historical querying may reach launch time, subject to actual page availability and possible delay during extreme volatility.

## 5.6 Liquidation — `allLiquidation.<symbol>`

| Source field | Meaning / unit | Required | Canonical mapping | Validation / limitation |
|---|---|---:|---|---|
| `topic` | Subscription topic | yes | routing | Exact symbol |
| `type` | Message type `snapshot` | yes | `message_type` | Does not mean replaceable state snapshot |
| `ts` | System generation time, ms | yes | `exchange_timestamp` | Different from update time |
| `data[].T` | Updated/event timestamp, ms | yes | `event_time` | UTC |
| `data[].s` | Symbol | yes | identity check | Exact linear perpetual |
| `data[].S` | Liquidated position side | yes | `source_side`; `Buy`→`position_side_liquidated=LONG`, `Sell`→SHORT per docs | Fixture locks exact mapping |
| `data[].v` | Executed size | yes | `source_quantity`; normalized unit via metadata fixture | >0 |
| `data[].p` | Bankruptcy price | yes | `source_price`, `price_semantic=bankruptcy_price` | >0 |

Current docs claim all liquidations with 500ms push frequency. Store completeness claim/version, connection gaps and observed batch structure; do not transform into Binance-equivalent completeness.

---

# 6. Fixture and freeze gates

Before implementation of each adapter:

1. Save sanitized official response/message fixtures with retrieval date and documentation URL.
2. Validate every accepted raw field against this registry.
3. Freeze source time unit, quantity unit, notional unit and nullable behavior.
4. Add unknown-field/schema-drift test.
5. For trades, prove maker/taker truth tables independently per venue/market/source kind.
6. For archives, compare archive schema with REST/WS schema; never assume parity.
7. Version the Data Contract when any field meaning, source, unit or transformation changes.

No CVD, delta or aggressive volume is valid until all relevant trade fixture gates pass. No OI conversion is valid without point-in-time conversion lineage. No liquidation aggregate is comparable across exchanges without explicit completeness/price/side semantics.
