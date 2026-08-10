# REVISED TECHNICAL DESIGN
## Local Crypto Quant & Opportunity System — PHASE 0–1

**Статус:** approved architecture; PHASE 0 complete; PHASE 1A complete; PHASE 1B complete; next actual stage is PHASE 1C  

**Global reservations (non-runtime):** Russian-first user localization (`ru-RU`, fallback `en-US`), Knowledge Base ownership/feature mapping, draft signal/risk-exit/lifecycle interface `0.2.0-draft`, and optional non-blocking PHASE 3F Polymarket external-event interface are approved. PHASE 1C is the next task.
**Версия документа:** 1.3.0  
**Дата проверки внешней документации:** 2026-08-10  
**Нормативная база:** `crypto_quant_master_spec.md`, `crypto_quant_phased_development_prompt.md` и 19 обязательных уточнений пользователя.

**Normative companion artifacts:**

- `crypto_quant_phase1_data_contracts.md` — field-level PHASE 1 source contracts;
- `crypto_quant_api_verification_evidence_2026-08-10.md` — dated API/archive audit note; immutable raw evidence capture is a PHASE 0 gate.

Этот документ формально включает 19 уточнений в архитектурный контракт проекта. При конфликте с более ранним неявным предположением применяется более строгая формулировка этого документа. Архитектура согласована; collectors реализуются только в явно разрешённой текущей фазе.

---

# 0. Нормативные архитектурные решения

1. Spot и perpetual — разные instruments и разные datasets.
2. Наличие historical liquidation archives проверяется отдельно для каждого exchange/contract family; отсутствие REST endpoint не считается доказательством отсутствия архива.
3. Ограниченная OI history bootstrap-ится, после чего OI непрерывно накапливается локально с provenance и gap control.
4. `raw_trade` и `aggregate_trade` — несовместимые source semantics и разные dataset identities.
5. Trades partitioning выбирается по измеренному размеру; baseline — day, при необходимости hour.
6. Retention создаёт append-only deletion audit trail; prediction journal имеет отдельную infinite-retention policy.
7. Historical retrieval time не подменяет historical availability/knowledge time.
8. Cross-exchange joins используют backward point-in-time as-of semantics, а не exact equality.
9. Missing periods регистрируются и анализируются; молчаливое удаление запрещено.
10. Units нормализуются только через versioned transformation contract.
11. Aggressor semantics проходят fixture gate до CVD/delta/aggressive-volume features.
12. Collectors проектируются с учётом sleep, hibernation, restart, network loss, crash и partial writes.
13. Instrument metadata хранится как append-only snapshots.
14. Будущая ML validation использует purging, embargo и walk-forward; chronological split сам по себе недостаточен.
15. PHASE 0–1 не включает Freqtrade/FreqAI и boosting libraries.
16. Каждый source dataset имеет строгий field-level Data Contract.
17. Knowledge-time является обязательной частью data/feature architecture.
18. Schemas и semantic transformations версионируются; incompatible generations не смешиваются.
19. Каждый bootstrap object имеет воспроизводимый provenance manifest.

---

# 1. Revised system architecture

```text
OFFICIAL SOURCES
  Binance Spot / USDⓈ-M              Bybit Spot / Linear
  REST · WebSocket · bulk archive    REST · WebSocket · history portal
                    │
                    ▼
SOURCE ADAPTERS + DATA CONTRACT REGISTRY
  explicit market/contract identity · rate limits · source semantics
                    │
                    ▼
RAW LANDING (source-faithful, immutable while retained)
  archive bytes · REST envelopes · WS batches · headers · request/session metadata
                    │
          ┌─────────┴─────────┐
          ▼                   ▼
VALIDATION                QUARANTINE
schema · IDs · units      invalid/ambiguous objects
timestamps · checksum          │
          │                     │
          └─────────┬───────────┘
                    ▼
NORMALIZATION BUILDS
  typed canonical datasets · explicit units · point-in-time timestamps
                    │
                    ▼
IMMUTABLE PARQUET GENERATIONS
  OHLCV · raw_trade · aggregate_trade · OI · funding · liquidation · metadata
                    │
        ┌───────────┼──────────────────┐
        ▼           ▼                  ▼
  DuckDB views   Gap/DQ registry   Manifests/checkpoints/deletion ledger
        │
        ▼
POINT-IN-TIME DATASET BUILDER (later phase)
  knowledge_time filter · backward as-of · staleness · feature eligibility
        │
        ├── Core Quant namespace (BTC/ETH)
        └── Opportunity namespaces B1/B2 (later, isolated)
```

Control plane состоит из отдельных append-only сущностей:

- instrument registry;
- source/data contract registry;
- ingestion run registry;
- provenance manifest event log;
- checkpoint registry;
- gap registry;
- deletion ledger;
- schema/version registry.

Не создаётся одна широкая таблица `market_data`. Каждый тип наблюдения имеет собственный contract и dataset identity.

---

# 2. Canonical instrument identity model

## 2.1 Identity

Stable `instrument_id` — UUIDv5 или детерминированный hash от immutable natural key:

```text
exchange
venue_environment
market_type
contract_type
native_symbol
base_asset
quote_asset
settle_asset
expiry (nullable)
```

Примеры разных identities:

```text
binance:prod:spot:spot:BTCUSDT:BTC:USDT:USDT:null
binance:prod:perpetual:linear_perpetual:BTCUSDT:BTC:USDT:USDT:null
bybit:prod:spot:spot:BTCUSDT:BTC:USDT:USDT:null
bybit:prod:perpetual:linear_perpetual:BTCUSDT:BTC:USDT:USDT:null
```

Tick size, quantity step и status не входят в identity: они изменяемы и хранятся в metadata snapshots.

## 2.2 Instrument fields

| Field | Type | Null | Meaning |
|---|---|---:|---|
| `instrument_id` | string/UUID | no | Stable canonical identity |
| `exchange` | enum | no | `binance`, `bybit` |
| `venue_environment` | enum | no | `prod`, `testnet` |
| `native_symbol` | string | no | Exchange symbol |
| `market_type` | enum | no | `spot`, `perpetual`, later `future` |
| `contract_type` | enum | no | `spot`, `linear_perpetual`, `inverse_perpetual`, later dated types |
| `base_asset` | string | no | Base asset |
| `quote_asset` | string | no | Quote asset |
| `settle_asset` | string | conditional | Settlement asset; for spot canonical default is quote asset |
| `quantity_unit` | enum/string | conditional | Must be known before a record containing quantity is admitted; metadata discovery rows may carry `UNKNOWN` plus a blocking DQ flag |
| `notional_unit` | enum/string | no | Usually quote/settle asset |
| `price_tick` | decimal | no | Current tick in this snapshot |
| `quantity_step` | decimal nullable | conditional | Required for a tradable instrument only after the source rule is fixture-verified; otherwise `null` + `metadata_incomplete` |
| `contract_size` | decimal | conditional | Contract multiplier if applicable |
| `contract_size_unit` | string | conditional | Unit of contract multiplier |
| `status` | string | no | Exchange trading/listing status |
| `listing_time` | timestamp UTC | conditional | Exchange listing/launch time |
| `delivery_or_delist_time` | timestamp UTC | conditional | Delivery/delisting time |
| `metadata_observed_at` | timestamp UTC | no | When collector observed metadata |
| `exchange_effective_at` | timestamp UTC | conditional | Only if source explicitly supplies effective time |
| `valid_from_knowledge_time` | timestamp UTC | no | Earliest conservative local use time |
| `valid_to_knowledge_time` | timestamp UTC | conditional | Exclusive end, normally next accepted snapshot knowledge time |
| `superseded_at` | timestamp UTC | conditional | Time a later snapshot superseded this version |
| `raw_payload_hash` | string | no | Lineage to source snapshot |

`metadata_observed_at` никогда автоматически не интерпретируется как фактическое exchange effective time.
Point-in-time validity is `[valid_from_knowledge_time, valid_to_knowledge_time)`. The end is derived after the next accepted snapshot; the latest snapshot has an open end. This interval describes local knowledge, not the unknown true exchange change time.

---

# 3. Common normalized record envelope

Все normalized schemas содержат:

| Field | Type | Purpose |
|---|---|---|
| `instrument_id` | UUID/string | Exact instrument identity |
| `source_dataset_id` | string | Includes exchange, market, data type and source semantics |
| `source_event_id` | string nullable | Native ID where available |
| `event_time` | timestamp[ns, UTC] | Economic observation/event time |
| `exchange_timestamp` | timestamp[ns, UTC] nullable | Timestamp explicitly supplied by exchange |
| `source_published_at` | timestamp[ns, UTC] nullable | Publication time only when explicitly supplied by source |
| `received_at` | timestamp[ns, UTC] nullable | Local receive time; absent for historical rows when not observed realtime |
| `retrieved_at` | timestamp[ns, UTC] nullable | Historical REST/archive retrieval time; audit only |
| `processed_at` | timestamp[ns, UTC] | Transformation time |
| `knowledge_time` | timestamp[ns, UTC] | Conservative time when model could use record |
| `knowledge_time_basis` | enum | How knowledge time was established |
| `clock_offset_ms` | int nullable | Measured local-to-exchange clock offset |
| `clock_uncertainty_ms` | int nullable | Timing uncertainty |
| `ingestion_run_id` | UUID/string | Provenance run |
| `raw_object_id` | string | Source object/envelope reference |
| `schema_version` | semver | Canonical schema version |
| `collector_version` | semver+git SHA | Adapter implementation |
| `normalization_version` | semver | Unit/semantic transformations |
| `dq_flags` | list/bitset | Non-destructive quality flags |

Raw numeric fields remain source strings/bytes. Normalized price, quantity and rates use Decimal128 with contract-specific scale; binary float is not canonical storage.

---

# 4. Canonical dataset schemas

## 4.1 OHLCV

Primary key:

```text
(source_dataset_id, instrument_id, interval, open_time)
```

| Field | Null | Notes |
|---|---:|---|
| `interval` | no | Canonical duration, e.g. `PT1M` |
| `open_time` | no | Inclusive start |
| `close_time` | no | Explicit close/end semantics documented per source |
| `open`, `high`, `low`, `close` | no | Price Decimal |
| `base_volume` | conditional | Explicit base asset volume |
| `quote_volume` | conditional | Explicit quote turnover/notional |
| `source_volume` | no | Original numeric value |
| `source_volume_unit` | no | Original unit |
| `trade_count` | yes | Only if source provides it |
| `taker_buy_base_volume` | yes | Source-provided only |
| `taker_buy_quote_volume` | yes | Source-provided only |
| `is_closed` | no | Open candles cannot enter final historical dataset |
| `candle_source` | no | `exchange` or `locally_aggregated` |
| `aggregation_version` | yes | Required for local resampling |
| `source_revision_id` | yes | Native revision/version if supplied |
| `observation_id` | no | Hash of logical key, values, raw object and observation time |

Historical endpoint rows do not prove publication time. `knowledge_time` follows the policy in section 5.

If two closed-candle observations have the same logical key but different values, neither is overwritten. Both remain in immutable generations. A revision/supersession event selects the active observation only when source order is provable; otherwise the conflict is quarantined.

## 4.2 Trades

`raw_trade` and `aggregate_trade` are separate physical and logical datasets.

### Raw trade

| Field | Null | Notes |
|---|---:|---|
| `native_trade_id` | conditional | Native stable trade ID |
| `sequence_id` | yes | Exchange cross sequence if supplied |
| `price` | no | Execution price |
| `source_quantity` | no | Exact source value |
| `source_quantity_unit` | no | Explicit unit |
| `quantity_base` | conditional | Only direct or versioned conversion |
| `notional_quote` | conditional | Direct or price×quantity with lineage |
| `taker_side` | conditional | `BUY`, `SELL`, `UNKNOWN` |
| `maker_side` | conditional | Derived only from verified semantics |
| `buyer_is_maker` | yes | Binance source flag |
| `is_block_trade` | yes | Bybit/source-specific |
| `is_rpi_trade` | yes | Source-specific |
| `classification_version` | conditional | Required when taker side derived |

### Exchange aggregate trade

Additional fields:

| Field | Null | Notes |
|---|---:|---|
| `aggregate_trade_id` | conditional | Native aggregate ID |
| `first_trade_id` | yes | First constituent ID |
| `last_trade_id` | yes | Last constituent ID |
| `aggregate_count` | yes | If supplied or safely derivable |
| `aggregation_kind` | no | `exchange_aggregate` |
| `aggregation_rule_id` | no | Source-specific documented rule |

Aggregate trades никогда не выдаются за raw trades и не дописываются в тот же dataset.

## 4.3 Open interest

Primary key includes source semantics and observation time.

| Field | Null | Notes |
|---|---:|---|
| `observation_time` | no | Timestamp returned for observation/bucket |
| `observation_interval` | no | E.g. 5m |
| `oi_source_value` | no | Exact source numeric value |
| `oi_source_unit` | no | Base/quote/contracts; never inferred silently |
| `oi_semantic` | no | E.g. `sum_both_sides`, `single_side`, `current_total` |
| `oi_base` | conditional | Direct source or explicit conversion |
| `oi_quote_notional` | conditional | Direct source or point-in-time conversion |
| `conversion_price` | yes | Price used for conversion |
| `conversion_price_source` | yes | Dataset lineage |
| `conversion_price_knowledge_time` | yes | Must be point-in-time valid |
| `is_backfilled` | no | Historical bootstrap marker |

Нельзя создавать quote OI без documented conversion; factor-of-two ambiguity запрещается явным `oi_semantic`.

## 4.4 Funding

| Field | Null | Notes |
|---|---:|---|
| `funding_time` | no | Settlement timestamp from source |
| `funding_rate` | no | Decimal fraction, not percent |
| `funding_interval_minutes` | no | From current/historical metadata contract |
| `rate_kind` | no | `realized_settlement`, later `predicted`/`current` separately |
| `mark_price` | yes | If source associates a mark price |
| `period_start`, `period_end` | yes | Only when semantics are provable |

Predicted/current funding rate is not mixed with realized settlement history.

## 4.5 Liquidations

| Field | Null | Notes |
|---|---:|---|
| `native_event_id` | yes | Many public streams do not supply one |
| `position_side_liquidated` | conditional | `LONG`, `SHORT`, `UNKNOWN` |
| `source_side` | conditional | Exact exchange side field |
| `source_side_semantic` | no | Position side vs order side |
| `source_quantity` | no | Exact source size |
| `source_quantity_unit` | no | Base/quote/contracts |
| `quantity_base` | conditional | Explicit conversion only |
| `notional_quote` | conditional | Explicit point-in-time conversion only |
| `source_price` | conditional | Exact source price |
| `price_semantic` | no | `bankruptcy`, `average_fill`, `order_price`, etc. |
| `order_type`, `time_in_force`, `order_status` | yes | When supplied |
| `last_filled_quantity`, `accumulated_filled_quantity` | yes | Binance fields |
| `message_id` | no | Local immutable WS envelope ID |
| `message_type` | no | Snapshot/event batch semantics |
| `completeness_class` | no | `all_events_claimed`, `throttled_latest_snapshot`, `archive_unknown`, etc. |
| `dedup_fingerprint` | no | Deterministic fallback when native ID absent |
| `dedup_collision_risk` | no | Explicit flag |

Bybit `snapshot` is treated as message/batch type, not as replaceable current state. Binance latest-within-window snapshots are explicitly marked incomplete for event-count reconstruction.

## 4.6 Instrument metadata snapshot

Contains all fields from section 2 plus:

- source filters and limits as structured fields where stable;
- full source payload or immutable raw reference;
- leverage/risk metadata when supplied;
- funding interval and funding caps;
- listing/trading status;
- `metadata_observed_at`;
- `exchange_effective_at` nullable;
- payload/content hash;
- version tuple.

Snapshots append when payload hash changes and periodically according to configured cadence. Old metadata is never overwritten.
Point-in-time metadata joins use the knowledge-time validity interval defined in section 2. They never backdate a newly observed rule merely because it may have been effective earlier.

---

# 5. Timestamp and knowledge-time model

## 5.1 Timestamp meanings

| Timestamp | Meaning |
|---|---|
| `event_time` | Time of economic event or observation represented by row |
| `exchange_timestamp` | Timestamp emitted by exchange; may equal event time but is not assumed identical |
| `source_published_at` | Publication time only if source explicitly supplies it |
| `received_at` | Local wall-clock time when bytes were received realtime |
| `processed_at` | Local transformation time |
| `retrieved_at` | Historical REST/archive download time; audit only |
| `knowledge_time` | Earliest conservative time at which the model pipeline could use the record |

`knowledge_time_basis`:

```text
observed_realtime
documented_publication_time
conservative_inferred
retrieval_only_unknown
```

## 5.2 Eligibility rule

A row is eligible at decision time `T` only if:

```text
knowledge_time <= T
AND data_quality permits use
AND source_age <= max_staleness
```

## 5.3 Dataset policies

- Realtime event: normally `knowledge_time = received_at`, adjusted conservatively for clock uncertainty.
- Closed candle: not earlier than close boundary and actual publication/receive; an open candle is ineligible for closed-candle features.
- Locally resampled candle: maximum constituent `knowledge_time` plus computation completion delay.
- Historical REST/archive: `retrieved_at` is not historical knowledge time. Use documented publication schedule, conservative lag policy with sensitivity test, or mark `retrieval_only_unknown` and prohibit latency-sensitive use.
- Funding: realized rate eligible only after settlement/publication, not merely because historical row has funding timestamp.
- OI: bucket timestamp does not itself prove availability; latency-sensitive use requires observed realtime history or documented conservative delay.
- Metadata: new rules become known at snapshot knowledge time unless an earlier official effective timestamp is explicitly available.

Clock quality is collected via exchange server-time samples. Latency reports without clock offset/uncertainty are invalid.

Any conservative publication-lag policy is pre-registered in the versioned Data Contract, includes an approved sensitivity range, and is hashed into the normalization build. Changing lag/admissibility creates a new normalization version and dataset generation; lag may not be tuned after observing test PnL.

---

# 6. PHASE 1 Data Contracts

Every contract row in the future registry must include:

```text
source
official_documentation_url
endpoint_or_topic
source_field
semantic_meaning
source_unit
timestamp_meaning
nullable
canonical_field
transformation
normalized_unit
validation_rules
known_limitations
contract_version
verified_at
```

Ни один adapter не реализуется, пока все его source fields не внесены в registry. Полный field-level registry является нормативным companion-файлом `crypto_quant_phase1_data_contracts.md`. Таблицы ниже — краткий source overview, а не замена реестра.

## 6.1 Binance Spot

Official sections: [Spot market REST](https://developers.binance.com/en/docs/catalog/core-trading-spot-trading/api/rest-api/market), [Spot WebSocket streams](https://github.com/binance/binance-spot-api-docs/blob/master/web-socket-streams.md), [official bulk archive](https://github.com/binance/binance-public-data).

| Dataset | Source fields | Canonical mapping / semantics | Limitation |
|---|---|---|---|
| Metadata | `/api/v3/exchangeInfo`: symbol, status, baseAsset, quoteAsset, filters | Instrument identity + metadata snapshot; tick/step parsed from filter types | Filters are mutable; snapshot required |
| OHLCV | `/api/v3/klines`: open time, O/H/L/C, volume, close time, quote volume, trade count, taker-buy volumes | Base/quote volumes kept separately; final candles only | Bulk spot timestamps from 2025-01-01 may be microseconds; explicit unit detection required |
| Aggregate trade | `/api/v3/aggTrades`: `a,p,q,f,l,T,m,M` | `m=true` means buyer is maker; taker/aggressor is SELL after fixture gate | Aggregates trades from same taker order/price/time; not raw |
| Raw trade archive | trade id, price, qty, quoteQty, time, isBuyerMaker, isBestMatch | Separate `binance_spot_raw_trade` dataset | Archive/API coverage and timestamp precision checked per object |

## 6.2 Binance USDⓈ-M perpetual

Official section: [USDⓈ-M market-data REST](https://developers.binance.com/en/docs/catalog/core-trading-derivatives-trading-usd-s-m-futures/api/rest-api/market-data), [liquidation stream](https://developers.binance.com/en/docs/products/derivatives-trading-usds-futures/websocket-market-streams/Liquidation-Order-Streams), [official bulk archive](https://github.com/binance/binance-public-data).

| Dataset | Endpoint / fields | Canonical mapping / semantics | Limitation |
|---|---|---|---|
| Metadata | `/fapi/v1/exchangeInfo` | contract/status/assets/filters metadata snapshot | Contract-size representation must be verified from actual response/contract rules |
| OHLCV | `/fapi/v1/klines` | Base volume, quote volume, trade count and taker volumes retained explicitly | REST limits/weights are limit-dependent |
| Aggregate trade | `/fapi/v1/aggTrades`: aggregate IDs, price, quantity, time, buyer-maker flag; current docs also expose normal quantity excluding RPI | Separate aggregate dataset and RPI-aware contract | REST agg history is limited; official bulk preferred for bootstrap |
| Raw trades | official futures `trades` bulk objects; REST raw endpoint where applicable | Separate raw dataset | Free depth/coverage verified object-by-object; no substitution with aggTrades |
| Funding | `/fapi/v1/fundingRate`: symbol, fundingRate, fundingTime, markPrice, rateType | Realized funding settlement; rate type retained | Shared endpoint-specific rate-limit bucket |
| OI current | `/fapi/v1/openInterest` | `current_total`, source unit retained | Current snapshot only |
| OI history | `/futures/data/openInterestHist`: sumOpenInterest, sumOpenInterestValue, timestamp | Keep source base-like value and source quote value separately | Current official docs: latest one month only; continuous local accumulation required |
| Liquidation | `<symbol>@forceOrder`: event time + force-order payload | Side/order/qty/price fields preserved; completeness `throttled_latest_snapshot` | Only latest liquidation order per symbol within 1000ms snapshot window; unsuitable for complete event-count backtest |

## 6.3 Bybit Spot

Official sections: [instruments info](https://bybit-exchange.github.io/docs/v5/market/instrument), [kline](https://bybit-exchange.github.io/docs/v5/market/kline), [recent trades](https://bybit-exchange.github.io/docs/v5/market/recent-trade), [public trade WS](https://bybit-exchange.github.io/docs/v5/websocket/public/trade), [historical portal](https://www.bybit.com/en/derivative-activity/history-data).

| Dataset | Source fields | Canonical mapping / semantics | Limitation |
|---|---|---|---|
| Metadata | category spot: symbol, baseCoin, quoteCoin, status, lotSizeFilter, priceFilter | Spot identity + mutable rules snapshot | Spot response differs structurally from linear |
| OHLCV | `/v5/market/kline`: list indices 0..6 | Reverse-sorted response normalized ascending; incomplete current candle rejected | Spot volume/turnover units must be captured and fixture-verified rather than inferred from linear docs |
| Raw trade | `execId, price, size, side, time, isBlockTrade, isRPITrade, seq` | `side` is taker side; raw dataset | REST gives recent trades only; spot limit 60; archive coverage verified separately |

## 6.4 Bybit Linear USDT perpetual

| Dataset | Endpoint / fields | Canonical mapping / semantics | Limitation |
|---|---|---|---|
| Metadata | `/v5/market/instruments-info`: contractType/status/baseCoin/quoteCoin/settleCoin/tickSize/qtyStep/fundingInterval/leverage | Linear perpetual identity + mutable rules | Pagination required for full linear universe; certain limits are adjusted periodically |
| OHLCV | `/v5/market/kline`: volume and turnover | For USDT/USDC contracts docs define volume in base and turnover in quote | Reverse order; current candle may be incomplete |
| Raw trade | recent REST / `publicTrade.<symbol>`: taker side, size, price, trade ID, sequence | Direct taker-side classification; size unit still validated against instrument contract | Archive schema/coverage may differ from realtime payload |
| Funding | `/v5/market/funding/history`: fundingRate, fundingRateTimestamp | Realized rate; interval from instruments-info | Limit 200; per-symbol interval |
| OI | `/v5/market/open-interest`: openInterest, singleOpenInterest, timestamp | Linear BTCUSDT unit is base coin; semantics stored separately | 5m minimum interval; possible latency during extreme volatility |
| Liquidation | `allLiquidation.<symbol>`: `T,s,S,v,p` | `S` is liquidated position side; `Buy` means long liquidated; `p` is bankruptcy price | 500ms push; `snapshot` is a message batch label, not replaceable state |

---

# 7. Raw vs normalized data model

## Raw landing

- byte-exact archive object plus source checksum where supplied;
- REST request parameters, URL, headers relevant to limits/time, response body and timestamps;
- WS envelope with connection/session ID, topic, receive timestamp and sequence information;
- immutable unique filenames/content hashes;
- no unit conversion and no semantic rewriting;
- invalid objects go to quarantine, not silent deletion.

## Normalized datasets

- typed canonical fields;
- explicit identity and units;
- common knowledge-time envelope;
- source raw reference and transformation contract;
- deterministic build ID from input hashes, code/config hash and version tuple;
- new immutable generation for rebuild/compaction.

```text
raw object ──hash/reference──► normalization build ──manifest──► parquet generation
```

---

# 8. Partitioning strategy

Path identity includes source semantics:

```text
data/{raw|normalized}/
  dataset={raw_trade|aggregate_trade|ohlcv|oi|funding|liquidation|instrument_metadata}/
  exchange={binance|bybit}/
  market_type={spot|perpetual}/
  contract_type={spot|linear_perpetual}/
  symbol={BTCUSDT|ETHUSDT}/
  schema_major=v1/
  date=YYYY-MM-DD/
```

Baseline:

| Dataset | Partition | Split condition |
|---|---|---|
| raw/aggregate trades | daily | Switch to hour if compressed daily partition repeatedly exceeds target |
| liquidations | daily | Hour only after measured burst/file-size need |
| OHLCV 1m | monthly or daily | Daily preferred initially for uniform recovery; compact later |
| OI | monthly | Daily if operational recovery benefits outweigh small files |
| funding | yearly/monthly | Monthly preferred |
| metadata snapshots | monthly | Low volume |

Engineering target is approximately 128–512 MB per Parquet file, but this is a benchmark hypothesis, not an exchange fact. Partition changes require a new storage-layout version and measured query/compaction report.

Compaction writes a new immutable generation and marks parents `SUPERSEDED`; readers resolve active generations through manifest/catalog views.

---

# 9. Retention and immutable manifest design

## 9.1 Manifest event log

Append-only event types:

```text
DISCOVERED
DOWNLOADED
INGESTED
VALIDATED
QUARANTINED
NORMALIZED
COMPACTED
SUPERSEDED
DELETION_PLANNED
DELETED
DELETE_FAILED
```

Each event stores:

- object/partition ID and path;
- source/archive/endpoint/topic;
- exchange and full instrument identity;
- retrieval time;
- coverage start/end;
- min/max event and knowledge time;
- rows and bytes;
- checksum algorithm/value and content hash;
- input/parent hashes;
- schema/collector/normalization versions;
- code git SHA, config hash and dependency-lock hash;
- known limitations;
- reason and timestamp for lifecycle action;
- run ID and actor/process identity.

Deletion is successful only after post-delete existence verification and a `DELETED` ledger event. Tombstone alone is insufficient.

## 9.2 Retention policies

- OHLCV, funding, OI, normalized liquidations and instrument metadata: permanent.
- Source-faithful archive objects and REST/WS envelopes: initial 30 days after successful normalization, except incident/research holds. Externally reproducible bulk objects may move to checksum-only Tier C after deletion.
- Canonical normalized `raw_trade` datasets: initial 30 days, configurable to 90 after measured disk report.
- Permanent trade aggregates: 1s/5s/1m/5m only after signed-volume contract gate and explicit aggregate-retention approval.
- Prediction journal: permanent, append-only, separate legal/retention policy; never compacted semantically, deleted or rewritten.

## 9.3 Reproducibility tiers

- Tier A: raw object retained — byte-level reprocessing reproducible.
- Tier B: raw deleted by policy, normalized generation + input checksums/manifests retained — lineage/audit reproducible, but byte-level rebuild is not guaranteed.
- Tier C: external archive still addressable with verified checksum — rebuild conditionally reproducible.

Reports must state the tier; retention must not falsely claim full reproducibility after raw deletion.

---

# 10. Historical bootstrap and provenance

```text
source discovery/inventory snapshot
  → download to unique staging object
  → checksum/content hash
  → field-contract validation
  → quarantine or atomic raw publish
  → raw manifest commit
  → normalization build
  → normalized manifest commit
  → coverage reconciliation
  → REST tail catch-up
  → WS handoff with overlap
  → deterministic dedup/reconcile
  → gap registry update
```

Requirements:

- preserve archive inventory/listing snapshot because URLs/objects may disappear;
- idempotency key combines source object hash and contract/version tuple;
- bootstrap/WS handoff has durable watermark and overlap window;
- overlap is deduplicated by source-specific ID/fingerprint;
- no history is called complete until coverage reconciliation passes;
- archive limitations and missing days are stored in manifest, not prose only.

## Liquidation archive audit result as of 2026-08-10

- A dated, non-exhaustive observation queried Binance public S3 for two candidate USDⓈ-M BTCUSDT prefixes; both listings were empty. It did **not** cover ETHUSDT, every possible official prefix, or preserve raw XML, so it is not proof of absence. A control observation suggested old COIN-M objects, but that control is also pending immutable raw capture.
- An interactive inspection of Bybit's advertised historical portal showed trade/order-book/price products and no liquidation product. This was a catalog observation, not an exhaustive archive-location audit and not independently reproducible evidence yet.
- Therefore historical liquidation availability for Binance USDⓈ-M BTC/ETH and Bybit Linear BTC/ETH remains **ASSUMPTION / REQUIRES VERIFICATION**. PHASE 1D.3 begins with an exhaustive, immutable inventory capture (URLs, raw responses/DOM or screenshots, timestamps and hashes), then records coverage, granularity, semantics and completeness before any bootstrap design.
- Controlled realtime collection remains mandatory. Binance snapshots are not treated as complete event history; Bybit stream completeness claims are retained together with source/version and observed gaps.

---

# 11. Gap and data-quality model

## 11.1 Gap registry

| Field | Meaning |
|---|---|
| `gap_id` | Stable ID |
| dataset/instrument/source | Exact affected source semantics |
| `gap_start`, `gap_end` | UTC bounds |
| `detected_at` | Detection time |
| `detection_method` | Candle cadence, ID/sequence discontinuity, connection outage, cross-source evidence |
| `status` | suspected, confirmed, backfilled, unfillable, exchange_outage, local_outage |
| `expected_count`, `observed_count` | If cadence/count is meaningful |
| `cause`, `evidence` | Known reason and evidence references |
| `volatility_context` | Versioned return/volatility/stress context |
| `affected_features` | Eligibility impact |
| `resolution` | Backfill/reject/accept-known-limitation |

Event-driven trades/liquidations do not infer a gap merely from an empty time interval. Evidence requires connection loss, sequence/ID discontinuity or corroborating source information.

## 11.2 DQ layers

1. Schema/type/null/domain.
2. Uniqueness, order and ID continuity.
3. Temporal interval and timestamp sanity.
4. Coverage, freshness and latency.
5. Unit/economic invariants.
6. Cross-source plausibility without declaring one exchange ground truth.
7. Missingness bias analysis: compare returns, volatility and liquidation context during missing vs observed periods.

Flags do not automatically delete rows. Severe DQ makes record/feature unavailable and can disable signals.

---

# 12. Cross-exchange as-of join design

For decision row at `decision_time` select the latest right-side observation satisfying:

```text
right.knowledge_time <= decision_time
AND right.knowledge_time is maximal among eligible records
AND decision_time - right.knowledge_time <= configured max_staleness
```

Join output carries:

- `right_event_time`;
- `right_knowledge_time`;
- `source_age_ms = decision_time - right_event_time`;
- `knowledge_age_ms = decision_time - right_knowledge_time`;
- `staleness_ms`;
- `match_quality`: `exact_available`, `asof_fresh`, `stale`, `missing`;
- right-side DQ flags.

If tolerance exceeded, canonical feature becomes `NULL/unavailable`; silent forward-fill prohibited. `max_staleness` is per feature/source/cadence, not one global constant.

Join only by `event_time` is forbidden because a record may have arrived after decision time.

---

# 13. Recovery design for local Windows laptop

## Durability

- one writer lease per dataset partition;
- immutable unique final filenames — no in-place Parquet replacement;
- write `.partial` on the same filesystem/volume;
- flush, close, checksum and validate before atomic rename;
- manifest commit only after durable final object exists;
- checkpoint advances only after batch commit;
- startup reconciliation scans orphan partials, missing files and manifest/file divergence.

## Runtime recovery

- bounded queues with disk spool fallback;
- exponential backoff with jitter and rate-limit-aware delays;
- heartbeat/stale detection;
- WS session ID plus last durable source ID/sequence/event time;
- reconnect → REST/archive overlap backfill → deterministic reconcile → gap registration if unfillable;
- sleep/resume detection via monotonic-vs-wall-clock jump;
- graceful shutdown drains to deadline, writes durable spool/checkpoint, then exits;
- crash injection tests for partial file, rename, pre/post-manifest and stale lock;
- Windows file-lock/antivirus conflicts use bounded retry and immutable generation filenames.

Atomic rename assumptions apply only within one volume and must be verified by crash tests on the actual filesystem. Operational autostart via Windows Task Scheduler is deferred until PHASE 8, but restart-safe process state is designed now.

---

# 14. Schema and dataset versioning

Version tuple:

```text
schema_version
data_contract_version
collector_version + git_sha
normalization_version
storage_layout_version
feature_version (later)
```

Rules:

- Schema major: incompatible physical/logical field change.
- Schema minor: additive nullable field.
- Schema patch: validation/doc correction without changed data meaning.
- Normalization major: changed units, signed-volume semantics, OI conversion, aggregation or source substitution even if columns do not change.
- Collector version: changed extraction/pagination/parser semantics.
- Dataset generation ID: input content hashes + contract/version tuple + code/config/dependency hashes.
- Readers fail loudly when incompatible majors are mixed.
- Compatibility matrix is explicit; no implicit union across versions.

Changing raw ↔ aggregate source always creates a different `source_dataset_id`; it is not a patch version.

---

# 15. Resource and data-growth estimate

Actual laptop baseline measured 2026-08-10:

- Intel Core i7-11800H, 8 cores / 16 threads;
- 15.7 GB RAM;
- SSD 476.3 GB, approximately 321.2 GB free;
- GPU not required.

## Runtime targets

| Workload | CPU | RAM target |
|---|---:|---:|
| REST/archive download | 1–2 cores, I/O-bound | 0.2–0.8 GB |
| WS collection | usually <1 core average | 0.3–1.0 GB |
| normalization/Parquet | 2–4 core bursts | 1–3 GB |
| DuckDB DQ scan | 2–6 core bursts | limited to 2–4 GB |
| whole PHASE 1 steady state | 1–3 cores typical | target 2–4 GB, hard guard below available RAM |

Batch processing, column pruning, bounded queues and DuckDB memory limits are mandatory. Raw history is never loaded completely into RAM.

## Data estimate

Assumptions: BTC/ETH, Binance+Bybit, spot+linear perpetual for OHLCV/trades, perpetual for derivatives, 1m candles, ZSTD Parquet, raw trades 30-day rolling retention, permanent aggregates, no raw L2.

| Horizon | With 30-day raw-trade retention | With indefinite raw-trade retention |
|---|---:|---:|
| 1 month | 2–7 GB typical; 7–25 GB stress | same |
| 3 months | 3–12 GB typical; 12–35 GB stress | 6–20 GB typical; up to ~60 GB stress |
| 1 year | 8–30 GB typical; 30–80 GB stress | 20–80 GB typical; 100–250+ GB stress |

These are planning ranges, not verified exchange facts. A one-day official archive sample is insufficient for tail sizing. After 24–72 hours the system must report measured rows/day, bytes/row, compression ratio, peak RSS, file sizes and 30/90/365-day projections. Reserve 50–100 GB for PHASE 1 including staging, compaction headroom and quarantine.

---

# 16. Revised PHASE 0–1 task breakdown

## PHASE 0 — architecture and foundation

1. Approve ADRs: identity, timestamp/knowledge-time, raw/normalized, versions, immutability/retention, recovery.
2. Create lightweight Python environment and locked dependencies.
3. Implement typed config, path policy, UTC structured logging and secret redaction.
4. Define schema registry and field-level Data Contract format.
5. Define manifest event, checkpoint, gap and deletion-ledger schemas.
6. Add deterministic IDs/hashes and Decimal/timestamp utilities.
7. Add fixture/test harness and CLI smoke/config-check.
8. Document architecture, data dictionary and recovery runbook.

## PHASE 1 — strict sequence

1. Re-run official source inventory and freeze versioned Data Contracts/fixtures.
2. Implement instrument metadata snapshots first: Binance Spot/USDⓈ-M, Bybit Spot/Linear.
3. Implement raw landing, atomic publish, manifests, quarantine and checkpoints.
4. PHASE 1A: Binance Spot OHLCV pilot → tests/gate; then Binance perpetual contract.
5. PHASE 1B: Bybit Spot/Linear OHLCV using same canonical schema → parity gate.
6. PHASE 1C: raw trades and aggregate trades as separate tasks/datasets; aggressor fixture gate; historical then realtime.
7. PHASE 1D.1: funding, one venue at a time.
8. PHASE 1D.2: OI bootstrap + continuous accumulation, one venue at a time.
9. PHASE 1D.3: rerun liquidation archive audit, then realtime collection with explicit completeness classes.
10. Add reconnect/checkpoint/overlap-backfill/recovery tests.
11. Build normalized immutable generations with lineage.
12. Add DuckDB catalog/views.
13. Add DQ checks, gap registry and point-in-time/as-of primitives.
14. Run a baseline pilot, propose DQ alert/hard-fail thresholds and per-source staleness limits, approve and freeze them in versioned config.
15. Run 24–72h soak, calibrate storage/resources and produce Data Quality Report.
16. STOP POINT №1; no ML.

---

# 17. Definition of Done

## PHASE 0

- Architectural ADRs and this design approved.
- Clean checkout installs reproducibly from lock file.
- Package imports; config and logging work in UTC.
- Secrets/data/partial files are gitignored and redacted.
- Deterministic identity/version/hash utilities have tests.
- Schema, Data Contract, manifest, checkpoint, gap and deletion schemas are defined and validated.
- Knowledge-time eligibility tests reject late-known records.
- Version compatibility tests reject incompatible dataset majors.
- `pytest` and Ruff quality gates pass from a clean environment.
- No collectors, ML or trading dependencies are introduced prematurely.
- README/architecture/data-contract/runbook commands are reproducible.

## PHASE 1

- Scope remains BTC/ETH, Binance/Bybit, explicit spot/linear-perpetual identities.
- Every source field has a versioned field-level Data Contract and sanitized fixture tied to an official documentation URL/date.
- Instrument metadata snapshots precede dependent market normalization.
- Raw → normalized lineage is queryable to object/content hash and code/config versions.
- Bootstrap and realtime handoff are idempotent and reconciled with overlap.
- Closed-candle, units, timestamps, source semantics and completeness validations pass.
- Raw/aggregate datasets cannot be mixed by readers.
- Maker/taker fixtures pass before any signed-volume feature is declared valid; CVD remains outside PHASE 1.
- OI bootstrap window is captured, then continuous accumulation, checkpoints and gaps are tested.
- Liquidation archive decisions are backed by exhaustive immutable official-inventory evidence for BTC and ETH and every documented location; realtime limitations are explicit.
- Restart, sleep/resume, network loss, timeout/429, partial write, stale lock and crash-recovery tests pass.
- All gaps are registered, resolved or explicitly unfillable; no silent fill/drop.
- As-of joins use knowledge time and reject stale/future-arrived observations.
- Retention dry-run and deletion ledger tests pass; prediction journal policy is isolated/permanent.
- DuckDB views query active immutable generations without data duplication.
- 24–72h soak report includes coverage, freshness, gaps, duplicates, latency/clock quality, quarantine, sizes, daily growth, RSS and stress-context missingness.
- Data Quality Report produced; STOP POINT reached; ML not started.

## Future ML validation policy fixed now

For 1h/4h/12h/24h labels, train/validation/test boundaries purge every sample whose outcome window overlaps the next fold, then apply an embargo at least as long as the maximum active label horizon unless a stricter pre-registered rule is justified. Walk-forward folds preserve the complete point-in-time pipeline.

Every experiment pre-registers a hypothesis family covering tested features, strategies, thresholds and hyperparameters. Selection occurs on train/validation only; the fixed test set is opened once. Reports disclose the number of trials/families, selection procedure and uncertainty. Later phases must use appropriate multiple-testing controls and deflated/probabilistic performance metrics instead of treating the best raw Sharpe/PnL as unbiased. Changing the family or selection rule spends the current test set and creates a new experiment version.

---

# 18. VERIFIED VS ASSUMED

## VERIFIED FROM CURRENT OFFICIAL DOCUMENTATION

Verified on 2026-08-10:

1. Binance Spot `/api/v3/klines` is weight 2 and max limit 1000; `/api/v3/aggTrades` is weight 4 and max limit 1000. Source: [Spot market REST](https://developers.binance.com/en/docs/catalog/core-trading-spot-trading/api/rest-api/market).
2. Binance public archive provides daily/monthly spot and futures klines/trades/aggTrades with checksum sidecars; spot timestamps from 2025-01-01 are microseconds. Source: [Binance public data](https://github.com/binance/binance-public-data).
3. Binance USDⓈ-M `/fapi/v1/klines` has limit-dependent weights and max limit 1500; `/fapi/v1/aggTrades` has current REST history not older than 24 hours and an endpoint weight documented as 20. Source: [USDⓈ-M market data](https://developers.binance.com/en/docs/catalog/core-trading-derivatives-trading-usd-s-m-futures/api/rest-api/market-data).
4. Binance funding history is `/fapi/v1/fundingRate`, with explicit funding time/rate/mark price fields and a shared 500/5min/IP bucket. Same official USDⓈ-M source.
5. Binance OI current and historical statistics are separate endpoints; current official OI statistics documentation states only the latest one month is available. Same official USDⓈ-M source.
6. Binance liquidation stream `<symbol>@forceOrder` is a latest-order snapshot within 1000ms, not a complete event feed. Source: [official liquidation stream section](https://developers.binance.com/en/docs/products/derivatives-trading-usds-futures/websocket-market-streams/Liquidation-Order-Streams) and current official connector references.
7. Bybit `/v5/market/kline` covers spot/linear/inverse, max 1000, returns reverse-sorted candles, and defines linear volume in base/turnover in quote versus inverse converse. Source: [Bybit kline](https://bybit-exchange.github.io/docs/v5/market/kline).
8. Bybit `/v5/market/recent-trade` defines `side` as taker side; spot max is 60 and other categories max 1000; docs point to an official historical trade portal. Source: [Bybit recent trades](https://bybit-exchange.github.io/docs/v5/market/recent-trade).
9. Bybit OI is derivatives-only, max 200 with cursor, intervals 5m–1d; `openInterest` is sum of both sides, linear BTCUSDT unit is BTC and inverse BTCUSD unit is USD. Source: [Bybit open interest](https://bybit-exchange.github.io/docs/v5/market/open-interest).
10. Bybit funding history max is 200, applies to perpetuals, and funding interval must be read from instruments-info. Source: [Bybit funding](https://bybit-exchange.github.io/docs/v5/market/history-fund-rate).
11. Bybit instruments-info exposes contract type/status/base/quote/settle/tick/qty/funding interval/leverage fields; linear universe requires pagination and some order-size limits change periodically. Source: [Bybit instruments](https://bybit-exchange.github.io/docs/v5/market/instrument).
12. Bybit `allLiquidation.<symbol>` claims all liquidations, pushes every 500ms, uses `S` as liquidated position side (`Buy` means long liquidated), `v` as size and `p` as bankruptcy price. Source: [Bybit all liquidation](https://bybit-exchange.github.io/docs/v5/websocket/public/all-liquidation).
13. Bybit default HTTP IP ceiling is 600 requests/5 seconds; WS connection limits and response limit headers are documented. Source: [Bybit rate limits](https://bybit-exchange.github.io/docs/v5/rate-limit).

## ARCHITECTURAL DECISION

- Stable instrument surrogate/hash and natural key.
- UTC timestamp[ns] plus source timestamp preservation.
- Decimal canonical numerics.
- Separate source datasets per market and raw/aggregate semantics.
- Immutable content-addressed objects and generations.
- Daily trade partitions with measured hour split and file-size target hypothesis.
- Append-only manifest/deletion/gap registries.
- Conservative knowledge-time admissibility and per-feature staleness.
- Bootstrap/WS overlap handoff.
- Raw retention 30 days initially.
- Reproducibility tiers.
- Lightweight Python/HTTP/WS/Parquet/DuckDB stack.
- 24–72h soak before final resource commitments.

## ASSUMPTION / REQUIRES VERIFICATION

1. Exact historical start date, missing-day rate, checksum consistency and terms for every Binance/Bybit archive object used.
2. Whether a newly introduced or separately hosted official liquidation archive appears before PHASE 1D.3; rerun inventory immediately before implementation.
3. Complete field schema and units of each Bybit historical trade archive file, especially spot versus linear, compared with realtime payloads.
4. Exact source quantity and contract-size representation for all four in-scope instrument identities from live metadata fixtures.
5. Historical availability/publication delay for OI, candles and funding. Event timestamps alone do not establish it.
6. Real network latency, rate-limit behaviour by region and endpoint accessibility from the user's location.
7. Real rows/day, compressed bytes/row and stress-day multipliers; current storage ranges require pilot calibration.
8. Safe Windows flush/rename/checkpoint behaviour of the selected libraries and filesystem; crash tests required.
9. Final DQ hard-fail thresholds, alert thresholds and per-feature `max_staleness`; these require observed baseline distributions.
10. Final historical backfill horizon desired for OHLCV/trades/funding; not yet specified.
11. Whether permanent 1-second trade aggregates are necessary for all eight streams or only selected market/source datasets after research hypothesis review.
12. The dated Binance bucket observation (zero keys for two BTCUSDT candidate prefixes) and the Bybit portal observation (no visible liquidation product) must be repeated exhaustively for BTC/ETH and every documented location, with immutable raw XML/DOM or screenshots, full URLs, timestamps and hashes. Until then they are leads, not archive-absence findings.

---

# 19. Approval gate

Architecture is approved: PHASE 0 complete; PHASE 1A complete; PHASE 1B complete. The next implementation task is PHASE 1C, starting with separate raw-trade/aggregate-trade source contracts and the aggressor-semantics fixture gate. No Polymarket, Risk/Exit Engine, UI or Telegram implementation is authorized by the global reservations below.

---

# 20. Approved global reservations after PHASE 0

These additions extend existing mechanisms and do not reopen PHASE 0:

- `localization.default_locale=ru-RU`, `fallback_locale=en-US`; internal technical identifiers stay English;
- YAML locale catalogs and a single tested i18n boundary serve future user-facing strings;
- the Russian Knowledge Base owns glossary and `feature_id -> documentation_id -> localization_key` mappings;
- `invalidation`, `stop_loss` and `take_profit` are independent concepts;
- draft `future_signal` interface reserves three target scenarios, target probability/time, risk/reward, expected value, position-risk fields, partial/break-even/trailing methods, hard gates and separate confidence/risk/opportunity values;
- Polymarket is a separate optional `prediction_*` dataset family reserved for PHASE 3F, isolated from exchange `InstrumentIdentity` and from PHASE 1 availability.

The interface remains `0.2.0-draft`; it is not a promise that target probabilities, risk scores or stops already exist. A non-runtime Trade Lifecycle reservation adds immutable signal revisions, time context, review triggers, state-machine states and dynamic-management audit fields. Numeric methods and thresholds require later versioned backtest/paper evidence. PHASE 1 scope, dependencies, data root, logging, control-plane contracts and recovery design are unchanged.
