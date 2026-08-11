# PHASE 1D.3B — Bybit Linear ETHUSDT Liquidation Parity

**Status:** FINAL DONE / ACCEPTED
**Verified:** 2026-08-11  
**Scope:** existing Bybit `allLiquidation.{symbol}` adapter, ETHUSDT parity only

## Source evidence

- [Bybit All Liquidation](https://bybit-exchange.github.io/docs/v5/websocket/public/all-liquidation): `allLiquidation.{symbol}`, 500 ms push, `T` updated timestamp, `s` symbol, `S` liquidated position side (`Buy` means long liquidated), `v` executed size, and `p` bankruptcy price.
- [Bybit Instruments Info](https://bybit-exchange.github.io/docs/v5/market/instrument): current ETHUSDT response observed `contractType=LinearPerpetual`, `baseCoin=ETH`, `quoteCoin=USDT`, `settleCoin=USDT`, and `status=Trading`.
- [Bybit USDT Perpetual Contract specification](https://www.bybit.com/en/help-center/article/?id=000001060&language=en_US): USDT perpetual contracts are settled in USDT and quoted in the base currency, including ETH.

The source-shaped metadata snapshot is stored outside Git at:

```text
C:\crypto_quant_data\control\instrument_metadata\bybit_linear_instruments_info_ETHUSDT_20260811T114448Z.json
SHA-256: 46ec2875a352bd834f8c8c9b4503969b2e1cfcf2dce7203c7964b69f77db0829
```

Evidence classification:

- `baseCoin=ETH`, `quoteCoin=USDT`, `settleCoin=USDT`, `LinearPerpetual`: VERIFIED by current official API response and documentation.
- `v` source meaning `executed size`: VERIFIED by the all-liquidation documentation.
- `v` economic unit for in-scope ETHUSDT linear perpetual: VERIFIED as base ETH through the official USDT-linear product convention and instrument identity.
- Local capture completeness: UNKNOWN/PARTIAL outside observed connection windows; the venue's `ALL_LIQUIDATIONS` statement remains a source claim.

## Change and invariants

A shared defect was proven before the ETH expansion: the Bybit liquidation module imported Binance's `funding_identity`, producing a Binance-derived `instrument_id` inside otherwise Bybit records. The adapter now reuses the existing Bybit Linear identity constructor. No data migration was required because active genuine BTC liquidation rows were zero and the earlier synthetic batch remains quarantined with its audit trail.

No separate ETH collector or ETH-specific source contract was created. The existing contract IDs, dataset ID, schema version, timestamps, side mapping, exact-wire replay boundary, immutable raw/normalized layout, manifest format, and checkpoint naming remain unchanged.

Fixture tests prove:

- ETH `Buy -> LONG` and `Sell -> SHORT`;
- exact Decimal source strings, `T`, `ts`, and `knowledge_time=received_at`;
- identical-content observations inside one batch retain multiplicity;
- exact raw-envelope replay deduplicates;
- BTC and ETH have distinct Bybit `instrument_id` values, raw paths, normalized partitions, manifest lineage, and checkpoints in the same temporary root;
- replaying ETH does not mutate the BTC checkpoint;
- wrong topic or payload symbol fails before raw, normalized, manifest, or checkpoint writes.

## Bounded live pilot

The production public linear WebSocket was observed for 35.31 seconds on `allLiquidation.ETHUSDT`:

```text
transport_status = PASS
subscription_ack = PASS
event_observation_status = NO_EVENT_OBSERVED_WITHIN_WINDOW
total_messages_received = 0
total_records_persisted = 0
flush_count = 0
```

A separate read-only keepalive probe received a matching WebSocket `PONG` after `PING`. No empty market-data artifacts were written. A genuine ETH liquidation event was not observed; per phase contract this is not a blocker and genuine-event lineage remains a PHASE 1D.3F soak responsibility.

## Limitations

- No historical liquidation bootstrap source was introduced.
- Cross-envelope economic-event dedup remains unguaranteed because the source provides no native event ID.
- This slice does not establish long-window delivery completeness, disconnect-gap behavior, or reconciliation; those remain PHASE 1D.3F.
- No liquidation features, signals, strategy rules, Binance collector, or execution logic were added.

## Independent acceptance audit

The independent acceptance audit on 2026-08-11 confirmed the original identity defect as fixed and added narrow regression evidence without changing source, normalization, deduplication, or storage semantics:

- the canonical Binance/Bybit x BTCUSDT/ETHUSDT perpetual identity matrix contains four distinct instrument IDs with the expected exchange, symbol, market, contract, base, quote, settle, quantity, and notional dimensions;
- a Bybit BTCUSDT Spot identity is distinct from the corresponding perpetual identity;
- mixed-symbol multi-event envelopes fail before any raw, normalized, manifest, or checkpoint write;
- persisting ETH in the same root does not change the previously persisted BTC Parquet bytes or hash;
- normalized Parquet and manifest instrument IDs match the requested canonical identity;
- manifests distinguish raw WebSocket envelope count (`raw_message_count`) from normalized economic-event count (`event_count`), while retaining `row_count` as the compatibility alias.

Validation at acceptance: focused liquidation tests `19 passed`; full suite `215 passed`; Ruff, config-check, health, dependency lock check, and Git whitespace validation passed.
