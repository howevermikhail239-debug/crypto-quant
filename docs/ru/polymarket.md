# Polymarket и внешние события

**Status:** future / PHASE 3F · **Owner:** external-event owner

Polymarket — отдельный optional external-event source для research features. Он не входит в PHASE 1, не блокирует baseline, не участвует в execution и не смешивается с exchange market-data entities.

В PHASE 3F заново проверяются официальные Gamma/Data/CLOB/WebSocket/history источники. Концептуальные entities: `prediction_event`, `prediction_market`, `prediction_market_snapshot`, `prediction_trade`, `prediction_orderbook_snapshot`, `prediction_resolution`. Taxonomy: macro, political, geopolitical, crypto, narrative и sports-reserved.

Каждый event требует explicit asset mapping и economic hypothesis. Quality filter учитывает liquidity, volume, spread, activity, resolution clarity, status и remaining time; плохое качество даёт `POLYMARKET_FEATURE_INVALID`. Resolution недоступен до его knowledge time. Outage делает features unavailable без forward-fill, но Binance/Bybit и Core baseline продолжают работу.

Добавление к модели возможно только после lead-lag исследования в обе стороны и одинакового out-of-sample ablation `M0` против `M1 + Polymarket`. Polymarket не считается ground truth или oracle.
