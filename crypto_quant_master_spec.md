# MASTER SPECIFICATION
## Глобальные дополнения — обязательные требования

Следующие требования нормативны, совместимы с завершённым PHASE 0 и имеют приоритет над более ранними неоднозначными формулировками:

- Default user locale is ru-RU; en-US is fallback. Technical identifiers, schemas and fields remain English. User-facing text uses localization keys, never literals embedded in collector, ML, risk or trading logic.
- Maintain a Russian Knowledge Base. Every model-eligible feature maps feature_id -> documentation_id -> localization_key and records point-in-time/DQ limitations, owner and review metadata.
- Future Simple/Expert views are interface requirements only; no UI is authorized now.
- invalidation (hypothesis failure) and stop_loss (position risk control) are separate; equality is never assumed. NO_TRADE can have invalidation but no position stop.
- Stop methods, partial exits, break-even and trailing logic are versioned research hypotheses. Hard DQ/security/liquidity/slippage/OOD gates can force NO_TRADE; model confidence and trade risk are separate.
- The DRAFT signal schema reserves separate invalidation, stop-loss, three target, risk, partial-exit, break-even and trailing fields: schemas/future_signal.schema.json.
- PHASE 3F reserves optional non-blocking Polymarket/prediction-market external-event features after PHASE 3E, with separate entities/contracts/provenance/knowledge-time/DQ and no execution semantics.

Полная методология дополнений зафиксирована в разделе «Глобальная архитектурная поправка: localization, risk/exit и prediction markets» ниже.

## Local Crypto Quant & Opportunity System
### Версия 1.0

---

## 0. Роль AI-разработчика

Ты выступаешь одновременно как:

- senior Python developer;
- quantitative developer;
- data engineer;
- ML engineer;
- системный архитектор;
- специалист по тестированию алгоритмических торговых систем.

Твоя задача — спроектировать и поэтапно реализовать локальную аналитическую систему для криптовалютного рынка.

Система должна строиться как исследовательская quant-платформа, а не как «бот, который угадывает цену».

---

# 1. Жёсткие ограничения проекта

1. Бюджет первой полноценной версии: **0 рублей**.
2. Использовать open-source ПО и бесплатные публичные API.
3. Система должна работать локально на обычном пользовательском ноутбуке.
4. GPU не является обязательным.
5. Не использовать платные LLM API.
6. Не использовать платные market-data API без отдельного согласования.
7. Максимизировать качество внутри бесплатного data stack.
8. Не оптимизировать систему под красивый исторический backtest.
9. Любой edge должен подтверждаться out-of-sample и forward testing.
10. До отдельного решения пользователя реальные сделки не совершаются.
11. Основной режим — analysis + recommendation + paper trading.
12. Все прогнозы сохраняются в immutable-журнал.
13. Нельзя задним числом менять прогнозы или результаты.
14. Все proprietary scores должны иметь документированную методологию.
15. Не утверждать, что система определяет BlackRock, конкретный фонд, market maker или «институционала», если данные этого прямо не позволяют.
16. Основной язык решений — probability, expected value, uncertainty и risk.
17. Сигнал `NO TRADE` является полноценным и желательным результатом.
18. Система должна предпочитать отсутствие сделки слабому сигналу.
19. Любой платный источник в будущем подключать только через A/B проверку его дополнительной ценности.
20. Реальный trading execution не входит в MVP.

---

# 2. Главная цель

Создать локальную платформу из **двух независимых моделей**.

## MODEL A — CORE QUANT MODEL

Основная системная модель для:

- BTC;
- ETH;
- позднее — SOL, BNB, XRP и других крупных ликвидных активов.

Задача:

анализировать:

- price action;
- spot market;
- futures/perpetual market;
- order flow;
- order book;
- open interest;
- funding;
- liquidations;
- volatility;
- cross-exchange divergences;
- market regime;

и рассчитывать:

- `P(UP)`;
- `P(NEUTRAL)`;
- `P(DOWN)`;

на нескольких временных горизонтах.

Главная цель Model A:

> обнаруживать статистически подтверждённый trading edge при контролируемом риске.

---

## MODEL B — CRYPTO OPPORTUNITY MODEL

Отдельная модель поиска рыночных возможностей.

Она делится на две подсистемы.

### MODEL B1 — ALTCOIN VALUE / ROTATION

Поиск:

- ликвидных альткоинов;
- потенциальной относительной недооценённости;
- sector rotation;
- relative strength;
- accumulation-like activity;
- раннего изменения рыночного режима;
- потенциально сильных narratives.

Горизонт:

- дни;
- недели.

### MODEL B2 — MEME / MOMENTUM SCANNER

Высокорисковая система для:

- новых DEX-токенов;
- мемкоинов;
- раннего подтверждённого momentum;
- притока ликвидности;
- ускорения объёма;
- фаз распределения;
- timely exit.

Горизонт:

- минуты;
- часы;
- максимум несколько дней.

Model B2 никогда не должна использовать тот же risk budget, что Model A.

---

# 3. Логическая архитектура

```text
                         PUBLIC DATA
                              │
          ┌───────────────────┼───────────────────┐
          │                   │                   │
       Binance              Bybit          DEX / Security
          │                   │                   │
          └───────────────────┼───────────────────┘
                              ▼
                        DATA COLLECTORS
                              │
             ┌────────────────┼────────────────┐
             ▼                ▼                ▼
          Trades          Order Book      Derivatives
                                            │
                              ┌─────────────┼─────────────┐
                              ▼             ▼             ▼
                             OI          Funding      Liquidations
                              │
                              ▼
                        RAW STORAGE
                              │
                              ▼
                       FEATURE ENGINE
                              │
                 ┌────────────┴────────────┐
                 ▼                         ▼
            MODEL A FEATURES          MODEL B FEATURES
                 │                         │
                 ▼                         ▼
             MODEL A                   MODEL B
             CORE QUANT            OPPORTUNITY MODEL
                 │                 ┌───────┴────────┐
                 │                 ▼                ▼
                 │                B1               B2
                 │             Altcoins           Memes
                 └───────────────┬─────────────────┘
                                 ▼
                         DECISION ENGINE
                                 │
                     ┌───────────┼───────────┐
                     ▼           ▼           ▼
                  BACKTEST    PAPERTRADE    ALERTS
                                 │
                                 ▼
                         PERFORMANCE STORE
                                 │
                                 ▼
                           TELEGRAM BOT
```

Model A и Model B используют общую инфраструктуру, но:

- имеют независимые feature sets;
- имеют независимые target definitions;
- имеют независимые models;
- имеют независимые risk policies;
- имеют раздельную статистику.

---

# 4. Технологический стек

Предпочтительный стек:

- Python 3.11+;
- pandas и/или Polars;
- NumPy;
- scikit-learn;
- LightGBM;
- XGBoost;
- CatBoost;
- DuckDB;
- Parquet;
- SQLite только для небольших служебных сущностей, если нужно;
- Freqtrade;
- FreqAI — только там, где интеграция оправдана;
- CCXT;
- официальные REST/WebSocket API бирж;
- asyncio;
- websockets;
- pydantic;
- pytest;
- Telegram Bot API;
- matplotlib или Plotly для отчётов;
- Git.

Не использовать без доказанной необходимости:

- Kafka;
- Spark;
- Kubernetes;
- Redis cluster;
- Airflow;
- отдельный distributed database;
- тяжёлую микросервисную архитектуру.

---

# 5. Ограничения по железу

Система должна быть пригодна для:

- CPU-only;
- 8 GB RAM — минимально допустимо;
- 16 GB RAM — предпочтительно;
- обычного SSD;
- Windows/Linux/macOS;
- локального запуска.

GPU может использоваться в будущем, но не должен быть prerequisite.

Перед каждым существенным компонентом оцени:

- RAM consumption;
- disk usage;
- CPU load;
- expected data growth.

---

# 6. Структура проекта

Предпочтительно:

```text
crypto_quant/
│
├── config/
│   ├── settings.yaml
│   ├── assets.yaml
│   └── logging.yaml
│
├── collectors/
│   ├── binance/
│   ├── bybit/
│   ├── dex/
│   └── security/
│
├── storage/
│   ├── parquet/
│   ├── duckdb/
│   ├── schemas/
│   └── retention/
│
├── features/
│   ├── technical/
│   ├── orderflow/
│   ├── orderbook/
│   ├── derivatives/
│   ├── cross_exchange/
│   ├── regime/
│   ├── altcoin/
│   └── meme/
│
├── models/
│   ├── core/
│   ├── altcoin/
│   ├── meme/
│   ├── calibration/
│   └── registry/
│
├── labels/
│
├── backtesting/
│
├── papertrading/
│
├── risk/
│
├── decision/
│
├── monitoring/
│
├── telegram/
│
├── reports/
│
├── tests/
│
├── scripts/
│
├── notebooks/
│
└── main.py
```

---

# 7. Data storage

## Основной формат

Использовать:

- Parquet — historical time series;
- DuckDB — аналитические запросы по Parquet.

CSV допустим только как export/debug format.

## Рекомендуемое партиционирование

```text
/data/
    raw/
        exchange=binance/
            symbol=BTCUSDT/
                year=2026/
                    month=08/
        exchange=bybit/
    processed/
    features/
    predictions/
    paper_trades/
    models/
    reports/
```

---

# 8. Retention policy

Не сохранять бесконтрольно все raw данные.

## OHLCV

Хранить постоянно.

## Funding / OI

Хранить постоянно.

## Liquidations

Хранить постоянно либо хранить raw ограниченное время + постоянные агрегаты.

## Trades

Raw trades:

- хранить 30–90 дней;
- затем оставлять агрегаты.

Постоянные агрегаты, например:

- 1s;
- 5s;
- 1m;
- 5m.

## L2 Order Book

Не хранить каждый update бесконечно.

Realtime collector должен рассчитывать и сохранять derived features:

- spread;
- microprice;
- bid depth;
- ask depth;
- imbalance;
- depth ±0.1%;
- depth ±0.5%;
- depth ±1%;
- order-book slope;
- concentration;
- liquidity walls;
- wall persistence;
- replenishment;
- absorption;
- liquidity migration.

Raw L2 допустимо временно хранить ограниченное число дней для исследований.

---

# 9. Источники данных

Перед написанием коннектора обязательно проверять **актуальную официальную документацию**, текущие endpoint и rate limits.

Не использовать endpoint «по памяти».

## Binance

Получать бесплатно, если доступно:

- OHLCV;
- trades;
- aggregate trades;
- spot order book;
- perpetual order book;
- funding;
- open interest;
- derivatives market data.

## Bybit

Получать:

- OHLCV;
- trades;
- spot/perpetual order book;
- funding;
- OI;
- liquidations;
- derivatives market data.

Binance и Bybit использовать одновременно для cross-exchange analysis.

## Для Model B

Потенциальные источники:

- CoinGecko;
- GeckoTerminal;
- DEX Screener;
- GoPlus;
- публичные blockchain RPC/API.

Каждый источник должен иметь:

- adapter;
- rate limiter;
- retry strategy;
- timestamp normalization;
- health checks.

---

# 10. Единый стандарт времени

Все данные приводить к UTC.

Хранить:

- exchange timestamp;
- local receive timestamp;
- processing timestamp.

Это нужно для:

- latency analysis;
- debugging;
- предотвращения временного leakage.

---

# 11. MODEL A — первоначальный scope

MVP:

- BTCUSDT;
- ETHUSDT;
- Binance;
- Bybit.

Не добавлять десятки монет до стабилизации pipeline.

---

# 12. Model A — feature families

## 12.1 Price / Technical

Кандидаты:

- returns;
- log returns;
- rolling return;
- EMA;
- SMA;
- RSI;
- ATR;
- ADX;
- Bollinger width;
- realized volatility;
- rolling skew/kurtosis;
- momentum;
- VWAP distance;
- local high/low distance;
- breakout strength;
- trend strength;
- volume z-score;
- realized range.

Классические индикаторы — только часть модели.

---

## 12.2 Trade / Order-flow features

Из raw trades рассчитывать:

- aggressive buy volume;
- aggressive sell volume;
- delta;
- cumulative delta / CVD;
- CVD acceleration;
- trade count;
- average size;
- median size;
- trade size quantiles;
- max trade;
- large buy volume;
- large sell volume;
- signed large-trade delta.

Whale-like activity определять статистически, например:

- rolling 99th percentile;
- 99.5th;
- 99.9th.

Не называть это «институциональными покупками».

---

## 12.3 Order-book features

Рассчитывать:

- spread;
- relative spread;
- bid/ask depth;
- weighted imbalance;
- microprice;
- depth asymmetry;
- slope;
- liquidity walls;
- wall lifetime;
- wall migration;
- replenishment;
- absorption;
- order cancellation intensity;
- order arrival intensity;
- spoofing-like anomaly score — только как эвристику.

Базовый imbalance:

```text
bid_depth / (bid_depth + ask_depth)
```

Считать на нескольких диапазонах от mid-price.

---

## 12.4 Derivatives

Использовать:

- open interest;
- OI delta;
- OI acceleration;
- funding rate;
- funding rolling percentile;
- funding z-score;
- basis;
- price/OI divergence;
- spot/perp divergence;
- long liquidation pressure;
- short liquidation pressure;
- liquidation imbalance.

Отдельно feature-engineering для комбинаций:

```text
Price ↑ + OI ↑
Price ↑ + OI ↓
Price ↓ + OI ↑
Price ↓ + OI ↓
```

---

## 12.5 Cross-exchange

Сравнивать Binance vs Bybit:

- price divergence;
- spot/perp divergence;
- CVD divergence;
- volume divergence;
- OI divergence;
- funding divergence;
- book imbalance divergence.

---

## 12.6 Market regime

Классифицировать:

- trend up;
- trend down;
- range;
- low volatility;
- high volatility;
- breakout;
- post-breakout;
- panic;
- liquidation cascade;
- mean-reversion regime.

Не считать, что одна модель одинаково работает во всех режимах.

---

# 13. Model A — targets

Не делать основной задачей прогноз абсолютной цены.

Основные horizons:

- 1h;
- 4h;
- 12h;
- 24h.

Для каждого:

- UP;
- NEUTRAL;
- DOWN.

Neutral threshold должен учитывать:

- expected volatility;
- fees;
- spread;
- slippage.

Дополнительный regression target:

- future return;
- future volatility;
- maximum favourable excursion;
- maximum adverse excursion.

---

# 14. Model A — baseline models

Обязательно:

1. Always neutral.
2. Random.
3. Momentum.
4. EMA crossover.
5. Logistic Regression.
6. LightGBM.
7. XGBoost.
8. CatBoost.

Сложная модель должна сравниваться с простыми.

Если сложность не улучшает unseen performance — не использовать.

---

# 15. Model A — ensemble

Позже можно создать:

- Technical Model;
- OrderFlow Model;
- Derivatives Model;
- Regime Model.

Meta-model объединяет их.

Weights не задавать «на глаз», если возможно обучить/калибровать.

Пример output:

```text
BTC / 4H

P(UP):      68%
P(NEUTRAL): 21%
P(DOWN):    11%

Expected return: +0.84%
Regime: bullish expansion
Volatility: medium/high

Technical: bullish
Spot CVD: bullish
Perp CVD: neutral
OI: +3.2%
Funding z-score: +0.7
Liquidation pressure: short-side
Order-book imbalance: +0.18

Decision:
LONG CANDIDATE

Confidence:
HIGH

Invalidation:
117300

Risk:
MEDIUM
```

Если преимущество слабое:

```text
NO TRADE
```

---

# 16. MODEL B — общий pipeline

Model B не должна запускать тяжёлый анализ для тысяч токенов.

Pipeline:

```text
UNIVERSE
   ↓
CHEAP FILTER
   ↓
SHORTLIST
   ↓
DEEP ANALYSIS
   ↓
SECURITY CHECK
   ↓
OPPORTUNITY SCORE
   ↓
WATCH / ENTRY / REJECT
```

---

# 17. MODEL B1 — Altcoin Value / Rotation

Искать:

- относительную силу;
- capital rotation;
- liquidity expansion;
- accumulation-like patterns;
- volume acceleration;
- sector outperformance;
- потенциальную относительную недооценённость.

Features:

- market cap;
- FDV;
- circulating supply;
- FDV/market-cap;
- volume;
- volume/market-cap;
- liquidity;
- liquidity growth;
- historical drawdown;
- relative strength vs BTC;
- relative strength vs ETH;
- sector-relative strength;
- volatility;
- spot volume growth;
- futures OI;
- funding;
- breakout state;
- accumulation proxies;
- exchange availability;
- token age;
- unlock/supply-related risk, если бесплатно доступно.

Не считать «упало на 80%» признаком недооценённости.

---

# 18. Altcoin Opportunity Score

0–100.

Компоненты должны быть документированы.

Пример групп:

- liquidity quality;
- market structure;
- relative strength;
- volume acceleration;
- derivatives positioning;
- valuation proxies;
- risk penalties.

Score должен быть либо:

- прозрачной формулой;
- либо output документированной ML-модели.

Не использовать декоративные числа.

---

# 19. MODEL B2 — Meme / Momentum

Первый приоритет сети:

- Solana.

Позднее:

- Base;
- Ethereum;
- BSC.

Model B2 — high-risk speculative subsystem.

---

# 20. Meme первичные фильтры

До ML фильтровать по:

- pool age;
- liquidity;
- market cap;
- FDV;
- volume 5m/15m/1h;
- volume acceleration;
- transaction count;
- buys;
- sells;
- buy/sell ratio;
- liquidity growth;
- price acceleration;
- trade-size distribution;
- liquidity/FDV;
- volume/liquidity.

Отсекать очевидный мусор до глубокого анализа.

---

# 21. Security Gate

До `ENTRY CANDIDATE` проверять, насколько возможно:

- honeypot;
- sell restrictions;
- abnormal tax;
- mint authority;
- freeze authority;
- owner privileges;
- upgradeability;
- blacklist ability;
- holder concentration;
- deployer holdings;
- liquidity concentration;
- suspicious transfer restrictions;
- abnormal supply mechanics;
- liquidity lock/burn signals;
- proxy risks.

Использовать GoPlus или бесплатные аналоги.

При high-severity security flag:

```text
REJECT
```

независимо от momentum.

---

# 22. Meme momentum features

Кандидаты:

- price acceleration;
- volume acceleration;
- liquidity acceleration;
- transaction acceleration;
- buy/sell ratio;
- large-buy pressure;
- large-sell pressure;
- new-high frequency;
- VWAP deviation;
- short-term volatility;
- liquidity/FDV;
- volume/liquidity;
- momentum persistence;
- trend smoothness;
- retracement depth;
- holder/activity growth proxies;
- pool age;
- distribution signals.

---

# 23. Meme state machine

Каждый токен имеет состояние:

```text
REJECT
WATCH
EARLY
MOMENTUM
EXTENDED
DISTRIBUTION
EXIT
```

Определения:

## WATCH

Есть признаки интереса, но подтверждений мало.

## EARLY

Появился подтверждённый приток ликвидности/объёма.

## MOMENTUM

Momentum устойчив и подтверждается.

## EXTENDED

Цена слишком растянута; риск позднего входа высок.

## DISTRIBUTION

Появляются признаки разгрузки.

## EXIT

Momentum сломан или риск резко вырос.

---

# 24. Meme Exit Engine

Выход должен быть отдельным модулем.

Отслеживать:

- decline in buy acceleration;
- sell pressure growth;
- large sells;
- liquidity withdrawal;
- transaction growth slowdown;
- price/CVD divergence;
- parabolic extension;
- inability to make new highs;
- retracement depth increase;
- momentum persistence break.

Пример:

```text
EXIT RISK: HIGH

Price since signal: +143%

Large sell pressure: rising
Buy acceleration: falling
Liquidity: -9%
CVD divergence: bearish
Momentum persistence: broken

State:
DISTRIBUTION / EXIT
```

---

# 25. Risk Engine

Paper trading first.

Поддерживать архитектурно:

- risk per trade;
- maximum daily loss;
- maximum portfolio drawdown;
- volatility-adjusted position size;
- correlation limits;
- analytical invalidation отдельно от position stop-loss;
- trailing exits;
- partial take profit;
- portfolio exposure caps.

Запрещено:

- martingale;
- uncontrolled averaging down;
- doubling after loss.

Risk budgets:

- Model A — normal;
- Model B1 — reduced;
- Model B2 — very small speculative allocation.

---

# 26. Backtesting methodology

Для time series запрещён random split.

Использовать:

- chronological split;
- walk-forward;
- rolling window;
- expanding window.

Пример:

```text
Train:      2021–2023
Validation: 2024
Test:       2025
Forward:    2026
```

Test period нельзя использовать для настройки.

---

# 27. Trading realism

Backtest должен учитывать:

- commissions;
- spread;
- slippage;
- latency;
- funding;
- liquidity;
- estimated market impact.

Для low-liquidity meme assets slippage должен моделироваться агрессивнее.

---

# 28. ML metrics

Считать:

- precision;
- recall;
- F1;
- ROC-AUC, где корректно;
- PR-AUC, где корректно;
- log loss;
- Brier Score;
- calibration error;
- confusion matrix.

Accuracy не является основной метрикой.

---

# 29. Trading metrics

Считать:

- total return;
- CAGR;
- Sharpe;
- Sortino;
- Calmar;
- maximum drawdown;
- expectancy;
- profit factor;
- win rate;
- average win;
- average loss;
- tail loss;
- turnover;
- fee impact;
- slippage impact.

Для Model B:

- precision@K;
- hit rate;
- average MFE;
- average MAE;
- time-to-peak;
- rug/security false-negative rate.

---

# 30. Feature leakage protection

Строго предотвращать:

- look-ahead bias;
- future leakage;
- survivorship bias;
- data snooping;
- accidental label leakage.

На timestamp T можно использовать только то, что реально было известно не позднее T.

---

# 31. Immutable Prediction Journal

Каждый прогноз сохранять:

```text
prediction_id
created_at
asset
venue
model_name
model_version
feature_version
price_at_signal
P_up
P_neutral
P_down
expected_return
signal
confidence
invalidation_type
invalidation_level
invalidation_timeframe
invalidation_condition
stop_loss_type
stop_loss_price
tp_conservative_price
tp_base_price
tp_aggressive_price
model_confidence
trade_risk_score
trade_risk_class
opportunity_score
data_quality_score
```

Запись после создания нельзя редактировать.

Изменение модели = новая версия.

---

# 32. Model Registry

Для каждой модели хранить:

```text
model_name
model_version
training_period
validation_period
test_period
feature_hash
parameters_hash
training_timestamp
validation_metrics
test_metrics
artifact_path
```

---

# 33. Paper Trading

Иметь отдельные виртуальные portfolios:

- Core A;
- Altcoin B1;
- Meme B2.

Учитывать:

- simulated entry;
- simulated exit;
- commissions;
- spread;
- slippage;
- funding;
- partial exits.

---

# 34. Telegram

Telegram — последний слой, а не ядро.

Команды:

```text
/status
/btc
/eth
/core
/opportunities
/memes
/watchlist
/portfolio
/performance
/models
/datahealth
```

Не спамить.

Автоматический alert отправлять только при значимом изменении состояния.

---

# 35. Data Health

Мониторить:

- WebSocket disconnect;
- stale feed;
- missing candles;
- delayed data;
- API errors;
- rate limits;
- clock drift;
- stale order book;
- missing OI;
- missing funding.

При плохих данных:

```text
SIGNALS DISABLED — DATA QUALITY FAILURE
```

Нельзя тихо строить прогноз на неполных данных.

---

# 36. Logging

Structured logging:

- DEBUG;
- INFO;
- WARNING;
- ERROR;
- CRITICAL.

Логи должны содержать:

- timestamp;
- component;
- exchange;
- symbol;
- error type;
- retry state.

---

# 37. Тестирование

Каждый серьёзный компонент должен иметь tests.

Минимум:

- unit tests;
- schema tests;
- timestamp tests;
- duplicate detection;
- gap detection;
- feature calculation tests;
- leakage tests;
- model pipeline tests;
- backtest consistency tests.

---

# 38. Фазы разработки

## PHASE 0 — Environment

Создать:

- Python environment;
- Git repo;
- config management;
- secrets management;
- logging;
- pytest;
- project skeleton.

## PHASE 1 — Core Data

Только:

- BTCUSDT;
- ETHUSDT;
- Binance;
- Bybit.

Собрать:

- OHLCV;
- trades;
- funding;
- OI;
- liquidations.

## PHASE 2 — Core Baseline

Создать:

- dataset;
- labels;
- technical features;
- Logistic Regression;
- LightGBM;
- chronological validation;
- baseline backtest.

## PHASE 3 — Core Advanced

Добавить:

- CVD;
- derivatives;
- cross-exchange divergence;
- market regime;
- order-book features.

## PHASE 4 — Core Paper

Запустить Model A в live paper mode.

Записывать immutable predictions.

## PHASE 5 — Altcoin Scanner

Добавить Model B1.

## PHASE 6 — Meme Scanner

Добавить:

- DEX data;
- security checks;
- Solana universe;
- cheap filters;
- momentum shortlist.

## PHASE 7 — Meme Exit Engine

Добавить distribution/exit logic.

## PHASE 8 — Telegram

Добавить пользовательский интерфейс.

## PHASE 9 — Forward Test

Минимум 30–90 дней paper/forward testing.

---

# 39. Gate criteria между фазами

Нельзя переходить дальше только потому, что код запускается.

Каждая фаза должна иметь Definition of Done.

Пример для Model A baseline:

- collector стабилен;
- данные без существенных gaps;
- timezone корректный;
- schema валидна;
- leakage tests пройдены;
- baseline рассчитан;
- chronological test выполнен;
- fees учтены;
- результаты сохранены;
- code/tests committed.

---

# 40. Реальные деньги

Автоматический live trading запрещён до отдельного решения.

Перед обсуждением real money предоставить:

```text
Forward period
Number of signals
Number of trades
Net PnL
Max Drawdown
Sharpe
Sortino
Profit Factor
Fees
Slippage
Worst day
Worst week
Tail losses
Calibration
Performance by market regime
```

Только после этого пользователь принимает решение.

---

# 41. Платные источники

Первая версия: 0 ₽.

Если предлагается платный API:

1. Объяснить, какие именно данные он добавляет.
2. Построить baseline без него.
3. Добавить paid feature.
4. Провести identical out-of-sample test.
5. Измерить incremental improvement.
6. Рассчитать, оправдывает ли улучшение стоимость.

Не покупать данные «для солидности».

---

# 42. Основная философия

Цель проекта не:

> сделать красивого Telegram-бота.

Цель:

> найти воспроизводимый statistical edge.

Не являются доказательством:

- большое число features;
- нейросеть;
- AI;
- сложная архитектура;
- высокий historical return;
- высокий win rate сам по себе.

Серьёзное доказательство:

```text
unseen data
+
walk-forward
+
forward testing
+
realistic costs
+
controlled risk
+
reproducibility
```

---

# 43. Правила поведения AI-разработчика

На каждом этапе:

1. Сначала объясни решение.
2. Затем покажи структуру файлов.
3. Затем предложи минимальную реализацию.
4. Затем напиши tests.
5. Затем покажи команды запуска.
6. Затем опиши ожидаемый результат.
7. Затем перечисли риски/ограничения.
8. Не переписывай работающие части без причины.
9. Все параметры выноси в config.
10. API keys не хранить в коде.
11. Trading permissions не подключать.
12. Использовать read-only market-data access.
13. Не добавлять сложную ML-модель без benchmark.
14. Следить за RAM/storage.
15. Для каждого feature объяснять рыночную гипотезу.
16. Для каждого score документировать метод.
17. Разделять observation и interpretation.
18. Не использовать «магические» термины.
19. При недостатке данных — `UNKNOWN` или `NO TRADE`.
20. Не выдумывать отсутствующие данные.
21. Не заявлять прибыльность до forward test.
22. Не делать вывод по одному удачному периоду.
23. Не оптимизировать параметры на test set.
24. Не изменять historical predictions.

---

# 44. Первая задача после получения этого ТЗ

Не начинай реализовывать весь проект.

Начни только с:

- PHASE 0;
- PHASE 1.

Сначала предоставь четыре артефакта.

## A. Technical Design

Опиши:

- выбранный стек;
- компоненты;
- data flow;
- структуру каталогов;
- Parquet schemas;
- DuckDB usage;
- config strategy;
- logging;
- оценку RAM;
- оценку disk usage.

## B. Data Source Matrix

Таблица:

```text
Data Type
Source
REST/WebSocket
Historical/Realtime
Current endpoint
Rate Limit
Expected Frequency
Storage Policy
Model Usage
```

Только по официальным актуальным источникам.

## C. MVP Scope

Только:

```text
BTCUSDT
ETHUSDT
Binance
Bybit
```

## D. Implementation Plan

Разбей работу PHASE 0–1 на небольшие задачи с Definition of Done.

Только после согласованного design переходи к первому Data Collector.

---

# 45. Конечные вопросы, на которые должна отвечать система

## CORE

> Есть ли сейчас статистически подтверждённый edge для long/short BTC или ETH?

## ALTCOINS

> Какие ликвидные активы сейчас показывают наиболее интересную относительную силу, ликвидность, структуру и потенциальную переоценку?

## MEMES

> Какие молодые токены показывают ранний подтверждённый приток покупателей/ликвидности, проходят security gate и ещё не находятся в поздней фазе пампа?

## RISK

> Когда лучше вообще ничего не делать?

Главная оптимизация:

> не максимальное количество сигналов, а максимальное качество решений и сохранение капитала.

---

# 46. Глобальная архитектурная поправка: localization, risk/exit и prediction markets

## 46.1 Localization и Knowledge Base

Пользовательский слой использует `default_locale=ru-RU` и `fallback_locale=en-US`. Telegram, alerts, reports, signals, explanations, risk messages, help, будущий UI и Knowledge Base не содержат user-facing literals внутри collector, ML, feature, Risk или Exit logic. Они используют versioned localization keys. Python identifiers, enums, schemas, API/database fields, filenames и feature IDs остаются английскими.

Knowledge Base развивается вместе с кодом. Владелец изменения feature/model/risk methodology обновляет связанную документацию в том же change set. Каждый model-eligible feature имеет mapping:

```text
feature_id -> documentation_id -> localization_key
```

Стандарт glossary entry: русское и английское названия, abbreviation, простое и техническое определения, calculation, interpretation, limitations и usage. Будущие interfaces поддерживают `SIMPLE` и `EXPERT` views; UI сейчас не реализуется.

## 46.2 Invalidation, stop-loss и take-profit

Это три разные сущности:

- `invalidation` описывает отмену аналитической гипотезы и может существовать при `NO_TRADE`;
- `stop_loss` ограничивает убыток paper/proposed/real position и отсутствует у `NO_TRADE`;
- `take_profit` задаёт рациональные сценарии фиксации прибыли trade candidate.

Invalidation поддерживает price level, candle close, structure, order-flow, regime, time и composite conditions через `invalidation_type`, `invalidation_level`, `invalidation_timeframe`, `invalidation_condition`. Stop учитывает invalidation, volatility, noise, spread, slippage, liquidity, timeframe, regime и capital risk. Методология versioned, например `structure_plus_atr_v1`; декоративный `entry ± N%` без проверки запрещён.

Для trade candidate резервируются `TP_CONSERVATIVE`, `TP_BASE`, `TP_AGGRESSIVE`. Каждый target хранит price, target-hit-before-stop probability, conditional expected time и cost-aware risk/reward, если они статистически оценимы; иначе поля `null` и пользователь видит «Вероятность пока не рассчитана». Directional probability не подменяет path-dependent target probability.

Partial exits, break-even и trailing stop являются configurable/versioned research hypotheses. Paper research сравнивает single против staged TP, fixed против probability-based TP, partial против single exit, break-even on/off и trailing on/off на одинаковых out-of-sample периодах с costs.

## 46.3 Risk Engine и Exit Engine interface

`model_confidence`, `opportunity_score`, `trade_risk_score` и `trade_risk_class` независимы. Risk classes: `LOW`, `MODERATE`, `HIGH`, `VERY_HIGH`, `EXTREME`; русские labels — Низкий, Умеренный, Высокий, Очень высокий, Экстремальный. Высокая confidence не означает низкий риск.

Trade risk учитывает market, liquidity, model, structural, data и asset risk. Hard gates `DATA_QUALITY_FAILURE`, `SECURITY_HIGH_RISK`, `LIQUIDITY_TOO_LOW`, `EXPECTED_SLIPPAGE_TOO_HIGH`, `MODEL_OOD` дают `NO_TRADE` независимо от upside. `EXTREME` также не допускается как обычный trade candidate. Risk class ограничивает будущий maximum position allocation; конкретные проценты определяются только paper research.

Position sizing следует capital-at-risk: при одинаковом risk budget большее расстояние до stop означает меньшую позицию. Meme B2 дополнительно резервирует emergency exits для liquidity collapse, security change, sell restriction, deployer movement, spread expansion и pool/contract anomaly.

Future paper audit хранит отдельно:

```text
hypothesis_invalidated_at
stop_triggered_at
simulated_fill_price
slippage
realized_loss
post_stop_mfe
exit_method_version
partial_exit_fills
```

Нормативный, но пока не runtime-реализованный signal/risk/exit/lifecycle интерфейс находится в `schemas/future_signal.schema.json` и имеет DRAFT version `0.2.0-draft`. Отдельный optional Polymarket context пока сохраняет собственную версию `0.1.0-draft`.

## 46.4 Polymarket / prediction-market intelligence

PHASE 3F — optional independent external-event feature family после PHASE 3E и до Core Feature Audit. Она не блокирует baseline или Binance/Bybit pipeline. Перед реализацией заново проверяются официальные Gamma API, Data API, CLOB market-data API, WebSocket, historical prices и metadata; endpoints не проектируются по памяти.

Prediction-market entities отделены от exchange instruments:

```text
prediction_event
prediction_market
prediction_market_snapshot
prediction_trade
prediction_orderbook_snapshot
prediction_resolution
```

Минимальная taxonomy: `MACRO`, `POLITICAL`, `GEOPOLITICAL`, `CRYPTO`, `NARRATIVE`, `SPORTS_RESERVED`. Каждый event имеет explicit event-to-asset mapping и economic hypothesis. Нельзя сворачивать рынки в один недокументированный sentiment score.

Future features могут включать probability level/change/velocity/acceleration, volume, liquidity, spread, depth imbalance, trade intensity, time-to-resolution и market-quality score. Quality filter учитывает liquidity, volume, spread, activity, resolution clarity, status и remaining time. При плохом качестве используется `POLYMARKET_FEATURE_INVALID`; infinite forward-fill запрещён.

Resolved outcome недоступен до knowledge time resolution. Все snapshots/revisions подчиняются event/received/knowledge time и lineage. Failure isolation обязательно: outage Polymarket делает его features unavailable, но Core baseline и Binance/Bybit продолжают работу.

Integration допускается только после ablation:

```text
M0 = Core without Polymarket
M1 = identical Core + Polymarket
```

На одинаковом out-of-sample периоде сравниваются Brier, log loss, calibration, expectancy, net PnL, Sharpe и drawdown. Lead-lag проверяется в обе стороны; correlation не объявляется causation. Без подтверждённого incremental edge Polymarket не включается в paper/production Core.

## 46.5 Русский signal contract

User-facing signal показывает только рассчитанные значения и явно разделяет model confidence, trade risk, invalidation, stop и три target scenarios. Simple view содержит asset, horizon, direction/NO_TRADE, probabilities, decision, confidence, risk, invalidation, stop, targets и основные причины. Expert view добавляет raw/model features, OI/CVD/funding/order book, Polymarket context, versions, DQ, expected value и methodology.

Если показатель отсутствует или ненадёжен, используется localized «Недостаточно данных»/«Вероятность пока не рассчитана» либо блок не показывается. Числа, target probabilities, time-to-target, R:R и expected value никогда не генерируются декоративно.

# H. TRADE LIFECYCLE / DYNAMIC POSITION MANAGEMENT

Этот обязательный будущий модуль управляет не только первоначальным сигналом, но и полной жизнью сценария. Он не является торговым runtime engine в PHASE 0–3 и не разрешает автоматическое исполнение или Telegram alerts раньше соответствующей фазы.

## H1. Time context и time-based invalidation

Каждый trade candidate/revision обязан иметь `signal_created_at`, `signal_horizon`, `expected_holding_time`, `maximum_holding_time`, `next_review_at` и `scenario_expiry_at`. Допустимые базовые горизонты: `1H`, `4H`, `12H`, `24H`; пользовательский текст показывает диапазон и ориентировочную длительность, а не ложную точность.

Сценарий поддерживает независимые `PRICE`, `STRUCTURE`, `FEATURE`, `TIME` и `COMPOSITE` invalidation. Time invalidation означает необходимость новой оценки, если edge не реализовался до зафиксированного срока.

## H2. Periodic re-evaluation

Открытый proposed/paper сценарий переоценивается scheduled и event-driven. Концептуальные initial defaults: 1H — 5–15m, 4H — 15–30m, 12H — 30–60m, 24H — 1–2h; это не production thresholds до отдельного backtest/paper evidence.

## H3. Event-driven re-evaluation

Внеплановая переоценка требуется при price shock, target reached, stop proximity, regime/CVD/OI/funding change, liquidation cascade, order-book break, external event-risk, data-quality degradation и security/liquidity event для Model B. Каждая переоценка создаёт новую immutable revision с `signal_id`, `revision_id`, `parent_revision_id`, `revision_number`, `revision_timestamp`, `knowledge_time`, review trigger и evidence hashes. Исходный сигнал не перезаписывается.

## H4. Signal revision

Каждая revision является полным immutable snapshot, а не мутабельным patch. Она хранит current probabilities/stop/targets/risk/time context и audit diff `revision_changes` с previous/new values, reason code, localized explanation key и evidence hashes. `knowledge_time` не подменяется `revision_timestamp`. Revision chain имеет один `signal_id`, уникальные IDs, строгий parent link и возрастающие revision number/time.

## H5. Dynamic Stop Loss

Допустимые stop recommendations: `KEEP_STOP`, `TIGHTEN_STOP`, `MOVE_TO_BREAK_EVEN`, `TRAIL_STOP`, `CANCEL_STOP_AND_EXIT`. Автоматическое расширение stop в сторону большего риска запрещено. Любое исключение возможно только как отдельное исследованное, versioned и явно одобренное правило; текущий contract не содержит действия `WIDEN_STOP`.

## H6. Dynamic Take Profit

TP1/TP2/TP3 могут быть пересмотрены при доказанном изменении volatility, structure, momentum, liquidity, regime или event risk. Каждая target revision имеет ID, scenario role, status, probability/time/R:R и localized explanation. Profit-protection recommendations могут включать partial take profit, stop tightening, break-even, trailing и отмену агрессивной цели. Все методы являются research hypotheses и требуют static-vs-dynamic backtest/paper comparison на одинаковых сигналах и одинаковых costs.

## H7. Profit protection

После движения в прибыль versioned policy может рекомендовать partial exit, stop tightening, break-even, trailing либо cancellation aggressive TP. Percentages и triggers не универсальны и не появляются без backtest. Любое изменение обязано иметь method version, reason и decision-time evidence.

## H8. Scenario state machine

Зарезервированные states: `NEW`, `ACTIVE`, `STRENGTHENED`, `WEAKENED`, `TP1_REACHED`, `TP2_REACHED`, `TP3_REACHED`, `TRAILING`, `INVALIDATED`, `EXIT_RECOMMENDED`, `CLOSED`, `EXPIRED`. Terminal states `INVALIDATED`, `CLOSED`, `EXPIRED` не имеют `next_review_at`.

`target_progress` (`NONE`, `TP1`, `TP2`, `TP3`) хранится отдельно и не может регрессировать; `management_mode` также отделён от scenario strength. Это предотвращает потерю факта TP1 при последующем состоянии `WEAKENED`. После terminal state child revision запрещена; re-entry создаёт новый `signal_id`.

## H9. Telegram meaningful-change policy

Будущий Telegram отправляет отдельное сообщение только при meaningful change: смена direction/NO_TRADE, material probability/risk/stop/target/holding-time change, target reached, invalidation, exit recommendation либо critical data/security event. Пересчёт сам по себе alert не создаёт. Thresholds, dedup и cooldown принадлежат versioned alert policy, а не Telegram handler.

## H10. Initial signal view

Первичное русское сообщение показывает asset/direction, понятный horizon range, базовую ожидаемую длительность, рассчитанные probabilities, stop/TP1/TP2/TP3 и следующую плановую переоценку. Нерассчитанное поле не выдумывается и отображается как «Недостаточно данных» либо скрывается.

## H11. Update message

Update показывает previous → new для materially changed probability, risk, stop, targets и expected horizon; target status, management recommendation и локализованные причины. Неизменившиеся значения могут быть кратко отмечены, но полная revision всё равно хранится в journal.

## H12. Weakening / exit update

При ослаблении сценария update отдельно сообщает причины, cancelled/superseded targets, tightened stop и position action (`REDUCE`, `CLOSE_PARTIAL`, `CLOSE_FULL`, `NO_ACTION`). Это recommendation для paper/proposed position, не автоматический order.

## H13. Audit и обязательное сравнение

Paper audit хранит всю revision chain и позволяет сравнить Static Stop/TP против Dynamic Stop/TP: realized outcome, stop/target changes, fills, costs, post-stop MFE, holding-time calibration и false early exits.

Static и Dynamic варианты используют одинаковые исходные signals, market path и cost assumptions. Dynamic rules не настраиваются на test set; оцениваются сохранённая/потерянная прибыль, false early exits и calibration holding-time estimates.

## H14. Current implementation boundary

Нормативный интерфейс — `schemas/future_signal.schema.json`, версия `0.2.0-draft`; он является только schema reservation, а не реализацией lifecycle engine.

Сейчас разрешены specification, interface schema, semantic safety validator, i18n и Knowledge Base. Scheduler, event bus, paper position linkage, dynamic algorithms, alert thresholds и Telegram остаются PHASE 4/поздними задачами. PHASE 1 scope не меняется; следующий фактический этап проекта — PHASE 1A.
