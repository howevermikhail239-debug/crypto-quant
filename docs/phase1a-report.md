# PHASE 1A report — Binance OHLCV 1m

Дата проверки: 2026-08-10. Scope завершён для четырёх разных canonical instruments: BTCUSDT и ETHUSDT на Binance Spot, а также BTCUSDT и ETHUSDT USDⓈ-M linear perpetual. Bybit и любые данные, кроме OHLCV, в PHASE 1A не входят.

## Проверенное покрытие источников

Снимок официального `data.binance.vision` inventory показал 108 monthly objects для каждого Spot-инструмента (2017-08…2026-07) и 79 для каждого USDⓈ-M инструмента (2020-01…2026-07). Это наблюдаемое покрытие на дату проверки, а не гарантия Binance о неизменности архива.

| Instrument | July 2026 rows | raw ZIP | Parquet | runtime |
|---|---:|---:|---:|---:|
| BTCUSDT Spot | 44,640 | 2,112,275 B | 3,851,723 B | 1.348 s |
| ETHUSDT Spot | 44,640 | 2,027,161 B | 3,580,686 B | 4.192 s |
| BTCUSDT USDⓈ-M | 44,640 | 1,844,583 B | 3,708,678 B | 3.895 s |
| ETHUSDT USDⓈ-M | 44,640 | 1,905,641 B | 3,619,342 B | 4.633 s |

В каждом архивном pilot отсутствуют duplicates, gaps и незакрытые свечи. Для всех четырёх сочетаний также выполнен bounded live REST tail; девять финальных свечей на инструмент сохранены отдельными immutable generations. Instrument metadata snapshots получены через соответствующие Spot и USDⓈ-M `exchangeInfo` endpoints.

Peak Windows working set, измеренный через `GetProcessMemoryInfo` во время полного monthly normalize/write, составил 219,140,096 B (≈209 MiB). Это pilot measurement одного процесса, а не верхняя гарантия для будущих параллельных jobs.

## Рост данных

По фактическим четырём pilot-файлам один полный месяц занимает 14,760,429 B Parquet и 7,889,660 B source ZIP. При сохранении обоих слоёв ориентир равен:

| Horizon | Normalized Parquet | Raw ZIP + Parquet |
|---|---:|---:|
| 1 month | 14.8 MB | 22.7 MB |
| 3 months | 44.3 MB | 68.0 MB |
| 1 year | 177.1 MB | 271.8 MB |

Экстраполяция текущего официального monthly inventory даёт примерно 1.38 GB Parquet. Фактический суммарный размер всех перечисленных ZIP-объектов в inventory — 741,903,319 B; raw + normalized full-history baseline — около 2.12 GB без daily tail, metadata, manifests, quarantine и временных файлов. Для безопасного bootstrap следует резервировать не менее 3 GB, хотя общий проектный SSD headroom остаётся существенно выше из-за будущих trades.

## Семантика и ограничения

- Spot и USDⓈ-M имеют разные source contracts, dataset IDs, instrument identities, metadata endpoints, paths, manifests и checkpoints.
- Spot archive timestamps используют документированную политику: milliseconds до 2025-01 и microseconds начиная с 2025-01. REST Spot и USDⓈ-M archive/REST используют milliseconds.
- Source `.CHECKSUM` проверяется до normalization. Raw checksum и Parquet checksum не смешиваются.
- Historical `knowledge_time` остаётся `null`; `retrieved_at` не подменяет historical availability.
- Monthly partition подтверждён измерениями: 40–45 тысяч строк и 3.6–3.9 MB на instrument-month. Daily partition создал бы неоправданные small files.
- Binance допускает последующие замены archive objects. Кроме того, в официальном репозитории есть открытое сообщение о расхождении отдельных Spot monthly файлов с daily/API. Конфликт регистрируется и не разрешается правилом «последний победил».
- Полнота июля не доказывает полноту всей истории. Full-history bootstrap и автоматический inventory reconciliation остаются следующей эксплуатационной задачей, а не скрытым предположением этого отчёта.

## Audit note

Во время расширения был обнаружен defect: первые два USDⓈ-M archive manifest events ошибочно попали в append-only Spot manifest, хотя data paths и dataset IDs были derivative. Записи не удалялись. После исправления те же content-addressed BTCUSDT/ETHUSDT objects повторно проверены и зарегистрированы в `binance_derivative_ohlcv.jsonl`; regression test запрещает повторение ошибки.

## Quality gate

`uv lock --check`, Ruff и полный pytest проходят: 42 tests, 81% coverage. Проверены contracts, fixtures, timestamp units, OHLC invariants, gaps/duplicates, archive checksum, REST pagination и 418/429, open-candle exclusion, deterministic output, idempotency, stale partial recovery, а также resume между Parquet, manifest и checkpoint.

PHASE 1A завершён. Следующий checkpoint — PHASE 1B / Bybit OHLCV; реализация Bybit здесь не начиналась.
