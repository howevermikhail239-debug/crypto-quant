# PHASE 1A — Binance OHLCV 1m runbook

Pipeline поддерживает BTCUSDT/ETHUSDT Spot и USDⓈ-M linear perpetual через один parameterized adapter. Monthly archives загружаются вместе с `.CHECKSUM`, нормализуются только после проверки checksum и сохраняются как immutable monthly Parquet objects. REST используется для tail/backfill только закрытых свечей (`close_time < conservative_cutoff`) и пишет отдельные immutable generations. Archive/REST overlap обязан совпасть; конфликт не перезаписывается молча.

Перед ingestion выполняется `recover_stale_partials(data_root)`: незавершённые `.partial` перемещаются в quarantine. Parquet сначала валидируется, затем публикуется; manifest записывается после data object, checkpoint — только после manifest. Повторный запуск достраивает отсутствующий manifest/checkpoint без duplicate data. Historical `knowledge_time` остаётся `null`.

Source contracts находятся в `schemas/contracts/`; полный результат и измерения — в [PHASE 1A report](phase1a-report.md). Runtime data, manifests, checkpoints, gap registry и metadata snapshots находятся во внешнем `C:/crypto_quant_data`, а не в Git.
