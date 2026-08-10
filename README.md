# Crypto Quant — local-first quant research pipeline

PHASE 0 foundation, PHASE 1A Binance OHLCV and PHASE 1B Bybit OHLCV are complete. The implemented market-data scope is BTCUSDT/ETHUSDT Spot and USDT linear perpetual on both venues, base interval 1m. It includes official archive/REST ingestion, metadata snapshots, typed normalization, Parquet, provenance, DQ and crash-safe resume. It has no trades, OI, funding, liquidations, ML, backtesting or trading logic.

See [the PHASE 1B report](docs/phase1b-report.md), [the PHASE 1A report](docs/phase1a-report.md), [the Binance OHLCV runbook](docs/runbook-phase1a-binance-spot-ohlcv.md), [the PHASE 0 runbook](docs/runbook-phase0.md), and the approved technical design documents in the repository root.

The external configurable data root defaults to `C:/crypto_quant_data`. It is not part of Git. The system policy is UTC-only and `best_effort_local`. The PHASE 0 CLI is available as `uv run crypto-quant` or `uv run python -m crypto_quant`.

The next checkpoint is PHASE 1C / Trades, with raw and aggregate trades kept separate and aggressor semantics fixture-gated before CVD. The repository also reserves, without runtime behavior, a Russian-first user-facing localization boundary, future signal/risk/exit/lifecycle contracts, and optional non-blocking PHASE 3F Polymarket external-event interfaces. See [the Russian Knowledge Base skeleton](docs/ru/README.md), [the lifecycle page](docs/ru/trade_lifecycle.md), and [the `0.2.0-draft` signal revision schema](schemas/future_signal.schema.json).
