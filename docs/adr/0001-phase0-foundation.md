# ADR 0001: PHASE 0 foundation

**Status:** accepted — 2026-08-10

The project uses Python 3.12 (`.python-version`), `uv`, hatchling, typed Pydantic configuration, YAML defaults, UTC timestamps and JSON logs. Runtime dependencies are deliberately limited to Pydantic, pydantic-settings and PyYAML. Pytest, pytest-cov and Ruff are development-only.

No collector, exchange adapter, HTTP/WebSocket client, Parquet/DuckDB dependency, ML framework or trading integration belongs in PHASE 0.

## Security and paths

`.env` is local and ignored. Optional credentials are typed as `SecretStr`, loaded only through `CRYPTO_QUANT__SECRETS__...`, excluded from safe dumps and redacted in both JSON context and log messages. A key cannot be introduced without a separately documented official-source benefit and minimum permissions. The default data root is external to Git (`C:/crypto_quant_data`), configurable, and rejected if it resolves inside the repository unless explicitly opted in.

## Scope defaults

PHASE 1 scope is Binance/Bybit, BTCUSDT/ETHUSDT, Spot and USDT-linear perpetual. It is `best_effort_local`; 24/7 completeness, VPS, NAS and cloud dependencies are explicitly excluded.
