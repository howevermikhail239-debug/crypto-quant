# Final PHASE 0 Definition of Done

PHASE 0 is complete only when all of the following are true:

- Python 3.12 and a reproducible `uv.lock` create a clean, importable environment.
- The package has no collector, exchange API, WebSocket, Parquet/DuckDB, ML, backtest or trading dependency.
- Typed YAML configuration fixes the approved market scope, retention, UTC, `best_effort_local`, external data root and configurable disk thresholds.
- `.env`, local data, logs, partial files and caches are excluded from Git; structured logging redacts configured secret fields.
- External data-root paths are guarded; initialization creates only control-plane directories.
- Deterministic identity, canonical hashing, UTC/knowledge-time and version-major compatibility primitives are tested.
- Pydantic and committed JSON schemas define Data Contract, manifest event, checkpoint, gap and deletion-ledger control-plane records.
- `config-check`, `health` and `paths-init` commands are documented and work without keys or market data.
- `uv lock --check`, Ruff and pytest pass.
- ADR, README, config/secrets policy and runbook are present.

PHASE 0 completed its automated quality gates on 2026-08-10: lock verification, Ruff and pytest passed. `health` intentionally reports `UNKNOWN` for growth projections until market-data history exists; this is not a claim of operational completeness. PHASE 1A requires an explicit next-step approval; no collectors are included in this phase.
