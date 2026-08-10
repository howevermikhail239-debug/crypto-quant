# PHASE 0 runbook

## Install

```powershell
uv python install 3.12
uv sync --all-groups
```

## Quality gates

```powershell
uv lock --check
uv run ruff check .
uv run pytest
```

## Safe local commands

```powershell
uv run crypto-quant config-check
uv run crypto-quant health
uv run crypto-quant paths-init
uv run python -m crypto_quant config-check
```

`health` never creates the data root. If the root exists, it performs a short write/atomic-rename/delete durability probe and reports `PASS`, `WARN`, `FAIL` or informational `UNKNOWN`. Exit codes are `0` when there is no warning/failure (informational `UNKNOWN` is allowed), `1` for `WARN`, and `2` for `FAIL`. `paths-init` creates the approved empty runtime tree: `raw`, `normalized`, `quarantine`, `spool`, `logs` and `control/*`. Neither command ingests market data.

## Configuration and secrets

Copy `.env.example` to `.env` only if a future optional secret is approved. The repository-root `.env` is loaded automatically and remains ignored. Do not put a secret into YAML, tests, logs or CLI output. Environment overrides use the `CRYPTO_QUANT__` prefix; e.g. `CRYPTO_QUANT__LOGGING__LEVEL=DEBUG`.
