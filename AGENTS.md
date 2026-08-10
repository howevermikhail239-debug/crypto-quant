# Crypto quant project policy

## Authority and delivery order

Follow this hierarchy: `crypto_quant_master_spec.md` defines product constraints and invariants; `crypto_quant_revised_technical_design.md` defines architecture and data semantics; `crypto_quant_phase1_data_contracts.md` defines source-field contracts; `crypto_quant_phased_development_prompt.md` defines phase order and delivery format. If they conflict, stop and surface the conflict; do not silently reconcile it.

Deliver one phase-sized vertical slice at a time. Do not start a later phase or build speculative features before the current phase gate/DoD is satisfied.

## Routing

Global default remains one Terra worker for bounded implementation or exploration. Use at most two Terra workers only for independent, non-overlapping work. Sol owns architecture, ambiguity, integration, and all mandatory gates below.

| Level | Change class | Required gates |
|---|---|---|
| Q0 | docs, formatting, isolated tests with no semantic change | normal review |
| Q1 | localized parser, logging, serialization, repository helper, ordinary adapter, fixture, UI, or refactor that preserves data and financial semantics | normal review + focused validation; Sol only on escalation |
| Q2 | timestamps, candle boundaries, alignment/resampling, features or labels, leakage/splits, backtest or execution assumptions, fills, fees/slippage/funding, sizing/leverage, PnL/equity/drawdown, risk metrics, Stop/TP/trailing/partial exits, or ML/quant methodology | mandatory Sol pre-gate; Sol post-gate when semantics, methodology, or accounting changes |
| Q3 | a major phase boundary, data/execution/risk/portfolio/backtest architecture, core execution semantics, lifecycle architecture, persisted trading-history schema, immutable journal architecture, multi-exchange normalization, destructive migration, or large cross-module refactor | mandatory Sol pre-gate and post-gate |

## Quant invariants

- Preserve explicit instrument identity, source/dataset semantics, units, time zones, event time, ingestion time, and knowledge time. Never invent a multiplier, unit, field meaning, or historical availability.
- Keep raw and normalized data distinct; version schemas and transformations; record gaps instead of silently dropping them; retain provenance and append-only manifests.
- Use backward point-in-time/as-of joins. Prevent look-ahead and leakage; require purging, embargo, and walk-forward validation where modeling is introduced.
- Keep signal, decision, order, and fill timestamps distinct. Define fill price, partial fills, intrabar ordering, Stop/TP precedence, position updates, realized/unrealized PnL, and equity updates explicitly.
- Apply commissions, slippage, funding, and borrow costs at the correct event, with explicit units and signs. Do not mix grains or allow joins to multiply rows silently; do not turn missing data into future-filled information.
- Preserve immutable prediction evidence. Do not rewrite historical predictions or results. Model `NO TRADE` as a valid result; real execution is out of scope unless the user explicitly authorizes it.
- Require deterministic results for identical inputs, configuration, code, and data versions, and reject research assumptions that cannot exist in paper/live operation.
- Report probability, uncertainty, risk, fees/slippage, and accounting assumptions without unsupported claims of institutional or actor attribution.

## Evidence contract

For every Q2/Q3 change, record the governing spec sections, affected invariants, source/fixture provenance where applicable, before/after semantics, changed files, focused tests and commands, results, limitations, and next phase gate. Use `$quant-critical-review` for these gates.
