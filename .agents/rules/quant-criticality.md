---
description: Crypto quant criticality gates (Q0-Q3), data semantics, methodology, and risk control rules
trigger: model_decision
---

# Crypto Quant Criticality & Methodology Rules

## Q0 - Q3 Change Classification & Gate Matrix

### Q0: Documentation & Formatting
- Scope: Docs, typos, renames without semantic impact, formatting, isolated non-semantic test updates.
- Antigravity Model: **Gemini 3.6 Flash Low** (or `flash_lite` / `flash`).
- Gate: Normal review. No Pro or Claude required.

### Q1: Localized & Mechanical Changes
- Scope: Parsers, logging, serialization, repository helpers, ordinary adapters, fixtures, routine refactoring preserving data & financial semantics.
- Antigravity Model: **Gemini 3.6 Flash Medium** (or Flash High for complex refactor).
- Gate: Normal review + focused validation. Pro only on escalation.

### Q2: Financial Semantics, Resampling & Features
- Scope: Timestamps, candle boundaries, alignment/resampling, features/labels, leakage/splits, backtest/execution assumptions, fills, fees/slippage/funding, sizing/leverage, PnL/equity/drawdown, risk metrics, Stop/TP precedence, ML methodology.
- Antigravity Execution:
  1. **Pre-Gate**: Gemini 3.1 Pro Low (`pro` subagent) defines invariants, semantics & acceptance criteria.
  2. **Execution**: Gemini 3.6 Flash High/Medium (`flash` subagent).
  3. **Post-Gate**: Gemini 3.1 Pro Low/High (`pro` subagent) when accounting/semantics change.

### Q3: Architecture, Core Lifecycle & Migrations
- Scope: Major phase boundaries, data/execution/risk/portfolio architecture, core execution semantics, trading history schema, immutable journal architecture, multi-exchange normalization, destructive migrations, large cross-module refactors.
- Antigravity Execution:
  1. **Pre-Gate**: Gemini 3.1 Pro High (`pro` subagent) sets overall design & invariants.
  2. **Execution**: Gemini 3.6 Flash High/Medium (`flash` subagent).
  3. **Post-Gate**: Gemini 3.1 Pro High (`pro` subagent).
  4. *Optional*: 1x Claude Sonnet 4.6 Thinking adversarial review if material uncertainty exists.

## Live Capital / Production Policy
Any change affecting order submission, cancel/replace, position sizing, leverage, liquidations, max loss, circuit breakers, emergency stop, exchange credentials, permissions, or live risk controls is treated as **Minimum Q3**:
- Mandatory: Pro High pre-gate → Flash execution → deterministic tests/paper verification → Pro High post-gate.
- Scarce independent audit via Claude Sonnet allowed before production activation; Opus only for unresolvable critical ambiguity.
