---
name: quant-critical-review
description: Gate critical changes in this crypto quant system for data semantics, methodology, accounting, and trading-risk integrity. Use before or after changes to schemas, source adapters, transformations, features, labels, backtests, models, risk logic, prediction journals, or paper-trading behavior.
---

# Quant critical review

Read the governing master specification, technical design, current phase contract, and relevant tests before judging a change. Treat the master specification as product authority, the revised technical design as architecture authority, and the phased plan as execution order.

Check: instrument and dataset identity; event, ingestion, knowledge, signal, decision, order, and fill time; source semantics and units; raw/normalized separation; append-only provenance and prediction evidence; point-in-time eligibility; missing-data treatment; join cardinality and grain; leakage, purging, embargo, and walk-forward validity; fill and intrabar assumptions; Stop/TP ordering; fees, slippage, funding, borrow costs; partial fills, position, realized/unrealized PnL and equity semantics; deterministic replay; research/live parity; and `NO TRADE` as a valid outcome.

Require evidence: affected invariants, contract/fixture provenance, before/after behavior, focused tests or reproducible commands, and known limitations. Reject invented units, silent gaps, rewritten historical predictions, exact-time cross-venue joins, and claims beyond data support.
