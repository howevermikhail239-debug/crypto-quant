# PHASE 1E.2 — DQ Baseline & Threshold Calibration

**Status:** IMPLEMENTED / READY FOR SHORT ACCEPTANCE

**Accepted parent:** `432d97705f30b1ddb19337a923022524d02d2800`

**Policy/profile version:** `1.0.0`

## Scope

This package extends the accepted PHASE 1E.1 health/DQ layer. It adds a reproducible, source/dataset/venue/market/instrument/contract-aware baseline; versioned threshold policies; deterministic machine-readable evaluation; and fail-closed data eligibility. It does not add features, models, trading logic, service orchestration, or a final Phase-1 Data Quality Report.

## Measured baseline

The fixed observation timestamp is `2026-08-12T08:30:00+00:00`. The machine-readable profile is stored outside Git at `C:\crypto_quant_data\reports\dq\phase1e2-baseline-v1.json` (SHA-256 `1559e144bbe6062a4404d3ecb57e243cee0714aba81861a9f3ec2d6737e9c546`). It contains 233 metric evaluations across active datasets and all four retained liquidation-soak stream identities:

- 83 `CALIBRATED`, 104 `UNCALIBRATED`, 46 `NOT_APPLICABLE`;
- 78 `PASS`, 108 `UNKNOWN`, 46 `NOT_APPLICABLE`, 1 `FAIL`;
- overall eligibility: `UNAVAILABLE` because a calibrated uniqueness invariant failed.

Duplicate rates were measured under accepted dataset-specific keys. All individual-trade, derived-bucket, funding, and OI identities passed at zero. Eight duplicate OHLCV natural keys were found among 44,658 rows for Binance USD-M BTCUSDT (`ins_435257d0ca986721c26c`), duplicate rate `0.00017913923597115858`. They are overlapping rows in two active tail generations covering 2026-08-10 08:19–08:26 UTC. No historical object was deleted or rewritten; the finding remains fail-closed for a separately authorized remediation.

Unexpected unknown-side rates were zero for all eight individual-trade identities and for the observed Binance liquidation `source_side`. Binance liquidation `position_side_liquidated=UNKNOWN` is `NOT_APPLICABLE`, reflecting the accepted source limitation rather than a parser defect.

GapRegistry evaluation found no unresolved gap for the active identities in this profile. Market-event inactivity itself created no gap.

All four accepted liquidation soak streams report `LOW_ACTIVITY_QUIET` with passing transport evidence. Their event-age thresholds remain uncalibrated, exact-wire duplicate rates remain unknown where zero messages were observed, and synchronous queue/high-watermark/writer-lag metrics are not applicable.

## Calibration policy

Calibrated policies are limited to source-semantic invariants:

- proven natural/native keys: maximum duplicate rate `0.0`; any violation makes the affected data unavailable;
- source-required side: maximum unexpected unknown-side rate `0.0`; any violation makes the affected data unavailable;
- GapRegistry categorical policy: `UNRECOVERABLE` makes data unavailable; `OPEN`, `PARTIAL`, or `UNKNOWN` degrades it; recovered/no unresolved gap passes.

These zero limits are contract invariants, not distribution-fitted convenience values.

Uncalibrated:

- event-driven freshness age: the short quiet liquidation soak does not establish an event-age threshold;
- queue utilization, queue high watermark, and writer lag for collectors without a retained production-quality observation distribution.

An uncalibrated applicable metric evaluates `UNKNOWN` and degrades eligibility; it never silently becomes PASS/USABLE.

Not applicable:

- operational freshness for immutable archive/bootstrap datasets;
- queue and writer metrics for the accepted synchronous liquidation soak;
- Binance liquidation position-side unknown rate.

## Active-generation correction

Baseline inspection exposed 12 obsolete derived-bucket objects whose manifests referenced superseded individual-trade hashes, including earlier Bybit generations routed under a Binance path. The catalog resolver now retains a derived generation only when its recorded source Parquet hash is active. Active artifacts changed from 96 to 84 and derived rows from 950,513 to 701,854. Source Parquet and old immutable derived objects were not mutated or deleted; they simply are no longer active catalog inputs. A regression test covers this lineage rule.

## Eligibility mapping

- `USABLE`: all applicable calibrated checks pass and no applicable threshold is unknown.
- `DEGRADED`: an applicable policy is uncalibrated/unknown or an unresolved non-terminal gap exists, with no hard failure.
- `UNAVAILABLE`: a calibrated hard invariant fails or an unrecoverable gap exists.

Eligibility concerns data fitness only. It contains no entry, strategy, risk, SL/TP, or execution logic.

## Remaining evidence and Phase 1E work

- Collect versioned queue-utilization/high-watermark/writer-lag distributions under representative sustained realtime load before setting those thresholds.
- Keep event-driven liquidation freshness uncalibrated until transport/cadence evidence supports a source-specific policy; event silence alone is insufficient.
- Resolve the eight Binance USD-M BTCUSDT OHLCV overlap duplicates through a separate manifest/generation remediation, preserving immutable history and audit lineage.
- Threshold short acceptance, any longer operational observation, the final Data Quality Report, and remaining Phase-1E gate work are separate future packages.

Phase-8 orchestration, service runner, scheduler, alert engine, feature/label generation, models, strategies, risk, Telegram/UI, backtesting, paper/live execution, and Phase 2 were not started.

## Validation

- Targeted PHASE 1E.1/1E.2 tests: 14 passed.
- Full suite: 269 passed, 80% aggregate coverage.
- Ruff, config-check, health, uv lock check, and `git diff --check`: PASS.
- Reproducible CLI rerun produced the same profile SHA-256 and the same fail-closed eligibility result.
- Known `growth_projections=UNKNOWN` remains informational and unrelated to this package.
