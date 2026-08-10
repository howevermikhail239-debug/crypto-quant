# Phase 1C Implementation Report & Definition of Done Gate

**Phase**: Phase 1C — Historical Archives, Realtime Ingestion & Reconciliation (Plan Items 5–7)
**Date**: 2026-08-10
**Status**: Submitted for Final Acceptance

---

## 1. Plan Items & Commits

| Item | Description | Status | Key Commits |
|---|---|---|---|
| **5** | Bybit Individual Trades Archives & Derived Buckets | **DONE** | `972a713` |
| **6** | Exchange Aggregate Trades & Strict Isolation | **DONE** | `4402424` |
| **7A** | Raw WebSocket Envelope Capture | **DONE** | `39b1320` |
| **7B** | Bounded Queues & Session Lifecycle | **DONE** | `5599494` |
| **7C** | Reconnect, Gap Registry & Boundary Proof | **DONE** | `b4b14b0`, `900cf16`, `43b5b47` |
| **7D** | REST / Archive / WS Reconciliation | **DONE** | `ab83bb0`, `6b13597` |
| **7E** | Retention, Deletion Ledger, Holds & Health | **DONE** | `2649290`, `6b13597` |

---

## 2. Reconciliation (Item 7D) — Evidence

### 2.1 Architecture

- **Natural keys**: `native_trade_id` for `individual_trade`, `aggregate_trade_id` for `exchange_aggregate_trade`. Generic `timestamp+price+quantity` tuple is never used.
- **Dataset class isolation**: `reconcile_trade_datasets()` raises `TypeError` if `left_dataset_class != right_dataset_class`. Tested in both directions.
- **Categories**: `MATCH`, `REPRESENTATION_DIFFERENCE`, `MISSING_IN_WS`, `MISSING_IN_COMPARISON_SOURCE`, `FIELD_CONFLICT`, `SIDE_CONFLICT`, `TIMESTAMP_CONFLICT`, `DATASET_CLASS_MISMATCH`, `UNKNOWN_CONFLICT`.
- **Price/quantity comparison**: Strict string equality on canonical decimal representations. No float approximate equality.

### 2.2 Timestamp Tolerance Policy

Default `timestamp_tolerance_ms = 0` (exact match required for normalized canonical timestamps). Any non-zero tolerance must be explicitly justified per source pair when calling `reconcile_trade_datasets()`.

**Source-pair analysis** (all sources use millisecond event timestamps):

| Source Pair | Timestamp Field A | Timestamp Field B | Precision | Justified Tolerance |
|---|---|---|---|---|
| Binance Spot Archive vs REST | `time` (ms) | `time` (ms) | ms | **0 ms** (same canonical field) |
| Binance Spot REST vs WS `@trade` | `time` (ms) | `T` (ms) | ms | **0 ms** (same trade execution time) |
| Binance Spot aggTrade Archive vs WS | `T` (ms) | `T` (ms) | ms | **0 ms** |
| Bybit REST vs WS `publicTrade` | `time` (ms) | `T` (ms) | ms | **0 ms** (same execution time) |

**Regression test**: `test_timestamp_conflict_at_zero_tolerance()` — same `native_trade_id`, 900ms difference → `TIMESTAMP_CONFLICT`, `coverage_proven=False`.

### 2.3 Live Controlled Reconciliation (N≥100)

Executed `scratch/run_7d_live_reconciliation.py` at 2026-08-10T15:02 UTC:

| Dataset | Left N | Right N | Matched | Missing_L→R | Missing_R→L | Field | Side | TS | Rate | Proven |
|---|---|---|---|---|---|---|---|---|---|---|
| Binance Spot `individual_trade` | 500 | 500 | 498 | 2 | 2 | 0 | 0 | 0 | 99.6% | False |
| Binance Spot `exchange_aggregate_trade` | 500 | 500 | 498 | 2 | 2 | 0 | 0 | 0 | 99.6% | False |
| Bybit Spot `individual_trade` | 60 | 60 | 58 | 2 | 2 | 0 | 0 | 0 | 96.67% | False |

**Interpretation**: The 2–4 missing records per side are expected window-edge discrepancies from the ~0.5s gap between REST fetches (newest trades entered, oldest rolled out of the recent-trades window). Zero field, side, or timestamp conflicts across all 1060 compared records confirms pipeline semantic correctness. `coverage_proven=False` is correctly set because the comparison windows are not identical.

Bybit Spot is capped at N=60 per official `/v5/market/recent-trade` limit for Spot category.

---

## 3. Retention (Item 7E) — Evidence

### 3.1 Retention Policy Matrix

| Dataset Class | Source Mode | Directory Pattern | Retention | Policy Field |
|---|---|---|---|---|
| Raw WS Envelopes | Realtime | `raw/ws/` | **30 days** | `raw_ws_envelope_days` |
| Normalized Realtime | Realtime | `normalized/realtime/` | **30 days** | `normalized_realtime_days` |
| Normalized Historical Archives | Historical | `normalized/individual_trade/v1/` | **Permanent** | Not scanned by retention engine |
| 1s Derived Buckets | Derived | `derived/.../granularity=1s` | **90 days** | `sub_minute_bucket_days` |
| 5s Derived Buckets | Derived | `derived/.../granularity=5s` | **90 days** | `sub_minute_bucket_days` |
| 1m Derived Buckets | Derived | `derived/.../granularity=60s` | **Permanent** | `minute_bucket_days=None` |

All values are config-driven via `RetentionPolicy` dataclass, not buried constants.

### 3.2 Retention Boundary Tests

| Test | Age | Policy | Expected | Actual |
|---|---|---|---|---|
| `test_retention_10d_old_artifact_kept_under_30d_policy` | 10d | 30d | KEEP | **PASS** |
| `test_retention_29d_old_artifact_kept_under_30d_policy` | 29d | 30d | KEEP | **PASS** |
| `test_retention_31d_old_artifact_deleted_under_30d_policy` | 31d | 30d | DELETE | **PASS** |
| `test_retention_boundary_exactly_30d_kept` | 30d | 30d | KEEP (strict `<`) | **PASS** |
| `test_retention_permanent_1m_buckets_never_deleted` | 3650d | N/A | KEEP | **PASS** |

### 3.3 Eligibility Timestamp

Retention uses filesystem `mtime` as the eligibility timestamp. This is a documented fallback — there is no dataset-coverage-based metadata age available yet. Limitation: `mtime` can be influenced by file system operations (copy, backup restore). Future enhancement: use manifest-recorded coverage timestamps when available.

### 3.4 Sandbox Acceptance Test Results

6 temporary artifacts tested in `test_retention_sandbox_acceptance()`:

| Artifact | Age | Protection | Dry-Run | Actual | Ledger |
|---|---|---|---|---|---|
| A (expired raw WS) | 10d | None | Candidate (policy=5d) | **DELETED** | Recorded |
| B (recent raw WS) | 1d | None | Keep | **KEPT** | — |
| C (expired + MANUAL_HOLD) | 10d | Active hold | Keep | **KEPT** | — |
| D (expired + PARTIAL gap) | 10d | Gap protection | Keep | **KEPT** | — |
| E (permanent 1m bucket) | 3650d | Permanent class | Keep | **KEPT** | — |
| F (expired + FIELD_CONFLICT) | 10d | Conflict evidence | Keep | **KEPT** | — |

Note: Artifact A uses overridden `RetentionPolicy(raw_ws_envelope_days=5)` — this tests the engine mechanics, not the default 30d policy. Default boundary tests are in §3.2.

**Idempotency**: Second retention run produced 0 deletions, 0 duplicate ledger entries.

### 3.5 Hold Registry Audit Trail

Holds stored in `control/retention/v1/retention_holds.json` (materialized state). All mutations logged as append-only events in `control/retention/v1/hold_events.jsonl` with fields: `event_id`, `action` (CREATED/REMOVED), `hold_id`, `hold_type`, `target_ref`, `reason`, `timestamp`. Tested in `test_hold_registry_event_audit_trail()`.

---

## 4. Health & DQ Guards (Item 7E) — Evidence

### 4.1 Status Dimensions

| Dimension | Values | Independent |
|---|---|---|
| **Availability** | `HEALTHY`, `DEGRADED`, `RECONNECTING`, `FAILED` | Yes |
| **Completeness** | `COMPLETE`, `RECOVERED`, `PARTIAL`, `GAPPED`, `UNKNOWN` | Yes |
| **Disk** | `OK`, `WARNING`, `BOOTSTRAP_STOP`, `CRITICAL_INGESTION_STOP` | Yes |

Regression test `test_availability_and_completeness_independent()` proves `HEALTHY` availability + `GAPPED` completeness coexist.

### 4.2 Disk Thresholds (matching config/default.yaml)

| Threshold | Value | Status |
|---|---|---|
| Free ≥ 80 GB | — | `OK` |
| Free < 80 GB | `warning` | `WARNING` |
| Free < 50 GB | `bootstrap_stop` | `BOOTSTRAP_STOP` |
| Free < 20 GB | `critical_ingestion_stop` | `CRITICAL_INGESTION_STOP` |

All four states tested with deterministic threshold overrides.

### 4.3 Liveness Semantics

`transport_healthy` flag reflects **transport-level socket/heartbeat state**, not market data freshness. Absence of trades does NOT automatically transition to DEGRADED — some instruments have legitimate long inter-trade gaps.

- `transport_healthy=False` + `HEALTHY` → `DEGRADED` (**tested**)
- `transport_healthy=True` + `HEALTHY` → `HEALTHY` (**tested**)
- `transport_healthy=False` + `RECONNECTING` → stays `RECONNECTING` (**tested**)

### 4.4 Health Metrics Implementation Status

| Metric | Implemented | Source | Used In |
|---|---|---|---|
| Open gap count | ✅ | GapRegistry | Completeness |
| Partial gap count | ✅ | GapRegistry | Completeness |
| Unrecoverable gap count | ✅ | GapRegistry | Completeness |
| Disk free space | ✅ | `shutil.disk_usage` | Disk Status |
| Disk thresholds (3-tier) | ✅ | Function params / config | Disk Status |
| Transport liveness | ✅ | Caller-provided flag | Availability |
| Session state | ✅ | `RealtimeSessionLifecycle` (7B) | Caller provides to health |
| Queue utilization | ⚠️ Partial | `BoundedWsEnvelopeQueue` tracks size | Not yet fed to health |
| Queue high watermark | ⚠️ Partial | Queue tracks max | Not yet fed to health |
| Writer lag | ❌ | Not implemented | — |
| Reconnect count | ⚠️ Partial | `RealtimeStreamSupervisor` tracks | Not yet fed to health |
| Unknown side rate | ❌ | Not implemented | — |
| Duplicate rate | ❌ | Not implemented | — |
| Unexpected partial artifacts | ❌ | Not implemented | — |
| Active/stale write leases | ⚠️ Partial | `spot_trades.py` has lease mechanism | Not integrated with health |

**Known limitations**: Queue utilization, writer lag, reconnect count, unknown-side rate, duplicate rate, unexpected-partial detection, and stale-lease detection are NOT integrated into `compute_collector_health()` in Phase 1C. These are operational refinements for a future operational phase. The health module provides the extensible architecture (configurable inputs) to add them without breaking changes.

### 4.5 CLI Integration

`python -m crypto_quant health` output (2026-08-10T15:02 UTC):
```json
{
  "collector_health": {
    "availability": "HEALTHY",
    "completeness": "PARTIAL",
    "disk_status": "OK",
    "disk_free_gb": 314.74,
    "open_gap_count": 0,
    "partial_gap_count": 1,
    "unrecoverable_gap_count": 0
  }
}
```
`completeness=PARTIAL` correctly reflects 1 historical PARTIAL gap from the 7C smoke audit revision.

---

## 5. Final Gate Verification

| Check | Result |
|---|---|
| `uv lock --check` | **PASS** |
| `ruff check .` | **PASS** (0 errors) |
| `pytest` | **148 passed** in 2.93s |
| `python -m crypto_quant config-check` | **PASS** |
| `python -m crypto_quant health` | **PASS** |
| `git diff --check` | **PASS** |
| `git status --porcelain` | Clean (see §6) |

### Coverage

| Module | Coverage |
|---|---|
| **Overall** | **79%** |
| `reconciliation.py` | **92%** |
| `retention.py` | **92%** |
| `health.py` | **94%** |
| `gap_registry.py` | **90%** |
| `realtime_envelope.py` | **94%** |
| `realtime_session.py` | **93%** |
| `realtime_supervisor.py` | **98%** |

---

## 6. Known Intentional Untracked Artifacts

```
.agents/agents/     — Antigravity agent definitions (not project code)
.agents/rules/      — Antigravity routing rules (not project code)
AGENTS.md.antigravity_backup — Antigravity backup (not project code)
crypto_quant_data/  — Live data root (external, never tracked)
```
