# HANDOFF

> *Historical Note*: Original HANDOFF.md was created during the mid-Phase-1C AI migration.
> This revision supersedes its operational status while preserving the historical fact of that migration.

## A. PROJECT PURPOSE
**Local Crypto Quant & Opportunity System**
**Goal:** A statistically reproducible local quantitative research platform.
(Not a price-guessing bot and not a live-money trading bot.)

## B. SOURCE-OF-TRUTH PRIORITY
1. Current Git repository
2. Committed tests
3. Committed source contracts / schemas
4. Committed code
5. Manifests / checkpoints / immutable evidence
6. Phase reports
7. `crypto_quant_master_spec.md`
8. `crypto_quant_revised_technical_design.md`
9. `crypto_quant_phased_development_prompt.md`
10. `HANDOFF.md`
*(If handoff contradicts repository — repository wins.)*

## C. CURRENT GIT STATE
- **Branch:** master
- **HEAD:** 302b8d7
- **Working Tree:** clean (after this handoff commit)
- **Test Count:** 215 passed

**Latest important commits:**
- `302b8d7` — Phase 1D.3B independent acceptance
- `34d52ea` — Bybit ETHUSDT liquidation parity
- `735544c` — Phase 1D.3A final semantic closure
- `79fffb0` — Phase 1D.3A acceptance: source contract / dedup / quarantine
- `e8e679d` — initial Bybit BTCUSDT liquidation pilot
- `2318e65` — Phase 1D.2 final acceptance: immutable OI generations and provenance
- `d2152e8` — Phase 1D.2 remediation

## D. PHASE STATUS
- PHASE 0: **DONE**
- PHASE 1A (Binance OHLCV): **DONE**
- PHASE 1B (Bybit OHLCV): **DONE**
- PHASE 1C (Trades): **FINAL DONE**
- PHASE 1D.1 (Funding): **FINAL DONE**
- PHASE 1D.2 (Open Interest): **FINAL DONE / ACCEPTED**
- PHASE 1D.3A (Bybit Linear BTCUSDT Liquidations): **FINAL DONE / ACCEPTED**
- PHASE 1D.3B (Bybit Linear ETHUSDT Liquidations): **FINAL DONE / ACCEPTED**

**Next Authorized Step (and ONLY step allowed next):**
- **PHASE 1D.3C**: Binance USD-M BTCUSDT Liquidations

## E. DATA LOCATIONS
- **Repository:** `C:\Users\Admin\Documents\ChatGPT\анализ крипты`
- **External market/control data:** `C:\crypto_quant_data` (Not in Git)
- **Evidence for liquidation source/archive audit:** `C:\crypto_quant_data\evidence\phase1d3_audit`
- **Evidence Index:** `phase1d3_audit_evidence_index.json` (Currently 20 artifacts, 20/20 hash-valid, 0 broken)

## F. CRITICAL GLOBAL INVARIANTS
- UTC canonical storage
- event_time != knowledge_time
- retrieved_at != historical knowledge_time
- source-contract first
- fixture gate before scale-up
- immutable raw artifacts
- immutable normalized generations
- manifest/checkpoint/hash lineage
- fail closed on unknown semantics
- no silent dataset-class fallback
- no guessed units
- no guessed taker/position side
- DQ can disable signals
- NO_TRADE is valid

## G. OI ACCEPTED INVARIANTS
**Binance OI:**
- Historical official source has rolling window.
- Local history: must preserve data after it leaves source window + accumulate forward.
- Cannot overwrite accumulated history with current rolling month.

**Bybit OI:**
- Current 200k observations represent a `PARTIAL_CONFIGURED_BOOTSTRAP`, not full history (due to max_pages=1000, limit=200).

**OI Storage:**
- Normalized: immutable generations. Old accepted generation bytes never change.
- Historical OI: `knowledge_time = NULL` until explicit admissibility methodology is added.

## H. LIQUIDATIONS 1D.3A ACCEPTED CONTRACT
- **Normative source contract:** `schemas/contracts/bybit_linear_all_liquidation_ws_v1.yaml`
- **Active topic:** `allLiquidation.{symbol}`
- **Semantics:**
  - `T` = event_updated_time
  - `p` = bankruptcy_price
  - `S` = Buy → LONG position liquidated
  - `S` = Sell → SHORT position liquidated
  - `v` = executed size
- **Quantity mapping:** For BTCUSDT, base BTC instrument-specific mapping is accepted, verified by official evidence.
- **Completeness:**
  - `source_claimed_completeness` = ALL_LIQUIDATIONS
  - `delivery_semantics` = BATCHED_500MS_PUSH
  - (Do not use old `UNTHROTTLED_EVENT_STREAM`)

## I. DEDUP GUARANTEE
- `native liquidation event ID` = absent
- `message_id` = SHA-256 exact raw WS message/envelope
- Identity preservation = `message_id` + `event_index`
- `dedup_guarantee` = EXACT_WIRE_REPLAY_ONLY (protects against exact duplicate delivery)
- Two identical-content events inside one batch MUST preserve multiplicity.
- **cross-envelope economic-event dedup = NOT GUARANTEED.** Do not add heuristic dedup without source proof.

## J. SYNTHETIC CONTAMINATION HISTORY
- During early 1D.3A, a synthetic test batch was accidentally written to `C:\crypto_quant_data`.
- It was quarantined with audit trail preserved.
- **Invariant:** active synthetic liquidation rows = 0.
- **Rule:** Quarantine evidence must not be deleted.
- **Rule:** Synthetic fixtures going forward must only use `tmp_path` / temporary test root.

## K. HISTORICAL LIQUIDATION SOURCE STATUS
**NO VERIFIED OFFICIAL HISTORICAL LIQUIDATION BOOTSTRAP SOURCE FOUND IN DOCUMENTED/TESTED PUBLIC LOCATIONS.**
*(Do not claim "historical liquidation data does not exist" globally.)*
Until a verified source exists, missed realtime WS events are UNRECOVERABLE / UNKNOWN.

## L. REAL LIVE EVENT STATUS
- BTC bounded WebSocket soak tests PROVED: connection, subscription ACK, heartbeat ping/pong, quiet-period liveness.
- Genuine BTC liquidation in short windows = NO. (This is NOT a blocker).
- Real production event lineage is DEFERRED_TO_PHASE_1D3F (soak/gap/DQ/reconciliation). Do not run infinite market event waits now.

## M. STRATEGY ARCHITECTURE RESERVATION
- **Critical Long-Term User Invariant:** The system in Phases 2–4 must support N independently versioned strategies without global redesign.
- **Model Role:** The ML model must NOT own entry, stop-loss, take-profit, or lifecycle exit logic.
- **Target Pipeline:**
  DATA → FEATURES → MODELS → DECISION CONTEXT → STRATEGY LAYER → RISK ENGINE → PORTFOLIO ALLOCATION → BACKTEST / PAPER → LIFECYCLE
- **Strategy Composability:**
  Strategies will compose of independent policies (Eligibility, Entry, Invalidation, Stop, Target, Holding, Lifecycle, PositionIntent).
- **Control:** Architecturally unlimited does NOT mean statistically uncontrolled search. Multiple testing will be controlled by an Experiment Registry later.
- **CRITICAL IMPERATIVE:** DO NOT IMPLEMENT STRATEGY ENGINE NOW. This is purely an architectural reservation.

## N. NEXT TASK FOR CODEX
**NEXT: PHASE 1D.3C — Binance USD-M BTCUSDT Liquidations**
- **Main Goal:** Implement a source-contract-first, symbol-specific Binance USD-M BTCUSDT liquidation vertical slice without importing Bybit completeness or field semantics.
- **Checklist:**
  - current official WebSocket routing and migration evidence
  - latest-versus-largest 1000 ms selection conflict
  - public stream versus private USER_DATA history separation
  - precise q/l/z, p/ap, E/T and side semantics
  - canonical Binance USD-M BTC identity
  - source-selection incompleteness as first-class DQ
  - wrong-symbol fail-closed
  - immutable raw
  - immutable normalized generations
  - manifest/checkpoint lineage
  - bounded BTC WebSocket transport test
- **Do not start Binance ETHUSDT, long soak, features, signals, strategies, or general refactoring.**

## O. VALIDATION COMMANDS FOR NEXT AGENT
Run these exactly as specified:
```powershell
.\.venv\Scripts\python.exe -m pytest -q
.\.venv\Scripts\python.exe -m ruff check .
.\.venv\Scripts\python.exe -m crypto_quant config-check
.\.venv\Scripts\python.exe -m crypto_quant health
& 'C:\Users\Admin\AppData\Local\Microsoft\WinGet\Packages\astral-sh.uv_Microsoft.Winget.Source_8wekyb3d8bbwe\uv.exe' lock --check
git diff --check
git status --short --branch
```

## P. AI ENVIRONMENT PORTABILITY
- **Primary repository instructions:** `AGENTS.md`, the normative specifications, committed contracts/tests, and this handoff.
- **Antigravity project agents:** PRESERVED IN GIT under `.agents/agents/` and `.agents/rules/`.
- **Antigravity backup:** `C:\crypto_quant_data\migration_backup\antigravity_agents\pre_codex_20260811\`
- **Integrity manifest:** `C:\crypto_quant_data\migration_backup\antigravity_agents\pre_codex_20260811\integrity_manifest.json`
- **Files:** 5 project-scoped Antigravity agent/rule files.
- **Hash validation:** 5/5 PASS against the sanitation backup and the restored repository files.
- **Restore target:** repository-relative `.agents/agents/` and `.agents/rules/`, preserving the manifest paths exactly.
- **Codex-specific instructions:** tracked `AGENTS.md` plus the tracked `.agents/skills/quant-critical-review/` skill.
- **Important:** switching AI environments must not overwrite or delete the other environment's project-scoped instructions. Do not copy global credentials, tokens, browser state, or home-directory configuration into this repository.
