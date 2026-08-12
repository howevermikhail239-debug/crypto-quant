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
- **Accepted parent before PHASE 1E.1:** `0857da87c161e67a16ecd37fe69e40998ae58043`
- **Current implementation:** PHASE 1E.2 IMPLEMENTED / READY FOR SHORT ACCEPTANCE (see `docs/phase1e2-dq-baseline-threshold-calibration-report.md`)
- **Working Tree:** expected clean after the PHASE 1E.2 implementation commit
- **Test Count:** 269 passed

**Latest important commits:**
- `HEAD` — independent acceptance of Phase 1D.3F, including four defect fixes and regression evidence
- `12162e4` — Phase 1D.3F implementation candidate
- `b57e039` — Phase 1D.3D Binance USD-M ETHUSDT liquidation parity (accepted)
- `137ef39` — Phase 1D.3C independent acceptance: Binance liquidation provenance and completeness
- `5abcfae` — Phase 1D.3C Binance USD-M BTCUSDT liquidation pilot
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
- PHASE 1D.3C (Binance USD-M BTCUSDT Liquidations): **FINAL DONE / ACCEPTED**
- PHASE 1D.3D (Binance USD-M ETHUSDT Liquidations Parity): **FINAL DONE / ACCEPTED**
- PHASE 1D.3F (Liquidation Soak / Gaps / Source-Local DQ): **FINAL DONE / ACCEPTED**
- PHASE 1E.1 (Storage & Operational DQ Foundation): **FINAL DONE / ACCEPTED**
- PHASE 1E.2 (DQ Baseline & Threshold Calibration): **IMPLEMENTED / READY FOR SHORT ACCEPTANCE**

**Next Authorized Step:**
- short independent acceptance of PHASE 1E.2 only; longer observation, final Data Quality Report, and remaining PHASE 1E gates stay separate

## E. DATA LOCATIONS
- **Repository:** `C:\Users\Admin\Documents\ChatGPT\анализ крипты`
- **External market/control data:** `C:\crypto_quant_data` (Not in Git)
- **Evidence for liquidation source/archive audit:** `C:\crypto_quant_data\evidence\phase1d3_audit`
- **Evidence Index:** `phase1d3_audit_evidence_index.json` (28 artifacts, 28/28 hash-valid; includes the exact derived ETHUSDT metadata record)

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

## I.1 BINANCE LIQUIDATIONS 1D.3C ACCEPTED CONTRACT
- **Accepted commit:** `137ef3953704ed930034d9b44157d432d6bffaa2`
- **Normative source contract:** `schemas/contracts/binance_usdm_liquidation_ws_v1.yaml`
- **WebSocket mode:** `REQUEST_SUBSCRIBE` on `wss://fstream.binance.com/market/ws`
- **Topic:** `btcusdt@forceOrder`
- **Selection:** at most one selected observation per symbol per 1000 ms; `DOC_CONFLICT_LATEST_VS_LARGEST`
- **Completeness:** `INCOMPLETE_THROTTLED_SNAPSHOT`; never complete event tape/volume/cascade ground truth
- **Times:** `event_time=o.T`, `exchange_timestamp=E`, realtime `knowledge_time=received_at`
- **Fields:** q/l/z and p/ap remain distinct; `source_side` preserved; `position_side_liquidated=UNKNOWN`
- **Identity:** Binance USD-M BTCUSDT `ins_dae8124762a847d14263`
- **Genuine event:** one BTC observation captured; raw/Parquet/manifest/checkpoint lineage independently reconciled
- **Production invariant:** active genuine BTC rows = 1; active synthetic rows = 0

## I.2 BINANCE LIQUIDATIONS 1D.3D ACCEPTED PARITY
- **Accepted implementation:** `b57e039d2a40a8d3f1fd772e80f6dff8e5df07cc`
- **Shared adapter/contract:** the same Binance USD-M liquidation adapter and `binance.usdm.ws.liquidation-order.v1` serve BTCUSDT and ETHUSDT.
- **ETH identity:** `ins_13dce2c0972bec4044d9`; Binance/perpetual/linear-perpetual/ETH/USDT/USDT.
- **ETH units:** q/l/z resolve through canonical `identity.quantity_unit=ETH`; no symbol-specific unit branch.
- **Isolation:** wrong-symbol/topic mismatch fail closed; raw, normalized, manifest and checkpoint identities are symbol-scoped; cross-symbol exact-wire dedup collision is prevented.
- **Live transport:** connection, SUBSCRIBE ACK, heartbeat and bounded termination verified for `ethusdt@forceOrder`.
- **Genuine ETH event:** NOT YET OBSERVED / DEFERRED; no fake ETH market state or checkpoint was created.
- **BTC immutability:** accepted genuine BTC raw and Parquet hashes remained byte-identical through parity and acceptance.

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
- Bybit bounded WebSocket tests proved connection, subscription ACK, heartbeat ping/pong and quiet-period liveness; no genuine Bybit liquidation was observed in the short accepted windows.
- Binance USD-M BTCUSDT bounded Market WebSocket pilot captured and persisted one genuine liquidation observation.
- Binance USD-M ETHUSDT bounded Market WebSocket pilot verified transport/subscription/heartbeat; no genuine ETH event was observed, which is not an acceptance blocker.
- Genuine Binance BTC raw → normalized → manifest → checkpoint lineage is accepted; raw and Parquet hashes independently reconciled.
- Binance source completeness remains `INCOMPLETE_THROTTLED_SNAPSHOT`, selection rule `DOC_CONFLICT_LATEST_VS_LARGEST`.
- PHASE 1D.3F longer-soak/gap/source-local DQ responsibilities are completed and accepted. PHASE 1E.1 adds storage views and policy-aware operational DQ without redefining source completeness.

## M. STRATEGY ARCHITECTURE RESERVATION
- **Critical Long-Term User Invariant:** The system in Phases 2–4 must support N independently versioned strategies without global redesign.
- **Model Role:** The ML model must NOT own entry, stop-loss, take-profit, or lifecycle exit logic.
- **Target Pipeline:**
  DATA → FEATURES → MODELS → DECISION CONTEXT → STRATEGY LAYER → RISK ENGINE → PORTFOLIO ALLOCATION → BACKTEST / PAPER → LIFECYCLE
- **Strategy Composability:**
  Strategies will compose of independent policies (Eligibility, Entry, Invalidation, Stop, Target, Holding, Lifecycle, PositionIntent).
- **Control:** Architecturally unlimited does NOT mean statistically uncontrolled search. Multiple testing will be controlled by an Experiment Registry later.
- **CRITICAL IMPERATIVE:** DO NOT IMPLEMENT STRATEGY ENGINE NOW. This is purely an architectural reservation.

## N. CURRENT ACCEPTED CHECKPOINT
**PHASE 1E.2 — IMPLEMENTED / READY FOR SHORT ACCEPTANCE.**

- Evidence: `docs/phase1e2-dq-baseline-threshold-calibration-report.md` and external `reports/dq/phase1e2-baseline-v1.json`.
- Current baseline eligibility is `UNAVAILABLE`: 8 duplicate Binance USD-M BTCUSDT OHLCV natural keys are preserved as a fail-closed DQ finding.
- Queue/high-watermark/writer-lag and event-driven freshness thresholds remain explicitly uncalibrated where evidence is insufficient.

Previous accepted checkpoint:
**PHASE 1E.1 — FINAL DONE / ACCEPTED.**

Earlier accepted checkpoint:
**PHASE 1D.3F — FINAL DONE / ACCEPTED.**

- Implementation evidence: `docs/phase1d3f-liquidation-soak-dq-report.md`.
- Independent evidence and defect record: `docs/phase1d3f-acceptance-report.md`.
- Accepted external run: `liq_soak_96d04877a3d64202807ac752e8976208`.
- Audit fixes: explicit ACK evidence, durable flush on disconnect, fail-closed Bybit side semantics, and rejection of nonexistent run reconciliation.
- PHASE 1E.1 evidence: `docs/phase1e1-storage-operational-dq-report.md`.
- Final validation: 262 tests PASS; Ruff/config/lock/data-root immutability audit PASS.
- Short independent acceptance: PASS; no defect found and production code unchanged.
- **Do not begin the next PHASE 1E sub-slice, features, models, Telegram, paper/live execution, or final threshold calibration without a separate approved gate.**

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
