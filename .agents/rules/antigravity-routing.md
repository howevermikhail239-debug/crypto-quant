---
description: Model tier routing, Flash-first execution, Pro escalation ladder, quota awareness, and Claude reserve policy
trigger: always_on
---

# Antigravity Model & Quota Routing Policy

## Model Selection Principles

### Enforceable Agent Model Tiers (Subagent API)
Antigravity subagent invocation schema supports enum tiers: `['inherit', 'flash_lite', 'flash', 'pro']`.
- Custom `terra` worker maps to `flash`.
- Custom `sol` architect maps to `pro`.
- Micro-variants (`low`, `medium`, `high`, `sonnet`, `opus`) are advisory session policy guidelines and are selected via main model selector / UI settings.

### Desired Model Ladder & Tier Mapping
1. **Tier 0 (Gemini 3.6 Flash Low)**: Typos, formatting, renames, simple git/file reads, doc edits, mechanical changes.
2. **Tier 1 (Gemini 3.6 Flash Medium) - Default Executor**: Primary worker for coding, bug fixes, features, tests, ordinary refactoring, SQL, codebase analysis.
3. **Tier 2 (Gemini 3.6 Flash High) - Strong Executor**: Multi-file bugs, deeper local reasoning, complex SQL/integration, retries after Flash Medium miss, execution of approved Q2/Q3 architecture.
4. **Tier 3 (Gemini 3.1 Pro Low) - Bounded Architecture**: Setting contracts, invariants, acceptance criteria, evaluating ambiguity/conflicting evidence. Hand off execution back to Flash.
5. **Tier 4 (Gemini 3.1 Pro High) - Highest Value Architect**: Core PnL/accounting semantics, execution/risk engine architecture, leakage/ML validation, destructive migrations, Q3 final gate.

### Reserve & Reviewer Policy
- **Gemini 3.5 Flash (Reserve Pool)**: Secondary projects, low-priority research, routine tasks to preserve 3.6 quota when separate quota buckets exist.
- **Claude Sonnet 4.6 Thinking (Scarce Reviewer)**: Scarce independent adversarial review for critical Q3 design, capital-at-risk validation, or challenging approved Pro decisions.
- **Claude Opus 4.6 Thinking (Emergency Red-Button)**: Final independent audit / tie-breaker before live execution or unresolvable Pro vs Sonnet conflicts.

### Quota Awareness & Parallelism
- Check quota status before heavy agentic sessions.
- Limit concurrent subagents to maximum 2 non-overlapping workers.
