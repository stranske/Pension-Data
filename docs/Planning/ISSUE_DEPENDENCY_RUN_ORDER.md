# Pension-Data Issue Dependency Run Order

Last updated: 2026-03-02

This document maps execution order and dependencies for open issues in
`stranske/Pension-Data` (EPIC 01 through EPIC 05 and their child issues).

## 1) Dependency Principles

- EPIC issues (`#12`, `#19`, `#26`, `#33`, `#40`) are planning umbrellas.
- Child issues are the implementation units to run.
- The project has one critical path:
  - foundation ingestion -> extraction/modeling -> entity intelligence -> query layer -> quality hardening
- Some tracks can run in parallel once prerequisites are met.

## 2) Child-Issue Dependency Matrix

### EPIC 01 (Foundation)

- `#13` Registry bootstrap  
  - Depends on: none
- `#14` Source-map schema/validation  
  - Depends on: `#13`
- `#15` Discovery + inventory survey  
  - Depends on: `#14`
- `#16` Immutable ingestion + checksum/supersession  
  - Depends on: `#15`
- `#17` Coverage-gap outputs  
  - Depends on: `#15`, `#16`
- `#18` Foundation observability + e2e + runbook  
  - Depends on: `#14` (start), finalize after `#15`, `#16`, `#17`
- `#51` Source authority quality gates + extraction-readiness outputs
  - Depends on: `#14`, `#15`, `#17`

### Evidence-driven amendments (2026-03-02)

- `#14` Source-map schema/validation must add:
  - source authority tier (`official`, `official-mirror`, `high-confidence-third-party`)
  - source-to-plan identity consistency checks
  - mismatch quarantine reasons (`wrong_plan`, `stale_period`, `non_official_only`)
- `#15` Discovery/inventory must add:
  - official annual-report resolution status per plan-year
  - inventory flags for manager-level holdings availability and consultant disclosure presence
- `#17` Coverage-gap outputs must add:
  - unresolved-official-source counts
  - source-mismatch rates and stale-period rates by cohort
  - machine-readable "ready vs blocked" extraction status per plan-year
- `#20` Bitemporal schema must explicitly include:
  - `plan_period` and benchmark versioning entities
  - manager/fund/vehicle relationship tables with partial-completeness flags
  - consultant engagement/recommendation attribution tables
  - plan cash-flow facts (AUM, employer/employee inflows, benefit/refund outflows)
- `#22` Investment extraction must explicitly include:
  - manager/fund position extraction when disclosed
  - manager lifecycle event inference (`entered`, `exited`, `still_invested`) with confidence + evidence
  - consultant mention/recommendation extraction when disclosed
- `#31` and `#39` must treat manager overlap as "coverage-aware":
  - expose completeness metadata so non-disclosing plans do not look like true non-holders
- `#42` SLA metrics must include:
  - citation density (target >= 1 cited fact per 10 pages, corpus average)
  - source mismatch and unresolved official-source rates
  - manager-level coverage and consultant-disclosure coverage by cohort

### EPIC 02 (Core schema + extraction)

- `#20` Bitemporal schema + dual reporting  
  - Depends on: `#16` (recommended), can be scaffolded after `#13`
- `#21` Funded/actuarial extraction  
  - Depends on: `#20`, `#16`
- `#22` Investment extraction (allocations/manager/fees)  
  - Depends on: `#20`, `#16`
- `#23` Page-level evidence/provenance linkage  
  - Depends on: `#20`; finalize with `#21`, `#22`
- `#24` Confidence routing + review queue integration  
  - Depends on: `#20`, `#21`, `#22`
- `#25` Fallback parser chain + golden extraction harness  
  - Depends on: `#21`, `#22`, `#24` (and integrates with `#23`)
- `#47` Manager/fund lifecycle extraction + coverage-aware disclosure semantics
  - Depends on: `#20`, `#22`, `#23`
- `#48` Consultant engagement + recommendation + attribution extraction
  - Depends on: `#20`, `#22`, `#23`
- `#49` AUM + sponsor cash-flow extraction + net-flow metrics
  - Depends on: `#20`, `#21`, `#23`
- `#50` Derivatives + securities-lending risk disclosure extraction
  - Depends on: `#20`, `#22`, `#23`

### EPIC 03 (Entity intelligence)

- `#27` Canonical entity registry  
  - Depends on: `#20` (schema alignment)
- `#28` Alias ingestion + candidate matching  
  - Depends on: `#27`, `#22`
- `#29` Lineage graph/events  
  - Depends on: `#27`
- `#30` Entity review queue workflow  
  - Depends on: `#27`, `#28`, `#24`
- `#31` Cross-plan entity lookup views  
  - Depends on: `#27`, `#28`, `#29`, `#22`, `#23`
- `#32` Entity regression suite + golden fixtures  
  - Depends on: `#28`, `#29`, `#30`, `#31`

### EPIC 04 (Query/API layer)

- `#34` API key auth + scoped authorization  
  - Depends on: none (can start after foundation conventions are set)
- `#35` Audited SQL endpoint  
  - Depends on: `#34`, `#20`
- `#36` Read-only NL-to-SQL (LangChain + LangSmith)  
  - Depends on: `#34`, `#35`
- `#37` Metric history API  
  - Depends on: `#35`, `#20`, `#23`
- `#38` Citation-ready export service  
  - Depends on: `#35`, `#23`, `#37`
- `#39` Initial saved analytical views  
  - Depends on: `#35`, `#37`, `#31`

### EPIC 05 (Operations/quality hardening)

- `#41` Cadence detector + adaptive scheduler  
  - Depends on: `#15`, `#16`
- `#42` SLA metrics + telemetry  
  - Depends on: `#16`, `#21`, `#22`, `#24`
- `#43` Anomaly detection + routing  
  - Depends on: `#21`, `#22`, `#24`, `#37`
- `#44` Parse replay harness + baseline diff  
  - Depends on: `#21`, `#22`, `#25`
- `#45` CI quality gates (replay + SLA)  
  - Depends on: `#42`, `#43`, `#44`
- `#46` Operator runbooks + diagnostics  
  - Depends on: can start anytime; finalize after `#41`-`#45`

## 3) Recommended Run Order (Execution Waves)

### Wave A - Foundation Core

1. `#13`
2. `#14`
3. `#15` and `#18` (start in parallel)
4. `#16`
5. `#17`
6. `#51`
7. `#18` (finalize)

### Wave B - Schema + Extraction Core

8. `#20`
9. `#21` and `#22` (parallel)
10. `#23` and `#24` (parallel once 21/22 stabilize)
11. `#47`, `#48`, `#49`, and `#50` (parallel after `#23`)
12. `#25`

### Wave C - Entity Intelligence

13. `#27`
14. `#28` and `#29` (parallel)
15. `#30`
16. `#31`
17. `#32`

### Wave D - Query/API Delivery

18. `#34`
19. `#35`
20. `#36` and `#37` (parallel)
21. `#38`
22. `#39`

### Wave E - Ops/Quality Hardening

23. `#41`
24. `#42` and `#44` (parallel), then `#43`
25. `#45`
26. `#46` (finalize)

## 4) Parallelization Notes

- Best parallel lanes after Wave B:
  - Lane 1: EPIC 03 child issues
  - Lane 2: EPIC 04 child issues through `#35`
- EPIC 05 should begin only after stable extraction (`#21`/`#22`/`#23`/`#47`-`#50`/`#25`) to avoid noisy quality baselines.

## 5) Minimal Gating Checkpoints

- Gate 1 (post-foundation): `#13`-`#18` complete before deep extraction rollout
- Gate 1a (source quality): `#51` complete before broad extraction rollout
- Gate 2 (post-extraction): `#20`-`#25` plus `#47`-`#50` complete, including manager lifecycle + consultant schema contract, before entity and query expansion
- Gate 3 (pre-production query): `#27`-`#39` complete
- Gate 4 (readiness): `#41`-`#46` complete
