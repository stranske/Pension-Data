# Pension-Data Plan (Working Draft)

Last updated: 2026-03-02

## 1) Product intent and users

Primary users (v1):
- Investment-team allocators: analyze peer portfolio construction and manager usage.
- Pension senior executives: analyze choices linked to funded-status outcomes.
- Policy analysts: evaluate system-level patterns and policy implications.

v1 analytical intent:
- Approximately 50/50 descriptive + diagnostic analysis.

## 2) Coverage and source scope (v1)

Coverage target:
- 5 years of data.
- All state employee pension systems.
- Plus a sample of 50 large/medium public employee systems including:
  - county
  - municipal/general employee
  - police/fire
  - teacher/public school
  - university
  - judicial
  - special district/authority

Minimum useful system count:
- At least 10 systems at initial launch stage.

Document scope for initial ingestion:
- Start with annual reports.
- During source survey, also inventory counts of:
  - board packets
  - asset-liability studies
  - consultant reports

Report packaging:
- Sectioned vs single-file annual reports are both acceptable.

## 3) Acquisition and retention rules

Accepted decisions:
- Maintain a source map per pension system.
- Detect and handle restatements/reissued files.
- Retain replaced/superseded versions as part of immutable history.
- Build a coverage-gap dashboard.

## 4) Data priorities and granularity

Mandatory fields include:
- funded ratio
- AAL / AVA
- discount rate
- contribution rates
- market value / fiduciary net position (AUM proxy)
- employer + employee contribution dollars
- benefit payments and refunds
- valuation/effective dates
- core participant counts

Investment extraction is high priority and must include:
- portfolio asset categories (allocations)
- manager and fund-level details (especially hedge fund / PE managers)
- manager lifecycle context (new mandates, terminations/exits, remaining funds)
- fee information where disclosed
- consultant disclosures and recommendation context where disclosed

Granularity:
- manager-level is priority and should be extracted whenever disclosed
- completeness flags are required when manager-level detail is missing or partial
- security-level not required for v1

Units:
- nominal dollars

Manager/consultant intelligence requirements:
- Preserve manager, fund, and vehicle names as-reported and normalized.
- Persist relationship events (entered, exited, mandate change) when documentary evidence exists.
- Link consultant references to role (investment, actuarial, OCIO, specialty), recommendation text, and implementation status where available.
- Preserve page-level evidence for all manager lifecycle and consultant-attribution rows.

## 5) Taxonomy and entity strategy

Asset taxonomy approach:
- First collect evidence of taxonomy breadth across systems.
- After enough evidence, lock a canonical taxonomy and map source terms.

Mergers/splits handling:
- Treat as exception workflow via issues for human summary/decision.

Entity resolution decisions:
- unresolved entities go to human review queue
- alias management is required
- entity lineage graph is required

## 6) Time model and publishing model

Time keys:
- multiple time keys are necessary (different fiscal year-ends)
- support effective date and ingestion date (bitemporal tracking)
- evaluate harmonized reporting views aligned to June and/or December cut points

Publishing behavior:
- do not block publication; route uncertainty to human review queue
- include anomaly alerts
- auto-retry and alternate extractors before escalation

## 7) Query, API, and platform decisions

API:
- key-based access
- citation-ready exports required
- LangChain-based interactive structures required
- LangSmith tracing required

Dashboards requested:
- funding trend
- allocation peer comparison
- holdings overlap / manager overlap

## 8) Operations

Refresh:
- detect source publication cadence and align refresh cadence accordingly

Cost approach:
- do not set a fixed budget initially; collect data first, budget after baseline

Validation tooling:
- parse replay table/harness against a golden corpus is desired

## 9) Clarifications resolved in this draft

Why page-level evidence can matter:
- audit trail for disputed values
- faster human QA when extraction confidence is low
- citation-ready outputs for analysts/executives

Why dual reporting can matter:
- preserve "as reported" values exactly
- separately store normalized/crosswalked values for peer comparability
- avoid loss of source fidelity while enabling apples-to-apples analysis

## 10) Finalized architecture and policy decisions

Confidence routing (common setup):
- >= 0.90: auto-accept
- 0.75 to 0.89: publish with warning + queue for review
- < 0.75: queue with high-priority human review

Evidence and reporting:
- page-level evidence is required for high-impact extracted metrics
- store both as-reported and normalized values

Referential integrity:
- soft referential integrity in ingestion/staging
- strict referential integrity in curated analytics views
- never drop partial records silently; retain parse artifacts and warnings

Metric history:
- metric history API is included in v1

SLA quality:
- explicit SLA quality targets are in scope for v1 operations

NL/SQL safety:
- start with read-only generated SQL execution for NL workflows
- SQL endpoint can be expanded later by API key scope/role
