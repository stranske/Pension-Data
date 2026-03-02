# Initial Saved Analytical Views (v1)

Issue: #39

This document defines the first three canonical saved analytical views and their semantics.

## Funding Trend (`funding_trend:v1`)

Purpose:
- Provide plan-level year-over-year funding trajectory with net external cash-flow context.

Output fields:
- `plan_id`
- `plan_period`
- `funded_ratio`
- `funded_ratio_change`
- `net_external_cash_flow_usd`

Assumptions:
- `plan_period` values are fiscal-year sortable tokens (for example `FY2024`, `FY2025`).
- `benefit_payments_usd` is modeled as outflow and subtracted from net external flow.

## Allocation Peer Compare (`allocation_peer_compare:v1`)

Purpose:
- Compare a subject plan's asset-class allocations to its peer-group distribution for a selected period.

Output fields:
- `plan_id`
- `plan_period`
- `asset_class`
- `plan_allocation_pct`
- `peer_mean_pct`
- `peer_median_pct`
- `delta_vs_peer_mean_pct`

Assumptions:
- Allocation percentages are normalized ratios in `[0, 1]`.
- Peer summary excludes the subject plan.

## Holdings Overlap (`holdings_overlap:v1`)

Purpose:
- Show manager/fund overlap between a subject plan and peer plans with coverage-aware disclosure metadata.

Output fields:
- `subject_plan_id`
- `counterparty_plan_id`
- `plan_period`
- `manager_name`
- `fund_name`
- `overlap_status`
- `overlap_usd`
- `subject_disclosure_state`
- `counterparty_disclosure_state`

Disclosure semantics:
- `disclosed`: explicit position data is present.
- `known_not_invested`: explicit evidence indicates no position.
- `not_disclosed`: no reliable disclosure for the position.

Overlap interpretation:
- `overlap` is emitted only when both plans disclose values for the same manager/fund position.
- `known_not_invested` is emitted when neither side is unknown due to non-disclosure and at least one side is explicitly not invested.
- `unknown_due_to_non_disclosure` is emitted when overlap cannot be established due to missing disclosure.
