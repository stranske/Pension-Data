# Entity Linkage Contract (One-PDF Components)

This contract defines canonical entity-link fields emitted in extraction component datasets.

### Scope

- Investment manager/fund positions (`PlanManagerFundPosition`)
- Manager lifecycle events (`ManagerLifecycleEvent`)
- Consultant entities/engagements/recommendations/attribution observations

### Canonical ID Rules

- `manager_canonical_id`: `manager:<normalized manager token>`
- `fund_canonical_id`: `fund:<normalized manager token>:<normalized fund token>` when manager is known; else `fund:<normalized fund token>`
- `consultant_canonical_id`: `consultant:<normalized consultant token>`
- Non-disclosed consultant rows use a scoped sentinel:
  - `consultant:not_disclosed:<normalized plan_id>:<normalized plan_period>`

Normalization uses the shared `normalize_entity_token(...)` helper to keep IDs deterministic.

### Linkage Status Rules

- `resolved`: disclosed name with deterministic canonical ID.
- `ambiguous`: extractor detected naming ambiguity for the row/group.
- `not_disclosed`: disclosure missing; sentinel ID emitted for analyzable joins.

### Consumer Expectations

- Join across component datasets using canonical IDs first, not free-text names.
- Treat `linkage_status != resolved` as low-confidence linkage.
- Keep warnings alongside linkage fields; warnings explain why linkage is ambiguous or non-disclosed.
