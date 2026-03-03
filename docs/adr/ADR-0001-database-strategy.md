# ADR-0001: SQLite Local, PostgreSQL Production

- Status: Accepted
- Date: 2026-03-03
- Related issue: #128

## Context

Pension-Data currently favors in-memory and file-based SQLite test flows.
The project now needs a production-ready shared database path without making local
onboarding heavy or requiring a local DB server.

## Options Considered

1. Keep SQLite everywhere.
2. Use MySQL for production and SQLite locally.
3. Use PostgreSQL for production and SQLite locally.

## Comparison (PostgreSQL vs MySQL)

- Analytical SQL features:
  PostgreSQL has stronger support for advanced CTE/query planning patterns commonly
  used in auditing and time-based analytics. MySQL supports CTEs/window functions,
  but PostgreSQL remains more predictable for complex analytical workloads.
- JSON/evidence storage:
  PostgreSQL `JSONB` indexing/querying is stronger for evidence payload expansion.
  MySQL JSON support is workable but less flexible for this roadmap.
- Type system + migration ergonomics:
  PostgreSQL offers robust temporal and custom type semantics that map well to
  bitemporal/lineage-oriented schemas. MySQL can do this, but with more caution
  around type behavior and portability.
- Team/local simplicity:
  Both are free/open-source. SQLite remains the simplest zero-server local path.

## Decision

Adopt **SQLite for local development/tests** and **PostgreSQL for production/shared** workloads.

## Consequences

- Positive:
  - Local setup stays zero-server and fast for contributors.
  - Production path gains stronger analytics and JSONB capabilities.
  - Dialect split is explicit and test-covered.
- Negative:
  - Two-dialect maintenance overhead.
  - Postgres-only migration scripts must be maintained alongside SQLite scripts.

## Implementation Notes

- Add a DB strategy config layer (`src/pension_data/db/strategy.py`) to resolve
  dialect and connection requirements.
- Maintain dialect-specific migration sequences in `src/pension_data/db/migrations/`.
- Keep query service read-only and dialect-aware for pagination placeholder syntax.
- Preserve SQLite defaults for local and CI workflows.
