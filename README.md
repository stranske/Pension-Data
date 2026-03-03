# Pension-Data

Pension-Data is a Python codebase for extracting, normalizing, and validating pension-report facts, with CI and agent workflows for high-throughput issue/PR execution.

## Current Scope

- Data extraction modules for funded status, actuarial, governance, and investment domains.
- Normalization and unit harmonization utilities.
- Registry/discovery/source-quality checks.
- Query services for saved views and metric history.
- Quality, replay, SLA, and contract-style test coverage.

## Quick Start

```bash
git clone https://github.com/stranske/Pension-Data.git
cd Pension-Data
pip install -e ".[dev]"
```

Run local checks:

```bash
ruff check src tests
black --check src tests
mypy src
pytest -q
```

## Database Strategy

- Local development/tests default to SQLite (no DB server required).
- Production/shared workloads target PostgreSQL.
- The strategy/config helpers live in `src/pension_data/db/strategy.py`.

See [docs/DATABASE_SETUP.md](docs/DATABASE_SETUP.md) and
[docs/adr/ADR-0001-database-strategy.md](docs/adr/ADR-0001-database-strategy.md).

## Repository Layout

```text
.github/workflows/        CI and agent automation workflows
apps/web/                 Cloudflare Pages web app scaffold
apps/mac-desktop/         Packaged macOS desktop app scaffold
src/pension_data/         Application modules
tests/                    Unit/integration/contract tests
docs/                     Runbooks, guides, issue formatting, ops notes
scripts/                  Utility scripts and LangChain-assisted tooling
```

## Workflow Notes

- `Gate` is the primary PR validation workflow.
- `Autofix` is triggered from failed CI contexts (not from every push).
- `Agents Gate Followups` evaluates follow-up execution after gate runs.

## Roadmap Gaps

Two intentionally incomplete components are tracked:

- A higher-quality analyst-facing UI/GUI.
- A LangChain interaction layer for findings exploration from repo outputs.
- A packaged Mac desktop app path (scaffold now present under `apps/mac-desktop/`).

See [docs/UI_LANGCHAIN_OPTIONS.md](docs/UI_LANGCHAIN_OPTIONS.md) for deployment options and tradeoffs under your environment constraints.

## Additional Docs

- [docs/AGENT_ISSUE_FORMAT.md](docs/AGENT_ISSUE_FORMAT.md)
- [docs/CI_SYSTEM_GUIDE.md](docs/CI_SYSTEM_GUIDE.md)
- [docs/KEEPALIVE_TROUBLESHOOTING.md](docs/KEEPALIVE_TROUBLESHOOTING.md)
- [docs/deploy/CLOUDFLARE_PAGES_SETUP.md](docs/deploy/CLOUDFLARE_PAGES_SETUP.md)
- [docs/ops/QUALITY_LAYER_OPERATIONS.md](docs/ops/QUALITY_LAYER_OPERATIONS.md)
- [docs/contracts/DOCUMENT_ORCHESTRATION_JOB_CONTRACT.md](docs/contracts/DOCUMENT_ORCHESTRATION_JOB_CONTRACT.md)
- [docs/DATABASE_SETUP.md](docs/DATABASE_SETUP.md)
- [docs/adr/ADR-0001-database-strategy.md](docs/adr/ADR-0001-database-strategy.md)

## License

MIT (see `LICENSE`).
