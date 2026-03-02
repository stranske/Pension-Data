# Foundation Ingestion Runbook

Last reviewed: 2026-03-02
Incident scope: source-map breakage, robots restrictions, and revised-file anomalies

## Purpose

Use this runbook when the foundation fixture pipeline (`registry -> source-map -> discovery -> ingestion -> coverage`) fails in CI or local operator runs.

## Failure Classes

- `source_map_breakage`: source-map load/validation fails before discovery starts.
- `robots_restriction`: discovery is blocked by robots or site-access controls.
- `revised_file_anomaly`: ingestion sees revised lineage references that do not map to discovered artifacts.
- `discovery_data_error`: discovery payload mismatch (plan/domain/schema mismatch).
- `ingestion_data_error`: malformed ingestion payload or missing discovered dependencies.

## Diagnostic Commands

```bash
python tools/foundation/run_fixture_pipeline.py \
  --fixture tests/e2e/foundation/fixtures/foundation_fixture_success.json \
  --output-root artifacts
```
Expected signal: command exits `0` and emits `artifacts/foundation/latest_run_ledger.json`.

```bash
python - <<'PY'
import json
from pathlib import Path
ledger = json.loads(Path("artifacts/foundation/latest_run_ledger.json").read_text())
print(ledger["status"], ledger["failures"])
PY
```
Expected signal: `status` and `failures` identify the failing stage and category.

```bash
pytest -q --no-cov tests/ops/test_foundation_ledger.py tests/e2e/foundation/test_fixture_pipeline.py
```
Expected signal: tests pass and validate taxonomy + fixture e2e artifact expectations.

## Remediation Steps

1. Inspect `artifacts/foundation/latest_run_ledger.json` and capture the first failure row (`stage`, `category`, and `message`).
2. If category is `source_map_breakage`, validate the source-map fixture/seed headers and values using `python -m pension_data.sources.lint <path>`.
3. If category is `robots_restriction`, confirm the failing URL host and either add an approved mirrored source or update crawl policy exceptions.
4. If category is `revised_file_anomaly`, reconcile `revised_of_source_url` links so every revised ingestion item points to a discovered base artifact.
5. Re-run the fixture pipeline command and confirm all five stages report `status=ok` with no failures.
6. Re-run targeted tests and update the PR with both the ledger outcome and artifact evidence paths.

## Verification Checklist

- `latest_run_ledger.json` reports `status: success`.
- `run_ledger.jsonl` includes the new run record.
- `coverage_readiness.json` exists and includes readiness rows.
- CI workflow `Foundation Fixture E2E` completes without errors.
