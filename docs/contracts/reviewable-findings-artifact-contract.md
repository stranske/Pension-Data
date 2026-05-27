# Reviewable Findings Artifact Contract

## Source Of Truth

- Schema artifact: `docs/data/reviewable-findings/findings.schema.json`
- First published payload path: `docs/data/reviewable-findings/extraction-quality-dashboard.json`
- Python helper: `src/pension_data/langchain/review_artifact.py`
- Contract tests: `tests/langchain/test_review_artifact_contract.py`

## First Slice

The first reviewable UI/LangChain slice is `extraction_quality_dashboard`.

This slice is intentionally narrower than the full web workspace. It reports extraction quality
findings that a reviewer can inspect in a static browser UI and ask asynchronous LangChain
explain/compare questions about.

## Artifact Envelope

Required top-level fields:

- `artifact_type`: always `pension_data.reviewable_findings`
- `schema_version`: integer contract version, starting at `1`
- `artifact_id`: stable id for one generated payload
- `generated_at`: UTC timestamp
- `source_artifact_ids`: non-empty identifiers for upstream extraction/readiness artifacts
- `slice`: metadata for the dashboard slice
- `findings`: finding rows
- `langchain_actions`: supported asynchronous interaction contracts

## Finding Rows

Each finding row must include:

- `finding_id`
- `entity`
- `period`
- `metric_family`
- `metric`
- `value`
- `confidence`
- `provenance_refs`
- `citations`

Optional `severity` values are `info`, `warning`, and `blocker`.

The required filter fields for the static UI are `entity`, `period`, `metric_family`, and
`confidence`. Citations and provenance references are mandatory so the UI can display source
tooltips and LangChain outputs can stay citation-bound.

## LangChain Actions

The artifact advertises two asynchronous actions:

- `explain`: accepts one or more `finding_ids` plus a question and writes a structured summary.
- `compare`: accepts two or more `finding_ids` plus a question and writes a structured comparison.

Outputs must include `request_id`, `generated_at`, `summary`, `citations`, and `artifact_path`.

### Chain Output vs Published Artifact

The five required output fields are enforced at the **published-artifact** layer, not on the
in-process chain step's response metadata. There are two layers in the pipeline and they hold
different invariants:

| Layer | Surface | `artifact_path` invariant |
| --- | --- | --- |
| Chain step | `ExplainResponse.metadata` / `CompareResponse.metadata` (`findings_explain.py`, `findings_compare.py`) | Initialized to `None`. The chain does not know the persistence path it will be written to; it returns transient request output. |
| Export step | `FindingsExportArtifact` built by `build_findings_export_artifact` in `findings_export.py` | The persistence bridge: takes the chain's `request_id`, `summary`, and `citations`, stamps `generated_at`, and attaches an `artifact_path` (the helper accepts `artifact_path=None`, so the non-empty invariant only applies when the export step is invoked with a concrete publication path; intermediate `FindingsExportArtifact` instances without a publication target legitimately carry `artifact_path=None`). |
| Published payload | The JSON written to the export path (and the recorded-output canaries in `tests/langchain/recorded_outputs/`) | `artifact_path` must be a non-empty string. The eval-harness schema check (`eval_harness.py`) and `tests/langchain/test_review_artifact_contract.py` both enforce this against the published payload, never against chain metadata. |

This split is intentional. The chain step is callable from request paths that do not persist
their output (e.g. unit-tested route adapters; `tests/langchain/test_findings_chains.py` asserts
the route-level `artifact_path` is `None`). The published-artifact contract only attaches once
the export persistence step runs.

If a live-runner script (`eval_harness._load_live_output` -> `live_command`) emits raw chain
metadata without first going through `build_findings_export_artifact`, the eval-harness schema
check fails with one of the literal messages
`schema invalid: findings_explain output requires non-empty string field 'artifact_path'` or
`schema invalid: findings_compare output requires non-empty string field 'artifact_path'`
(the harness emits a separate per-action message; see `eval_harness.py` checks for
`findings_explain` and `findings_compare`). The end-to-end smoke test in
`tests/langchain/test_chain_to_artifact_pipeline.py` exercises the chain → export →
eval-harness path against this contract.

See `docs/reports/reviewable-findings-contract-audit.md` for the audit that first surfaced this
two-layer model.

## Generation Plan

The first generator reads extraction persistence outputs and source-readiness artifacts, then writes
`artifacts/reviewable-findings/extraction-quality-dashboard.json` as a CI/release artifact. The
`.github/workflows/foundation-fixture-e2e.yml` `Foundation Fixture E2E` workflow prepares
`artifacts/extraction_persistence/persistence_contract.json` and
`artifacts/coverage/readiness_rows.csv`, runs
`scripts/langchain/build_reviewable_findings_artifact.py`, validates the output with
`validate_reviewable_findings_artifact(...)`, and uploads `artifacts/reviewable-findings/`.
