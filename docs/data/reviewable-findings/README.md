# Reviewable Findings Artifact

The first static UI and LangChain artifact slice is `extraction_quality_dashboard`.

- Contract: `docs/data/reviewable-findings/findings.schema.json`
- Published artifact path: `docs/data/reviewable-findings/extraction-quality-dashboard.json`
- Python contract helper: `pension_data.langchain.review_artifact.reviewable_findings_schema()`
- Validator: `pension_data.langchain.review_artifact.validate_reviewable_findings_artifact(...)`
- Generator command: `python scripts/langchain/build_reviewable_findings_artifact.py`

The generator reads real extraction persistence and readiness outputs and derives finding rows
from that data. The required source artifact paths are passed as CLI arguments:

- `--persistence-contract` (default `extraction_persistence/persistence_contract.json`): the
  contract JSON written by `write_extraction_persistence_artifacts()` after an extraction run.
- `--readiness-csv` (default `coverage/readiness_rows.csv`): the readiness CSV
  written by `write_coverage_artifacts()` (or an equivalent source-authority readiness export).

When either source artifact is missing, unreadable, or fails to parse the generator raises
`ReviewableFindingsArtifactError` and exits non-zero; it does not silently fall back to a
hand-authored fixture. The checked-in artifact at the published path remains a stable contract
sample for static UI hosting and reviewer workflows, but it is not the production data path.
In-process callers must also provide both source paths; calling the builder without source artifacts
is treated as a contract error.

The `.github/workflows/foundation-fixture-e2e.yml` `Foundation Fixture E2E` workflow prepares those
source artifacts under `artifacts/`, runs the generator with
`artifacts/extraction_persistence/persistence_contract.json` and
`artifacts/coverage/readiness_rows.csv`, validates the generated dashboard artifact, and uploads
`artifacts/reviewable-findings/` for review.

The persistence contract is used to validate that the source data describes required readiness
columns before findings are derived from the readiness CSV. Real-data generation includes
`total_candidate_findings` and `truncated` metadata; if more than 25 usable findings are present,
the generator emits a runtime warning and marks the artifact as truncated. The `compare` LangChain
action is emitted only when at least two findings are available.

Required finding rows include `entity`, `period`, `metric_family`, `metric`, `value`, `confidence`,
`provenance_refs`, and `citations`. These are the minimum fields the static UI can filter/render and
the LangChain explain/compare chains can cite without repo-local execution.

LangChain actions are asynchronous. The artifact advertises `explain` and `compare` request shapes;
workflow or comment-driven runs write result artifacts back with request id, timestamp, summary,
citations, and an output artifact path.
