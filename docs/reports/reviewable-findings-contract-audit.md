# Reviewable Findings Contract Audit

Issue: https://github.com/stranske/Pension-Data/issues/426

## Inventory

Command:

```bash
rg -l "artifact_path|reviewable_findings|extraction_quality_dashboard|langchain_actions|required_output_fields" src/ docs/ tests/ scripts/
```

Files with reviewable-findings contract terms:

- `docs/contracts/reviewable-findings-artifact-contract.md`
- `docs/data/reviewable-findings/README.md`
- `docs/data/reviewable-findings/extraction-quality-dashboard.json`
- `docs/data/reviewable-findings/findings.schema.json`
- `docs/LANGCHAIN_FOUNDATIONS.md`
- `docs/UI_LANGCHAIN_OPTIONS.md`
- `docs/contracts/quant-data-model-contract.md`
- `scripts/langchain/build_reviewable_findings_artifact.py`
- `src/pension_data/coverage/component_completeness.py`
- `src/pension_data/api/routes/findings.py`
- `src/pension_data/langchain/__init__.py`
- `src/pension_data/langchain/eval_harness.py`
- `src/pension_data/langchain/findings_compare.py`
- `src/pension_data/langchain/findings_explain.py`
- `src/pension_data/langchain/findings_export.py`
- `src/pension_data/langchain/review_artifact.py`
- `src/pension_data/ops/foundation.py`
- `tests/e2e/foundation/test_fixture_pipeline.py`
- `tests/langchain/prompt_dataset.json`
- `tests/langchain/recorded_outputs/findings_funded_ratio_explain.json`
- `tests/langchain/recorded_outputs/findings_period_compare.json`
- `tests/langchain/test_findings_chains.py`
- `tests/langchain/test_review_artifact_contract.py`
- `tests/ops/test_one_pdf_pilot.py`
- `tests/quality/test_sla_metrics.py`

## Clause Disposition

| Clause | Disposition | Evidence | Follow-up |
| --- | --- | --- | --- |
| Source-of-truth schema, published path, helper, and tests are named in the contract. | implemented-and-tested | `docs/contracts/reviewable-findings-artifact-contract.md:3-8`; `src/pension_data/langchain/review_artifact.py:10-16`; `tests/langchain/test_review_artifact_contract.py:27-36` verifies the JSON schema equals `reviewable_findings_schema()`. | None |
| First slice is `extraction_quality_dashboard`. | implemented-and-tested | `docs/contracts/reviewable-findings-artifact-contract.md:10-16`; `src/pension_data/langchain/review_artifact.py:16,62-65,254-259`; `tests/langchain/test_review_artifact_contract.py:33-34,69-76`. | None |
| Artifact envelope requires `artifact_type`, `schema_version`, `artifact_id`, `generated_at`, `source_artifact_ids`, `slice`, `findings`, and `langchain_actions`. | implemented-and-tested | `docs/contracts/reviewable-findings-artifact-contract.md:18-29`; `src/pension_data/langchain/review_artifact.py:20-29,117-137,245-253`; `tests/langchain/test_review_artifact_contract.py:40-56`. | None |
| Finding rows require `finding_id`, `entity`, `period`, `metric_family`, `metric`, `value`, `confidence`, `provenance_refs`, and `citations`. | implemented-and-tested | `docs/contracts/reviewable-findings-artifact-contract.md:31-43`; `src/pension_data/langchain/review_artifact.py:36-46,152-196,260-284`; `tests/langchain/test_review_artifact_contract.py:58-67,145-153`. | None |
| Optional `severity` is limited to `info`, `warning`, and `blocker`. | implemented-and-tested | `docs/contracts/reviewable-findings-artifact-contract.md:45`; `src/pension_data/langchain/review_artifact.py:47,183-188`. | None |
| Static UI filter fields are `entity`, `period`, `metric_family`, and `confidence`; citations/provenance are mandatory. | implemented-and-tested | `docs/contracts/reviewable-findings-artifact-contract.md:47-49`; `src/pension_data/langchain/review_artifact.py:66-70,189-196`; `tests/langchain/test_review_artifact_contract.py:34,58-67,145-153`. | None |
| Artifact advertises asynchronous `explain` and `compare` actions. | implemented-and-tested | `docs/contracts/reviewable-findings-artifact-contract.md:51-56`; `src/pension_data/langchain/review_artifact.py:48,71-80,198-230,286-300`; `tests/langchain/test_review_artifact_contract.py:118-124`. | None |
| LangChain action outputs include `request_id`, `generated_at`, `summary`, `citations`, and `artifact_path`. | implemented-and-tested | `docs/contracts/reviewable-findings-artifact-contract.md:58`; `src/pension_data/langchain/review_artifact.py:71-80`; `src/pension_data/langchain/findings_explain.py:32-49,77-82`; `src/pension_data/langchain/findings_compare.py:33-51,79-84`; `src/pension_data/langchain/findings_export.py:15-25,47-78,81-90`; `src/pension_data/api/routes/findings.py:18-34,65-76,99-110`; `tests/langchain/test_findings_chains.py:147-167,222`; `tests/langchain/test_review_artifact_contract.py` now includes a schema canary for non-empty output fields. | None |
| Generator reads extraction persistence outputs and source-readiness artifacts, writes the published artifact, and fails validation before publishing. | covered-by-per-instance-candidate | Contract: `docs/contracts/reviewable-findings-artifact-contract.md:60-64`; gap evidence: `src/pension_data/langchain/review_artifact.py:234-301` still builds fixture rows while only listing `extraction_persistence/persistence_contract.json` and `coverage/source_authority_readiness.csv` as source ids; `scripts/langchain/build_reviewable_findings_artifact.py:35-42` calls the fixture builder. This is already tracked by https://github.com/stranske/Pension-Data/issues/425 and open PR https://github.com/stranske/Pension-Data/pull/427. | https://github.com/stranske/Pension-Data/issues/425 |
| README claim that the checked-in artifact is generated from existing extraction persistence and readiness outputs. | covered-by-per-instance-candidate | `docs/data/reviewable-findings/README.md:9-11` conflicts with the fixture builder in `src/pension_data/langchain/review_artifact.py:234-301`; same source-backed generator gap as issue #425. | https://github.com/stranske/Pension-Data/issues/425 |

## Result

The audit found two known uncovered clauses, both already represented by the per-instance generator-wiring issue #425 and PR #427. The prior `artifact_path` output-field gap is implemented on `main` through explain/compare metadata, route adapters, export artifacts, and tests, so no new follow-up issue is needed from this audit.

No additional follow-up issues are required.
