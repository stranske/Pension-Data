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
| LangChain action outputs include `request_id`, `generated_at`, `summary`, `citations`, and `artifact_path`. | implemented-and-tested at the published-artifact / recorded-output layer; documentation follow-up filed for the two-layer model | `docs/contracts/reviewable-findings-artifact-contract.md:58`; `src/pension_data/langchain/review_artifact.py:71-80`; chain step initializes `artifact_path=None` in metadata by design (`src/pension_data/langchain/findings_explain.py:77-82`, `src/pension_data/langchain/findings_compare.py:79-84`) and the route-level result preserves that (`tests/langchain/test_findings_chains.py:236`); the export persistence layer populates `artifact_path` for the published artifact (`src/pension_data/langchain/findings_export.py:15-90`); the eval-harness schema check enforces non-empty `artifact_path` against the live-runner / recorded-output payload, not the chain metadata (`src/pension_data/langchain/eval_harness.py:270-305`); `tests/langchain/test_review_artifact_contract.py:67-93` is the recorded-output canary that pins non-empty `artifact_path` in the published payload. The chain-vs-published layering is currently load-bearing but not documented in the contract doc; follow-up filed at https://github.com/stranske/Pension-Data/issues/429 to add a "chain output vs published artifact" subsection and an end-to-end smoke test. | https://github.com/stranske/Pension-Data/issues/429 |
| Generator reads extraction persistence outputs and source-readiness artifacts, writes the published artifact, and fails validation before publishing. | covered-by-per-instance-candidate | Contract: `docs/contracts/reviewable-findings-artifact-contract.md:60-64`; gap evidence: `src/pension_data/langchain/review_artifact.py:234-301` still builds fixture rows while only listing `extraction_persistence/persistence_contract.json` and `coverage/source_authority_readiness.csv` as source ids; `scripts/langchain/build_reviewable_findings_artifact.py:35-42` calls the fixture builder. This is already tracked by https://github.com/stranske/Pension-Data/issues/425 and open PR https://github.com/stranske/Pension-Data/pull/427. | https://github.com/stranske/Pension-Data/issues/425 |
| README claim that the checked-in artifact is generated from existing extraction persistence and readiness outputs. | covered-by-per-instance-candidate | `docs/data/reviewable-findings/README.md:9-11` conflicts with the fixture builder in `src/pension_data/langchain/review_artifact.py:234-301`; same source-backed generator gap as issue #425. | https://github.com/stranske/Pension-Data/issues/425 |

## Result

The audit found two known uncovered clauses already represented by the per-instance generator-wiring issue #425 and PR #427. The `artifact_path` clause is satisfied at the published-artifact and recorded-output layers — the chain step initializes `artifact_path=None` in metadata by design and the export persistence layer populates it; the eval-harness schema check and the recorded-output canary both enforce non-empty `artifact_path` at the published-payload surface. This two-layer model is currently undocumented in the contract doc and is not exercised by an end-to-end smoke test; follow-up filed at https://github.com/stranske/Pension-Data/issues/429.

Follow-ups filed from this audit:
- https://github.com/stranske/Pension-Data/issues/429 — document chain-output vs published-artifact layering and add an end-to-end smoke test.
