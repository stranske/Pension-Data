## 2026-05-27T09:09Z - codex opener materialized issue #470

- Repo: `stranske/Pension-Data`
- Issue: `#470` - Wire reviewable findings generator to CI and fix default readiness CSV path mismatch
- Branch: `codex/issue-470-reviewable-findings-ci`
- Lane: opener materialization
- Changes: fixed the reviewable findings generator default readiness CSV path to `coverage/readiness_rows.csv`; extended `Foundation Fixture E2E` to prepare generator source artifacts, build and validate `artifacts/reviewable-findings/extraction-quality-dashboard.json`, assert missing readiness CSV failure, and upload the reviewable-findings artifact; updated contract/docs with the CI path.
- Validation: `pytest -q tests/langchain/test_review_artifact_contract.py tests/langchain/test_chain_to_artifact_pipeline.py`; `pytest -q --no-cov tests/ops/test_foundation_ledger.py tests/e2e/foundation/test_fixture_pipeline.py`; `ruff check scripts/langchain/build_reviewable_findings_artifact.py`; `ruff format --check scripts/langchain/build_reviewable_findings_artifact.py`; local foundation fixture pipeline plus generator validation and missing-readiness negative path under `/tmp/pension-470-reviewable-findings`.
- Next action: open ready-for-review PR with `agent:codex`, `agents:keepalive`, `autofix`, `repo-review-approved`, and `priority:low`; keepalive owns CI follow-up.
