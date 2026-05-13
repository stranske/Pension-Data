# Workloop State — stranske/Pension-Data

## 2026-05-13T20:50:00Z — opener claude_code lane

- **Repo:** stranske/Pension-Data
- **Issue:** [#425](https://github.com/stranske/Pension-Data/issues/425) — Wire reviewable-findings generator to read from real extraction/readiness outputs instead of fixture data
- **Branch:** `claude/issue-425-real-extraction-readiness` (off `origin/main` `0699744`)
- **Implementation:** Modified `build_extraction_quality_dashboard_artifact()` in `src/pension_data/langchain/review_artifact.py` to accept `persistence_contract_path` and `readiness_csv_path`. When both are provided the generator reads them and derives finding rows from real extraction/readiness data; missing/unreadable/unparseable inputs raise `ReviewableFindingsArtifactError` before any hardcoded fallback can execute. CLI script `scripts/langchain/build_reviewable_findings_artifact.py` now exposes `--persistence-contract` / `--readiness-csv` defaulting to `extraction_persistence/persistence_contract.json` and `coverage/source_authority_readiness.csv`, surfacing the error on stderr with exit 1. Added new tests in `tests/langchain/test_review_artifact_contract.py` covering real-data path, mixed/missing path arguments, malformed JSON, and empty-CSV cases. Removed the "while generator wiring is finalized" qualifier in `docs/data/reviewable-findings/README.md` and documented the new CLI args.
- **Validation:**
  - `pytest tests/langchain/test_review_artifact_contract.py -v --no-cov` → 23 passed
  - `pytest tests/langchain/ -q --no-cov` → 68 passed
  - `ruff check` + `black --check` clean on touched files
  - `python scripts/langchain/build_reviewable_findings_artifact.py` (no args, no real source files present) → exits 1 with `ReviewableFindingsArtifactError: source artifact not found: extraction_persistence/persistence_contract.json`
  - `rg "while generator wiring is finalized" docs/data/reviewable-findings/README.md` → no matches
- **Next action:** PR to be opened ready-for-review with labels `agent:claude`, `agents:keepalive`, `autofix`; keepalive owns CI/check follow-up.
- **Notes:** Work performed in writable temp clone `/tmp/pension-data-issue-425-claude` because the Dropbox-backed `Pension-Data` checkout has pre-existing uncommitted artifacts from codex's issue #423/#424 work that must not be disturbed.
