# Entity Regression Golden Fixtures

This folder stores the fixture baseline for `tools/entity_regression/runner.py`.

## Baseline update workflow

1. Run:
   ```bash
   python tools/entity_regression/runner.py \
     --fixture tests/entities/golden/entity_regression_cases.json \
     --report-out artifacts/entity_regression/report.json
   ```
2. Inspect `artifacts/entity_regression/report.json` and confirm mismatches are intentional.
3. Update expected values in `entity_regression_cases.json`.
4. Open a PR that explains why the mapping/lineage baseline changed.
