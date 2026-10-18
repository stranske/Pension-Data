# Funding-trend financial-flow delivery (#959)

The manifest declares the real orchestration financial-flow artifact. The loader validates plan/period/document identity and path containment before incorporating disclosures into the existing trend version/conflict policy. Benefits and refunds count once as outflows; absent disclosures remain null.

Validation: `.venv/bin/python -m pytest tests/api/test_artifact_data.py tests/api/test_app_serving.py tests/query/test_saved_views.py tests/extract/funded/test_financial_flows.py tests/ops/test_one_pdf_pilot.py -q`: **107 passed**. Ruff and strict mypy on the four source files pass; Black and diff checks pass. The worktree-local environment uses `/opt/anaconda3/bin/python3` and installed system packages; canonical cloud-only virtual-environment configuration stalled startup and was left untouched.

The actual orchestration-to-loader and authenticated HTTP tests supply disclosed table rows at the parser boundary, run production orchestration/extraction/persistence, and assert 100m+20m-30m-5m=85m. They do not claim the default PDF parser recognizes monetary table labels: its vocabulary remains outside this issue. The one-PDF manifest contract is separately exercised by the real harness tests.

See `flow-join-red-green.txt` for the executed disconnection failure and exact-byte restoration pass. Additional controls cover three identity mismatches, invalid numeric disclosures, escaping paths, agreeing replays and conflicting disclosures, missing artifacts, and positive/negative outflow representations.

Hosted CI, independent review, guarded merge and post-merge comparison remain separate delivery stages.
