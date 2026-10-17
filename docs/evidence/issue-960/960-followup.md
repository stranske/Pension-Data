# Parser publication boundary verification

The validator integration was already present at the start of this follow-up.
The numeric regressions now have the requested individual test names, cover
negative AVA as well as AAL and participant count, and verify complete parser
inputs, domain error counters, queue uniqueness, persisted publication rows and
IDs, and eligibility for reprocessing.

Verified implementation tasks:

- [x] Call `validate_parser_outputs` and publish only `promotable_rows`.
- [x] Record error findings as funded validation failures and increment domain error counters.
- [x] Merge validator review records with finding and runbook context.
- [x] Keep blocked funded rows out of published rows and IDs.
- [x] Provide individually named negative AAL and participant-count regressions.
- [x] Verify invalid provenance through production orchestration and high-priority review.
- [x] Preserve publication and review routing for valid low-confidence facts.
- [x] Document orchestration enforcement and the operator-visible failure message.

The [machine-readable receipt](960-followup-causal.json) contains exact commands,
console output, exit codes, and source/test hashes. Against the local `origin/main`
source (`56a3e156`), both requested negative-value tests fail because funded rows
are published. Replacing only the validation boundary with that revision's
`missing_metric` block makes all three numeric regressions fail; restoring the
fixed source byte-for-byte makes all three pass. The full requested ops/quality
suite, including invalid provenance, low confidence, isolation and real-PDF
pilots, passes: **93 passed, 0 skipped**.

Tests used `python -m pytest` with `--no-cov` and `-m "not slow"`. The literal
`uv run` commands remain unverified: `uv` is absent and network DNS prevented
installation. Black 26.10.1's repository check passes with
`BLACK_NUM_WORKERS=1` and a writable cache in `/tmp` because the sandbox
disallows its default worker socket. The successful exit and output are in the receipt.
Ruff and `git diff --check` also pass.
