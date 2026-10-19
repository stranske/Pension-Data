# Quant producer verification — 2026-10-19

Task reconciliation reviewed commits `cd6d0e4`, `c783638`, `e8ff7e2`, `bdd917b`
and `d5df557`. PR #986 already had all 11 task and acceptance checkboxes checked;
none needed changing. The existing focused suite collected and passed 43 tests.
The PR was open with `draft=false`. Updating its body with this reconciliation
was blocked by the connector: "MCP tool call requires approval, but approval
policy is never". This file retains the delivery evidence locally.

A required-input follow-up found that an absent or null manifest
`source_document_id` bypassed document matching. The new regression cases failed
before the fix: both cases allowed CLI publication, while empty and whitespace
identities failed with a row-mismatch error instead of required-input validation.
The producer now requires the manifest identity before reading metric rows.

After the fix:

```text
python -m pytest tests/quant/test_contracts.py -q -m 'not slow'
collected 21 items
21 passed in 1.64s

python -m pytest tests/quant/test_metric_engine.py tests/web/test_workspace_bundle_quant_contract.py tests/web/test_build_workspace_bundle.py tests/export/test_workspace_bundle.py -q -m 'not slow'
collected 28 items
28 passed in 1.62s
```

The six added CLI cases verify missing, null, empty and whitespace manifest
identities, plus core and cash-flow rows belonging to another document. Each
asserts exit status 1 and byte-for-byte preservation of the previous quant artifact.
The existing tests continue to verify funded-gap, unfunded-ratio and cash-flow
values and input sensitivity, reproducibility failures, and optional omissions.
Ruff passed for both changed Python files; mypy passed for the producer.
The repository-wide Black gate passed: 461 files would be left unchanged.
Its default multiprocessing path cannot bind a socket in this sandbox; a serial
Black library pass populated a writable temporary cache, after which the required
CLI gate succeeded with that cache and one worker:

```sh
BLACK_CACHE_DIR=/tmp/quant-black-cache BLACK_NUM_WORKERS=1 black --check --line-length 100 --exclude '(\.workflows-lib|node_modules)' .
```

Delivery remains producer-only, without scenario execution or UI readiness.

The requested commit was attempted after validation, but staging failed because
`.git` is read-only (`Unable to create .../.git/index.lock: Read-only file system`).
The implementation and evidence remain as working-tree changes.
