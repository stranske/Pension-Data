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

## Publication failure follow-up

Reviewed the subsequent history through `dffb701`. PR #986 remains open with
`draft=false`, and all 11 task and acceptance checkboxes were already checked.
No checkbox changes were needed. A PR-body update recording this reconciliation
was rejected again because the connector requires approval and the approval
policy is `never`.

Added two regression cases that fail final artifact replacement after the new
file is fully written. They verify byte-for-byte retention of an existing valid
artifact, no published artifact on a failed initial write, and removal of
temporary files in both cases.

```text
python -m pytest tests/quant/test_contracts.py -q -m 'not slow'
collected 23 items
23 passed in 1.62s

python -m pytest tests/quant/test_metric_engine.py tests/web/test_workspace_bundle_quant_contract.py tests/web/test_build_workspace_bundle.py tests/export/test_workspace_bundle.py -q -m 'not slow'
collected 28 items
28 passed in 1.74s
```

Ruff and `git diff --check` also passed. The repository-wide Black CLI gate
documented above passed after populating the temporary cache with a serial
check: 461 files would be left unchanged. Delivery remains producer-only.

The follow-up commit was attempted after this gate, but `git add` again failed
because `.git/index.lock` cannot be created on the read-only filesystem. These
test and evidence changes remain uncommitted.

## Required-fact and reported-zero regression follow-up

Reviewed recent commits through `7c1054e` and the live PR #986 body. All 11 task
and acceptance checkboxes were already checked; no checkbox changes were needed.
The PR was open with `draft=false`. The attempted PR-body reconciliation update
was blocked because the connector requires approval and this run's approval
policy is `never`.

Added eight CLI regression cases for zero/negative AAL, negative/boolean AVA,
non-USD amounts, out-of-range/boolean confidence, and missing page evidence.
Each checks the specific validation failure, exit status 1, and byte-for-byte
preservation of the existing published artifact. A ninth case publishes explicitly
reported zero cash flows, verifies zero net cash flow with its source confidence
and page evidence, and confirms that a ratio with zero benefits is omitted.
These tests verify existing producer behavior; no production logic changed.

```text
python -m pytest tests/quant/test_contracts.py -q -m 'not slow'
collected 32 items
32 passed in 2.59s

python -m pytest tests/quant/test_metric_engine.py tests/web/test_workspace_bundle_quant_contract.py tests/web/test_build_workspace_bundle.py tests/export/test_workspace_bundle.py -q -m 'not slow'
collected 28 items
28 passed in 1.79s
```

Ruff and `git diff --check` passed. The repository-wide Black CLI gate passed:
461 files would be left unchanged. As in the earlier runs, a serial Black
library check first populated `/tmp/quant-black-cache`, then the required CLI
command ran successfully with `BLACK_CACHE_DIR=/tmp/quant-black-cache` and
`BLACK_NUM_WORKERS=1`.

Delivery remains producer-only, without scenario execution or UI readiness.

After validation, `git add` for the test and evidence files failed because
`.git/index.lock` cannot be created on the read-only filesystem, so no commit
could be made. The changes remain in the working tree. A final live PR read
confirmed #986 is still open with `draft=false`.
