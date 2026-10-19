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


## Opener review recovery, 2026-10-19

The historical checkbox and test counts above describe their recorded heads,
not completed review reconciliation. At incoming head
`7c1054e788f7c7260d4368abafb2f9359b88b6d4`, these full active findings remained:

- [Producer contract cases](https://github.com/stranske/Pension-Data/pull/986#discussion_r4317339691):
  added distinct positive benefit/refund, multiple disclosed rows and superseded
  AAL cases. Native commit `add52a4c` independently added confidence and evidence
  refusal cases in the quant suite; these are retained without duplicate web
  mutations. Each new failure case uses
  the real CLI and checks both the intended refusal reason and byte preservation.
  A separate explicit undisclosed-row success publishes funded gap and unfunded
  ratio, with both optional-flow omission warnings and no fabricated flow facts.
- [Reconciliation evidence](https://github.com/stranske/Pension-Data/pull/986#discussion_r4317638944):
  this section records the pending findings and their implemented checks. Reviewer
  disposition is requested on the recovery head; no self-resolution is claimed.
- [Unfiltered required command](https://github.com/stranske/Pension-Data/pull/986#discussion_r4317638958):
  `python -m pytest tests/quant/test_contracts.py -q` collected 23 and passed 23
  without a marker exclusion (98.10 seconds via the canonical repository venv).

The initial focused command
`python -m pytest tests/quant/test_metric_engine.py tests/web/test_workspace_bundle_quant_contract.py tests/web/test_build_workspace_bundle.py tests/export/test_workspace_bundle.py -q`
collected and passed 36 before reconciliation (2.99 seconds using the owner-selected Anaconda runtime
in this worktree; no local venv is selected here). `PYTHONPATH=src` and
`PYTEST_DISABLE_PLUGIN_AUTOLOAD=1` were set for both suites. Existing optional
NumExpr/Bottleneck version warnings are retained. The initial new fixtures used
an incorrect guessed flow path: 4 failed/32 passed; reading the actual manifest's
flow artifact path corrected them. Those failures are not the deliberate proof.

Actual mutation proof: disabling the production superseded-row eligibility
guard made the `superseded-aal` CLI case fail because publication returned 0
instead of 1. Exact source-byte restoration made the case pass. Black (100),
Ruff and diff whitespace checks pass. No production file, prior test assertion,
workflow, UI, scenario engine, or eligibility policy is changed by this recovery.

All recorded native reservations across all heads and their actual
implementation/push jobs were terminal before the serialized source/PR claims.
Current-head CI, reviewer disposition, the seven-minute review floor and normal
merge/source-acceptance checks remain required after publication.


Before push, native commit `add52a4cd970432d9e7a335e7250ba5d21f00e60`
arrived with nine quant cases. Its actual native implementation/push jobs were
proven terminal; its entire payload was preserved. The recovery was rebased on
that head and duplicate confidence/evidence web cases removed. Final unfiltered
combined command (all five files listed above, including `tests/quant/test_contracts.py`)
collected and passed 65 tests. Counts are 32 quant-contract and 33 other focused
cases. The actual superseded-row production mutation/restoration proof remains
valid because that production file is byte-identical across both native heads.

## Published point and lineage consistency follow-up

Reviewed recent producer and regression commits through `4af10a8` and the live
PR #986 body. All 11 task and acceptance checkboxes were already checked; no
checkbox changes were needed. The PR was open with `draft=false`. The attempted
PR-body reconciliation update was rejected: "MCP tool call requires approval,
but approval policy is never".

The publication validator previously checked a point's metric identity without
checking its value, unit, confidence or page references against the associated
engine observation. Four new regression cases each changed one of those fields;
all four failed before the fix because publication incorrectly succeeded. The
validator now rejects these mismatches before writing. Each regression verifies
byte-for-byte preservation of the previous valid artifact and no temporary file.
Provenance comparison accepts both in-memory tuples and serialized JSON arrays.

```text
python -m pytest tests/quant/test_contracts.py -q
collected 40 items
40 passed in 1.94s

python -m pytest tests/quant/test_metric_engine.py tests/web/test_workspace_bundle_quant_contract.py tests/web/test_build_workspace_bundle.py tests/export/test_workspace_bundle.py -q -m 'not slow'
collected 33 items
33 passed in 1.99s
```

Ruff, producer mypy and `git diff --check` passed. Black formatted both changed
Python files with line length 100; neither needed reformatting. A serial Black
check populated the temporary cache after the initial CLI calls stalled, and
the required repository-wide CLI gate passed with that cache:

```text
BLACK_CACHE_DIR=/tmp/quant-black-cache BLACK_NUM_WORKERS=1 timeout 30s black --check --line-length 100 --exclude '(\.workflows-lib|node_modules)' .
461 files would be left unchanged.
```

Delivery remains producer-only, without scenario execution or UI readiness.

The commit was attempted after validation, but `git add` failed because `.git`
is read-only and `index.lock` cannot be created. The source, tests and this
evidence remain as uncommitted working-tree changes.

## Required funded-series publication follow-up

Reviewed recent commits through `4b7cb38` and the live PR #986 body. All 11 task
and acceptance checkboxes were already checked; none needed changing. The PR
was open with `draft=false`. The attempted PR-body reconciliation update was
rejected: "MCP tool call requires approval, but approval policy is never".

Publication previously accepted a payload with either or both required funded
series removed, provided another numeric series remained. Three new cases
reproduced this failure before the fix: removing funded gap, unfunded ratio, or
both did not raise. Validation now requires both funded series before replacing
the artifact. Each regression starts with a completed cash-flow pilot fixture,
retains optional numeric series, and verifies byte-for-byte preservation of the
previous artifact and absence of temporary files after rejection.

An exploratory extreme-AAL case already failed the existing finite-value gate;
it did not demonstrate a production bug and was removed from the final change.

```text
python -m pytest tests/quant/test_contracts.py -q
collected 54 items
54 passed in 2.56s

python -m pytest tests/quant/test_metric_engine.py tests/web/test_workspace_bundle_quant_contract.py tests/web/test_build_workspace_bundle.py tests/export/test_workspace_bundle.py -q -m 'not slow'
collected 33 items
33 passed in 2.28s
```

Ruff, producer mypy and `git diff --check` passed. Delivery remains producer-only,
without scenario execution or UI readiness.

Black formatted the changed Python files with line length 100. After a serial
check populated the temporary cache, the required repository-wide CLI gate passed:

```text
BLACK_CACHE_DIR=/tmp/quant-black-cache BLACK_NUM_WORKERS=1 black --check --line-length 100 --exclude '(\.workflows-lib|node_modules)' .
461 files would be left unchanged.
```

Staging the follow-up for the requested commit failed because `.git/index.lock`
cannot be created on the read-only filesystem. The source, tests and evidence
remain as working-tree changes. A final live PR read confirmed #986 is open
with `draft=false`.
