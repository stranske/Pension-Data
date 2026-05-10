# Workloop State

## 2026-05-10T05:06:11Z - opener lane implemented issue #394

- Automation: `pd-workloop-resume` opener lane.
- Source repo: `stranske/Pension-Data`.
- Source issue: `#394` (`Label apps/web demo workspace bundle as fixture-only so the reviewable UI does not overclaim live readiness`, `priority:low`, `repo-review-approved`).
- Branch: `codex/issue-394-fixture-origin`.
- Selection:
  - ACTION A succeeded from the neutral Code workspace.
  - Initial cap-health reported `total_opener_owned=4`, `raw_cap_reached=false`, `normal_cap_reached=false`, and one stale blocking-label item on `stranske/Inv-Man-Intake#406`.
  - Ran opener infra repair, which removed stale `needs-human` from `#406`; post-repair cap-health reported `drainable_count=4`, `non_drainable_count=0`, and cap below five.
  - Full priority discovery skipped operational `stranske/Workflows#2073` and issues already served by existing PRs, leaving `stranske/Pension-Data#394` as the next eligible opener issue.
- Implementation:
  - Added required `data_origin` to the shared runtime contract with allowed values `fixture`, `generated`, and `live`.
  - Marked `apps/web/data/workspace.json` as `fixture`.
  - Added browser-side workspace-origin validation and a visible `Demo data - not live` origin badge.
  - Tightened web smoke checks so fixture-origin bundles are accepted for local demo mode but rejected when runtime config or deployed runtime smoke is required.
  - Extended the desktop runtime-contract validator and documented fixture-only behavior in web/deploy docs.
  - Added focused `tests/web/test_workspace_contract.py` coverage.
- Validation:
  - `python scripts/web/smoke_test.py --base-dir apps/web`
  - `python -m pytest -q --no-cov tests/web/test_workspace_contract.py`
  - `node apps/mac-desktop/scripts/validate_runtime_contract.mjs`
  - `python -m ruff check scripts/web/smoke_test.py tests/web/test_workspace_contract.py`
  - `python -m black --check scripts/web/smoke_test.py tests/web/test_workspace_contract.py`
  - `git diff --check`
- Push/PR:
  - Pushed commit `1b53f7c` (`Issue #394: label web bundle fixture origin`) to `codex/issue-394-fixture-origin`.
  - Opened ready-for-review PR `#413` (`https://github.com/stranske/Pension-Data/pull/413`) with labels `agent:codex`, `agents:keepalive`, and `autofix`.
  - Verified `isDraft=false` and `closingIssuesReferences` includes `#394`.
  - Emitted relay event `pr_opened active.source_repo=stranske/Pension-Data active.source_issue=394 active.source_pr=413 active.next_action=wait_for_keepalive`.
- Post-open cap hygiene:
  - Immediate cap-health classified PR `#413` as `needs-dispatch-evidence` while raw cap reached five.
  - Ran `opener-repair-infra-stalls.py --json`; it added `agent:retry` and dispatched Gate Followups for PR `#413`.
  - Direct GitHub evidence after repair showed keepalive and runner-dispatch comments for PR `#413`, a `runner-dispatch:codex:413` pending marker, and fresh Gate/guard/Python CI rows active or pending for the branch.
- Next action: wait for keepalive/Gate to drain PR `#413`; closer/verifier handles post-merge completion.
