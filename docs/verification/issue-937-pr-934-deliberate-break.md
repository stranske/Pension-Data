# Issue #937 deliberate-break transcript

Issue #937 requests durable fail/pass evidence for the fork read-only-token
Gate behavior added by merged PR #934. This transcript records a fresh replay
against current `main`; it does not claim to reconstruct historical command
output from PR #934.

## Provenance

- Base: `origin/main` at `4b62510a326423292048c90cc1ead079c9d272a8`
- Source delivery: PR #934, squash merge `062bd840e69e08fb1eae39a217d0013d179887bf`
- Workflow: `.github/workflows/pr-00-gate.yml`
- Focused gate: `tests/test_gate_commit_status_fork_tolerance.py`
- Environment: Python 3.12.2; `PYTEST_ADDOPTS` unset
- Command:

  ```console
  python -m pytest --no-cov tests/test_gate_commit_status_fork_tolerance.py -q
  ```

## Baseline

The unmodified workflow passed all ten focused cases:

```text
tests/test_gate_commit_status_fork_tolerance.py ..........               [100%]

============================== 10 passed in 0.37s ==============================
```

## Deliberate break: expected failure

The replay temporarily disabled only the fork read-only-token fallback by
changing the production predicate from:

```javascript
const readOnlyForkToken =
  error?.status === 403 && isForkPullRequest && !hitRateLimit;
```

to:

```javascript
const readOnlyForkToken =
  false &&
  error?.status === 403 &&
  isForkPullRequest &&
  !hitRateLimit;
```

The command exited `1`. Six fork and deleted-fork cases failed while the four
same-repository, rate-limit, unrelated-error, and happy-path cases continued to
pass:

```text
tests/test_gate_commit_status_fork_tolerance.py FFFFFF....               [100%]

=================================== FAILURES ===================================
________________ test_fork_read_only_403_does_not_fail_the_gate ________________

outcomes = {'fork_read_only': {'failures': [], 'warnings': [], 'summaryWrites': 0, 'summaryRaw': [], ...}, 'fork_read_only_failur...: [], ...}, 'fork_read_only_pending': {'failures': [], 'warnings': [], 'summaryWrites': 0, 'summaryRaw': [], ...}, ...}

    def test_fork_read_only_403_does_not_fail_the_gate(outcomes: dict[str, Any]) -> None:
>       assert outcomes["fork_read_only"]["threw"] is None
E       AssertionError: assert {'status': 403, 'message': 'Resource not accessible by integration'} is None

tests/test_gate_commit_status_fork_tolerance.py:199: AssertionError
_______________ test_fork_read_only_403_reports_the_real_verdict _______________

outcomes = {'fork_read_only': {'failures': [], 'warnings': [], 'summaryWrites': 0, 'summaryRaw': [], ...}, 'fork_read_only_failur...: [], ...}, 'fork_read_only_pending': {'failures': [], 'warnings': [], 'summaryWrites': 0, 'summaryRaw': [], ...}, ...}

    def test_fork_read_only_403_reports_the_real_verdict(outcomes: dict[str, Any]) -> None:
        case = outcomes["fork_read_only"]
        warning = " ".join(case["warnings"])
        summary = " ".join(case["summaryRaw"])
>       assert "read-only" in warning
E       AssertionError: assert 'read-only' in ''

tests/test_gate_commit_status_fork_tolerance.py:207: AssertionError
______________ test_fork_read_only_403_preserves_failure_verdict _______________

outcomes = {'fork_read_only': {'failures': [], 'warnings': [], 'summaryWrites': 0, 'summaryRaw': [], ...}, 'fork_read_only_failur...: [], ...}, 'fork_read_only_pending': {'failures': [], 'warnings': [], 'summaryWrites': 0, 'summaryRaw': [], ...}, ...}

    def test_fork_read_only_403_preserves_failure_verdict(
        outcomes: dict[str, Any],
    ) -> None:
        case = outcomes["fork_read_only_failure"]
        warning = " ".join(case["warnings"])
        summary = " ".join(case["summaryRaw"])
>       assert case["threw"] is None
E       AssertionError: assert {'status': 403, 'message': 'Resource not accessible by integration'} is None

tests/test_gate_commit_status_fork_tolerance.py:221: AssertionError
__ test_fork_read_only_403_fails_closed_for_other_non_success_verdicts[error] __

outcomes = {'fork_read_only': {'failures': [], 'warnings': [], 'summaryWrites': 0, 'summaryRaw': [], ...}, 'fork_read_only_failur...: [], ...}, 'fork_read_only_pending': {'failures': [], 'warnings': [], 'summaryWrites': 0, 'summaryRaw': [], ...}, ...}
state = 'error'

    @pytest.mark.parametrize("state", ["error", "pending"])
    def test_fork_read_only_403_fails_closed_for_other_non_success_verdicts(
        outcomes: dict[str, Any], state: str
    ) -> None:
        case = outcomes[f"fork_read_only_{state}"]
>       assert case["threw"] is None
E       AssertionError: assert {'status': 403, 'message': 'Resource not accessible by integration'} is None

tests/test_gate_commit_status_fork_tolerance.py:233: AssertionError
_ test_fork_read_only_403_fails_closed_for_other_non_success_verdicts[pending] _

outcomes = {'fork_read_only': {'failures': [], 'warnings': [], 'summaryWrites': 0, 'summaryRaw': [], ...}, 'fork_read_only_failur...: [], ...}, 'fork_read_only_pending': {'failures': [], 'warnings': [], 'summaryWrites': 0, 'summaryRaw': [], ...}, ...}
state = 'pending'

    @pytest.mark.parametrize("state", ["error", "pending"])
    def test_fork_read_only_403_fails_closed_for_other_non_success_verdicts(
        outcomes: dict[str, Any], state: str
    ) -> None:
        case = outcomes[f"fork_read_only_{state}"]
>       assert case["threw"] is None
E       AssertionError: assert {'status': 403, 'message': 'Resource not accessible by integration'} is None

tests/test_gate_commit_status_fork_tolerance.py:233: AssertionError
_____________ test_deleted_fork_read_only_403_reports_the_verdict ______________

outcomes = {'fork_read_only': {'failures': [], 'warnings': [], 'summaryWrites': 0, 'summaryRaw': [], ...}, 'fork_read_only_failur...: [], ...}, 'fork_read_only_pending': {'failures': [], 'warnings': [], 'summaryWrites': 0, 'summaryRaw': [], ...}, ...}

    def test_deleted_fork_read_only_403_reports_the_verdict(
        outcomes: dict[str, Any],
    ) -> None:
        case = outcomes["deleted_fork_read_only"]
        warning = " ".join(case["warnings"])
>       assert case["threw"] is None
E       AssertionError: assert {'status': 403, 'message': 'Resource not accessible by integration'} is None

tests/test_gate_commit_status_fork_tolerance.py:243: AssertionError
=========================== short test summary info ============================
FAILED tests/test_gate_commit_status_fork_tolerance.py::test_fork_read_only_403_does_not_fail_the_gate
FAILED tests/test_gate_commit_status_fork_tolerance.py::test_fork_read_only_403_reports_the_real_verdict
FAILED tests/test_gate_commit_status_fork_tolerance.py::test_fork_read_only_403_preserves_failure_verdict
FAILED tests/test_gate_commit_status_fork_tolerance.py::test_fork_read_only_403_fails_closed_for_other_non_success_verdicts[error]
FAILED tests/test_gate_commit_status_fork_tolerance.py::test_fork_read_only_403_fails_closed_for_other_non_success_verdicts[pending]
FAILED tests/test_gate_commit_status_fork_tolerance.py::test_deleted_fork_read_only_403_reports_the_verdict
========================= 6 failed, 4 passed in 0.41s ==========================
```

## Restoration: expected pass

The temporary predicate change was removed exactly. Both
`git diff --exit-code origin/main -- .github/workflows/pr-00-gate.yml tests/test_gate_commit_status_fork_tolerance.py`
and the equivalent cached-diff command produced no output, proving the
production workflow and focused test were restored. The same focused command
then exited `0`:

```text
tests/test_gate_commit_status_fork_tolerance.py ..........               [100%]

============================== 10 passed in 0.36s ==============================
```

This replay demonstrates that the named gate detects loss of fork and
deleted-fork read-only-token tolerance while keeping unrelated behavior green,
and that exact restoration returns the complete focused suite to green.
