# Issue #935 deliberate-break transcript

Issue #935 requests durable fail/pass evidence for the documentation drift gate
added by merged PR #927. This transcript records a fresh replay against current
`main`; it does not claim to reconstruct the historical command output from PR
#927.

## Provenance

- Base: `origin/main` at `062bd840e69e08fb1eae39a217d0013d179887bf`
- Test: `tests/docs/test_contract_doc_drift.py::test_run_contract_doc_mentions_pension_data_emitter`
- Contract: `docs/contracts/run-contract-v1.md`
- Command:

  ```console
  uv run pytest tests/docs/test_contract_doc_drift.py::test_run_contract_doc_mentions_pension_data_emitter -q --no-cov
  ```

## Baseline

The unmodified `main` contract passed the named gate:

```text
tests/docs/test_contract_doc_drift.py .                                  [100%]
============================== 1 passed in 0.22s ===============================
```

## Deliberate break: expected failure

Immediately after the active-emitter status line, the replay temporarily
reintroduced exactly:

```text
No participant emits an envelope yet (that is P1+).
```

The command exited `1` and the named test failed on its stale-claim assertion:

```text
tests/docs/test_contract_doc_drift.py F                                  [100%]

_____________ test_run_contract_doc_mentions_pension_data_emitter ______________

    def test_run_contract_doc_mentions_pension_data_emitter() -> None:
        text = RUN_CONTRACT_DOC.read_text(encoding="utf-8")
>       assert STALE_NO_EMITTER_CLAIM not in text
E       AssertionError: assert 'No particip...envelope yet' not in '# Research ...th` trace.\n'
E         'No participant emits an envelope yet' is contained here:
E           > No participant emits an envelope yet (that is P1+).

tests/docs/test_contract_doc_drift.py:15: AssertionError
============================== 1 failed in 0.25s ===============================
```

## Restoration: expected pass

The temporary sentence was removed exactly. Both `git diff --exit-code 062bd840e69e08fb1eae39a217d0013d179887bf -- docs/contracts/run-contract-v1.md` and `git diff --cached --exit-code 062bd840e69e08fb1eae39a217d0013d179887bf -- docs/contracts/run-contract-v1.md` produced no output, proving the
production contract was restored. The same command exited `0`:

```text
tests/docs/test_contract_doc_drift.py .                                  [100%]
============================== 1 passed in 0.20s ===============================
```

This replay demonstrates that the named gate detects the stale no-emitter
claim and returns green after exact restoration.
