# PR #907 deliberate-break evidence disposition

Issue #919 asks for recovery of the deliberate-break transcript required by
source issue #878. This disposition is based only on the durable PR record; it
does not reconstruct or infer historical command output.

## Evidence inspected

- `gh pr view 907 -R stranske/Pension-Data --json body,comments`
- `gh pr checks 907 -R stranske/Pension-Data`
- merged head `9db7617964152f7de62dead5529d08f6b8d514cb`
- merge-gate evidence in
  [PR comment 5781794753](https://github.com/stranske/Pension-Data/pull/907#issuecomment-5781794753)
- post-merge provider report in
  [PR comment 5781898991](https://github.com/stranske/Pension-Data/pull/907#issuecomment-5781898991)

## Deliberate-break result

The PR body states the required break scenario: force `enable_ocr=False`, run
`tests/parser/test_doc_lineage_backend.py::test_scanned_fixture_triggers_ocr_path`,
capture the failure, restore the implementation, and rerun the test. The
durable PR body and comments do **not** contain the actual failing output or the
restored-pass output. Those historical outputs therefore cannot be recovered
from PR #907. They must not be recreated after the fact and represented as the
original transcript.

## Final check state

The live `gh pr checks` read on 2026-09-27 reports successful final product and
Gate coverage, including:

- `Gate / gate`, `gate`, and `gate-summary`
- Python 3.12 and 3.13
- Ruff, formatting, and mypy
- entity, extraction-golden, foundation-fixture, NL-SQL, one-PDF pilot,
  PostgreSQL, replay, SLA, and backplane-conformance checks
- the post-merge verifier

CodeRabbit is reported as successful with the annotation `Review rate limited`.
The remaining listed contexts are skipped orchestration branches rather than
failing product checks. No failing final check is reported.

The exact-head merge-gate comment independently records zero active review
threads and a complete successful current-topology readback before merge. The
post-merge provider report records an OpenAI `PASS`; the Anthropic
`CONCERNS` entry is explicitly a provider-usage failure, not a code finding.

## Disposition

The final check state and restored implementation are durable, but the required
red/green transcript is not. Issue #919 is the linked evidence follow-up that
records this unrecoverable gap without fabricating historical output.
