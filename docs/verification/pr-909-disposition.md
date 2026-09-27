# PR #909 deliberate-break evidence disposition

Issue #921 asks for recovery of the deliberate-break transcript required by
source issue #880. This disposition is based only on the durable PR record; it
does not reconstruct or infer historical command output.

## Evidence inspected

- `gh pr view 909 -R stranske/Pension-Data --json body,comments`
- `gh pr checks 909 -R stranske/Pension-Data`
- PR source head (`headRefOid`, checks assessed) `28aa75f1bb1adf93f8539e6594f5bc0c477fe360`
- merge commit (`mergeCommit`) `ad42f6794f930675d9c78b8922586891a1e3c591`
- fallback-review disposition in
  [PR comment 5789013892](https://github.com/stranske/Pension-Data/pull/909#issuecomment-5789013892)
- final focused validation in
  [PR comment 5789244043](https://github.com/stranske/Pension-Data/pull/909#issuecomment-5789244043)
- exact-head merge adjudication in
  [PR comment 5789569483](https://github.com/stranske/Pension-Data/pull/909#issuecomment-5789569483)
- post-merge verifier disposition in
  [PR comment 5789625286](https://github.com/stranske/Pension-Data/pull/909#issuecomment-5789625286)

## Deliberate-break result

The PR body and linked issue #880 require a deliberate-break gate for
`tests/harvest/test_ncsr_sample_offline.py::test_ncsr_fixture_ingests`: omit
the filing date so the test **must FAIL**, then revert to the passing
implementation. The fallback-review comment says that a manual break/revert
proof existed in opener run evidence, but the durable PR body and comments do
**not** contain the actual failing pytest output or the restored-pass output
from that break cycle. Those historical outputs therefore cannot be recovered
from PR #909. They must not be recreated after the fact and represented as the
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

The final focused validation comment records eight passing N-CSR tests. The
post-merge provider comparison records OpenAI `PASS`; the Anthropic
`CONCERNS` entry is explicitly a provider-usage failure, not a code finding.
No failing final check is reported.

## Disposition

The final check state and restored implementation are durable, but the required
red/green transcript is not. Issue #921 is the linked evidence follow-up that
records this unrecoverable gap without fabricating historical output.
