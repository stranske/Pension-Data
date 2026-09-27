# PR #908 deliberate-break evidence disposition

Issue #920 asks for recovery of the deliberate-break transcript required by
source issue #879. This disposition is based only on the durable PR record; it
does not reconstruct or infer historical command output.

## Evidence inspected

- `gh pr view 908 -R stranske/Pension-Data --json body,comments`
- `gh pr checks 908 -R stranske/Pension-Data`
- merged head `6018039575105744c0314ae87efae3863ef35cb9`
- post-merge provider report in
  [PR comment 5789116514](https://github.com/stranske/Pension-Data/pull/908#issuecomment-5789116514)
- head `8602926cd533edce5d41e0ce54aeb0d00b771c54` validation note in
  [PR comment 5788865874](https://github.com/stranske/Pension-Data/pull/908#issuecomment-5788865874)

## Deliberate-break result

The PR body and linked issue #879 require a deliberate-break gate for
`tests/harvest/test_calpers_ic_offline.py::test_ic_items_parsed`: return zero
items so the test **must FAIL**, then revert to the passing implementation. The
durable PR body and comments do **not** contain the actual failing pytest output
or the restored-pass output from that break cycle. Those historical outputs
therefore cannot be recovered from PR #908. They must not be recreated after the
fact and represented as the original transcript.

## Final check state

The live `gh pr checks` read on 2026-09-27 reports successful final product and
Gate coverage, including:

- `Gate / gate`, `gate`, and `gate-summary`
- Python 3.12 and 3.13
- Ruff, formatting, and mypy
- entity, extraction-golden, foundation-fixture, NL-SQL, one-PDF pilot,
  PostgreSQL, replay, SLA, and backplane-conformance checks
- the post-merge verifier

CodeRabbit pre-merge commentary is rate-limited rather than reporting a failing
product check. The post-merge provider comparison records OpenAI `PASS`; the
Anthropic `CONCERNS` entry is explicitly a provider-usage failure, not a code
finding. No failing final check is reported.

## Disposition

The final check state and restored implementation are durable, but the required
red/green transcript is not. Issue #920 is the linked evidence follow-up that
records this unrecoverable gap without fabricating historical output.
