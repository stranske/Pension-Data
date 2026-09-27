# PR #910 deliberate-break evidence disposition

Issue #922 asks for recovery of the deliberate-break transcript required by
source issue #882. This disposition is based only on the durable PR record; it
does not reconstruct or infer historical command output.

## Evidence inspected

- `gh pr view 910 -R stranske/Pension-Data --json body,comments`
- `gh pr checks 910 -R stranske/Pension-Data`
- PR source head (`headRefOid`, checks assessed)
  `2d6c5eadbbd11064029f4c972e77b595afd73d57`
- merge commit (`mergeCommit`) `53f815d73d3c72542b53892b9ae1efba66c166f0`
- current-head review-fix validation in
  [PR comment 5790304185](https://github.com/stranske/Pension-Data/pull/910#issuecomment-5790304185)
- fallback-review and exact-head merge adjudication in
  [PR comment 5790845407](https://github.com/stranske/Pension-Data/pull/910#issuecomment-5790845407)
- post-merge verifier disposition in
  [PR comment 5791002314](https://github.com/stranske/Pension-Data/pull/910#issuecomment-5791002314)

## Deliberate-break result

The PR body and linked issue #882 require a deliberate-break gate for
`tests/web/test_no_external_cdn.py`: reintroduce an external CDN script tag so
the test **must FAIL**, then revert to the passing implementation.

The current-head validation comment preserves a different deliberate break:
replacing the digest-coupled service-worker cache name with zeros made
`scripts/web/sync_renderer_shell.py --check` fail, and restoring it returned
green. The closer adjudication later states that an advisory reviewer exercised
in-memory breaks of the no-external-CDN gate, but it does not contain the actual
failing output or restored-pass output from that break cycle. The durable PR
body and comments therefore do not preserve the required external-CDN
fail-to-pass transcript. Those historical outputs cannot be recovered from PR
#910 and must not be recreated after the fact and represented as the original
transcript.

## Final check state

The live `gh pr checks` read on 2026-09-27 reports successful final product and
Gate coverage, including:

- `Gate / gate`, `gate`, and `gate-summary`
- Python 3.12 and 3.13
- Ruff, formatting, and mypy
- `web-smoke` and backplane conformance
- entity, extraction-golden, foundation-fixture, NL-SQL, one-PDF pilot,
  PostgreSQL, replay, and SLA checks
- the post-merge verifier

The exact-head merge adjudication records 84 passing renderer/web tests and
zero active non-outdated review threads. The post-merge provider comparison
records OpenAI `PASS`; the Anthropic `CONCERNS` entry is explicitly a provider
usage-limit failure, not an implementation finding. No failing final check is
reported.

## Disposition

The final check state and restored implementation are durable, but the required
external-CDN red/green transcript is not. Issue #922 is the linked evidence
follow-up that records this unrecoverable gap without fabricating historical
output.
