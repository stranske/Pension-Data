# Replay Baseline Update Workflow

This workflow defines controlled baseline updates for the replay harness introduced in issue #44.

## Goals

- Preserve deterministic replay artifacts for unchanged parser versions.
- Require explicit operator intent when replacing a baseline snapshot.
- Keep update context traceable to a ticket or PR.

## Standard Usage

Create a new snapshot without replacing an existing one:

```bash
python scripts/replay_runner.py \
  --corpus path/to/golden.jsonl \
  --parser tests.replay.fixtures_parser:parser \
  --snapshot-out tools/replay/baselines/current.json
```

Notes:
- If `--generated-at` is not provided, the snapshot uses a deterministic default timestamp (`1970-01-01T00:00:00+00:00`).
- This keeps snapshot bytes reproducible for unchanged parser behavior.

## Controlled Baseline Replacement

Replacing an existing baseline requires both `--overwrite` and a ticket/PR reference:

```bash
python scripts/replay_runner.py \
  --corpus path/to/golden.jsonl \
  --parser tests.replay.fixtures_parser:parser \
  --snapshot-out tools/replay/baselines/current.json \
  --overwrite \
  --baseline-update-ticket "#44"
```

If `--overwrite` is provided without `--baseline-update-ticket`, the runner exits with an error.

## Review Checklist

Before merging a baseline replacement:

1. Run `python scripts/replay_diff.py --baseline <old> --current <new> --report-out <report>`.
2. Verify expected vs unexpected drift classification in the report.
3. Link the diff report in the PR discussion.
4. Confirm reviewer approval for the baseline update ticket/reference.
