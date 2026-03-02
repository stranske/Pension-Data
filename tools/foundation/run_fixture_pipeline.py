"""Run the foundation fixture pipeline and persist artifacts + run ledger."""

from __future__ import annotations

import argparse
import json
import sys
from dataclasses import asdict
from pathlib import Path

from pension_data.ops.foundation import run_foundation_fixture_pipeline


def _build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--fixture",
        default="tests/e2e/foundation/fixtures/foundation_fixture_success.json",
        help="Path to foundation fixture JSON.",
    )
    parser.add_argument(
        "--output-root",
        default="artifacts",
        help="Base directory where foundation artifacts will be written.",
    )
    parser.add_argument(
        "--run-id",
        default=None,
        help="Optional deterministic run id.",
    )
    return parser


def main(argv: list[str] | None = None) -> int:
    parser = _build_arg_parser()
    args = parser.parse_args(argv)

    fixture_path = Path(args.fixture).resolve()
    output_root = Path(args.output_root).resolve()

    ledger, artifact_paths = run_foundation_fixture_pipeline(
        fixture_path=fixture_path,
        output_root=output_root,
        run_id=args.run_id,
    )
    print(
        json.dumps(
            {
                "run_id": ledger.run_id,
                "status": ledger.status,
                "stage_metrics": [asdict(item) for item in ledger.stage_metrics],
                "failure_count": len(ledger.failures),
                "artifacts": artifact_paths,
            },
            indent=2,
            sort_keys=True,
        )
    )
    return 0 if ledger.status == "success" else 1


if __name__ == "__main__":
    sys.exit(main())
