#!/usr/bin/env python3
"""Run LangChain regression evaluation in mock or live mode."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

from pension_data.langchain.eval_harness import evaluate_dataset, load_eval_dataset  # noqa: E402


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--dataset",
        type=Path,
        default=Path("tests/langchain/prompt_dataset.json"),
        help="Path to evaluation dataset (JSON or YAML).",
    )
    parser.add_argument(
        "--mode",
        choices=("mock", "live"),
        default="mock",
        help="Mock mode uses recorded outputs; live mode shells out to --live-command.",
    )
    parser.add_argument(
        "--live-command",
        help="Shell command that reads case JSON from stdin and prints output JSON to stdout.",
    )
    parser.add_argument(
        "--live-timeout-sec",
        type=int,
        default=45,
        help="Timeout in seconds for each live-command invocation.",
    )
    parser.add_argument(
        "--output",
        type=Path,
        default=Path("artifacts/langchain/eval_report.json"),
        help="Path to write evaluation report JSON.",
    )
    args = parser.parse_args()
    if args.mode == "live" and not args.live_command:
        parser.error("--live-command is required when --mode live")
    return args


def main() -> int:
    args = parse_args()
    dataset = load_eval_dataset(args.dataset)
    report = evaluate_dataset(
        dataset,
        mode=args.mode,
        live_command=args.live_command,
        live_timeout_sec=args.live_timeout_sec,
    )

    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(report.as_dict(), indent=2, sort_keys=True), encoding="utf-8")

    print(f"LangChain eval status: {report.status}")
    print(f"Report: {args.output}")
    print(
        "Metrics: "
        f"schema={report.schema_validity_rate:.3f}, "
        f"citation_coverage={report.citation_coverage_rate:.3f}, "
        f"no_hallucination={report.no_hallucination_rate:.3f}, "
        f"safety={report.safety_pass_rate:.3f}"
    )
    if report.failures:
        print("Failures:")
        for failure in report.failures:
            print(f"- {failure}")
    return 0 if report.status == "pass" else 1


if __name__ == "__main__":
    raise SystemExit(main())
