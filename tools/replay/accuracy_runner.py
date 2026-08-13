"""Produce and gate comparable extraction-accuracy evidence without network access."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

from tools.replay.harness import (
    assert_accuracy_thresholds,
    evaluate_extraction_accuracy,
    load_snapshot,
)


def run(argv: list[str] | None = None) -> int:
    """Write field/document metrics and return 2 for a threshold regression."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--baseline", required=True, type=Path)
    parser.add_argument("--current", required=True, type=Path)
    parser.add_argument("--report-out", required=True, type=Path)
    args = parser.parse_args(argv)
    try:
        baseline = load_snapshot(args.baseline)
        current = load_snapshot(args.current)
        report = evaluate_extraction_accuracy(baseline=baseline, current=current)
        args.report_out.parent.mkdir(parents=True, exist_ok=True)
        args.report_out.write_text(
            json.dumps(report, indent=2, sort_keys=True) + "\n", encoding="utf-8"
        )
        assert_accuracy_thresholds(baseline=baseline, report=report)
    except (FileNotFoundError, ValueError, json.JSONDecodeError) as exc:
        print(f"extraction-accuracy error: {exc}", file=sys.stderr)
        return 2
    print(
        "Extraction accuracy: "
        f"precision={report['field_precision']:.3f} recall={report['field_recall']:.3f} "
        f"document_pass_rate={report['document_pass_rate']:.3f}",
    )
    return 0


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(run())
