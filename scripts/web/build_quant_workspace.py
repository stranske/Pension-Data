#!/usr/bin/env python3
"""Publish producer-only metric-engine data from one completed pilot run."""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "src"))

from pension_data.quant.workspace_producer import (  # noqa: E402
    DEFAULT_QUANT_WORKSPACE_PATH,
    build_quant_workspace,
    write_quant_workspace,
)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--pilot-run-dir", required=True, type=Path)
    parser.add_argument("--out", type=Path, default=DEFAULT_QUANT_WORKSPACE_PATH)
    args = parser.parse_args(argv)
    try:
        payload = build_quant_workspace(args.pilot_run_dir)
        # Publication must never overwrite its own input capture.
        if args.out.resolve().is_relative_to(args.pilot_run_dir.resolve()):
            raise ValueError("quant output must be outside the pilot input directory")
        write_quant_workspace(args.out, payload)
    except (ValueError, OSError, TypeError, KeyError) as exc:
        print(f"quant producer failed: {exc}", file=sys.stderr)
        return 1
    print(f"wrote producer-only quant workspace: {args.out}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
