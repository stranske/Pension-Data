#!/usr/bin/env python3
"""Materialize the canonical renderer shell inside the deployable web root."""

from __future__ import annotations

import argparse
import shutil
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
MAPPINGS = {
    Path("packages/renderer-shell/index.html"): Path("apps/web/index.html"),
    Path("packages/renderer-shell/app.js"): Path("apps/web/renderer-shell/app.js"),
    Path("packages/renderer-shell/styles.css"): Path("apps/web/styles.css"),
    Path("packages/renderer-shell/components.css"): Path("apps/web/components.css"),
    Path("packages/renderer-shell/tokens.css"): Path("apps/web/tokens.css"),
    Path("packages/renderer-shell/vendor/plotly-2.35.2.min.js"): Path(
        "apps/web/vendor/plotly-2.35.2.min.js"
    ),
}


def sync(*, check: bool) -> list[str]:
    """Check or update every explicit renderer-shell materialization."""
    stale: list[str] = []
    for source_relative, target_relative in MAPPINGS.items():
        source = ROOT / source_relative
        target = ROOT / target_relative
        if not source.is_file():
            stale.append(f"missing source: {source_relative}")
            continue
        if target.is_file() and target.read_bytes() == source.read_bytes():
            continue
        stale.append(f"stale target: {target_relative}")
        if not check:
            target.parent.mkdir(parents=True, exist_ok=True)
            shutil.copyfile(source, target)
    return stale


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--check", action="store_true", help="Fail instead of updating stale copies."
    )
    args = parser.parse_args()
    stale = sync(check=args.check)
    if stale and args.check:
        print("renderer-shell materialization is stale:", file=sys.stderr)
        for item in stale:
            print(f"- {item}", file=sys.stderr)
        return 1
    if stale:
        print(f"Updated {len(stale)} renderer-shell file(s).")
    else:
        print("Renderer-shell materialization is current.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
