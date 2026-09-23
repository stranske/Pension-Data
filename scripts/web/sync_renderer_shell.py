#!/usr/bin/env python3
"""Materialize the canonical renderer shell inside the deployable web root."""

from __future__ import annotations

import argparse
import hashlib
import re
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
SERVICE_WORKER = Path("apps/web/sw.js")
CACHE_NAME_PATTERN = re.compile(
    r'^const CACHE_NAME = "pension-data-web-[0-9a-f]{12}";$', re.MULTILINE
)


def renderer_shell_digest() -> str:
    """Return a stable digest covering every canonical renderer-shell asset."""
    digest = hashlib.sha256()
    for source_relative in sorted(MAPPINGS, key=str):
        source = ROOT / source_relative
        digest.update(str(source_relative).encode("utf-8"))
        digest.update(b"\0")
        digest.update(source.read_bytes())
        digest.update(b"\0")
    return digest.hexdigest()[:12]


def sync(*, check: bool) -> tuple[list[str], list[str]]:
    """Check or update every explicit renderer-shell materialization."""
    missing: list[str] = []
    stale: list[str] = []
    for source_relative, target_relative in MAPPINGS.items():
        source = ROOT / source_relative
        target = ROOT / target_relative
        if not source.is_file():
            missing.append(f"missing source: {source_relative}")
            continue
        if target.is_file() and target.read_bytes() == source.read_bytes():
            continue
        stale.append(f"stale target: {target_relative}")
        if not check:
            target.parent.mkdir(parents=True, exist_ok=True)
            shutil.copyfile(source, target)

    if missing:
        return missing, stale

    service_worker = ROOT / SERVICE_WORKER
    if not service_worker.is_file():
        missing.append(f"missing service worker: {SERVICE_WORKER}")
        return missing, stale
    service_worker_text = service_worker.read_text(encoding="utf-8")
    expected = f'const CACHE_NAME = "pension-data-web-{renderer_shell_digest()}";'
    current = CACHE_NAME_PATTERN.search(service_worker_text)
    if current is None or current.group(0) != expected:
        stale.append(f"stale cache version: {SERVICE_WORKER}")
        if not check:
            if current is None:
                missing.append(f"invalid cache declaration: {SERVICE_WORKER}")
            else:
                service_worker.write_text(
                    CACHE_NAME_PATTERN.sub(expected, service_worker_text, count=1),
                    encoding="utf-8",
                )
    return missing, stale


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--check", action="store_true", help="Fail instead of updating stale copies."
    )
    args = parser.parse_args()
    missing, stale = sync(check=args.check)
    if missing:
        print("renderer-shell sources are missing or invalid:", file=sys.stderr)
        for item in missing:
            print(f"- {item}", file=sys.stderr)
        return 1
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
