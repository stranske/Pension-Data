#!/usr/bin/env python3
"""Smoke checks for web scaffold integrity (local files and deployed URL)."""

from __future__ import annotations

import argparse
import json
from pathlib import Path
from urllib.error import HTTPError, URLError
from urllib.parse import urljoin
from urllib.request import urlopen

REQUIRED_CONFIG_KEYS = ("environment", "apiBaseUrl", "artifactBaseUrl")
REQUIRED_LOCAL_FILES = (
    "index.html",
    "styles.css",
    "app.js",
    "config/default.json",
)


def _load_json(path: Path) -> dict[str, object]:
    payload = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise ValueError(f"JSON object required in {path}")
    return payload


def _assert_config(payload: dict[str, object], *, path_label: str) -> None:
    for key in REQUIRED_CONFIG_KEYS:
        value = payload.get(key)
        if not isinstance(value, str) or not value.strip():
            raise ValueError(f"missing required config key '{key}' in {path_label}")


def _smoke_local(base_dir: Path, *, require_runtime: bool) -> None:
    for relative_path in REQUIRED_LOCAL_FILES:
        if not (base_dir / relative_path).exists():
            raise ValueError(f"missing required file: {base_dir / relative_path}")

    index = (base_dir / "index.html").read_text(encoding="utf-8")
    markers = (
        'data-testid="web-foundation-root"',
        'data-testid="environment-badge"',
        "./styles.css",
        "./app.js",
    )
    for marker in markers:
        if marker not in index:
            raise ValueError(f"index.html missing marker: {marker}")

    _assert_config(_load_json(base_dir / "config/default.json"), path_label="config/default.json")

    runtime_path = base_dir / "config/runtime.json"
    if require_runtime:
        if not runtime_path.exists():
            raise ValueError("runtime config required but missing: config/runtime.json")
        _assert_config(_load_json(runtime_path), path_label="config/runtime.json")


def _fetch_text(url: str) -> str:
    try:
        with urlopen(url, timeout=20) as response:  # noqa: S310
            body = response.read().decode("utf-8", errors="replace")
            if response.status < 200 or response.status >= 300:
                raise ValueError(f"request failed ({response.status}): {url}")
            return body
    except HTTPError as exc:
        raise ValueError(f"HTTP error {exc.code} for {url}") from exc
    except URLError as exc:
        raise ValueError(f"URL error for {url}: {exc.reason}") from exc


def _smoke_url(base_url: str, *, expect_runtime: bool) -> None:
    root = base_url.rstrip("/") + "/"
    html = _fetch_text(root)
    if "Cloudflare Web Workspace Foundation" not in html:
        raise ValueError("deployed page missing expected heading marker")
    if 'data-testid="environment-badge"' not in html:
        raise ValueError("deployed page missing environment badge marker")

    default_payload = json.loads(_fetch_text(urljoin(root, "config/default.json")))
    if not isinstance(default_payload, dict):
        raise ValueError("default config endpoint did not return object JSON")
    _assert_config(default_payload, path_label="config/default.json")

    if expect_runtime:
        runtime_payload = json.loads(_fetch_text(urljoin(root, "config/runtime.json")))
        if not isinstance(runtime_payload, dict):
            raise ValueError("runtime config endpoint did not return object JSON")
        _assert_config(runtime_payload, path_label="config/runtime.json")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--base-dir",
        type=Path,
        default=Path("apps/web"),
        help="Local web app directory for file-level smoke checks.",
    )
    parser.add_argument(
        "--url",
        help="Optional deployed URL for remote smoke checks (for example, Cloudflare Pages URL).",
    )
    parser.add_argument(
        "--require-runtime",
        action="store_true",
        help="Require runtime config file for local checks.",
    )
    parser.add_argument(
        "--expect-runtime",
        action="store_true",
        help="Require runtime config endpoint for remote checks.",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    _smoke_local(args.base_dir, require_runtime=args.require_runtime)
    if args.url:
        _smoke_url(args.url, expect_runtime=args.expect_runtime)
    print("Web smoke checks passed.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
