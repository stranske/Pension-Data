#!/usr/bin/env python3
"""Smoke checks for web scaffold integrity (local files and deployed URL)."""

from __future__ import annotations

import argparse
import json
import os
from pathlib import Path
from urllib.error import HTTPError, URLError
from urllib.parse import urljoin
from urllib.request import Request, urlopen

REQUIRED_CONFIG_KEYS = ("environment", "apiBaseUrl", "artifactBaseUrl")
REQUIRED_LOCAL_FILES = (
    "index.html",
    "styles.css",
    "app.js",
    "sw.js",
    "manifest.webmanifest",
    "icons/pension-data-mark.svg",
    "icons/pension-data-mark-192.png",
    "icons/pension-data-mark-512.png",
    "config/default.json",
    "data/workspace.json",
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
        "./manifest.webmanifest",
        "./styles.css",
        "./app.js",
    )
    for marker in markers:
        if marker not in index:
            raise ValueError(f"index.html missing marker: {marker}")

    _assert_config(_load_json(base_dir / "config/default.json"), path_label="config/default.json")
    workspace_payload = _load_json(base_dir / "data/workspace.json")
    contract_version = workspace_payload.get("contractVersion")
    if not isinstance(contract_version, str) or not contract_version.strip():
        raise ValueError("workspace bundle missing contractVersion")
    datasets = workspace_payload.get("datasets")
    if not isinstance(datasets, list) or not datasets:
        raise ValueError("workspace dataset inventory is empty")

    runtime_path = base_dir / "config/runtime.json"
    if require_runtime:
        if not runtime_path.exists():
            raise ValueError("runtime config required but missing: config/runtime.json")
        _assert_config(_load_json(runtime_path), path_label="config/runtime.json")


def _fetch_text(url: str, *, headers: dict[str, str] | None = None) -> str:
    request = Request(url, headers=headers or {})
    try:
        with urlopen(request, timeout=20) as response:  # noqa: S310
            body = response.read().decode("utf-8", errors="replace")
            if response.status < 200 or response.status >= 300:
                raise ValueError(f"request failed ({response.status}): {url}")
            return body
    except HTTPError as exc:
        raise ValueError(f"HTTP error {exc.code} for {url}") from exc
    except URLError as exc:
        raise ValueError(f"URL error for {url}: {exc.reason}") from exc


def _smoke_url(base_url: str, *, expect_runtime: bool, headers: dict[str, str] | None) -> None:
    root = base_url.rstrip("/") + "/"
    html = _fetch_text(root, headers=headers)
    if "Cloudflare Web Workspace Foundation" not in html:
        raise ValueError("deployed page missing expected heading marker")
    if 'data-testid="environment-badge"' not in html:
        raise ValueError("deployed page missing environment badge marker")

    default_payload = json.loads(_fetch_text(urljoin(root, "config/default.json"), headers=headers))
    if not isinstance(default_payload, dict):
        raise ValueError("default config endpoint did not return object JSON")
    _assert_config(default_payload, path_label="config/default.json")

    manifest_payload = json.loads(_fetch_text(urljoin(root, "manifest.webmanifest"), headers=headers))
    if not isinstance(manifest_payload, dict):
        raise ValueError("manifest endpoint did not return object JSON")
    if not isinstance(manifest_payload.get("name"), str) or not manifest_payload.get("name"):
        raise ValueError("manifest missing required name field")
    if not isinstance(manifest_payload.get("start_url"), str) or not manifest_payload.get("start_url"):
        raise ValueError("manifest missing required start_url field")

    _fetch_text(urljoin(root, "sw.js"), headers=headers)
    _fetch_text(urljoin(root, "icons/pension-data-mark-192.png"), headers=headers)
    _fetch_text(urljoin(root, "icons/pension-data-mark-512.png"), headers=headers)

    workspace_payload = json.loads(_fetch_text(urljoin(root, "data/workspace.json"), headers=headers))
    if not isinstance(workspace_payload, dict):
        raise ValueError("workspace endpoint did not return object JSON")
    contract_version = workspace_payload.get("contractVersion")
    if not isinstance(contract_version, str) or not contract_version.strip():
        raise ValueError("workspace bundle missing contractVersion")
    datasets = workspace_payload.get("datasets")
    if not isinstance(datasets, list) or not datasets:
        raise ValueError("workspace dataset inventory is empty")

    if expect_runtime:
        runtime_payload = json.loads(
            _fetch_text(urljoin(root, "config/runtime.json"), headers=headers)
        )
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
    parser.add_argument(
        "--cf-access-client-id",
        default=os.getenv("CF_ACCESS_CLIENT_ID", ""),
        help="Optional Cloudflare Access client ID for protected deployments.",
    )
    parser.add_argument(
        "--cf-access-client-secret",
        default=os.getenv("CF_ACCESS_CLIENT_SECRET", ""),
        help="Optional Cloudflare Access client secret for protected deployments.",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    _smoke_local(args.base_dir, require_runtime=args.require_runtime)
    if args.url:
        headers: dict[str, str] = {}
        if args.cf_access_client_id and args.cf_access_client_secret:
            headers["CF-Access-Client-Id"] = args.cf_access_client_id
            headers["CF-Access-Client-Secret"] = args.cf_access_client_secret
        _smoke_url(args.url, expect_runtime=args.expect_runtime, headers=headers or None)
    print("Web smoke checks passed.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
