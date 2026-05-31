#!/usr/bin/env python3
"""Serve the web workspace with an in-perimeter generated or live bundle."""

from __future__ import annotations

import argparse
import json
from collections.abc import Mapping
from http.server import SimpleHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any
from urllib.parse import urlsplit

ROOT = Path(__file__).resolve().parents[2]
WEB_ROOT = ROOT / "apps" / "web"
ALLOWED_ORIGINS = frozenset({"generated", "live"})
DISALLOWED_LLM_CONFIG_KEYS = frozenset(
    {
        "llmBaseUrl",
        "llmEndpoint",
        "openaiBaseUrl",
        "anthropicBaseUrl",
        "langchainEndpoint",
    }
)


def _load_json(path: Path) -> dict[str, Any]:
    payload = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise ValueError(f"JSON object required: {path}")
    return payload


def _assert_workspace_bundle(
    payload: Mapping[str, Any],
    *,
    path_label: str,
    allow_fixture_demo: bool,
) -> None:
    data_origin = payload.get("data_origin")
    allowed = ALLOWED_ORIGINS | (frozenset({"fixture"}) if allow_fixture_demo else frozenset())
    if data_origin not in allowed:
        allowed_text = ", ".join(sorted(allowed))
        raise ValueError(f"{path_label} must declare data_origin of {allowed_text}")
    datasets = payload.get("datasets")
    if not isinstance(datasets, list) or not datasets:
        raise ValueError(f"{path_label} must contain at least one dataset")


def load_workspace_bundle(path: Path, *, allow_fixture_demo: bool = False) -> dict[str, Any]:
    """Load and validate a real-data in-perimeter workspace bundle."""
    payload = _load_json(path)
    _assert_workspace_bundle(
        payload,
        path_label=str(path),
        allow_fixture_demo=allow_fixture_demo,
    )
    return payload


def is_external_url(value: str) -> bool:
    """Return true when a config URL points outside local/internal browser context."""
    parsed = urlsplit(value)
    if not parsed.scheme and not parsed.netloc:
        return False
    host = (parsed.hostname or "").casefold()
    return host not in {"", "localhost", "127.0.0.1", "::1"}


def build_runtime_config(*, artifact_base_url: str) -> dict[str, Any]:
    """Config for bundle-only real-data viewing with no external API endpoint."""
    config = {
        "environment": "internal",
        "apiBaseUrl": "",
        "artifactBaseUrl": artifact_base_url,
        "enableQueryOverrides": False,
    }
    for key in ("apiBaseUrl", "artifactBaseUrl"):
        if is_external_url(str(config[key])):
            raise ValueError(f"{key} must be empty, relative, localhost, or loopback")
    overlap = DISALLOWED_LLM_CONFIG_KEYS.intersection(config)
    if overlap:
        labels = ", ".join(sorted(overlap))
        raise ValueError(f"runtime config must not include LLM endpoint keys: {labels}")
    return config


def make_handler(
    *,
    web_root: Path,
    workspace_bundle: Mapping[str, Any],
    runtime_config: Mapping[str, Any],
) -> type[SimpleHTTPRequestHandler]:
    workspace_bytes = json.dumps(workspace_bundle, indent=2, sort_keys=True).encode("utf-8")
    config_bytes = json.dumps(runtime_config, indent=2, sort_keys=True).encode("utf-8")

    class InPerimeterWorkspaceHandler(SimpleHTTPRequestHandler):
        def __init__(self, *args: Any, **kwargs: Any) -> None:
            super().__init__(*args, directory=str(web_root), **kwargs)

        def _send_json(self, payload: bytes) -> None:
            self.send_response(200)
            self.send_header("Content-Type", "application/json; charset=utf-8")
            self.send_header("Content-Length", str(len(payload)))
            self.send_header("Cache-Control", "no-store")
            self.end_headers()
            self.wfile.write(payload)

        def do_GET(self) -> None:  # noqa: N802 - stdlib handler API
            path = urlsplit(self.path).path
            if path == "/data/workspace.json":
                self._send_json(workspace_bytes)
                return
            if path in {"/config/default.json", "/config/runtime.json"}:
                self._send_json(config_bytes)
                return
            super().do_GET()

    return InPerimeterWorkspaceHandler


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--bundle",
        required=True,
        type=Path,
        help="Generated or live workspace JSON bundle to serve as /data/workspace.json.",
    )
    parser.add_argument(
        "--web-root",
        default=WEB_ROOT,
        type=Path,
        help="Static web app root; defaults to apps/web.",
    )
    parser.add_argument("--host", default="127.0.0.1", help="Bind host; loopback by default.")
    parser.add_argument("--port", default=8766, type=int, help="Bind port.")
    parser.add_argument(
        "--artifact-base-url",
        default="/artifacts",
        help="Relative, localhost, or loopback artifact URL exposed to the SPA.",
    )
    parser.add_argument(
        "--allow-fixture-demo",
        action="store_true",
        help="Allow fixture bundles for dry-run demos; real-data review should omit this.",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    bundle = load_workspace_bundle(args.bundle, allow_fixture_demo=args.allow_fixture_demo)
    config = build_runtime_config(artifact_base_url=args.artifact_base_url)
    handler = make_handler(web_root=args.web_root, workspace_bundle=bundle, runtime_config=config)
    server = ThreadingHTTPServer((args.host, args.port), handler)
    print(f"serving in-perimeter workspace at http://{args.host}:{args.port}/")
    print(f"bundle: {args.bundle} (data_origin={bundle['data_origin']})")
    server.serve_forever()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
