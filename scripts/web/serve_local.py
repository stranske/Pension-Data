#!/usr/bin/env python3
"""Serve the web workspace with an in-perimeter generated or live bundle."""

from __future__ import annotations

import argparse
import ipaddress
import json
import os
import socket
import sys
from collections.abc import Mapping
from http.server import SimpleHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any, BinaryIO
from urllib.parse import unquote, urlsplit

sys.path.insert(0, str(Path(__file__).resolve().parent))
from workspace_contract import validate_workspace_bundle  # noqa: E402

ROOT = Path(__file__).resolve().parents[2]
WEB_ROOT = ROOT / "apps" / "web"
ALLOWED_ORIGINS = frozenset({"generated", "live"})
CSP_HEADER = (
    "default-src 'self'; "
    "script-src 'self'; "
    "connect-src 'self'; "
    "img-src 'self' data: blob:; "
    "style-src 'self' 'unsafe-inline'; "
    "object-src 'none'; "
    "base-uri 'self'"
)
DISALLOWED_LLM_CONFIG_KEYS = frozenset(
    {
        "llmBaseUrl",
        "llmEndpoint",
        "openaiBaseUrl",
        "anthropicBaseUrl",
        "langchainEndpoint",
    }
)


def loopback_host(value: str) -> str:
    """Return a numeric loopback bind address without trusting DNS resolution."""
    host = value.strip().casefold()
    if host in {"localhost", "localhost."}:
        return "127.0.0.1"
    try:
        address = ipaddress.ip_address(host)
    except ValueError:
        pass
    else:
        if address.is_loopback:
            return str(address)
    raise argparse.ArgumentTypeError("--host must be a loopback IP address or localhost")


class IPv6ThreadingHTTPServer(ThreadingHTTPServer):
    address_family = socket.AF_INET6


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
    # Delegate to the shared validator so serve_local enforces the SAME invariants
    # smoke_test does (crucially the contractVersion match it previously skipped).
    # serve's origin policy: generated/live, plus fixture only in explicit demo mode.
    accepted = ALLOWED_ORIGINS | (frozenset({"fixture"}) if allow_fixture_demo else frozenset())
    validate_workspace_bundle(payload, path_label=path_label, accepted_origins=accepted)


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
    parsed = urlsplit(artifact_base_url)
    if (
        parsed.scheme
        or parsed.netloc
        or parsed.query
        or parsed.fragment
        or not artifact_base_url.startswith("/")
        or any(part in {"", ".", ".."} for part in artifact_base_url.strip("/").split("/"))
        or any(char in artifact_base_url for char in "%\\\x00\r\n\t")
        or artifact_base_url.split("/")[1] in {"config", "data"}
    ):
        raise ValueError(
            "artifactBaseUrl must be a non-root local URL path outside /config and /data"
        )
    config = {
        "environment": "internal",
        "apiBaseUrl": "",
        "artifactBaseUrl": artifact_base_url.rstrip("/"),
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
    artifact_root: Path | None = None,
) -> type[SimpleHTTPRequestHandler]:
    artifact_prefix = str(runtime_config["artifactBaseUrl"]).rstrip("/")
    resolved_artifact_root = artifact_root.resolve(strict=True) if artifact_root else None
    if resolved_artifact_root is not None and not resolved_artifact_root.is_dir():
        raise ValueError("artifact root must be a directory")
    workspace_bytes = json.dumps(workspace_bundle, indent=2, sort_keys=True).encode("utf-8")
    config_bytes = json.dumps(runtime_config, indent=2, sort_keys=True).encode("utf-8")

    class InPerimeterWorkspaceHandler(SimpleHTTPRequestHandler):
        def __init__(self, *args: Any, **kwargs: Any) -> None:
            super().__init__(*args, directory=str(web_root), **kwargs)

        def end_headers(self) -> None:
            self.send_header("Content-Security-Policy", CSP_HEADER)
            self.send_header("Referrer-Policy", "no-referrer")
            super().end_headers()

        def _send_json(self, payload: bytes) -> None:
            self.send_response(200)
            self.send_header("Content-Type", "application/json; charset=utf-8")
            self.send_header("Content-Length", str(len(payload)))
            self.send_header("Cache-Control", "no-store")
            self.end_headers()
            self.wfile.write(payload)

        def send_head(self) -> BinaryIO | None:
            # Handle both GET and HEAD before the static server can normalize '..'.
            path = unquote(urlsplit(self.path).path)
            if path != artifact_prefix and not path.startswith(artifact_prefix + "/"):
                return super().send_head()
            if resolved_artifact_root is None:
                self.send_error(404, "No artifact root configured")
                return None
            relative_path = path[len(artifact_prefix) :].lstrip("/")
            if ".." in relative_path.split("/") or "\\" in relative_path or "\x00" in relative_path:
                self.send_error(403, "Artifact path is not permitted")
                return None
            try:
                artifact = (resolved_artifact_root / relative_path).resolve(strict=True)
                if not artifact.is_relative_to(resolved_artifact_root):
                    self.send_error(403, "Artifact path is outside the configured root")
                    return None
                if not artifact.is_file():
                    self.send_error(404, "Artifact file not found")
                    return None
                file = artifact.open("rb")
            except (OSError, RuntimeError, ValueError):
                self.send_error(404, "Artifact file not found")
                return None
            try:
                self.send_response(200)
                self.send_header("Content-Type", self.guess_type(str(artifact)))
                self.send_header("Content-Length", str(os.fstat(file.fileno()).st_size))
                self.send_header("Cache-Control", "no-store")
                self.end_headers()
                return file
            except BaseException:
                file.close()
                raise

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
    parser.add_argument(
        "--host",
        default="127.0.0.1",
        type=loopback_host,
        help="Loopback IP address or localhost only; defaults to 127.0.0.1.",
    )
    parser.add_argument("--port", default=8766, type=int, help="Bind port.")
    parser.add_argument(
        "--artifact-root",
        type=Path,
        help="Directory containing evidence artifacts; required to serve evidence links.",
    )
    parser.add_argument(
        "--artifact-base-url",
        default="/artifacts",
        help="Local URL path for the artifact root; defaults to /artifacts.",
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
    handler = make_handler(
        web_root=args.web_root,
        workspace_bundle=bundle,
        runtime_config=config,
        artifact_root=args.artifact_root,
    )
    server_type = IPv6ThreadingHTTPServer if ":" in args.host else ThreadingHTTPServer
    server = server_type((args.host, args.port), handler)
    url_host = f"[{args.host}]" if ":" in args.host else args.host
    print(f"serving in-perimeter workspace at http://{url_host}:{args.port}/")
    print(f"bundle: {args.bundle} (data_origin={bundle['data_origin']})")
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        print("\nshutting down in-perimeter workspace server")
    finally:
        server.server_close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
