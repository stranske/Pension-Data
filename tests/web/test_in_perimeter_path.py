"""Tests for the in-perimeter real-data web workspace path."""

from __future__ import annotations

import errno
import importlib.util
import json
import socket
import sys
import threading
from pathlib import Path
from unittest.mock import Mock
from urllib.request import urlopen

import pytest

ROOT = Path(__file__).resolve().parents[2]
SERVE_LOCAL_PATH = ROOT / "scripts" / "web" / "serve_local.py"

spec = importlib.util.spec_from_file_location("serve_local", SERVE_LOCAL_PATH)
assert spec is not None and spec.loader is not None
serve_local = importlib.util.module_from_spec(spec)
spec.loader.exec_module(serve_local)


def _generated_bundle(tmp_path: Path) -> Path:
    bundle = {
        "contractVersion": "1.0.0",
        "data_origin": "generated",
        "datasets": [
            {
                "domain": "pension",
                "freshness": "generated",
                "id": "one-pdf-pilot-review",
                "kind": "core_metrics",
                "lastUpdated": "2026-05-30",
                "name": "Generated review bundle",
                "rows": [
                    {
                        "confidence": 0.95,
                        "entity": "CA-PERS",
                        "metric": "funded_ratio",
                        "metric_family": "funded_status",
                        "plan_period": "FY2024",
                        "provenance": {
                            "evidence_refs": ["page=52"],
                            "source_document": "calpers-fy2024",
                        },
                        "value": 0.81,
                    }
                ],
            }
        ],
    }
    path = tmp_path / "workspace.json"
    path.write_text(json.dumps(bundle), encoding="utf-8")
    return path


def _fetch_json(url: str) -> dict[str, object]:
    with urlopen(url, timeout=5) as response:  # noqa: S310 - local test server
        payload = json.loads(response.read().decode("utf-8"))
    assert isinstance(payload, dict)
    return payload


def _fetch_header(url: str, header: str) -> str:
    with urlopen(url, timeout=5) as response:  # noqa: S310 - local test server
        value: str = response.headers.get(header, "")
    return value


def test_local_server_serves_generated_bundle_and_non_external_config(tmp_path: Path) -> None:
    bundle = serve_local.load_workspace_bundle(_generated_bundle(tmp_path))
    config = serve_local.build_runtime_config(artifact_base_url="/artifacts")
    handler = serve_local.make_handler(
        web_root=ROOT / "apps" / "web",
        workspace_bundle=bundle,
        runtime_config=config,
    )
    try:
        server = serve_local.ThreadingHTTPServer(("127.0.0.1", 0), handler)
    except PermissionError as exc:
        pytest.skip(f"socket bind not permitted in this environment: {exc}")
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        base_url = f"http://127.0.0.1:{server.server_port}"
        workspace = _fetch_json(f"{base_url}/data/workspace.json")
        served_config = _fetch_json(f"{base_url}/config/default.json")
        csp = _fetch_header(f"{base_url}/", "Content-Security-Policy")
    finally:
        server.shutdown()
        thread.join(timeout=5)
        server.server_close()

    assert workspace["data_origin"] == "generated"
    assert workspace["datasets"]
    assert "script-src 'self'" in csp
    assert "connect-src 'self'" in csp
    assert served_config["apiBaseUrl"] == ""
    assert served_config["artifactBaseUrl"] == "/artifacts"
    assert served_config["enableQueryOverrides"] is False
    assert not serve_local.is_external_url(str(served_config["apiBaseUrl"]))
    assert not serve_local.is_external_url(str(served_config["artifactBaseUrl"]))
    assert serve_local.DISALLOWED_LLM_CONFIG_KEYS.isdisjoint(served_config)


def test_fixture_bundle_is_rejected_for_real_data_path(tmp_path: Path) -> None:
    path = _generated_bundle(tmp_path)
    payload = json.loads(path.read_text(encoding="utf-8"))
    payload["data_origin"] = "fixture"
    path.write_text(json.dumps(payload), encoding="utf-8")

    with pytest.raises(ValueError, match="data_origin of generated, live"):
        serve_local.load_workspace_bundle(path)


def test_external_artifact_url_is_rejected() -> None:
    with pytest.raises(ValueError, match="artifactBaseUrl"):
        serve_local.build_runtime_config(artifact_base_url="https://example.test/artifacts")


def test_runtime_config_has_no_llm_endpoint_keys() -> None:
    config = serve_local.build_runtime_config(artifact_base_url="/artifacts")
    assert serve_local.DISALLOWED_LLM_CONFIG_KEYS.isdisjoint(config)


@pytest.mark.parametrize(
    "host",
    [
        "0.0.0.0",
        "::",
        "192.168.1.10",
        "10.0.0.1",
        "8.8.8.8",
        "2001:db8::1",
        "localhost.example.com",
        "example.com",
        "",
        "   ",
        "127.0.0.1.example.com",
        "127.1",
    ],
)
def test_in_perimeter_server_rejects_non_loopback_host(
    host: str, tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    server = Mock()
    monkeypatch.setattr(serve_local, "ThreadingHTTPServer", server)
    monkeypatch.setattr(serve_local, "IPv6ThreadingHTTPServer", server)
    monkeypatch.setattr(
        sys,
        "argv",
        [str(SERVE_LOCAL_PATH), "--bundle", str(_generated_bundle(tmp_path)), "--host", host],
    )

    with pytest.raises(SystemExit) as exc:
        serve_local.main()

    assert exc.value.code == 2
    assert "--host must be a loopback IP address or localhost" in capsys.readouterr().err
    server.assert_not_called()


@pytest.mark.parametrize(
    ("host", "expected", "ipv6"),
    [
        (None, "127.0.0.1", False),
        ("127.0.0.1", "127.0.0.1", False),
        ("127.0.0.2", "127.0.0.2", False),
        ("localhost", "127.0.0.1", False),
        (" LOCALHOST. ", "127.0.0.1", False),
        ("::1", "::1", True),
        ("0:0:0:0:0:0:0:1", "::1", True),
    ],
)
def test_in_perimeter_server_accepts_loopback_host(
    host: str | None,
    expected: str,
    ipv6: bool,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    ipv4_server, ipv6_server = Mock(), Mock()
    monkeypatch.setattr(serve_local, "ThreadingHTTPServer", ipv4_server)
    monkeypatch.setattr(serve_local, "IPv6ThreadingHTTPServer", ipv6_server)
    argv = [str(SERVE_LOCAL_PATH), "--bundle", str(_generated_bundle(tmp_path))]
    if host is not None:
        argv.extend(["--host", host])
    monkeypatch.setattr(sys, "argv", argv)

    assert serve_local.main() == 0

    selected, unused = (ipv6_server, ipv4_server) if ipv6 else (ipv4_server, ipv6_server)
    selected.assert_called_once()
    assert selected.call_args.args[0] == (expected, 8766)
    unused.assert_not_called()
    selected.return_value.serve_forever.assert_called_once_with()
    selected.return_value.server_close.assert_called_once_with()
    url_host = f"[{expected}]" if ipv6 else expected
    assert f"http://{url_host}:8766/" in capsys.readouterr().out


@pytest.mark.skipif(not socket.has_ipv6, reason="IPv6 unavailable")
def test_in_perimeter_ipv6_server_binds_loopback() -> None:
    try:
        server = serve_local.IPv6ThreadingHTTPServer(
            ("::1", 0), serve_local.SimpleHTTPRequestHandler
        )
    except OSError as exc:
        if exc.errno in {
            errno.EACCES,
            errno.EPERM,
            errno.EAFNOSUPPORT,
            errno.EPROTONOSUPPORT,
            errno.EADDRNOTAVAIL,
        }:
            pytest.skip(f"IPv6 loopback bind unavailable in this environment: {exc}")
        raise
    try:
        assert server.address_family == socket.AF_INET6
        assert server.server_address[0] == "::1"
    finally:
        server.server_close()
