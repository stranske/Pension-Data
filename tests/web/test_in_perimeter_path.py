"""Tests for the in-perimeter real-data web workspace path."""

from __future__ import annotations

import errno
import importlib.util
import json
import socket
import sys
import threading
from collections.abc import Iterator
from contextlib import contextmanager
from pathlib import Path
from typing import BinaryIO, cast
from unittest.mock import Mock
from urllib.error import HTTPError
from urllib.parse import quote
from urllib.request import Request, urlopen

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
                            "source_document": "documents/annual report.pdf",
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


@contextmanager
def _artifact_server(tmp_path: Path, artifact_base_url: str = "/artifacts") -> Iterator[str]:
    artifact_root = tmp_path / "evidence"
    artifact_root.mkdir(exist_ok=True)
    handler = serve_local.make_handler(
        web_root=ROOT / "apps" / "web",
        workspace_bundle=serve_local.load_workspace_bundle(_generated_bundle(tmp_path)),
        runtime_config=serve_local.build_runtime_config(artifact_base_url=artifact_base_url),
        artifact_root=artifact_root,
    )
    server = serve_local.ThreadingHTTPServer(("127.0.0.1", 0), handler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        yield f"http://127.0.0.1:{server.server_port}"
    finally:
        server.shutdown()
        thread.join(timeout=5)
        server.server_close()


@pytest.mark.parametrize("artifact_base_url", ["/artifacts", "/review/evidence/"])
def test_local_server_serves_artifact_links_from_configured_root(
    tmp_path: Path, artifact_base_url: str
) -> None:
    evidence = tmp_path / "evidence" / "documents"
    evidence.mkdir(parents=True)
    content = b"%PDF-1.4\nlocal evidence bytes\n"
    (evidence / "annual report.pdf").write_bytes(content)
    with _artifact_server(tmp_path, artifact_base_url) as base_url:
        config = _fetch_json(f"{base_url}/config/runtime.json")
        with urlopen(f"{base_url}/data/workspace.json", timeout=5) as response:
            workspace = json.load(response)
        provenance = workspace["datasets"][0]["rows"][0]["provenance"]
        # Match app.js: encodeURIComponent(source_document) plus the evidence token.
        document = quote(provenance["source_document"], safe="")
        token = quote(provenance["evidence_refs"][0], safe="")
        artifact_url = f"{base_url}{config['artifactBaseUrl']}/{document}#{token}"
        with urlopen(artifact_url, timeout=5) as response:
            assert response.status == 200
            assert response.read() == content
            assert response.headers["Content-Type"] == "application/pdf"
            assert response.headers["Content-Length"] == str(len(content))
            assert response.headers["Content-Security-Policy"] == serve_local.CSP_HEADER
            assert response.headers["Referrer-Policy"] == "no-referrer"
            assert response.headers["Cache-Control"] == "no-store"
        with urlopen(Request(artifact_url, method="HEAD"), timeout=5) as response:
            assert response.status == 200
            assert response.read() == b""
            assert response.headers["Content-Length"] == str(len(content))
        assert _fetch_json(f"{base_url}/data/workspace.json")["data_origin"] == "generated"
        with urlopen(f"{base_url}/app.js", timeout=5) as response:
            assert response.status == 200
            assert response.read() == b'import "./renderer-shell/app.js";\n'
        with urlopen(f"{base_url}/renderer-shell/app.js", timeout=5) as response:
            assert response.status == 200
            assert b"Demo data - not live" in response.read()


def test_local_server_refuses_artifact_traversal_and_directory_listing(tmp_path: Path) -> None:
    root = tmp_path / "evidence"
    root.mkdir()
    outside = tmp_path / "private.txt"
    outside.write_text("must never be served")
    sibling = tmp_path / "evidence-private"
    sibling.mkdir()
    (sibling / "secret.txt").write_text("sibling secret")
    (root / "escape.txt").symlink_to(outside)
    (root / "escape-dir").symlink_to(sibling, target_is_directory=True)
    rejected = {
        "../private.txt": 403,
        "%2e%2e/private.txt": 403,
        "%2e%2e%2fprivate.txt": 403,
        "nested/../../private.txt": 403,
        "..%5cprivate.txt": 403,
        "escape.txt": 403,
        "escape-dir/secret.txt": 403,
        "%00": 403,
        "%252e%252e/private.txt": 404,
        "missing.pdf": 404,
        "": 404,
    }
    with _artifact_server(tmp_path) as base_url:
        for method in ("GET", "HEAD"):
            for suffix, status in rejected.items():
                with pytest.raises(HTTPError) as exc:
                    urlopen(Request(f"{base_url}/artifacts/{suffix}", method=method), timeout=5)
                assert exc.value.code == status, (method, suffix)
                assert exc.value.headers["Content-Security-Policy"] == serve_local.CSP_HEADER
                assert exc.value.headers["Cache-Control"] == "no-store"
                exc.value.close()


@pytest.mark.parametrize(
    "base_url",
    [
        "",
        "/",
        "artifacts",
        "//localhost/artifacts",
        "http://localhost/artifacts",
        "/artifacts?x=1",
        "/artifacts#page",
        "/a/../b",
        "/a//b",
        "/a%2fb",
        "/a\\b",
        "/config",
        "/data/evidence",
    ],
)
def test_artifact_base_url_requires_a_local_mount_path(base_url: str) -> None:
    with pytest.raises(ValueError, match="artifactBaseUrl"):
        serve_local.build_runtime_config(artifact_base_url=base_url)


def test_main_passes_artifact_root_to_handler(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    root = tmp_path / "evidence"
    root.mkdir()
    make_handler = Mock(wraps=serve_local.make_handler)
    monkeypatch.setattr(serve_local, "make_handler", make_handler)
    monkeypatch.setattr(serve_local, "ThreadingHTTPServer", Mock())
    monkeypatch.setattr(
        sys,
        "argv",
        [
            str(SERVE_LOCAL_PATH),
            "--bundle",
            str(_generated_bundle(tmp_path)),
            "--artifact-root",
            str(root),
            "--artifact-base-url",
            "/evidence",
        ],
    )
    assert serve_local.main() == 0
    assert make_handler.call_args.kwargs["artifact_root"] == root
    assert make_handler.call_args.kwargs["runtime_config"]["artifactBaseUrl"] == "/evidence"


def test_artifact_route_without_root_does_not_fall_back_to_static_files(tmp_path: Path) -> None:
    (tmp_path / "artifacts").mkdir()
    (tmp_path / "artifacts" / "secret.txt").write_text("not an evidence root")
    handler = serve_local.make_handler(
        web_root=tmp_path,
        workspace_bundle=serve_local.load_workspace_bundle(_generated_bundle(tmp_path)),
        runtime_config=serve_local.build_runtime_config(artifact_base_url="/artifacts"),
    )
    server = serve_local.ThreadingHTTPServer(("127.0.0.1", 0), handler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        with pytest.raises(HTTPError) as exc:
            urlopen(f"http://127.0.0.1:{server.server_port}/artifacts/secret.txt", timeout=5)
        assert exc.value.code == 404
        exc.value.close()
    finally:
        server.shutdown()
        thread.join(timeout=5)
        server.server_close()


@pytest.mark.parametrize("root_kind", ["missing", "file"])
def test_invalid_artifact_root_is_rejected_before_listening(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, root_kind: str
) -> None:
    root = tmp_path / "evidence"
    if root_kind == "file":
        root.write_text("not a directory")
    server = Mock()
    monkeypatch.setattr(serve_local, "ThreadingHTTPServer", server)
    monkeypatch.setattr(
        sys,
        "argv",
        [
            str(SERVE_LOCAL_PATH),
            "--bundle",
            str(_generated_bundle(tmp_path)),
            "--artifact-root",
            str(root),
        ],
    )
    with pytest.raises((FileNotFoundError, ValueError)):
        serve_local.main()
    server.assert_not_called()


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


@pytest.mark.parametrize("replacement", ["file", "directory", "root"])
@pytest.mark.parametrize("method", ["GET", "HEAD"])
def test_artifact_replacement_race_cannot_escape_root(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, replacement: str, method: str
) -> None:
    root = tmp_path / "evidence"
    document_dir = root / "documents"
    document_dir.mkdir(parents=True)
    document = document_dir / "report.pdf"
    document.write_bytes(b"intended artifact")
    outside = tmp_path / "private"
    (outside / "documents").mkdir(parents=True)
    secret = outside / "documents" / "report.pdf"
    secret.write_bytes(b"outside-root secret must never be served")
    original_open = serve_local._open_artifact_file
    replaced = False

    def replace_before_open(path: Path) -> BinaryIO:
        nonlocal replaced
        assert path == document.resolve()
        # Controlled seam: validation completed, but no file descriptor was opened.
        if replacement == "file":
            document.unlink()
            document.symlink_to(secret)
        elif replacement == "directory":
            document_dir.rename(root / "original-documents")
            document_dir.symlink_to(secret.parent, target_is_directory=True)
        else:
            root.rename(tmp_path / "original-evidence")
            root.symlink_to(outside, target_is_directory=True)
        replaced = True
        return cast(BinaryIO, original_open(path))

    monkeypatch.setattr(serve_local, "_open_artifact_file", replace_before_open)
    with _artifact_server(tmp_path) as base_url:
        with pytest.raises(HTTPError) as exc:
            urlopen(
                Request(f"{base_url}/artifacts/documents%2Freport.pdf", method=method),
                timeout=5,
            )
        assert replaced
        assert exc.value.code == 403
        assert exc.value.headers["Cache-Control"] == "no-store"
        assert b"outside-root secret" not in exc.value.read()
        exc.value.close()


@pytest.mark.parametrize(
    "endpoint", ["/data/workspace.json", "/config/default.json", "/config/runtime.json"]
)
def test_dynamic_json_head_matches_generated_get(tmp_path: Path, endpoint: str) -> None:
    with _artifact_server(tmp_path) as base_url:
        with urlopen(base_url + endpoint, timeout=5) as response:
            payload = response.read()
            content_type = response.headers["Content-Type"]
        with urlopen(Request(base_url + endpoint, method="HEAD"), timeout=5) as response:
            assert response.status == 200
            assert response.read() == b""
            assert response.headers["Content-Length"] == str(len(payload))
            assert response.headers["Content-Type"] == content_type
            assert response.headers["Cache-Control"] == "no-store"
            assert response.headers["Content-Security-Policy"] == serve_local.CSP_HEADER


def _render_citation_href_from_shell(
    *,
    app_js_source: str,
    runtime_config: dict[str, object],
    workspace: dict[str, object],
) -> str:
    import shutil
    import subprocess

    if shutil.which("node") is None:
        pytest.fail("node is required for pilot citation renderer coverage")

    script = f"""
const vm = require("vm");
class Element {{
  constructor(tag = "") {{
    this.tag = tag;
    this.children = [];
    this.listeners = {{}};
    this._text = "";
    this.value = "";
    this.href = "";
    this.dataset = {{}};
    this.classList = {{ add() {{}} }};
  }}
  get textContent() {{ return this._text; }}
  set textContent(value) {{ this._text = String(value); this.children = []; }}
  appendChild(child) {{ this.children.push(child); return child; }}
  append(...children) {{ children.forEach((child) => this.appendChild(child)); }}
  addEventListener(name, handler) {{ (this.listeners[name] ||= []).push(handler); }}
  setAttribute() {{}}
}}
const elements = new Map();
const byId = (id) => elements.get(id) || (elements.set(id, new Element(id)), elements.get(id));
const links = [];
const document = {{
  getElementById: byId,
  createElement: (tag) => {{
    const el = new Element(tag);
    if (tag === "a") links.push(el);
    return el;
  }},
  createDocumentFragment: () => new Element("#fragment"),
  createTextNode: (text) => {{ const node = new Element("#text"); node.textContent = text; return node; }},
  querySelector: () => new Element(),
}};
const source = {json.dumps(app_js_source)}
  .replace(/init\\(\\)\\.catch\\([\\s\\S]*$/, "")
  + "\\nglobalThis.__app = {{ state, renderDetail }};";
const context = {{ document, Node: Element, window: {{ setTimeout: () => 0, clearTimeout: () => {{}} }}, console, URLSearchParams }};
vm.runInNewContext(source, context);
const {{ state, renderDetail }} = context.__app;
byId("detail-content");
const workspace = {json.dumps(workspace)};
state.config = {json.dumps(runtime_config)};
state.datasets = workspace.datasets;
state.selectedDatasetId = workspace.datasets[0].id;
state.selectedRowIndex = 0;
renderDetail();
if (!links.length || !links[0].href) {{
  throw new Error("no citation link rendered");
}}
process.stdout.write(links[0].href);
"""
    completed = subprocess.run(
        ["node", "-e", script],
        check=True,
        capture_output=True,
        text=True,
        cwd=ROOT,
    )
    return completed.stdout.strip()


def test_pilot_default_document_id_citation_serves_source_pdf(tmp_path: Path) -> None:
    import hashlib

    from pension_data.ops.one_pdf_pilot import OnePdfPilotInput, run_one_pdf_pilot

    builder_path = ROOT / "scripts" / "web" / "build_workspace_bundle.py"
    builder_spec = importlib.util.spec_from_file_location(
        "build_workspace_bundle_for_pilot_citation", builder_path
    )
    assert builder_spec is not None and builder_spec.loader is not None
    builder_module = importlib.util.module_from_spec(builder_spec)
    builder_spec.loader.exec_module(builder_module)

    fixture_pdf = ROOT / "tests" / "parser" / "fixtures" / "calpers_fy2024_excerpt.pdf"
    expected_bytes = fixture_pdf.read_bytes()
    expected_sha256 = hashlib.sha256(expected_bytes).hexdigest()

    pilot_result = run_one_pdf_pilot(
        pilot_input=OnePdfPilotInput(
            pdf_path=fixture_pdf,
            plan_id="CA-PERS",
            plan_period="FY2024",
            effective_date="2024-06-30",
            ingestion_date="2026-01-01",
        ),
        output_root=tmp_path / "outputs",
        run_id="calpers-citation-pilot",
    )
    run_root = Path(pilot_result["run_manifest_json"]).parent
    direct_bundle = serve_local.load_workspace_bundle(Path(pilot_result["workspace_bundle_json"]))
    rebuilt_bundle = builder_module.build_workspace_bundle(run_root)

    handler = serve_local.make_handler(
        web_root=ROOT / "apps" / "web",
        workspace_bundle=rebuilt_bundle,
        runtime_config=serve_local.build_runtime_config(artifact_base_url="/artifacts"),
        artifact_root=run_root,
    )
    server = serve_local.ThreadingHTTPServer(("127.0.0.1", 0), handler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        base_url = f"http://127.0.0.1:{server.server_port}"
        runtime_config = _fetch_json(f"{base_url}/config/runtime.json")
        workspace = _fetch_json(f"{base_url}/data/workspace.json")
        with urlopen(f"{base_url}/renderer-shell/app.js", timeout=5) as app_response:
            app_js_source = app_response.read().decode("utf-8")
        citation_href = _render_citation_href_from_shell(
            app_js_source=app_js_source,
            runtime_config=runtime_config,
            workspace=workspace,
        )
        with urlopen(f"{base_url}{citation_href}", timeout=5) as pdf_response:
            assert pdf_response.status == 200
            assert pdf_response.read() == expected_bytes
            assert pdf_response.headers["Content-Type"] == "application/pdf"
            assert pdf_response.headers["Content-Length"] == str(len(expected_bytes))
    finally:
        server.shutdown()
        thread.join(timeout=5)
        server.server_close()

    assert rebuilt_bundle == direct_bundle
    expected_locator = f"sources/{expected_sha256}.pdf"
    expected_doc_id = f"pilot:CA-PERS:FY2024:{expected_sha256[:16]}"
    assert citation_href == f"/artifacts/{expected_locator}#page=2"
    for bundle in (direct_bundle, rebuilt_bundle):
        rows = bundle["datasets"][0]["rows"]
        assert rows
        for row in rows:
            provenance = row["provenance"]
            assert provenance["source_document"] == expected_doc_id
            assert provenance["source_locator"] == expected_locator


def test_desktop_runtime_contract_validator_enforces_provenance_parity() -> None:
    import shutil
    import subprocess

    if shutil.which("node") is None:
        pytest.fail("node is required for desktop runtime contract validator coverage")

    validator_url = (
        ROOT / "apps" / "mac-desktop" / "scripts" / "validate_runtime_contract.mjs"
    ).as_uri()
    contract_path = ROOT / "apps" / "contracts" / "runtime-contract.json"
    script = f"""
import fs from "node:fs";
import {{ validateRuntimeContract }} from {json.dumps(validator_url)};

const contract = JSON.parse(fs.readFileSync({json.dumps(str(contract_path))}, "utf8"));
const makeBundle = (provenance) => ({{
  contractVersion: contract.version,
  data_origin: "generated",
  datasets: [{{
    id: "pilot",
    name: "Pilot",
    domain: "pension",
    kind: "core_metrics",
    freshness: "generated",
    lastUpdated: "2026-01-01",
    rows: [{{ entity: "CA-PERS", provenance }}],
  }}],
}});

validateRuntimeContract(makeBundle({{
  source_document: "pilot:CA-PERS:FY2024:deadbeef",
  evidence_refs: ["p.2"],
  source_locator: "sources/abc123.pdf",
}}), contract);

for (const bad of [
  {{ source_document: null, evidence_refs: ["p.2"] }},
  {{ source_document: "   ", evidence_refs: ["p.2"] }},
  {{ source_document: "pilot:CA-PERS:FY2024:deadbeef", evidence_refs: 17 }},
  {{ source_document: "pilot:CA-PERS:FY2024:deadbeef", evidence_refs: [""] }},
  {{ source_document: "pilot:CA-PERS:FY2024:deadbeef", evidence_refs: ["p.2"], source_locator: "../escape.pdf" }},
  {{ source_document: "pilot:CA-PERS:FY2024:deadbeef", evidence_refs: ["p.2"], source_locator: "pilot:CA-PERS:FY2024:deadbeef" }},
]) {{
  let failed = false;
  try {{
    validateRuntimeContract(makeBundle(bad), contract);
  }} catch {{
    failed = true;
  }}
  if (!failed) {{
    throw new Error("expected validator failure for: " + JSON.stringify(bad));
  }}
}}
"""
    subprocess.run(
        ["node", "--input-type=module", "-e", script],
        check=True,
        capture_output=True,
        text=True,
        cwd=ROOT,
    )
