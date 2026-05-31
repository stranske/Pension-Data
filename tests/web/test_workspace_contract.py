"""Tests for the static web workspace data-origin contract."""

from __future__ import annotations

import importlib.util
import json
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[2]
WEB_DIR = ROOT / "apps" / "web"
SMOKE_PATH = ROOT / "scripts" / "web" / "smoke_test.py"

spec = importlib.util.spec_from_file_location("web_smoke_test", SMOKE_PATH)
assert spec is not None and spec.loader is not None
web_smoke_test = importlib.util.module_from_spec(spec)
spec.loader.exec_module(web_smoke_test)


def _copy_web_fixture(tmp_path: Path) -> Path:
    target = tmp_path / "web"
    for relative in web_smoke_test.REQUIRED_LOCAL_FILES:
        source = WEB_DIR / relative
        destination = target / relative
        destination.parent.mkdir(parents=True, exist_ok=True)
        destination.write_bytes(source.read_bytes())
    return target


def _write_runtime_config(base_dir: Path) -> None:
    runtime_path = base_dir / "config" / "runtime.json"
    runtime_path.write_text(
        json.dumps(
            {
                "environment": "runtime-test",
                "apiBaseUrl": "https://api.example.test",
                "artifactBaseUrl": "https://artifacts.example.test",
            }
        ),
        encoding="utf-8",
    )


def test_packaged_workspace_declares_fixture_origin() -> None:
    contract = json.loads((ROOT / "apps" / "contracts" / "runtime-contract.json").read_text())
    workspace = json.loads((WEB_DIR / "data" / "workspace.json").read_text())

    assert "data_origin" in contract["workspaceBundle"]["requiredTopLevelFields"]
    assert contract["workspaceBundle"]["dataOrigins"] == ["fixture", "generated", "live"]
    assert workspace["data_origin"] == "fixture"


def test_runtime_contract_requires_data_origin_top_level_field(tmp_path: Path) -> None:
    contract_path = tmp_path / "runtime-contract.json"
    contract = json.loads((ROOT / "apps" / "contracts" / "runtime-contract.json").read_text())
    contract["workspaceBundle"]["requiredTopLevelFields"] = ["contractVersion", "datasets"]
    contract_path.write_text(json.dumps(contract), encoding="utf-8")

    original_contract_path = web_smoke_test.CONTRACT_PATH
    try:
        web_smoke_test.CONTRACT_PATH = contract_path
        with pytest.raises(ValueError, match="missing required workspace fields: data_origin"):
            web_smoke_test._load_runtime_contract()
    finally:
        web_smoke_test.CONTRACT_PATH = original_contract_path


def test_local_smoke_accepts_checked_in_fixture_bundle() -> None:
    web_smoke_test._smoke_local(WEB_DIR, require_runtime=False)


def test_runtime_required_smoke_rejects_fixture_bundle(tmp_path: Path) -> None:
    base_dir = _copy_web_fixture(tmp_path)
    _write_runtime_config(base_dir)

    with pytest.raises(
        ValueError,
        match=r"fixture workspace bundle is not allowed for runtime smoke: .*data/workspace\.json",
    ):
        web_smoke_test._smoke_local(base_dir, require_runtime=True)


def test_remote_expect_runtime_rejects_fixture_bundle(monkeypatch: pytest.MonkeyPatch) -> None:
    fixture_workspace = json.loads(
        (WEB_DIR / "data" / "workspace.json").read_text(encoding="utf-8")
    )

    def fake_fetch_text(url: str, *, headers: dict[str, str] | None = None) -> str:
        del headers
        if url.endswith("/"):
            return (
                "<html><body>Cloudflare Web Workspace Foundation"
                '<span data-testid="environment-badge"></span>'
                '<span data-testid="data-origin-badge"></span></body></html>'
            )
        if url.endswith("config/default.json"):
            return json.dumps(
                {
                    "environment": "prod",
                    "apiBaseUrl": "https://api.example.test",
                    "artifactBaseUrl": "https://artifacts.example.test",
                }
            )
        if url.endswith("manifest.webmanifest"):
            return json.dumps({"name": "Pension Data", "start_url": "/"})
        if (
            url.endswith("sw.js")
            or url.endswith("icons/pension-data-mark-192.png")
            or url.endswith("icons/pension-data-mark-512.png")
        ):
            return "ok"
        if url.endswith("data/workspace.json"):
            return json.dumps(fixture_workspace)
        raise AssertionError(f"unexpected url: {url}")

    monkeypatch.setattr(web_smoke_test, "_fetch_text", fake_fetch_text)

    with pytest.raises(ValueError, match="fixture workspace bundle is not allowed"):
        web_smoke_test._smoke_url("https://example.test", expect_runtime=True, headers=None)


def test_remote_public_deploy_rejects_live_bundle(monkeypatch: pytest.MonkeyPatch) -> None:
    live_workspace = json.loads((WEB_DIR / "data" / "workspace.json").read_text(encoding="utf-8"))
    live_workspace["data_origin"] = "live"

    def fake_fetch_text(url: str, *, headers: dict[str, str] | None = None) -> str:
        del headers
        if url.endswith("/"):
            return (
                "<html><body>Cloudflare Web Workspace Foundation"
                '<span data-testid="environment-badge"></span>'
                '<span data-testid="data-origin-badge"></span></body></html>'
            )
        if url.endswith("config/default.json"):
            return json.dumps(
                {
                    "environment": "prod",
                    "apiBaseUrl": "https://api.example.test",
                    "artifactBaseUrl": "https://artifacts.example.test",
                }
            )
        if url.endswith("manifest.webmanifest"):
            return json.dumps({"name": "Pension Data", "start_url": "/"})
        if (
            url.endswith("sw.js")
            or url.endswith("icons/pension-data-mark-192.png")
            or url.endswith("icons/pension-data-mark-512.png")
        ):
            return "ok"
        if url.endswith("data/workspace.json"):
            return json.dumps(live_workspace)
        raise AssertionError(f"unexpected url: {url}")

    monkeypatch.setattr(web_smoke_test, "_fetch_text", fake_fetch_text)

    with pytest.raises(
        ValueError,
        match=r"Refusing to deploy non-synthetic bundle to external Cloudflare Pages: .*data/workspace\.json",
    ):
        web_smoke_test._smoke_url("https://example.test", expect_runtime=False, headers=None)


def test_remote_public_deploy_accepts_fixture_bundle(monkeypatch: pytest.MonkeyPatch) -> None:
    fixture_workspace = json.loads(
        (WEB_DIR / "data" / "workspace.json").read_text(encoding="utf-8")
    )

    def fake_fetch_text(url: str, *, headers: dict[str, str] | None = None) -> str:
        del headers
        if url.endswith("/"):
            return (
                "<html><body>Cloudflare Web Workspace Foundation"
                '<span data-testid="environment-badge"></span>'
                '<span data-testid="data-origin-badge"></span></body></html>'
            )
        if url.endswith("config/default.json"):
            return json.dumps(
                {
                    "environment": "prod",
                    "apiBaseUrl": "https://api.example.test",
                    "artifactBaseUrl": "https://artifacts.example.test",
                }
            )
        if url.endswith("manifest.webmanifest"):
            return json.dumps({"name": "Pension Data", "start_url": "/"})
        if (
            url.endswith("sw.js")
            or url.endswith("icons/pension-data-mark-192.png")
            or url.endswith("icons/pension-data-mark-512.png")
        ):
            return "ok"
        if url.endswith("data/workspace.json"):
            return json.dumps(fixture_workspace)
        raise AssertionError(f"unexpected url: {url}")

    monkeypatch.setattr(web_smoke_test, "_fetch_text", fake_fetch_text)

    web_smoke_test._smoke_url("https://example.test", expect_runtime=False, headers=None)


def test_missing_or_unknown_origin_fails_workspace_validation() -> None:
    workspace = json.loads((WEB_DIR / "data" / "workspace.json").read_text())
    workspace.pop("data_origin")
    with pytest.raises(ValueError, match="requires data_origin"):
        web_smoke_test._assert_workspace_bundle(
            workspace,
            path_label="data/workspace.json",
            reject_fixture=False,
        )

    workspace["data_origin"] = "sample"
    workspace["contractVersion"] = "1.0.0"
    with pytest.raises(ValueError, match="requires data_origin"):
        web_smoke_test._assert_workspace_bundle(
            workspace,
            path_label="data/workspace.json",
            reject_fixture=False,
        )


def test_workspace_contract_version_must_match_runtime_contract() -> None:
    workspace = json.loads((WEB_DIR / "data" / "workspace.json").read_text())
    workspace["contractVersion"] = "0.0.0"
    with pytest.raises(ValueError, match="does not match runtime contract"):
        web_smoke_test._assert_workspace_bundle(
            workspace,
            path_label="data/workspace.json",
            reject_fixture=False,
        )


def test_ui_surfaces_fixture_origin_marker() -> None:
    index = (WEB_DIR / "index.html").read_text(encoding="utf-8")
    app = (WEB_DIR / "app.js").read_text(encoding="utf-8")

    assert 'data-testid="data-origin-badge"' in index
    assert "Using packaged fixture bundle (demo data)." in index
    assert "Demo data - not live" in app
    assert "packaged bundle (fixture demo)" in app
    assert "data_origin" in app


def test_fixture_guard_refuses_generated_bundle(tmp_path: Path) -> None:
    base_dir = _copy_web_fixture(tmp_path)
    workspace_path = base_dir / "data" / "workspace.json"
    workspace = json.loads(workspace_path.read_text(encoding="utf-8"))
    workspace["data_origin"] = "generated"
    workspace_path.write_text(json.dumps(workspace), encoding="utf-8")

    with pytest.raises(
        ValueError,
        match=r"Refusing to deploy non-synthetic bundle to external Cloudflare Pages: .*data/workspace\.json",
    ):
        web_smoke_test._smoke_local(base_dir, require_runtime=False, require_fixture=True)
