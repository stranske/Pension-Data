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


def test_local_smoke_accepts_checked_in_fixture_bundle() -> None:
    web_smoke_test._smoke_local(WEB_DIR, require_runtime=False)


def test_runtime_required_smoke_rejects_fixture_bundle(tmp_path: Path) -> None:
    base_dir = _copy_web_fixture(tmp_path)
    _write_runtime_config(base_dir)

    with pytest.raises(ValueError, match="fixture workspace bundle is not allowed"):
        web_smoke_test._smoke_local(base_dir, require_runtime=True)


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
    assert "Demo data - not live" in app
    assert "data_origin" in app
