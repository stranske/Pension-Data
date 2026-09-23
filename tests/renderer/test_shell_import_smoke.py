"""Smoke coverage for the reusable renderer-shell materialization."""

from __future__ import annotations

import importlib.util
import re
import subprocess
import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[2]
PACKAGE_ROOT = ROOT / "packages" / "renderer-shell"
WEB_ROOT = ROOT / "apps" / "web"


def _load_sync_module():
    path = ROOT / "scripts" / "web" / "sync_renderer_shell.py"
    spec = importlib.util.spec_from_file_location("sync_renderer_shell", path)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def test_shell_materialization_is_current() -> None:
    subprocess.run(
        [sys.executable, "scripts/web/sync_renderer_shell.py", "--check"],
        cwd=ROOT,
        check=True,
    )


def test_web_app_imports_materialized_shell_inside_static_root() -> None:
    assert (WEB_ROOT / "app.js").read_text(encoding="utf-8") == (
        'import "./renderer-shell/app.js";\n'
    )
    assert (WEB_ROOT / "renderer-shell" / "app.js").is_file()
    assert "../../packages" not in (WEB_ROOT / "app.js").read_text(encoding="utf-8")


def test_package_entrypoints_and_assets_are_self_contained() -> None:
    index = (PACKAGE_ROOT / "index.html").read_text(encoding="utf-8")
    app = (PACKAGE_ROOT / "app.js").read_text(encoding="utf-8")

    for relative in (
        "index.html",
        "app.js",
        "styles.css",
        "components.css",
        "tokens.css",
        "vendor/plotly-2.35.2.min.js",
    ):
        assert (PACKAGE_ROOT / relative).is_file()
    assert '<script src="./app.js" type="module"></script>' in index
    assert 'const PLOTLY_VENDOR_PATH = "./vendor/plotly-2.35.2.min.js";' in app


def test_service_worker_precaches_wrapper_and_shell_implementation() -> None:
    service_worker = (WEB_ROOT / "sw.js").read_text(encoding="utf-8")
    sync_module = _load_sync_module()

    assert '"./app.js"' in service_worker
    assert '"./renderer-shell/app.js"' in service_worker
    assert (
        f'const CACHE_NAME = "pension-data-web-{sync_module.renderer_shell_digest()}";'
        in service_worker
    )


def test_sync_write_mode_updates_stale_target(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    sync_module = _load_sync_module()
    source = tmp_path / "package" / "app.js"
    target = tmp_path / "web" / "app.js"
    service_worker = tmp_path / "web" / "sw.js"
    source.parent.mkdir(parents=True)
    target.parent.mkdir(parents=True)
    source.write_text("new\n", encoding="utf-8")
    target.write_text("old\n", encoding="utf-8")
    service_worker.write_text(
        'const CACHE_NAME = "pension-data-web-000000000000";\n', encoding="utf-8"
    )
    monkeypatch.setattr(sync_module, "ROOT", tmp_path)
    monkeypatch.setattr(sync_module, "MAPPINGS", {Path("package/app.js"): Path("web/app.js")})
    monkeypatch.setattr(sync_module, "SERVICE_WORKER", Path("web/sw.js"))

    missing, stale = sync_module.sync(check=False)

    assert missing == []
    assert "stale target: web/app.js" in stale
    assert "stale cache version: web/sw.js" in stale
    assert target.read_text(encoding="utf-8") == "new\n"
    assert re.search(r"pension-data-web-[0-9a-f]{12}", service_worker.read_text())


def test_sync_write_mode_fails_for_missing_source(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    sync_module = _load_sync_module()
    monkeypatch.setattr(sync_module, "ROOT", tmp_path)
    monkeypatch.setattr(sync_module, "MAPPINGS", {Path("package/missing.js"): Path("web/app.js")})
    monkeypatch.setattr(sys, "argv", ["sync_renderer_shell.py"])

    assert sync_module.main() == 1


def test_shell_uses_distinct_border_token_and_origin_label() -> None:
    styles = (PACKAGE_ROOT / "styles.css").read_text(encoding="utf-8")
    app = (PACKAGE_ROOT / "app.js").read_text(encoding="utf-8")

    assert "--shell-border:" in styles
    assert "var(--card-border)" not in styles
    assert 'payload.data_origin === "fixture"' in app
    assert '"packaged bundle"' in app
