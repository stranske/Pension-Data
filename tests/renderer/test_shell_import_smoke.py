"""Smoke coverage for the reusable renderer-shell materialization."""

from __future__ import annotations

import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
PACKAGE_ROOT = ROOT / "packages" / "renderer-shell"
WEB_ROOT = ROOT / "apps" / "web"


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

    assert '"./app.js"' in service_worker
    assert '"./renderer-shell/app.js"' in service_worker
