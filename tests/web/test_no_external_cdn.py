from __future__ import annotations

import re
from pathlib import Path

WEB_ROOT = Path("apps/web")
PLOTLY_VENDOR = WEB_ROOT / "vendor" / "plotly-2.35.2.min.js"
TOKENS_CSS = WEB_ROOT / "tokens.css"
COMPONENTS_CSS = WEB_ROOT / "components.css"
EXTERNAL_ASSET_RE = re.compile(
    r"""<(?:script|link)\b[^>]+(?:src|href)=["']https?://""",
    re.IGNORECASE,
)


def test_offline_web_bundle_uses_no_external_scripts_or_styles() -> None:
    for path in [WEB_ROOT / "index.html", WEB_ROOT / "app.js"]:
        content = path.read_text(encoding="utf-8")
        assert not EXTERNAL_ASSET_RE.search(content), (
            f"{path} references an external script/style asset"
        )
        assert "cdn.plot.ly" not in content


def test_offline_web_bundle_links_shared_design_system_locally() -> None:
    index_html = (WEB_ROOT / "index.html").read_text(encoding="utf-8")
    service_worker = (WEB_ROOT / "sw.js").read_text(encoding="utf-8")

    assert TOKENS_CSS.exists()
    assert COMPONENTS_CSS.exists()
    assert '<link rel="stylesheet" href="./tokens.css" />' in index_html
    assert '<link rel="stylesheet" href="./components.css" />' in index_html
    assert '<main id="app" class="ds theme-air layout"' in index_html
    assert "https://" not in re.sub(r"https://github.com/[^\"]+", "", index_html)
    assert '"./tokens.css"' in service_worker
    assert '"./components.css"' in service_worker


def test_plotly_is_vendored_and_precached_for_offline_chart_studio() -> None:
    index_html = (WEB_ROOT / "index.html").read_text(encoding="utf-8")
    app_js = (WEB_ROOT / "app.js").read_text(encoding="utf-8")
    service_worker = (WEB_ROOT / "sw.js").read_text(encoding="utf-8")

    assert PLOTLY_VENDOR.exists()
    assert (
        "Licensed under the MIT license"
        in PLOTLY_VENDOR.read_text(encoding="utf-8", errors="ignore")[:500]
    )
    assert "./vendor/plotly-2.35.2.min.js" in index_html
    assert "./vendor/plotly-2.35.2.min.js" in app_js
    assert "./vendor/plotly-2.35.2.min.js" in service_worker
    assert "offline Plotly bundle did not load" in app_js
    assert "escapeInlineScript(plotlySource)" in app_js
