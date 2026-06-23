from __future__ import annotations

import re
from pathlib import Path

WEB_ROOT = Path("apps/web")
PLOTLY_VENDOR = WEB_ROOT / "vendor" / "plotly-2.35.2.min.js"
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


def test_offline_web_styles_and_worker_have_no_external_urls() -> None:
    # Defense-in-depth for the confidential-data offline posture: the served
    # stylesheet and service worker are first-party code (not config), so they
    # must contain no external URL at all — this catches CSS ``@import``/``url()``
    # font/asset refs and any future external fetch in the worker, which the
    # script/link-tag scan above (index.html + app.js only) would miss.
    for name in ("styles.css", "sw.js"):
        content = (WEB_ROOT / name).read_text(encoding="utf-8")
        normalized_content = content.lower()
        assert (
            "http://" not in normalized_content
            and "https://" not in normalized_content
        ), (
            f"apps/web/{name} references an external URL; the offline bundle "
            "must be fully self-contained"
        )


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
