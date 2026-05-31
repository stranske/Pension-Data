"""Guardrails for public-hosting docs and synthetic-data deployment language."""

from __future__ import annotations

import re
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
DOCS_DIR = ROOT / "docs"
WEB_DIR = ROOT / "apps" / "web"

UI_OPTIONS_DOC = DOCS_DIR / "UI_LANGCHAIN_OPTIONS.md"
CLOUDFLARE_DOC = DOCS_DIR / "deploy" / "CLOUDFLARE_PAGES_SETUP.md"

PUBLIC_HOSTING_DOCS: tuple[Path, ...] = (
    UI_OPTIONS_DOC,
    CLOUDFLARE_DOC,
)

PROHIBITED_ALLOW_PATTERNS: tuple[re.Pattern[str], ...] = (
    re.compile(r"cloudflare\\s+pages.{0,120}data_origin\\s*:\\s*live", re.IGNORECASE),
    re.compile(r"github\\s+pages.{0,120}data_origin\\s*:\\s*live", re.IGNORECASE),
    re.compile(r"publish.{0,120}data_origin\\s*:\\s*live", re.IGNORECASE),
)


def _read(path: Path) -> str:
    return path.read_text(encoding="utf-8")


def test_ui_options_includes_real_data_recommendation_and_llm_boundary() -> None:
    text = _read(UI_OPTIONS_DOC)

    assert (
        "## Option 1: Zero-Egress Browser UI or Internal Hosting (Recommended for Real Data)"
        in text
    )
    assert "GitHub Pages / Cloudflare Pages" in text
    assert "fixture-only" in text
    assert "## Data zones & LLM boundary" in text
    assert "OPENAI_BASE_URL" in text
    assert "ANTHROPIC_BASE_URL" in text
    assert "PENSION_DATA_DATA_ZONE=proprietary" in text


def test_cloudflare_doc_has_fixture_only_banner() -> None:
    text = _read(CLOUDFLARE_DOC)
    assert (
        "> For fixture/synthetic data only. Do not publish bundles whose `data_origin` is `live`."
        in text
    )


def test_public_hosting_docs_do_not_allow_live_bundle_publication() -> None:
    for doc in PUBLIC_HOSTING_DOCS:
        text = _read(doc)
        for pattern in PROHIBITED_ALLOW_PATTERNS:
            assert not pattern.search(
                text
            ), f"public-hosting doc must not allow live bundle publication: {doc}"


def test_review_gate_grep_signal_has_no_live_bundle_in_web_scaffold() -> None:
    workspace = _read(WEB_DIR / "data" / "workspace.json")
    assert '"data_origin": "live"' not in workspace
