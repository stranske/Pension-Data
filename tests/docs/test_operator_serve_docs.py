"""Guard the artifact-root requirement in proprietary serving instructions."""

from __future__ import annotations

from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
OPERATOR_SERVE_DOCS: tuple[Path, ...] = (
    ROOT / "README.md",
    ROOT / "docs" / "deploy" / "PC_ZERO_INSTALL_IT_REVIEW.md",
    ROOT / "docs" / "deploy" / "IN_PERIMETER_REAL_DATA_REVIEW.md",
)


def test_proprietary_serve_docs_mention_query_artifact_root() -> None:
    for path in OPERATOR_SERVE_DOCS:
        text = path.read_text(encoding="utf-8")
        assert (
            "PENSION_DATA_QUERY_ARTIFACT_ROOT" in text
        ), f"{path.relative_to(ROOT)} must document the proprietary query artifact root"
        assert (
            "one_pdf_pilot/<run-id>" in text
        ), f"{path.relative_to(ROOT)} must document the expected pilot output layout"
