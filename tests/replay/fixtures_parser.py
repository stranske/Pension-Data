"""Parser fixtures used by replay runner tests."""

from __future__ import annotations

from tools.replay.harness import CorpusDocument


def parser(document: CorpusDocument) -> dict[str, object]:
    """Return deterministic field data for replay tests."""
    funded_ratio = 0.80 if document.document_id == "doc-a" else 0.72
    manager_count = 23 if document.document_id == "doc-a" else 17
    return {
        "manager_count": {
            "value": manager_count,
            "confidence": 0.95,
            "evidence": f"{document.document_id}-page-2",
        },
        "funded_ratio": funded_ratio,
    }
