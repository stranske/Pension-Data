"""Tests for funded extraction fallback parser used by golden corpus replay."""

from __future__ import annotations

import json

from tools.golden_extract.fallback_extract_parser import parse
from tools.replay.harness import CorpusDocument


def _doc(*, document_id: str, payload: dict[str, object]) -> CorpusDocument:
    return CorpusDocument(document_id=document_id, content=json.dumps(payload))


def test_parser_uses_table_primary_when_required_metrics_exist_in_table() -> None:
    payload = {
        "domain": "funded",
        "plan_id": "CA-PERS",
        "plan_period": "FY2025",
        "raw": {
            "source_document_id": "doc:ca:2025:funded",
            "source_url": "https://example.gov/ca-2025-funded.pdf",
            "effective_date": "2025-06-30",
            "ingestion_date": "2026-01-15",
            "default_money_unit_scale": "million_usd",
            "text_blocks": [],
            "table_rows": [
                {"label": "Funded Ratio", "value": "80.0%", "evidence_ref": "p.45"},
                {"label": "AAL", "value": "$600 million", "evidence_ref": "p.45"},
                {"label": "AVA", "value": "$480 million", "evidence_ref": "p.45"},
                {"label": "Discount Rate", "value": "6.75%", "evidence_ref": "p.46"},
                {"label": "Employer Contribution Rate", "value": "12.4%", "evidence_ref": "p.47"},
                {"label": "Employee Contribution Rate", "value": "7.5%", "evidence_ref": "p.47"},
                {"label": "Participant Count", "value": "320000", "evidence_ref": "p.48"},
            ],
        },
    }

    fields = parse(_doc(document_id="doc-table", payload=payload))
    assert "__escalation__" not in fields
    assert fields["__fallback_stage__"].value == "table_primary"
    assert fields["funded_ratio"].evidence == "p.45"


def test_parser_falls_back_to_text_stage_when_table_stage_is_incomplete() -> None:
    payload = {
        "domain": "funded",
        "plan_id": "TX-ERS",
        "plan_period": "FY2025",
        "raw": {
            "source_document_id": "doc:tx:2025:funded",
            "source_url": "https://example.gov/tx-2025-funded.pdf",
            "effective_date": "2025-08-31",
            "ingestion_date": "2026-02-02",
            "default_money_unit_scale": "million_usd",
            "text_blocks": [
                "Funded ratio 81.2%.",
                "Actuarial accrued liability was $410.5 million.",
                "Actuarial value of assets was $333.7 million.",
                "Discount rate remained 6.75%.",
                "Employer contribution rate was 11.1%.",
                "Employee contribution rate was 7.1%.",
                "Participant count was 112000.",
            ],
            "table_rows": [],
        },
    }

    fields = parse(_doc(document_id="doc-text-fallback", payload=payload))
    assert "__escalation__" not in fields
    assert fields["__fallback_stage__"].value == "text_fallback"
    assert fields["funded_ratio"].evidence == "text:1"


def test_parser_emits_escalation_when_all_stages_exhausted() -> None:
    payload = {
        "domain": "funded",
        "plan_id": "WA-SRS",
        "plan_period": "FY2025",
        "raw": {
            "source_document_id": "doc:wa:2025:funded",
            "source_url": "https://example.gov/wa-2025-funded.pdf",
            "effective_date": "2025-06-30",
            "ingestion_date": "2026-01-15",
            "default_money_unit_scale": "million_usd",
            "text_blocks": ["No funded metrics disclosed in this section."],
            "table_rows": [],
        },
    }

    fields = parse(_doc(document_id="doc-escalate", payload=payload))
    escalation = fields["__escalation__"]
    assert escalation.value == "parser_fallback_exhaustion"
    assert escalation.evidence == "stages:3"
