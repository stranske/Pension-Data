"""Coverage for document-orchestration table-row parsing helpers."""

from __future__ import annotations

from pension_data.extract.actuarial.metrics import RawFundedActuarialInput
from pension_data.ops import document_orchestration as orchestration


def _raw(table_rows: tuple[dict[str, str], ...]) -> RawFundedActuarialInput:
    return RawFundedActuarialInput(
        source_document_id="doc:test",
        source_url="https://example.test/report.pdf",
        effective_date="2024-06-30",
        ingestion_date="2026-01-01",
        default_money_unit_scale="million_usd",
        text_blocks=(),
        table_rows=table_rows,
    )


def test_table_entries_skip_blank_rows() -> None:
    raw = _raw(
        (
            {"label": "", "value": "", "evidence_ref": ""},
            {"label": "Funded Ratio", "value": "78%", "evidence_ref": "p.1"},
        )
    )
    entries = orchestration._table_entries(raw)
    assert len(entries) == 1
    assert entries[0][0] == "Funded Ratio"


def test_fee_type_from_token_recognizes_spaced_labels() -> None:
    assert orchestration._fee_type_from_token("management fee") == "management_fee"
    assert orchestration._fee_type_from_token("performance fee") == "performance_fee"
    assert orchestration._fee_type_from_token("administrative fee") == "other_fee"


def test_risk_kind_from_tokens_rejects_short_labels() -> None:
    assert orchestration._risk_kind_from_tokens(["risk"]) is None


def test_build_allocation_inputs_use_amount_when_not_percent_disclosure() -> None:
    raw = _raw(
        (
            {
                "label": "allocation:Private Equity",
                "value": "$120 million",
                "evidence_ref": "p.10",
            },
        )
    )
    rows = orchestration._build_allocation_inputs(raw)
    assert len(rows) == 1
    assert rows[0].amount_value is not None
    assert rows[0].percent_value is None


def test_build_risk_inputs_ignore_unknown_risk_tokens() -> None:
    raw = _raw(
        (
            {
                "label": "risk:unknown:topic:policy_limit",
                "value": "$10 million",
                "evidence_ref": "p.20",
            },
        )
    )
    derivatives, securities = orchestration._build_risk_inputs(raw)
    assert derivatives == []
    assert securities == []
