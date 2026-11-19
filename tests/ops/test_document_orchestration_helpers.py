"""Coverage for document-orchestration table-row parsing helpers."""

from __future__ import annotations

import pytest

from pension_data.db.models.artifacts import RawArtifactRecord
from pension_data.db.models.funded_actuarial import FundedActuarialStagingFact
from pension_data.extract.actuarial.metrics import RawFundedActuarialInput
from pension_data.ops import document_orchestration as orchestration
from pension_data.ops.document_orchestration import (
    DocumentOrchestrationState,
    SourceDocumentJobItem,
)


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


def _job_item(**overrides: object) -> SourceDocumentJobItem:
    base: dict[str, object] = {
        "plan_id": "CA-PERS",
        "plan_period": "FY2024",
        "source_url": "https://example.test/doc.pdf",
        "fetched_at": "2026-01-01T00:00:00Z",
        "mime_type": "application/pdf",
        "content_bytes": b"pdf",
        "source_document_id": "doc:test",
        "effective_date": "2024-06-30",
        "ingestion_date": "2026-01-01",
        "default_money_unit_scale": "million_usd",
    }
    base.update(overrides)
    return SourceDocumentJobItem(**base)  # type: ignore[arg-type]


def test_normalize_state_returns_default_when_none() -> None:
    state = orchestration._normalize_state(None)
    assert state.artifact_records == ()


def test_sorted_unique_drops_falsy_values() -> None:
    assert orchestration._sorted_unique(["b", "", "a", "b"]) == ("a", "b")


def test_document_key_includes_plan_and_source() -> None:
    item = _job_item()
    assert orchestration._document_key(item) == "CA-PERS|FY2024|https://example.test/doc.pdf"


def test_retry_returns_attempt_count_and_reraises_after_limit() -> None:
    attempts = {"count": 0}

    def flaky() -> str:
        attempts["count"] += 1
        if attempts["count"] < 3:
            raise RuntimeError("transient")
        return "ok"

    value, count = orchestration._retry(flaky, max_retries=3)
    assert value == "ok"
    assert count == 3

    def always_fail() -> None:
        raise ValueError("nope")

    with pytest.raises(ValueError, match="nope"):
        orchestration._retry(always_fail, max_retries=1)


@pytest.mark.parametrize(
    ("field", "message"),
    [
        ("plan_id", "plan_id is required"),
        ("plan_period", "plan_period is required"),
        ("source_url", "source_url is required"),
        ("source_document_id", "source_document_id is required"),
        ("effective_date", "effective_date is required"),
        ("ingestion_date", "ingestion_date is required"),
        ("mime_type", "mime_type is required"),
        ("fetched_at", "fetched_at is required"),
    ],
)
def test_validate_job_items_rejects_blank_required_fields(field: str, message: str) -> None:
    with pytest.raises(ValueError, match=message):
        orchestration._validate_job_items([_job_item(**{field: "   "})])


def test_validate_job_items_sorts_rows() -> None:
    later = _job_item(
        plan_id="B-PLAN",
        source_url="https://example.test/second.pdf",
        fetched_at="2026-02-01T00:00:00Z",
    )
    earlier = _job_item(
        plan_id="A-PLAN",
        source_url="https://example.test/first.pdf",
        fetched_at="2026-01-01T00:00:00Z",
    )
    validated = orchestration._validate_job_items([later, earlier])
    assert [row.plan_id for row in validated] == ["A-PLAN", "B-PLAN"]


def test_active_artifact_by_key_keeps_only_active_rows() -> None:
    record = RawArtifactRecord(
        artifact_id="art:1",
        plan_id="CA-PERS",
        plan_period="FY2024",
        source_url="https://example.test/a.pdf",
        fetched_at="2026-01-01T00:00:00Z",
        mime_type="application/pdf",
        byte_size=10,
        checksum_sha256="abc",
        is_active=True,
        supersedes_artifact_id=None,
        superseded_by_artifact_id=None,
        first_seen_at="2026-01-01T00:00:00Z",
        last_seen_at="2026-01-01T00:00:00Z",
    )
    inactive = RawArtifactRecord(
        artifact_id="art:0",
        plan_id=record.plan_id,
        plan_period=record.plan_period,
        source_url=record.source_url,
        fetched_at=record.fetched_at,
        mime_type=record.mime_type,
        byte_size=record.byte_size,
        checksum_sha256=record.checksum_sha256,
        is_active=False,
        supersedes_artifact_id=None,
        superseded_by_artifact_id=None,
        first_seen_at=record.first_seen_at,
        last_seen_at=record.last_seen_at,
    )
    active = orchestration._active_artifact_by_key((record, inactive))
    assert list(active.values()) == [record]


def test_confidence_inputs_maps_staging_facts() -> None:
    fact = FundedActuarialStagingFact(
        plan_id="CA-PERS",
        plan_period="FY2024",
        metric_name="funded_ratio",
        as_reported_value=0.78,
        normalized_value=0.78,
        as_reported_unit="ratio",
        normalized_unit="ratio",
        effective_date="2024-06-30",
        ingestion_date="2026-01-01",
        source_document_id="doc:test",
        source_url="https://example.test/report.pdf",
        extraction_method="table",
        confidence=0.9,
        parser_version="test",
        evidence_refs=("p.1",),
    )
    inputs = orchestration._confidence_inputs([fact])
    assert len(inputs) == 1
    assert inputs[0].row_id == "doc:test:funded_ratio"


def test_money_entry_value_skips_rate_disclosures() -> None:
    assert orchestration._money_entry_value("Employer rate", "4.5%") is None
    assert orchestration._money_entry_value("Contributions", "$12 million") == 12.0


def test_first_matching_entry_skips_rate_only_rows() -> None:
    entries = (
        ("Employer Contributions", "12.4%", "p.rate"),
        ("Employer Contributions", "$18 million", "p.money"),
    )
    match = orchestration._first_matching_entry(entries, ("employer contribution",))
    assert match == ("Employer Contributions", "$18 million", "p.money")


def test_build_financial_flow_input_detects_negative_outflows() -> None:
    raw = _raw(
        (
            {"label": "Benefit Payments", "value": "-40", "evidence_ref": "p.3"},
            {"label": "Beginning AUM", "value": "$100 million", "evidence_ref": "p.1"},
        )
    )
    flow = orchestration._build_financial_flow_input(raw=raw)
    assert flow.outflows_reported_as_negative is True
    assert flow.benefit_payments == -40.0


def test_build_allocation_inputs_parses_percent_rows() -> None:
    raw = _raw(({"label": "allocation:Equities", "value": "45%", "evidence_ref": "p.2"},))
    rows = orchestration._build_allocation_inputs(raw)
    assert rows[0].percent_value == 45.0
    assert rows[0].amount_value is None


def test_build_fee_inputs_parses_rate_and_amount_fields() -> None:
    raw = _raw(
        (
            {
                "label": "fee:Alpha Capital:management_fee:rate",
                "value": "0.55%",
                "evidence_ref": "p.4",
            },
            {
                "label": "fee:Beta Partners:performance_fee:amount",
                "value": "$2 million",
                "evidence_ref": "p.5",
            },
        )
    )
    fees = orchestration._build_fee_inputs(raw)
    assert fees[0].rate_value == 0.55
    assert fees[1].amount_value == 2.0


def test_build_manager_disclosure_inputs_groups_position_rows() -> None:
    raw = _raw(
        (
            {
                "label": "position:Alpha Capital:Global Equity:commitment",
                "value": "$50 million",
                "evidence_ref": "p.6",
            },
            {
                "label": "position:Alpha Capital:Global Equity:market_value",
                "value": "$48 million",
                "evidence_ref": "p.7",
            },
        )
    )
    rows = orchestration._build_manager_disclosure_inputs(
        raw=raw,
        plan_id="CA-PERS",
        plan_period="FY2024",
    )
    assert len(rows) == 1
    assert rows[0].commitment == 50.0
    assert rows[0].market_value == 48.0
    assert rows[0].evidence_refs == ("p.6", "p.7")


def test_build_risk_inputs_populates_derivatives_and_securities() -> None:
    raw = _raw(
        (
            {
                "label": "risk:derivatives:interest_rate_swaps:policy_limit",
                "value": "$25 million",
                "evidence_ref": "p.8",
            },
            {
                "label": "risk:securities_lending:repo_program:collateral",
                "value": "$10 million",
                "evidence_ref": "p.9",
            },
        )
    )
    derivatives, securities = orchestration._build_risk_inputs(raw)
    assert derivatives[0].policy_limit_value == 25.0
    assert securities[0].collateral_value == 10.0


def test_build_consultant_mentions_parses_text_blocks() -> None:
    raw = RawFundedActuarialInput(
        source_document_id="doc:test",
        source_url="https://example.test/report.pdf",
        effective_date="2024-06-30",
        ingestion_date="2026-01-01",
        default_money_unit_scale="million_usd",
        text_blocks=(
            "consultant:Jane Actuary|Chief Actuary",
            "recommendation:Jane Actuary|discount_rate|lower to 6.9%|adopted",
            "attribution:Jane Actuary|return_assumption|observed beat|strong",
        ),
        table_rows=(),
    )
    consultants, recommendations, attributions = orchestration._build_consultant_mentions(raw)
    assert consultants[0].consultant_name == "Jane Actuary"
    assert recommendations[0].recommendation_text == "lower to 6.9%"
    assert attributions[0].strength == "strong"


def test_jsonable_serializes_nested_dataclasses() -> None:
    state = DocumentOrchestrationState(published_fact_ids=("fact:1",))
    payload = orchestration._jsonable(state)
    assert payload["published_fact_ids"] == ["fact:1"]


def test_jsonable_serializes_builtin_dict_and_list() -> None:
    assert orchestration._jsonable({"alpha": 1, "nested": {"beta": 2}}) == {
        "alpha": 1,
        "nested": {"beta": 2},
    }
    assert orchestration._jsonable([1, ("pair",), {"key": "value"}]) == [
        1,
        ["pair"],
        {"key": "value"},
    ]


def test_build_financial_flow_input_collects_evidence_from_matched_rows() -> None:
    raw = _raw(
        (
            {"label": "Beginning AUM", "value": "$100 million", "evidence_ref": "p.1"},
            {"label": "Ending AUM", "value": "$110 million", "evidence_ref": "p.2"},
        )
    )
    flow = orchestration._build_financial_flow_input(raw=raw)
    assert flow.evidence_refs == ("p.1", "p.2")


def test_build_financial_flow_input_omits_empty_evidence_refs() -> None:
    raw = _raw(
        (
            {"label": "Beginning AUM", "value": "$100 million", "evidence_ref": ""},
            {"label": "Ending AUM", "value": "$110 million", "evidence_ref": "p.2"},
        )
    )
    flow = orchestration._build_financial_flow_input(raw=raw)
    assert flow.evidence_refs == ("p.2",)


def test_build_allocation_inputs_skip_unparseable_category_values() -> None:
    raw = _raw(
        (
            {
                "label": "allocation:Private Markets",
                "value": "not disclosed",
                "evidence_ref": "p.skip",
            },
        )
    )
    assert orchestration._build_allocation_inputs(raw) == []


def test_build_manager_disclosure_inputs_skip_empty_evidence_refs() -> None:
    raw = _raw(
        (
            {
                "label": "position:Alpha:Fund I:commitment",
                "value": "$10 million",
                "evidence_ref": "",
            },
        )
    )
    rows = orchestration._build_manager_disclosure_inputs(
        raw=raw,
        plan_id="CA-PERS",
        plan_period="FY2024",
    )
    assert rows[0].commitment == 10.0
    assert rows[0].evidence_refs == ()


def test_build_manager_disclosure_inputs_sets_known_not_invested_only() -> None:
    raw = _raw(
        (
            {
                "label": "position:Gamma Partners:Fund III:known_not_invested",
                "value": "true",
                "evidence_ref": "p.kni-only",
            },
            {
                "label": "position:Gamma Partners:Fund III:footnotes",
                "value": "see note 3",
                "evidence_ref": "",
            },
        )
    )
    rows = orchestration._build_manager_disclosure_inputs(
        raw=raw,
        plan_id="CA-PERS",
        plan_period="FY2024",
    )
    assert len(rows) == 1
    assert rows[0].known_not_invested is True
    assert rows[0].explicit_not_disclosed is False


def test_build_manager_disclosure_inputs_set_disclosure_flag_fields() -> None:
    raw = _raw(
        (
            {
                "label": "position:Beta Partners:Fund II:not_disclosed",
                "value": "true",
                "evidence_ref": "p.nd",
            },
            {
                "label": "position:Beta Partners:Fund II:known_not_invested",
                "value": "true",
                "evidence_ref": "p.kni",
            },
        )
    )
    rows = orchestration._build_manager_disclosure_inputs(
        raw=raw,
        plan_id="CA-PERS",
        plan_period="FY2024",
    )
    assert len(rows) == 1
    assert rows[0].explicit_not_disclosed is True
    assert rows[0].known_not_invested is True
    assert rows[0].evidence_refs == ("p.nd", "p.kni")


def test_build_risk_inputs_skips_empty_evidence_refs() -> None:
    raw = _raw(
        (
            {
                "label": "risk:derivatives:swaps:policy_limit",
                "value": "$5 million",
                "evidence_ref": "",
            },
        )
    )
    derivatives, securities = orchestration._build_risk_inputs(raw)
    assert len(derivatives) == 1
    assert derivatives[0].evidence_refs == ()
    assert securities == []


def test_build_risk_inputs_appends_evidence_without_numeric_field_match() -> None:
    raw = _raw(
        (
            {
                "label": "risk:derivatives:swaps:footnotes",
                "value": "See footnote 14",
                "evidence_ref": "p.70",
            },
        )
    )
    derivatives, securities = orchestration._build_risk_inputs(raw)
    assert len(derivatives) == 1
    assert derivatives[0].evidence_refs == ("p.70",)
    assert securities == []


def test_build_consultant_mentions_parses_attribution_and_skips_unknown_blocks() -> None:
    raw = RawFundedActuarialInput(
        source_document_id="doc:test",
        source_url="https://example.test/report.pdf",
        effective_date="2024-06-30",
        ingestion_date="2026-01-01",
        default_money_unit_scale="million_usd",
        text_blocks=(
            "footnote:ignored line",
            "attribution:Jane Actuary|discount_rate|observed beat|strong",
        ),
        table_rows=(),
    )
    consultants, recommendations, attributions = orchestration._build_consultant_mentions(raw)
    assert consultants == []
    assert recommendations == []
    assert len(attributions) == 1
    assert attributions[0].consultant_name == "Jane Actuary"
    assert attributions[0].strength == "strong"
