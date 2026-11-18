"""Coverage gaps for issue #1044 (lane B autopilot chunk)."""

from __future__ import annotations

import json
from dataclasses import replace
from io import BytesIO
from pathlib import Path
from unittest.mock import patch

import pytest
from pypdf import PdfWriter
from pypdf.generic import DecodedStreamObject, DictionaryObject, NameObject

from pension_data.db.models.core_facts import (
    ActuarialFact,
    AllocationFact,
    BitemporalFactContext,
    DualReportedValue,
    FeeFact,
    FundedStatusFact,
    HoldingFact,
)
from pension_data.db.models.investment_positions import PlanManagerFundPosition
from pension_data.extract.actuarial import metrics as actuarial_metrics
from pension_data.extract.actuarial.metrics import (
    RawFundedActuarialInput,
    extract_funded_and_actuarial_metrics,
)
from pension_data.extract.governance.consultants import (
    ConsultantMention,
    RecommendationMention,
    extract_consultant_records,
    normalize_attribution_strength,
    normalize_board_decision_status,
)
from pension_data.extract.investment.risk_disclosures import (
    DerivativesDisclosureInput,
    SecuritiesLendingDisclosureInput,
    _slugify,
    _to_ratio,
    _to_usd,
    extract_risk_exposure_observations,
)
from pension_data.extract.persistence import (
    _collect_refs_from_row_dicts,
    _manager_name_for_relationship,
    build_extraction_persistence_artifacts,
    write_extraction_persistence_artifacts,
)
from pension_data.finite_guards import bounded_confidence_or_none
from pension_data.normalize import numeric_parsing
from pension_data.normalize.numeric_parsing import parse_numeric_token
from pension_data.parser.doc_lineage_backend import DocLineagePages
from pension_data.parser.hybrid_backend import HybridBackendConfig
from pension_data.parser.pdf_pipeline import (
    PDFParserInput,
    _decode_pdf_string_token,
    _extract_page_lines,
    _looks_like_noise_line,
    _metric_like_row_from_line,
    _split_pages,
    parse_pdf_to_funded_input,
)
from pension_data.provenance.metrics import _canonical_refs, build_core_metric_evidence_artifacts
from pension_data.quant.attribution_optimization import (
    OptimizationConstraints,
    QuantExperimentRecord,
    QuantExperimentRegistry,
    QuantExperimentRunner,
    compute_attribution,
    optimize_allocation,
    reconcile_attribution,
)
from pension_data.quant.metric_engine import (
    _as_refs,
    _to_float,
    aggregate_metric_series,
    compute_derived_metrics,
)
from pension_data.sources.edgar.client import (
    EdgarApiError,
    Filing13F,
    _recent_filings_block,
    parse_13f_hr_filings,
)


def _bitemporal() -> BitemporalFactContext:
    return BitemporalFactContext(
        plan_id="CA-PERS",
        plan_period="FY2025",
        effective_date="2025-06-30",
        ingestion_date="2026-01-15",
        benchmark_version="v1",
        source_document_id="doc:ca:2025",
    )


def _dual() -> DualReportedValue:
    return DualReportedValue(
        as_reported_value=1.0,
        normalized_value=1.0,
        as_reported_unit="ratio",
        normalized_unit="ratio",
    )


# --- finite_guards (target 1) -------------------------------------------------


def test_bounded_confidence_or_none_rejects_bool() -> None:
    assert bounded_confidence_or_none(True) is None
    assert bounded_confidence_or_none(False) is None


# --- metric_engine (target 2) -------------------------------------------------


def test_metric_engine_skips_invalid_core_rows_and_zero_aal_unfunded_ratio() -> None:
    observations = compute_derived_metrics(
        core_metric_rows=[
            {
                "metric_name": "",
                "normalized_value": 1.0,
                "plan_id": "P",
                "plan_period": "FY",
                "fact_id": "f1",
            },
            {
                "fact_id": "fact:aal",
                "plan_id": "CA-PERS",
                "plan_period": "FY2024",
                "metric_name": "aal_usd",
                "normalized_value": 0.0,
                "confidence": 0.9,
            },
            {
                "fact_id": "fact:ava",
                "plan_id": "CA-PERS",
                "plan_period": "FY2024",
                "metric_name": "ava_usd",
                "normalized_value": 0.0,
                "confidence": 0.9,
            },
            {
                "cash_flow_id": "flow:1",
                "plan_id": "CA-PERS",
                "plan_period": "FY2024",
                "employer_contributions_normalized": True,
            },
        ],
        cash_flow_rows=[
            {
                "cash_flow_id": "flow:1",
                "plan_id": "",
                "plan_period": "FY2024",
                "employer_contributions_normalized": 1.0,
                "employee_contributions_normalized": 1.0,
                "benefit_payments_normalized": -1.0,
                "refunds_normalized": 0.0,
            },
        ],
    )
    names = {row.metric_name for row in observations}
    assert "unfunded_ratio" not in names
    assert "funded_gap_usd" in names


def test_aggregate_metric_series_uses_unweighted_mean_when_weights_are_zero() -> None:
    observations = compute_derived_metrics(
        core_metric_rows=[
            {
                "fact_id": "fact:aal",
                "plan_id": "CA-PERS",
                "plan_period": "FY2024",
                "metric_name": "aal_usd",
                "normalized_value": 100.0,
                "confidence": 0.0,
            },
            {
                "fact_id": "fact:ava",
                "plan_id": "CA-PERS",
                "plan_period": "FY2024",
                "metric_name": "ava_usd",
                "normalized_value": 80.0,
                "confidence": 0.0,
            },
        ],
    )
    gap = next(row for row in observations if row.metric_name == "funded_gap_usd")
    observations = (gap, replace(gap, value=60.0, plan_period="FY2025"))
    agg = aggregate_metric_series(observations, metric_name="funded_gap_usd")
    assert agg.confidence_weight_sum == 0.0
    assert agg.sample_count == 2
    assert agg.weighted_mean == 40.0


@pytest.mark.parametrize("field", ["plan_id", "plan_period", "fact_id"])
@pytest.mark.parametrize("invalid", [None, 42, "", "   "])
def test_metric_engine_rejects_invalid_core_identity(field: str, invalid: object) -> None:
    aal = {
        "fact_id": "fact:aal",
        "plan_id": "P",
        "plan_period": "FY2024",
        "metric_name": "aal_usd",
        "normalized_value": 100.0,
    }
    ava = {**aal, "fact_id": "fact:ava", "metric_name": "ava_usd", "normalized_value": 80.0}
    # Corrupt both rows so an invalid group cannot simply lose its matching input.
    assert (
        compute_derived_metrics(core_metric_rows=[{**aal, field: invalid}, {**ava, field: invalid}])
        == ()
    )
    valid = compute_derived_metrics(core_metric_rows=[aal, ava])
    assert {row.metric_name: row.value for row in valid} == {
        "funded_gap_usd": 20.0,
        "unfunded_ratio": 0.2,
    }


@pytest.mark.parametrize(
    ("field", "invalid"),
    [
        ("metric_name", None),
        ("metric_name", "   "),
        ("normalized_value", None),
        ("normalized_value", []),
        ("normalized_value", "not disclosed"),
        ("normalized_value", "   "),
        ("normalized_value", True),
        ("normalized_value", float("nan")),
        ("normalized_value", "inf"),
    ],
)
def test_metric_engine_does_not_compute_from_unusable_values(field: str, invalid: object) -> None:
    aal = {
        "fact_id": "fact:aal",
        "plan_id": "P",
        "plan_period": "FY2024",
        "metric_name": "aal_usd",
        "normalized_value": 100.0,
    }
    ava = {**aal, "fact_id": "fact:ava", "metric_name": "ava_usd", "normalized_value": 80.0}
    assert compute_derived_metrics(core_metric_rows=[{**aal, field: invalid}, ava]) == ()


@pytest.mark.parametrize("field", ["plan_id", "plan_period", "cash_flow_id"])
@pytest.mark.parametrize("invalid", [None, 42, "", "   "])
def test_metric_engine_rejects_invalid_cash_flow_identity(field: str, invalid: object) -> None:
    row = {
        "cash_flow_id": "flow:1",
        "plan_id": "P",
        "plan_period": "FY2024",
        "employer_contributions_normalized": 18.0,
        "employee_contributions_normalized": 9.0,
        "benefit_payments_normalized": -5.0,
        "refunds_normalized": -1.0,
    }
    assert (
        compute_derived_metrics(core_metric_rows=[], cash_flow_rows=[{**row, field: invalid}]) == ()
    )
    valid = compute_derived_metrics(core_metric_rows=[], cash_flow_rows=[row])
    assert {item.metric_name: item.value for item in valid} == {
        "net_cash_flow_usd": 21.0,
        "contribution_to_benefit_ratio": 5.4,
    }


@pytest.mark.parametrize(
    ("field", "invalid", "expected"),
    [
        ("employer_contributions_normalized", None, {}),
        ("employee_contributions_normalized", None, {}),
        ("benefit_payments_normalized", None, {}),
        ("refunds_normalized", None, {"contribution_to_benefit_ratio": 5.4}),
        ("benefit_payments_normalized", 0.0, {"net_cash_flow_usd": 26.0}),
        ("refunds_normalized", False, {"contribution_to_benefit_ratio": 5.4}),
        ("refunds_normalized", "not disclosed", {"contribution_to_benefit_ratio": 5.4}),
    ],
)
def test_metric_engine_emits_only_metrics_with_usable_cash_inputs(
    field: str, invalid: object, expected: dict[str, float]
) -> None:
    row = {
        "cash_flow_id": "flow:1",
        "plan_id": "P",
        "plan_period": "FY2024",
        "employer_contributions_normalized": 18.0,
        "employee_contributions_normalized": 9.0,
        "benefit_payments_normalized": -5.0,
        "refunds_normalized": -1.0,
    }
    observations = compute_derived_metrics(
        core_metric_rows=[], cash_flow_rows=[{**row, field: invalid}]
    )
    assert {item.metric_name: item.value for item in observations} == expected
    assert all(item.source_fact_ids == ("flow:1",) for item in observations)


def test_metric_engine_unknown_confidence_uses_equal_weights() -> None:
    rows = [
        {
            "fact_id": f"fact:{metric}",
            "plan_id": "P",
            "plan_period": "FY2024",
            "metric_name": metric,
            "normalized_value": value,
        }
        for metric, value in (("aal_usd", 100.0), ("ava_usd", 80.0))
    ]
    observations = compute_derived_metrics(core_metric_rows=rows)
    assert all(item.confidence is None for item in observations)
    aggregate = aggregate_metric_series(observations, metric_name="funded_gap_usd")
    assert aggregate.weighted_mean == 20.0
    assert aggregate.confidence_weight_sum == 1.0
    assert aggregate.sample_count == 1


# --- risk_disclosures (target 3) ----------------------------------------------


def test_risk_disclosures_emit_policy_only_and_usd_conversions() -> None:
    observations, diagnostics = extract_risk_exposure_observations(
        plan_id="CA-PERS",
        plan_period="FY2024",
        derivatives_disclosures=[
            DerivativesDisclosureInput(
                usage_type="swaps",
                policy_limit_value=1.5,
                realized_exposure_value=None,
                value_unit="million_usd",
                as_reported_text="policy cap",
                source_kind="table",
                confidence=0.8,
                evidence_refs=("p1",),
                source_url="https://example.org",
            ),
        ],
        securities_lending_disclosures=[
            SecuritiesLendingDisclosureInput(
                program_name="   ",
                policy_limit_value=2.0,
                realized_exposure_value=1.0,
                collateral_value=None,
                value_unit="usd",
                as_reported_text="lending",
                source_kind="narrative",
                confidence=0.7,
                evidence_refs=("p2",),
                source_url="https://example.org",
            ),
        ],
    )
    policy = next(row for row in observations if row.observation_kind == "policy_limit")
    assert policy.value_usd == 1_500_000.0
    assert policy.value_ratio is None
    assert any(d.code == "policy_only" for d in diagnostics)


@pytest.mark.parametrize(("realized", "code"), [(0.0, "realized_only"), (None, "not_disclosed")])
def test_derivatives_missing_policy_diagnostic_preserves_exposure_and_evidence(
    realized: float | None, code: str
) -> None:
    observations, diagnostics = extract_risk_exposure_observations(
        plan_id="P",
        plan_period="FY2024",
        derivatives_disclosures=[
            DerivativesDisclosureInput(
                usage_type="Interest rate swaps",
                policy_limit_value=None,
                realized_exposure_value=realized,
                value_unit="usd",
                as_reported_text="No outstanding exposure; policy cap not disclosed",
                source_kind="narrative",
                confidence=0.8,
                evidence_refs=("p.10",),
                source_url="https://example.org/report.pdf",
            ),
        ],
        securities_lending_disclosures=[],
    )
    derivatives = [row for row in observations if row.disclosure_type == "derivatives"]
    if realized is None:
        assert derivatives == []
    else:
        assert len(derivatives) == 1
        assert derivatives[0].observation_kind == "realized_exposure"
        assert derivatives[0].value_usd == realized
    assert [(row.code, row.metric_name, row.evidence_refs) for row in diagnostics] == [
        (code, "derivatives:interest_rate_swaps", ("p.10",)),
        ("not_disclosed", "securities_lending:not_disclosed", ()),
    ]


# --- consultants (target 4) ---------------------------------------------------


def test_consultant_normalization_handles_none_and_missing_topic_warning() -> None:
    assert normalize_board_decision_status(None) == "not_disclosed"
    assert normalize_attribution_strength(None) == "speculative"

    extracted = extract_consultant_records(
        plan_id="CA-PERS",
        plan_period="FY2025",
        consultant_mentions=[
            ConsultantMention(
                consultant_name="   ",
                role_description="advisor",
                confidence=0.5,
                evidence_refs=("p1",),
                source_url="https://example.org",
            ),
        ],
        recommendation_mentions=[
            RecommendationMention(
                consultant_name="Mercer",
                topic=None,
                recommendation_text="review allocation",
                board_decision_status="adopted",
                confidence=0.8,
                evidence_refs=("p2",),
                source_url="https://example.org",
            ),
        ],
        attribution_mentions=[],
    )
    assert extracted["consultant_entities"] == []
    assert any(w.code == "missing_topic" for w in extracted["warnings"])


# --- pdf_pipeline (target 5) ---------------------------------------------------


@pytest.mark.parametrize(
    ("token", "expected"),
    [
        ("abc\\", "abc\\"),
        ("tab\\there", "tab\there"),
        ("paren\\(test\\)", "paren(test)"),
        ("oct\\101", "octA"),
        ("line\\\ncont", "linecont"),
        ("line\\\r\ncont", "linecont"),
    ],
)
def test_decode_pdf_string_token_escape_families(token: str, expected: str) -> None:
    assert _decode_pdf_string_token(token) == expected


def test_pdf_noise_filtering_and_metric_like_rows() -> None:
    assert _looks_like_noise_line("%PDF-1.4") is True
    assert _looks_like_noise_line("xref") is True
    assert _looks_like_noise_line("1 0 obj stream endstream") is True
    assert _looks_like_noise_line("!") is True
    assert _metric_like_row_from_line("Funded ratio was 81.2%") is None
    long_low_space = "A" * 200
    assert _looks_like_noise_line(long_low_space) is True

    pages = _split_pages("%PDF-1.4\fPage two content")
    assert len(pages) == 2

    parsed = parse_pdf_to_funded_input(
        PDFParserInput(
            source_document_id="doc:ocr-empty",
            source_url="https://example.org",
            effective_date="2024-06-30",
            ingestion_date="2026-01-01",
            default_money_unit_scale="million_usd",
            pdf_bytes=b"%PDF-1.4\n",
            parser_backend="legacy",
            ocr_extract=lambda _: (),
        )
    )
    assert parsed.escalation_required is True


def test_pdf_complex_table_year_columns_and_percent_suffix() -> None:
    multi_year = (
        b"%PDF-1.4\n%%Page: 1 1\n"
        b"Funded ratio | 2023 | 2024 | 78.0 | 81.2%\n"
        b"AAL | $410.0 million\n"
        b"AVA | $333.7 million\n"
        b"Discount rate | 6.75%\n"
        b"Employer contribution rate | 11.1%\n"
        b"Employee contribution rate | 7.1%\n"
        b"Participant count | 112000\n"
    )
    result = parse_pdf_to_funded_input(
        PDFParserInput(
            source_document_id="doc:multi",
            source_url="https://example.org",
            effective_date="2024-06-30",
            ingestion_date="2026-01-01",
            default_money_unit_scale="million_usd",
            pdf_bytes=multi_year,
            parser_backend="legacy",
        )
    )
    assert result.raw is not None
    assert any(row.get("complex_table") == "true" for row in result.raw.table_rows)


def test_pdf_empty_text_arrays_do_not_hide_metric_text() -> None:
    assert _extract_page_lines("[120 -3] TJ\n(AAL $410 million) Tj") == ["AAL $410 million"]
    assert _decode_pdf_string_token("\\1z") == "\x01z"
    assert _looks_like_noise_line("   ") is True
    assert _metric_like_row_from_line("Funded ratio not disclosed") is None
    assert _metric_like_row_from_line("Funded ratio 81.2 %") == ("Funded ratio", "81.2%")


def test_pdf_failed_builders_leave_no_partial_or_hybrid_result() -> None:
    with patch(
        "pension_data.parser.pdf_pipeline._build_text_stage",
        side_effect=ValueError("unreadable native PDF"),
    ):
        parsed = parse_pdf_to_funded_input(
            PDFParserInput(
                source_document_id="doc:unreadable",
                source_url="https://example.org/report.pdf",
                effective_date="2024-06-30",
                ingestion_date="2026-01-01",
                default_money_unit_scale="million_usd",
                pdf_bytes=b"%PDF-1.4\n",
                parser_backend="legacy",
                hybrid_config=HybridBackendConfig(enable_docling=True),
            )
        )
    assert parsed.raw is None
    assert parsed.stage_name is None
    assert parsed.stage_confidence == 0.0
    assert parsed.hybrid_result is None
    assert parsed.provenance_refs == ()
    assert parsed.escalation_required is True
    assert len(parsed.attempts) == 3
    assert "configure_ocr_fallback" in parsed.actionable_flags


def test_pdf_disabled_hybrid_keeps_native_extraction() -> None:
    with patch("pension_data.parser.pdf_pipeline.run_hybrid_table_extraction") as hybrid:
        parsed = parse_pdf_to_funded_input(
            PDFParserInput(
                source_document_id="doc:native",
                source_url="https://example.org/report.pdf",
                effective_date="2024-06-30",
                ingestion_date="2026-01-01",
                default_money_unit_scale="million_usd",
                pdf_bytes=b"%PDF-1.4\nAAL $410 million\n",
                parser_backend="legacy",
                hybrid_config=HybridBackendConfig(enable_docling=False),
            )
        )
    assert parsed.raw is not None
    assert parsed.raw.table_rows[0]["value"] == "$410 million"
    assert parsed.hybrid_result is None
    hybrid.assert_not_called()


def test_pdf_native_text_preserves_page_numbers_after_blank_page() -> None:
    writer = PdfWriter()
    writer.add_blank_page(width=612, height=792)
    page = writer.add_blank_page(width=612, height=792)
    page[NameObject("/Resources")] = DictionaryObject(
        {
            NameObject("/Font"): DictionaryObject(
                {
                    NameObject("/F1"): DictionaryObject(
                        {
                            NameObject("/Type"): NameObject("/Font"),
                            NameObject("/Subtype"): NameObject("/Type1"),
                            NameObject("/BaseFont"): NameObject("/Helvetica"),
                        }
                    )
                }
            )
        }
    )
    stream = DecodedStreamObject()
    stream.set_data(b"BT /F1 12 Tf 72 700 Td (AAL was $410 million) Tj ET")
    page[NameObject("/Contents")] = stream
    buffer = BytesIO()
    writer.write(buffer)

    result = parse_pdf_to_funded_input(
        PDFParserInput(
            source_document_id="doc:native-text",
            source_url="https://example.org/report.pdf",
            effective_date="2024-06-30",
            ingestion_date="2026-01-01",
            default_money_unit_scale="million_usd",
            pdf_bytes=buffer.getvalue(),
            parser_backend="legacy",
        )
    )

    assert result.stage_name == "text_fallback"
    assert result.raw is not None
    assert result.raw.text_blocks == ("AAL was $410 million",)
    assert result.raw.text_block_evidence_refs == ("p.2#text",)
    assert result.provenance_refs == ("p.2#text",)
    assert "aal_usd" not in result.missing_metrics


def test_pdf_complete_metrics_still_escalate_unreadable_doc_lineage_pages() -> None:
    document = DocLineagePages(
        page_lines=(
            (
                7,
                (
                    "Funded ratio | 81.2%",
                    "AAL | $410 million",
                    "AVA | $333.7 million",
                    "Discount rate | 6.75%",
                    "Employer contribution rate | 11.1%",
                    "Employee contribution rate | 7.1%",
                    "Participant count | 112000",
                ),
            ),
        ),
        pages_recognized=1,
        pages_unreadable=2,
    )
    with (
        patch("pension_data.parser.pdf_pipeline.find_spec", return_value=object()),
        patch("pension_data.parser.doc_lineage_backend.extract", return_value=document) as extract,
    ):
        result = parse_pdf_to_funded_input(
            PDFParserInput(
                source_document_id="doc:partly-unreadable",
                source_url="https://example.org/report.pdf",
                effective_date="2024-06-30",
                ingestion_date="2026-01-01",
                default_money_unit_scale="million_usd",
                pdf_bytes=b"source PDF",
                parser_backend="doc-lineage",
            )
        )

    extract.assert_called_once_with(b"source PDF", enable_ocr=True, ocr_backend=None)
    assert result.stage_name == "doc_lineage"
    assert result.raw is not None
    assert result.missing_metrics == ()
    assert result.escalation_required is True
    assert result.escalation is not None
    assert "doc_lineage_unreadable_pages:2" in result.actionable_flags
    assert result.provenance_refs == ("p.7#text", "p.7#table")
    assert len(result.attempts) == 1


# --- persistence (target 6) ----------------------------------------------------


def test_build_persistence_requires_manager_context_for_positions() -> None:
    with pytest.raises(ValueError, match="manager_position_context is required"):
        build_extraction_persistence_artifacts(
            funded_actuarial_rows=[],
            manager_position_rows=[
                PlanManagerFundPosition(
                    plan_id="P",
                    plan_period="FY2025",
                    manager_name="Alpha",
                    fund_name="Fund I",
                    commitment=None,
                    unfunded=None,
                    market_value=1.0,
                    completeness="complete",
                )
            ],
        )


def test_write_persistence_artifacts_validate_shape(tmp_path: Path) -> None:
    with pytest.raises(ValueError, match="persistence_contract"):
        write_extraction_persistence_artifacts(
            {"staging_core_metrics_rows": []}, output_root=tmp_path
        )


# --- edgar client (target 7) --------------------------------------------------


def test_edgar_submissions_json_validation_and_filing_properties() -> None:
    with pytest.raises(EdgarApiError, match="not valid JSON"):
        _recent_filings_block("not-json")

    with pytest.raises(EdgarApiError, match="not a JSON object"):
        _recent_filings_block("[]")

    with pytest.raises(EdgarApiError, match="no 'filings' object"):
        _recent_filings_block("{}")

    with pytest.raises(EdgarApiError, match="no 'filings.recent' object"):
        _recent_filings_block('{"filings": {}}')

    with pytest.raises(EdgarApiError, match="was not an array"):
        _recent_filings_block('{"filings": {"recent": {"form": "nope"}}}')

    with pytest.raises(EdgarApiError, match="missing its accession number"):
        parse_13f_hr_filings(
            '{"filings": {"recent": {"form": ["13F-HR"], "accessionNumber": [""]}}}'
        )

    filing = Filing13F(
        accession_number="0000919079-25-000042",
        form="13F-HR/A",
        filing_date="2025-01-01",
        report_date="2024-12-31",
        primary_document="form.xml",
    )
    assert filing.is_amendment is True
    assert filing.accession_no_dashes == "000091907925000042"

    only_original = parse_13f_hr_filings(
        (
            '{"filings": {"recent": {'
            '"form": ["13F-HR", "13F-HR/A"], '
            '"accessionNumber": ["0001", "0002"], '
            '"filingDate": ["2025-01-02", "2025-01-01"], '
            '"reportDate": ["2024-12-31", "2024-12-31"], '
            '"primaryDocument": ["a.xml", "b.xml"]'
            "}}}"
        ),
        include_amendments=False,
    )
    assert len(only_original) == 1
    assert only_original[0].form == "13F-HR"


# --- actuarial metrics (target 8) ---------------------------------------------


def test_actuarial_table_row_guards_skip_empty_or_unparseable_values() -> None:
    raw = RawFundedActuarialInput(
        source_document_id="doc:x",
        source_url="",
        effective_date="2024-06-30",
        ingestion_date="2025-01-05",
        default_money_unit_scale="million_usd",
        text_blocks=(),
        table_rows=(
            {"label": "", "value": "100", "evidence_ref": "p.1#table"},
            {"label": "AAL", "value": "", "evidence_ref": "p.1#table"},
            {"label": "AAL", "value": "n/a", "evidence_ref": "p.1#table"},
        ),
    )
    facts, _ = extract_funded_and_actuarial_metrics(plan_id="p", plan_period="FY2024", raw=raw)
    assert "aal_usd" not in {row.metric_name for row in facts}


def test_actuarial_normalize_metric_value_raises_when_money_normalization_fails() -> None:
    with (
        patch(
            "pension_data.extract.actuarial.metrics.normalize_money_to_usd",
            return_value=None,
        ),
        pytest.raises(ValueError, match="money normalization"),
    ):
        actuarial_metrics._normalize_metric_value(
            metric_kind="money",
            raw_text="$1 million",
            raw_value=1.0,
            fallback_money_scale="million_usd",
        )


# --- provenance metrics (target 9) --------------------------------------------


def test_provenance_artifacts_cover_all_high_impact_families() -> None:
    ctx = _bitemporal()
    artifacts = build_core_metric_evidence_artifacts(
        funded_facts=(
            FundedStatusFact(
                context=ctx,
                metric_name="funded_ratio",
                metric_value=_dual(),
                confidence=0.9,
                evidence_refs=("p.1",),
            ),
        ),
        actuarial_facts=(
            ActuarialFact(
                context=ctx,
                metric_name="normal_cost_rate",
                metric_value=_dual(),
                confidence=0.8,
                evidence_refs=("p.2",),
            ),
        ),
        allocation_facts=(
            AllocationFact(
                context=ctx,
                metric_name="public_equity_weight",
                metric_value=_dual(),
                confidence=0.7,
                evidence_refs=("p.3",),
            ),
        ),
        holding_facts=(
            HoldingFact(
                context=ctx,
                manager_name="Alpha",
                fund_name="Fund I",
                vehicle_name=None,
                metric_name="market_value",
                metric_value=_dual(),
                relationship_completeness="complete",
                confidence=0.6,
                evidence_refs=("p.4",),
            ),
        ),
        fee_facts=(
            FeeFact(
                context=ctx,
                fee_category="investment_management",
                manager_name="Alpha",
                metric_value=_dual(),
                confidence=0.5,
                evidence_refs=("p.5",),
            ),
        ),
    )
    links = artifacts["metric_evidence_links"]
    families = {link.metric_family for link in links}
    assert families == {"actuarial", "allocation", "fee", "funded", "holding"}


# --- numeric_parsing (target 10) ----------------------------------------------


def test_token_to_float_value_error_and_skip_unparseable_tokens() -> None:
    with patch("pension_data.normalize.numeric_parsing.float", side_effect=ValueError("bad")):
        assert numeric_parsing._token_to_float("12") is None
    assert parse_numeric_token("FY 2024 no-match-x then 12.5") == pytest.approx(12.5)


# --- attribution_optimization (target 11) ------------------------------------


def test_attribution_and_optimizer_error_paths() -> None:
    with pytest.raises(ValueError, match="missing realized return"):
        compute_attribution(weights={"equity": 0.5}, realized_returns={})

    with pytest.raises(ValueError, match="tolerance must be >= 0"):
        reconcile_attribution(computed_total=0.1, source_aggregate=0.1, tolerance=-1.0)

    with pytest.raises(ValueError, match="expected_returns is required"):
        optimize_allocation(
            expected_returns={},
            risk_penalties={},
            constraints=OptimizationConstraints(
                min_weight={},
                max_weight={},
                target_total_weight=1.0,
                precision_step=0.1,
            ),
            sensitivity_lambda=0.5,
        )

    with pytest.raises(ValueError, match="sensitivity_lambda"):
        optimize_allocation(
            expected_returns={"equity": 0.08},
            risk_penalties={"equity": 0.1},
            constraints=OptimizationConstraints(
                min_weight={"equity": 0.9},
                max_weight={"equity": 0.95},
                target_total_weight=1.0,
                precision_step=0.1,
            ),
            sensitivity_lambda=-0.1,
        )

    with pytest.raises(ValueError, match="no feasible allocation"):
        optimize_allocation(
            expected_returns={"equity": 0.08},
            risk_penalties={"equity": 0.1},
            constraints=OptimizationConstraints(
                min_weight={"equity": 0.6},
                max_weight={"equity": 0.6},
                target_total_weight=1.0,
                precision_step=0.5,
            ),
            sensitivity_lambda=0.5,
        )


def test_experiment_registry_compare_and_runner_attribution(tmp_path: Path) -> None:
    registry = QuantExperimentRegistry()
    with pytest.raises(ValueError, match="unknown experiment_id"):
        registry.compare("missing-a", "missing-b")

    runner = QuantExperimentRunner(registry=registry)
    rows, total, recon = runner.run_attribution_experiment(
        experiment_id="attr-1",
        weights={"equity": 1.0},
        realized_returns={"equity": 0.05},
        source_aggregate=0.05,
        artifact_links=("artifacts/attr-1.json",),
    )
    assert rows[0].bucket == "equity"
    assert total == pytest.approx(0.05)
    assert recon is not None and recon.within_tolerance is True

    export_path = registry.export_json_file(str(tmp_path / "registry.json"))
    payload = json.loads(Path(export_path).read_text(encoding="utf-8"))
    assert payload[0]["experiment_id"] == "attr-1"


def test_metric_engine_float_coercion_and_ref_dedup() -> None:
    assert _to_float(True) is None
    assert _to_float("   ") is None
    assert _to_float("not-a-number") is None
    assert _to_float(2.5) == 2.5
    assert _as_refs(b"bytes") == ()
    assert _as_refs(42) == ()
    assert _as_refs(["p.1", 2, "p.1", " p.2 "]) == ("p.1", "p.2")

    observations = compute_derived_metrics(
        core_metric_rows=[
            {
                "fact_id": "fact:ava",
                "plan_id": "CA-PERS",
                "plan_period": "FY2024",
                "metric_name": "ava_usd",
                "normalized_value": 10.0,
            },
        ],
    )
    assert observations == ()


def test_risk_helpers_and_lending_diagnostics() -> None:
    assert _slugify("   ") == "not_specified"
    assert _to_usd(None, unit="million_usd") is None
    assert _to_usd(1.0, unit="ratio") is None
    assert _to_ratio(None, unit="ratio") is None

    _, diagnostics = extract_risk_exposure_observations(
        plan_id="P",
        plan_period="FY",
        derivatives_disclosures=[],
        securities_lending_disclosures=[
            SecuritiesLendingDisclosureInput(
                program_name="program-a",
                policy_limit_value=1.0,
                realized_exposure_value=None,
                collateral_value=None,
                value_unit="ratio",
                as_reported_text="cap only",
                source_kind="table",
                confidence=0.5,
                evidence_refs=("p1",),
                source_url="https://example.org",
            ),
            SecuritiesLendingDisclosureInput(
                program_name="program-b",
                policy_limit_value=None,
                realized_exposure_value=0.2,
                collateral_value=None,
                value_unit="ratio",
                as_reported_text="realized only",
                source_kind="table",
                confidence=0.5,
                evidence_refs=("p2",),
                source_url="https://example.org",
            ),
            SecuritiesLendingDisclosureInput(
                program_name="program-c",
                policy_limit_value=None,
                realized_exposure_value=None,
                collateral_value=None,
                value_unit="ratio",
                as_reported_text="n/a",
                source_kind="narrative",
                confidence=0.5,
                evidence_refs=("p3",),
                source_url="https://example.org",
            ),
        ],
    )
    codes = {item.code for item in diagnostics}
    assert {"policy_only", "realized_only", "not_disclosed"}.issubset(codes)


def test_consultant_primary_source_url_falls_back_to_not_disclosed() -> None:
    extracted = extract_consultant_records(
        plan_id="CA-PERS",
        plan_period="FY2025",
        consultant_mentions=[
            ConsultantMention(
                consultant_name="Mercer",
                role_description="advisor",
                confidence=0.8,
                evidence_refs=("p1",),
                source_url="   ",
            ),
        ],
        recommendation_mentions=[],
        attribution_mentions=[],
    )
    assert extracted["consultant_entities"][0].source_metadata["source_url"] == "not_disclosed"


def test_pdf_decoding_truncated_octal_and_empty_tj_arrays() -> None:
    assert _decode_pdf_string_token("oct\\99") == "oct99"
    assert _decode_pdf_string_token("trail\\") == "trail\\"
    assert _decode_pdf_string_token("partial\\7") == "partial\x07"
    lines = _extract_page_lines("nonsense line without tokens")
    assert lines == ["nonsense line without tokens"]

    metric_row = _metric_like_row_from_line("AAL $410.0 million")
    assert metric_row is not None
    assert metric_row[0].upper() == "AAL"

    long_words = " ".join(f"word{i}" for i in range(20))
    assert _metric_like_row_from_line(f"Funded ratio {long_words} 81.2%") is None

    percent_row = _metric_like_row_from_line("Funded ratio 81.2% funded")
    assert percent_row is not None and percent_row[1] == "81.2%"


def test_persistence_manager_name_and_ref_collection(tmp_path: Path) -> None:
    unnamed = PlanManagerFundPosition(
        plan_id="P",
        plan_period="FY",
        manager_name=None,
        fund_name="F",
        commitment=None,
        unfunded=None,
        market_value=None,
        completeness="not_disclosed",
    )
    assert _manager_name_for_relationship(unnamed) != ""
    assert _collect_refs_from_row_dicts(
        [{"evidence_refs": "bad"}, {"evidence_refs": ["p.1", "p.1"]}]
    ) == ("p.1",)

    artifacts = {
        "persistence_contract": {},
        "staging_core_metrics_rows": [],
        "staging_manager_fund_vehicle_relationship_rows": [],
        "extraction_warning_rows": [],
        "schema_component_datasets": {123: []},
    }
    with pytest.raises(ValueError, match="component dataset keys must be strings"):
        write_extraction_persistence_artifacts(artifacts, output_root=tmp_path)


def test_edgar_filing_rows_tolerate_short_parallel_arrays() -> None:
    filings = parse_13f_hr_filings(
        '{"filings": {"recent": {"form": ["13F-HR"], "accessionNumber": ["0001"]}}}'
    )
    assert filings[0].filing_date == ""
    assert filings[0].report_date == ""


def test_actuarial_ratio_normalization_errors_raise() -> None:
    with patch("pension_data.extract.actuarial.metrics.to_ratio", return_value=None):
        with pytest.raises(ValueError, match="ratio normalization"):
            actuarial_metrics._normalize_metric_value(
                metric_kind="ratio",
                raw_text="81.2%",
                raw_value=81.2,
                fallback_money_scale="million_usd",
            )
        with pytest.raises(ValueError, match="ratio normalization"):
            actuarial_metrics._normalize_metric_value(
                metric_kind="ratio",
                raw_text="0.812",
                raw_value=0.812,
                fallback_money_scale="million_usd",
            )


def test_canonical_refs_skip_blank_and_duplicate_values() -> None:
    assert _canonical_refs(("p.1", " p.1 ", "", "p.2")) == ("p.1", "p.2")


def test_attribution_optimizer_grid_and_registry_branches() -> None:
    with pytest.raises(ValueError, match="missing constraints"):
        optimize_allocation(
            expected_returns={"equity": 0.08},
            risk_penalties={"equity": 0.1},
            constraints=OptimizationConstraints(
                min_weight={},
                max_weight={"equity": 1.0},
                target_total_weight=1.0,
                precision_step=0.1,
            ),
            sensitivity_lambda=0.5,
        )

    with pytest.raises(ValueError, match="precision_step must be > 0"):
        optimize_allocation(
            expected_returns={"equity": 0.08},
            risk_penalties={"equity": 0.1},
            constraints=OptimizationConstraints(
                min_weight={"equity": 0.0},
                max_weight={"equity": 1.0},
                target_total_weight=1.0,
                precision_step=0.0,
            ),
            sensitivity_lambda=0.5,
        )

    with pytest.raises(ValueError, match="optimization grid too large"):
        optimize_allocation(
            expected_returns={f"b{i}": 0.1 for i in range(1, 7)},
            risk_penalties={f"b{i}": 0.1 for i in range(1, 7)},
            constraints=OptimizationConstraints(
                min_weight={f"b{i}": 0.0 for i in range(1, 7)},
                max_weight={f"b{i}": 1.0 for i in range(1, 7)},
                target_total_weight=1.0,
                precision_step=0.1,
            ),
            sensitivity_lambda=0.5,
        )

    registry = QuantExperimentRegistry()
    registry.add_record(
        QuantExperimentRecord(
            experiment_id="left",
            module="attribution",
            seed=None,
            input_hash="a",
            output_hash="b",
            artifact_links=(),
            objective_value=1.0,
            total_return=None,
        )
    )
    registry.add_record(
        QuantExperimentRecord(
            experiment_id="right",
            module="attribution",
            seed=None,
            input_hash="c",
            output_hash="d",
            artifact_links=(),
            objective_value=None,
            total_return=0.05,
        )
    )
    comparison = registry.compare("left", "right")
    assert comparison.objective_delta is None
    assert comparison.total_return_delta is None

    runner = QuantExperimentRunner(registry=registry)
    _, _, reconciliation = runner.run_attribution_experiment(
        experiment_id="attr-no-recon",
        weights={"equity": 1.0},
        realized_returns={"equity": 0.05},
        source_aggregate=None,
    )
    assert reconciliation is None
