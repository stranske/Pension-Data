"""Tests for pilot-artifact-backed API query snapshots."""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from pension_data.api.artifact_data import QueryArtifactError, load_query_data_snapshot


def _metric_row(
    *,
    fact_id: str = "fact:funded",
    plan_id: str = "SYN-PLAN",
    normalized_value: float = 0.784,
    confidence: float = 0.9,
    source_document_id: str = "doc:syn-plan",
    ingestion_date: str = "2026-01-01",
) -> dict[str, object]:
    return {
        "fact_id": fact_id,
        "plan_id": plan_id,
        "plan_period": "FY2024",
        "metric_family": "funded",
        "metric_name": "funded_ratio",
        "as_reported_value": normalized_value * 100,
        "normalized_value": normalized_value,
        "as_reported_unit": "percent",
        "normalized_unit": "ratio",
        "confidence": confidence,
        "evidence_refs": ["p.4#table"],
        "effective_date": "2024-06-30",
        "ingestion_date": ingestion_date,
        "benchmark_version": "parser-v1",
        "source_document_id": source_document_id,
    }


def _write_run(
    root: Path,
    run_id: str,
    rows: list[dict[str, object]],
    *,
    plan_id: str = "SYN-PLAN",
    plan_period: str = "FY2024",
    source_document_id: str = "doc:syn-plan",
) -> Path:
    run_root = root / "one_pdf_pilot" / run_id
    persistence_root = run_root / "extraction_persistence"
    persistence_root.mkdir(parents=True)
    manifest = {
        "run_id": run_id,
        "ledger_status": "success",
        "input": {
            "plan_id": plan_id,
            "plan_period": plan_period,
            "source_document_id": source_document_id,
        },
    }
    (run_root / "run_manifest.json").write_text(json.dumps(manifest), encoding="utf-8")
    (persistence_root / "staging_core_metrics.json").write_text(json.dumps(rows), encoding="utf-8")
    return run_root


def test_load_query_data_snapshot_maps_metric_and_funding_rows(tmp_path: Path) -> None:
    (tmp_path / "one_pdf_pilot" / "in-progress").mkdir(parents=True)
    _write_run(tmp_path, "run-a", [_metric_row()])

    snapshot = load_query_data_snapshot(tmp_path)

    assert len(snapshot.metric_history_rows) == 1
    metric = snapshot.metric_history_rows[0]
    assert metric.entity_id == "SYN-PLAN"
    assert metric.normalized_value == pytest.approx(0.784)
    assert metric.report_id == "doc:syn-plan"
    assert metric.valid_from == "2024-06-30"
    assert metric.asserted_at == "2026-01-01"
    assert metric.provenance_refs[0].page_number == 4
    assert snapshot.funding_trend_inputs[0].funded_ratio == pytest.approx(0.784)
    assert snapshot.funding_trend_inputs[0].employer_contributions_usd is None


def _write_flow_artifact(run_root: Path, **overrides: object) -> Path:
    row = {
        "plan_id": "SYN-PLAN",
        "plan_period": "FY2024",
        "source_document_id": "doc:syn-plan",
        "effective_period": "2024-06-30",
        "reported_at": "2026-01-01",
        "employer_contributions_usd": 100.0,
        "employee_contributions_usd": 20.0,
        "benefit_payments_usd": -30.0,
        "refunds_usd": -5.0,
        **overrides,
    }
    path = run_root / "flows.json"
    path.write_text(json.dumps([row]), encoding="utf-8")
    manifest_path = run_root / "run_manifest.json"
    manifest = json.loads(manifest_path.read_text())
    manifest["artifact_files"] = {"financial_flow_rows_json": "flows.json"}
    manifest_path.write_text(json.dumps(manifest), encoding="utf-8")
    return path


def _run_flow_pilot(tmp_path: Path) -> tuple[Path, dict[str, str]]:
    """Exercise real orchestration from disclosed parser rows, then its saved artifacts.

    The default PDF parser only recognizes funded metrics; monetary parser coverage
    is outside issue 959. Supply its public parser boundary explicitly here.
    """
    from pension_data.extract.actuarial.metrics import RawFundedActuarialInput
    from pension_data.ops.document_orchestration import (
        DocumentOrchestrationState,
        SourceDocumentJobItem,
        run_document_orchestration,
    )

    root = tmp_path / "artifacts"
    run = root / "one_pdf_pilot" / "flow-pilot"
    raw = RawFundedActuarialInput(
        source_document_id="doc:syn-plan",
        source_url="https://example.test/flow.pdf",
        effective_date="2024-06-30",
        ingestion_date="2026-01-01",
        default_money_unit_scale="million_usd",
        text_blocks=(),
        table_rows=tuple(
            {"label": label, "value": value, "evidence_ref": "p.1#table"}
            for label, value in [
                ("Funded Ratio", "78.4%"),
                ("AAL", "$640 million"),
                ("AVA", "$501.8 million"),
                ("Discount Rate", "6.8%"),
                ("Employer Contribution Rate", "12.4%"),
                ("Employee Contribution Rate", "7.5%"),
                ("Participant Count", "325000"),
                ("Employer Contributions", "$100 million"),
                ("Employee Contributions", "$20 million"),
                ("Benefit Payments", "$-30 million"),
                ("Refunds", "$-5 million"),
            ]
        ),
    )
    job = SourceDocumentJobItem(
        plan_id="SYN-PLAN",
        plan_period="FY2024",
        source_url=raw.source_url,
        fetched_at="2026-01-01T00:00:00Z",
        mime_type="application/pdf",
        content_bytes=b"synthetic disclosed flow report",
        source_document_id=raw.source_document_id,
        effective_date=raw.effective_date,
        ingestion_date=raw.ingestion_date,
        default_money_unit_scale="million_usd",
    )
    ledger, _, artifacts = run_document_orchestration(
        documents=[job],
        parser=lambda _item, _artifact: raw,
        state=DocumentOrchestrationState(),
        run_id="flow-pilot",
        output_root=run,
    )
    assert ledger.status == "success"
    _write_run(root, "flow-pilot", artifacts["published_rows"])
    manifest_path = run / "run_manifest.json"
    manifest = json.loads(manifest_path.read_text())
    # Relative to the run directory so relocation still resolves via containment.
    manifest["artifact_files"] = {
        "financial_flow_rows_json": "document_orchestration/flow-pilot/financial_flow_rows.json"
    }
    manifest_path.write_text(json.dumps(manifest))
    return root, {"run_manifest_json": str(manifest_path)}


def _flow_artifact_path(run_manifest_json: str, declared: str) -> Path:
    path = Path(declared)
    if path.is_absolute():
        return path
    return Path(run_manifest_json).parent / path


def test_funding_trend_joins_actual_pilot_financial_flow_output(tmp_path: Path) -> None:
    from pension_data.query.saved_views.service import execute_funding_trend_view

    root, result = _run_flow_pilot(tmp_path)
    manifest = json.loads(Path(result["run_manifest_json"]).read_text())
    declared = manifest["artifact_files"]["financial_flow_rows_json"]
    assert not Path(declared).is_absolute()
    flow = json.loads(_flow_artifact_path(result["run_manifest_json"], declared).read_text())[0]
    assert flow["benefit_payments_usd"] == -30_000_000
    assert flow["refunds_usd"] == -5_000_000
    assert flow["net_external_cash_flow_usd"] == 85_000_000

    snapshot = load_query_data_snapshot(root)
    assert len(snapshot.funding_trend_inputs) == 1
    rows = execute_funding_trend_view(list(snapshot.funding_trend_inputs))
    assert len(rows) == 1
    assert rows[0].net_external_cash_flow_usd == 85_000_000


def test_funding_trend_missing_flow_artifact_yields_null_net(tmp_path: Path) -> None:
    from pension_data.query.saved_views.service import execute_funding_trend_view

    _write_run(tmp_path, "run-a", [_metric_row()])
    snapshot = load_query_data_snapshot(tmp_path)
    assert snapshot.funding_trend_inputs[0].employer_contributions_usd is None
    rows = execute_funding_trend_view(list(snapshot.funding_trend_inputs))
    assert rows[0].net_external_cash_flow_usd is None


@pytest.mark.parametrize("disclosure", ["empty", "omitted", "null"])
def test_funding_trend_undisclosed_flow_components_remain_null(
    tmp_path: Path, disclosure: str
) -> None:
    from pension_data.query.saved_views.service import execute_funding_trend_view

    run = _write_run(tmp_path, "run-a", [_metric_row()])
    path = _write_flow_artifact(run)
    payload = json.loads(path.read_text())
    for field in (
        "employer_contributions_usd",
        "employee_contributions_usd",
        "benefit_payments_usd",
        "refunds_usd",
    ):
        if disclosure == "omitted":
            del payload[0][field]
        else:
            payload[0][field] = None
    path.write_text(json.dumps([] if disclosure == "empty" else payload), encoding="utf-8")

    snapshot = load_query_data_snapshot(tmp_path)

    assert len(snapshot.funding_trend_inputs) == 1
    trend = snapshot.funding_trend_inputs[0]
    assert trend.funded_ratio == pytest.approx(0.784)
    assert trend.employer_contributions_usd is None
    assert trend.employee_contributions_usd is None
    assert trend.benefit_payments_usd is None
    assert trend.refunds_usd is None
    rows = execute_funding_trend_view(list(snapshot.funding_trend_inputs))
    assert rows[0].net_external_cash_flow_usd is None


@pytest.mark.parametrize(
    "field, direction",
    [
        ("employer_contributions_usd", 1),
        ("employee_contributions_usd", 1),
        ("benefit_payments_usd", -1),
        ("refunds_usd", -1),
    ],
)
@pytest.mark.parametrize("amount", [0.0, 25.0])
def test_funding_trend_partial_disclosure_preserves_zero_and_unknown_components(
    tmp_path: Path, field: str, direction: int, amount: float
) -> None:
    from pension_data.query.saved_views.service import execute_funding_trend_view

    components: dict[str, float | None] = {
        "employer_contributions_usd": None,
        "employee_contributions_usd": None,
        "benefit_payments_usd": None,
        "refunds_usd": None,
    }
    components[field] = direction * amount
    run = _write_run(tmp_path, "run-a", [_metric_row()])
    _write_flow_artifact(run, **components)

    snapshot = load_query_data_snapshot(tmp_path)

    assert len(snapshot.funding_trend_inputs) == 1
    trend = snapshot.funding_trend_inputs[0]
    for component in components:
        if component == field:
            assert getattr(trend, component) == amount
        else:
            assert getattr(trend, component) is None
    rows = execute_funding_trend_view(list(snapshot.funding_trend_inputs))
    assert rows[0].net_external_cash_flow_usd == direction * amount


def test_funding_trend_resolves_absolute_flow_path_under_run(tmp_path: Path) -> None:
    """Absolute manifest paths still resolve when they stay inside the run tree."""
    from pension_data.query.saved_views.service import execute_funding_trend_view

    run = _write_run(tmp_path, "run-a", [_metric_row()])
    _write_flow_artifact(run)
    manifest_path = run / "run_manifest.json"
    manifest = json.loads(manifest_path.read_text())
    absolute = (run / "flows.json").resolve()
    manifest["artifact_files"] = {"financial_flow_rows_json": str(absolute)}
    manifest_path.write_text(json.dumps(manifest), encoding="utf-8")

    snapshot = load_query_data_snapshot(tmp_path)
    rows = execute_funding_trend_view(list(snapshot.funding_trend_inputs))
    assert rows[0].net_external_cash_flow_usd == 85


@pytest.mark.parametrize("field", ["plan_id", "plan_period", "source_document_id"])
def test_funding_trend_rejects_mismatched_flow_identity(tmp_path: Path, field: str) -> None:
    run = _write_run(tmp_path, "run-a", [_metric_row()])
    _write_flow_artifact(run, **{field: "OTHER"})
    with pytest.raises(QueryArtifactError, match=f"{field} conflicts with run manifest"):
        load_query_data_snapshot(tmp_path)


@pytest.mark.parametrize("value", [True, "100", float("inf"), float("nan"), 10**400, -(10**400)])
def test_funding_trend_rejects_invalid_flow_amount(tmp_path: Path, value: object) -> None:
    run = _write_run(tmp_path, "run-a", [_metric_row()])
    _write_flow_artifact(run, employer_contributions_usd=value)
    with pytest.raises(QueryArtifactError, match="employer_contributions_usd must be"):
        load_query_data_snapshot(tmp_path)


def test_funding_trend_flow_path_cannot_escape_run(tmp_path: Path) -> None:
    run = _write_run(tmp_path / "root", "run-a", [_metric_row()])
    flow = _write_flow_artifact(run)
    outside = tmp_path / "outside.json"
    flow.rename(outside)
    flow.symlink_to(outside)
    with pytest.raises(QueryArtifactError, match="escapes configured root"):
        load_query_data_snapshot(tmp_path / "root")


@pytest.mark.parametrize("path_kind", ["relative", "absolute", "symlink"])
def test_funding_trend_flow_path_cannot_reference_another_run(
    tmp_path: Path, path_kind: str
) -> None:
    """Root containment alone must not allow one run to borrow another's flows."""
    run = _write_run(tmp_path, "run-a", [_metric_row()])
    _write_flow_artifact(run)
    other_run = _write_run(tmp_path, "run-b", [_metric_row()])
    other_flow = _write_flow_artifact(other_run)
    # Both runs intentionally have matching document identities so identity
    # validation cannot mask a missing run-directory containment check.
    manifest_path = run / "run_manifest.json"
    manifest = json.loads(manifest_path.read_text())
    if path_kind == "relative":
        declared = "../run-b/flows.json"
    elif path_kind == "absolute":
        declared = str(other_flow.resolve())
    else:
        link = run / "borrowed-flows.json"
        link.symlink_to(other_flow)
        declared = link.name
    manifest["artifact_files"]["financial_flow_rows_json"] = declared
    manifest_path.write_text(json.dumps(manifest), encoding="utf-8")

    with pytest.raises(QueryArtifactError, match="financial_flow_rows.json escapes its run"):
        load_query_data_snapshot(tmp_path)


def test_funding_trend_declared_missing_flow_artifact_is_rejected(tmp_path: Path) -> None:
    """A broken declaration must not silently use another available flow file."""
    run = _write_run(tmp_path, "run-a", [_metric_row()])
    _write_flow_artifact(run)
    manifest_path = run / "run_manifest.json"
    manifest = json.loads(manifest_path.read_text())
    manifest["artifact_files"]["financial_flow_rows_json"] = "missing-flows.json"
    manifest_path.write_text(json.dumps(manifest), encoding="utf-8")

    with pytest.raises(QueryArtifactError, match="missing query artifact financial_flow_rows.json"):
        load_query_data_snapshot(tmp_path)


def test_funding_trend_rejects_invalid_manifest_flow_path(tmp_path: Path) -> None:
    run = _write_run(tmp_path, "run-a", [_metric_row()])
    _write_flow_artifact(run)
    manifest_path = run / "run_manifest.json"
    manifest = json.loads(manifest_path.read_text())
    manifest["artifact_files"]["financial_flow_rows_json"] = "invalid\x00flows.json"
    manifest_path.write_text(json.dumps(manifest), encoding="utf-8")

    with pytest.raises(
        QueryArtifactError, match="invalid query artifact financial_flow_rows.json path"
    ):
        load_query_data_snapshot(tmp_path)


def test_funding_trend_repeated_flows_are_not_summed(tmp_path: Path) -> None:
    from pension_data.query.saved_views.service import execute_funding_trend_view

    for name in ("a", "b"):
        _write_flow_artifact(_write_run(tmp_path, name, [_metric_row()]))
    snapshot = load_query_data_snapshot(tmp_path)
    assert len(snapshot.metric_history_rows) == 1
    assert (
        execute_funding_trend_view(list(snapshot.funding_trend_inputs))[
            0
        ].net_external_cash_flow_usd
        == 85
    )


def test_funding_trend_conflicting_flows_remain_unresolved(tmp_path: Path) -> None:
    _write_flow_artifact(_write_run(tmp_path, "a", [_metric_row()]))
    _write_flow_artifact(_write_run(tmp_path, "b", [_metric_row()]), employer_contributions_usd=200)
    snapshot = load_query_data_snapshot(tmp_path)
    assert snapshot.funding_trend_inputs == ()
    assert snapshot.funding_trend_conflicts[0].metric_target == "employer_contributions_usd"


@pytest.mark.parametrize("metric_name", ["benefit_payments", "benefit_payments_usd"])
@pytest.mark.parametrize("staged_benefits", [-30.0, 30.0])
@pytest.mark.parametrize("disclosed_benefits", [-30.0, 30.0])
def test_funding_trend_joins_equivalent_signed_benefits(
    tmp_path: Path, metric_name: str, staged_benefits: float, disclosed_benefits: float
) -> None:
    """Signed and magnitude representations agree before version conflict detection."""
    from pension_data.query.saved_views.service import execute_funding_trend_view

    benefit_row = {
        **_metric_row(fact_id="fact:benefits"),
        "metric_family": "cash_flow",
        "metric_name": metric_name,
        "as_reported_value": staged_benefits,
        "normalized_value": staged_benefits,
        "as_reported_unit": "USD",
        "normalized_unit": "USD",
    }
    run = _write_run(tmp_path, "run", [_metric_row(), benefit_row])
    _write_flow_artifact(run, benefit_payments_usd=disclosed_benefits)

    snapshot = load_query_data_snapshot(tmp_path)

    assert snapshot.funding_trend_conflicts == ()
    assert snapshot.funding_trend_unresolved_periods == ()
    assert len(snapshot.funding_trend_inputs) == 1
    assert snapshot.funding_trend_inputs[0].benefit_payments_usd == 30.0
    rows = execute_funding_trend_view(list(snapshot.funding_trend_inputs))
    assert rows[0].net_external_cash_flow_usd == 85.0
    # Projection normalization must preserve the original metric history value.
    history = next(row for row in snapshot.metric_history_rows if row.metric_name == metric_name)
    assert history.normalized_value == staged_benefits


def test_load_query_data_snapshot_rejects_conflicting_duplicate_fact_id(
    tmp_path: Path,
) -> None:
    _write_run(tmp_path, "run-a", [_metric_row()])
    _write_run(tmp_path, "run-b", [_metric_row(normalized_value=0.8)])

    with pytest.raises(QueryArtifactError, match="conflicting duplicate fact_id"):
        load_query_data_snapshot(tmp_path)


def test_load_query_data_snapshot_rejects_manifest_identity_mismatch(tmp_path: Path) -> None:
    _write_run(tmp_path, "run-a", [_metric_row(plan_id="OTHER")])

    with pytest.raises(QueryArtifactError, match="plan_id conflicts with run manifest"):
        load_query_data_snapshot(tmp_path)


def test_load_query_data_snapshot_rejects_nonfinite_confidence(tmp_path: Path) -> None:
    _write_run(tmp_path, "run-a", [_metric_row(confidence=float("nan"))])

    with pytest.raises(QueryArtifactError, match="confidence must be finite"):
        load_query_data_snapshot(tmp_path)


def test_load_query_data_snapshot_keeps_revised_rows_and_marks_cross_document_conflict(
    tmp_path: Path,
) -> None:
    _write_run(
        tmp_path,
        "run-original",
        [
            _metric_row(
                fact_id="fact:funded:original",
                normalized_value=0.784,
                source_document_id="doc:syn-plan:original",
            )
        ],
        source_document_id="doc:syn-plan:original",
    )
    _write_run(
        tmp_path,
        "run-revised",
        [
            _metric_row(
                fact_id="fact:funded:revised",
                normalized_value=0.8,
                source_document_id="doc:syn-plan:revised",
                ingestion_date="2026-02-01",
            )
        ],
        source_document_id="doc:syn-plan:revised",
    )

    snapshot = load_query_data_snapshot(tmp_path)

    funded_rows = [row for row in snapshot.metric_history_rows if row.metric_name == "funded_ratio"]
    assert len(funded_rows) == 2
    assert {row.normalized_value for row in funded_rows} == {0.784, 0.8}
    assert snapshot.funding_trend_inputs == ()
    assert len(snapshot.funding_trend_unresolved_periods) == 1
    unresolved = snapshot.funding_trend_unresolved_periods[0]
    assert unresolved.entity_id == "SYN-PLAN"
    assert unresolved.plan_period == "FY2024"
    assert unresolved.conflicts[0].metric_target == "funded_ratio"
    assert len(unresolved.conflicts[0].versions) == 2


@pytest.mark.parametrize("other_ingestion_date", ["2026-01-01", "2026-02-01"])
@pytest.mark.parametrize("reverse_run_order", [False, True])
def test_load_query_data_snapshot_agreeing_documents_remain_resolved(
    tmp_path: Path, other_ingestion_date: str, reverse_run_order: bool
) -> None:
    """Document identity and ingestion order alone cannot create a value conflict."""
    observations = [
        ("doc:syn-plan:original", "2026-01-01"),
        ("doc:syn-plan:other", other_ingestion_date),
    ]
    if reverse_run_order:
        observations.reverse()
    for index, (source_document_id, ingestion_date) in enumerate(observations):
        row = _metric_row(
            fact_id=f"fact:funded:{source_document_id}",
            source_document_id=source_document_id,
            ingestion_date=ingestion_date,
        )
        _write_run(
            tmp_path,
            f"run-{index}",
            [row],
            source_document_id=source_document_id,
        )
        _write_run(
            tmp_path,
            f"run-{index}-copy",
            [row],
            source_document_id=source_document_id,
        )

    snapshot = load_query_data_snapshot(tmp_path)

    assert len(snapshot.metric_history_rows) == 2
    assert {
        (row.source_document_id, row.ingestion_date) for row in snapshot.metric_history_rows
    } == set(observations)
    assert {row.normalized_value for row in snapshot.metric_history_rows} == {0.784}
    assert len(snapshot.funding_trend_inputs) == 1
    trend_input = snapshot.funding_trend_inputs[0]
    assert (trend_input.plan_id, trend_input.plan_period) == ("SYN-PLAN", "FY2024")
    assert trend_input.funded_ratio == pytest.approx(0.784)
    assert snapshot.funding_trend_conflicts == ()
    assert snapshot.funding_trend_unresolved_periods == ()


def test_load_query_data_snapshot_same_document_revision_supersedes_for_trend(
    tmp_path: Path,
) -> None:
    _write_run(
        tmp_path,
        "run-original",
        [
            _metric_row(
                fact_id="fact:funded:original",
                normalized_value=0.784,
                ingestion_date="2026-01-01",
            )
        ],
    )
    _write_run(
        tmp_path,
        "run-revised",
        [
            _metric_row(
                fact_id="fact:funded:revised",
                normalized_value=0.8,
                ingestion_date="2026-02-01",
            )
        ],
    )

    snapshot = load_query_data_snapshot(tmp_path)

    assert len(snapshot.metric_history_rows) == 2
    assert snapshot.funding_trend_inputs[0].funded_ratio == pytest.approx(0.8)
    assert snapshot.funding_trend_unresolved_periods == ()


@pytest.mark.parametrize("other_document_value", [0.8, 0.82])
@pytest.mark.parametrize("reverse_run_order", [False, True])
def test_load_query_data_snapshot_cross_document_conflict_survives_same_document_revision(
    tmp_path: Path, other_document_value: float, reverse_run_order: bool
) -> None:
    """Same-document supersession cannot establish authority across documents."""
    revisions = [
        ("doc:syn-plan", 0.784, "2026-01-01"),
        ("doc:syn-plan", 0.8, "2026-02-01"),
        ("doc:other", other_document_value, "2026-03-01"),
    ]
    ordered_revisions = list(reversed(revisions)) if reverse_run_order else revisions
    for index, (source_document_id, value, ingestion_date) in enumerate(ordered_revisions):
        _write_run(
            tmp_path,
            f"run-{index}",
            [
                _metric_row(
                    fact_id=f"fact:funded:{index}",
                    source_document_id=source_document_id,
                    normalized_value=value,
                    ingestion_date=ingestion_date,
                )
            ],
            source_document_id=source_document_id,
        )

    snapshot = load_query_data_snapshot(tmp_path)

    assert len(snapshot.metric_history_rows) == 3
    assert {
        (row.source_document_id, row.normalized_value, row.ingestion_date)
        for row in snapshot.metric_history_rows
    } == set(revisions)
    assert snapshot.funding_trend_inputs == ()
    assert len(snapshot.funding_trend_unresolved_periods) == 1
    unresolved = snapshot.funding_trend_unresolved_periods[0]
    assert (unresolved.entity_id, unresolved.plan_period) == ("SYN-PLAN", "FY2024")
    assert len(unresolved.conflicts) == 1
    conflict = unresolved.conflicts[0]
    assert (conflict.entity_id, conflict.plan_period, conflict.metric_target) == (
        "SYN-PLAN",
        "FY2024",
        "funded_ratio",
    )
    assert snapshot.funding_trend_conflicts == (conflict,)
    assert len(conflict.versions) == 3
    assert {
        (version.source_document_id, version.normalized_value, version.ingestion_date)
        for version in conflict.versions
    } == set(revisions)
    assert {version.effective_date for version in conflict.versions} == {"2024-06-30"}


@pytest.mark.parametrize(
    ("metric_name", "metric_target"),
    [
        ("employer_contributions", "employer_contributions_usd"),
        ("employer_contributions_usd", "employer_contributions_usd"),
        ("employee_contributions", "employee_contributions_usd"),
        ("employee_contributions_usd", "employee_contributions_usd"),
        ("benefit_payments", "benefit_payments_usd"),
        ("benefit_payments_usd", "benefit_payments_usd"),
    ],
)
def test_load_query_data_snapshot_cash_flow_conflict_is_scoped_to_period(
    tmp_path: Path, metric_name: str, metric_target: str
) -> None:
    """A cash-flow disagreement omits only its period, even with a stable funded ratio."""
    for version, value, ingestion_date in (
        ("original", 100.0, "2026-01-01"),
        ("revised", 120.0, "2026-02-01"),
    ):
        source_document_id = f"doc:syn-plan:{version}"
        funded_row = _metric_row(
            fact_id=f"fact:funded:{version}",
            source_document_id=source_document_id,
            ingestion_date=ingestion_date,
        )
        cash_flow_row = {
            **funded_row,
            "fact_id": f"fact:cash-flow:{version}",
            "metric_name": metric_name,
            "metric_family": "cash_flow",
            "as_reported_value": value,
            "normalized_value": value,
            "as_reported_unit": "USD",
            "normalized_unit": "USD",
        }
        _write_run(
            tmp_path,
            f"run-{version}",
            [funded_row, cash_flow_row],
            source_document_id=source_document_id,
        )
    _write_run(
        tmp_path,
        "run-clean-period",
        [
            {
                **_metric_row(fact_id="fact:funded:clean", normalized_value=0.85),
                "plan_period": "FY2025",
                "effective_date": "2025-06-30",
            }
        ],
        plan_period="FY2025",
    )

    snapshot = load_query_data_snapshot(tmp_path)

    assert len(snapshot.metric_history_rows) == 5
    cash_flow_rows = [row for row in snapshot.metric_history_rows if row.metric_name == metric_name]
    assert {row.normalized_value for row in cash_flow_rows} == {100.0, 120.0}
    assert len(snapshot.funding_trend_inputs) == 1
    trend_input = snapshot.funding_trend_inputs[0]
    assert (trend_input.plan_id, trend_input.plan_period) == ("SYN-PLAN", "FY2025")
    assert trend_input.funded_ratio == pytest.approx(0.85)
    assert len(snapshot.funding_trend_unresolved_periods) == 1
    unresolved = snapshot.funding_trend_unresolved_periods[0]
    assert (unresolved.entity_id, unresolved.plan_period) == ("SYN-PLAN", "FY2024")
    assert len(unresolved.conflicts) == 1
    conflict = unresolved.conflicts[0]
    assert (conflict.entity_id, conflict.plan_period, conflict.metric_target) == (
        "SYN-PLAN",
        "FY2024",
        metric_target,
    )
    assert snapshot.funding_trend_conflicts == (conflict,)
    assert {
        (
            version.normalized_value,
            version.source_document_id,
            version.effective_date,
            version.ingestion_date,
        )
        for version in conflict.versions
    } == {
        (100.0, "doc:syn-plan:original", "2024-06-30", "2026-01-01"),
        (120.0, "doc:syn-plan:revised", "2024-06-30", "2026-02-01"),
    }


@pytest.mark.parametrize("reverse_run_order", [False, True])
def test_load_query_data_snapshot_keeps_conflicts_separate_between_plans(
    tmp_path: Path, reverse_run_order: bool
) -> None:
    """Conflicts sharing a period retain each plan's versions and clean periods."""
    observations = [
        ("PLAN-ALPHA", "original", 0.784),
        ("PLAN-ALPHA", "revised", 0.8),
        ("PLAN-BETA", "original", 0.65),
        ("PLAN-BETA", "revised", 0.7),
    ]
    ordered = list(reversed(observations)) if reverse_run_order else observations
    for index, (plan_id, version, value) in enumerate(ordered):
        source_document_id = f"doc:{plan_id}:{version}"
        _write_run(
            tmp_path,
            f"run-{index}",
            [
                _metric_row(
                    fact_id=f"fact:{plan_id}:{version}",
                    plan_id=plan_id,
                    normalized_value=value,
                    source_document_id=source_document_id,
                    ingestion_date="2026-01-01" if version == "original" else "2026-02-01",
                )
            ],
            plan_id=plan_id,
            source_document_id=source_document_id,
        )
    _write_run(
        tmp_path,
        "run-clean-period",
        [
            {
                **_metric_row(fact_id="fact:alpha:clean", plan_id="PLAN-ALPHA"),
                "plan_period": "FY2025",
                "effective_date": "2025-06-30",
            }
        ],
        plan_id="PLAN-ALPHA",
        plan_period="FY2025",
    )

    snapshot = load_query_data_snapshot(tmp_path)

    assert len(snapshot.metric_history_rows) == 5
    assert len(snapshot.funding_trend_inputs) == 1
    clean_input = snapshot.funding_trend_inputs[0]
    assert (clean_input.plan_id, clean_input.plan_period) == ("PLAN-ALPHA", "FY2025")
    assert clean_input.funded_ratio == pytest.approx(0.784)
    assert [
        (period.entity_id, period.plan_period)
        for period in snapshot.funding_trend_unresolved_periods
    ] == [("PLAN-ALPHA", "FY2024"), ("PLAN-BETA", "FY2024")]
    assert snapshot.funding_trend_conflicts == tuple(
        conflict
        for period in snapshot.funding_trend_unresolved_periods
        for conflict in period.conflicts
    )
    for period in snapshot.funding_trend_unresolved_periods:
        assert len(period.conflicts) == 1
        conflict = period.conflicts[0]
        assert (conflict.entity_id, conflict.plan_period, conflict.metric_target) == (
            period.entity_id,
            "FY2024",
            "funded_ratio",
        )
        assert {
            (item.source_document_id, item.normalized_value, item.ingestion_date)
            for item in conflict.versions
        } == {
            (
                f"doc:{plan_id}:{version}",
                value,
                "2026-01-01" if version == "original" else "2026-02-01",
            )
            for plan_id, version, value in observations
            if plan_id == period.entity_id
        }


def test_load_query_data_snapshot_collects_all_conflicts_in_one_period(tmp_path: Path) -> None:
    metric_values = {
        "funded_ratio": (0.784, 0.8),
        "employer_contributions": (100.0, 120.0),
        "benefit_payments": (80.0, 90.0),
    }
    for index, version in enumerate(("original", "revised")):
        source_document_id = f"doc:syn-plan:{version}"
        rows = []
        for metric_name, values in metric_values.items():
            row = _metric_row(
                fact_id=f"fact:{metric_name}:{version}",
                normalized_value=values[index],
                source_document_id=source_document_id,
                ingestion_date=f"2026-0{index + 1}-01",
            )
            if metric_name != "funded_ratio":
                row.update(
                    metric_name=metric_name,
                    metric_family="cash_flow",
                    as_reported_value=values[index],
                    as_reported_unit="USD",
                    normalized_unit="USD",
                )
            rows.append(row)
        _write_run(tmp_path, f"run-{version}", rows, source_document_id=source_document_id)

    snapshot = load_query_data_snapshot(tmp_path)

    assert len(snapshot.metric_history_rows) == 6
    assert snapshot.funding_trend_inputs == ()
    assert len(snapshot.funding_trend_unresolved_periods) == 1
    unresolved = snapshot.funding_trend_unresolved_periods[0]
    assert (unresolved.entity_id, unresolved.plan_period) == ("SYN-PLAN", "FY2024")
    assert snapshot.funding_trend_conflicts == unresolved.conflicts
    assert [conflict.metric_target for conflict in unresolved.conflicts] == [
        "benefit_payments_usd",
        "employer_contributions_usd",
        "funded_ratio",
    ]
    for conflict in unresolved.conflicts:
        assert (conflict.entity_id, conflict.plan_period) == ("SYN-PLAN", "FY2024")
        metric_name = conflict.metric_target.removesuffix("_usd")
        assert {
            (
                version.normalized_value,
                version.source_document_id,
                version.effective_date,
                version.ingestion_date,
            )
            for version in conflict.versions
        } == {
            (metric_values[metric_name][0], "doc:syn-plan:original", "2024-06-30", "2026-01-01"),
            (metric_values[metric_name][1], "doc:syn-plan:revised", "2024-06-30", "2026-02-01"),
        }


@pytest.mark.parametrize(
    ("first_ingestion", "second_ingestion"),
    [
        pytest.param("2026-02-01T06:00:00Z", "2026-02-01T06:00:00+00:00", id="aware-equivalents"),
        pytest.param("2026-02-01T06:00:00", "2026-02-01T06:00:00Z", id="naive-aware"),
        pytest.param("2026-02-01T06:00:00Z", "2026-02-01T06:00:00", id="aware-naive"),
        pytest.param("2026-02-01T06:00:00", "2026-02-01T11:00:00+05:00", id="naive-offset"),
        pytest.param("2026-02-01", "2026-02-01T00:00:00Z", id="date-only-aware"),
    ],
)
def test_load_query_data_snapshot_same_document_ingestion_tie_is_unresolved(
    tmp_path: Path, first_ingestion: str, second_ingestion: str
) -> None:
    """Equivalent ingestion instants cannot adjudicate differing same-document values."""
    _write_run(
        tmp_path,
        "run-a",
        [
            _metric_row(
                fact_id="fact:funded:a",
                normalized_value=0.784,
                ingestion_date=first_ingestion,
            )
        ],
    )
    _write_run(
        tmp_path,
        "run-b",
        [
            _metric_row(
                fact_id="fact:funded:b",
                normalized_value=0.8,
                ingestion_date=second_ingestion,
            )
        ],
    )

    snapshot = load_query_data_snapshot(tmp_path)

    assert len(snapshot.metric_history_rows) == 2
    assert {
        (row.normalized_value, row.ingestion_date, row.source_document_id, row.effective_date)
        for row in snapshot.metric_history_rows
    } == {
        (0.784, first_ingestion, "doc:syn-plan", "2024-06-30"),
        (0.8, second_ingestion, "doc:syn-plan", "2024-06-30"),
    }
    assert snapshot.funding_trend_inputs == ()
    assert len(snapshot.funding_trend_unresolved_periods) == 1
    unresolved = snapshot.funding_trend_unresolved_periods[0]
    assert unresolved.entity_id == "SYN-PLAN"
    assert unresolved.plan_period == "FY2024"
    assert len(unresolved.conflicts) == 1
    conflict = unresolved.conflicts[0]
    assert conflict.metric_target == "funded_ratio"
    assert snapshot.funding_trend_conflicts == (conflict,)
    assert len(conflict.versions) == 2
    assert {
        (
            version.normalized_value,
            version.ingestion_date,
            version.source_document_id,
            version.effective_date,
        )
        for version in conflict.versions
    } == {
        (0.784, first_ingestion, "doc:syn-plan", "2024-06-30"),
        (0.8, second_ingestion, "doc:syn-plan", "2024-06-30"),
    }


def test_load_query_data_snapshot_tied_conflict_order_is_independent_of_run_names(
    tmp_path: Path,
) -> None:
    """Renaming runs cannot reorder otherwise identical conflicting versions."""
    snapshots = []
    for label, values in (
        ("forward", (0.784, 0.8)),
        ("reverse", (0.8, 0.784)),
    ):
        root = tmp_path / label
        for index, value in enumerate(values):
            _write_run(
                root,
                f"run-{index}",
                [
                    _metric_row(
                        fact_id=f"fact:funded:{value}",
                        normalized_value=value,
                    )
                ],
            )
        snapshot = load_query_data_snapshot(root)
        assert {row.normalized_value for row in snapshot.metric_history_rows} == {0.784, 0.8}
        assert snapshot.funding_trend_inputs == ()
        assert len(snapshot.funding_trend_conflicts) == 1
        assert len(snapshot.funding_trend_unresolved_periods) == 1
        snapshots.append(snapshot)

    assert snapshots[0].funding_trend_conflicts == snapshots[1].funding_trend_conflicts
    assert (
        snapshots[0].funding_trend_unresolved_periods
        == snapshots[1].funding_trend_unresolved_periods
    )


def test_load_query_data_snapshot_same_document_timezone_aware_supersession(
    tmp_path: Path,
) -> None:
    _write_run(
        tmp_path,
        "run-earlier-offset",
        [
            _metric_row(
                fact_id="fact:funded:earlier",
                normalized_value=0.784,
                # Wall-clock string sorts after the later UTC instant, but is earlier.
                ingestion_date="2026-02-01T10:00:00+05:00",
            )
        ],
    )
    _write_run(
        tmp_path,
        "run-later-utc",
        [
            _metric_row(
                fact_id="fact:funded:later",
                normalized_value=0.8,
                ingestion_date="2026-02-01T06:00:00Z",
            )
        ],
    )

    snapshot = load_query_data_snapshot(tmp_path)

    assert snapshot.funding_trend_inputs[0].funded_ratio == pytest.approx(0.8)
    assert snapshot.funding_trend_unresolved_periods == ()


@pytest.mark.parametrize("reverse_run_order", [False, True])
@pytest.mark.parametrize(
    "revisions",
    [
        pytest.param(
            [
                (0.784, "2026-01-01"),
                (0.8, "2026-02-01T06:00:00Z"),
                (0.8, "2026-02-01T11:00:00+05:00"),
            ],
            id="agreeing-latest-copies",
        ),
        pytest.param(
            [
                (0.784, "2026-01-01T06:00:00Z"),
                (0.75, "2026-01-01T11:00:00+05:00"),
                (0.8, "2026-02-01"),
            ],
            id="superseded-historical-tie",
        ),
    ],
)
def test_load_query_data_snapshot_resolves_same_document_three_version_history(
    tmp_path: Path, revisions: list[tuple[float, str]], reverse_run_order: bool
) -> None:
    """Only disagreement at the latest instant blocks same-document supersession."""
    ordered_revisions = list(reversed(revisions)) if reverse_run_order else revisions
    for index, (value, ingestion_date) in enumerate(ordered_revisions):
        _write_run(
            tmp_path,
            f"run-{index}",
            [
                _metric_row(
                    fact_id=f"fact:funded:{index}",
                    normalized_value=value,
                    ingestion_date=ingestion_date,
                )
            ],
        )

    snapshot = load_query_data_snapshot(tmp_path)

    assert len(snapshot.metric_history_rows) == 3
    assert {
        (row.normalized_value, row.ingestion_date) for row in snapshot.metric_history_rows
    } == set(revisions)
    assert {row.source_document_id for row in snapshot.metric_history_rows} == {"doc:syn-plan"}
    assert len(snapshot.funding_trend_inputs) == 1
    trend_input = snapshot.funding_trend_inputs[0]
    assert (trend_input.plan_id, trend_input.plan_period) == ("SYN-PLAN", "FY2024")
    assert trend_input.funded_ratio == pytest.approx(0.8)
    assert snapshot.funding_trend_conflicts == ()
    assert snapshot.funding_trend_unresolved_periods == ()


def test_load_query_data_snapshot_rejects_symlink_escape(tmp_path: Path) -> None:
    root = tmp_path / "root"
    pilot_root = root / "one_pdf_pilot"
    pilot_root.mkdir(parents=True)
    outside = tmp_path / "outside-run"
    outside.mkdir()
    (pilot_root / "escaped").symlink_to(outside, target_is_directory=True)

    with pytest.raises(QueryArtifactError, match="escapes configured root"):
        load_query_data_snapshot(root)


def test_explicit_null_flow_declaration_is_invalid(tmp_path):
    run = _write_run(tmp_path, "run", [_metric_row()])
    _write_flow_artifact(run)
    manifest_path = run / "run_manifest.json"
    manifest = json.loads(manifest_path.read_text())
    manifest["artifact_files"]["financial_flow_rows_json"] = None
    manifest_path.write_text(json.dumps(manifest))
    with pytest.raises(QueryArtifactError, match="must be a non-empty path"):
        load_query_data_snapshot(tmp_path)


def test_funding_trend_rejects_overflowed_flow_total(tmp_path):
    run = _write_run(tmp_path, "run", [_metric_row()])
    _write_flow_artifact(run, benefit_payments_usd=-1e308, refunds_usd=-1e308)
    with pytest.raises(QueryArtifactError, match="net cash flow must be finite"):
        load_query_data_snapshot(tmp_path)
