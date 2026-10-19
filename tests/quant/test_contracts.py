"""Tests for quantitative analysis contracts and reproducibility gates."""

from __future__ import annotations

import json
import shutil
import subprocess
import sys
from pathlib import Path

import pytest

from pension_data.quant.contracts import (
    PILOT_CASH_FLOW_FIELD_MAP,
    PILOT_CORE_METRIC_NAMES,
    QuantDataPoint,
    QuantScenarioContract,
    QuantSeriesContract,
    QuantWorkspaceContract,
    ReproducibilityEnvelope,
    missing_reproducibility_fields,
    normalize_provenance_refs,
)
from pension_data.quant.workspace_producer import (
    DEFAULT_QUANT_WORKSPACE_PATH,
    build_quant_workspace,
    write_quant_workspace,
)

_ROOT = Path(__file__).resolve().parents[2]
_FIXTURES = Path(__file__).resolve().parent / "fixtures"
_COMPLETED_PILOT = _FIXTURES / "completed_pilot_run"
_COMPLETED_PILOT_FLOWS = _FIXTURES / "completed_pilot_run_with_flows"
_CLI = _ROOT / "scripts" / "web" / "build_quant_workspace.py"


def _metric_values(payload: dict) -> dict[str, float]:
    return {
        series["metric_name"]: series["points"][0]["y_value"]
        for series in payload["scenarios"][0]["series"]
    }


def test_normalize_provenance_refs_is_deterministic() -> None:
    refs = normalize_provenance_refs((" p.10 ", "p.10", "", "p.11"))
    assert refs == ("p.10", "p.11")


def test_missing_reproducibility_fields_for_seeded_mode() -> None:
    envelope = ReproducibilityEnvelope(
        run_id="scenario:baseline-vs-stress",
        config_hash="cfg:abcd1234",
        code_version="git:5ceacee",
        input_snapshot_id="snapshot:2026-03-03",
        generated_at="2026-03-03T15:00:00Z",
        seed=None,
        source_artifact_ids=("artifact:doc:ca:2024:v1",),
    )
    missing = missing_reproducibility_fields(envelope, requires_seed=True)
    assert missing == ("seed",)


def test_workspace_contract_embeds_reproducible_scenario_payload() -> None:
    envelope = ReproducibilityEnvelope(
        run_id="scenario:stress-200bps",
        config_hash="cfg:f0c34ae1",
        code_version="git:5ceacee",
        input_snapshot_id="snapshot:2026-03-03",
        generated_at="2026-03-03T15:00:00Z",
        seed=42,
        source_artifact_ids=("artifact:doc:ca:2024:v1",),
    )
    scenario = QuantScenarioContract(
        scenario_id="stress-200bps",
        scenario_label="Rates +200 bps",
        module="scenario_analysis",
        baseline_scenario_id="baseline",
        reproducibility=envelope,
        series=(
            QuantSeriesContract(
                series_id="funded_ratio",
                metric_name="funded_ratio",
                label="Funded Ratio",
                chart_kind="line",
                points=(
                    QuantDataPoint(
                        x_label="FY2024",
                        y_value=0.782,
                        y_unit="ratio",
                        confidence=0.9,
                        provenance_refs=("p.40",),
                    ),
                ),
            ),
        ),
    )
    workspace = QuantWorkspaceContract(
        plan_id="CA-PERS",
        plan_period="FY2024",
        as_of_date="2024-06-30",
        module="scenario_analysis",
        scenarios=(scenario,),
    )
    assert workspace.scenarios[0].reproducibility.seed == 42
    assert workspace.scenarios[0].series[0].points[0].provenance_refs == ("p.40",)


def test_completed_pilot_fixture_emits_default_quant_workspace_artifact(tmp_path: Path) -> None:
    """Producer seam publishes a nonempty metric_engine artifact from a completed pilot."""
    run_dir = tmp_path / "completed_pilot_run"
    shutil.copytree(_COMPLETED_PILOT, run_dir)
    # AAL 640e6 − AVA 501.8e6 = funded gap 138.2e6; unfunded ratio = gap / AAL.
    expected_gap = 138_200_000.0
    expected_ratio = expected_gap / 640_000_000.0

    payload = build_quant_workspace(run_dir)
    assert payload["module"] == "metric_engine"
    assert payload["scenarios"]
    assert _metric_values(payload) == pytest.approx(
        {"funded_gap_usd": expected_gap, "unfunded_ratio": expected_ratio}
    )
    envelope = ReproducibilityEnvelope(**payload["scenarios"][0]["reproducibility"])
    assert missing_reproducibility_fields(envelope, requires_seed=False) == ()
    assert set(envelope.source_artifact_ids) == {
        row["id"] for row in payload["input_artifacts"].values()
    }

    out = tmp_path / DEFAULT_QUANT_WORKSPACE_PATH
    write_quant_workspace(out, payload)
    published = json.loads(out.read_text(encoding="utf-8"))
    assert published["module"] == "metric_engine"
    assert _metric_values(published)["funded_gap_usd"] == expected_gap

    core = run_dir / "extraction_persistence" / "staging_core_metrics.json"
    rows = json.loads(core.read_text(encoding="utf-8"))
    next(row for row in rows if row["metric_name"] == "ava_usd")["normalized_value"] = 400_000_000.0
    core.write_text(json.dumps(rows), encoding="utf-8")
    changed = build_quant_workspace(run_dir)
    assert _metric_values(changed)["funded_gap_usd"] == 240_000_000.0
    assert _metric_values(changed)["unfunded_ratio"] == 0.375


def test_cli_writes_default_quant_workspace_path_from_completed_pilot(tmp_path: Path) -> None:
    run_dir = tmp_path / "completed_pilot_run"
    shutil.copytree(_COMPLETED_PILOT, run_dir)
    result = subprocess.run(
        [sys.executable, str(_CLI), "--pilot-run-dir", str(run_dir)],
        cwd=tmp_path,
        capture_output=True,
        text=True,
        check=False,
    )
    assert result.returncode == 0, result.stderr
    assert "producer-only" in result.stdout
    artifact = tmp_path / DEFAULT_QUANT_WORKSPACE_PATH
    assert artifact.is_file()
    payload = json.loads(artifact.read_text(encoding="utf-8"))
    assert payload["module"] == "metric_engine"
    assert _metric_values(payload)["funded_gap_usd"] == 138_200_000.0
    assert payload["scenarios"][0]["series"]


def test_pilot_identifier_mapping_feeds_compute_derived_metrics(tmp_path: Path) -> None:
    """Explicit contracts.py maps pilot fields into metric-engine inputs."""
    assert PILOT_CORE_METRIC_NAMES == frozenset({"aal_usd", "ava_usd"})
    assert PILOT_CASH_FLOW_FIELD_MAP["employer_contributions_usd"] == (
        "employer_contributions_normalized"
    )
    run_dir = tmp_path / "completed_pilot_run_with_flows"
    shutil.copytree(_COMPLETED_PILOT_FLOWS, run_dir)
    payload = build_quant_workspace(run_dir)
    values = _metric_values(payload)
    assert values["funded_gap_usd"] == 138_200_000.0
    assert values["net_cash_flow_usd"] == 85_000_000.0
    assert values["contribution_to_benefit_ratio"] == 4.0
    for fact_id, lineage in payload["fact_lineage"].items():
        if lineage["identity_kind"] == "persisted fact_id":
            assert lineage["pilot_metric_name"] in PILOT_CORE_METRIC_NAMES
            assert lineage["metric_engine_input"] == lineage["pilot_metric_name"]
        else:
            assert lineage["pilot_to_metric_engine_fields"] == dict(PILOT_CASH_FLOW_FIELD_MAP)
            assert fact_id.startswith(lineage["artifact_id"])
    # Changing a mapped pilot cash-flow field changes the derived metric.
    flow_path = run_dir / "document_orchestration/quant-fixture/financial_flow_rows.json"
    rows = json.loads(flow_path.read_text(encoding="utf-8"))
    rows[0]["employer_contributions_usd"] = 200_000_000.0
    flow_path.write_text(json.dumps(rows), encoding="utf-8")
    changed = build_quant_workspace(run_dir)
    assert _metric_values(changed)["net_cash_flow_usd"] == 185_000_000.0
    assert _metric_values(changed)["contribution_to_benefit_ratio"] == pytest.approx(220 / 30)
