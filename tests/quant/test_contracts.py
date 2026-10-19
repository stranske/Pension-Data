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
    require_reproducibility,
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
    assert frozenset({"aal_usd", "ava_usd"}) == PILOT_CORE_METRIC_NAMES
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


def test_nonempty_workspace_preserves_lineage_and_truthful_reproducibility(
    tmp_path: Path,
) -> None:
    run_dir = tmp_path / "completed_pilot_run"
    shutil.copytree(_COMPLETED_PILOT, run_dir)
    payload = build_quant_workspace(run_dir)
    workspace = QuantWorkspaceContract(
        plan_id=payload["plan_id"],
        plan_period=payload["plan_period"],
        as_of_date=payload["as_of_date"],
        module=payload["module"],
        scenarios=tuple(
            QuantScenarioContract(
                scenario_id=scenario["scenario_id"],
                scenario_label=scenario["scenario_label"],
                module=scenario["module"],
                baseline_scenario_id=scenario["baseline_scenario_id"],
                reproducibility=ReproducibilityEnvelope(**scenario["reproducibility"]),
                series=tuple(
                    QuantSeriesContract(
                        series_id=series["series_id"],
                        metric_name=series["metric_name"],
                        label=series["label"],
                        chart_kind=series["chart_kind"],
                        points=tuple(QuantDataPoint(**point) for point in series["points"]),
                    )
                    for series in scenario["series"]
                ),
                warnings=tuple(scenario["warnings"]),
            )
            for scenario in payload["scenarios"]
        ),
    )
    assert workspace.module == "metric_engine"
    assert workspace.scenarios
    assert workspace.scenarios[0].series
    envelope = workspace.scenarios[0].reproducibility
    require_reproducibility(
        envelope,
        requires_seed=False,
        expected_source_artifact_ids=envelope.source_artifact_ids,
    )
    assert set(envelope.source_artifact_ids) == {
        row["id"] for row in payload["input_artifacts"].values()
    }
    assert envelope.config_hash == payload["scenarios"][0]["reproducibility"]["config_hash"]
    assert envelope.input_snapshot_id.startswith("sha256:")
    assert envelope.code_version.startswith("sha256:")

    core_rows = {
        row["fact_id"]: row
        for row in json.loads(
            (run_dir / "extraction_persistence/staging_core_metrics.json").read_text(
                encoding="utf-8"
            )
        )
    }
    for metric in payload["metric_lineage"]:
        assert metric["source_fact_ids"]
        assert metric["confidence"] == min(
            core_rows[fact_id]["confidence"] for fact_id in metric["source_fact_ids"]
        )
        for fact_id in metric["source_fact_ids"]:
            evidence = payload["fact_lineage"][fact_id]
            assert evidence["evidence_refs"] == core_rows[fact_id]["evidence_refs"]
            assert evidence["confidence"] == core_rows[fact_id]["confidence"]
            assert (
                evidence["artifact_id"]
                == payload["input_artifacts"]["staging_core_metrics_json"]["id"]
            )
    assert payload["scenarios"][0]["warnings"] == (
        "net_cash_flow_usd omitted: optional cash-flow inputs absent or incomplete",
        "contribution_to_benefit_ratio omitted: optional cash-flow inputs absent or incomplete",
    )


@pytest.mark.parametrize("field", ["config_hash", "source_artifact_ids"])
def test_missing_reproducibility_retains_prior_quant_artifact(tmp_path: Path, field: str) -> None:
    run_dir = tmp_path / "completed_pilot_run"
    shutil.copytree(_COMPLETED_PILOT, run_dir)
    out = tmp_path / DEFAULT_QUANT_WORKSPACE_PATH
    write_quant_workspace(out, build_quant_workspace(run_dir))
    before = out.read_bytes()
    code = """
import importlib.util, sys
spec = importlib.util.spec_from_file_location('quant_cli', sys.argv[1])
cli = importlib.util.module_from_spec(spec); spec.loader.exec_module(cli)
original = cli.build_quant_workspace
def broken(run):
    payload = original(run)
    del payload['scenarios'][0]['reproducibility'][sys.argv[4]]
    return payload
cli.build_quant_workspace = broken
sys.exit(cli.main(['--pilot-run-dir', sys.argv[2], '--out', sys.argv[3]]))
"""
    result = subprocess.run(
        [sys.executable, "-c", code, str(_CLI), str(run_dir), str(out), field],
        capture_output=True,
        text=True,
        check=False,
    )
    assert result.returncode == 1
    assert "reproducibility" in result.stderr
    assert out.read_bytes() == before


def test_missing_required_core_input_retains_prior_artifact_and_rejects_empty(
    tmp_path: Path,
) -> None:
    run_dir = tmp_path / "completed_pilot_run"
    shutil.copytree(_COMPLETED_PILOT, run_dir)
    out = tmp_path / "quant-workspace.generated.json"
    write_quant_workspace(out, build_quant_workspace(run_dir))
    before = out.read_bytes()
    core = run_dir / "extraction_persistence/staging_core_metrics.json"
    rows = [
        row
        for row in json.loads(core.read_text(encoding="utf-8"))
        if row["metric_name"] != "aal_usd"
    ]
    core.write_text(json.dumps(rows), encoding="utf-8")
    with pytest.raises(ValueError, match="eligible AAL and AVA"):
        build_quant_workspace(run_dir)
    result = subprocess.run(
        [sys.executable, str(_CLI), "--pilot-run-dir", str(run_dir), "--out", str(out)],
        capture_output=True,
        text=True,
        check=False,
    )
    assert result.returncode == 1
    assert out.read_bytes() == before
    retained = json.loads(before.decode())
    assert retained["scenarios"][0]["series"][0]["metric_name"] == "funded_gap_usd"


def test_partial_cash_flow_omits_metric_with_warning_not_zero(tmp_path: Path) -> None:
    run_dir = tmp_path / "completed_pilot_run_with_flows"
    shutil.copytree(_COMPLETED_PILOT_FLOWS, run_dir)
    flow_path = run_dir / "document_orchestration/quant-fixture/financial_flow_rows.json"
    rows = json.loads(flow_path.read_text(encoding="utf-8"))
    rows[0]["refunds_usd"] = None
    flow_path.write_text(json.dumps(rows), encoding="utf-8")
    payload = build_quant_workspace(run_dir)
    values = _metric_values(payload)
    assert "net_cash_flow_usd" not in values
    assert values["contribution_to_benefit_ratio"] == pytest.approx(120 / 30)
    assert payload["scenarios"][0]["warnings"] == (
        "net_cash_flow_usd omitted: optional cash-flow inputs absent or incomplete",
    )
