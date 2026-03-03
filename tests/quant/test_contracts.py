"""Tests for quantitative analysis contracts and reproducibility gates."""

from __future__ import annotations

from pension_data.quant.contracts import (
    QuantDataPoint,
    QuantScenarioContract,
    QuantSeriesContract,
    QuantWorkspaceContract,
    ReproducibilityEnvelope,
    missing_reproducibility_fields,
    normalize_provenance_refs,
)


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
