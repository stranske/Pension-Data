"""Exercise the quant producer against completed production pilot fixtures."""

from __future__ import annotations

import copy
import json
import subprocess
import sys
from dataclasses import replace
from pathlib import Path

import pytest

from pension_data.ops.one_pdf_pilot import OnePdfPilotInput, run_one_pdf_pilot
from pension_data.quant.contracts import ReproducibilityEnvelope, missing_reproducibility_fields
from pension_data.quant.workspace_producer import build_quant_workspace, write_quant_workspace

ROOT = Path(__file__).resolve().parents[2]
CLI = ROOT / "scripts/web/build_quant_workspace.py"


def _pilot(tmp_path, monkeypatch, *, cash=False):
    if cash:
        import pension_data.ops.one_pdf_pilot as pilot

        parser = pilot.parse_pdf_to_funded_input

        def with_flows(payload):
            result = parser(payload)
            assert result.raw is not None
            rows = tuple(
                {"label": label, "value": value, "evidence_ref": "p.1#flows"}
                for label, value in (
                    ("Employer Contributions", "$100 million"),
                    ("Employee Contributions", "$20 million"),
                    ("Benefit Payments", "$-30 million"),
                    ("Refunds", "$-5 million"),
                )
            )
            return replace(
                result, raw=replace(result.raw, table_rows=(*result.raw.table_rows, *rows))
            )

        monkeypatch.setattr(pilot, "parse_pdf_to_funded_input", with_flows)
    pdf = tmp_path / "pilot.pdf"
    pdf.write_bytes(
        b"AAL: $640 million\nAVA: $501.8 million\nFunded Ratio: 78.4%\n"
        b"Discount Rate: 6.8%\nEmployer Contribution Rate: 21.4%\n"
        b"Employee Contribution Rate: 7.5%\nParticipant Count: 325000\n"
    )
    result = run_one_pdf_pilot(
        pilot_input=OnePdfPilotInput(
            pdf_path=pdf,
            plan_id="PLAN",
            plan_period="FY2024",
            effective_date="2024-06-30",
            ingestion_date="2026-10-19",
        ),
        output_root=tmp_path / "pilot-output",
        run_id="quant-fixture",
    )
    return Path(result["run_manifest_json"]).parent


def _values(payload):
    return {
        series["metric_name"]: series["points"][0]["y_value"]
        for series in payload["scenarios"][0]["series"]
    }


def _rewrite(path, transform):
    value = json.loads(path.read_text())
    transform(value)
    path.write_text(json.dumps(value))


def test_production_pilot_numeric_values_lineage_and_input_sensitivity(tmp_path, monkeypatch):
    run = _pilot(tmp_path, monkeypatch)
    payload = build_quant_workspace(run)
    assert payload["module"] == "metric_engine"
    assert _values(payload) == pytest.approx(
        {"funded_gap_usd": 138_200_000, "unfunded_ratio": 0.2159375}
    )
    envelope = ReproducibilityEnvelope(**payload["scenarios"][0]["reproducibility"])
    assert missing_reproducibility_fields(envelope, requires_seed=False) == ()
    assert set(envelope.source_artifact_ids) == {
        v["id"] for v in payload["input_artifacts"].values()
    }
    for descriptor in payload["input_artifacts"].values():
        import hashlib

        assert (
            hashlib.sha256((run / descriptor["path"]).read_bytes()).hexdigest()
            == descriptor["sha256"]
        )
    rows = json.loads((run / "extraction_persistence/staging_core_metrics.json").read_text())
    by_id = {row["fact_id"]: row for row in rows}
    for metric in payload["metric_lineage"]:
        assert set(metric["source_fact_ids"]) <= by_id.keys()
        assert metric["confidence"] == min(
            by_id[k]["confidence"] for k in metric["source_fact_ids"]
        )
        for fact_id in metric["source_fact_ids"]:
            evidence = payload["fact_lineage"][fact_id]
            assert evidence["evidence_refs"]
            assert evidence["source_document_id"] == by_id[fact_id]["source_document_id"]
    assert len(payload["scenarios"][0]["warnings"]) == 2
    core = run / "extraction_persistence/staging_core_metrics.json"
    _rewrite(
        core,
        lambda rows: next(row for row in rows if row["metric_name"] == "ava_usd").update(
            normalized_value=400_000_000
        ),
    )
    changed = build_quant_workspace(run)
    assert _values(changed)["funded_gap_usd"] == 240_000_000
    assert _values(changed)["unfunded_ratio"] == 0.375
    after = changed["scenarios"][0]["reproducibility"]
    assert after["input_snapshot_id"] != envelope.input_snapshot_id
    assert after["config_hash"] == envelope.config_hash


def test_pilot_cash_flow_artifact_reaches_real_metric_engine(tmp_path, monkeypatch):
    run = _pilot(tmp_path, monkeypatch, cash=True)
    payload = build_quant_workspace(run)
    assert _values(payload)["net_cash_flow_usd"] == 85_000_000
    assert _values(payload)["contribution_to_benefit_ratio"] == 4
    assert payload["scenarios"][0]["warnings"] == ()
    cash_metric = next(
        row for row in payload["metric_lineage"] if row["metric_name"] == "net_cash_flow_usd"
    )
    (fact_id,) = cash_metric["source_fact_ids"]
    assert payload["fact_lineage"][fact_id]["identity_kind"].startswith(
        "artifact digest plus row index"
    )
    flow_path = run / payload["input_artifacts"]["financial_flow_rows_json"]["path"]
    _rewrite(flow_path, lambda rows: rows[0].update(employer_contributions_usd=200_000_000))
    changed = build_quant_workspace(run)
    assert _values(changed)["net_cash_flow_usd"] == 185_000_000
    assert _values(changed)["contribution_to_benefit_ratio"] == pytest.approx(220 / 30)
    _rewrite(flow_path, lambda rows: rows[0].update(refunds_usd=None))
    partial = build_quant_workspace(run)
    assert "net_cash_flow_usd" not in _values(partial)
    assert _values(partial)["contribution_to_benefit_ratio"] == pytest.approx(220 / 30)
    assert partial["scenarios"][0]["warnings"] == (
        "net_cash_flow_usd omitted: optional cash-flow inputs absent or incomplete",
    )


@pytest.mark.parametrize("field", ["config_hash", "source_artifact_ids"])
def test_missing_reproducibility_fails_real_cli_without_replacing_output(
    tmp_path, monkeypatch, field
):
    run = _pilot(tmp_path, monkeypatch)
    out = tmp_path / "outputs/web/quant-workspace.generated.json"
    write_quant_workspace(out, build_quant_workspace(run))
    before = out.read_bytes()
    # Fault injection changes only the built envelope. The real CLI and real
    # publication validator still run in a separate interpreter process.
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
        [sys.executable, "-c", code, str(CLI), str(run), str(out), field],
        capture_output=True,
        text=True,
    )
    assert result.returncode == 1
    assert "reproducibility" in result.stderr
    assert out.read_bytes() == before


@pytest.mark.parametrize(
    "failure",
    [
        "unfinished",
        "missing-aal",
        "duplicate",
        "foreign-plan",
        "nonfinite",
        "unsafe-path",
        "missing-flow",
    ],
)
def test_invalid_input_never_replaces_previous_artifact(tmp_path, monkeypatch, failure):
    run = _pilot(tmp_path, monkeypatch)
    out = tmp_path / "quant.json"
    write_quant_workspace(out, build_quant_workspace(run))
    before = out.read_bytes()
    manifest = run / "run_manifest.json"
    core = run / "extraction_persistence/staging_core_metrics.json"
    if failure == "unfinished":
        _rewrite(manifest, lambda value: value.update(ledger_status="failed"))
    elif failure == "missing-aal":
        _rewrite(
            core,
            lambda rows: rows.__setitem__(
                slice(None), [row for row in rows if row["metric_name"] != "aal_usd"]
            ),
        )
    elif failure == "duplicate":
        _rewrite(
            core,
            lambda rows: rows.append(
                copy.deepcopy(next(row for row in rows if row["metric_name"] == "aal_usd"))
            ),
        )
    elif failure == "foreign-plan":
        _rewrite(
            core,
            lambda rows: next(row for row in rows if row["metric_name"] == "aal_usd").update(
                plan_id="OTHER"
            ),
        )
    elif failure == "nonfinite":
        _rewrite(
            core,
            lambda rows: next(row for row in rows if row["metric_name"] == "aal_usd").update(
                normalized_value=float("nan")
            ),
        )
    elif failure == "unsafe-path":
        _rewrite(
            manifest,
            lambda value: value["artifact_files"].update(staging_core_metrics_json=str(out)),
        )
    else:
        _rewrite(
            manifest,
            lambda value: value["artifact_files"].update(financial_flow_rows_json="absent.json"),
        )
    result = subprocess.run(
        [sys.executable, str(CLI), "--pilot-run-dir", str(run), "--out", str(out)],
        capture_output=True,
        text=True,
    )
    assert result.returncode == 1, result.stdout + result.stderr
    assert out.read_bytes() == before


def test_cli_default_output_is_nonempty_producer_only_contract(tmp_path, monkeypatch):
    run = _pilot(tmp_path, monkeypatch)
    result = subprocess.run(
        [sys.executable, str(CLI), "--pilot-run-dir", str(run)],
        cwd=tmp_path,
        capture_output=True,
        text=True,
    )
    assert result.returncode == 0, result.stderr
    payload = json.loads((tmp_path / "outputs/web/quant-workspace.generated.json").read_text())
    assert _values(payload)["funded_gap_usd"] == 138_200_000
    assert "producer-only" in result.stdout
