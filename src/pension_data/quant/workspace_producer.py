"""Publish deterministic metric-engine contracts from completed pilot artifacts."""

from __future__ import annotations

import hashlib
import json
import math
import os
import tempfile
from dataclasses import asdict
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from pension_data.quant import contracts, metric_engine
from pension_data.quant.contracts import (
    PILOT_CASH_FLOW_FIELD_MAP,
    PILOT_CASH_FLOW_OUTFLOW_FIELDS,
    PILOT_CORE_METRIC_NAMES,
    QuantScenarioContract,
    QuantWorkspaceContract,
    ReproducibilityEnvelope,
    require_reproducibility,
)

# Default publication path used by scripts/web/build_quant_workspace.py.
# Producer-only: consumers do not yet render this artifact.
DEFAULT_QUANT_WORKSPACE_PATH = Path("outputs/web/quant-workspace.generated.json")


def _text(row: dict[str, Any], key: str) -> str:
    value = row.get(key)
    if not isinstance(value, str) or not value.strip():
        raise ValueError(f"non-empty {key} required")
    return value.strip()


def _number(value: object) -> float:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise ValueError("finite numeric normalized value required")
    if not math.isfinite(value):
        raise ValueError("finite numeric normalized value required")
    return float(value)


def _refs(value: object) -> list[str]:
    if isinstance(value, str):
        value = json.loads(value)
    if (
        not isinstance(value, list)
        or not value
        or any(not isinstance(ref, str) or not ref.strip() for ref in value)
    ):
        raise ValueError("non-empty page evidence_refs required")
    return value


def _digest(value: bytes) -> str:
    return hashlib.sha256(value).hexdigest()


def _canonical(value: object) -> bytes:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), allow_nan=False).encode()


def build_quant_workspace(pilot_run_dir: Path) -> dict[str, Any]:
    """Load one successful run, validate its eligible facts, and compute real metrics.

    Artifact identities identify captured bytes, while fact identities identify
    rows within them. Neither is substituted for the other in lineage.
    """
    run_dir = pilot_run_dir.resolve(strict=True)
    artifacts: dict[str, dict[str, Any]] = {}
    raw_inputs: dict[str, object] = {}

    def capture(key: str, path: Path) -> object:
        path = path.resolve(strict=True)
        if not path.is_relative_to(run_dir) or not path.is_file():
            raise ValueError(f"{key} must be a file inside the pilot run")
        data = path.read_bytes()
        digest = _digest(data)
        artifacts[key] = {
            "id": f"{key}:sha256:{digest}",
            "path": str(path.relative_to(run_dir)),
            "sha256": digest,
        }
        raw_inputs[key] = json.loads(data)
        return raw_inputs[key]

    manifest = capture("run_manifest_json", run_dir / "run_manifest.json")
    if not isinstance(manifest, dict) or manifest.get("ledger_status") != "success":
        raise ValueError("completed pilot ledger_status=success required")
    inputs = manifest.get("input")
    files = manifest.get("artifact_files")
    if not isinstance(inputs, dict) or not isinstance(files, dict):
        raise ValueError("pilot input and artifact_files objects required")
    plan_id, period = _text(inputs, "plan_id"), _text(inputs, "plan_period")
    as_of = _text(inputs, "effective_date")
    source_document_id = _text(inputs, "source_document_id")

    def load_rows(key: str) -> list[dict[str, Any]]:
        path = Path(_text(files, key))
        if not path.is_absolute():
            # Support run-relative manifests and the pilot's legacy cwd-rooted
            # output paths. Every selected path must still resolve inside this run.
            relative = run_dir / path
            path = relative if relative.exists() else Path.cwd() / path
        rows = capture(key, path)
        if not isinstance(rows, list) or any(not isinstance(row, dict) for row in rows):
            raise ValueError(f"{key} must contain a row list")
        return rows

    lineage: dict[str, dict[str, Any]] = {}

    def check_row(row: dict[str, Any], *, require_evidence: bool = True) -> dict[str, Any]:
        if (_text(row, "plan_id"), _text(row, "plan_period")) != (plan_id, period):
            raise ValueError("row plan/period differs from pilot input")
        document = _text(row, "source_document_id")
        if document != source_document_id:
            raise ValueError("row source document differs from pilot input")
        confidence = row.get("confidence")
        if confidence is not None and not 0 <= _number(confidence) <= 1:
            raise ValueError("confidence must be in [0, 1] or null")
        refs = row.get("evidence_refs")
        refs = [] if not require_evidence and refs in (None, "", []) else _refs(refs)
        return {**row, "evidence_refs": refs}

    core: list[dict[str, Any]] = []
    metric_names: set[str] = set()
    for index, row in enumerate(load_rows("staging_core_metrics_json")):
        name = row.get("metric_name")
        if name not in PILOT_CORE_METRIC_NAMES:
            continue
        if row.get("valid_to") is not None or row.get("superseded_at") is not None:
            continue
        row = check_row(row)
        fact_id = _text(row, "fact_id")
        if name in metric_names or fact_id in lineage:
            raise ValueError("ambiguous duplicate required metric/fact identity")
        if _text(row, "normalized_unit").casefold() != "usd":
            raise ValueError("required core inputs must be normalized USD")
        value = _number(row.get("normalized_value"))
        if name == "aal_usd" and value <= 0:
            raise ValueError("positive AAL required for an unfunded ratio")
        if name == "ava_usd" and value < 0:
            raise ValueError("nonnegative AVA required")
        metric_names.add(name)
        core.append(row)
        lineage[fact_id] = {
            "artifact_id": artifacts["staging_core_metrics_json"]["id"],
            "row_index": index,
            "source_document_id": row["source_document_id"],
            "evidence_refs": row["evidence_refs"],
            "confidence": row.get("confidence"),
            "identity_kind": "persisted fact_id",
            "pilot_metric_name": name,
            "metric_engine_input": name,
        }
    if metric_names != set(PILOT_CORE_METRIC_NAMES):
        raise ValueError(
            "eligible AAL and AVA facts are required; empty results are not publishable"
        )

    flows: list[dict[str, Any]] = []
    warnings: list[str] = []
    flow_key = "financial_flow_rows_json"
    if flow_key in files:
        for index, row in enumerate(load_rows(flow_key)):
            disclosed = any(
                row.get(pilot_field) is not None for pilot_field in PILOT_CASH_FLOW_FIELD_MAP
            )
            row = check_row(row, require_evidence=disclosed)
            if not disclosed:
                # A real pilot can explicitly record a not-disclosed flow row
                # with no page evidence or quantities. It is absence, not zero.
                continue
            # The pilot flow artifact has no persisted cash_flow_id. Bind its
            # synthetic row identity to its exact artifact bytes and row offset.
            fact_id = f"{artifacts[flow_key]['id']}:row:{index}"
            flow: dict[str, Any] = {
                **row,
                "cash_flow_id": fact_id,
            }
            mapped_fields: dict[str, str] = {}
            for pilot_field, engine_field in PILOT_CASH_FLOW_FIELD_MAP.items():
                flow_value = row.get(pilot_field)
                if flow_value is not None:
                    flow_value = _number(flow_value)
                    if pilot_field in PILOT_CASH_FLOW_OUTFLOW_FIELDS and flow_value > 0:
                        raise ValueError(
                            "pilot benefit/refund values must have normalized outflow signs"
                        )
                flow[engine_field] = flow_value
                mapped_fields[pilot_field] = engine_field
            flows.append(flow)
            lineage[fact_id] = {
                "artifact_id": artifacts[flow_key]["id"],
                "row_index": index,
                "source_document_id": row["source_document_id"],
                "evidence_refs": row["evidence_refs"],
                "confidence": row.get("confidence"),
                "identity_kind": "artifact digest plus row index; not a persisted fact_id",
                "pilot_to_metric_engine_fields": mapped_fields,
            }
        if len(flows) > 1:
            raise ValueError("ambiguous multiple cash-flow rows for one pilot plan/period")
    observations = metric_engine.compute_derived_metrics(
        core_metric_rows=core, cash_flow_rows=flows
    )
    names = {row.metric_name for row in observations}
    for name in ("net_cash_flow_usd", "contribution_to_benefit_ratio"):
        if name not in names:
            warnings.append(f"{name} omitted: optional cash-flow inputs absent or incomplete")
    config = {
        "producer_schema": 1,
        "eligibility": "successful-pilot/current-single-plan-period/normalized-USD/v1",
        "catalog": [asdict(row) for row in metric_engine.default_metric_catalog()],
    }
    assert contracts.__file__ is not None and metric_engine.__file__ is not None
    code_bytes = b"".join(
        path.read_bytes()
        for path in (Path(contracts.__file__), Path(metric_engine.__file__), Path(__file__))
    )
    envelope = ReproducibilityEnvelope(
        run_id=_text(manifest, "run_id"),
        config_hash=_digest(_canonical(config)),
        code_version="sha256:" + _digest(code_bytes),
        input_snapshot_id="sha256:" + _digest(_canonical(artifacts)),
        generated_at=datetime.now(UTC).isoformat(),
        source_artifact_ids=tuple(row["id"] for _, row in sorted(artifacts.items())),
    )
    workspace = QuantWorkspaceContract(
        plan_id=plan_id,
        plan_period=period,
        as_of_date=as_of,
        module="metric_engine",
        scenarios=(
            QuantScenarioContract(
                scenario_id="metric-engine",
                scenario_label="Observed pilot metrics (producer only)",
                module="metric_engine",
                baseline_scenario_id=None,
                reproducibility=envelope,
                series=metric_engine.build_metric_series_contracts(observations),
                warnings=tuple(warnings),
            ),
        ),
    )
    payload = {
        **asdict(workspace),
        "input_artifacts": artifacts,
        "fact_lineage": lineage,
        "metric_lineage": [asdict(row) for row in observations],
        "producer_config": config,
    }
    validate_quant_workspace(payload)
    return payload


def validate_quant_workspace(payload: dict[str, Any]) -> None:
    """Reject missing reproducibility, empty/nonfinite output or broken lineage."""
    if payload.get("module") != "metric_engine" or not payload.get("scenarios"):
        raise ValueError("non-empty metric_engine workspace required")
    artifact_ids = {row["id"] for row in payload["input_artifacts"].values()}
    metrics = {}
    for metric in payload["metric_lineage"]:
        if not metric["source_fact_ids"] or any(
            fact_id not in payload["fact_lineage"] for fact_id in metric["source_fact_ids"]
        ):
            raise ValueError("metric source fact lineage is incomplete")
        for fact_id in metric["source_fact_ids"]:
            fact = payload["fact_lineage"][fact_id]
            if fact.get("artifact_id") not in artifact_ids:
                raise ValueError("fact lineage must resolve to a captured input artifact")
            try:
                _refs(fact.get("evidence_refs"))
            except ValueError as exc:
                raise ValueError("fact lineage requires page evidence") from exc
        metrics[(metric["plan_id"], metric["plan_period"], metric["metric_name"])] = metric
    for scenario in payload["scenarios"]:
        values = scenario.get("reproducibility", {})
        try:
            envelope = ReproducibilityEnvelope(**values)
        except TypeError as exc:
            raise ValueError("missing reproducibility fields") from exc
        require_reproducibility(
            envelope,
            requires_seed=False,
            expected_source_artifact_ids=tuple(sorted(artifact_ids)),
        )
        if envelope.config_hash != _digest(_canonical(payload["producer_config"])):
            raise ValueError("invalid reproducibility fields: config_hash mismatch")
        if envelope.input_snapshot_id != "sha256:" + _digest(
            _canonical(payload["input_artifacts"])
        ):
            raise ValueError("invalid reproducibility fields: input_snapshot_id mismatch")
        if not scenario.get("series"):
            raise ValueError("non-empty metric series required")
        for series in scenario["series"]:
            if not series.get("points"):
                raise ValueError("non-empty metric points required")
            for point in series["points"]:
                _number(point["y_value"])
                key = (payload["plan_id"], point["x_label"], series["metric_name"])
                if key not in metrics:
                    raise ValueError("published metric point is missing metric lineage")
                metric = metrics[key]
                if (
                    point["y_value"] != metric["value"]
                    or point.get("y_unit") != metric["unit"]
                    or point.get("confidence") != metric["confidence"]
                    or _canonical(point.get("provenance_refs"))
                    != _canonical(contracts.normalize_provenance_refs(metric["provenance_refs"]))
                ):
                    raise ValueError("published metric point differs from metric lineage")
    _canonical(payload)


def write_quant_workspace(path: Path, payload: dict[str, Any]) -> Path:
    """Validate completely before atomically replacing a previous valid output."""
    validate_quant_workspace(payload)
    serialized = json.dumps(payload, indent=2, sort_keys=True, allow_nan=False) + "\n"
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = None
    try:
        with tempfile.NamedTemporaryFile(
            mode="w", encoding="utf-8", dir=path.parent, delete=False
        ) as stream:
            temporary = Path(stream.name)
            stream.write(serialized)
        os.replace(temporary, path)
    finally:
        if temporary is not None:
            temporary.unlink(missing_ok=True)
    return path
