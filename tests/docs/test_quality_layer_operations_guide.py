"""Guards to keep quality-layer operations guide references actionable."""

from __future__ import annotations

from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
GUIDE_PATH = ROOT / "docs" / "ops" / "QUALITY_LAYER_OPERATIONS.md"

EXPECTED_REFERENCES: tuple[str, ...] = (
    "src/pension_data/scheduling/cadence.py",
    "src/pension_data/scheduling/planner.py",
    "src/pension_data/quality/sla_metrics.py",
    "src/pension_data/monitoring/telemetry.py",
    "src/pension_data/quality/anomaly_rules.py",
    "src/pension_data/quality/parser_output_validation.py",
    "src/pension_data/review_queue/anomalies.py",
    "tools/replay/harness.py",
    "tools/replay/runner.py",
    "tools/replay/diff_runner.py",
    "tools/ci_quality/replay_gate.py",
    "docs/ops/INCIDENT_CLASSES.md",
    "docs/runbooks/PIPELINE_RUNBOOK_LINKS.md",
    "docs/runbooks/source-map-breakage.md",
    "docs/runbooks/revised-file-mismatch.md",
    "docs/runbooks/parser-fallback-exhaustion.md",
    "docs/runbooks/parser-output-validation-failure.md",
    "docs/runbooks/parser-low-confidence-output.md",
    "docs/runbooks/anomaly-flood.md",
)


def _read(path: Path) -> str:
    return path.read_text(encoding="utf-8")


def test_quality_layer_operations_guide_references_existing_repo_paths() -> None:
    assert GUIDE_PATH.exists(), "missing quality-layer operations guide"
    text = _read(GUIDE_PATH)
    for reference in EXPECTED_REFERENCES:
        assert f"`{reference}`" in text, f"guide missing reference: {reference}"
        assert (ROOT / reference).exists(), f"guide references missing path: {reference}"
