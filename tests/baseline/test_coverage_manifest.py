"""Coverage manifest -- which flat metric keys are exercised; emit a report.

Uses the generic ``baseline_kit.CoverageManifest``. The app supplies the input
space (every flat metric key the surfaces produce across all scenarios) and the
touched set (metric keys referenced by at least one directional check).
"""

from __future__ import annotations

import os
from pathlib import Path

from baseline_kit import CoverageManifest, load_catalog

from . import adapter
from .conftest import CATALOG_PATH, REPO_ROOT

REPORT_PATH = REPO_ROOT / "docs" / "reports" / "baseline-coverage.md"


def _all_metric_keys(catalog) -> set[str]:
    base = catalog["base"]
    keys: set[str] = set()
    for scenario in catalog["scenarios"]:
        keys.update(adapter.run_scenario(scenario, base).keys())
    return keys


def _build_manifest() -> CoverageManifest:
    catalog = load_catalog(CATALOG_PATH)
    touched = {d["metric"] for d in catalog.get("directionals", [])}
    return CoverageManifest(
        all_keys=_all_metric_keys(catalog),
        touched_keys=touched,
        priority_params=list(catalog.get("priority_metrics", [])),
        title="Pension-Data baseline coverage manifest",
    )


def test_directional_metrics_exist():
    m = _build_manifest()
    assert not m.unknown_catalog_keys, (
        "Directional checks reference metric keys the adapter never produces: "
        f"{sorted(m.unknown_catalog_keys)}"
    )


def test_priority_metrics_covered():
    m = _build_manifest()
    assert not m.priority_gaps, "Priority metrics with no directional check: " + ", ".join(
        m.priority_gaps
    )


def test_emit_coverage_report(tmp_path: Path):
    m = _build_manifest()
    report_path = (
        REPORT_PATH
        if os.environ.get("BASELINE_REFRESH_REPORT") == "1"
        else tmp_path / "baseline-coverage.md"
    )
    report_path.parent.mkdir(parents=True, exist_ok=True)
    report_path.write_text(m.to_markdown())
    assert report_path.exists()
