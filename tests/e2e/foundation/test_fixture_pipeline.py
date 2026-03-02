"""Foundation fixture e2e pipeline tests."""

from __future__ import annotations

import json
from pathlib import Path

from pension_data.ops.foundation import run_foundation_fixture_pipeline

FIXTURE_DIR = Path(__file__).parent / "fixtures"


def _load_fixture_payload(name: str) -> dict[str, object]:
    return json.loads((FIXTURE_DIR / name).read_text(encoding="utf-8"))


def test_fixture_pipeline_runs_registry_to_coverage_and_writes_artifacts(tmp_path: Path) -> None:
    ledger, artifact_paths = run_foundation_fixture_pipeline(
        fixture_path=FIXTURE_DIR / "foundation_fixture_success.json",
        output_root=tmp_path,
        run_id="fixture-success-run",
        started_at="2026-03-02T00:00:00+00:00",
        completed_at="2026-03-02T00:00:30+00:00",
    )

    assert ledger.status == "success"
    assert [item.stage for item in ledger.stage_metrics] == [
        "registry",
        "source_map",
        "discovery",
        "ingestion",
        "coverage",
    ]
    assert all(item.status == "ok" for item in ledger.stage_metrics)
    assert ledger.failures == ()

    for key in (
        "discovery_rows_json",
        "ingestion_summary_json",
        "coverage_readiness_json",
        "latest_run_ledger_json",
        "run_ledger_jsonl",
    ):
        path = Path(artifact_paths[key])
        assert path.exists(), f"missing artifact file for key: {key}"

    coverage_payload = json.loads(
        Path(artifact_paths["coverage_readiness_json"]).read_text(encoding="utf-8")
    )
    assert len(coverage_payload["readiness_rows"]) == 3
    assert coverage_payload["summary_by_cohort"][0]["cohort"] == "state"


def test_fixture_pipeline_classifies_robots_restriction_and_skips_downstream_stages(
    tmp_path: Path,
) -> None:
    ledger, artifact_paths = run_foundation_fixture_pipeline(
        fixture_path=FIXTURE_DIR / "foundation_fixture_robots_failure.json",
        output_root=tmp_path,
        run_id="fixture-robots-failure",
        started_at="2026-03-02T00:00:00+00:00",
        completed_at="2026-03-02T00:00:10+00:00",
    )

    assert ledger.status == "failed"
    assert len(ledger.failures) == 1
    failure = ledger.failures[0]
    assert failure.stage == "discovery"
    assert failure.category == "robots_restriction"

    stage_status = {item.stage: item.status for item in ledger.stage_metrics}
    assert stage_status["registry"] == "ok"
    assert stage_status["source_map"] == "ok"
    assert stage_status["discovery"] == "error"
    assert stage_status["ingestion"] == "skipped"
    assert stage_status["coverage"] == "skipped"

    latest_ledger = json.loads(
        Path(artifact_paths["latest_run_ledger_json"]).read_text(encoding="utf-8")
    )
    assert latest_ledger["status"] == "failed"
    assert latest_ledger["failures"][0]["category"] == "robots_restriction"


def test_fixture_pipeline_domain_validation_allows_source_urls_with_port(tmp_path: Path) -> None:
    payload = _load_fixture_payload("foundation_fixture_success.json")
    discovered_rows = payload["discovered_records"]
    assert isinstance(discovered_rows, list)
    ingestion_rows = payload["ingestion_items"]
    assert isinstance(ingestion_rows, list)

    replacements: dict[str, str] = {}
    for row in discovered_rows:
        assert isinstance(row, dict)
        source_url = row.get("source_url")
        assert isinstance(source_url, str)
        if source_url.startswith("https://reports.ca.gov/"):
            updated = source_url.replace("https://reports.ca.gov/", "https://reports.ca.gov:443/")
            row["source_url"] = updated
            replacements[source_url] = updated

    for row in ingestion_rows:
        assert isinstance(row, dict)
        source_url = row.get("source_url")
        if isinstance(source_url, str) and source_url in replacements:
            row["source_url"] = replacements[source_url]
        revised = row.get("revised_of_source_url")
        if isinstance(revised, str) and revised in replacements:
            row["revised_of_source_url"] = replacements[revised]

    fixture_path = tmp_path / "fixture-with-port.json"
    fixture_path.write_text(json.dumps(payload, indent=2, sort_keys=True), encoding="utf-8")
    (tmp_path / "registry_seed.csv").write_text(
        (FIXTURE_DIR / "registry_seed.csv").read_text(encoding="utf-8"),
        encoding="utf-8",
    )
    (tmp_path / "source_map.csv").write_text(
        (FIXTURE_DIR / "source_map.csv").read_text(encoding="utf-8"),
        encoding="utf-8",
    )

    ledger, _artifact_paths = run_foundation_fixture_pipeline(
        fixture_path=fixture_path,
        output_root=tmp_path,
        run_id="fixture-port-host",
        started_at="2026-03-02T00:00:00+00:00",
        completed_at="2026-03-02T00:00:30+00:00",
    )
    assert ledger.status == "success"


def test_fixture_pipeline_fails_when_source_map_plan_id_is_not_in_registry(tmp_path: Path) -> None:
    source_map_with_unknown_plan = (FIXTURE_DIR / "source_map.csv").read_text(
        encoding="utf-8"
    ) + "ZZ-UNKNOWN,Unknown Plan,ZZ-UNKNOWN,https://unknown.example.org,reports.ca.gov,annual_report,single_page,5,1,official,,,,\n"
    source_map_path = tmp_path / "source_map_with_unknown.csv"
    source_map_path.write_text(source_map_with_unknown_plan, encoding="utf-8")

    fixture_payload = _load_fixture_payload("foundation_fixture_success.json")
    fixture_payload["source_map_seed"] = source_map_path.name
    fixture_path = tmp_path / "fixture-invalid-source-map.json"
    fixture_path.write_text(json.dumps(fixture_payload, indent=2, sort_keys=True), encoding="utf-8")

    # Registry seed is referenced relatively by fixture path, so copy it next to the temp fixture.
    (tmp_path / "registry_seed.csv").write_text(
        (FIXTURE_DIR / "registry_seed.csv").read_text(encoding="utf-8"),
        encoding="utf-8",
    )

    ledger, _artifact_paths = run_foundation_fixture_pipeline(
        fixture_path=fixture_path,
        output_root=tmp_path,
        run_id="fixture-registry-mismatch",
        started_at="2026-03-02T00:00:00+00:00",
        completed_at="2026-03-02T00:00:10+00:00",
    )
    assert ledger.status == "failed"
    assert len(ledger.failures) == 1
    assert ledger.failures[0].stage == "source_map"
    assert ledger.failures[0].category == "source_map_breakage"
    assert "not present in registry seed" in ledger.failures[0].message
