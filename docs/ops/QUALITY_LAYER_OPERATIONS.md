# Quality Layer Operations Guide

This guide documents the operations-and-quality layer for Pension Data and maps each
acceptance criterion to the existing implementation surface.

## Cadence-Aware Scheduling

- Publication signal extraction and cadence profiling:
  - `src/pension_data/scheduling/cadence.py`
- Adaptive refresh planning with confidence-aware windows:
  - `src/pension_data/scheduling/planner.py`

Operator checks:

```bash
pytest -q tests/scheduling/test_cadence_planner.py
```

## SLA Metrics And Telemetry

- Deterministic SLA catalog and quality metric computation:
  - `src/pension_data/quality/sla_metrics.py`
- Stage-scoped telemetry emission and baseline artifacts:
  - `src/pension_data/monitoring/telemetry.py`

Operator checks:

```bash
pytest -q tests/quality/test_sla_metrics.py
```

## Anomaly Detection And Review Routing

- Rule-based funded/allocation anomaly detection:
  - `src/pension_data/quality/anomaly_rules.py`
- Prioritized review queue routing with evidence context:
  - `src/pension_data/review_queue/anomalies.py`

Policy note:
- Publication must not be blocked solely by review routing.
- The readiness path already degrades non-blockingly when anomaly routing fails.

Operator checks:

```bash
pytest -q tests/quality/test_anomaly_rules.py tests/coverage/test_readiness_outputs.py
```

## Replay Regression Harness And CI Gate

- Golden-corpus replay harness and deterministic snapshots:
  - `tools/replay/harness.py`
  - `tools/replay/runner.py`
  - `tools/replay/diff_runner.py`
- CI replay regression gate:
  - `tools/ci_quality/replay_gate.py`

Operator checks:

```bash
pytest -q tests/replay/test_harness.py tests/replay/test_runner.py tests/replay/test_diff_runner.py tests/ci_quality/test_replay_gate.py
```

## Incident Runbooks

Top failure/anomaly classes are documented in:

- `docs/ops/INCIDENT_CLASSES.md`
- `docs/runbooks/PIPELINE_RUNBOOK_LINKS.md`
- `docs/runbooks/source-map-breakage.md`
- `docs/runbooks/revised-file-mismatch.md`
- `docs/runbooks/parser-fallback-exhaustion.md`
- `docs/runbooks/anomaly-flood.md`

Runbook documentation checks:

```bash
pytest -q tests/docs/test_runbook_presence.py tests/test_incident_runbook_links.py
```
