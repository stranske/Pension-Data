# Anomaly Flood

Last reviewed: 2026-03-02
Incident class: `anomaly_flood`

## Symptoms

- Abrupt spike in anomaly events across multiple systems.
- Review queue backlog grows faster than triage throughput.
- Alert channels are saturated with repeated anomaly notifications.

## Diagnostic Commands

```bash
gh pr checks --watch
```

```bash
pytest -q tests/test_main.py tests/test_dependency_version_alignment.py
```

## Remediation Steps

1. Confirm incident scope by recording anomaly rate, queue depth, and first-seen timestamp from monitoring.
2. Sample recent anomaly events and split them into false positives vs. true anomalies by class.
3. If one class is noisy, apply a temporary threshold increase or suppression with an explicit expiry window.
4. Re-prioritize queue routing so high-severity cohorts remain visible while low-value noise is throttled.
5. Monitor 30-60 minutes for stabilization and rollback temporary controls if high-severity anomalies become hidden.
6. After rates normalize, remove temporary suppressions and document the tuning change that prevented recurrence.

## Expected Signals

- Queue depth trend flattens to normal operating range.
- Alert volume returns to expected baseline.
- High-severity anomaly rate remains visible and actionable.
