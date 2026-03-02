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
pytest -q -k "anomaly or sla or quality"
```

## Remediation Steps

1. Confirm whether anomaly spike aligns with known data publication dates.
2. Sample events and classify false positives versus legitimate anomalies.
3. Tighten thresholds or temporarily suppress noisy anomaly class.
4. Prioritize high-impact cohorts and re-balance review queue routing.
5. Backfill triage notes and revert temporary suppressions after stabilization.

## Expected Signals

- Queue depth trend flattens to normal operating range.
- Alert volume returns to expected baseline.
- High-severity anomaly rate remains visible and actionable.

