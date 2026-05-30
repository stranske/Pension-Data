## 2026-05-30T18:00Z - opener selected issue #484

- Automation: `pd-workloop-resume` / `codex` opener lane from the neutral Code workspace. Selected repo `stranske/Pension-Data`.
- Source issue: #484, `Assemble Pension-Data's internal HTTP serving layer and isolate the LLM boundary`.
- Branch: `codex/issue-484-serving-layer`.
- Cap/drain preflight: raw opener cap below 5. Repaired trip-planner #1266 by adding `agent:retry` and dispatching Gate Followups; post-repair cap-health shows #1266 draining with active Gate/Gate Followups evidence. Trend_Model_Project #5353 remains scoped-blocked on product/scope decision; no opener quick-recovery attempted.
- Liveness disposition before selection: Manager-Database #1088 and Pension-Data #478 are merged/reopened for verifier sequencing; Inv-Man-Intake #469/#470 and learning-management-system #180 are scoped-blocked; Trend_Model_Project #5343/#5344 are in the #5353 blocker cone; trip-planner #1260 is linked to open PR #1266.
- Implementation plan: add `pension_data.api.app` FastAPI app factory, deterministic saved-view/metric-history routes, static `apps/web` mounting, proprietary-zone LLM disable gate, `pension-data-serve` console script, docs, and focused TestClient coverage.
- Validation so far: `python -m pytest tests/api/test_app_serving.py tests/api/test_saved_views_route.py -q` -> 12 passed; `python -m ruff check src/pension_data/api/app.py tests/api/test_app_serving.py` -> passed; `python -m mypy src/pension_data/api/app.py` -> passed. Live smoke with `PYTHONPATH=src PENSION_DATA_DATA_ZONE=proprietary PENSION_DATA_PORT=8766 python -m pension_data.api.app`: `/health` 200, `/index.html` 200, `/data/workspace.json` reports `fixture` and `core_facts`.
