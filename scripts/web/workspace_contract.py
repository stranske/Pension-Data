#!/usr/bin/env python3
"""Single source of truth for web workspace bundle-contract validation (#639).

`smoke_test.py`, `serve_local.py`, and `build_workspace_bundle.py` previously each
validated the workspace bundle differently — most importantly `serve_local` did NOT
check `contractVersion`, so it could serve a bundle that `smoke_test` rejects. This
module owns the shared invariants (contractVersion matches the runtime contract; a
non-empty dataset inventory; `data_origin` within the caller's accepted set). Each
caller supplies its own legitimate accepted-origins policy — serve rejects fixture
by default, smoke allows it — but they now agree on every shared rule.
"""

from __future__ import annotations

import json
from collections.abc import Iterable, Mapping
from pathlib import Path
from typing import Any

CONTRACT_PATH = Path("apps/contracts/runtime-contract.json")


class WorkspaceContractError(ValueError):
    """Raised when a workspace bundle violates the runtime contract."""


def load_runtime_contract(contract_path: Path = CONTRACT_PATH) -> dict[str, Any]:
    """Load and structurally validate the runtime contract JSON."""
    if not contract_path.exists():
        raise WorkspaceContractError(f"runtime contract missing: {contract_path}")
    payload = json.loads(contract_path.read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise WorkspaceContractError(f"runtime contract must be a JSON object: {contract_path}")
    version = payload.get("version")
    workspace_bundle = payload.get("workspaceBundle")
    if not isinstance(version, str) or not version.strip():
        raise WorkspaceContractError(f"runtime contract missing version: {contract_path}")
    if not isinstance(workspace_bundle, dict):
        raise WorkspaceContractError(
            f"runtime contract workspaceBundle must be an object: {contract_path}"
        )
    required_fields = workspace_bundle.get("requiredTopLevelFields")
    if not isinstance(required_fields, list) or not required_fields:
        raise WorkspaceContractError(
            "runtime contract requiredTopLevelFields must be a non-empty list"
        )
    normalized_required = {
        field for field in required_fields if isinstance(field, str) and field.strip()
    }
    if len(normalized_required) != len(required_fields):
        raise WorkspaceContractError(
            "runtime contract requiredTopLevelFields must contain non-empty strings"
        )
    missing_fields = {"contractVersion", "data_origin", "datasets"}.difference(normalized_required)
    if missing_fields:
        raise WorkspaceContractError(
            "runtime contract missing required workspace fields: "
            + ", ".join(sorted(missing_fields))
        )
    return payload


def allowed_data_origins(contract: Mapping[str, Any]) -> frozenset[str]:
    """Return the full set of data origins the runtime contract permits."""
    workspace_bundle = contract.get("workspaceBundle")
    if not isinstance(workspace_bundle, dict):
        raise WorkspaceContractError("runtime contract workspaceBundle must be an object")
    origins = workspace_bundle.get("dataOrigins")
    if not isinstance(origins, list) or not origins:
        raise WorkspaceContractError("runtime contract dataOrigins must be a non-empty list")
    normalized = [origin for origin in origins if isinstance(origin, str) and origin.strip()]
    if len(normalized) != len(origins):
        raise WorkspaceContractError("runtime contract dataOrigins must be non-empty strings")
    return frozenset(normalized)


def validate_workspace_bundle(
    payload: Mapping[str, Any],
    *,
    path_label: str,
    accepted_origins: Iterable[str],
    contract: Mapping[str, Any] | None = None,
) -> str:
    """Validate a bundle against the shared contract invariants; return its data_origin.

    ``accepted_origins`` is the caller's policy (a subset of the contract origins);
    the ``contractVersion`` match and non-empty ``datasets`` checks are shared by all.
    """
    resolved = dict(contract) if contract is not None else load_runtime_contract()
    accepted = frozenset(accepted_origins)

    contract_version = payload.get("contractVersion")
    if not isinstance(contract_version, str) or not contract_version.strip():
        raise WorkspaceContractError(f"workspace bundle missing contractVersion in {path_label}")
    if contract_version != resolved.get("version"):
        raise WorkspaceContractError(
            f"workspace contractVersion '{contract_version}' does not match runtime contract "
            f"in {path_label}"
        )

    data_origin = payload.get("data_origin")
    if not isinstance(data_origin, str) or data_origin not in accepted:
        raise WorkspaceContractError(
            f"workspace bundle requires data_origin of {', '.join(sorted(accepted))} in {path_label}"
        )

    datasets = payload.get("datasets")
    if not isinstance(datasets, list) or not datasets:
        raise WorkspaceContractError(f"workspace dataset inventory is empty in {path_label}")
    return data_origin
