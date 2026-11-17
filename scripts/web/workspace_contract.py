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

    workspace_bundle = resolved.get("workspaceBundle")
    review_fields = (
        frozenset(workspace_bundle.get("reviewContextRequiredFields", ()))
        if isinstance(workspace_bundle, Mapping)
        else frozenset()
    )
    allowed_tiers = (
        frozenset(workspace_bundle.get("sourceAuthorityTiers", ()))
        if isinstance(workspace_bundle, Mapping)
        else frozenset()
    )
    allowed_statuses = (
        frozenset(workspace_bundle.get("sourceAuthorityStatuses", ()))
        if isinstance(workspace_bundle, Mapping)
        else frozenset()
    )
    for dataset_index, dataset in enumerate(datasets):
        if not isinstance(dataset, Mapping):
            continue
        rows = dataset.get("rows")
        if not isinstance(rows, list):
            continue
        for row_index, row in enumerate(rows):
            if not isinstance(row, Mapping) or "review_context" not in row:
                continue
            review_context = row.get("review_context")
            if not isinstance(review_context, Mapping):
                raise WorkspaceContractError(
                    f"dataset {dataset_index} row {row_index} review_context must be an object "
                    f"in {path_label}"
                )
            if review_fields:
                missing_review = review_fields.difference(review_context)
                if missing_review:
                    raise WorkspaceContractError(
                        f"dataset {dataset_index} row {row_index} review_context missing fields "
                        f"{', '.join(sorted(missing_review))} in {path_label}"
                    )
            escalation = review_context.get("escalation_required")
            if escalation is not None and not isinstance(escalation, bool):
                raise WorkspaceContractError(
                    f"dataset {dataset_index} row {row_index} escalation_required must be "
                    f"boolean or null in {path_label}"
                )
            flags = review_context.get("actionable_flags")
            if not isinstance(flags, list) or any(
                not isinstance(flag, str) or not flag.strip() for flag in flags
            ):
                raise WorkspaceContractError(
                    f"dataset {dataset_index} row {row_index} actionable_flags must be "
                    f"a list of non-empty strings in {path_label}"
                )
            tier = review_context.get("source_authority_tier")
            if tier is not None and (
                not isinstance(tier, str) or (allowed_tiers and tier not in allowed_tiers)
            ):
                raise WorkspaceContractError(
                    f"dataset {dataset_index} row {row_index} source_authority_tier is invalid "
                    f"in {path_label}"
                )
            status = review_context.get("source_authority_status")
            expected_status = "declared" if tier is not None else "unknown"
            if (
                not isinstance(status, str)
                or (allowed_statuses and status not in allowed_statuses)
                or status != expected_status
            ):
                raise WorkspaceContractError(
                    f"dataset {dataset_index} row {row_index} source_authority_status is invalid "
                    f"in {path_label}"
                )
    return data_origin
