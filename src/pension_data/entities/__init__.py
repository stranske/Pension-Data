"""Entity lookup services for cross-plan exposure discovery."""

from pension_data.entities.lineage import (
    current_canonical_entity_id,
    historical_predecessors,
    record_lineage_event,
    successor_chain,
)
from pension_data.entities.lookup_service import (
    EntityExposureIndex,
    LookupExecutionTrace,
    build_entity_exposure_index,
    lookup_entity_exposures,
    resolve_canonical_entity_id,
)

__all__ = [
    "EntityExposureIndex",
    "LookupExecutionTrace",
    "build_entity_exposure_index",
    "current_canonical_entity_id",
    "historical_predecessors",
    "lookup_entity_exposures",
    "record_lineage_event",
    "resolve_canonical_entity_id",
    "successor_chain",
]
