"""Tests for lazy exports on ``pension_data.db.models``."""

from __future__ import annotations

import pytest

import pension_data.db.models as models
import pension_data.quality.confidence  # noqa: F401 - breaks review_queue import cycle


def test_models_namespace_lazy_loads_review_queue_types() -> None:
    lazy_record = models.ExtractionReviewQueueRecord
    lazy_audit = models.ReviewQueueAuditEntry
    from pension_data.db.models import review_queue

    assert lazy_record is review_queue.ExtractionReviewQueueRecord
    assert lazy_audit is review_queue.ReviewQueueAuditEntry


def test_models_namespace_unknown_attribute_raises() -> None:
    with pytest.raises(AttributeError, match=r"has no attribute 'NotARealModel'"):
        _ = models.NotARealModel


def test_models_namespace_all_exports_resolve() -> None:
    unresolved: list[str] = []
    for name in models.__all__:
        try:
            obj = getattr(models, name)
        except AttributeError:
            unresolved.append(name)
            continue
        assert obj is not None, name
    assert unresolved == []
