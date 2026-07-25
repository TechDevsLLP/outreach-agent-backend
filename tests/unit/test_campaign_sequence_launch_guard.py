"""Offline tests for the canonical sequence launch invariant."""

import pytest
from bson import ObjectId

from services.campaign_launch_service import (
    SEQUENCE_GRAPH_CONTRACT,
    SequenceLaunchValidationError,
    approve_day,
    ensure_sequence_ready_for_launch,
    run_approve_and_launch,
)
from services.sequence_service import build_default_sequence_graph

pytestmark = pytest.mark.unit

_ACCOUNT_ID = "0123456789abcdef01234567"
def canonical_campaign(**overrides) -> dict:
    campaign = {
        "_id": ObjectId(),
        "is_smart_campaign": True,
        "status": "draft",
        "sequence_contract": SEQUENCE_GRAPH_CONTRACT,
    }
    campaign.update(overrides)
    return campaign


def test_valid_persisted_sequence_passes_launch_guard():
    campaign = canonical_campaign(sequence_graph=build_default_sequence_graph())
    ensure_sequence_ready_for_launch(campaign)


def test_declared_sequence_without_persisted_graph_is_rejected():
    with pytest.raises(SequenceLaunchValidationError, match="not saved") as exc:
        ensure_sequence_ready_for_launch(canonical_campaign())
    assert exc.value.status_code == 409


def test_invalid_persisted_sequence_is_rejected():
    campaign = canonical_campaign(
        sequence_graph={
            "version": 1,
            "nodes": [{"id": "n1", "type": "touch", "channel": "fax"}],
            "edges": [],
            "settings": {"max_touches": 8, "stop_on_reply": True, "default_gap_days": 2},
        }
    )
    with pytest.raises(SequenceLaunchValidationError, match="invalid"):
        ensure_sequence_ready_for_launch(campaign)


def test_legacy_campaign_without_sequence_declaration_is_unchanged():
    ensure_sequence_ready_for_launch(
        {"_id": ObjectId(), "is_smart_campaign": True, "follow_up_flow": {"nodes": []}}
    )


async def test_approve_day_rejects_missing_graph_before_database_work():
    with pytest.raises(SequenceLaunchValidationError, match="not saved"):
        await approve_day(canonical_campaign(), 1)


async def test_legacy_auto_launch_rejects_missing_graph_before_database_work():
    with pytest.raises(SequenceLaunchValidationError, match="not saved"):
        await run_approve_and_launch(canonical_campaign(), ObjectId(_ACCOUNT_ID))
