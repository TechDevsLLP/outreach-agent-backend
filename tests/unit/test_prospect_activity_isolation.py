"""Two-tenant regressions for shared-prospect outreach activity."""

from copy import deepcopy
from datetime import datetime
from types import SimpleNamespace

import pytest
from bson import ObjectId

import database
import routes.linkedin_outreach as linkedin_routes
from fastapi import HTTPException
from services.prospect_activity_state_service import record_prospect_activity


pytestmark = pytest.mark.unit


def _get(document, path):
    value = document
    for part in path.split("."):
        if not isinstance(value, dict):
            return None
        value = value.get(part)
    return value


def _matches(document, query):
    for key, expected in query.items():
        actual = _get(document, key)
        if isinstance(expected, dict) and "$in" in expected:
            if actual not in expected["$in"]:
                return False
        elif actual != expected:
            return False
    return True


def _set(document, path, value):
    target = document
    parts = path.split(".")
    for part in parts[:-1]:
        target = target.setdefault(part, {})
    target[parts[-1]] = deepcopy(value)


class _Collection:
    def __init__(self, docs=()):
        self.docs = [deepcopy(doc) for doc in docs]
        self.upserts = []

    async def find_one(self, query, projection=None, **kwargs):
        for document in self.docs:
            if _matches(document, query):
                return deepcopy(document)
        return None

    async def update_one(self, query, update, upsert=False):
        self.upserts.append(upsert)
        for document in self.docs:
            if not _matches(document, query):
                continue
            before = deepcopy(document)
            for key, value in update.get("$set", {}).items():
                _set(document, key, value)
            for key, value in update.get("$push", {}).items():
                current = _get(document, key)
                if current is None:
                    _set(document, key, [])
                    current = _get(document, key)
                current.append(deepcopy(value))
            return SimpleNamespace(matched_count=1, modified_count=int(document != before))
        return SimpleNamespace(matched_count=0, modified_count=0)

    async def update_many(self, query, update, upsert=False):
        modified = 0
        for document in self.docs:
            if not _matches(document, query):
                continue
            before = deepcopy(document)
            for key, value in update.get("$set", {}).items():
                _set(document, key, value)
            for key, value in update.get("$push", {}).items():
                current = _get(document, key)
                if current is None:
                    _set(document, key, [])
                    current = _get(document, key)
                current.append(deepcopy(value))
            modified += int(document != before)
        return SimpleNamespace(matched_count=modified, modified_count=modified)


async def test_same_shared_prospect_has_isolated_tenant_activity(monkeypatch):
    prospect_id = ObjectId()
    account_a, account_b = ObjectId(), ObjectId()
    states = _Collection([
        {"_id": ObjectId(), "account_id": str(account_a), "prospect_id": str(prospect_id)},
        {"_id": ObjectId(), "account_id": str(account_b), "prospect_id": str(prospect_id)},
    ])
    monkeypatch.setattr(database, "prospect_state_collection", states)
    monkeypatch.setattr(database, "campaign_enrollments_collection", _Collection())
    accepted_at = datetime(2026, 7, 15, 12, 0)

    assert await record_prospect_activity(
        account_id=account_a, prospect_id=prospect_id,
        fields={"connection_accepted_at": accepted_at},
        event={"event": "linkedin_connection_accepted", "timestamp": accepted_at},
    )

    state_a, state_b = states.docs
    assert state_a["connection_accepted_at"] == accepted_at
    assert len(state_a["outreach_history"]) == 1
    assert "connection_accepted_at" not in state_b
    assert states.upserts == [False]


async def test_arbitrary_prospect_id_cannot_create_activity_ownership(monkeypatch):
    states = _Collection()
    enrollments = _Collection()
    monkeypatch.setattr(database, "prospect_state_collection", states)
    monkeypatch.setattr(database, "campaign_enrollments_collection", enrollments)

    with pytest.raises(PermissionError, match="outside tenant scope"):
        await record_prospect_activity(
            account_id=ObjectId(), prospect_id=ObjectId(),
            fields={"connection_request_sent_at": datetime.utcnow()},
        )

    assert states.docs == [] and enrollments.docs == []
    assert states.upserts == [] and enrollments.upserts == []


async def test_manual_linkedin_lookup_rejects_other_tenant_before_shared_read(monkeypatch):
    prospect_id = ObjectId()
    account_a, account_b = ObjectId(), ObjectId()
    states = _Collection([
        {"_id": ObjectId(), "account_id": str(account_a), "prospect_id": str(prospect_id)},
    ])
    monkeypatch.setattr(database, "prospect_state_collection", states)
    monkeypatch.setattr(database, "campaign_enrollments_collection", _Collection())

    class _SharedReadMustNotRun:
        async def find_one(self, *args, **kwargs):
            raise AssertionError("unauthorized tenant must not read the shared person")

    monkeypatch.setattr(linkedin_routes, "prospects_collection", _SharedReadMustNotRun())
    with pytest.raises(HTTPException) as exc:
        await linkedin_routes._get_prospect(str(prospect_id), str(account_b))
    assert exc.value.status_code == 404


async def test_campaign_activity_updates_only_matching_tenant_enrollment(monkeypatch):
    prospect_id, campaign_id = ObjectId(), ObjectId()
    account_a, account_b = ObjectId(), ObjectId()
    states = _Collection([
        {"_id": ObjectId(), "account_id": str(account_a), "prospect_id": str(prospect_id)},
        {"_id": ObjectId(), "account_id": str(account_b), "prospect_id": str(prospect_id)},
    ])
    enrollments = _Collection([
        {"_id": ObjectId(), "account_id": str(account_a), "campaign_id": str(campaign_id), "prospect_id": str(prospect_id)},
        {"_id": ObjectId(), "account_id": str(account_b), "campaign_id": str(campaign_id), "prospect_id": str(prospect_id)},
    ])
    monkeypatch.setattr(database, "prospect_state_collection", states)
    monkeypatch.setattr(database, "campaign_enrollments_collection", enrollments)

    sent_at = datetime(2026, 7, 15, 13, 0)
    assert await record_prospect_activity(
        account_id=account_a, prospect_id=prospect_id, campaign_id=campaign_id,
        fields={"connection_followup_sent_at": sent_at},
        event={"event": "followup_sent", "timestamp": sent_at},
    )

    assert _get(enrollments.docs[0], "linkedin_activity.connection_followup_sent_at") == sent_at
    assert _get(enrollments.docs[1], "linkedin_activity.connection_followup_sent_at") is None
