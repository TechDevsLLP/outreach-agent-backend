"""Offline race and failure-boundary tests for campaign send safety."""

from __future__ import annotations

import asyncio
from copy import deepcopy
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest
from bson import ObjectId

from models.send_attempt import SendAttemptIdentity, SendAttemptState
from services import campaign_engine
from services import daily_cap_service
from services.send_attempt_service import SendAttemptService, claim_due_enrollment


def _matches(document, query):
    for key, expected in query.items():
        if key == "$or":
            if not any(_matches(document, clause) for clause in expected):
                return False
            continue
        actual = document.get(key)
        if isinstance(expected, dict):
            for operator, operand in expected.items():
                if operator == "$in" and actual not in operand:
                    return False
                if operator == "$lte" and not (actual is not None and actual <= operand):
                    return False
                if operator == "$exists" and (key in document) is not bool(operand):
                    return False
                if operator == "$nin" and actual in operand:
                    return False
        elif actual != expected:
            return False
    return True


def _apply(document, update):
    for key, value in update.get("$setOnInsert", {}).items():
        document.setdefault(key, deepcopy(value))
    for key, value in update.get("$set", {}).items():
        document[key] = deepcopy(value)
    for key, value in update.get("$inc", {}).items():
        document[key] = document.get(key, 0) + value
    for key in update.get("$unset", {}):
        document.pop(key, None)


class _AtomicCollection:
    def __init__(self, documents=None):
        self.documents = [deepcopy(doc) for doc in (documents or [])]
        self.lock = asyncio.Lock()

    async def find_one(self, query, projection=None):
        async with self.lock:
            for document in self.documents:
                if _matches(document, query):
                    return deepcopy(document)
        return None

    async def find_one_and_update(
        self, query, update, *, upsert=False, sort=None, return_document=None
    ):
        del return_document
        async with self.lock:
            matches = [doc for doc in self.documents if _matches(doc, query)]
            if sort:
                for field, direction in reversed(sort):
                    matches.sort(key=lambda doc: doc.get(field), reverse=direction < 0)
            if matches:
                document = matches[0]
                _apply(document, update)
                return deepcopy(document)
            if not upsert:
                return None
            document = {
                key: deepcopy(value)
                for key, value in query.items()
                if not key.startswith("$") and not isinstance(value, dict)
            }
            _apply(document, update)
            document.setdefault("_id", ObjectId())
            self.documents.append(document)
            return deepcopy(document)

    async def update_one(self, query, update, **kwargs):
        async with self.lock:
            for document in self.documents:
                if _matches(document, query):
                    before = deepcopy(document)
                    _apply(document, update)
                    return SimpleNamespace(
                        matched_count=1, modified_count=int(before != document)
                    )
        return SimpleNamespace(matched_count=0, modified_count=0)


def _identity() -> SendAttemptIdentity:
    return SendAttemptIdentity(
        account_id="tenant-a",
        enrollment_id="enrollment-1",
        sequence_version="7",
        node_id="email-node-2",
        generation=3,
    )


@pytest.mark.asyncio
async def test_two_workers_racing_one_due_enrollment_get_one_lease():
    now = datetime(2026, 7, 15, 10, 0)
    collection = _AtomicCollection([
        {
            "_id": ObjectId(),
            "status": "active",
            "next_action_at": now - timedelta(minutes=1),
        }
    ])

    first, second = await asyncio.gather(
        claim_due_enrollment(collection, worker_id="worker-a", now=now),
        claim_due_enrollment(collection, worker_id="worker-b", now=now),
    )

    winners = [value for value in (first, second) if value]
    assert len(winners) == 1
    assert winners[0]["execution_lease_owner"] in {"worker-a", "worker-b"}


@pytest.mark.asyncio
async def test_failure_before_provider_is_retryable_without_calling_provider():
    service = SendAttemptService(_AtomicCollection())
    provider_calls = 0

    async def preflight():
        raise ValueError("sender disconnected")

    async def provider(_):
        nonlocal provider_calls
        provider_calls += 1
        return {"message_id": "must-not-send"}

    attempt = await service.dispatch(
        identity=_identity(),
        channel="email",
        payload={"body": "hello"},
        worker_id="worker-a",
        before_provider=preflight,
        provider_call=provider,
    )

    assert attempt["state"] == SendAttemptState.RETRY_SCHEDULED.value
    assert attempt["failure_phase"] == "before_provider"
    assert provider_calls == 0


@pytest.mark.asyncio
async def test_failure_after_provider_boundary_is_ambiguous_and_never_blind_retried():
    service = SendAttemptService(_AtomicCollection())
    provider_calls = 0

    async def provider(_):
        nonlocal provider_calls
        provider_calls += 1
        raise TimeoutError("provider accepted request but response was lost")

    kwargs = dict(
        identity=_identity(),
        channel="email",
        payload={"body": "hello"},
        worker_id="worker-a",
        before_provider=lambda: {"sender": "mailbox-a"},
        provider_call=provider,
    )
    first = await service.dispatch(**kwargs)
    replay = await service.dispatch(**kwargs)

    assert first["state"] == SendAttemptState.AMBIGUOUS.value
    assert replay["state"] == SendAttemptState.AMBIGUOUS.value
    assert provider_calls == 1


@pytest.mark.asyncio
async def test_two_workers_same_send_key_cross_provider_boundary_once():
    service = SendAttemptService(_AtomicCollection())
    provider_calls = 0

    async def provider(_):
        nonlocal provider_calls
        provider_calls += 1
        await asyncio.sleep(0)
        return {"provider": "google", "message_id": "provider-message-1"}

    common = dict(
        identity=_identity(),
        channel="email",
        payload={"body": "frozen hello"},
        before_provider=lambda: {"sender": "mailbox-a"},
        provider_call=provider,
    )
    first, second = await asyncio.gather(
        service.dispatch(worker_id="worker-a", **common),
        service.dispatch(worker_id="worker-b", **common),
    )

    assert provider_calls == 1
    assert {first["state"], second["state"]} <= {
        SendAttemptState.DISPATCHING.value,
        SendAttemptState.SENT.value,
    }
    stored = await service.collection.find_one({"send_key": _identity().send_key})
    assert stored["state"] == SendAttemptState.SENT.value


@pytest.mark.asyncio
async def test_provider_reconciliation_sent_closes_attempt_without_resend():
    service = SendAttemptService(_AtomicCollection())
    provider_calls = 0

    async def uncertain_provider(_):
        nonlocal provider_calls
        provider_calls += 1
        raise TimeoutError("response lost")

    common = dict(
        identity=_identity(),
        channel="email",
        payload={"body": "hello"},
        worker_id="worker-a",
        before_provider=lambda: {"sender": "mailbox-a"},
        provider_call=uncertain_provider,
    )
    ambiguous = await service.dispatch(**common)
    reconciled = await service.reconcile(
        send_key=_identity().send_key,
        outcome="sent",
        provider_result={"provider": "google", "message_id": "confirmed-1"},
    )
    replay = await service.dispatch(**common)

    assert ambiguous["state"] == SendAttemptState.AMBIGUOUS.value
    assert reconciled["state"] == SendAttemptState.SENT.value
    assert replay["provider_result"]["message_id"] == "confirmed-1"
    assert provider_calls == 1


@pytest.mark.parametrize(
    "state,owns_cap",
    [
        (SendAttemptState.PREPARED.value, True),
        (SendAttemptState.DISPATCHING.value, True),
        (SendAttemptState.AMBIGUOUS.value, True),
        (SendAttemptState.SENT.value, True),
        (SendAttemptState.RETRY_SCHEDULED.value, False),
        (SendAttemptState.FAILED_TERMINAL.value, False),
    ],
)
def test_attempt_cap_ownership_matches_retry_boundary(state, owns_cap):
    assert campaign_engine._attempt_owns_daily_cap({"state": state}) is owns_cap


@pytest.mark.asyncio
async def test_not_sent_reconciliation_releases_caps_exactly_once(monkeypatch):
    enrollment_id = ObjectId()
    account_id = ObjectId()
    campaign_id = ObjectId()
    identity = SendAttemptIdentity(
        account_id=str(account_id),
        enrollment_id=str(enrollment_id),
        sequence_version="1",
        node_id="email-1",
        generation=0,
    )
    attempts = _AtomicCollection([
        {
            "send_key": identity.send_key,
            **identity.model_dump(),
            "state": SendAttemptState.AMBIGUOUS.value,
            "channel": "email",
            "sender_record_id": "mailbox-1",
            "payload": {"campaign_id": str(campaign_id), "channel": "email"},
        }
    ])
    enrollment_updates = SimpleNamespace(update_one=AsyncMock())
    release_campaign = AsyncMock()
    release_sender = AsyncMock()
    monkeypatch.setattr(campaign_engine.database, "send_attempts_collection", attempts)
    monkeypatch.setattr(
        campaign_engine.database, "campaign_enrollments_collection", enrollment_updates
    )
    monkeypatch.setattr(campaign_engine.database, "db", SimpleNamespace())
    monkeypatch.setattr(daily_cap_service, "release_slot", release_campaign)
    monkeypatch.setattr(daily_cap_service, "release_sender_slot", release_sender)

    first = await campaign_engine.reconcile_ambiguous_send(
        identity.send_key, "not_sent"
    )
    replay = await campaign_engine.reconcile_ambiguous_send(
        identity.send_key, "not_sent"
    )

    assert first["state"] == SendAttemptState.RETRY_SCHEDULED.value
    assert replay is None
    release_campaign.assert_awaited_once_with(
        campaign_engine.database.db, str(campaign_id), "email"
    )
    release_sender.assert_awaited_once_with(
        campaign_engine.database.db, "mailbox-1", "email"
    )


@pytest.mark.asyncio
async def test_tenant_sender_lookup_rejects_other_tenants_sender(monkeypatch):
    tenant_a, tenant_b = str(ObjectId()), str(ObjectId())
    sender_id = ObjectId()

    class _LinkedAccounts:
        async def find_one(self, query):
            assert tenant_a in {str(value) for value in query["account_id"]["$in"]}
            assert tenant_b not in {str(value) for value in query["account_id"]["$in"]}
            # The only existing sender belongs to B, so Mongo would not match.
            return None

    monkeypatch.setattr(
        campaign_engine.database, "linkedin_accounts_collection", _LinkedAccounts()
    )
    result = await campaign_engine._get_linkedin_account_for_campaign(
        {"linkedin_account_id": str(sender_id)},
        {"account_id": tenant_a},
    )
    assert result is None
