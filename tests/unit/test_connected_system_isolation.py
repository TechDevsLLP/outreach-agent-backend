"""Offline tests for tenant/provider propagation across connected systems."""

from copy import deepcopy
from types import SimpleNamespace

import pytest
from bson import ObjectId

from services import ai_reply_service, webhook_service


def _account_matches(stored, expected):
    if isinstance(expected, dict) and "$in" in expected:
        return stored in expected["$in"] or str(stored) in {str(v) for v in expected["$in"]}
    return str(stored) == str(expected)


class _LinkedAccounts:
    def __init__(self, documents):
        self.documents = documents
        self.calls = 0

    async def find_one(self, query, projection=None):
        self.calls += 1
        for doc in self.documents:
            if doc.get("unipile_account_id") != query.get("unipile_account_id"):
                continue
            return deepcopy(doc)
        return None


class _Conversations:
    def __init__(self, documents):
        self.documents = documents
        self.calls = []

    async def find_one(self, query, projection=None):
        self.calls.append(deepcopy(query))
        for doc in self.documents:
            if not _account_matches(doc.get("account_id"), query.get("account_id")):
                continue
            if doc.get("channel") != query.get("channel"):
                continue
            if doc.get("provider_account_id") != query.get("provider_account_id"):
                continue
            if doc.get("provider_thread_id") != query.get("provider_thread_id"):
                continue
            return deepcopy(doc)
        return None

    async def update_one(self, query, update, **kwargs):
        self.calls.append(deepcopy(query))
        return SimpleNamespace(modified_count=1)


class _NoopCollection:
    async def find_one(self, *args, **kwargs):
        return None

    async def update_one(self, *args, **kwargs):
        return SimpleNamespace(modified_count=1)


@pytest.mark.asyncio
async def test_signed_provider_identity_selects_only_its_tenant_conversation(monkeypatch):
    tenant_a, tenant_b = str(ObjectId()), str(ObjectId())
    conversation_a, conversation_b = ObjectId(), ObjectId()
    monkeypatch.setattr(
        webhook_service,
        "linkedin_accounts_collection",
        _LinkedAccounts([
            {"account_id": tenant_a, "unipile_account_id": "provider-a"},
            {"account_id": tenant_b, "unipile_account_id": "provider-b"},
        ]),
    )
    monkeypatch.setattr(
        webhook_service,
        "conversations_collection",
        _Conversations([
            {
                "_id": conversation_a,
                "account_id": tenant_a,
                "channel": "linkedin",
                "provider_account_id": "provider-a",
                "provider_thread_id": "same-chat",
                "prospect_id": str(ObjectId()),
                "prospect_name": "Tenant A prospect",
            },
            {
                "_id": conversation_b,
                "account_id": tenant_b,
                "channel": "linkedin",
                "provider_account_id": "provider-b",
                "provider_thread_id": "same-chat",
                "prospect_id": str(ObjectId()),
                "prospect_name": "Tenant B prospect",
            },
        ]),
    )
    monkeypatch.setattr(webhook_service, "campaign_enrollments_collection", _NoopCollection())
    monkeypatch.setattr(webhook_service, "prospects_collection", _NoopCollection())
    appended = []

    async def _append(conversation_id, message, account_id, provider_account_id):
        appended.append((conversation_id, account_id, provider_account_id))
        return {"_id": conversation_id}

    async def _noop(*args, **kwargs):
        return None

    monkeypatch.setattr(webhook_service, "add_message_to_conversation", _append)
    import services.notification_service as notifications
    import services.sequence_service as sequence
    monkeypatch.setattr(notifications, "create_notification", _noop)
    monkeypatch.setattr(sequence, "mark_sequence_replied", _noop)

    for provider in ("provider-a", "provider-b"):
        result = await webhook_service.process_unipile_webhook({
            "event": "message_received",
            "account_id": provider,
            "chat_id": "same-chat",
            "message": {"id": f"message-{provider}", "text": "hello"},
        })
        assert result["status"] == "processed"

    assert appended == [
        (str(conversation_a), tenant_a, "provider-a"),
        (str(conversation_b), tenant_b, "provider-b"),
    ]


@pytest.mark.asyncio
async def test_webhook_missing_or_conflicting_provider_identity_fails_closed(monkeypatch):
    linked = _LinkedAccounts([])
    monkeypatch.setattr(webhook_service, "linkedin_accounts_collection", linked)

    missing = await webhook_service.process_unipile_webhook({
        "event": "message_received",
        "chat_id": "chat-1",
        "message": {"id": "message-1", "text": "hello"},
    })
    conflicting = await webhook_service.process_unipile_webhook({
        "event": "message_received",
        "account_id": "provider-a",
        "account": {"id": "provider-b"},
        "chat_id": "chat-1",
        "message": {"id": "message-1", "text": "hello"},
    })

    assert missing == {"status": "ignored", "reason": "invalid_provider_account"}
    assert conflicting == {"status": "ignored", "reason": "invalid_provider_account"}
    assert linked.calls == 0


@pytest.mark.asyncio
async def test_ai_draft_lookup_rejects_missing_tenant_before_database(monkeypatch):
    class _NeverCalled:
        async def find_one(self, *args, **kwargs):  # pragma: no cover
            raise AssertionError("database must not be queried")

    monkeypatch.setattr(ai_reply_service, "conversations_collection", _NeverCalled())
    with pytest.raises(ValueError, match="account_id is required"):
        await ai_reply_service.regenerate_reply_draft(str(ObjectId()), "")


