"""Pure offline regression tests for tenant/provider conversation isolation."""

from copy import deepcopy
from datetime import datetime
from types import SimpleNamespace

import pytest
from bson import ObjectId

from models.conversation import Message
from services import conversation_service, reply_ingest


def _values(document, dotted_key):
    values = [document]
    for part in dotted_key.split("."):
        next_values = []
        for value in values:
            if isinstance(value, list):
                for item in value:
                    if isinstance(item, dict) and part in item:
                        next_values.append(item[part])
            elif isinstance(value, dict) and part in value:
                next_values.append(value[part])
        values = next_values
    return values


def _matches(document, query):
    for key, expected in query.items():
        actual_values = _values(document, key)
        if isinstance(expected, dict):
            if "$in" in expected:
                if not any(value in expected["$in"] for value in actual_values):
                    # Mongo treats a missing field as null for {$in: [null]}.
                    if actual_values or None not in expected["$in"]:
                        return False
            elif "$ne" in expected:
                if any(value == expected["$ne"] for value in actual_values):
                    return False
            else:  # pragma: no cover - guards accidental unsupported test queries
                raise AssertionError(f"Unsupported query operator: {expected}")
        elif expected not in actual_values:
            return False
    return True


class _MemoryCollection:
    def __init__(self, documents=None):
        self.documents = [deepcopy(doc) for doc in (documents or [])]
        self.calls = []

    async def find_one(self, query, projection=None):
        self.calls.append(("find_one", deepcopy(query)))
        for document in self.documents:
            if _matches(document, query):
                return deepcopy(document)
        return None

    async def insert_one(self, document):
        stored = deepcopy(document)
        stored.setdefault("_id", ObjectId())
        self.documents.append(stored)
        self.calls.append(("insert_one", deepcopy(stored)))
        return SimpleNamespace(inserted_id=stored["_id"])

    async def update_one(self, query, update, **kwargs):
        self.calls.append(("update_one", deepcopy(query)))
        for document in self.documents:
            if not _matches(document, query):
                continue
            before = deepcopy(document)
            for key, value in update.get("$set", {}).items():
                document[key] = deepcopy(value)
            for key, value in update.get("$addToSet", {}).items():
                target = document.setdefault(key, [])
                if value not in target:
                    target.append(deepcopy(value))
            return SimpleNamespace(
                matched_count=1,
                modified_count=int(document != before),
            )
        return SimpleNamespace(matched_count=0, modified_count=0)

    async def find_one_and_update(self, query, update, **kwargs):
        self.calls.append(("find_one_and_update", deepcopy(query)))
        for document in self.documents:
            if not _matches(document, query):
                continue
            for key, value in update.get("$push", {}).items():
                document.setdefault(key, []).append(deepcopy(value))
            for key, value in update.get("$set", {}).items():
                document[key] = deepcopy(value)
            for key, value in update.get("$inc", {}).items():
                document[key] = document.get(key, 0) + value
            return deepcopy(document)
        return None


@pytest.mark.asyncio
async def test_same_shared_prospect_and_chat_are_isolated_by_tenant_and_provider(monkeypatch):
    collection = _MemoryCollection()
    monkeypatch.setattr(conversation_service, "conversations_collection", collection)

    shared_prospect = str(ObjectId())
    tenant_a = str(ObjectId())
    tenant_b = str(ObjectId())

    conversation_a = await conversation_service.get_or_create_conversation(
        prospect_id=shared_prospect,
        channel="linkedin",
        unipile_chat_id="same-provider-chat-id",
        account_id=tenant_a,
        provider_account_id="linkedin-sender-a",
    )
    conversation_b = await conversation_service.get_or_create_conversation(
        prospect_id=shared_prospect,
        channel="linkedin",
        unipile_chat_id="same-provider-chat-id",
        account_id=tenant_b,
        provider_account_id="linkedin-sender-b",
    )

    assert conversation_a["_id"] != conversation_b["_id"]
    assert len(collection.documents) == 2
    assert {doc["account_id"] for doc in collection.documents} == {tenant_a, tenant_b}

    # Even a known conversation id cannot be used as a cross-tenant append key.
    cross_tenant_result = await conversation_service.add_message_to_conversation(
        str(conversation_a["_id"]),
        Message(
            direction="inbound",
            content_text="must not cross",
            unipile_message_id="li-message-1",
        ),
        account_id=tenant_b,
        provider_account_id="linkedin-sender-b",
    )
    assert cross_tenant_result is None
    assert collection.documents[0]["message_count"] == 0


@pytest.mark.asyncio
async def test_provider_message_replay_appends_only_once(monkeypatch):
    tenant_id = str(ObjectId())
    conversation_id = ObjectId()
    collection = _MemoryCollection([
        {
            "_id": conversation_id,
            "account_id": tenant_id,
            "channel": "email",
            "provider_account_id": "mailbox-1",
            "provider_thread_id": "thread-1",
            "prospect_id": str(ObjectId()),
            "messages": [],
            "message_count": 0,
        }
    ])
    monkeypatch.setattr(conversation_service, "conversations_collection", collection)
    replayed_message = Message(
        direction="inbound",
        content_text="Yes, let's talk",
        provider="google",
        provider_message_id="provider-message-1",
        status="received",
    )

    for _ in range(2):
        result = await conversation_service.add_message_to_conversation(
            str(conversation_id),
            replayed_message,
            account_id=tenant_id,
            provider_account_id="mailbox-1",
        )
        assert result is not None

    stored = collection.documents[0]
    assert stored["message_count"] == 1
    assert [m["provider_message_id"] for m in stored["messages"]] == [
        "provider-message-1"
    ]


@pytest.mark.asyncio
async def test_reply_fanout_claim_is_idempotent_within_tenant_provider(monkeypatch):
    tenant_id = str(ObjectId())
    campaign_message_id = ObjectId()
    collection = _MemoryCollection([
        {
            "_id": campaign_message_id,
            "account_id": tenant_id,
            "status": "sent",
        }
    ])
    monkeypatch.setattr(
        reply_ingest.database, "campaign_messages_collection", collection
    )
    now = datetime.utcnow()

    first = await reply_ingest._claim_reply_once(
        campaign_message_id,
        tenant_id,
        "mailbox-1",
        "provider-reply-1",
        now,
    )
    replay = await reply_ingest._claim_reply_once(
        campaign_message_id,
        tenant_id,
        "mailbox-1",
        "provider-reply-1",
        now,
    )

    assert first is True
    assert replay is False
    assert collection.documents[0]["processed_reply_keys"] == [
        f"{tenant_id}:mailbox-1:provider-reply-1"
    ]


@pytest.mark.asyncio
async def test_missing_tenant_context_fails_before_database_access(monkeypatch):
    collection = _MemoryCollection()
    monkeypatch.setattr(conversation_service, "conversations_collection", collection)

    with pytest.raises(ValueError, match="account_id is required"):
        await conversation_service.get_or_create_conversation(
            prospect_id=str(ObjectId()),
            channel="email",
            account_id=None,
        )

    with pytest.raises(ValueError, match="requires account_id"):
        await reply_ingest._claim_reply_once(
            ObjectId(), None, "mailbox-1", "provider-message-1", datetime.utcnow()
        )

    assert collection.calls == []
