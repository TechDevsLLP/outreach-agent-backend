"""Tenant-bound regressions for activity feed and summary reads."""

from types import SimpleNamespace

import pytest
from bson import ObjectId

from services import activity_feed_service


pytestmark = pytest.mark.unit


class _Cursor:
    def __init__(self, docs=None):
        self.docs = list(docs or [])

    async def to_list(self, length):
        return self.docs[:length]


class _Collection:
    def __init__(self):
        self.pipelines = []
        self.queries = []

    def aggregate(self, pipeline):
        self.pipelines.append(pipeline)
        return _Cursor()

    async def count_documents(self, query):
        self.queries.append(query)
        return 0


async def test_activity_feed_requires_and_applies_tenant_to_every_pipeline(monkeypatch):
    account_id = str(ObjectId())
    conversations = _Collection()
    monkeypatch.setattr(
        activity_feed_service, "conversations_collection", conversations
    )

    result = await activity_feed_service.get_activity_feed(account_id=account_id)

    assert result["items"] == []
    assert len(conversations.pipelines) == 2
    for pipeline in conversations.pipelines:
        match = pipeline[0]["$match"]
        assert match["account_id"]["$in"] == [account_id, ObjectId(account_id)]


async def test_activity_feed_fails_closed_without_tenant(monkeypatch):
    conversations = _Collection()
    monkeypatch.setattr(
        activity_feed_service, "conversations_collection", conversations
    )

    with pytest.raises(ValueError, match="account_id is required"):
        await activity_feed_service.get_activity_feed(account_id="")

    assert conversations.pipelines == []


async def test_activity_summary_scopes_conversations_and_overlays(monkeypatch):
    account_id = str(ObjectId())
    conversations = _Collection()
    state = _Collection()
    monkeypatch.setattr(
        activity_feed_service, "conversations_collection", conversations
    )
    monkeypatch.setattr(
        activity_feed_service, "prospect_state_collection", state
    )

    summary = await activity_feed_service.get_activity_summary(account_id)

    assert summary["today_sent"] == 0
    assert len(conversations.queries) == 4
    assert len(state.queries) == 2
    for query in conversations.queries + state.queries:
        assert query["account_id"]["$in"] == [account_id, ObjectId(account_id)]
