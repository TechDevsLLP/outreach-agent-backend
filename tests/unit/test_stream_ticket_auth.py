"""SSE tickets: EventSource can't send a bearer header, so streams take `?ticket=`.

The ticket must be useless once expired and must never be a way around the
normal account checks.
"""
from datetime import datetime, timedelta, timezone

import pytest
from bson import ObjectId
from fastapi import HTTPException

import auth as auth_module


class _FakeCollection:
    def __init__(self, docs=None):
        self.docs = list(docs or [])

    async def insert_one(self, doc):
        self.docs.append(doc)

    async def find_one(self, query):
        for doc in self.docs:
            if doc.get("_id") != query.get("_id"):
                continue
            expiry = query.get("expires_at", {}).get("$gt")
            if expiry is not None and doc["expires_at"] <= expiry:
                continue
            return doc
        return None


@pytest.fixture
def fake_db(monkeypatch):
    import database

    tickets = _FakeCollection()
    monkeypatch.setattr(database, "stream_tickets_collection", tickets, raising=False)
    return tickets


@pytest.mark.asyncio
async def test_ticket_is_opaque_and_scoped(fake_db):
    ticket, ttl = await auth_module.create_stream_ticket("507f1f77bcf86cd799439011", "acct-1")

    assert ttl == auth_module.STREAM_TICKET_TTL_SECONDS
    assert len(ticket) >= 32
    stored = fake_db.docs[0]
    assert stored["_id"] == ticket
    assert stored["account_id"] == "acct-1"
    # The JWT must never be what travels in the query string.
    assert "." not in ticket


@pytest.mark.asyncio
async def test_unknown_ticket_is_rejected(fake_db):
    with pytest.raises(HTTPException) as exc:
        await auth_module._account_context_from_ticket("not-a-real-ticket")
    assert exc.value.status_code == 401


@pytest.mark.asyncio
async def test_expired_ticket_is_rejected_even_before_ttl_sweep(fake_db):
    fake_db.docs.append({
        "_id": "stale",
        "user_id": str(ObjectId()),
        "account_id": "acct-1",
        "expires_at": datetime.now(timezone.utc) - timedelta(seconds=1),
    })
    with pytest.raises(HTTPException) as exc:
        await auth_module._account_context_from_ticket("stale")
    assert exc.value.status_code == 401
