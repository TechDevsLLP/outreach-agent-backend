"""Email replies must continue the prospect's existing thread.

The thread handle used to be read only from campaign_messages. That collection
is scoped to a campaign and deleted with it, and it stores prospect_id as an
ObjectId while conversations store a string, so the lookup could return nothing
and every reply started a brand new email thread with the prospect.
"""
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest
from bson import ObjectId

from services import email_delivery_service


CONVERSATION = {
    "_id": ObjectId(),
    "account_id": "acct-1",
    "prospect_id": "69c2387c38b085f2a25d0572",
    "channel": "email",
    "provider_account_id": str(ObjectId()),
    "provider_thread_id": "19f985a1e15cb311",
    "messages": [
        {"direction": "outbound", "provider_message_id": "19f985a1e15cb311"},
        {"direction": "inbound", "provider_message_id": "19f9d949015b0d83"},
    ],
}


def _patch_db(monkeypatch, *, campaign_message=None, email_account=None):
    monkeypatch.setattr(
        email_delivery_service.database,
        "campaign_messages_collection",
        SimpleNamespace(find_one=AsyncMock(return_value=campaign_message)),
    )
    monkeypatch.setattr(
        email_delivery_service.database,
        "email_accounts_collection",
        SimpleNamespace(find_one=AsyncMock(return_value=email_account)),
    )


@pytest.mark.asyncio
async def test_thread_ref_comes_from_conversation_when_campaign_messages_empty(monkeypatch):
    """A deleted campaign must not cost the prospect their email thread."""
    account = {"_id": ObjectId(CONVERSATION["provider_account_id"]), "email": "s@x.com"}
    _patch_db(monkeypatch, campaign_message=None, email_account=account)

    ctx = await email_delivery_service.resolve_email_thread_context(CONVERSATION)

    assert ctx["thread_ref"] == "19f985a1e15cb311"
    assert ctx["email_account"] == account
    # Replies target the newest message in the thread, whoever sent it.
    assert ctx["provider_message_id"] == "19f9d949015b0d83"


@pytest.mark.asyncio
async def test_conversation_thread_ref_wins_over_campaign_messages(monkeypatch):
    """The conversation is the durable record; a stale campaign row must not win."""
    account = {"_id": ObjectId(CONVERSATION["provider_account_id"]), "email": "s@x.com"}
    _patch_db(
        monkeypatch,
        campaign_message={
            "provider_thread_id": "STALE-THREAD",
            "provider_message_id": "STALE-MSG",
            "email_account_id": ObjectId(),
        },
        email_account=account,
    )

    ctx = await email_delivery_service.resolve_email_thread_context(CONVERSATION)

    assert ctx["thread_ref"] == "19f985a1e15cb311"


@pytest.mark.asyncio
async def test_falls_back_to_campaign_messages_when_conversation_has_no_thread(monkeypatch):
    """Legacy conversations without a provider_thread_id still thread correctly."""
    acct_id = ObjectId()
    account = {"_id": acct_id, "email": "s@x.com"}
    _patch_db(
        monkeypatch,
        campaign_message={
            "provider_thread_id": "FROM-CAMPAIGN",
            "provider_message_id": "msg-1",
            "email_account_id": acct_id,
        },
        email_account=account,
    )
    legacy = {k: v for k, v in CONVERSATION.items()
              if k not in {"provider_thread_id", "provider_account_id", "messages"}}

    ctx = await email_delivery_service.resolve_email_thread_context(legacy)

    assert ctx["thread_ref"] == "FROM-CAMPAIGN"


def test_id_variants_matches_both_storage_types():
    """campaign_messages stores an ObjectId, conversations store a string."""
    hex_id = "69c2387c38b085f2a25d0572"
    variants = email_delivery_service._id_variants(hex_id)
    assert hex_id in variants
    assert ObjectId(hex_id) in variants
    assert email_delivery_service._id_variants(None) == []
