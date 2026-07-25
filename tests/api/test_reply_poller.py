"""
Tests for the unified email reply poller (services/email_reply_poller.py),
which replaced the Gmail-only poller now that Zoho and SMTP/IMAP are
first-class channels. Provider HTTP/IMAP calls are faked via
services.email_providers.get_provider — these are DB-integration tests, not
HTTP endpoint tests.
"""
from datetime import datetime, timedelta

import pytest
from bson import ObjectId

import database
from services.email_providers.base import DraftResult, EmailProvider, ReplyMeta, SendResult

pytestmark = pytest.mark.api


class _FakeReplyProvider(EmailProvider):
    """A stub EmailProvider that reports one canned reply per thread."""

    def __init__(self, email_account, replies_by_thread):
        super().__init__(email_account)
        self._replies_by_thread = replies_by_thread

    async def send(self, *a, **kw):
        raise NotImplementedError

    async def create_draft(self, *a, **kw):
        raise NotImplementedError

    async def fetch_new_replies(self, thread_ref, sender_email):
        return self._replies_by_thread.get(thread_ref, [])

    async def get_message_body(self, provider_message_id):
        return f"Full body for {provider_message_id}"

    async def verify(self):
        return True


@pytest.fixture
async def email_account_a(identity_a):
    now = datetime.utcnow()
    result = await database.email_accounts_collection.insert_one({
        "account_id": identity_a["account_id"],
        "user_id": identity_a["user_id"],
        "email": "sender@example.com",
        "display_name": "Sender",
        "provider": "zoho",
        "status": "connected",
        "created_at": now,
        "updated_at": now,
    })
    return await database.email_accounts_collection.find_one({"_id": result.inserted_id})


@pytest.fixture
async def prospect_for_reply(identity_a):
    now = datetime.utcnow()
    result = await database.prospects_collection.insert_one({
        "full_name": "Reply Test Prospect",
        "first_name": "Reply",
        "email": "prospect-reply@example.com",
        "company_name": "Acme Reply Co",
        "status": "contacted",
        "created_at": now,
        "last_updated_at": now,
    })
    return await database.prospects_collection.find_one({"_id": result.inserted_id})


@pytest.fixture
async def sent_campaign_message(identity_a, email_account_a, prospect_for_reply):
    now = datetime.utcnow()
    doc = {
        "campaign_id": ObjectId(),
        "campaign_enrollment_id": ObjectId(),
        "account_id": identity_a["account_id"],
        "prospect_id": str(prospect_for_reply["_id"]),
        "step_number": 1,
        "channel": "email",
        "action": "email",
        "direction": "outbound",
        "subject": "Quick question",
        "content_text": "Hi there",
        "content_html": "Hi there",
        "provider_message_id": "sent-msg-1",
        "email_account_id": email_account_a["_id"],
        "provider": "zoho",
        "provider_thread_id": "thread-abc",
        "status": "sent",
        "scheduled_at": now,
        "sent_at": now,
        "created_at": now,
    }
    result = await database.campaign_messages_collection.insert_one(doc)
    return await database.campaign_messages_collection.find_one({"_id": result.inserted_id})


async def test_check_email_replies_marks_message_replied_and_writes_conversation(
    monkeypatch, sent_campaign_message, prospect_for_reply,
):
    from services import email_reply_poller

    reply = ReplyMeta(
        provider_message_id="reply-msg-1",
        from_email="prospect-reply@example.com",
        subject="Re: Quick question",
        snippet="Sounds great, let's talk",
        thread_ref="thread-abc",
    )
    fake_provider = _FakeReplyProvider(None, {"thread-abc": [reply]})
    monkeypatch.setattr(email_reply_poller, "get_provider", lambda account: fake_provider)

    await email_reply_poller.check_email_replies()

    updated_msg = await database.campaign_messages_collection.find_one({"_id": sent_campaign_message["_id"]})
    assert updated_msg["status"] == "replied"
    assert updated_msg["replied_at"] is not None

    conversation = await database.conversations_collection.find_one(
        {"prospect_id": str(prospect_for_reply["_id"]), "channel": "email"}
    )
    assert conversation is not None
    inbound_messages = [m for m in conversation["messages"] if m["direction"] == "inbound"]
    assert len(inbound_messages) == 1
    assert inbound_messages[0]["content_text"] == "Full body for reply-msg-1"
    assert conversation["needs_classification"] is True

    updated_prospect = await database.prospects_collection.find_one({"_id": prospect_for_reply["_id"]})
    assert updated_prospect["status"] == "replied"


async def test_check_email_replies_no_reply_yet_leaves_message_sent(
    monkeypatch, sent_campaign_message,
):
    from services import email_reply_poller

    fake_provider = _FakeReplyProvider(None, {})  # no replies for any thread
    monkeypatch.setattr(email_reply_poller, "get_provider", lambda account: fake_provider)

    await email_reply_poller.check_email_replies()

    updated_msg = await database.campaign_messages_collection.find_one({"_id": sent_campaign_message["_id"]})
    assert updated_msg["status"] == "sent"
    assert updated_msg.get("replied_at") is None


async def test_check_email_replies_skips_unsupported_provider(monkeypatch, sent_campaign_message):
    """get_provider() returning None (e.g. retired microsoft stub) must not raise."""
    from services import email_reply_poller

    monkeypatch.setattr(email_reply_poller, "get_provider", lambda account: None)

    await email_reply_poller.check_email_replies()  # should not raise

    updated_msg = await database.campaign_messages_collection.find_one({"_id": sent_campaign_message["_id"]})
    assert updated_msg["status"] == "sent"
