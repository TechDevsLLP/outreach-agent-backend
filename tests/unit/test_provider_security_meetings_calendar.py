"""Focused provider-boundary regressions for Apify, Calendar, and meetings."""

import hashlib
from concurrent.futures import ThreadPoolExecutor
from copy import deepcopy
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest
from bson import ObjectId
from fastapi import HTTPException

import routes.calendar as calendar_routes
import routes.public_booking as public_booking_routes
from services import calendar_service, employee_scraper_service, linkedin_post_scraper_service
from services import meeting_service

pytestmark = pytest.mark.unit


@pytest.mark.asyncio
async def test_public_booking_binds_invite_to_known_prospect_email(monkeypatch):
    conversation_id = ObjectId()
    conversations = SimpleNamespace(
        find_one=AsyncMock(return_value={"prospect_email": "known@example.com"})
    )
    prospects = SimpleNamespace(find_one=AsyncMock())
    monkeypatch.setattr(public_booking_routes.database, "conversations_collection", conversations)
    monkeypatch.setattr(public_booking_routes.database, "prospects_collection", prospects)
    meeting = {
        "account_id": str(ObjectId()),
        "conversation_id": str(conversation_id),
    }

    assert await public_booking_routes._trusted_booking_email(
        meeting, "KNOWN@example.com"
    ) == "known@example.com"
    with pytest.raises(HTTPException, match="does not match"):
        await public_booking_routes._trusted_booking_email(
            meeting, "attacker@example.com"
        )
    prospects.find_one.assert_not_awaited()


@pytest.mark.asyncio
async def test_public_booking_requires_valid_email_when_no_known_email(monkeypatch):
    monkeypatch.setattr(
        public_booking_routes.database,
        "conversations_collection",
        SimpleNamespace(find_one=AsyncMock(return_value=None)),
    )
    monkeypatch.setattr(
        public_booking_routes.database,
        "prospects_collection",
        SimpleNamespace(find_one=AsyncMock(return_value=None)),
    )

    with pytest.raises(HTTPException, match="valid attendee email"):
        await public_booking_routes._trusted_booking_email(
            {"account_id": str(ObjectId())}, "not-an-email"
        )


@pytest.mark.parametrize(
    "module",
    [employee_scraper_service, linkedin_post_scraper_service],
)
def test_apify_dataset_clients_are_lazy_and_thread_safe(monkeypatch, module):
    constructions = []

    class _DatasetClient:
        def __init__(self, api_key):
            constructions.append(api_key)

        def dataset(self, dataset_id):
            return dataset_id

    monkeypatch.setattr(module, "ApifyClient", _DatasetClient)
    client = module._LazyApifyClient("secret")
    assert client._client is None

    with ThreadPoolExecutor(max_workers=8) as pool:
        results = list(pool.map(client.dataset, ["dataset"] * 32))

    assert results == ["dataset"] * 32
    assert constructions == ["secret"]


class _Request:
    def __init__(self, headers, body=b""):
        self.headers = headers
        self._body = body

    async def body(self):
        return self._body


class _CalendarChannels:
    def __init__(self, document):
        self.document = deepcopy(document)
        self.updates = []

    async def find_one(self, query):
        if query.get("channel_id") != self.document.get("channel_id"):
            return None
        return deepcopy(self.document)

    async def update_one(self, query, update):
        self.updates.append((deepcopy(query), deepcopy(update)))
        if "last_message_number" in query:
            candidate = query["last_message_number"]["$lt"]
            if self.document["last_message_number"] >= candidate:
                return SimpleNamespace(matched_count=0)
        if "$set" in update:
            self.document.update(update["$set"])
        return SimpleNamespace(matched_count=1)


@pytest.mark.asyncio
async def test_google_webhook_requires_exact_channel_secret_resource_and_tenant_provider(monkeypatch):
    account_id = str(ObjectId())
    provider_account_id = ObjectId()
    token = "provider-random-channel-secret"
    channels = _CalendarChannels(
        {
            "channel_id": "channel-1",
            "channel_token_hash": hashlib.sha256(token.encode()).hexdigest(),
            "resource_id": "resource-1",
            "account_id": account_id,
            "provider_account_id": str(provider_account_id),
            "provider": "google",
            "status": "active",
            "last_message_number": 0,
        }
    )
    email_accounts = SimpleNamespace(find_one=AsyncMock(return_value={"_id": provider_account_id}))
    monkeypatch.setattr(calendar_routes.database, "calendar_webhook_channels_collection", channels)
    monkeypatch.setattr(calendar_routes.database, "email_accounts_collection", email_accounts)
    meetings = SimpleNamespace(update_many=AsyncMock())
    monkeypatch.setattr(calendar_routes.database, "meetings_collection", meetings)

    headers = {
        "X-Goog-Channel-ID": "channel-1",
        "X-Goog-Channel-Token": token,
        "X-Goog-Resource-ID": "resource-1",
        "X-Goog-Resource-State": "exists",
        "X-Goog-Message-Number": "1",
    }
    assert await calendar_routes.google_calendar_webhook(_Request(headers)) == {"ok": True}
    assert await calendar_routes.google_calendar_webhook(_Request(headers)) == {
        "ok": True,
        "duplicate": True,
    }
    meetings.update_many.assert_awaited_once()
    meeting_query = meetings.update_many.await_args.args[0]
    assert meeting_query["account_id"] == account_id
    assert meeting_query["calendar_provider_account_id"] == str(provider_account_id)
    provider_query = email_accounts.find_one.await_args.args[0]
    assert provider_query == {
        "_id": provider_account_id,
        "account_id": account_id,
        "provider": "google",
        "oauth_scopes": {"$elemMatch": {"$regex": "calendar"}},
    }

    bad_headers = {**headers, "X-Goog-Channel-Token": "wrong"}
    with pytest.raises(HTTPException) as rejected:
        await calendar_routes.google_calendar_webhook(_Request(bad_headers))
    assert rejected.value.status_code == 403
    meetings.update_many.assert_awaited_once()


@pytest.mark.asyncio
async def test_google_webhook_rejects_body_and_resource_mismatch_before_sync(monkeypatch):
    account_id = str(ObjectId())
    provider_account_id = ObjectId()
    token = "secret"
    channels = _CalendarChannels(
        {
            "channel_id": "channel-1",
            "channel_token_hash": hashlib.sha256(token.encode()).hexdigest(),
            "resource_id": "resource-1",
            "account_id": account_id,
            "provider_account_id": str(provider_account_id),
            "provider": "google",
            "status": "active",
            "last_message_number": 0,
        }
    )
    monkeypatch.setattr(calendar_routes.database, "calendar_webhook_channels_collection", channels)
    monkeypatch.setattr(
        calendar_routes.database,
        "email_accounts_collection",
        SimpleNamespace(find_one=AsyncMock(return_value={"_id": provider_account_id})),
    )
    meetings = SimpleNamespace(update_many=AsyncMock())
    monkeypatch.setattr(calendar_routes.database, "meetings_collection", meetings)

    base = {
        "X-Goog-Channel-ID": "channel-1",
        "X-Goog-Channel-Token": token,
        "X-Goog-Resource-ID": "wrong-resource",
        "X-Goog-Resource-State": "exists",
        "X-Goog-Message-Number": "1",
    }
    with pytest.raises(HTTPException) as body_error:
        await calendar_routes.google_calendar_webhook(_Request(base, body=b"not-empty"))
    assert body_error.value.status_code == 400
    with pytest.raises(HTTPException) as resource_error:
        await calendar_routes.google_calendar_webhook(_Request(base))
    assert resource_error.value.status_code == 403
    meetings.update_many.assert_not_awaited()


class _EmptyCursor:
    def __aiter__(self):
        return self

    async def __anext__(self):
        raise StopAsyncIteration


class _WatchChannels:
    def __init__(self):
        self.inserted = None
        self.deleted = []
        self.deleted_many = []
        self.updated = []

    async def find_one(self, query):
        return None

    def find(self, query):
        return _EmptyCursor()

    async def insert_one(self, document):
        self.inserted = deepcopy(document)

    async def delete_one(self, query):
        self.deleted.append(deepcopy(query))

    async def delete_many(self, query):
        self.deleted_many.append(deepcopy(query))
        return SimpleNamespace(deleted_count=0)

    async def update_one(self, query, update):
        self.updated.append((deepcopy(query), deepcopy(update)))
        return SimpleNamespace(matched_count=1)


@pytest.mark.asyncio
async def test_calendar_watch_stores_only_token_hash_and_binds_provider(monkeypatch):
    provider_account_id = ObjectId()
    monkeypatch.setattr(
        calendar_service,
        "_get_calendar_credential",
        AsyncMock(return_value=({"_id": provider_account_id}, "access-token")),
    )
    channels = _WatchChannels()
    monkeypatch.setattr(calendar_service.database, "calendar_webhook_channels_collection", channels)

    class _Response:
        status_code = 200
        text = ""

        def __init__(self, request_json):
            self.request_json = request_json

        def json(self):
            return {
                "id": self.request_json["id"],
                "resourceId": "resource-1",
                "resourceUri": "https://www.googleapis.com/calendar/v3/calendars/primary/events",
                "expiration": "1893456000000",
            }

    class _Client:
        def __init__(self, *args, **kwargs):
            pass

        async def __aenter__(self):
            return self

        async def __aexit__(self, *args):
            return False

        async def post(self, url, headers, json):
            self.request_json = deepcopy(json)
            return _Response(json)

    monkeypatch.setattr(calendar_service.httpx, "AsyncClient", _Client)
    from config import get_settings
    callback = f"{get_settings().api_base_url.rstrip('/')}/api/calendar/webhooks/google"

    result = await calendar_service.register_calendar_watch("tenant-a", callback)

    assert result["status"] == "active"
    assert result["resource_id"] == "resource-1"
    assert "channel_token" not in result
    assert channels.inserted["provider_account_id"] == str(provider_account_id)
    assert channels.inserted["account_id"] == "tenant-a"
    assert "channel_token" not in channels.inserted
    assert len(channels.inserted["channel_token_hash"]) == 64
    assert channels.updated[0][0]["provider_account_id"] == str(provider_account_id)


@pytest.mark.asyncio
async def test_calendar_watch_rejects_arbitrary_callback_before_credentials(monkeypatch):
    credentials = AsyncMock()
    monkeypatch.setattr(calendar_service, "_get_calendar_credential", credentials)
    with pytest.raises(ValueError, match="configured OutFlo"):
        await calendar_service.register_calendar_watch(
            "tenant-a", "https://attacker.example/google-calendar"
        )
    credentials.assert_not_awaited()


@pytest.mark.asyncio
async def test_calendar_status_uses_stored_google_provider_value(monkeypatch):
    account_id = ObjectId()
    email_accounts = SimpleNamespace(
        find_one=AsyncMock(return_value={"email": "sender@example.com"})
    )
    monkeypatch.setattr(calendar_routes.database, "email_accounts_collection", email_accounts)

    result = await calendar_routes.calendar_status(
        account_ctx={"account": {"_id": account_id}}
    )

    assert result == {
        "connected": True,
        "provider": "google",
        "email": "sender@example.com",
    }
    assert email_accounts.find_one.await_args.args[0]["provider"] == "google"


@pytest.mark.asyncio
async def test_meeting_confirmation_is_tenant_scoped_and_provider_idempotent(monkeypatch):
    account_id = str(ObjectId())
    meeting_id = ObjectId()
    enrollment_id = ObjectId()
    campaign_id = ObjectId()
    slot = {"label": "Tomorrow", "datetime_iso": "2030-01-02T10:00:00Z"}
    proposed = {
        "_id": meeting_id,
        "account_id": account_id,
        "enrollment_id": str(enrollment_id),
        "status": "proposed",
        "confirmed_slot_index": None,
        "proposed_slots": [slot],
    }
    booking = {**proposed, "status": "booking", "confirmed_slot_index": 0}
    booked = {
        **booking,
        "status": "booked",
        "calendar_event_id": "event-1",
        "calendar_event_link": "https://calendar.example/event-1",
    }

    meetings = SimpleNamespace(
        find_one=AsyncMock(side_effect=[deepcopy(proposed), deepcopy(booked)]),
        find_one_and_update=AsyncMock(side_effect=[deepcopy(booking), deepcopy(booked)]),
        update_one=AsyncMock(),
    )
    enrollments = SimpleNamespace(
        update_one=AsyncMock(),
        find_one=AsyncMock(return_value={"campaign_id": campaign_id}),
    )
    provider_account_id = ObjectId()
    campaigns = SimpleNamespace(
        find_one=AsyncMock(return_value={"email_account_id": provider_account_id}),
        update_one=AsyncMock(),
    )
    monkeypatch.setattr(meeting_service.database, "meetings_collection", meetings)
    monkeypatch.setattr(meeting_service.database, "campaign_enrollments_collection", enrollments)
    monkeypatch.setattr(meeting_service.database, "campaigns_collection", campaigns)
    monkeypatch.setattr(
        meeting_service.database,
        "email_accounts_collection",
        SimpleNamespace(
            find_one=AsyncMock(return_value={"_id": provider_account_id})
        ),
    )
    create_event = AsyncMock(
        side_effect=lambda _account_id, event, **_kwargs: {
            "id": event["id"],
            "htmlLink": "https://calendar.example/event-1",
            "_outflo_provider_account_id": str(provider_account_id),
            "_outflo_calendar_id": "primary",
        }
    )
    monkeypatch.setattr(calendar_service, "create_event", create_event)

    first = await meeting_service.confirm_slot_and_send_invite(
        str(meeting_id), 0, account_id=account_id, prospect_email="prospect@example.com"
    )
    second = await meeting_service.confirm_slot_and_send_invite(
        str(meeting_id), 0, account_id=account_id, prospect_email="prospect@example.com"
    )

    assert first["status"] == second["status"] == "booked"
    create_event.assert_awaited_once()
    event = create_event.await_args.args[1]
    assert len(event["id"]) == 32
    assert event["conferenceData"]["createRequest"]["requestId"]
    assert create_event.await_args.kwargs == {
        "provider_account_id": str(provider_account_id),
        "calendar_id": "primary",
    }
    claim_query = meetings.find_one_and_update.await_args_list[0].args[0]
    assert "account_id" in claim_query
    counter_update = campaigns.update_one.await_args.args[1]
    assert isinstance(counter_update, list)
    assert "meetings_booked_keys" in counter_update[0]["$set"]


@pytest.mark.asyncio
async def test_meeting_confirmation_rejects_cross_tenant_before_provider(monkeypatch):
    meetings = SimpleNamespace(find_one=AsyncMock(return_value=None))
    monkeypatch.setattr(meeting_service.database, "meetings_collection", meetings)
    create_event = AsyncMock()
    monkeypatch.setattr(calendar_service, "create_event", create_event)

    with pytest.raises(ValueError, match="Meeting not found"):
        await meeting_service.confirm_slot_and_send_invite(
            str(ObjectId()), 0, account_id=str(ObjectId())
        )
    create_event.assert_not_awaited()


@pytest.mark.asyncio
async def test_meeting_proposal_creation_and_reply_are_idempotent(monkeypatch):
    account_id = str(ObjectId())
    enrollment_id = ObjectId()
    conversation_id = ObjectId()
    prospect_id = str(ObjectId())
    meeting_id = ObjectId()
    slot = {"label": "Tomorrow", "datetime_iso": "2030-01-02T10:00:00Z"}
    prepared = {
        "_id": meeting_id,
        "account_id": account_id,
        "enrollment_id": str(enrollment_id),
        "conversation_id": str(conversation_id),
        "prospect_id": prospect_id,
        "status": "proposed",
        "booking_token": "stable-token",
        "proposed_slots": [slot],
        "proposal_reply_state": "prepared",
    }
    dispatching = {**prepared, "proposal_reply_state": "dispatching"}
    sent = {**prepared, "proposal_reply_state": "sent"}

    meetings = SimpleNamespace(
        find_one=AsyncMock(side_effect=[None, deepcopy(sent)]),
        find_one_and_update=AsyncMock(
            side_effect=[deepcopy(prepared), deepcopy(dispatching), None]
        ),
        update_one=AsyncMock(),
    )
    enrollments = SimpleNamespace(
        find_one=AsyncMock(return_value={"_id": enrollment_id, "prospect_id": prospect_id}),
        update_one=AsyncMock(),
    )
    conversations = SimpleNamespace(
        find_one=AsyncMock(return_value={"_id": conversation_id, "prospect_id": prospect_id}),
        update_one=AsyncMock(),
    )
    monkeypatch.setattr(meeting_service.database, "meetings_collection", meetings)
    monkeypatch.setattr(meeting_service.database, "campaign_enrollments_collection", enrollments)
    monkeypatch.setattr(meeting_service.database, "conversations_collection", conversations)
    monkeypatch.setattr(calendar_service, "propose_three_slots", AsyncMock(return_value=[slot]))
    reply = AsyncMock()
    monkeypatch.setattr(meeting_service, "send_reply", reply)

    kwargs = {
        "enrollment_id": str(enrollment_id),
        "conversation_id": str(conversation_id),
        "account_id": account_id,
        "prospect_id": prospect_id,
        "company_profile": {},
        "prospect_name": "Ada",
        "company_name": "Analytical Engines",
        "message_text": "Interested",
        "conversation_context": "",
        # Explicit: this test covers the immediate-send path. The production
        # default is draft_only=True (see the no-auto-send test below).
        "draft_only": False,
    }
    first = await meeting_service.propose_meeting(**kwargs)
    second = await meeting_service.propose_meeting(**kwargs)

    assert first["_id"] == second["_id"] == str(meeting_id)
    assert first["booking_token"] == second["booking_token"] == "stable-token"
    reply.assert_awaited_once()


@pytest.mark.asyncio
async def test_meeting_proposal_defaults_to_draft_and_never_sends(monkeypatch):
    """By default a proposal is staged for approval, never delivered.

    Guards the no-auto-send guarantee: nothing may reach a prospect without a
    human approving the draft.
    """
    account_id = str(ObjectId())
    enrollment_id = ObjectId()
    conversation_id = ObjectId()
    prospect_id = str(ObjectId())
    meeting_id = ObjectId()
    slot = {"label": "Tomorrow", "datetime_iso": "2030-01-02T10:00:00Z"}
    prepared = {
        "_id": meeting_id,
        "account_id": account_id,
        "enrollment_id": str(enrollment_id),
        "conversation_id": str(conversation_id),
        "prospect_id": prospect_id,
        "prospect_name": "Ada",
        "status": "proposed",
        "booking_token": "stable-token",
        "proposed_slots": [slot],
        "proposal_reply_state": "prepared",
    }

    meetings = SimpleNamespace(
        find_one=AsyncMock(return_value=None),
        find_one_and_update=AsyncMock(return_value=deepcopy(prepared)),
        update_one=AsyncMock(),
    )
    enrollments = SimpleNamespace(
        find_one=AsyncMock(return_value={"_id": enrollment_id, "prospect_id": prospect_id}),
        update_one=AsyncMock(),
    )
    conversations = SimpleNamespace(
        find_one=AsyncMock(return_value={"_id": conversation_id, "prospect_id": prospect_id}),
        update_one=AsyncMock(),
    )
    campaigns = SimpleNamespace(find_one=AsyncMock(return_value=None))
    monkeypatch.setattr(meeting_service.database, "meetings_collection", meetings)
    monkeypatch.setattr(meeting_service.database, "campaign_enrollments_collection", enrollments)
    monkeypatch.setattr(meeting_service.database, "conversations_collection", conversations)
    monkeypatch.setattr(meeting_service.database, "campaigns_collection", campaigns, raising=False)
    monkeypatch.setattr(calendar_service, "propose_three_slots", AsyncMock(return_value=[slot]))
    reply = AsyncMock()
    monkeypatch.setattr(meeting_service, "send_reply", reply)

    await meeting_service.propose_meeting(
        enrollment_id=str(enrollment_id),
        conversation_id=str(conversation_id),
        account_id=account_id,
        prospect_id=prospect_id,
        company_profile={},
        prospect_name="Ada",
        company_name="Analytical Engines",
        message_text="Interested",
        conversation_context="",
    )

    # Nothing sent, and the proposal copy is parked as a pending draft.
    reply.assert_not_awaited()
    conversations.update_one.assert_awaited()
    staged = conversations.update_one.await_args.args[1]["$set"]["ai_draft_reply"]
    assert staged["status"] == "pending"
    assert staged["source"] == "meeting_proposal"
    assert "Tomorrow" in staged["draft_text"]
    calendar_service.propose_three_slots.assert_awaited_once()
    creation_calls = [
        call for call in meetings.find_one_and_update.await_args_list if call.kwargs.get("upsert")
    ]
    assert len(creation_calls) == 1
