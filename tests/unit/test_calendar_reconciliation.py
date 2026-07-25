"""Offline Google Calendar reconciliation and scheduler-boundary tests."""

from copy import deepcopy
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock

import pytest
from bson import ObjectId

from services import calendar_service, meeting_service, scheduler_service

pytestmark = pytest.mark.unit


class _Cursor:
    def __init__(self, documents):
        self.documents = [deepcopy(document) for document in documents]
        self.limit_value = None

    def sort(self, *_args):
        return self

    def limit(self, value):
        self.limit_value = value
        return self

    async def to_list(self, length):
        return deepcopy(self.documents[:length])


def _bound_meeting(**overrides):
    meeting = {
        "_id": ObjectId(),
        "account_id": "tenant-a",
        "enrollment_id": str(ObjectId()),
        "status": "booked",
        "calendar_provider": "google",
        "calendar_provider_account_id": str(ObjectId()),
        "calendar_id": "primary",
        "calendar_event_id": "event-1",
        "calendar_attendee_email": "prospect@example.com",
        "calendar_start_at": "2030-01-02T10:00:00Z",
        "calendar_end_at": "2030-01-02T10:25:00Z",
        "calendar_sync_attempt_count": 1,
    }
    meeting.update(overrides)
    return meeting


@pytest.mark.asyncio
async def test_calendar_credential_exactly_matches_tenant_google_account(monkeypatch):
    provider_account_id = ObjectId()
    email_accounts = SimpleNamespace(
        find_one=AsyncMock(return_value={"_id": provider_account_id})
    )
    monkeypatch.setattr(calendar_service.database, "email_accounts_collection", email_accounts)
    monkeypatch.setattr(
        calendar_service,
        "refresh_token_if_needed",
        AsyncMock(return_value="access-token"),
    )

    account, token = await calendar_service._get_calendar_credential(
        "tenant-a", str(provider_account_id)
    )

    assert account["_id"] == provider_account_id and token == "access-token"
    query = email_accounts.find_one.await_args.args[0]
    assert query["_id"] == provider_account_id
    assert query["account_id"] == "tenant-a"
    assert query["provider"] == "google"


@pytest.mark.asyncio
async def test_calendar_credential_without_binding_fails_closed_when_ambiguous(monkeypatch):
    candidates = _Cursor([{"_id": ObjectId()}, {"_id": ObjectId()}])
    email_accounts = SimpleNamespace(find=MagicMock(return_value=candidates))
    refresh = AsyncMock()
    monkeypatch.setattr(calendar_service.database, "email_accounts_collection", email_accounts)
    monkeypatch.setattr(calendar_service, "refresh_token_if_needed", refresh)

    assert await calendar_service._get_calendar_credential("tenant-a") is None
    refresh.assert_not_awaited()
    assert email_accounts.find.call_args.args[0]["provider"] == "google"


@pytest.mark.asyncio
async def test_cancelled_event_is_exactly_bound_claimed_and_idempotent(monkeypatch):
    candidate = _bound_meeting()
    claimed = {**candidate, "calendar_sync_lease_owner": "worker-a"}
    cursor = _Cursor([candidate])
    meetings = SimpleNamespace(
        find=MagicMock(return_value=cursor),
        find_one_and_update=AsyncMock(side_effect=[deepcopy(claimed), None]),
        update_one=AsyncMock(return_value=SimpleNamespace(modified_count=1)),
    )
    enrollments = SimpleNamespace(
        update_one=AsyncMock(return_value=SimpleNamespace(modified_count=1))
    )
    monkeypatch.setattr(meeting_service.database, "meetings_collection", meetings)
    monkeypatch.setattr(
        meeting_service.database, "campaign_enrollments_collection", enrollments
    )
    fetch = AsyncMock(
        return_value={"id": "event-1", "status": "cancelled", "updated": "u1"}
    )

    first = await meeting_service.sync_meeting_statuses(
        "tenant-a", worker_id="worker-a", event_fetcher=fetch
    )
    second = await meeting_service.sync_meeting_statuses(
        "tenant-a", worker_id="worker-b", event_fetcher=fetch
    )

    assert first == {"claimed": 1, "updated": 1, "cancelled": 1, "failed": 0}
    assert second == {"claimed": 0, "updated": 0, "cancelled": 0, "failed": 0}
    fetch.assert_awaited_once_with(
        "tenant-a",
        provider_account_id=candidate["calendar_provider_account_id"],
        calendar_id="primary",
        event_id="event-1",
    )
    apply_query, apply_update = meetings.update_one.await_args_list[0].args
    assert apply_query["account_id"] == "tenant-a"
    assert apply_query["calendar_provider_account_id"] == candidate["calendar_provider_account_id"]
    assert apply_query["calendar_sync_fingerprint"] == {
        "$ne": apply_update["$set"]["calendar_sync_fingerprint"]
    }
    assert apply_update["$set"]["status"] == "cancelled"
    assert apply_update["$set"]["cancellation_reason"] == "provider_event_cancelled"
    enrollment_query, enrollment_update = enrollments.update_one.await_args.args
    assert enrollment_query["account_id"] == "tenant-a"
    assert enrollment_query["status"] == "meeting_booked"
    assert enrollment_update["$set"]["status"] == "active"


@pytest.mark.asyncio
async def test_reschedule_and_attendee_acceptance_are_projected(monkeypatch):
    candidate = _bound_meeting()
    meetings = SimpleNamespace(
        find=MagicMock(return_value=_Cursor([candidate])),
        find_one_and_update=AsyncMock(return_value=deepcopy(candidate)),
        update_one=AsyncMock(return_value=SimpleNamespace(modified_count=1)),
    )
    enrollments = SimpleNamespace(update_one=AsyncMock())
    monkeypatch.setattr(meeting_service.database, "meetings_collection", meetings)
    monkeypatch.setattr(
        meeting_service.database, "campaign_enrollments_collection", enrollments
    )
    fetch = AsyncMock(
        return_value={
            "id": "event-1",
            "status": "confirmed",
            "updated": "u2",
            "start": {"dateTime": "2030-01-03T11:00:00Z"},
            "end": {"dateTime": "2030-01-03T11:25:00Z"},
            "attendees": [
                {"email": "PROSPECT@example.com", "responseStatus": "accepted"}
            ],
        }
    )

    result = await meeting_service.sync_meeting_statuses(
        "tenant-a", worker_id="worker-a", event_fetcher=fetch
    )

    assert result["updated"] == 1 and result["cancelled"] == 0
    projection = meetings.update_one.await_args_list[0].args[1]["$set"]
    assert projection["status"] == "rescheduled"
    assert projection["calendar_attendee_status"] == "accepted"
    assert projection["calendar_start_at"] == "2030-01-03T11:00:00Z"
    assert projection["rescheduled_at"]
    enrollments.update_one.assert_not_awaited()


@pytest.mark.asyncio
async def test_attendee_decline_cancels_and_provider_error_only_schedules_retry(monkeypatch):
    declined = _bound_meeting(calendar_event_id="event-declined")
    failed = _bound_meeting(calendar_event_id="event-failed")
    meetings = SimpleNamespace(
        find=MagicMock(return_value=_Cursor([declined, failed])),
        find_one_and_update=AsyncMock(side_effect=[deepcopy(declined), deepcopy(failed)]),
        update_one=AsyncMock(return_value=SimpleNamespace(modified_count=1)),
    )
    enrollments = SimpleNamespace(update_one=AsyncMock())
    monkeypatch.setattr(meeting_service.database, "meetings_collection", meetings)
    monkeypatch.setattr(
        meeting_service.database, "campaign_enrollments_collection", enrollments
    )

    async def fetch(_account_id, *, event_id, **_binding):
        if event_id == "event-failed":
            raise calendar_service.CalendarEventFetchError("rate limited", status_code=429)
        return {
            "id": event_id,
            "status": "confirmed",
            "attendees": [
                {"email": "prospect@example.com", "responseStatus": "declined"}
            ],
        }

    result = await meeting_service.sync_meeting_statuses(
        "tenant-a", worker_id="worker-a", event_fetcher=fetch
    )

    assert result == {"claimed": 2, "updated": 1, "cancelled": 1, "failed": 1}
    applied = meetings.update_one.await_args_list[0].args[1]["$set"]
    assert applied["cancellation_reason"] == "attendee_declined"
    retry = meetings.update_one.await_args_list[-1].args[1]["$set"]
    assert retry["calendar_sync_error"] == "rate limited"
    assert "status" not in retry
    assert retry["next_calendar_sync_at"]


@pytest.mark.asyncio
async def test_get_event_uses_exact_mailbox_calendar_and_normalizes_not_found(monkeypatch):
    provider_account_id = str(ObjectId())
    credential = AsyncMock(
        return_value=({"_id": ObjectId(provider_account_id)}, "access-token")
    )
    monkeypatch.setattr(calendar_service, "_get_calendar_credential", credential)
    requests = []

    class _Client:
        def __init__(self, *args, **kwargs):
            pass

        async def __aenter__(self):
            return self

        async def __aexit__(self, *args):
            return False

        async def get(self, url, **kwargs):
            requests.append((url, kwargs))
            return SimpleNamespace(status_code=404)

    monkeypatch.setattr(calendar_service.httpx, "AsyncClient", _Client)
    result = await calendar_service.get_event(
        "tenant-a",
        provider_account_id=provider_account_id,
        calendar_id="team/calendar@example.com",
        event_id="event/one",
    )

    credential.assert_awaited_once_with("tenant-a", provider_account_id)
    assert "team%2Fcalendar%40example.com" in requests[0][0]
    assert requests[0][0].endswith("event%2Fone")
    assert result == {
        "id": "event/one",
        "status": "cancelled",
        "_outflo_not_found": True,
    }


class _AsyncDocuments:
    def __init__(self, documents):
        self.documents = documents

    def __aiter__(self):
        self.iterator = iter(self.documents)
        return self

    async def __anext__(self):
        try:
            return next(self.iterator)
        except StopIteration:
            raise StopAsyncIteration


@pytest.mark.asyncio
async def test_scheduler_bounds_groups_and_passes_exact_provider_binding(monkeypatch):
    aggregate = MagicMock(
        return_value=_AsyncDocuments(
            [{"_id": {"account_id": "tenant-a", "provider_account_id": "mailbox-a"}}]
        )
    )
    monkeypatch.setattr(
        meeting_service.database,
        "meetings_collection",
        SimpleNamespace(aggregate=aggregate),
    )
    sync = AsyncMock()
    monkeypatch.setattr(meeting_service, "sync_meeting_statuses", sync)

    await scheduler_service._meeting_status_sync()

    pipeline = aggregate.call_args.args[0]
    assert {"$limit": 10} in pipeline
    sync.assert_awaited_once_with(
        "tenant-a", provider_account_id="mailbox-a", max_meetings=5
    )
