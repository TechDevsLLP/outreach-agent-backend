"""
Google Calendar service.
Uses the same httpx + OAuth token pattern as gmail_service.py.
"""

import asyncio
import hashlib
import logging
import secrets
from datetime import datetime, timedelta, timezone
from typing import Optional
from urllib.parse import quote, urlsplit
from uuid import uuid4

import httpx
from bson import ObjectId

import database
from services.gmail_service import refresh_token_if_needed

logger = logging.getLogger(__name__)

CALENDAR_API_BASE = "https://www.googleapis.com/calendar/v3"


class CalendarEventFetchError(RuntimeError):
    """A bounded reconciliation fetch failed without authoritative event state."""

    def __init__(self, message: str, *, status_code: int | None = None):
        super().__init__(message)
        self.status_code = status_code


# ---------------------------------------------------------------------------
# Internal: token retrieval
# ---------------------------------------------------------------------------

async def _get_calendar_access_token(
    account_id: str, provider_account_id: str | None = None
) -> Optional[str]:
    """
    Look up the Gmail email account for account_id that has calendar scope,
    refresh the token if needed, and return a valid access token.
    Returns None if no matching account exists or refresh fails.
    """
    credential = await _get_calendar_credential(account_id, provider_account_id)
    return credential[1] if credential else None


async def _get_calendar_credential(
    account_id: str, provider_account_id: str | None = None
) -> Optional[tuple[dict, str]]:
    """Return one exact tenant-owned Google account and its usable token.

    A caller-supplied provider account is matched by both ``_id`` and tenant.
    Without one, selection succeeds only when the tenant has exactly one
    calendar-capable Google account. This prevents mailbox drift when a second
    Google account is connected after a meeting is booked.
    """
    if not account_id or not str(account_id).strip():
        raise ValueError("account_id is required for calendar access")

    query: dict = {
        "account_id": account_id,
        "provider": "google",
        "oauth_scopes": {"$elemMatch": {"$regex": "calendar"}},
        "status": {"$in": ["connected", "active"]},
    }
    if provider_account_id:
        try:
            query["_id"] = ObjectId(str(provider_account_id))
        except Exception:
            logger.warning("Invalid Google provider account binding")
            return None
        email_account = await database.email_accounts_collection.find_one(query)
    else:
        candidates = await database.email_accounts_collection.find(query).limit(2).to_list(2)
        if len(candidates) != 1:
            if candidates:
                logger.warning(
                    "Calendar account selection is ambiguous for account_id=%s",
                    account_id,
                )
            return None
        email_account = candidates[0]
    if not email_account:
        logger.warning(
            f"No Gmail account with calendar scope found for account_id={account_id}"
        )
        return None

    token = await refresh_token_if_needed(email_account)
    return (email_account, token) if token else None


# ---------------------------------------------------------------------------
# Internal: deterministic fallback slots
# ---------------------------------------------------------------------------

def _next_business_days(n: int) -> list[datetime]:
    """Return the next n weekdays (Mon–Fri) starting from tomorrow (UTC)."""
    results: list[datetime] = []
    cursor = datetime.now(timezone.utc).replace(
        hour=0, minute=0, second=0, microsecond=0
    ) + timedelta(days=1)
    while len(results) < n:
        if cursor.weekday() < 5:  # 0=Mon … 4=Fri
            results.append(cursor)
        cursor += timedelta(days=1)
    return results


async def _deterministic_slots(n: int = 3) -> list[dict]:
    """
    Return n slot dicts spread across the next business days at 10 AM, 2 PM, 4 PM UTC.
    Mirrors the logic in meeting_service._build_proposed_slots().
    """
    hours = [10, 14, 16]
    business_days = _next_business_days(n)
    slots: list[dict] = []
    for i, bday in enumerate(business_days):
        hour = hours[i % len(hours)]
        slot_dt = bday.replace(hour=hour, minute=0, second=0, microsecond=0)
        label = slot_dt.strftime("%-d %b at %-I:%M %p UTC")
        slots.append({
            "label": label,
            "datetime_iso": slot_dt.strftime("%Y-%m-%dT%H:%M:%SZ"),
        })
    return slots


# ---------------------------------------------------------------------------
# Internal: freebusy parsing
# ---------------------------------------------------------------------------

def _parse_busy_intervals(freebusy_response: dict) -> list[tuple[datetime, datetime]]:
    """Extract busy intervals from a Google Calendar freebusy API response."""
    calendars = freebusy_response.get("calendars", {})
    primary = calendars.get("primary", {})
    busy_raw = primary.get("busy", [])
    intervals: list[tuple[datetime, datetime]] = []
    for interval in busy_raw:
        try:
            start = datetime.fromisoformat(interval["start"].replace("Z", "+00:00"))
            end = datetime.fromisoformat(interval["end"].replace("Z", "+00:00"))
            intervals.append((start, end))
        except (KeyError, ValueError) as exc:
            logger.warning(f"Skipping malformed busy interval {interval}: {exc}")
    return intervals


def _is_free(
    candidate: datetime,
    duration_minutes: int,
    busy: list[tuple[datetime, datetime]],
) -> bool:
    """Return True if [candidate, candidate+duration_minutes) doesn't overlap any busy interval."""
    candidate_end = candidate + timedelta(minutes=duration_minutes)
    for b_start, b_end in busy:
        # Overlap condition: not (candidate_end <= b_start or candidate >= b_end)
        if not (candidate_end <= b_start or candidate >= b_end):
            return False
    return True


def _find_free_slots(
    busy: list[tuple[datetime, datetime]],
    duration_minutes: int,
    n: int,
    search_days: int = 5,
) -> list[dict]:
    """
    Search the next search_days business days (9 AM – 6 PM UTC) for n free slots
    of duration_minutes, stepping every 30 minutes.
    """
    slots: list[dict] = []
    business_days = _next_business_days(search_days)
    step = timedelta(minutes=30)
    window_start_hour = 9
    window_end_hour = 18

    for bday in business_days:
        cursor = bday.replace(hour=window_start_hour, minute=0, second=0, microsecond=0)
        window_end = bday.replace(hour=window_end_hour, minute=0, second=0, microsecond=0)
        while cursor + timedelta(minutes=duration_minutes) <= window_end:
            if _is_free(cursor, duration_minutes, busy):
                label = cursor.strftime("%-d %b at %-I:%M %p UTC")
                slots.append({
                    "label": label,
                    "datetime_iso": cursor.strftime("%Y-%m-%dT%H:%M:%SZ"),
                })
                if len(slots) >= n:
                    return slots
                # Skip past this slot to avoid overlapping candidates
                cursor += timedelta(minutes=duration_minutes)
            else:
                cursor += step

    return slots


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

async def propose_three_slots(account_id: str, duration_minutes: int = 25) -> list[dict]:
    """
    Return 3 available slot dicts for the account's Google Calendar.

    Flow:
      1. Get a valid access token for the account.
      2. If no token → return deterministic fallback slots.
      3. Query the Google Calendar freebusy API for the next 5 business days.
      4. Find 3 free slots of duration_minutes within 9 AM–6 PM UTC windows.
      5. If freebusy query fails → return deterministic fallback slots.

    Each slot dict has keys: "label" (human-readable) and "datetime_iso" (UTC ISO 8601).
    """
    access_token = await _get_calendar_access_token(account_id)
    if not access_token:
        logger.info(
            f"No calendar token for account_id={account_id}; using deterministic fallback slots"
        )
        return await _deterministic_slots(3)

    # Build the 5-business-day search window
    now = datetime.now(timezone.utc)
    business_days = _next_business_days(5)
    time_min = business_days[0].replace(hour=9, minute=0, second=0, microsecond=0)
    time_max = business_days[-1].replace(hour=18, minute=0, second=0, microsecond=0)

    try:
        async with httpx.AsyncClient(timeout=15) as client:
            resp = await client.post(
                f"{CALENDAR_API_BASE}/freeBusy",
                headers={
                    "Authorization": f"Bearer {access_token}",
                    "Content-Type": "application/json",
                },
                json={
                    "timeMin": time_min.strftime("%Y-%m-%dT%H:%M:%SZ"),
                    "timeMax": time_max.strftime("%Y-%m-%dT%H:%M:%SZ"),
                    "items": [{"id": "primary"}],
                },
            )

        if resp.status_code == 401:
            logger.warning(
                f"Calendar freebusy 401 (expired token) for account_id={account_id}"
            )
            return await _deterministic_slots(3)

        if resp.status_code == 403:
            logger.warning(
                f"Calendar freebusy 403 (insufficient scope) for account_id={account_id}"
            )
            return await _deterministic_slots(3)

        if resp.status_code != 200:
            logger.warning(
                f"Calendar freebusy failed for account_id={account_id}: "
                f"HTTP {resp.status_code}: {resp.text}"
            )
            return await _deterministic_slots(3)

        freebusy_data = resp.json()
        busy = _parse_busy_intervals(freebusy_data)
        slots = _find_free_slots(busy, duration_minutes, n=3, search_days=5)

        if len(slots) < 3:
            # Not enough free slots found — pad with deterministic ones
            logger.info(
                f"Only {len(slots)} free slot(s) found for account_id={account_id}; "
                "padding with deterministic fallbacks"
            )
            fallbacks = await _deterministic_slots(3)
            # Append fallbacks that don't duplicate found slots
            existing_isos = {s["datetime_iso"] for s in slots}
            for fb in fallbacks:
                if fb["datetime_iso"] not in existing_isos and len(slots) < 3:
                    slots.append(fb)

        return slots[:3]

    except Exception as exc:
        logger.warning(
            f"Calendar freebusy error for account_id={account_id}: {exc}; "
            "falling back to deterministic slots"
        )
        return await _deterministic_slots(3)


async def create_event(
    account_id: str,
    event_data: dict,
    *,
    provider_account_id: str | None = None,
    calendar_id: str = "primary",
) -> Optional[dict]:
    """
    Create a Google Calendar event on the account's primary calendar.

    Returns the created event dict (contains `id`, `htmlLink`, `status`) or None on failure.
    Pass conferenceData in event_data to request a Google Meet link; this function
    automatically appends conferenceDataVersion=1 to the query.
    """
    credential = await _get_calendar_credential(account_id, provider_account_id)
    if not credential:
        logger.warning(
            f"Cannot create calendar event — no valid token for account_id={account_id}"
        )
        return None
    email_account, access_token = credential
    bound_provider_account_id = str(email_account["_id"])
    encoded_calendar_id = quote(str(calendar_id), safe="")

    try:
        async with httpx.AsyncClient(timeout=30) as client:
            resp = await client.post(
                f"{CALENDAR_API_BASE}/calendars/{encoded_calendar_id}/events",
                headers={
                    "Authorization": f"Bearer {access_token}",
                    "Content-Type": "application/json",
                },
                params={"conferenceDataVersion": "1"},
                json=event_data,
            )

        # Event IDs generated by meeting_service are deterministic. If a prior
        # request crossed the provider boundary but its response was lost,
        # Google returns 409; fetch that exact event instead of creating a
        # duplicate with a new provider request.
        if resp.status_code == 409 and event_data.get("id"):
            async with httpx.AsyncClient(timeout=15) as client:
                existing = await client.get(
                    f"{CALENDAR_API_BASE}/calendars/{encoded_calendar_id}/events/"
                    f"{quote(str(event_data['id']), safe='')}",
                    headers={"Authorization": f"Bearer {access_token}"},
                )
            if existing.status_code == 200:
                result = existing.json()
                result["_outflo_provider_account_id"] = bound_provider_account_id
                result["_outflo_calendar_id"] = str(calendar_id)
                return result

        if resp.status_code not in (200, 201):
            logger.warning(
                f"Calendar create_event failed for account_id={account_id}: "
                f"HTTP {resp.status_code}: {resp.text}"
            )
            return None

        result = resp.json()
        result["_outflo_provider_account_id"] = bound_provider_account_id
        result["_outflo_calendar_id"] = str(calendar_id)
        return result

    except Exception as exc:
        logger.warning(
            f"Calendar create_event error for account_id={account_id}: {exc}"
        )
        return None


async def get_event(
    account_id: str,
    *,
    provider_account_id: str,
    calendar_id: str,
    event_id: str,
) -> dict:
    """Fetch an event through its immutable tenant/mailbox/calendar binding.

    A 404/410 is authoritative deletion evidence and is normalized as a
    cancelled event. Authentication, rate-limit, server and transport failures
    raise ``CalendarEventFetchError`` so reconciliation preserves local state
    and retries later.
    """
    if not all(str(value or "").strip() for value in (
        account_id, provider_account_id, calendar_id, event_id
    )):
        raise ValueError("Exact calendar event binding is required")
    credential = await _get_calendar_credential(account_id, provider_account_id)
    if not credential:
        raise CalendarEventFetchError("Bound Google calendar account is unavailable")
    email_account, access_token = credential
    if str(email_account.get("_id")) != str(provider_account_id):
        raise CalendarEventFetchError("Google calendar account binding changed")

    url = (
        f"{CALENDAR_API_BASE}/calendars/{quote(str(calendar_id), safe='')}/events/"
        f"{quote(str(event_id), safe='')}"
    )
    try:
        async with httpx.AsyncClient(timeout=15) as client:
            response = await client.get(
                url,
                headers={"Authorization": f"Bearer {access_token}"},
                params={"alwaysIncludeEmail": "true"},
            )
    except Exception as exc:
        raise CalendarEventFetchError("Google Calendar event fetch failed") from exc

    if response.status_code in {404, 410}:
        return {
            "id": str(event_id),
            "status": "cancelled",
            "_outflo_not_found": True,
        }
    if response.status_code != 200:
        raise CalendarEventFetchError(
            f"Google Calendar event fetch returned HTTP {response.status_code}",
            status_code=response.status_code,
        )
    try:
        event = response.json()
    except Exception as exc:
        raise CalendarEventFetchError("Google Calendar returned invalid JSON") from exc
    if str(event.get("id") or "") != str(event_id):
        raise CalendarEventFetchError("Google Calendar returned the wrong event")
    return event


def _build_calendar_event(
    meeting: dict,
    slot: dict,
    prospect_email: str,
    agenda: str,
    duration_minutes: int = 25,
) -> dict:
    """
    Build a Google Calendar event dict ready for the Calendar API.

    Args:
        meeting: Meeting document (must contain at least 'prospect_name').
        slot: Slot dict with 'datetime_iso' key (UTC ISO 8601, e.g. "2026-06-09T10:00:00Z").
        prospect_email: Email address of the prospect to invite.
        agenda: Text description / agenda for the event.
        duration_minutes: Duration of the meeting in minutes.

    Returns a dict suitable for passing directly to create_event().
    """
    prospect_name = meeting.get("prospect_name", "Prospect")
    start_dt = datetime.fromisoformat(slot["datetime_iso"].replace("Z", "+00:00"))
    end_dt = start_dt + timedelta(minutes=duration_minutes)

    return {
        "summary": f"Discovery Call — {prospect_name}",
        "description": agenda,
        "start": {
            "dateTime": start_dt.strftime("%Y-%m-%dT%H:%M:%SZ"),
            "timeZone": "UTC",
        },
        "end": {
            "dateTime": end_dt.strftime("%Y-%m-%dT%H:%M:%SZ"),
            "timeZone": "UTC",
        },
        "attendees": [{"email": prospect_email}],
        "conferenceData": {
            "createRequest": {
                "requestId": str(uuid4()),
                "conferenceSolutionKey": {"type": "hangoutsMeet"},
            }
        },
    }


async def register_calendar_watch(account_id: str, webhook_url: str) -> Optional[dict]:
    """
    Register a Google Calendar push notification channel for the account's primary calendar.

    Idempotent: if an active channel for this (account, mailbox) still has more
    than 24h left before expiration, it is returned as-is with no new Google API
    call. Otherwise a new channel is created and any other active channels for
    the same (account, mailbox) are stopped via Google's `channels/stop` API and
    marked ``stopped`` — without this, a caller re-registering on every poll
    (e.g. the 15-min fallback poller) mints a fresh, never-cleaned-up channel
    every time.

    A random high-entropy channel token is stored only as a SHA-256 digest. The
    callback later resolves account/provider ownership from our channel record;
    no tenant identity supplied by the caller is trusted.
    """
    from config import get_settings

    settings = get_settings()
    expected_url = f"{settings.api_base_url.rstrip('/')}/api/calendar/webhooks/google"
    parsed = urlsplit(webhook_url)
    expected = urlsplit(expected_url)
    if (
        parsed.scheme != expected.scheme
        or parsed.netloc != expected.netloc
        or parsed.path != expected.path
        or parsed.query
        or parsed.fragment
        or (parsed.scheme != "https" and settings.app_env.strip().lower() not in {"development", "test"})
    ):
        raise ValueError("webhook_url must be the configured OutFlo calendar callback")

    credential = await _get_calendar_credential(account_id)
    if not credential:
        logger.warning(
            f"Cannot register calendar watch — no valid token for account_id={account_id}"
        )
        return None
    email_account, access_token = credential
    provider_account_id = str(email_account["_id"])

    now = datetime.now(timezone.utc)
    existing = await database.calendar_webhook_channels_collection.find_one({
        "account_id": str(account_id),
        "provider_account_id": provider_account_id,
        "status": "active",
        "expires_at": {"$gt": now + timedelta(hours=24)},
    })
    if existing:
        return {
            "id": existing["channel_id"],
            "resource_id": existing.get("resource_id"),
            "expiration": existing["expires_at"].isoformat(),
            "status": "active",
        }

    channel_id = str(uuid4())
    channel_token = secrets.token_urlsafe(32)
    token_digest = hashlib.sha256(channel_token.encode("utf-8")).hexdigest()

    channel_body = {
        "id": channel_id,
        "type": "web_hook",
        "address": webhook_url,
        "token": channel_token,
    }

    channel_doc = {
        "channel_id": channel_id,
        "channel_token_hash": token_digest,
        "account_id": str(account_id),
        "provider": "google",
        "provider_account_id": provider_account_id,
        "resource_id": None,
        "status": "pending",
        "last_message_number": 0,
        "created_at": now,
        # Pending rows cannot accumulate forever if the provider request dies.
        "expires_at": now + timedelta(hours=1),
    }
    await database.calendar_webhook_channels_collection.insert_one(channel_doc)

    try:
        async with httpx.AsyncClient(timeout=15) as client:
            resp = await client.post(
                f"{CALENDAR_API_BASE}/calendars/primary/events/watch",
                headers={
                    "Authorization": f"Bearer {access_token}",
                    "Content-Type": "application/json",
                },
                json=channel_body,
            )

        if resp.status_code not in (200, 201):
            logger.warning(
                f"Calendar watch registration failed for account_id={account_id}: "
                f"HTTP {resp.status_code}: {resp.text}"
            )
            await database.calendar_webhook_channels_collection.delete_one(
                {"channel_id": channel_id, "status": "pending"}
            )
            return None

        provider_channel = resp.json()
        resource_id = provider_channel.get("resourceId")
        response_channel_id = provider_channel.get("id")
        if response_channel_id != channel_id or not resource_id:
            logger.error("Calendar watch response did not match the registered channel")
            await database.calendar_webhook_channels_collection.delete_one(
                {"channel_id": channel_id, "status": "pending"}
            )
            return None

        try:
            expiration = datetime.fromtimestamp(
                int(provider_channel.get("expiration")) / 1000,
                tz=timezone.utc,
            )
        except (TypeError, ValueError, OSError):
            expiration = now + timedelta(days=7)

        updated = await database.calendar_webhook_channels_collection.update_one(
            {
                "channel_id": channel_id,
                "account_id": str(account_id),
                "provider_account_id": provider_account_id,
                "status": "pending",
            },
            {
                "$set": {
                    "resource_id": str(resource_id),
                    "resource_uri": provider_channel.get("resourceUri"),
                    "status": "active",
                    "expires_at": expiration,
                    "updated_at": datetime.now(timezone.utc),
                }
            },
        )
        if updated.matched_count != 1:
            logger.error("Calendar watch ownership changed during registration")
            return None

        logger.info(
            f"Registered Google Calendar push channel for account_id={account_id}"
        )

        await _stop_superseded_channels(
            account_id=str(account_id),
            provider_account_id=provider_account_id,
            keep_channel_id=channel_id,
            access_token=access_token,
        )
        await _cleanup_stale_channels()

        # Never expose the provider callback token or its digest.
        return {
            "id": channel_id,
            "resource_id": str(resource_id),
            "expiration": expiration.isoformat(),
            "status": "active",
        }

    except Exception as exc:
        await database.calendar_webhook_channels_collection.delete_one(
            {"channel_id": channel_id, "status": "pending"}
        )
        logger.warning(
            f"Calendar watch registration error for account_id={account_id}: {exc}"
        )
        return None


async def _stop_superseded_channels(
    *,
    account_id: str,
    provider_account_id: str,
    keep_channel_id: str,
    access_token: str,
) -> None:
    """Stop and mark ``stopped`` any other active channel for this (account, mailbox).

    Best-effort: a failed `channels/stop` call just leaves that channel to
    expire on its own 7-day TTL instead of blocking the new registration.
    """
    cursor = database.calendar_webhook_channels_collection.find({
        "account_id": account_id,
        "provider_account_id": provider_account_id,
        "status": "active",
        "channel_id": {"$ne": keep_channel_id},
    })
    async for doc in cursor:
        stopped = await _stop_google_channel(access_token, doc["channel_id"], doc.get("resource_id"))
        await database.calendar_webhook_channels_collection.update_one(
            {"_id": doc["_id"], "status": "active"},
            {"$set": {
                "status": "stopped" if stopped else "stop_failed",
                "stopped_at": datetime.now(timezone.utc),
            }},
        )


async def _stop_google_channel(access_token: str, channel_id: str, resource_id: Optional[str]) -> bool:
    """Best-effort call to Google's `channels/stop`. Returns whether it succeeded."""
    if not resource_id:
        return False
    try:
        async with httpx.AsyncClient(timeout=15) as client:
            resp = await client.post(
                f"{CALENDAR_API_BASE}/channels/stop",
                headers={
                    "Authorization": f"Bearer {access_token}",
                    "Content-Type": "application/json",
                },
                json={"id": channel_id, "resourceId": resource_id},
            )
        if resp.status_code in (200, 204):
            return True
        # A 404 means Google already dropped the channel — treat as stopped.
        if resp.status_code == 404:
            return True
        logger.debug(
            f"channels/stop returned HTTP {resp.status_code} for channel {channel_id}: {resp.text}"
        )
        return False
    except Exception as exc:
        logger.debug(f"channels/stop failed for channel {channel_id}: {exc}")
        return False


async def _cleanup_stale_channels() -> None:
    """Opportunistically purge channel docs that expired more than 7 days ago.

    The `calendar_channel_expiry_ttl_idx` TTL index already reaps documents at
    `expires_at`; this is a belt-and-braces sweep for stragglers, not the
    primary cleanup path.
    """
    cutoff = datetime.now(timezone.utc) - timedelta(days=7)
    try:
        await database.calendar_webhook_channels_collection.delete_many(
            {"expires_at": {"$lt": cutoff}}
        )
    except Exception as exc:
        logger.debug(f"Stale calendar channel cleanup failed: {exc}")


# ---------------------------------------------------------------------------
# Backwards-compatible stub (used by meeting_service before this was wired up)
# ---------------------------------------------------------------------------

async def get_freebusy(account_id: str, start: datetime, end: datetime) -> list:
    """
    Return busy intervals for the account's primary calendar between start and end.
    Returns a list of {"start": iso_str, "end": iso_str} dicts, or [] on failure.
    """
    access_token = await _get_calendar_access_token(account_id)
    if not access_token:
        return []

    try:
        async with httpx.AsyncClient(timeout=15) as client:
            resp = await client.post(
                f"{CALENDAR_API_BASE}/freeBusy",
                headers={
                    "Authorization": f"Bearer {access_token}",
                    "Content-Type": "application/json",
                },
                json={
                    "timeMin": start.strftime("%Y-%m-%dT%H:%M:%SZ"),
                    "timeMax": end.strftime("%Y-%m-%dT%H:%M:%SZ"),
                    "items": [{"id": "primary"}],
                },
            )

        if resp.status_code != 200:
            logger.warning(
                f"get_freebusy failed for account_id={account_id}: "
                f"HTTP {resp.status_code}: {resp.text}"
            )
            return []

        return _parse_busy_intervals(resp.json())

    except Exception as exc:
        logger.warning(f"get_freebusy error for account_id={account_id}: {exc}")
        return []
