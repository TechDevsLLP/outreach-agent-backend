"""
Google Calendar service.
Uses the same httpx + OAuth token pattern as gmail_service.py.
"""

import asyncio
import logging
from datetime import datetime, timedelta, timezone
from typing import Optional
from uuid import uuid4

import httpx

import database
from services.gmail_service import refresh_token_if_needed

logger = logging.getLogger(__name__)

CALENDAR_API_BASE = "https://www.googleapis.com/calendar/v3"


# ---------------------------------------------------------------------------
# Internal: token retrieval
# ---------------------------------------------------------------------------

async def _get_calendar_access_token(account_id: str) -> Optional[str]:
    """
    Look up the Gmail email account for account_id that has calendar scope,
    refresh the token if needed, and return a valid access token.
    Returns None if no matching account exists or refresh fails.
    """
    email_account = await database.email_accounts_collection.find_one({
        "account_id": account_id,
        "provider": "gmail",
        "oauth_scopes": {"$elemMatch": {"$regex": "calendar"}},
    })
    if not email_account:
        logger.warning(
            f"No Gmail account with calendar scope found for account_id={account_id}"
        )
        return None

    return await refresh_token_if_needed(email_account)


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


async def create_event(account_id: str, event_data: dict) -> Optional[dict]:
    """
    Create a Google Calendar event on the account's primary calendar.

    Returns the created event dict (contains `id`, `htmlLink`, `status`) or None on failure.
    Pass conferenceData in event_data to request a Google Meet link; this function
    automatically appends conferenceDataVersion=1 to the query.
    """
    access_token = await _get_calendar_access_token(account_id)
    if not access_token:
        logger.warning(
            f"Cannot create calendar event — no valid token for account_id={account_id}"
        )
        return None

    try:
        async with httpx.AsyncClient(timeout=30) as client:
            resp = await client.post(
                f"{CALENDAR_API_BASE}/calendars/primary/events",
                headers={
                    "Authorization": f"Bearer {access_token}",
                    "Content-Type": "application/json",
                },
                params={"conferenceDataVersion": "1"},
                json=event_data,
            )

        if resp.status_code not in (200, 201):
            logger.warning(
                f"Calendar create_event failed for account_id={account_id}: "
                f"HTTP {resp.status_code}: {resp.text}"
            )
            return None

        return resp.json()

    except Exception as exc:
        logger.warning(
            f"Calendar create_event error for account_id={account_id}: {exc}"
        )
        return None


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

    The channel token is set to account_id so we can identify the account in webhook callbacks.
    Returns the channel resource dict on success, or None on failure.
    """
    access_token = await _get_calendar_access_token(account_id)
    if not access_token:
        logger.warning(
            f"Cannot register calendar watch — no valid token for account_id={account_id}"
        )
        return None

    channel_body = {
        "id": str(uuid4()),
        "type": "web_hook",
        "address": webhook_url,
        "token": account_id,
    }

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
            return None

        logger.info(
            f"Registered Google Calendar push channel for account_id={account_id}"
        )
        return resp.json()

    except Exception as exc:
        logger.warning(
            f"Calendar watch registration error for account_id={account_id}: {exc}"
        )
        return None


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
