"""
Meeting Service.
Handles meeting proposal flow: generate time slots, record meeting doc,
send reply with booking link, and transition enrollment status.
"""

import hashlib
import json
import logging
import os
import socket
import uuid
from datetime import datetime, timedelta, timezone

from bson import ObjectId
from pymongo import ReturnDocument
from pymongo.errors import DuplicateKeyError

import database
from services.conversation_service import send_reply
from services.conversation_service import _account_filter

logger = logging.getLogger(__name__)

BOOKING_BASE_URL = "https://book.outflo.io/book"


class MeetingConfirmationInProgress(RuntimeError):
    """The exact same booking operation is already crossing the provider boundary."""


CALENDAR_SYNC_STATUSES = ("booked", "rescheduled")
CALENDAR_SYNC_LEASE_SECONDS = 90
CALENDAR_SYNC_INTERVAL_SECONDS = 600


def _event_datetime(event: dict, field: str) -> str | None:
    value = event.get(field)
    if not isinstance(value, dict):
        return None
    result = value.get("dateTime") or value.get("date")
    return str(result) if result else None


def _normalize_email(value: str | None) -> str:
    return str(value or "").strip().lower()


async def _campaign_calendar_account_id(meeting: dict, account_id: str) -> str | None:
    """Resolve the campaign's explicitly selected mailbox, if one exists."""
    existing = str(meeting.get("calendar_provider_account_id") or "").strip()
    if existing:
        return existing
    enrollment_id = meeting.get("enrollment_id")
    if not enrollment_id:
        return None
    try:
        enrollment_oid = ObjectId(str(enrollment_id))
    except Exception:
        return None
    enrollment = await database.campaign_enrollments_collection.find_one(
        {"_id": enrollment_oid, **_account_filter(account_id)},
        {"campaign_id": 1},
    )
    campaign_id = (enrollment or {}).get("campaign_id")
    if not campaign_id:
        return None
    try:
        campaign_oid = campaign_id if isinstance(campaign_id, ObjectId) else ObjectId(str(campaign_id))
    except Exception:
        return None
    campaign = await database.campaigns_collection.find_one(
        {"_id": campaign_oid, **_account_filter(account_id)},
        {"email_account_id": 1},
    )
    sender_id = (campaign or {}).get("email_account_id")
    if not sender_id:
        return None
    try:
        sender_oid = sender_id if isinstance(sender_id, ObjectId) else ObjectId(str(sender_id))
    except Exception:
        return None
    google_sender = await database.email_accounts_collection.find_one(
        {
            "_id": sender_oid,
            "account_id": account_id,
            "provider": "google",
            "oauth_scopes": {"$elemMatch": {"$regex": "calendar"}},
            "status": {"$in": ["connected", "active"]},
        },
        {"_id": 1},
    )
    return str(google_sender["_id"]) if google_sender else None


def _next_business_days(n: int = 3) -> list[datetime]:
    """Return the next n business days (Mon–Fri) starting from tomorrow."""
    results = []
    cursor = datetime.utcnow().replace(hour=0, minute=0, second=0, microsecond=0) + timedelta(days=1)
    while len(results) < n:
        if cursor.weekday() < 5:  # 0=Mon … 4=Fri
            results.append(cursor)
        cursor += timedelta(days=1)
    return results


def _build_proposed_slots() -> list[dict]:
    """Return 3 proposed slots across the next business days at 10am, 2pm, 4pm UTC."""
    business_days = _next_business_days(3)
    hours = [10, 14, 16]
    slots = []
    for i, bday in enumerate(business_days):
        hour = hours[i % len(hours)]
        slot_dt = bday.replace(hour=hour, minute=0, second=0, microsecond=0)
        label = slot_dt.strftime("%-d %b at %-I:%M %p UTC")  # e.g. "9 Jun at 10:00 AM UTC"
        slots.append({
            "label": label,
            "datetime_iso": slot_dt.isoformat() + "Z",
        })
    return slots


def _build_calendar_event(
    meeting: dict,
    slot: dict,
    prospect_email: str,
    agenda: str,
    idempotency_key: str,
) -> dict:
    """Build a Google Calendar event payload from meeting + slot data."""
    start_iso = slot.get("datetime_iso", "")
    # Parse start time and compute end (+25 minutes)
    try:
        start_dt = datetime.fromisoformat(start_iso.rstrip("Z"))
        end_dt = start_dt + timedelta(minutes=25)
        end_iso = end_dt.isoformat() + "Z"
    except Exception:
        end_iso = start_iso  # fallback

    attendees = []
    if prospect_email:
        attendees.append({"email": prospect_email})

    event = {
        # Google accepts caller-selected base32hex event IDs. A SHA-256 hex
        # prefix is a valid subset and makes insert/retry provider-idempotent.
        "id": hashlib.sha256(f"outflo:{idempotency_key}".encode()).hexdigest()[:32],
        "summary": agenda or "25-minute discovery call",
        "description": agenda or "Discovery call",
        "start": {"dateTime": start_iso, "timeZone": "UTC"},
        "end": {"dateTime": end_iso, "timeZone": "UTC"},
        "attendees": attendees,
        "reminders": {"useDefault": True},
        "conferenceData": {
            "createRequest": {
                "requestId": idempotency_key,
                "conferenceSolutionKey": {"type": "hangoutsMeet"},
            }
        },
    }
    return event


def _build_reply_text(
    prospect_name: str,
    slots: list[dict],
    booking_token: str,
    campaign_cta_url: str | None = None,
) -> str:
    """Compose the meeting-proposal reply.

    Prefers the campaign's configured CTA URL so the prospect lands on the
    booking page the user actually set up; falls back to the hosted OutFlo
    booking page tied to this proposal's token.
    """
    name_part = prospect_name if prospect_name and prospect_name != "there" else "there"
    slot_lines = "\n".join(f"  • {s['label']}" for s in slots)
    booking_url = (campaign_cta_url or "").strip() or f"{BOOKING_BASE_URL}/{booking_token}"
    return (
        f"Hi {name_part},\n\n"
        f"Thanks for your interest, great to hear from you!\n\n"
        f"I'd love to connect. Here are 3 times that work for me:\n\n"
        f"{slot_lines}\n\n"
        f"Or pick any time that works best for you:\n"
        f"{booking_url}\n\n"
        f"Looking forward to connecting!"
    )


async def propose_meeting(
    enrollment_id: str,
    conversation_id: str,
    account_id: str,
    prospect_id: str,
    company_profile: dict,
    prospect_name: str,
    company_name: str,
    message_text: str,
    conversation_context: str,
    draft_only: bool = True,
) -> dict:
    """
    Record a meeting proposal with 3 time slots and update enrollment status.

    ``draft_only`` defaults to True: the reply is stored on the conversation as
    ``ai_draft_reply`` (status "pending") for human approval and
    ``proposal_reply_state`` stays "prepared", so approve_draft completes the
    dispatch later. Nothing reaches a prospect without explicit approval.
    Pass draft_only=False only for a deliberate immediate send.

    Returns the inserted meetings document as a dict.
    """
    account_id = str(account_id or "").strip()
    if not account_id:
        raise ValueError("account_id is required for meeting creation")

    try:
        enrollment_object_id = ObjectId(enrollment_id)
        conversation_object_id = ObjectId(conversation_id)
    except Exception as exc:
        raise ValueError("Invalid enrollment or conversation ID") from exc

    # Resolve both mutable parents through the tenant boundary before creating
    # a public booking token or sending a provider message.
    enrollment = await database.campaign_enrollments_collection.find_one(
        {"_id": enrollment_object_id, **_account_filter(account_id)},
        {"_id": 1, "prospect_id": 1},
    )
    conversation = await database.conversations_collection.find_one(
        {"_id": conversation_object_id, **_account_filter(account_id)},
        {"_id": 1, "prospect_id": 1},
    )
    if not enrollment or not conversation:
        raise ValueError("Tenant-owned enrollment and conversation are required")
    if enrollment.get("prospect_id") and str(enrollment["prospect_id"]) != str(prospect_id):
        raise ValueError("Enrollment prospect does not match meeting prospect")
    if conversation.get("prospect_id") and str(conversation["prospect_id"]) != str(prospect_id):
        raise ValueError("Conversation prospect does not match meeting prospect")

    now = datetime.utcnow()
    proposal_key = f"{account_id}:{enrollment_id}"
    proposed_slots = _build_proposed_slots()

    existing_meeting = await database.meetings_collection.find_one(
        {"proposal_key": proposal_key, **_account_filter(account_id)}
    )

    # Try real calendar slots (falls back to deterministic if not connected)
    if not existing_meeting:
        from services.calendar_service import propose_three_slots
        try:
            real_slots = await propose_three_slots(account_id=account_id, duration_minutes=25)
            if real_slots:
                proposed_slots = real_slots
        except Exception:
            pass  # Keep deterministic slots

    # 1. Idempotently create one proposal per tenant+enrollment. Concurrent
    # callers converge on the unique proposal_key and reuse the same token.
    inserted_booking_token = str(uuid.uuid4())
    try:
        meeting_doc = existing_meeting or await database.meetings_collection.find_one_and_update(
            {"proposal_key": proposal_key},
            {
                "$setOnInsert": {
                    "proposal_key": proposal_key,
                    "account_id": account_id,
                    "enrollment_id": enrollment_id,
                    "prospect_id": prospect_id,
                    "conversation_id": conversation_id,
                    "prospect_name": prospect_name,
                    "company_name": company_name,
                    "status": "proposed",
                    "booking_token": inserted_booking_token,
                    "booking_expires_at": now + timedelta(days=30),
                    "proposed_slots": proposed_slots,
                    "confirmed_slot_index": None,
                    "calendar_event_id": None,
                    "calendar_event_link": None,
                    "booked_at": None,
                    "proposal_reply_state": "prepared",
                    "proposed_at": now,
                    "created_at": now,
                    "updated_at": now,
                }
            },
            upsert=True,
            return_document=ReturnDocument.AFTER,
        )
    except DuplicateKeyError:
        meeting_doc = await database.meetings_collection.find_one(
            {"proposal_key": proposal_key, **_account_filter(account_id)}
        )
    if not meeting_doc or str(meeting_doc.get("account_id")) != account_id:
        raise RuntimeError("Unable to create tenant-scoped meeting proposal")

    meeting_id = meeting_doc["_id"]
    booking_token = meeting_doc["booking_token"]
    proposed_slots = meeting_doc.get("proposed_slots") or proposed_slots

    # 2. Claim the proposal reply once. Once dispatch begins, failures are
    # quarantined as ambiguous instead of blindly sending a duplicate reply.
    # Use the campaign's own CTA URL when the user configured one.
    campaign_cta_url = None
    try:
        enrollment_doc = await database.campaign_enrollments_collection.find_one(
            {"_id": enrollment_object_id, **_account_filter(account_id)},
            {"campaign_id": 1},
        )
        if enrollment_doc and enrollment_doc.get("campaign_id"):
            campaign_doc = await database.campaigns_collection.find_one(
                {"_id": ObjectId(str(enrollment_doc["campaign_id"]))},
                {"cta_url": 1, "cta_type": 1},
            )
            if campaign_doc and (campaign_doc.get("cta_url") or "").strip():
                campaign_cta_url = campaign_doc["cta_url"].strip()
    except Exception as exc:
        logger.warning("Could not resolve campaign CTA URL for enrollment %s: %s",
                       enrollment_id, exc)

    reply_text = _build_reply_text(
        meeting_doc.get("prospect_name") or prospect_name,
        proposed_slots,
        booking_token,
        campaign_cta_url=campaign_cta_url,
    )
    reply_operation_id = hashlib.sha256(proposal_key.encode()).hexdigest()

    if draft_only:
        # Store the proposal as a pending draft instead of sending it. The
        # meeting, its slots and its booking token are still created, so
        # approving the draft sends exactly the copy shown for review.
        import uuid as _uuid
        await database.conversations_collection.update_one(
            {"_id": conversation_object_id, **_account_filter(account_id)},
            {"$set": {
                "ai_draft_reply": {
                    "draft_id": str(_uuid.uuid4()),
                    "draft_text": reply_text,
                    "generated_at": now,
                    "status": "pending",
                    "conversation_id": conversation_id,
                    "in_response_to": (message_text or "")[:200],
                    "source": "meeting_proposal",
                    "meeting_id": str(meeting_id),
                },
                "updated_at": now,
            }},
        )
        logger.info(
            "Meeting proposal drafted (awaiting approval) for enrollment %s", enrollment_id
        )
        claimed = None
    else:
        claimed = await database.meetings_collection.find_one_and_update(
            {
                "_id": meeting_id,
                **_account_filter(account_id),
                "proposal_reply_state": "prepared",
            },
            {
                "$set": {
                    "proposal_reply_state": "dispatching",
                    "proposal_reply_operation_id": reply_operation_id,
                    "updated_at": now,
                }
            },
            return_document=ReturnDocument.AFTER,
        )
    if claimed:
        try:
            await send_reply(
                conversation_id=conversation_id,
                content_text=reply_text,
                account_id=account_id,
            )
            await database.meetings_collection.update_one(
                {
                    "_id": meeting_id,
                    **_account_filter(account_id),
                    "proposal_reply_operation_id": reply_operation_id,
                    "proposal_reply_state": "dispatching",
                },
                {"$set": {"proposal_reply_state": "sent", "proposal_reply_sent_at": datetime.utcnow()}},
            )
            logger.info("Meeting proposal reply sent for enrollment %s", enrollment_id)
        except Exception as exc:
            await database.meetings_collection.update_one(
                {
                    "_id": meeting_id,
                    **_account_filter(account_id),
                    "proposal_reply_operation_id": reply_operation_id,
                    "proposal_reply_state": "dispatching",
                },
                {
                    "$set": {
                        "proposal_reply_state": "ambiguous",
                        "proposal_reply_error": str(exc)[:500],
                        "updated_at": datetime.utcnow(),
                    }
                },
            )
            logger.error(
                "Meeting proposal reply has ambiguous provider outcome for enrollment %s",
                enrollment_id,
                exc_info=True,
            )

    # 3. Transition enrollment status
    if meeting_doc.get("status") in {"proposed", "rescheduling"}:
        try:
            await database.campaign_enrollments_collection.update_one(
                {
                    "_id": ObjectId(enrollment_id),
                    **_account_filter(account_id),
                    "status": {"$nin": ["meeting_booked", "completed", "opted_out"]},
                },
                {
                    "$set": {
                        "status": "meeting_proposed",
                        "meeting_id": str(meeting_id),
                        "last_activity_at": now,
                    }
                },
            )
        except Exception as exc:
            logger.error(f"Failed to update enrollment {enrollment_id} to meeting_proposed: {exc}", exc_info=True)

    # Return serialisable dict
    result = dict(meeting_doc)
    result["_id"] = str(meeting_id)
    return result


async def confirm_slot_and_send_invite(
    meeting_id: str,
    slot_index: int,
    account_id: str,
    prospect_email: str = "",
    agenda: str = "",
) -> dict:
    """
    Confirm a proposed slot, create a Google Calendar event, and transition enrollment
    to meeting_booked.
    """
    account_id = str(account_id or "").strip()
    if not account_id:
        raise ValueError("account_id is required for meeting confirmation")
    try:
        meeting_object_id = ObjectId(meeting_id)
    except Exception as exc:
        raise ValueError("Invalid meeting ID") from exc

    # 1. Find meeting doc
    meeting = await database.meetings_collection.find_one(
        {"_id": meeting_object_id, **_account_filter(account_id)}
    )
    if not meeting:
        raise ValueError(f"Meeting not found: {meeting_id}")

    # 2. Validate slot_index
    proposed_slots = meeting.get("proposed_slots", [])
    if slot_index < 0 or slot_index >= len(proposed_slots):
        raise ValueError(f"slot_index {slot_index} is out of range (meeting has {len(proposed_slots)} slots)")

    if meeting.get("status") == "booked":
        if meeting.get("confirmed_slot_index") == slot_index:
            result = dict(meeting)
            result["_id"] = str(result["_id"])
            return result
        raise ValueError("Meeting is already booked for a different slot")

    operation_id = hashlib.sha256(
        f"{account_id}:{meeting_id}:{slot_index}".encode()
    ).hexdigest()[:32]
    now = datetime.utcnow()
    lease_expires_at = now + timedelta(minutes=5)

    # 3. Atomically claim the exact booking operation. Concurrent/replayed
    # confirmations cannot make a second provider call.
    claimed = await database.meetings_collection.find_one_and_update(
        {
            "_id": meeting_object_id,
            **_account_filter(account_id),
            "$and": [
                {
                    "$or": [
                        {"status": {"$in": ["proposed", "rescheduling", "booking_failed"]}},
                        {
                            "status": "booking",
                            "booking_lease_expires_at": {"$lte": now},
                        },
                    ]
                },
                {
                    "$or": [
                        {"confirmed_slot_index": None},
                        {"confirmed_slot_index": slot_index},
                    ]
                },
            ],
        },
        {
            "$set": {
                "status": "booking",
                "confirmed_slot_index": slot_index,
                "booking_operation_id": operation_id,
                "booking_lease_expires_at": lease_expires_at,
                "updated_at": now,
            }
        },
        return_document=ReturnDocument.AFTER,
    )
    if not claimed:
        current = await database.meetings_collection.find_one(
            {"_id": meeting_object_id, **_account_filter(account_id)}
        )
        if current and current.get("status") == "booked" and current.get("confirmed_slot_index") == slot_index:
            result = dict(current)
            result["_id"] = str(result["_id"])
            return result
        if current and current.get("status") == "booking" and current.get("confirmed_slot_index") == slot_index:
            raise MeetingConfirmationInProgress("Meeting confirmation is already in progress")
        raise ValueError("Meeting cannot be confirmed in its current state")

    # 4. Try to create Google Calendar event
    try:
        from services.calendar_service import create_event
        slot = proposed_slots[slot_index]
        event_data = _build_calendar_event(
            meeting,
            slot,
            prospect_email,
            agenda,
            idempotency_key=operation_id,
        )
        preferred_provider_account_id = await _campaign_calendar_account_id(
            meeting, account_id
        )
        cal_event = await create_event(
            account_id,
            event_data,
            provider_account_id=preferred_provider_account_id,
            calendar_id=str(meeting.get("calendar_id") or "primary"),
        )
        if not cal_event or cal_event.get("id") != event_data["id"]:
            raise RuntimeError("Calendar provider did not create an event")
        bound_provider_account_id = str(
            cal_event.get("_outflo_provider_account_id") or ""
        ).strip()
        bound_calendar_id = str(cal_event.get("_outflo_calendar_id") or "").strip()
        if not bound_provider_account_id or not bound_calendar_id:
            raise RuntimeError("Calendar provider did not return an exact event binding")
    except Exception as exc:
        await database.meetings_collection.update_one(
            {
                "_id": meeting_object_id,
                **_account_filter(account_id),
                "booking_operation_id": operation_id,
                "status": "booking",
            },
            {
                "$set": {
                    "status": "booking_failed",
                    "booking_error": str(exc)[:500],
                    "updated_at": datetime.utcnow(),
                }
            },
        )
        logger.error("Failed to create calendar event for meeting %s", meeting_id, exc_info=True)
        raise RuntimeError("Failed to create calendar event") from exc

    booked_at = datetime.utcnow()
    finalized = await database.meetings_collection.find_one_and_update(
        {
            "_id": meeting_object_id,
            **_account_filter(account_id),
            "booking_operation_id": operation_id,
            "status": "booking",
        },
        {
            "$set": {
                "status": "booked",
                "calendar_event_id": cal_event.get("id"),
                "calendar_event_link": cal_event.get("htmlLink"),
                "calendar_provider": "google",
                "calendar_provider_account_id": bound_provider_account_id,
                "calendar_id": bound_calendar_id,
                "calendar_attendee_email": _normalize_email(prospect_email),
                "calendar_attendee_status": "needsAction",
                "calendar_start_at": _event_datetime(cal_event, "start")
                or slot.get("datetime_iso"),
                "calendar_end_at": _event_datetime(cal_event, "end"),
                "next_calendar_sync_at": booked_at,
                "booking_error": None,
                "booked_at": booked_at,
                "updated_at": booked_at,
            },
            "$unset": {"booking_lease_expires_at": ""},
        },
        return_document=ReturnDocument.AFTER,
    )
    if not finalized:
        raise RuntimeError("Meeting booking state changed during confirmation")

    # 5. Update enrollment status to meeting_booked
    enrollment_id = meeting.get("enrollment_id")
    if enrollment_id:
        try:
            await database.campaign_enrollments_collection.update_one(
                {"_id": ObjectId(enrollment_id), **_account_filter(account_id)},
                {"$set": {"status": "meeting_booked", "last_activity_at": now}},
            )
            # Increment the campaign counter exactly once per meeting. The
            # update pipeline remembers the meeting key and is replay-safe.
            enrollment = await database.campaign_enrollments_collection.find_one(
                {"_id": ObjectId(enrollment_id), **_account_filter(account_id)},
                {"campaign_id": 1},
            )
            campaign_id = enrollment.get("campaign_id") if enrollment else None
            if campaign_id:
                await database.campaigns_collection.update_one(
                    {"_id": campaign_id, **_account_filter(account_id)},
                    [
                        {
                            "$set": {
                                "meetings_booked": {
                                    "$cond": [
                                        {
                                            "$in": [
                                                meeting_id,
                                                {"$ifNull": ["$meetings_booked_keys", []]},
                                            ]
                                        },
                                        {"$ifNull": ["$meetings_booked", 0]},
                                        {"$add": [{"$ifNull": ["$meetings_booked", 0]}, 1]},
                                    ]
                                },
                                "meetings_booked_keys": {
                                    "$setUnion": [
                                        {"$ifNull": ["$meetings_booked_keys", []]},
                                        [meeting_id],
                                    ]
                                },
                            }
                        }
                    ],
                )
        except Exception as exc:
            logger.error(f"Failed to update enrollment {enrollment_id} to meeting_booked: {exc}", exc_info=True)

    # 6. Return updated meeting doc
    result = dict(finalized)
    result["_id"] = str(result["_id"])
    return result


def _calendar_event_projection(meeting: dict, event: dict, now: datetime) -> tuple[dict, bool]:
    """Return the deterministic local projection and whether it cancels."""
    attendees = event.get("attendees") if isinstance(event.get("attendees"), list) else []
    attendee_email = _normalize_email(meeting.get("calendar_attendee_email"))
    attendee_status = "unknown"
    if attendee_email:
        for attendee in attendees:
            if _normalize_email(attendee.get("email")) == attendee_email:
                attendee_status = str(attendee.get("responseStatus") or "needsAction")
                break

    missing = bool(event.get("_outflo_not_found"))
    provider_cancelled = str(event.get("status") or "").lower() == "cancelled"
    declined = attendee_status == "declined"
    cancelled = missing or provider_cancelled or declined
    start_at = _event_datetime(event, "start")
    end_at = _event_datetime(event, "end")
    time_changed = bool(
        start_at
        and meeting.get("calendar_start_at")
        and str(start_at) != str(meeting.get("calendar_start_at"))
    ) or bool(
        end_at
        and meeting.get("calendar_end_at")
        and str(end_at) != str(meeting.get("calendar_end_at"))
    )

    fingerprint_payload = {
        "id": event.get("id"),
        "status": event.get("status"),
        "updated": event.get("updated"),
        "start": start_at,
        "end": end_at,
        "attendee_email": attendee_email,
        "attendee_status": attendee_status,
        "missing": missing,
    }
    fingerprint = hashlib.sha256(
        json.dumps(fingerprint_payload, sort_keys=True, separators=(",", ":")).encode()
    ).hexdigest()

    if missing:
        reason = "provider_event_missing"
    elif provider_cancelled:
        reason = "provider_event_cancelled"
    elif declined:
        reason = "attendee_declined"
    else:
        reason = None

    status = "cancelled" if cancelled else ("rescheduled" if time_changed else "booked")
    projection: dict = {
        "status": status,
        "calendar_attendee_status": attendee_status,
        "calendar_event_status": str(event.get("status") or "unknown"),
        "calendar_provider_updated_at": event.get("updated"),
        "calendar_sync_fingerprint": fingerprint,
        "last_calendar_sync_at": now,
        "next_calendar_sync_at": now + timedelta(seconds=CALENDAR_SYNC_INTERVAL_SECONDS),
        "calendar_sync_error": None,
        "updated_at": now,
    }
    if start_at:
        projection["calendar_start_at"] = start_at
    if end_at:
        projection["calendar_end_at"] = end_at
    if cancelled:
        projection.update(
            {
                "cancelled_at": now,
                "cancellation_reason": reason,
                "next_calendar_sync_at": None,
            }
        )
    elif time_changed:
        projection["rescheduled_at"] = now
    return projection, cancelled


async def _reactivate_cancelled_enrollment(meeting: dict, account_id: str, now: datetime) -> None:
    enrollment_id = meeting.get("enrollment_id")
    if not enrollment_id:
        return
    try:
        enrollment_oid = ObjectId(str(enrollment_id))
    except Exception:
        return
    await database.campaign_enrollments_collection.update_one(
        {
            "_id": enrollment_oid,
            **_account_filter(account_id),
            "status": "meeting_booked",
            "meeting_id": {"$in": [str(meeting["_id"]), meeting["_id"]]},
        },
        {
            "$set": {
                "status": "active",
                "next_action_at": now + timedelta(hours=4),
                "meeting_id": None,
                "last_activity_at": now,
                "last_transition_reason": "meeting_booked_cancelled_by_calendar",
            }
        },
    )


async def sync_meeting_statuses(
    account_id: str,
    *,
    provider_account_id: str | None = None,
    max_meetings: int = 20,
    worker_id: str | None = None,
    event_fetcher=None,
) -> dict:
    """Boundedly reconcile tenant meetings with their exact Google events.

    Every row is claimed with a Mongo lease. Provider errors preserve the
    meeting state and schedule bounded retry; authoritative event state is
    applied only while the worker still owns the exact tenant/mailbox/calendar
    binding.
    """
    account_id = str(account_id or "").strip()
    if not account_id:
        raise ValueError("account_id is required for calendar reconciliation")
    max_meetings = max(1, min(int(max_meetings), 100))
    worker_id = worker_id or f"{socket.gethostname()}:{os.getpid()}:calendar"
    now = datetime.now(timezone.utc).replace(tzinfo=None)
    query: dict = {
        "account_id": account_id,
        "calendar_provider": "google",
        "calendar_provider_account_id": {"$type": "string"},
        "calendar_id": {"$type": "string"},
        "calendar_event_id": {"$type": "string"},
        "status": {"$in": list(CALENDAR_SYNC_STATUSES)},
        "$and": [
            {"$or": [
                {"next_calendar_sync_at": {"$exists": False}},
                {"next_calendar_sync_at": None},
                {"next_calendar_sync_at": {"$lte": now}},
            ]},
            {"$or": [
                {"calendar_sync_lease_expires_at": {"$exists": False}},
                {"calendar_sync_lease_expires_at": {"$lte": now}},
            ]},
        ],
    }
    if provider_account_id:
        query["calendar_provider_account_id"] = str(provider_account_id)

    candidates = await database.meetings_collection.find(query).sort(
        "next_calendar_sync_at", 1
    ).limit(max_meetings).to_list(max_meetings)
    fetch = event_fetcher
    if fetch is None:
        from services.calendar_service import get_event
        fetch = get_event

    result = {"claimed": 0, "updated": 0, "cancelled": 0, "failed": 0}
    for candidate in candidates:
        lease_expires_at = now + timedelta(seconds=CALENDAR_SYNC_LEASE_SECONDS)
        claim_query = {
            "_id": candidate["_id"],
            "account_id": account_id,
            "calendar_provider": "google",
            "calendar_provider_account_id": candidate.get("calendar_provider_account_id"),
            "calendar_id": candidate.get("calendar_id"),
            "calendar_event_id": candidate.get("calendar_event_id"),
            "status": {"$in": list(CALENDAR_SYNC_STATUSES)},
            "$or": [
                {"calendar_sync_lease_expires_at": {"$exists": False}},
                {"calendar_sync_lease_expires_at": {"$lte": now}},
            ],
        }
        claimed = await database.meetings_collection.find_one_and_update(
            claim_query,
            {
                "$set": {
                    "calendar_sync_lease_owner": worker_id,
                    "calendar_sync_lease_expires_at": lease_expires_at,
                    "last_calendar_sync_attempt_at": now,
                },
                "$inc": {"calendar_sync_attempt_count": 1},
            },
            return_document=ReturnDocument.AFTER,
        )
        if not claimed:
            continue
        result["claimed"] += 1
        try:
            event = await fetch(
                account_id,
                provider_account_id=str(claimed["calendar_provider_account_id"]),
                calendar_id=str(claimed["calendar_id"]),
                event_id=str(claimed["calendar_event_id"]),
            )
            if str(event.get("id") or "") != str(claimed["calendar_event_id"]):
                raise RuntimeError("Calendar provider returned a mismatched event")
            projection, cancelled = _calendar_event_projection(claimed, event, now)
            fingerprint = projection["calendar_sync_fingerprint"]
            applied = await database.meetings_collection.update_one(
                {
                    "_id": claimed["_id"],
                    "account_id": account_id,
                    "calendar_provider_account_id": claimed["calendar_provider_account_id"],
                    "calendar_id": claimed["calendar_id"],
                    "calendar_event_id": claimed["calendar_event_id"],
                    "calendar_sync_lease_owner": worker_id,
                    "calendar_sync_fingerprint": {"$ne": fingerprint},
                },
                {
                    "$set": projection,
                    "$unset": {
                        "calendar_sync_lease_owner": "",
                        "calendar_sync_lease_expires_at": "",
                    },
                },
            )
            if applied.modified_count:
                result["updated"] += 1
                if cancelled:
                    result["cancelled"] += 1
                    await _reactivate_cancelled_enrollment(claimed, account_id, now)
            else:
                await database.meetings_collection.update_one(
                    {
                        "_id": claimed["_id"],
                        "account_id": account_id,
                        "calendar_sync_lease_owner": worker_id,
                    },
                    {
                        "$set": {
                            "last_calendar_sync_at": now,
                            "next_calendar_sync_at": now + timedelta(
                                seconds=CALENDAR_SYNC_INTERVAL_SECONDS
                            ),
                        },
                        "$unset": {
                            "calendar_sync_lease_owner": "",
                            "calendar_sync_lease_expires_at": "",
                        },
                    },
                )
        except Exception as exc:
            result["failed"] += 1
            attempts = int(claimed.get("calendar_sync_attempt_count") or 1)
            retry_seconds = min(3600, 60 * (2 ** min(attempts - 1, 6)))
            await database.meetings_collection.update_one(
                {
                    "_id": claimed["_id"],
                    "account_id": account_id,
                    "calendar_sync_lease_owner": worker_id,
                },
                {
                    "$set": {
                        "calendar_sync_error": str(exc)[:500],
                        "next_calendar_sync_at": now + timedelta(seconds=retry_seconds),
                        "updated_at": now,
                    },
                    "$unset": {
                        "calendar_sync_lease_owner": "",
                        "calendar_sync_lease_expires_at": "",
                    },
                },
            )
    return result
