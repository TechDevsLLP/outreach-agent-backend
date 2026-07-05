"""
Meeting Service.
Handles meeting proposal flow: generate time slots, record meeting doc,
send reply with booking link, and transition enrollment status.
"""

import logging
import uuid
from datetime import datetime, timedelta

from bson import ObjectId

import database
from services.conversation_service import send_reply

logger = logging.getLogger(__name__)

BOOKING_BASE_URL = "https://book.outflo.io/book"


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


def _build_calendar_event(meeting: dict, slot: dict, prospect_email: str, agenda: str) -> dict:
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

    return {
        "summary": agenda or "25-minute discovery call",
        "description": agenda or "Discovery call",
        "start": {"dateTime": start_iso, "timeZone": "UTC"},
        "end": {"dateTime": end_iso, "timeZone": "UTC"},
        "attendees": attendees,
        "reminders": {"useDefault": True},
    }


def _build_reply_text(
    prospect_name: str,
    slots: list[dict],
    booking_token: str,
) -> str:
    name_part = prospect_name if prospect_name and prospect_name != "there" else "there"
    slot_lines = "\n".join(f"  • {s['label']}" for s in slots)
    booking_url = f"{BOOKING_BASE_URL}/{booking_token}"
    return (
        f"Hi {name_part},\n\n"
        f"Thanks for your interest — great to hear from you!\n\n"
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
) -> dict:
    """
    Record a meeting proposal, send reply with 3 time slots, and update enrollment status.

    Returns the inserted meetings document as a dict.
    """
    now = datetime.utcnow()
    booking_token = str(uuid.uuid4())
    proposed_slots = _build_proposed_slots()

    # Try real calendar slots (falls back to deterministic if not connected)
    from services.calendar_service import propose_three_slots
    try:
        real_slots = await propose_three_slots(account_id=account_id, duration_minutes=25)
        if real_slots:
            proposed_slots = real_slots
    except Exception:
        pass  # Keep deterministic slots

    # 1. Insert meeting document
    meeting_doc = {
        "account_id": account_id,
        "enrollment_id": enrollment_id,
        "prospect_id": prospect_id,
        "conversation_id": conversation_id,
        "status": "proposed",
        "booking_token": booking_token,
        "proposed_slots": proposed_slots,
        "confirmed_slot_index": None,
        "calendar_event_id": None,
        "calendar_event_link": None,
        "booked_at": None,
        "created_at": now,
        "updated_at": now,
    }
    insert_result = await database.meetings_collection.insert_one(meeting_doc)
    meeting_id = insert_result.inserted_id
    meeting_doc["_id"] = meeting_id

    # 2. Build and send reply
    reply_text = _build_reply_text(prospect_name, proposed_slots, booking_token)
    try:
        await send_reply(conversation_id=conversation_id, content_text=reply_text)
        logger.info(f"Meeting proposal reply sent for enrollment {enrollment_id} (meeting {meeting_id})")
    except Exception as exc:
        logger.error(f"Failed to send meeting proposal reply for enrollment {enrollment_id}: {exc}", exc_info=True)

    # 3. Transition enrollment status
    try:
        await database.campaign_enrollments_collection.update_one(
            {"_id": ObjectId(enrollment_id)},
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
    prospect_email: str = "",
    agenda: str = "",
) -> dict:
    """
    Confirm a proposed slot, create a Google Calendar event, and transition enrollment
    to meeting_booked.
    """
    now = datetime.utcnow()

    # 1. Find meeting doc
    meeting = await database.meetings_collection.find_one({"_id": ObjectId(meeting_id)})
    if not meeting:
        raise ValueError(f"Meeting not found: {meeting_id}")

    # 2. Validate slot_index
    proposed_slots = meeting.get("proposed_slots", [])
    if slot_index < 0 or slot_index >= len(proposed_slots):
        raise ValueError(f"slot_index {slot_index} is out of range (meeting has {len(proposed_slots)} slots)")

    # 3. Update meeting to booked
    await database.meetings_collection.update_one(
        {"_id": ObjectId(meeting_id)},
        {
            "$set": {
                "status": "booked",
                "confirmed_slot_index": slot_index,
                "booked_at": now,
                "updated_at": now,
            }
        },
    )

    # 4. Try to create Google Calendar event
    try:
        from services.calendar_service import create_event
        account_id = meeting.get("account_id", "")
        slot = proposed_slots[slot_index]
        event_data = _build_calendar_event(meeting, slot, prospect_email, agenda)
        cal_event = await create_event(account_id, event_data)
        if cal_event:
            await database.meetings_collection.update_one(
                {"_id": ObjectId(meeting_id)},
                {
                    "$set": {
                        "calendar_event_id": cal_event.get("id"),
                        "calendar_event_link": cal_event.get("htmlLink"),
                    }
                },
            )
            meeting["calendar_event_id"] = cal_event.get("id")
            meeting["calendar_event_link"] = cal_event.get("htmlLink")
    except Exception as exc:
        logger.error(f"Failed to create calendar event for meeting {meeting_id}: {exc}", exc_info=True)

    # 5. Update enrollment status to meeting_booked
    enrollment_id = meeting.get("enrollment_id")
    if enrollment_id:
        try:
            await database.campaign_enrollments_collection.update_one(
                {"_id": ObjectId(enrollment_id)},
                {"$set": {"status": "meeting_booked", "last_activity_at": now}},
            )
            # Increment campaign meetings_booked counter
            enrollment = await database.campaign_enrollments_collection.find_one(
                {"_id": ObjectId(enrollment_id)}, {"campaign_id": 1}
            )
            campaign_id = enrollment.get("campaign_id") if enrollment else None
            if campaign_id:
                await database.campaigns_collection.update_one(
                    {"_id": campaign_id},
                    {"$inc": {"meetings_booked": 1}},
                )
        except Exception as exc:
            logger.error(f"Failed to update enrollment {enrollment_id} to meeting_booked: {exc}", exc_info=True)

    # 6. Return updated meeting doc
    meeting["status"] = "booked"
    meeting["confirmed_slot_index"] = slot_index
    meeting["booked_at"] = now
    meeting["updated_at"] = now
    result = dict(meeting)
    result["_id"] = str(result["_id"])
    return result


async def sync_meeting_statuses(account_id: str) -> None:
    """Called when Google Calendar push webhook fires — check proposed meetings for acceptance."""
    meetings = await database.meetings_collection.find(
        {
            "account_id": account_id,
            "status": "proposed",
            "calendar_event_id": {"$exists": True, "$ne": None},
        }
    ).to_list(length=20)
    for meeting in meetings:
        # Check if event was accepted (simplified — just log for now; real acceptance check needs event status polling)
        logger.info(f"Syncing meeting {meeting['_id']} from calendar webhook")
