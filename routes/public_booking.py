"""
Public Booking Routes — no authentication required.
Serves the hosted prospect booking page and handles slot confirmation.
"""

import logging
import re
from datetime import datetime
from typing import Optional

from bson import ObjectId
from fastapi import APIRouter, HTTPException
from pydantic import BaseModel

import database
from services.conversation_service import _account_filter
from services.meeting_service import (
    MeetingConfirmationInProgress,
    confirm_slot_and_send_invite,
)

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/public", tags=["public_booking"])


def _valid_email(value: str) -> bool:
    return bool(re.fullmatch(r"[^\s@]+@[^\s@]+\.[^\s@]+", value.strip()))


async def _trusted_booking_email(meeting: dict, supplied: str | None) -> str:
    """Bind an invite to the meeting's prospect when that fact is known."""
    trusted = ""
    conversation_id = meeting.get("conversation_id")
    if conversation_id:
        try:
            conversation = await database.conversations_collection.find_one(
                {
                    "_id": ObjectId(str(conversation_id)),
                    **_account_filter(meeting.get("account_id")),
                },
                {"prospect_email": 1},
            )
            trusted = str((conversation or {}).get("prospect_email") or "").strip().lower()
        except Exception:
            trusted = ""
    if not trusted and meeting.get("prospect_id"):
        try:
            prospect = await database.prospects_collection.find_one(
                {"_id": ObjectId(str(meeting["prospect_id"]))}, {"email": 1}
            )
            trusted = str((prospect or {}).get("email") or "").strip().lower()
        except Exception:
            trusted = ""

    supplied_normalized = str(supplied or "").strip().lower()
    if trusted and supplied_normalized and supplied_normalized != trusted:
        raise HTTPException(status_code=400, detail="Attendee email does not match this booking")
    recipient = trusted or supplied_normalized
    if not _valid_email(recipient):
        raise HTTPException(status_code=400, detail="A valid attendee email is required")
    return recipient


@router.get("/book/{token}")
async def get_booking_page(token: str):
    """Return slot data for the hosted prospect booking page."""
    meeting = await database.meetings_collection.find_one({
        "booking_token": token,
        "status": "proposed",
        "booking_expires_at": {"$gt": datetime.utcnow()},
    })
    if not meeting:
        raise HTTPException(status_code=404, detail="Booking link not found or already confirmed")

    # Get prospect name/company from conversations
    conv = None
    if meeting.get("conversation_id"):
        try:
            conv = await database.conversations_collection.find_one(
                {
                    "_id": ObjectId(meeting["conversation_id"]),
                    **_account_filter(meeting.get("account_id")),
                }
            )
        except Exception:
            pass

    # Get agenda from company_profile
    profile = await database.company_profiles_collection.find_one(
        {"account_id": meeting.get("account_id")}
    )

    return {
        "meeting_id": str(meeting["_id"]),
        "prospect_name": (conv or {}).get("prospect_name", ""),
        "prospect_email": (conv or {}).get("prospect_email", ""),
        "host_company": (profile or {}).get("company_name", ""),
        "agenda": (profile or {}).get("discovery_call_agenda", "25-minute discovery call"),
        "duration_minutes": 25,
        "proposed_slots": meeting.get("proposed_slots", []),
        "expires_at": meeting.get("booking_expires_at"),
    }


class ConfirmBookingRequest(BaseModel):
    slot_index: int
    attendee_email: Optional[str] = None
    attendee_name: Optional[str] = None


@router.post("/book/{token}/confirm")
async def confirm_booking(token: str, body: ConfirmBookingRequest):
    """Prospect picks a slot — create calendar event and return confirmation."""
    meeting = await database.meetings_collection.find_one({
        "booking_token": token,
        "booking_expires_at": {"$gt": datetime.utcnow()},
    })
    if not meeting:
        raise HTTPException(status_code=404, detail="Booking link not found or already confirmed")

    meeting_id = str(meeting["_id"])
    profile = await database.company_profiles_collection.find_one(
        {"account_id": meeting.get("account_id")}
    )
    agenda = (profile or {}).get("discovery_call_agenda", "25-minute discovery call")
    attendee_email = await _trusted_booking_email(meeting, body.attendee_email)

    try:
        result = await confirm_slot_and_send_invite(
            meeting_id=meeting_id,
            slot_index=body.slot_index,
            account_id=str(meeting.get("account_id") or ""),
            prospect_email=attendee_email,
            agenda=agenda,
        )
    except MeetingConfirmationInProgress as exc:
        raise HTTPException(status_code=409, detail=str(exc))
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc))
    except Exception as exc:
        logger.error("Public booking confirmation failed", exc_info=True)
        raise HTTPException(status_code=500, detail="Failed to confirm booking")

    proposed_slots = meeting.get("proposed_slots", [])
    confirmed_slot = proposed_slots[body.slot_index] if body.slot_index < len(proposed_slots) else {}

    return {
        "confirmed": True,
        "slot": confirmed_slot,
        "calendar_link": result.get("calendar_event_link"),
        "message": "You're booked! A calendar invite will arrive shortly.",
    }
