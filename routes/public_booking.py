"""
Public Booking Routes — no authentication required.
Serves the hosted prospect booking page and handles slot confirmation.
"""

import logging
from typing import Optional

from bson import ObjectId
from fastapi import APIRouter, HTTPException
from pydantic import BaseModel

import database
from services.meeting_service import confirm_slot_and_send_invite

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/public", tags=["public_booking"])


@router.get("/book/{token}")
async def get_booking_page(token: str):
    """Return slot data for the hosted prospect booking page."""
    meeting = await database.meetings_collection.find_one({
        "booking_token": token,
        "status": "proposed",
    })
    if not meeting:
        raise HTTPException(status_code=404, detail="Booking link not found or already confirmed")

    # Get prospect name/company from conversations
    conv = None
    if meeting.get("conversation_id"):
        try:
            conv = await database.conversations_collection.find_one(
                {"_id": ObjectId(meeting["conversation_id"])}
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
        "expires_at": None,
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
        "status": "proposed",
    })
    if not meeting:
        raise HTTPException(status_code=404, detail="Booking link not found or already confirmed")

    meeting_id = str(meeting["_id"])
    profile = await database.company_profiles_collection.find_one(
        {"account_id": meeting.get("account_id")}
    )
    agenda = (profile or {}).get("discovery_call_agenda", "25-minute discovery call")

    try:
        result = await confirm_slot_and_send_invite(
            meeting_id=meeting_id,
            slot_index=body.slot_index,
            prospect_email=body.attendee_email or "",
            agenda=agenda,
        )
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc))
    except Exception as exc:
        logger.error(f"confirm_booking error for token {token}: {exc}", exc_info=True)
        raise HTTPException(status_code=500, detail="Failed to confirm booking")

    proposed_slots = meeting.get("proposed_slots", [])
    confirmed_slot = proposed_slots[body.slot_index] if body.slot_index < len(proposed_slots) else {}

    return {
        "confirmed": True,
        "slot": confirmed_slot,
        "calendar_link": result.get("calendar_event_link"),
        "message": "You're booked! A calendar invite will arrive shortly.",
    }
