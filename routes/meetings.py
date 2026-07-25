"""
Meetings Routes — authenticated endpoints for managing meeting proposals and confirmations.
"""

import logging
from typing import Optional

from bson import ObjectId
from fastapi import APIRouter, Depends, HTTPException, Query
from pydantic import BaseModel

import database
from auth import get_account_context
from services.conversation_service import _account_filter
from services.meeting_service import (
    MeetingConfirmationInProgress,
    confirm_slot_and_send_invite,
)

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/meetings", tags=["meetings"])


def _serialize_meeting(meeting: dict) -> dict:
    """Convert ObjectId fields to strings for JSON serialisation."""
    result = dict(meeting)
    if "_id" in result:
        result["_id"] = str(result["_id"])
    return result


async def _join_prospect_info(meeting: dict) -> dict:
    """Join prospect name/email from conversations_collection."""
    conv_id = meeting.get("conversation_id")
    if conv_id:
        try:
            conv = await database.conversations_collection.find_one(
                {"_id": ObjectId(conv_id), **_account_filter(meeting.get("account_id"))},
                {"prospect_name": 1, "prospect_email": 1, "prospect_company": 1},
            )
            if conv:
                meeting["prospect_name"] = conv.get("prospect_name", "")
                meeting["prospect_email"] = conv.get("prospect_email", "")
                meeting["prospect_company"] = conv.get("prospect_company", "")
        except Exception:
            pass
    return meeting


# ── List meetings ─────────────────────────────────────────────────────────────

@router.get("")
async def list_meetings(
    status: Optional[str] = Query(default="all", description="proposed|booked|cancelled|rescheduling|all"),
    limit: int = Query(default=50, ge=1, le=200),
    account_ctx: dict = Depends(get_account_context),
):
    """List meetings for this account with optional status filter."""
    account_id = str(account_ctx["account"]["_id"])

    query: dict = {"account_id": account_id}
    if status and status != "all":
        query["status"] = status

    cursor = database.meetings_collection.find(query).sort("created_at", -1).limit(limit)
    meetings = await cursor.to_list(length=limit)

    results = []
    for m in meetings:
        m = _serialize_meeting(m)
        m = await _join_prospect_info(m)
        results.append(m)

    return {"meetings": results, "total": len(results)}


# ── Get single meeting ────────────────────────────────────────────────────────

@router.get("/{meeting_id}")
async def get_meeting(
    meeting_id: str,
    account_ctx: dict = Depends(get_account_context),
):
    """Get a single meeting by ID, with prospect info joined from conversations."""
    account_id = str(account_ctx["account"]["_id"])

    try:
        meeting = await database.meetings_collection.find_one(
            {"_id": ObjectId(meeting_id), "account_id": account_id}
        )
    except Exception:
        raise HTTPException(status_code=400, detail="Invalid meeting ID")

    if not meeting:
        raise HTTPException(status_code=404, detail="Meeting not found")

    meeting = _serialize_meeting(meeting)
    meeting = await _join_prospect_info(meeting)
    return meeting


# ── Confirm slot ──────────────────────────────────────────────────────────────

class ConfirmSlotRequest(BaseModel):
    slot_index: int
    prospect_email: Optional[str] = ""
    agenda: Optional[str] = ""


@router.post("/{meeting_id}/confirm-slot")
async def confirm_slot(
    meeting_id: str,
    body: ConfirmSlotRequest,
    account_ctx: dict = Depends(get_account_context),
):
    """Confirm a proposed slot and create a Google Calendar event."""
    account_id = str(account_ctx["account"]["_id"])

    # Verify meeting belongs to this account
    try:
        meeting = await database.meetings_collection.find_one(
            {"_id": ObjectId(meeting_id), **_account_filter(account_id)},
            {"_id": 1, "status": 1},
        )
    except Exception:
        raise HTTPException(status_code=400, detail="Invalid meeting ID")

    if not meeting:
        raise HTTPException(status_code=404, detail="Meeting not found")

    try:
        result = await confirm_slot_and_send_invite(
            meeting_id=meeting_id,
            slot_index=body.slot_index,
            account_id=account_id,
            prospect_email=body.prospect_email or "",
            agenda=body.agenda or "",
        )
    except MeetingConfirmationInProgress as exc:
        raise HTTPException(status_code=409, detail=str(exc))
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc))
    except Exception as exc:
        logger.error(f"confirm_slot error for {meeting_id}: {exc}", exc_info=True)
        raise HTTPException(status_code=500, detail="Failed to confirm slot")

    return result


# ── Cancel meeting ────────────────────────────────────────────────────────────

@router.post("/{meeting_id}/cancel")
async def cancel_meeting(
    meeting_id: str,
    account_ctx: dict = Depends(get_account_context),
):
    """Cancel a meeting and reactivate the enrollment at the n5 node."""
    from datetime import datetime
    account_id = str(account_ctx["account"]["_id"])
    now = datetime.utcnow()

    try:
        meeting = await database.meetings_collection.find_one(
            {"_id": ObjectId(meeting_id), "account_id": account_id}
        )
    except Exception:
        raise HTTPException(status_code=400, detail="Invalid meeting ID")

    if not meeting:
        raise HTTPException(status_code=404, detail="Meeting not found")

    # Update meeting status
    await database.meetings_collection.update_one(
        {"_id": ObjectId(meeting_id), **_account_filter(account_id)},
        {"$set": {"status": "cancelled", "updated_at": now}},
    )

    # Reactivate enrollment at n5 node
    enrollment_id = meeting.get("enrollment_id")
    if enrollment_id:
        try:
            await database.campaign_enrollments_collection.update_one(
                {"_id": ObjectId(enrollment_id), **_account_filter(account_id)},
                {
                    "$set": {
                        "status": "active",
                        "current_node": "n5",
                        "last_activity_at": now,
                    }
                },
            )
        except Exception as exc:
            logger.error(f"Failed to reactivate enrollment {enrollment_id} after cancel: {exc}", exc_info=True)

    return {"cancelled": True, "meeting_id": meeting_id}


# ── Reschedule meeting ────────────────────────────────────────────────────────

class RescheduleRequest(BaseModel):
    reason: Optional[str] = ""


@router.post("/{meeting_id}/reschedule")
async def reschedule_meeting(
    meeting_id: str,
    body: RescheduleRequest,
    account_ctx: dict = Depends(get_account_context),
):
    """
    Set meeting to rescheduling status, clear calendar event ID, and regenerate proposed slots.
    """
    from datetime import datetime, timedelta
    from services.meeting_service import _build_proposed_slots
    from services.calendar_service import propose_three_slots

    account_id = str(account_ctx["account"]["_id"])
    now = datetime.utcnow()

    try:
        meeting = await database.meetings_collection.find_one(
            {"_id": ObjectId(meeting_id), "account_id": account_id}
        )
    except Exception:
        raise HTTPException(status_code=400, detail="Invalid meeting ID")

    if not meeting:
        raise HTTPException(status_code=404, detail="Meeting not found")

    # Generate new proposed slots
    new_slots = _build_proposed_slots()
    try:
        real_slots = await propose_three_slots(account_id=account_id, duration_minutes=25)
        if real_slots:
            new_slots = real_slots
    except Exception:
        pass  # Keep deterministic slots

    await database.meetings_collection.update_one(
        {"_id": ObjectId(meeting_id), **_account_filter(account_id)},
        {
            "$set": {
                "status": "rescheduling",
                "calendar_event_id": None,
                "calendar_event_link": None,
                "confirmed_slot_index": None,
                "booked_at": None,
                "proposed_slots": new_slots,
                "booking_expires_at": now + timedelta(days=30),
                "reschedule_reason": body.reason or "",
                "updated_at": now,
            }
        },
    )

    # Update enrollment status
    enrollment_id = meeting.get("enrollment_id")
    if enrollment_id:
        try:
            await database.campaign_enrollments_collection.update_one(
                {"_id": ObjectId(enrollment_id), **_account_filter(account_id)},
                {"$set": {"status": "meeting_proposed", "last_activity_at": now}},
            )
        except Exception as exc:
            logger.error(f"Failed to update enrollment {enrollment_id} to meeting_proposed on reschedule: {exc}", exc_info=True)

    updated = await database.meetings_collection.find_one(
        {"_id": ObjectId(meeting_id), **_account_filter(account_id)}
    )
    return _serialize_meeting(updated) if updated else {"meeting_id": meeting_id, "status": "rescheduling"}
