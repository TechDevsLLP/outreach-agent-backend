"""
Enrollment State Machine.
Single source of truth for enrollment status transitions.
Called by reply_handler_service and meeting_service to ensure consistent state.

Valid transitions:
  active         → meeting_proposed   (POSITIVE reply → propose_meeting)
  active         → paused             (SOFT_OBJECTION → 3-day pause)
  active         → paused_ooo         (OOO → pause until return date)
  active         → awaiting_human     (HARD_OBJECTION hostile ≥0.6 → escalate)
  active         → completed          (HARD_OBJECTION non-hostile → graceful close)
  active         → opted_out          (UNSUBSCRIBE)
  meeting_proposed → meeting_booked   (prospect confirms slot)
  meeting_proposed → active           (72h no-confirm → re-engage at n5)
  meeting_booked   → active           (calendar cancellation → re-engage)
  paused           → active           (resume_at elapsed)
  paused_ooo       → active           (ooo_resume_at elapsed)
  awaiting_human   → completed        (human marks handled)
  any              → active           (manual reset)
"""

import logging
from datetime import datetime, timedelta, timezone
from typing import Optional

from bson import ObjectId

import database

logger = logging.getLogger(__name__)

# Valid status values
VALID_STATUSES = {
    "active", "enrolled", "paused", "paused_ooo", "meeting_proposed",
    "meeting_booked", "awaiting_human", "completed", "opted_out", "replied",
}


async def transition(
    enrollment_id: str,
    to_status: str,
    reason: str = "",
    resume_at: Optional[datetime] = None,
    meeting_id: Optional[str] = None,
    extra_fields: Optional[dict] = None,
) -> bool:
    """
    Apply a status transition to an enrollment.
    Returns True on success, False if the enrollment was not found or transition invalid.
    """
    if to_status not in VALID_STATUSES:
        logger.warning(f"Invalid target status: {to_status}")
        return False

    now = datetime.now(timezone.utc)
    update: dict = {
        "status": to_status,
        "last_activity_at": now,
    }

    if reason:
        update["last_transition_reason"] = reason

    if to_status == "paused" and resume_at:
        update["next_action_at"] = resume_at
    elif to_status == "paused_ooo" and resume_at:
        update["ooo_resume_at"] = resume_at
        update["next_action_at"] = resume_at
    elif to_status in ("completed", "opted_out", "meeting_booked", "replied"):
        update["next_action_at"] = None
        update["completed_at"] = now
    elif to_status == "awaiting_human":
        update["escalated_to_human"] = True
        update["next_action_at"] = None
        if reason:
            update["escalated_reason"] = reason
    elif to_status == "meeting_proposed" and meeting_id:
        update["meeting_id"] = meeting_id
    elif to_status == "meeting_booked":
        if meeting_id:
            update["meeting_id"] = meeting_id

    if extra_fields:
        update.update(extra_fields)

    try:
        result = await database.campaign_enrollments_collection.update_one(
            {"_id": ObjectId(enrollment_id)},
            {"$set": update},
        )
        if result.matched_count == 0:
            logger.warning(f"Enrollment {enrollment_id} not found for transition to {to_status}")
            return False
        logger.info(f"Enrollment {enrollment_id}: → {to_status} ({reason or 'no reason'})")
        return True
    except Exception as e:
        logger.error(f"State machine transition failed for {enrollment_id}: {e}", exc_info=True)
        return False


async def reactivate_after_no_confirm(enrollment_id: str) -> bool:
    """
    Called 72h after meeting_proposed with no prospect confirmation.
    Re-activates enrollment at a follow-up node.
    """
    now = datetime.now(timezone.utc)
    resume_at = now + timedelta(hours=2)
    return await transition(
        enrollment_id=enrollment_id,
        to_status="active",
        reason="meeting_proposed_no_confirm_72h",
        extra_fields={"next_action_at": resume_at},
    )


async def handle_calendar_cancel(enrollment_id: str) -> bool:
    """
    Called when Google Calendar fires a cancellation webhook for a booked meeting.
    Re-engages the enrollment.
    """
    now = datetime.now(timezone.utc)
    resume_at = now + timedelta(hours=4)
    return await transition(
        enrollment_id=enrollment_id,
        to_status="active",
        reason="meeting_booked_cancelled_by_prospect",
        extra_fields={"next_action_at": resume_at, "meeting_id": None},
    )


async def mark_human_handled(enrollment_id: str) -> bool:
    """Human has reviewed and handled an escalated conversation."""
    return await transition(
        enrollment_id=enrollment_id,
        to_status="completed",
        reason="human_handled",
        extra_fields={"escalated_to_human": False},
    )
