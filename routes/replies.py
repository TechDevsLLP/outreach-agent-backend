"""
Reply classification management API.
POST /api/replies/classify              — manual classify trigger
GET  /api/replies/preview/{conv_id}    — get latest classification for a conversation
POST /api/replies/override             — override classifier result and re-trigger handler
POST /api/replies/escalate             — manually escalate a conversation
"""
import logging
from typing import Optional

from bson import ObjectId
from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel

import database
from auth import get_account_context
from services.conversation_service import _account_filter

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/replies", tags=["replies"])


async def _find_active_enrollment_id(account_id: str, prospect_id) -> Optional[str]:
    """Look up the in-flight enrollment for a prospect so classify/override/
    escalate can thread enrollment-side effects instead of silently skipping them."""
    if not prospect_id:
        return None
    candidates = [str(prospect_id)]
    try:
        candidates.append(ObjectId(str(prospect_id)))
    except Exception:
        pass
    enrollment = await database.campaign_enrollments_collection.find_one(
        {
            **_account_filter(account_id),
            "prospect_id": {"$in": candidates},
            "status": {"$in": ["active", "enrolled", "replied"]},
        },
        {"_id": 1},
    )
    return str(enrollment["_id"]) if enrollment else None


@router.get("/preview/{conversation_id}")
async def get_reply_preview(
    conversation_id: str,
    account_ctx: dict = Depends(get_account_context),
):
    """Return the latest classification for a conversation."""
    account_id = str(account_ctx["account"]["_id"])
    conv = await database.conversations_collection.find_one(
        {"_id": ObjectId(conversation_id), "account_id": account_id}
    )
    if not conv:
        raise HTTPException(status_code=404, detail="Conversation not found")

    from services.reply_classifier_service import get_latest_classification
    classification = await get_latest_classification(conversation_id)
    return {
        "conversation_id": conversation_id,
        "needs_classification": conv.get("needs_classification", False),
        "scheduled_reply_at": conv.get("scheduled_reply_at"),
        "auto_reply_sent_at": conv.get("auto_reply_sent_at"),
        "classification": classification,
    }


class ClassifyRequest(BaseModel):
    conversation_id: str
    force: bool = False


@router.post("/classify")
async def manually_classify(
    body: ClassifyRequest,
    account_ctx: dict = Depends(get_account_context),
):
    """Manually trigger classification for a conversation."""
    account_id = str(account_ctx["account"]["_id"])
    conv = await database.conversations_collection.find_one(
        {"_id": ObjectId(body.conversation_id), "account_id": account_id}
    )
    if not conv:
        raise HTTPException(status_code=404, detail="Conversation not found")

    messages = conv.get("messages") or []
    inbound = next((m for m in reversed(messages) if m.get("direction") == "inbound"), None)
    if not inbound:
        raise HTTPException(status_code=400, detail="No inbound message found to classify")

    enrollment_id = await _find_active_enrollment_id(account_id, conv.get("prospect_id"))

    from services.reply_classifier_service import classify_reply
    classification = await classify_reply(
        conversation_id=body.conversation_id,
        message_text=inbound.get("content_text", ""),
        channel=conv.get("channel", "email"),
        account_id=account_id,
        enrollment_id=enrollment_id,
        prospect_id=conv.get("prospect_id"),
    )
    return {"classification": classification}


class OverrideRequest(BaseModel):
    conversation_id: str
    category: str
    trigger_handler: bool = True


@router.post("/override")
async def override_classification(
    body: OverrideRequest,
    account_ctx: dict = Depends(get_account_context),
):
    """Override the classification category and optionally re-trigger the handler."""
    from models.reply_classification import REPLY_CATEGORIES
    if body.category not in REPLY_CATEGORIES:
        raise HTTPException(status_code=400, detail=f"Invalid category: {body.category}")

    account_id = str(account_ctx["account"]["_id"])
    conv = await database.conversations_collection.find_one(
        {"_id": ObjectId(body.conversation_id), "account_id": account_id}
    )
    if not conv:
        raise HTTPException(status_code=404, detail="Conversation not found")

    if body.trigger_handler:
        messages = conv.get("messages") or []
        inbound = next((m for m in reversed(messages) if m.get("direction") == "inbound"), None)
        message_text = inbound.get("content_text", "") if inbound else ""
        enrollment_id = await _find_active_enrollment_id(account_id, conv.get("prospect_id"))

        from services.reply_classifier_service import classify_reply
        from services.reply_handler_service import handle_classified_reply
        classification = await classify_reply(
            conversation_id=body.conversation_id,
            message_text=message_text,
            channel=conv.get("channel", "email"),
            account_id=account_id,
            enrollment_id=enrollment_id,
            prospect_id=conv.get("prospect_id"),
        )
        classification["category"] = body.category
        await handle_classified_reply(
            classification=classification,
            conversation=conv,
            message_text=message_text,
        )

    return {"ok": True, "category": body.category}


class EscalateRequest(BaseModel):
    conversation_id: str
    reason: Optional[str] = "Manual escalation"


@router.post("/escalate")
async def escalate_conversation(
    body: EscalateRequest,
    account_ctx: dict = Depends(get_account_context),
):
    """Manually escalate a conversation to human review."""
    account_id = str(account_ctx["account"]["_id"])
    conv = await database.conversations_collection.find_one(
        {"_id": ObjectId(body.conversation_id), "account_id": account_id}
    )
    if not conv:
        raise HTTPException(status_code=404, detail="Conversation not found")

    from services.reply_handler_service import _escalate_to_human
    enrollment_id = await _find_active_enrollment_id(account_id, conv.get("prospect_id"))
    await _escalate_to_human(
        enrollment_id=enrollment_id,
        conversation_id=body.conversation_id,
        account_id=account_id,
        reason=body.reason or "Manual escalation",
    )
    return {"ok": True}
