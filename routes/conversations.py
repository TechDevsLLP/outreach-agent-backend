"""
Conversations Routes - Authenticated inbox API endpoints.
"""

import logging
from typing import Optional
from fastapi import APIRouter, HTTPException, Depends, Query
from pydantic import BaseModel

from auth import get_account_context
from models.conversation import ReplyRequest, ComposeRequest
from services import conversation_service

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/conversations", tags=["conversations"])


@router.get("")
async def list_conversations(
    page: int = Query(1, ge=1),
    page_size: int = Query(20, ge=1, le=100),
    channel: Optional[str] = Query(None),
    is_read: Optional[bool] = Query(None),
    search: Optional[str] = Query(None),
    prospect_id: Optional[str] = Query(None),
    account_ctx: dict = Depends(get_account_context),
):
    """List conversations with pagination and filters."""
    account_id = account_ctx["account"]["_id"]
    result = await conversation_service.list_conversations(
        page=page,
        page_size=page_size,
        channel=channel,
        is_read=is_read,
        search=search,
        prospect_id=prospect_id,
        account_id=account_id,
    )

    # Enrich LinkedIn conversations with connection_accepted flag
    from database import prospects_collection
    from bson import ObjectId
    linkedin_convs = [c for c in result.get("conversations", []) if c.get("channel") == "linkedin"]
    if linkedin_convs:
        prospect_ids = [c["prospect_id"] for c in linkedin_convs if c.get("prospect_id")]
        try:
            oids = [ObjectId(pid) for pid in prospect_ids]
        except Exception:
            oids = []
        if oids:
            accepted_map = {}
            async for p in prospects_collection.find(
                {"_id": {"$in": oids}, "connection_accepted_at": {"$ne": None}},
                {"connection_accepted_at": 1, "connection_followup_sent_at": 1},
            ):
                accepted_map[str(p["_id"])] = {
                    "connection_accepted": True,
                    "followup_sent": p.get("connection_followup_sent_at") is not None,
                }
            for c in linkedin_convs:
                info = accepted_map.get(c.get("prospect_id"), {})
                c["connection_accepted"] = info.get("connection_accepted", False)
                c["followup_sent"] = info.get("followup_sent", False)

    # Sort: accepted-needing-followup first, then unread, then by last_message_at desc
    from datetime import datetime as _dt
    conversations = result.get("conversations", [])

    def _sort_key(c):
        needs_followup = c.get("connection_accepted", False) and not c.get("followup_sent", False)
        is_unread = not c.get("is_read", True)
        priority = 0 if needs_followup else (1 if is_unread else 2)
        # For time: use epoch 0 as fallback so missing dates sort last
        last_msg = c.get("last_message_at")
        if isinstance(last_msg, _dt):
            ts = last_msg.timestamp()
        elif isinstance(last_msg, str) and last_msg:
            try:
                ts = _dt.fromisoformat(last_msg.replace("Z", "+00:00")).timestamp()
            except Exception:
                ts = 0
        else:
            ts = 0
        return (priority, -ts)

    conversations.sort(key=_sort_key)
    result["conversations"] = conversations

    return result


@router.get("/stats")
async def get_inbox_stats(account_ctx: dict = Depends(get_account_context)):
    """Get inbox statistics (unread counts, response rate)."""
    account_id = account_ctx["account"]["_id"]
    stats = await conversation_service.get_inbox_stats(account_id=account_id)
    return stats.model_dump()


@router.get("/prospect/{prospect_id}")
async def get_prospect_conversations(
    prospect_id: str,
    account_ctx=Depends(get_account_context),
):
    """Get all conversations for a specific prospect."""
    from database import conversations_collection
    from bson import ObjectId
    account_id = account_ctx["account"]["_id"]
    conversations = await conversation_service.get_conversations_for_prospect(prospect_id)
    # Filter to only conversations belonging to this account
    conversations = [c for c in conversations if not c.get("account_id") or c.get("account_id") == account_id]
    return {"conversations": conversations}


@router.get("/{conversation_id}")
async def get_conversation(
    conversation_id: str,
    account_ctx: dict = Depends(get_account_context),
):
    """Get a single conversation with all messages, enriched with prospect connection status."""
    account_id = account_ctx["account"]["_id"]
    conversation = await conversation_service.get_conversation(conversation_id)
    if not conversation:
        raise HTTPException(status_code=404, detail="Conversation not found")

    # Verify ownership — allow legacy conversations with no account_id
    conv_account = conversation.get("account_id")
    if conv_account and conv_account != account_id:
        raise HTTPException(status_code=403, detail="Not authorized")

    # Enrich with connection status for LinkedIn conversations
    prospect_id = conversation.get("prospect_id")
    if prospect_id and conversation.get("channel") == "linkedin":
        from database import prospects_collection
        from bson import ObjectId
        prospect = await prospects_collection.find_one(
            {"_id": ObjectId(prospect_id)},
            {
                "connection_accepted_at": 1,
                "connection_followup_sent_at": 1,
                "connection_request_sent_at": 1,
            },
        )
        if prospect:
            conversation["connection_accepted_at"] = prospect.get("connection_accepted_at")
            conversation["connection_followup_sent_at"] = prospect.get("connection_followup_sent_at")
            conversation["connection_request_sent_at"] = prospect.get("connection_request_sent_at")
            # Include pre-built followup message if accepted but not yet sent
            if prospect.get("connection_accepted_at") and not prospect.get("connection_followup_sent_at"):
                from services.connection_tracker_service import _build_followup_message
                first_name = conversation.get("prospect_name", "there").split()[0]
                conversation["suggested_followup"] = _build_followup_message(first_name)

    return conversation


@router.patch("/{conversation_id}/read")
async def mark_conversation_read(
    conversation_id: str,
    account_ctx: dict = Depends(get_account_context),
):
    """Mark a conversation as read."""
    account_id = account_ctx["account"]["_id"]
    # Verify ownership before marking read
    from database import conversations_collection
    from bson import ObjectId
    conv = await conversations_collection.find_one(
        {"_id": ObjectId(conversation_id)}, {"account_id": 1}
    )
    if conv and conv.get("account_id") and conv["account_id"] != account_id:
        raise HTTPException(status_code=403, detail="Not authorized")
    success = await conversation_service.mark_conversation_read(conversation_id)
    if not success:
        raise HTTPException(status_code=404, detail="Conversation not found")
    return {"status": "ok"}


@router.post("/reply")
async def reply_to_conversation(
    payload: ReplyRequest,
    account_ctx: dict = Depends(get_account_context),
):
    """Send a reply in an existing conversation (dispatches to email or LinkedIn)."""
    account_id = account_ctx["account"]["_id"]
    # Verify ownership before sending reply
    from database import conversations_collection
    from bson import ObjectId
    conv = await conversations_collection.find_one(
        {"_id": ObjectId(payload.conversation_id)}, {"account_id": 1}
    )
    if conv and conv.get("account_id") and conv["account_id"] != account_id:
        raise HTTPException(status_code=403, detail="Not authorized")
    try:
        result = await conversation_service.send_reply(
            conversation_id=payload.conversation_id,
            content_text=payload.content_text,
            content_html=payload.content_html,
            subject=payload.subject,
        )
        return result
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        logger.error(f"Reply failed: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Failed to send reply: {str(e)}")


@router.post("/compose")
async def compose_new_message(
    payload: ComposeRequest,
    account_ctx: dict = Depends(get_account_context),
):
    """Compose a new message to a prospect (starts new conversation)."""
    account_id = account_ctx["account"]["_id"]
    try:
        result = await conversation_service.compose_new_message(
            prospect_id=payload.prospect_id,
            channel=payload.channel,
            content_text=payload.content_text,
            content_html=payload.content_html,
            subject=payload.subject,
            account_id=account_id,
        )
        return result
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        logger.error(f"Compose failed: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Failed to compose message: {str(e)}")


from services.ai_reply_service import (
    regenerate_reply_draft,
    approve_draft,
    reject_draft,
)


class ApproveDraftRequest(BaseModel):
    edited_text: Optional[str] = None


@router.get("/{conversation_id}/ai-draft")
async def get_ai_draft(
    conversation_id: str,
    account_ctx: dict = Depends(get_account_context),
):
    """Get the current AI draft reply for a conversation."""
    account_id = account_ctx["account"]["_id"]
    from database import conversations_collection
    from bson import ObjectId
    conversation = await conversations_collection.find_one(
        {"_id": ObjectId(conversation_id)},
        {"ai_draft_reply": 1, "account_id": 1},
    )
    if not conversation:
        raise HTTPException(status_code=404, detail="Conversation not found")
    if conversation.get("account_id") and conversation["account_id"] != account_id:
        raise HTTPException(status_code=403, detail="Not authorized")
    return {"draft": conversation.get("ai_draft_reply")}


@router.post("/{conversation_id}/ai-draft/regenerate")
async def regenerate_draft_endpoint(
    conversation_id: str,
    account_ctx: dict = Depends(get_account_context),
):
    """Regenerate the AI draft reply."""
    try:
        draft = await regenerate_reply_draft(conversation_id)
        return {"draft": draft}
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))


@router.post("/{conversation_id}/ai-draft/approve")
async def approve_draft_endpoint(
    conversation_id: str,
    request: ApproveDraftRequest,
    account_ctx: dict = Depends(get_account_context),
):
    """Approve (and optionally edit) the AI draft, then send it."""
    try:
        result = await approve_draft(conversation_id, request.edited_text)
        return result
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))


@router.post("/{conversation_id}/ai-draft/reject")
async def reject_draft_endpoint(
    conversation_id: str,
    account_ctx: dict = Depends(get_account_context),
):
    """Reject the AI draft."""
    result = await reject_draft(conversation_id)
    return result


