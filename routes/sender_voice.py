"""
Sender voice profile API.
GET  /api/sender-voice      — fetch current voice profile
PATCH /api/sender-voice     — manual override
POST /api/sender-voice/resynthesize — re-synthesize from stored posts
"""
import logging
from typing import Optional

from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel

import database
from auth import get_account_context

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/sender-voice", tags=["sender_voice"])


@router.get("")
async def get_sender_voice(account_ctx: dict = Depends(get_account_context)):
    """Return the current sender voice profile for this account."""
    account_id = str(account_ctx["account"]["_id"])
    profile = await database.company_profiles_collection.find_one({"account_id": account_id})
    if not profile:
        raise HTTPException(status_code=404, detail="Company profile not found")
    return {
        "sender_linkedin_url": profile.get("sender_linkedin_url"),
        "sender_name": profile.get("sender_name"),
        "sender_role": profile.get("sender_role"),
        "voice_profile": profile.get("sender_voice_profile"),
        "post_count": len(profile.get("sender_linkedin_posts", [])),
    }


class VoiceOverrideRequest(BaseModel):
    tone_markers: Optional[list[str]] = None
    formality_level: Optional[str] = None
    synthesized_summary: Optional[str] = None
    sender_linkedin_url: Optional[str] = None


@router.patch("")
async def update_sender_voice(
    body: VoiceOverrideRequest,
    account_ctx: dict = Depends(get_account_context),
):
    """Manually override parts of the voice profile."""
    account_id = str(account_ctx["account"]["_id"])
    from datetime import datetime, timezone
    update: dict = {"updated_at": datetime.now(timezone.utc)}
    if body.tone_markers is not None:
        update["sender_voice_profile.tone_markers"] = body.tone_markers
    if body.formality_level is not None:
        update["sender_voice_profile.formality_level"] = body.formality_level
    if body.synthesized_summary is not None:
        update["sender_voice_profile.synthesized_summary"] = body.synthesized_summary
    if body.sender_linkedin_url is not None:
        update["sender_linkedin_url"] = body.sender_linkedin_url
    if len(update) == 1:
        return {"ok": True, "message": "Nothing to update"}
    await database.company_profiles_collection.update_one(
        {"account_id": account_id},
        {"$set": update},
    )
    return {"ok": True}


@router.post("/resynthesize")
async def resynthesize_voice(account_ctx: dict = Depends(get_account_context)):
    """Re-synthesize voice profile from stored raw posts."""
    account_id = str(account_ctx["account"]["_id"])
    try:
        from services.sender_voice_service import resynthesize_voice_profile
        voice_profile = await resynthesize_voice_profile(account_id)
        return {"voice_profile": voice_profile}
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
