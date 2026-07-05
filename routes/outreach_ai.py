"""
On-demand AI outreach message generation — used by the prospect detail page's
"Outreach with AI" dialog. Takes a pitch/offering string, regenerates a single
channel's message, returns the raw payload for the UI to edit and send.
"""

import logging

from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel, Field
from bson import ObjectId

from auth import get_account_context
from database import prospects_collection, companies_collection
from services.openrouter_service import OpenRouterClient
from services.outreach_service import generate_outreach_v2

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/outreach", tags=["outreach"])


class GenerateMessageRequest(BaseModel):
    prospect_id: str
    channel: str = Field(..., pattern="^(email|connect|inmail)$")
    pitch: str = Field(..., min_length=1, max_length=2000)


@router.post("/generate-message")
async def generate_message(
    body: GenerateMessageRequest,
    account_ctx=Depends(get_account_context),
):
    account_id = ObjectId(account_ctx["account"]["_id"])

    try:
        oid = ObjectId(body.prospect_id)
    except Exception:
        raise HTTPException(status_code=400, detail="Invalid prospect ID")

    prospect = await prospects_collection.find_one({"_id": oid, "account_id": account_id})
    if not prospect:
        raise HTTPException(status_code=404, detail="Prospect not found")

    # Merge competitors from company doc so the prompt has them
    if prospect.get("company_linkedin") and not prospect.get("competitors"):
        comp_doc = await companies_collection.find_one({"linkedin_url": prospect["company_linkedin"]})
        if comp_doc and comp_doc.get("competitors"):
            prospect["competitors"] = comp_doc["competitors"]

    profile = prospect.get("linkedin_profile_data")
    company = prospect.get("company_linkedin_data")
    assessment = prospect.get("ai_assessment")

    client = OpenRouterClient()
    try:
        full = await generate_outreach_v2(
            prospect=prospect,
            profile=profile,
            company=company,
            assessment=assessment,
            client=client,
            custom_context=body.pitch,
        )
    finally:
        await client.close()

    if body.channel == "email":
        ce = full.get("cold_email") or {}
        variants = ce.get("subject_variants") or []
        subject = next((v.get("subject") for v in variants if v.get("is_selected")), None) \
            or (variants[0].get("subject") if variants else "")
        return {
            "channel": "email",
            "subject": subject,
            "body": ce.get("body", ""),
        }

    if body.channel == "connect":
        conn = full.get("linkedin_connection_request") or {}
        return {
            "channel": "connect",
            "message": conn.get("message", ""),
        }

    # inmail
    inmail = full.get("linkedin_inmail") or {}
    return {
        "channel": "inmail",
        "subject": inmail.get("subject", ""),
        "message": inmail.get("message", ""),
    }
