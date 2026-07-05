"""
SendGrid Email Activity Routes
Aggregate email stats + suppression data from SendGrid APIs,
plus per-prospect outreach detail from the database.
"""

import logging
from typing import Optional
from fastapi import APIRouter, Depends, Query, HTTPException

from bson import ObjectId
from auth import get_account_context
from database import prospects_collection
from services.sendgrid_activity_service import get_email_activity

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/sendgrid", tags=["sendgrid-activity"])


@router.get("/activity")
async def get_activity(
    start_date: str = Query(default="2025-01-01", description="Stats start date (YYYY-MM-DD)"),
    account_ctx=Depends(get_account_context),
):
    """Fetch email activity stats and suppression data from SendGrid."""
    try:
        data = await get_email_activity(start_date=start_date)
        return {"status": "success", **data}
    except Exception as e:
        logger.error(f"Failed to fetch email activity: {e}")
        raise HTTPException(status_code=502, detail=f"SendGrid API error: {str(e)}")


@router.get("/sent-emails")
async def get_sent_emails(
    page: int = Query(default=1, ge=1),
    page_size: int = Query(default=20, ge=1, le=100),
    search: Optional[str] = Query(default=None),
    account_ctx=Depends(get_account_context),
):
    """
    Fetch per-prospect outreach emails with subject, body, recipient details.
    Only returns prospects that have actually been sent an email (last_outreach_sent is set).
    """
    account_id = ObjectId(account_ctx["account"]["_id"])
    query: dict = {
        "account_id": account_id,
        "last_outreach_sent": {"$exists": True, "$ne": None},
        "outreach_messages.cold_email.body": {"$exists": True, "$ne": ""},
    }

    if search:
        query["$or"] = [
            {"email": {"$regex": search, "$options": "i"}},
            {"full_name": {"$regex": search, "$options": "i"}},
            {"company_name": {"$regex": search, "$options": "i"}},
            {"outreach_messages.cold_email.subject_variants.subject": {"$regex": search, "$options": "i"}},
        ]

    total = await prospects_collection.count_documents(query)
    skip = (page - 1) * page_size

    prospects = await prospects_collection.find(
        query,
        {
            "email": 1,
            "full_name": 1,
            "first_name": 1,
            "company_name": 1,
            "outreach_messages.cold_email.body": 1,
            "outreach_messages.cold_email.subject_variants": 1,
            "outreach_history": 1,
            "last_outreach_sent": 1,
            "last_outreach_opened": 1,
            "last_outreach_replied": 1,
            "email_delivered": 1,
            "status": 1,
        },
    ).sort("last_outreach_sent", -1).skip(skip).limit(page_size).to_list(page_size)

    emails = []
    for p in prospects:
        cold = p.get("outreach_messages", {}).get("cold_email", {})
        variants = cold.get("subject_variants", [])

        # Pick the sent variant, or first selected, or just first
        sent_variant = None
        for v in variants:
            if v.get("times_sent") and v["times_sent"] > 0:
                sent_variant = v
                break
        if not sent_variant:
            for v in variants:
                if v.get("is_selected"):
                    sent_variant = v
                    break
        if not sent_variant and variants:
            sent_variant = variants[0]

        # Determine email-level status from tracking fields
        opened = p.get("last_outreach_opened") is not None
        replied = p.get("status") == "replied"
        delivered = p.get("email_delivered", False)

        # Also check outreach_history for open/reply events
        for h in p.get("outreach_history", []):
            evt = h.get("event", "")
            if evt == "email_opened":
                opened = True
            elif evt in ("email_reply_received", "replied"):
                replied = True

        if replied:
            email_status = "replied"
        elif opened:
            email_status = "opened"
        elif delivered:
            email_status = "delivered"
        else:
            email_status = "sent"

        emails.append({
            "prospect_id": str(p["_id"]),
            "to_email": p.get("email", ""),
            "to_name": p.get("full_name") or p.get("first_name") or "",
            "company": p.get("company_name") or "",
            "subject": sent_variant.get("subject", "") if sent_variant else "",
            "variant_id": sent_variant.get("variant_id", "") if sent_variant else "",
            "body_preview": cold.get("body", "")[:200],
            "body_html": cold.get("body", ""),
            "subject_variants": [
                {
                    "variant_id": v.get("variant_id"),
                    "subject": v.get("subject"),
                    "times_sent": v.get("times_sent") or 0,
                    "times_opened": v.get("times_opened") or 0,
                    "times_replied": v.get("times_replied") or 0,
                }
                for v in variants
            ],
            "sent_at": p.get("last_outreach_sent"),
            "opened_at": p.get("last_outreach_opened"),
            "email_status": email_status,
            "status": p.get("status", "new"),
        })

    return {
        "status": "success",
        "emails": emails,
        "total": total,
        "page": page,
        "page_size": page_size,
        "total_pages": (total + page_size - 1) // page_size,
    }
