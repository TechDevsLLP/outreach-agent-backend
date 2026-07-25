"""
Global entity search for the command palette (⌘K).
Searches across campaigns, prospects, companies, and conversations.
"""
import logging

from bson import ObjectId
from fastapi import APIRouter, Depends, HTTPException, Query

from auth import get_account_context
import database

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/search", tags=["global_search"])


@router.get("")
async def global_search(
    q: str = Query(..., min_length=1),
    limit: int = Query(10, ge=1, le=25),
    account_ctx: dict = Depends(get_account_context),
):
    """Search across campaigns, prospects, companies, and conversations."""
    account_id = str(account_ctx["account"]["_id"])
    account_oid = ObjectId(account_id)
    regex = {"$regex": q, "$options": "i"}

    campaigns, prospects, companies, conversations = await _search_all(
        account_id, account_oid, q, regex, limit
    )

    return {
        "campaigns": campaigns,
        "prospects": prospects,
        "companies": companies,
        "conversations": conversations,
    }


async def _search_all(
    account_id: str,
    account_oid: ObjectId,
    q: str,
    regex: dict,
    limit: int,
) -> tuple:
    """Run parallel searches across all entity types."""
    import asyncio

    campaign_task = database.campaigns_collection.find(
        {"account_id": account_oid, "name": regex},
        {"_id": 1, "name": 1},
    ).sort("created_at", -1).limit(limit).to_list(limit)

    prospect_task = database.prospects_collection.find(
        {"account_id": account_id, "$or": [
            {"full_name": regex},
            {"company_name": regex},
            {"email": regex},
        ]},
        {"_id": 1, "full_name": 1, "company_name": 1, "job_title": 1},
    ).sort("ai_prospect_score", -1).limit(limit).to_list(limit)

    company_task = database.companies_collection.find(
        {"$or": [
            {"name": regex},
            {"domain": regex},
            {"website": regex},
        ]},
        {"_id": 1, "name": 1, "domain": 1},
    ).limit(limit).to_list(limit)

    conversation_task = database.conversations_collection.find(
        {"account_id": account_id, "$or": [
            {"prospect_name": regex},
            {"prospect_company": regex},
            {"last_message_preview": regex},
        ]},
        {"_id": 1, "prospect_name": 1, "prospect_company": 1, "last_message_preview": 1},
    ).sort("updated_at", -1).limit(limit).to_list(limit)

    campaigns_raw, prospects_raw, companies_raw, conversations_raw = await asyncio.gather(
        campaign_task, prospect_task, company_task, conversation_task
    )

    campaigns = [{"id": str(c["_id"]), "name": c["name"]} for c in campaigns_raw]

    prospects = [
        {
            "id": str(p["_id"]),
            "full_name": p.get("full_name"),
            "company_name": p.get("company_name"),
            "job_title": p.get("job_title"),
        }
        for p in prospects_raw
    ]

    companies = [
        {"id": str(c["_id"]), "name": c.get("name"), "domain": c.get("domain")}
        for c in companies_raw
    ]

    conversations = [
        {
            "id": str(c["_id"]),
            "prospect_name": c.get("prospect_name"),
            "prospect_company": c.get("prospect_company"),
            "last_message_preview": c.get("last_message_preview"),
        }
        for c in conversations_raw
    ]

    return campaigns, prospects, companies, conversations
