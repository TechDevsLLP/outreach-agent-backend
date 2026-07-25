"""
Aggregate overview endpoint.
Combines account stats, recent campaigns, and company profile in one request.
"""
import logging

from bson import ObjectId
from fastapi import APIRouter, Depends

from auth import get_account_context
import database

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/overview", tags=["overview"])


@router.get("")
async def get_overview(account_ctx: dict = Depends(get_account_context)):
    """Return aggregate overview data: stats, recent campaigns, profile snippet."""
    account_id = ObjectId(account_ctx["account"]["_id"])
    account_id_str = str(account_id)

    import asyncio

    # 1. Account stats (same aggregation as /api/campaigns/account-stats)
    stats_pipeline = [
        {"$match": {"account_id": account_id}},
        {
            "$group": {
                "_id": None,
                "total_campaigns": {"$sum": 1},
                "active_campaigns": {
                    "$sum": {"$cond": [{"$eq": ["$status", "active"]}, 1, 0]}
                },
                "total_enrolled": {"$sum": "$total_enrolled"},
                "total_replied": {"$sum": "$replied_count"},
                "total_meetings_booked": {"$sum": "$meetings_booked"},
                "emails_sent": {"$sum": "$emails_sent"},
                "emails_opened": {"$sum": "$emails_opened"},
                "linkedin_connections_sent": {"$sum": "$linkedin_connections_sent"},
                "linkedin_connections_accepted": {"$sum": "$linkedin_connections_accepted"},
                "linkedin_inmails_sent": {"$sum": "$linkedin_inmails_sent"},
            }
        },
    ]
    stats_rows = await database.campaigns_collection.aggregate(stats_pipeline).to_list(1)
    stats = stats_rows[0] if stats_rows else {}
    stats.pop("_id", None)

    # 2. Recent campaigns (top 5 by created_at)
    recent_cursor = (
        database.campaigns_collection.find(
            {"account_id": account_id},
            {
                "_id": 1,
                "name": 1,
                "status": 1,
                "total_enrolled": 1,
                "emails_sent": 1,
                "total_replied": 1,
                "created_at": 1,
            },
        )
        .sort("created_at", -1)
        .limit(5)
    )
    recent_campaigns = []
    async for c in recent_cursor:
        recent_campaigns.append({
            "id": str(c["_id"]),
            "name": c.get("name"),
            "status": c.get("status"),
            "total_enrolled": c.get("total_enrolled", 0),
            "emails_sent": c.get("emails_sent", 0),
            "total_replied": c.get("total_replied", 0),
            "created_at": c.get("created_at").isoformat() if c.get("created_at") else None,
        })

    # 3. Company profile snippet
    profile = await database.company_profiles_collection.find_one(
        {"account_id": account_id_str},
        {
            "_id": 1,
            "company_name": 1,
            "website_url": 1,
            "target_industries": 1,
            "services": 1,
            "onboarding_stage": 1,
        },
    )
    profile_snippet = None
    if profile:
        profile_snippet = {
            "company_name": profile.get("company_name"),
            "website_url": profile.get("website_url"),
            "target_industries": profile.get("target_industries", [])[:3],
            "services": profile.get("services", [])[:3],
            "onboarding_stage": profile.get("onboarding_stage", 0),
        }

    return {
        "stats": stats,
        "recent_campaigns": recent_campaigns,
        "profile_snippet": profile_snippet,
    }
