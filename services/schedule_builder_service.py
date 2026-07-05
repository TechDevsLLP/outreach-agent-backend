"""
Builds per-day message batches for smart campaigns.

Day-1 batch logic:
- All enrolled prospects sorted by ai_prospect_score DESC
- Top 20 → linkedin_connection track
- Next 25 → email track
- Respects daily caps (20 LI connection, 25 email, 5 inmail, 20 LI message)
- Overflow pushed to next_action_at += 1 day

For subsequent days:
- Collects enrollments where next_action_at falls on target date
- Applies caps in same order (score-desc priority)
- Overflow pushed to +1 day
"""
from __future__ import annotations
from datetime import datetime, timezone, timedelta
from typing import Optional
from bson import ObjectId
import logging

logger = logging.getLogger(__name__)

from services.daily_cap_service import DEFAULT_CAPS  # noqa: single source of truth


async def build_followup_batch(db, campaign_id: str, target_date: datetime) -> list[dict]:
    """
    Collect enrollments whose next_action_at falls on target_date, sorted by score desc.
    Returns list of {enrollment_id, prospect_id, channel, node_id, scheduled_at}.
    """
    day_start = target_date.replace(hour=0, minute=0, second=0, microsecond=0, tzinfo=timezone.utc)
    day_end = day_start + timedelta(days=1)
    try:
        camp_id = ObjectId(campaign_id)
    except Exception:
        camp_id = campaign_id

    pipeline = [
        {"$match": {
            "campaign_id": str(campaign_id),
            "status": {"$in": ["active", "enrolled"]},
            "next_action_at": {"$gte": day_start, "$lt": day_end},
        }},
        {"$lookup": {
            "from": "prospects",
            "let": {"pid": "$prospect_id"},
            "pipeline": [
                {"$match": {"$expr": {"$eq": ["$_id", {"$toObjectId": "$$pid"}]}}},
                {"$project": {"ai_prospect_score": 1, "linkedin": 1, "email": 1, "priority_tier": 1}},
            ],
            "as": "prospect_data",
        }},
        {"$addFields": {"prospect": {"$arrayElemAt": ["$prospect_data", 0]}}},
        {"$sort": {"prospect.ai_prospect_score": -1}},
    ]
    result = []
    async for doc in db.campaign_enrollments.aggregate(pipeline):
        flow_state = doc.get("flow_state") or {}
        current_node_id = flow_state.get("current_node_id", "")
        prospect = doc.get("prospect") or {}
        result.append({
            "enrollment_id": str(doc["_id"]),
            "prospect_id": str(doc.get("prospect_id", "")),
            "node_id": current_node_id,
            "scheduled_at": doc.get("next_action_at"),
            "ai_prospect_score": prospect.get("ai_prospect_score", 0),
            "priority_tier": prospect.get("priority_tier", "cold"),
        })
    return result
