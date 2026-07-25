# version2/services/activity_feed_service.py
from datetime import datetime, timedelta
from typing import Optional
from bson import ObjectId
from database import conversations_collection, prospect_state_collection

ACTIVITY_TYPES = [
    "email_sent", "email_delivered", "email_opened", "email_clicked", "email_bounced",
    "email_reply_received",
    "linkedin_connection_sent", "linkedin_connection_accepted",
    "linkedin_message_sent", "linkedin_reply_received",
    "linkedin_inmail_sent", "linkedin_inmail_reply_received",
    "followup_sent", "prospect_added_to_schedule",
    "ai_draft_ready",
]


def _account_filter(account_id: str) -> dict:
    if not account_id or not str(account_id).strip():
        raise ValueError("account_id is required for activity access")
    values: list[object] = [str(account_id)]
    try:
        values.append(ObjectId(str(account_id)))
    except Exception:
        pass
    return {"account_id": {"$in": values}}


async def get_activity_feed(
    page: int = 1,
    page_size: int = 50,
    activity_type: Optional[str] = None,
    channel: Optional[str] = None,
    since: Optional[datetime] = None,
    prospect_id: Optional[str] = None,
    account_id: str = "",
) -> dict:
    """
    Build a tenant-owned activity feed from canonical conversation messages.

    Returns {items: [...], total: int, page: int, page_size: int}
    """
    if since is None:
        since = datetime.utcnow() - timedelta(days=7)

    pipeline = []

    # Stage 1: Match conversations with messages after `since`
    match_filter = {
        **_account_filter(account_id),
        "last_message_at": {"$gte": since},
    }
    if channel:
        match_filter["channel"] = channel
    if prospect_id:
        match_filter["prospect_id"] = prospect_id
    pipeline.append({"$match": match_filter})

    # Stage 2: Unwind messages
    pipeline.append({"$unwind": "$messages"})
    pipeline.append({"$match": {"messages.timestamp": {"$gte": since}}})

    # Stage 3: Project into unified activity shape
    pipeline.append({
        "$project": {
            "_id": 0,
            "activity_id": "$messages.message_id",
            "timestamp": "$messages.timestamp",
            "activity_type": {
                "$switch": {
                    "branches": [
                        {"case": {"$eq": ["$messages.direction", "inbound"]}, "then": {
                            "$concat": [{"$ifNull": ["$channel", "email"]}, "_reply_received"]
                        }},
                        {"case": {"$eq": ["$messages.direction", "outbound"]}, "then": {
                            "$concat": [{"$ifNull": ["$channel", "email"]}, "_sent"]
                        }},
                    ],
                    "default": "unknown"
                }
            },
            "channel": "$channel",
            "prospect_id": "$prospect_id",
            "prospect_name": "$prospect_name",
            "prospect_email": "$prospect_email",
            "prospect_company": "$prospect_company",
            "conversation_id": {"$toString": "$_id"},
            "message_preview": {"$substrCP": [{"$ifNull": ["$messages.content_text", ""]}, 0, 150]},
            "message_subject": "$messages.subject",
            "message_status": "$messages.status",
            "direction": "$messages.direction",
            "variant_id": "$messages.variant_id",
        }
    })

    # Apply activity_type filter if specified
    if activity_type:
        pipeline.append({"$match": {"activity_type": activity_type}})

    # Sort by timestamp descending
    pipeline.append({"$sort": {"timestamp": -1}})

    # Count total before pagination
    count_pipeline = pipeline + [{"$count": "total"}]
    count_result = await conversations_collection.aggregate(count_pipeline).to_list(1)
    total = count_result[0]["total"] if count_result else 0

    # Paginate
    skip = (page - 1) * page_size
    pipeline.append({"$skip": skip})
    pipeline.append({"$limit": page_size})

    items = await conversations_collection.aggregate(pipeline).to_list(page_size)

    return {
        "items": items,
        "total": total,
        "page": page,
        "page_size": page_size,
    }


async def get_activity_summary(account_id: str) -> dict:
    """Quick counts for dashboard widgets."""
    now = datetime.utcnow()
    today_start = now.replace(hour=0, minute=0, second=0, microsecond=0)

    # Today's activity counts
    account_filter = _account_filter(account_id)
    today_sent = await conversations_collection.count_documents({
        **account_filter,
        "messages": {
            "$elemMatch": {
                "direction": "outbound",
                "timestamp": {"$gte": today_start}
            }
        }
    })

    today_replies = await conversations_collection.count_documents({
        **account_filter,
        "messages": {
            "$elemMatch": {
                "direction": "inbound",
                "timestamp": {"$gte": today_start}
            }
        }
    })

    unread_count = await conversations_collection.count_documents(
        {**account_filter, "is_read": False}
    )

    # Pending connections (sent but not accepted)
    pending_connections = await prospect_state_collection.count_documents({
        **account_filter,
        "connection_request_sent_at": {"$ne": None},
        "$or": [
            {"connection_accepted_at": {"$exists": False}},
            {"connection_accepted_at": None},
        ],
    })

    # Connections accepted today
    accepted_today = await prospect_state_collection.count_documents({
        **account_filter,
        "connection_accepted_at": {"$gte": today_start}
    })

    # AI drafts pending review (stored at conversation level, not message level)
    pending_drafts = await conversations_collection.count_documents({
        **account_filter,
        "ai_draft_reply": {"$exists": True, "$ne": None},
        "ai_draft_reply.status": "pending",
    })

    return {
        "today_sent": today_sent,
        "today_replies": today_replies,
        "unread_conversations": unread_count,
        "pending_connections": pending_connections,
        "accepted_today": accepted_today,
        "pending_ai_drafts": pending_drafts,
    }
