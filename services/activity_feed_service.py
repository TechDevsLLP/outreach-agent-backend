# version2/services/activity_feed_service.py
from datetime import datetime, timedelta
from typing import Optional
from database import prospects_collection, conversations_collection

ACTIVITY_TYPES = [
    "email_sent", "email_delivered", "email_opened", "email_clicked", "email_bounced",
    "email_reply_received",
    "linkedin_connection_sent", "linkedin_connection_accepted",
    "linkedin_message_sent", "linkedin_reply_received",
    "linkedin_inmail_sent", "linkedin_inmail_reply_received",
    "followup_sent", "prospect_added_to_schedule",
    "ai_draft_ready",
]


async def get_activity_feed(
    page: int = 1,
    page_size: int = 50,
    activity_type: Optional[str] = None,
    channel: Optional[str] = None,
    since: Optional[datetime] = None,
    prospect_id: Optional[str] = None,
    account_id: Optional[str] = None,
) -> dict:
    """
    Build a unified activity feed from:
    1. Conversation messages (sent/received across email+linkedin)
    2. Prospect outreach_history events
    3. Schedule execution events

    Returns {items: [...], total: int, page: int, page_size: int}
    """
    if since is None:
        since = datetime.utcnow() - timedelta(days=7)

    pipeline = []

    # Stage 1: Match conversations with messages after `since`
    match_filter = {"last_message_at": {"$gte": since}}
    if channel:
        match_filter["channel"] = channel
    if account_id:
        from bson import ObjectId as _ObjectId
        match_filter["account_id"] = _ObjectId(account_id)
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

    # Now also pull prospect-level events (connection_accepted, email_opened, etc.)
    # from prospect.outreach_history for richer feed
    prospect_match: dict = {"outreach_history": {"$exists": True, "$ne": []}}
    if account_id:
        from bson import ObjectId as _ObjectId
        prospect_match["account_id"] = _ObjectId(account_id)
    if prospect_id:
        from bson import ObjectId as _ObjectId
        prospect_match["_id"] = _ObjectId(prospect_id)
    prospect_pipeline = [
        {"$match": prospect_match},
        {"$unwind": "$outreach_history"},
        {"$match": {"outreach_history.timestamp": {"$gte": since}}},
    ]

    prospect_pipeline.append({
        "$project": {
            "_id": 0,
            "activity_id": {"$toString": "$outreach_history.timestamp"},
            "timestamp": "$outreach_history.timestamp",
            "activity_type": "$outreach_history.event",
            "channel": "$outreach_history.channel",
            "prospect_id": {"$toString": "$_id"},
            "prospect_name": "$full_name",
            "prospect_email": "$email",
            "prospect_company": "$company_name",
            "conversation_id": "$outreach_history.conversation_id",
            "message_preview": {"$ifNull": ["$outreach_history.preview", ""]},
            "message_subject": {"$ifNull": ["$outreach_history.subject", None]},
            "message_status": None,
            "direction": "outbound",
            "variant_id": None,
        }
    })
    if activity_type:
        prospect_pipeline.append({"$match": {"activity_type": activity_type}})

    prospect_pipeline.append({"$sort": {"timestamp": -1}})
    prospect_pipeline.append({"$limit": page_size})

    prospect_items = await prospects_collection.aggregate(prospect_pipeline).to_list(page_size)

    # Merge and sort both streams
    all_items = items + prospect_items
    all_items.sort(key=lambda x: x["timestamp"], reverse=True)
    all_items = all_items[:page_size]

    return {
        "items": all_items,
        "total": total + len(prospect_items),
        "page": page,
        "page_size": page_size,
    }


async def get_activity_summary() -> dict:
    """Quick counts for dashboard widgets."""
    now = datetime.utcnow()
    today_start = now.replace(hour=0, minute=0, second=0, microsecond=0)

    # Today's activity counts
    today_sent = await conversations_collection.count_documents({
        "messages": {
            "$elemMatch": {
                "direction": "outbound",
                "timestamp": {"$gte": today_start}
            }
        }
    })

    today_replies = await conversations_collection.count_documents({
        "messages": {
            "$elemMatch": {
                "direction": "inbound",
                "timestamp": {"$gte": today_start}
            }
        }
    })

    unread_count = await conversations_collection.count_documents({"is_read": False})

    # Pending connections (sent but not accepted)
    pending_connections = await prospects_collection.count_documents({
        "outreach_history.event": "linkedin_connection_sent",
        "connection_accepted_at": None,
        "status": "contacted",
    })

    # Connections accepted today
    accepted_today = await prospects_collection.count_documents({
        "connection_accepted_at": {"$gte": today_start}
    })

    # AI drafts pending review (stored at conversation level, not message level)
    pending_drafts = await conversations_collection.count_documents({
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
