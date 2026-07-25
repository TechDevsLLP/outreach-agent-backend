"""
Notification Service.
Handles tenant-scoped notification persistence and database-backed SSE replay.
"""

import asyncio
import json
import logging
from datetime import datetime
from typing import AsyncGenerator, Optional

from bson import ObjectId

from database import notifications_collection

logger = logging.getLogger(__name__)

_shutdown_event = asyncio.Event()


def shutdown_sse():
    """Signal all SSE streams to stop so uvicorn reload doesn't hang."""
    _shutdown_event.set()


async def create_notification(
    account_id: str,
    type: str,
    title: str,
    body: str,
    campaign_id: Optional[str] = None,
    prospect_id: Optional[str] = None,
    conversation_id: Optional[str] = None,
    channel: Optional[str] = None,
) -> dict:
    """Persist one canonical, tenant-owned notification."""
    account_id = str(account_id).strip()
    if not account_id:
        raise ValueError("account_id is required for notifications")
    doc = {
        "account_id": account_id,
        "type": type,
        "title": title,
        "body": body,
        "campaign_id": str(campaign_id) if campaign_id else None,
        "prospect_id": prospect_id,
        "conversation_id": conversation_id,
        "channel": channel,
        "is_read": False,
        "read_at": None,
        "created_at": datetime.utcnow(),
    }

    result = await notifications_collection.insert_one(doc)
    doc["_id"] = result.inserted_id
    notification_id = str(result.inserted_id)
    logger.info("Notification created: type=%s id=%s", type, notification_id)
    return doc


async def list_notifications(
    account_id: str,
    page: int = 1,
    page_size: int = 20,
    is_read: Optional[bool] = None,
    type: Optional[str] = None,
) -> dict:
    """List notifications with pagination and filtering."""
    query: dict = {"account_id": str(account_id)}
    if is_read is not None:
        query["is_read"] = is_read
    if type is not None:
        query["type"] = type

    total = await notifications_collection.count_documents(query)
    skip = (page - 1) * page_size

    cursor = notifications_collection.find(query).sort("created_at", -1).skip(skip).limit(page_size)
    items = []
    async for doc in cursor:
        doc["_id"] = str(doc["_id"])
        items.append(doc)

    return {
        "notifications": items,
        "total": total,
        "page": page,
        "page_size": page_size,
        "total_pages": (total + page_size - 1) // page_size,
    }


async def get_unread_count(account_id: str) -> int:
    return await notifications_collection.count_documents(
        {"account_id": str(account_id), "is_read": False}
    )


async def mark_read(account_id: str, notification_id: str) -> bool:
    result = await notifications_collection.update_one(
        {
            "_id": ObjectId(notification_id),
            "account_id": str(account_id),
            "is_read": False,
        },
        {"$set": {"is_read": True, "read_at": datetime.utcnow()}},
    )
    return result.modified_count > 0


async def mark_all_read(account_id: str) -> int:
    result = await notifications_collection.update_many(
        {"account_id": str(account_id), "is_read": False},
        {"$set": {"is_read": True, "read_at": datetime.utcnow()}},
    )
    return result.modified_count


async def delete_notification(account_id: str, notification_id: str) -> bool:
    result = await notifications_collection.delete_one(
        {"_id": ObjectId(notification_id), "account_id": str(account_id)}
    )
    return result.deleted_count > 0


async def event_stream(
    account_id: str,
    last_event_id: Optional[str] = None,
) -> AsyncGenerator[str, None]:
    """
    SSE async generator.
    - Sends initial unread count
    - Replays missed tenant notifications if Last-Event-ID is provided
    - Polls durable Mongo state so events from worker/scheduler replicas appear
    - 30s keepalive heartbeat
    """
    account_id = str(account_id)
    try:
        # Send initial unread count
        unread = await get_unread_count(account_id)
        yield _format_sse("count", {"unread_count": unread})

        last_seen: Optional[ObjectId] = None
        if last_event_id:
            try:
                last_seen = ObjectId(last_event_id)
            except Exception:
                last_seen = None
        if last_seen is None:
            latest = await notifications_collection.find_one(
                {"account_id": account_id},
                {"_id": 1},
                sort=[("_id", -1)],
            )
            last_seen = latest.get("_id") if latest else None

        heartbeat_elapsed = 0
        while not _shutdown_event.is_set():
            query: dict = {"account_id": account_id}
            if last_seen is not None:
                query["_id"] = {"$gt": last_seen}
            cursor = notifications_collection.find(query).sort("_id", 1).limit(100)
            emitted = False
            async for doc in cursor:
                emitted = True
                last_seen = doc["_id"]
                notification_id = str(last_seen)
                yield _format_sse(
                    "notification",
                    _notification_event(doc),
                    event_id=notification_id,
                )
            if emitted:
                heartbeat_elapsed = 0
                yield _format_sse(
                    "count",
                    {"unread_count": await get_unread_count(account_id)},
                )
            await asyncio.sleep(2)
            heartbeat_elapsed += 2
            if heartbeat_elapsed >= 30:
                yield ": heartbeat\n\n"
                heartbeat_elapsed = 0
    except (asyncio.CancelledError, GeneratorExit):
        pass


def _notification_event(doc: dict) -> dict:
    created_at = doc.get("created_at")
    return {
        "id": str(doc["_id"]),
        "type": doc["type"],
        "title": doc["title"],
        "body": doc.get("body", ""),
        "campaign_id": doc.get("campaign_id"),
        "prospect_id": doc.get("prospect_id"),
        "conversation_id": doc.get("conversation_id"),
        "channel": doc.get("channel"),
        "created_at": created_at.isoformat() if created_at else None,
    }


def _format_sse(event: str, data: dict, event_id: Optional[str] = None) -> str:
    """Format a server-sent event."""
    lines = []
    if event_id:
        lines.append(f"id: {event_id}")
    lines.append(f"event: {event}")
    lines.append(f"data: {json.dumps(data)}")
    lines.append("")
    lines.append("")
    return "\n".join(lines)
