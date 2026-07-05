"""
Notification Service.
Handles notification CRUD, SSE broadcasting, and real-time event streaming.
"""

import asyncio
import json
import logging
from datetime import datetime
from typing import AsyncGenerator, Optional

from bson import ObjectId

from database import notifications_collection

logger = logging.getLogger(__name__)

# In-process SSE clients: set of asyncio.Queue instances
_sse_clients: set[asyncio.Queue] = set()
_shutdown_event = asyncio.Event()


def _register_client() -> asyncio.Queue:
    queue: asyncio.Queue = asyncio.Queue()
    _sse_clients.add(queue)
    logger.info(f"SSE client connected (total: {len(_sse_clients)})")
    return queue


def _unregister_client(queue: asyncio.Queue):
    _sse_clients.discard(queue)
    logger.info(f"SSE client disconnected (total: {len(_sse_clients)})")


def shutdown_sse():
    """Signal all SSE streams to stop so uvicorn reload doesn't hang."""
    _shutdown_event.set()


async def _broadcast(event_type: str, data: dict, event_id: Optional[str] = None):
    """Send an event to all connected SSE clients."""
    for queue in _sse_clients.copy():
        try:
            await queue.put({"event": event_type, "data": data, "id": event_id})
        except Exception:
            pass


async def create_notification(
    type: str,
    title: str,
    body: str,
    prospect_id: Optional[str] = None,
    conversation_id: Optional[str] = None,
    channel: Optional[str] = None,
) -> dict:
    """Create a notification and broadcast to SSE clients."""
    doc = {
        "type": type,
        "title": title,
        "body": body,
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

    # Get current unread count
    unread = await notifications_collection.count_documents({"is_read": False})

    # Broadcast to SSE clients
    await _broadcast(
        "notification",
        {
            "id": notification_id,
            "type": type,
            "title": title,
            "body": body,
            "prospect_id": prospect_id,
            "conversation_id": conversation_id,
            "channel": channel,
            "created_at": doc["created_at"].isoformat(),
            "unread_count": unread,
        },
        event_id=notification_id,
    )

    logger.info(f"Notification created: {type} - {title} (id={notification_id})")
    return doc


async def list_notifications(
    page: int = 1,
    page_size: int = 20,
    is_read: Optional[bool] = None,
    type: Optional[str] = None,
) -> dict:
    """List notifications with pagination and filtering."""
    query: dict = {}
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


async def get_unread_count() -> int:
    return await notifications_collection.count_documents({"is_read": False})


async def mark_read(notification_id: str) -> bool:
    result = await notifications_collection.update_one(
        {"_id": ObjectId(notification_id), "is_read": False},
        {"$set": {"is_read": True, "read_at": datetime.utcnow()}},
    )
    if result.modified_count > 0:
        unread = await get_unread_count()
        await _broadcast("count", {"unread_count": unread})
    return result.modified_count > 0


async def mark_all_read() -> int:
    result = await notifications_collection.update_many(
        {"is_read": False},
        {"$set": {"is_read": True, "read_at": datetime.utcnow()}},
    )
    if result.modified_count > 0:
        await _broadcast("count", {"unread_count": 0})
    return result.modified_count


async def delete_notification(notification_id: str) -> bool:
    # Check if it was unread before deleting
    doc = await notifications_collection.find_one({"_id": ObjectId(notification_id)})
    result = await notifications_collection.delete_one({"_id": ObjectId(notification_id)})
    if result.deleted_count > 0 and doc and not doc.get("is_read"):
        unread = await get_unread_count()
        await _broadcast("count", {"unread_count": unread})
    return result.deleted_count > 0


async def event_stream(last_event_id: Optional[str] = None) -> AsyncGenerator[str, None]:
    """
    SSE async generator.
    - Sends initial unread count
    - Replays missed notifications if Last-Event-ID provided
    - Streams new events from queue
    - 30s keepalive heartbeat
    """
    queue = _register_client()
    try:
        # Send initial unread count
        unread = await get_unread_count()
        yield _format_sse("count", {"unread_count": unread})

        # Replay missed notifications since last_event_id
        if last_event_id:
            try:
                replay_query = {
                    "_id": {"$gt": ObjectId(last_event_id)},
                }
                cursor = notifications_collection.find(replay_query).sort("_id", 1)
                async for doc in cursor:
                    notification_id = str(doc["_id"])
                    yield _format_sse(
                        "notification",
                        {
                            "id": notification_id,
                            "type": doc["type"],
                            "title": doc["title"],
                            "body": doc["body"],
                            "prospect_id": doc.get("prospect_id"),
                            "conversation_id": doc.get("conversation_id"),
                            "channel": doc.get("channel"),
                            "created_at": doc["created_at"].isoformat(),
                        },
                        event_id=notification_id,
                    )
            except Exception as e:
                logger.warning(f"Failed to replay notifications: {e}")

        # Stream events from queue with heartbeat
        while not _shutdown_event.is_set():
            try:
                event = await asyncio.wait_for(queue.get(), timeout=30.0)
                yield _format_sse(event["event"], event["data"], event.get("id"))
            except asyncio.TimeoutError:
                # Send heartbeat
                yield ": heartbeat\n\n"
    except (asyncio.CancelledError, GeneratorExit):
        pass
    finally:
        _unregister_client(queue)


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
