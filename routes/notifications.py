"""
Notification API routes.
Provides CRUD endpoints + SSE stream for real-time notifications.
"""

from datetime import datetime
from typing import Optional

from bson import ObjectId
from fastapi import APIRouter, Depends, HTTPException, Query, Request
from fastapi.responses import StreamingResponse
from jose import JWTError, jwt

from auth import get_account_context
from config import get_settings
from database import notifications_collection
from services.notification_service import event_stream

router = APIRouter(prefix="/api/notifications", tags=["notifications"])
settings = get_settings()


async def _validate_sse_token(token: str) -> str:
    """Validate JWT from query param for SSE connections (EventSource can't set headers)."""
    credentials_exception = HTTPException(status_code=401, detail="Invalid or expired token")
    try:
        payload = jwt.decode(token, settings.jwt_secret_key, algorithms=[settings.jwt_algorithm])
        email: str = payload.get("sub")
        if email is None or email != settings.admin_email:
            raise credentials_exception
    except JWTError:
        raise credentials_exception
    return email


@router.get("/stream")
async def notification_stream(
    token: str = Query(..., description="JWT token for SSE auth"),
    request: Request = None,
):
    """SSE stream for real-time notifications. Pass JWT as ?token= query param."""
    await _validate_sse_token(token)

    last_event_id = request.headers.get("Last-Event-ID") if request else None

    return StreamingResponse(
        event_stream(last_event_id),
        media_type="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "Connection": "keep-alive",
            "X-Accel-Buffering": "no",
        },
    )


@router.get("")
async def get_notifications(
    page: int = Query(1, ge=1),
    page_size: int = Query(20, ge=1, le=100),
    is_read: Optional[bool] = Query(None),
    type: Optional[str] = Query(None),
    account_ctx=Depends(get_account_context),
):
    """List notifications with pagination and filtering."""
    account_id = ObjectId(account_ctx["account"]["_id"])
    query: dict = {"account_id": account_id}
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


@router.get("/unread-count")
async def get_notification_unread_count(
    account_ctx=Depends(get_account_context),
):
    """Get the count of unread notifications."""
    account_id = ObjectId(account_ctx["account"]["_id"])
    count = await notifications_collection.count_documents({"account_id": account_id, "is_read": False})
    return {"unread_count": count}


@router.patch("/{notification_id}/read")
async def mark_notification_read(
    notification_id: str,
    account_ctx=Depends(get_account_context),
):
    """Mark a single notification as read."""
    account_id = ObjectId(account_ctx["account"]["_id"])
    result = await notifications_collection.update_one(
        {"_id": ObjectId(notification_id), "account_id": account_id, "is_read": False},
        {"$set": {"is_read": True, "read_at": datetime.utcnow()}},
    )
    if result.modified_count == 0:
        raise HTTPException(status_code=404, detail="Notification not found or already read")
    return {"status": "ok"}


@router.patch("/read-all")
async def mark_all_notifications_read(
    account_ctx=Depends(get_account_context),
):
    """Mark all unread notifications as read."""
    account_id = ObjectId(account_ctx["account"]["_id"])
    result = await notifications_collection.update_many(
        {"account_id": account_id, "is_read": False},
        {"$set": {"is_read": True, "read_at": datetime.utcnow()}},
    )
    return {"marked_read": result.modified_count}


@router.delete("/{notification_id}")
async def delete_notification_endpoint(
    notification_id: str,
    account_ctx=Depends(get_account_context),
):
    """Delete a single notification."""
    account_id = ObjectId(account_ctx["account"]["_id"])
    result = await notifications_collection.delete_one(
        {"_id": ObjectId(notification_id), "account_id": account_id}
    )
    if result.deleted_count == 0:
        raise HTTPException(status_code=404, detail="Notification not found")
    return {"status": "ok"}
