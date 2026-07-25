"""
Google Calendar routes.

Provides:
  POST /api/calendar/webhooks/google  — Google push notification handler (no auth)
  POST /api/calendar/register-watch   — Register a push notification channel
  GET  /api/calendar/status           — Calendar connection status for the account
"""

import hashlib
import hmac
import logging
from datetime import datetime, timezone

from bson import ObjectId
from fastapi import APIRouter, Depends, HTTPException, Request

from auth import get_account_context
import database
from services.calendar_service import register_calendar_watch

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/calendar", tags=["calendar"])


# ---------------------------------------------------------------------------
# Google push notification webhook (no auth — Google calls this)
# ---------------------------------------------------------------------------

@router.post("/webhooks/google")
async def google_calendar_webhook(request: Request):
    """
    Handle Google Calendar push notifications.

    Google sends these headers:
      X-Goog-Channel-ID      — the channel UUID we registered
      X-Goog-Resource-State  — "sync" (initial handshake), "exists" (change), "not_exists" (deleted)
      X-Goog-Channel-Token   — a random secret generated for this exact watch
    """
    body = await request.body()
    if body:
        raise HTTPException(status_code=400, detail="Unexpected webhook body")

    channel_id = request.headers.get("X-Goog-Channel-ID", "")
    channel_token = request.headers.get("X-Goog-Channel-Token", "")
    resource_id = request.headers.get("X-Goog-Resource-ID", "")
    resource_state = request.headers.get("X-Goog-Resource-State", "")
    message_number_raw = request.headers.get("X-Goog-Message-Number", "")

    if not all((channel_id, channel_token, resource_id, resource_state)):
        raise HTTPException(status_code=403, detail="Invalid calendar webhook")
    if resource_state not in {"sync", "exists", "not_exists"}:
        raise HTTPException(status_code=400, detail="Unsupported resource state")

    channel = await database.calendar_webhook_channels_collection.find_one(
        {
            "channel_id": channel_id,
            "provider": "google",
            "status": {"$in": ["pending", "active"]},
            "expires_at": {"$gt": datetime.now(timezone.utc)},
        }
    )
    if not channel:
        raise HTTPException(status_code=403, detail="Invalid calendar webhook")

    received_hash = hashlib.sha256(channel_token.encode("utf-8")).hexdigest()
    expected_hash = str(channel.get("channel_token_hash") or "")
    if not expected_hash or not hmac.compare_digest(received_hash, expected_hash):
        raise HTTPException(status_code=403, detail="Invalid calendar webhook")

    account_id = str(channel.get("account_id") or "")
    provider_account_id = str(channel.get("provider_account_id") or "")
    if not account_id or not provider_account_id:
        raise HTTPException(status_code=403, detail="Invalid calendar webhook")

    try:
        provider_account_object_id = ObjectId(provider_account_id)
    except Exception:
        raise HTTPException(status_code=403, detail="Invalid calendar webhook")
    provider_account = await database.email_accounts_collection.find_one(
        {
            "_id": provider_account_object_id,
            "account_id": account_id,
            "provider": "google",
            "oauth_scopes": {"$elemMatch": {"$regex": "calendar"}},
        },
        {"_id": 1},
    )
    if not provider_account:
        raise HTTPException(status_code=403, detail="Invalid calendar webhook")

    stored_resource_id = str(channel.get("resource_id") or "")
    if channel.get("status") == "active":
        if not stored_resource_id or not hmac.compare_digest(resource_id, stored_resource_id):
            raise HTTPException(status_code=403, detail="Invalid calendar webhook")
    elif resource_state != "sync":
        # The only callback allowed during the provider-registration race is
        # Google's initial sync handshake, authenticated by the channel secret.
        raise HTTPException(status_code=409, detail="Calendar channel is not active")
    else:
        await database.calendar_webhook_channels_collection.update_one(
            {"channel_id": channel_id, "status": "pending", "resource_id": None},
            {"$set": {"resource_id": resource_id}},
        )

    # Initial sync confirmation — just acknowledge
    if resource_state == "sync":
        return {"ok": True}

    try:
        message_number = int(message_number_raw)
        if message_number <= 0:
            raise ValueError
    except (TypeError, ValueError):
        raise HTTPException(status_code=400, detail="Invalid message number")

    # Claim this monotonically increasing delivery before doing work. Provider
    # retries and replays receive 200 but cannot fan out duplicate sync jobs.
    claimed = await database.calendar_webhook_channels_collection.update_one(
        {
            "channel_id": channel_id,
            "status": "active",
            "resource_id": resource_id,
            "last_message_number": {"$lt": message_number},
        },
        {"$set": {"last_message_number": message_number}},
    )
    if claimed.matched_count != 1:
        return {"ok": True, "duplicate": True}

    # A calendar change occurred. Mark only meetings bound to this exact tenant
    # and Google account due; the leased scheduler performs provider I/O. Google
    # webhook acknowledgements must not wait on a fan-out of event reads.
    if resource_state == "exists":
        await database.meetings_collection.update_many(
            {
                "account_id": account_id,
                "calendar_provider": "google",
                "calendar_provider_account_id": provider_account_id,
                "calendar_id": {"$type": "string"},
                "calendar_event_id": {"$type": "string"},
                "status": {"$in": ["booked", "rescheduled"]},
            },
            {
                "$set": {
                    "next_calendar_sync_at": datetime.utcnow(),
                    "calendar_webhook_message_number": message_number,
                }
            },
        )
    elif resource_state == "not_exists":
        await database.calendar_webhook_channels_collection.update_one(
            {"channel_id": channel_id, "account_id": account_id},
            {"$set": {"status": "stopped"}},
        )

    return {"ok": True}


# ---------------------------------------------------------------------------
# Register push notification channel
# ---------------------------------------------------------------------------

@router.post("/register-watch")
async def register_watch(
    request: Request,
    account_ctx: dict = Depends(get_account_context),
):
    """
    Register a Google Calendar push notification channel for the authenticated account.

    The callback is fixed to the configured OutFlo API origin; tenants cannot
    redirect provider notifications to arbitrary hosts.
    """
    account_id = str(account_ctx["account"]["_id"])

    from config import get_settings
    settings = get_settings()
    webhook_url = f"{settings.api_base_url.rstrip('/')}/api/calendar/webhooks/google"

    try:
        result = await register_calendar_watch(account_id, webhook_url)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc))
    if not result:
        raise HTTPException(
            status_code=400,
            detail="Failed to register calendar watch — ensure calendar scope is granted",
        )
    return result


# ---------------------------------------------------------------------------
# Calendar connection status
# ---------------------------------------------------------------------------

@router.get("/status")
async def calendar_status(account_ctx: dict = Depends(get_account_context)):
    """Return whether the account has an active calendar connection and the provider."""
    account_id = str(account_ctx["account"]["_id"])

    google_account = await database.email_accounts_collection.find_one({
        "account_id": account_id,
        "provider": "google",
        "oauth_scopes": {"$elemMatch": {"$regex": "calendar"}},
    })
    if google_account:
        return {
            "connected": True,
            "provider": "google",
            "email": google_account.get("email"),
        }
    return {"connected": False, "provider": None, "email": None}
