"""
Google Calendar routes.

Provides:
  POST /api/calendar/webhooks/google  — Google push notification handler (no auth)
  POST /api/calendar/register-watch   — Register a push notification channel
  GET  /api/calendar/status           — Calendar connection status for the account
"""

import asyncio
import logging

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
      X-Goog-Channel-Token   — the token we set during register_calendar_watch (= account_id)
    """
    channel_id = request.headers.get("X-Goog-Channel-ID", "")
    resource_state = request.headers.get("X-Goog-Resource-State", "")
    account_id = request.headers.get("X-Goog-Channel-Token", "")

    logger.debug(
        f"Google Calendar webhook: channel={channel_id} state={resource_state} account={account_id}"
    )

    # Initial sync confirmation — just acknowledge
    if resource_state == "sync":
        return {"ok": True}

    # A calendar change occurred — trigger a background meeting status sync
    if resource_state == "exists" and account_id:
        try:
            from services.meeting_service import sync_meeting_statuses  # type: ignore
            asyncio.create_task(sync_meeting_statuses(account_id))
        except (ImportError, AttributeError):
            # sync_meeting_statuses may not exist yet; log and continue
            logger.debug(
                "sync_meeting_statuses not available yet — skipping calendar change sync"
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

    Optional body: {"webhook_url": "https://..."} — defaults to the api_base_url setting.
    """
    account_id = str(account_ctx["account"]["_id"])

    # Parse optional JSON body (webhook_url override)
    try:
        body = await request.json()
    except Exception:
        body = {}

    from config import get_settings
    settings = get_settings()
    webhook_url = body.get("webhook_url") or f"{settings.api_base_url}/api/calendar/webhooks/google"

    result = await register_calendar_watch(account_id, webhook_url)
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
    """Return whether the account has an active Google Calendar connection."""
    account_id = str(account_ctx["account"]["_id"])

    email_account = await database.email_accounts_collection.find_one({
        "account_id": account_id,
        "provider": "gmail",
        "oauth_scopes": {"$elemMatch": {"$regex": "calendar"}},
    })

    return {
        "connected": bool(email_account),
        "email": (email_account or {}).get("email"),
    }
