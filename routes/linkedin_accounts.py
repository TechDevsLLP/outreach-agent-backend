"""
LinkedIn account management routes.

Connects LinkedIn accounts via Unipile's hosted auth flow.
Router prefix: /api/linkedin-accounts
"""

import json
import logging
import secrets
from datetime import datetime, timezone, timedelta
from typing import Literal, Optional

import httpx
from bson import ObjectId
from fastapi import APIRouter, BackgroundTasks, Depends, HTTPException, Query, Request, status
from pydantic import BaseModel, Field

from auth import get_account_context
from config import get_settings
import database
from models.linkedin_account import LinkedInAccountDocument, LinkedInAccountResponse
from routes.webhooks import _verify_unipile_signature

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/linkedin-accounts", tags=["LinkedIn Accounts"])


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _serialize_linkedin_account(doc: dict) -> LinkedInAccountResponse:
    """Convert a raw MongoDB document to a LinkedInAccountResponse."""
    doc["_id"] = str(doc["_id"])
    return LinkedInAccountResponse(**doc)


async def _get_linkedin_account_or_404(linkedin_account_id: str, account_id: str) -> dict:
    """Fetch a LinkedIn account by its _id, ensuring it belongs to the given account."""
    try:
        oid = ObjectId(linkedin_account_id)
    except Exception:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="LinkedIn account not found")

    doc = await database.linkedin_accounts_collection.find_one(
        {"_id": oid, "account_id": account_id}
    )
    if doc is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="LinkedIn account not found")
    return doc


async def _fetch_unipile_profile(unipile_account_id: str, settings) -> dict:
    """
    Fetch account/profile info from Unipile for a given unipile_account_id.
    Returns the parsed JSON response dict.
    """
    async with httpx.AsyncClient() as client:
        resp = await client.get(
            f"{settings.unipile_base_url}/accounts/{unipile_account_id}",
            headers={"X-API-KEY": settings.unipile_token},
        )
    if resp.status_code != 200:
        raise HTTPException(
            status_code=status.HTTP_502_BAD_GATEWAY,
            detail=f"Failed to fetch Unipile profile: {resp.text}",
        )
    return resp.json()


async def _fetch_unipile_user_profile(unipile_account_id: str, public_id: str, settings) -> dict:
    """
    Fetch LinkedIn user profile from Unipile /users/{public_id} endpoint.
    This returns profile photo, follower/connection counts, headline, etc.
    Returns empty dict on failure so callers can degrade gracefully.
    """
    try:
        async with httpx.AsyncClient() as client:
            resp = await client.get(
                f"{settings.unipile_base_url}/users/{public_id}",
                headers={"X-API-KEY": settings.unipile_token},
                params={"account_id": unipile_account_id},
            )
        if resp.status_code == 200:
            return resp.json()
    except Exception:
        pass
    return {}


def _extract_profile_fields(profile_data: dict, user_profile: Optional[dict] = None) -> dict:
    """
    Extract normalized profile fields from Unipile API responses.

    profile_data: response from GET /accounts/{id}
      Relevant fields:
        connection_params.im.publicIdentifier  → LinkedIn username (public_id)
        connection_params.im.id                → provider_id
        name                                   → display name fallback
        sources[].status                       → account health

    user_profile: response from GET /users/{public_identifier}
      Relevant fields:
        public_identifier    → LinkedIn username
        provider_id          → LinkedIn member URN
        first_name, last_name
        headline
        profile_picture_url
        follower_count       → followers (singular)
        connections_count    → connections (plural — Unipile is inconsistent)
    """
    account_data = profile_data.get("account", profile_data)
    conn_params = account_data.get("connection_params", {})
    im = conn_params.get("im", {})
    up = user_profile or {}

    # public_id: Unipile nests it under connection_params.im.publicIdentifier
    public_id = (
        im.get("publicIdentifier")
        or im.get("public_identifier")
        or up.get("public_identifier")
        or up.get("public_id")
        or account_data.get("public_id")
    )

    # provider_id (LinkedIn member URN)
    profile_id = (
        im.get("id")
        or up.get("provider_id")
        or account_data.get("profile_id")
    )

    # Name: users endpoint returns first_name + last_name, accounts returns name
    if up.get("first_name") or up.get("last_name"):
        name = f"{up.get('first_name', '')} {up.get('last_name', '')}".strip()
    else:
        name = up.get("name") or account_data.get("name") or im.get("username")

    # Photo URL
    photo_url = (
        up.get("profile_picture_url_large")
        or up.get("profile_picture_url")
        or account_data.get("profile_picture_url")
    )

    # Counts: follower_count (singular), connections_count (plural) — verified from API
    followers_count = (
        up.get("follower_count")
        or up.get("followers_count")
        or account_data.get("follower_count")
        or account_data.get("followers_count")
    )
    connections_count = (
        up.get("connections_count")
        or up.get("connection_count")
        or account_data.get("connections_count")
        or account_data.get("connection_count")
    )

    # Profile URL
    profile_url = None
    if public_id:
        profile_url = f"https://www.linkedin.com/in/{public_id}"

    # Account status: check sources array first, then top-level status
    sources = account_data.get("sources", [])
    if sources:
        unipile_status = sources[0].get("status", "OK")
    else:
        unipile_status = account_data.get("status", "OK")

    return {
        "profile_id": profile_id,
        "public_id": public_id,
        "name": name,
        "headline": up.get("headline") or account_data.get("headline"),
        "profile_photo_url": photo_url,
        "profile_url": profile_url,
        "connections_count": connections_count,
        "followers_count": followers_count,
        "unipile_status": unipile_status,
    }


HOSTED_AUTH_TTL_HOURS = 24


async def _issue_hosted_auth_nonce(account_id: str, user_id: str) -> tuple[str, datetime]:
    """Record a pending hosted-auth request and return its (nonce, expires_at).

    Unipile's hosted-auth ``notify_url`` callback carries no signature or shared
    secret — there is no way to configure headers on it — so the callback proves
    its provenance by echoing back a nonce that only we could have issued.
    """
    nonce = secrets.token_urlsafe(32)
    expires_at = datetime.now(timezone.utc) + timedelta(hours=HOSTED_AUTH_TTL_HOURS)
    await database.linkedin_auth_requests_collection.insert_one(
        {
            "nonce": nonce,
            "account_id": account_id,
            "user_id": user_id,
            "created_at": datetime.now(timezone.utc),
            "expires_at": expires_at,
        }
    )
    return nonce, expires_at


async def _consume_hosted_auth_nonce(nonce: str) -> Optional[dict]:
    """Atomically claim a pending hosted-auth request, or return None.

    Deleting on read makes the nonce single-use, so a replayed or forged notify
    body cannot re-bind a Unipile account to a tenant. The TTL index on
    ``expires_at`` reaps requests the user abandoned, but Mongo's TTL reaper runs
    only once a minute, so expiry is re-checked here rather than trusted.
    """
    if not nonce:
        return None
    doc = await database.linkedin_auth_requests_collection.find_one_and_delete({"nonce": nonce})
    if not doc:
        return None
    expires_at = doc.get("expires_at")
    if expires_at is not None:
        if expires_at.tzinfo is None:
            expires_at = expires_at.replace(tzinfo=timezone.utc)
        if expires_at < datetime.now(timezone.utc):
            logger.warning("Hosted-auth nonce expired at %s; rejecting notify", expires_at)
            return None
    return doc


async def _upsert_linkedin_account_from_unipile(
    unipile_account_id: str,
    account_id: str,
    user_id: str,
    fallback_status: str,
    settings,
    background_tasks: BackgroundTasks,
) -> None:
    """
    Idempotently create/update a linkedin_accounts record from Unipile data.

    Race-safe against concurrent callers (notify webhook, /connect/webhook, /sync)
    via upsert keyed on the unique unipile_account_id index (database.py). Fires
    replan_channels_on_sender_add exactly once, only on first creation.
    """
    now = datetime.now(timezone.utc)

    profile_data = await _fetch_unipile_profile(unipile_account_id, settings)
    partial = _extract_profile_fields(profile_data)
    user_profile: dict = {}
    if partial.get("public_id"):
        user_profile = await _fetch_unipile_user_profile(unipile_account_id, partial["public_id"], settings)
    profile_fields = _extract_profile_fields(profile_data, user_profile)
    unipile_status = profile_fields.pop("unipile_status", fallback_status)

    result = await database.linkedin_accounts_collection.update_one(
        {"unipile_account_id": unipile_account_id},
        {
            "$set": {
                **profile_fields,
                "unipile_status": unipile_status,
                "last_profile_sync_at": now,
                "updated_at": now,
            },
            "$setOnInsert": {
                "account_id": account_id,
                "user_id": user_id,
                "unipile_account_id": unipile_account_id,
                "daily_connection_limit": 25,
                "daily_inmail_limit": 10,
                "daily_message_limit": 30,
                "warmup_enabled": True,
                "warmup_status": "warming",
                "warmup_day": 0,
                "warmup_started_at": now,
                "created_at": now,
            },
        },
        upsert=True,
    )

    if result.upserted_id is not None:
        from services.campaign_launch_service import replan_channels_on_sender_add
        background_tasks.add_task(replan_channels_on_sender_add, account_id, "linkedin")
        # First LinkedIn connect for this tenant: sync the sender voice profile
        # in the background so outreach generation has voice data even when the
        # user connected LinkedIn outside/after the onboarding wizard stage 2.
        background_tasks.add_task(_sync_sender_voice_after_connect, account_id)


async def _sync_sender_voice_after_connect(account_id: str) -> None:
    """Background: pull posts + synthesize sender voice after a LinkedIn connect.

    Skips if a good (non-low-confidence) voice profile already exists so a
    reconnect never clobbers a curated profile. Errors are logged and recorded
    on company_profiles.sender_voice_sync_error by the service — never raised.
    """
    try:
        profile = await database.company_profiles_collection.find_one(
            {"account_id": account_id},
            {"sender_voice_profile": 1},
        )
        existing = (profile or {}).get("sender_voice_profile") or {}
        if existing and not existing.get("low_confidence"):
            logger.info(f"[voice-sync] account={account_id}: voice profile already present, skipping post-connect sync")
            return
        from services.sender_voice_service import update_sender_voice_from_unipile
        await update_sender_voice_from_unipile(account_id)
    except Exception as e:
        logger.error(f"[voice-sync] post-connect voice sync failed for account={account_id}: {e}", exc_info=True)


# ---------------------------------------------------------------------------
# GET /api/linkedin-accounts
# ---------------------------------------------------------------------------

@router.get("", response_model=list[LinkedInAccountResponse])
async def list_linkedin_accounts(
    account_ctx: dict = Depends(get_account_context),
):
    """List all LinkedIn accounts for the current account, sorted newest first."""
    account_id = account_ctx["account"]["_id"]
    cursor = database.linkedin_accounts_collection.find(
        {"account_id": account_id}
    ).sort("created_at", -1)
    docs = await cursor.to_list(length=200)
    return [_serialize_linkedin_account(doc) for doc in docs]


# ---------------------------------------------------------------------------
# POST /api/linkedin-accounts/sync
# ---------------------------------------------------------------------------

@router.post("/sync")
async def sync_linkedin_accounts_from_unipile(
    account_ctx: dict = Depends(get_account_context),
):
    """
    Retired: workspace-wide account discovery cannot prove tenant ownership.

    The hosted-auth callback is the only supported linking path because it
    carries the account context established before the provider redirect.
    """
    raise HTTPException(
        status_code=status.HTTP_409_CONFLICT,
        detail="Workspace account sync is disabled. Connect LinkedIn using hosted auth.",
    )


# ---------------------------------------------------------------------------
# POST /api/linkedin-accounts/connect/hosted-auth
# ---------------------------------------------------------------------------

@router.post("/connect/hosted-auth")
async def initiate_hosted_auth(
    account_ctx: dict = Depends(get_account_context),
):
    """
    Generate a Unipile hosted auth link for connecting a LinkedIn account.
    Returns {auth_url: str} for the frontend to redirect the user to.
    """
    settings = get_settings()
    account_id = account_ctx["account"]["_id"]
    user_id = account_ctx["user"]["_id"]

    # Build redirect URLs — the frontend can override these via query params if needed
    frontend_url = settings.frontend_url
    success_redirect_url = f"{frontend_url}/linkedin?status=success"
    failure_redirect_url = f"{frontend_url}/linkedin?status=failed"

    # Unipile hosted-auth v2 schema: type="create", providers=["LINKEDIN"],
    # expiresOn (ISO 8601 UTC with ms), api_url (server root, no /api/v1 suffix)
    nonce, expires_at = await _issue_hosted_auth_nonce(account_id, user_id)
    expires_on = expires_at.strftime("%Y-%m-%dT%H:%M:%S.000Z")
    api_url = settings.unipile_base_url.removesuffix("/api/v1")

    # "name" is echoed back verbatim on the notify_url callback — it's the only way
    # to carry our tenant + user identity through Unipile's hosted-auth flow back to
    # the notify webhook, since Unipile has no concept of our JWT/session. The nonce
    # rides along in the same field because that callback is otherwise unauthenticated:
    # it is what proves the callback answers a link *we* issued for *this* tenant.
    payload = {
        "type": "create",
        "providers": ["LINKEDIN"],
        "expiresOn": expires_on,
        "api_url": api_url,
        "name": f"{account_id}:{user_id}:{nonce}",
        "success_redirect_url": success_redirect_url,
        "failure_redirect_url": failure_redirect_url,
        "notify_url": f"{settings.api_base_url}/api/linkedin-accounts/connect/notify",
    }

    async with httpx.AsyncClient() as client:
        resp = await client.post(
            f"{settings.unipile_base_url}/hosted/accounts/link",
            json=payload,
            headers={"X-API-KEY": settings.unipile_token},
        )

    if resp.status_code not in (200, 201):
        raise HTTPException(
            status_code=status.HTTP_502_BAD_GATEWAY,
            detail=f"Unipile hosted auth request failed ({resp.status_code}): {resp.text}",
        )

    data = resp.json()
    auth_url = data.get("url") or data.get("auth_url") or data.get("link")
    if not auth_url:
        raise HTTPException(
            status_code=status.HTTP_502_BAD_GATEWAY,
            detail=f"Unipile did not return an auth URL. Response: {data}",
        )

    return {"auth_url": auth_url}


# ---------------------------------------------------------------------------
# POST /api/linkedin-accounts/connect/webhook
# ---------------------------------------------------------------------------

class LinkedInWebhookBody(BaseModel):
    unipile_account_id: str
    status: str = "OK"


@router.post("/connect/webhook")
async def linkedin_webhook(
    body: LinkedInWebhookBody,
    background_tasks: BackgroundTasks,
    account_ctx: dict = Depends(get_account_context),
):
    """
    Authenticated fallback: lets a logged-in frontend confirm a LinkedIn connection
    itself if it somehow already knows the Unipile account id. Unipile itself cannot
    call this route (it carries no JWT) — the real Unipile callback is the public
    /connect/notify route below, wired via notify_url in initiate_hosted_auth.
    """
    settings = get_settings()
    account_id = account_ctx["account"]["_id"]
    user_id = account_ctx["user"]["_id"]

    await _upsert_linkedin_account_from_unipile(
        unipile_account_id=body.unipile_account_id,
        account_id=account_id,
        user_id=user_id,
        fallback_status=body.status,
        settings=settings,
        background_tasks=background_tasks,
    )

    return {"message": "LinkedIn account connected"}


# ---------------------------------------------------------------------------
# POST /api/linkedin-accounts/connect/notify
# ---------------------------------------------------------------------------

@router.post("/connect/notify")
async def linkedin_connect_notify(request: Request, background_tasks: BackgroundTasks):
    """
    Public Unipile hosted-auth callback (notify_url set in initiate_hosted_auth).

    Unlike /connect/webhook, this carries no JWT — Unipile calls it directly once
    the hosted-auth flow completes, so the onboarding UI's connection state is
    reflected server-side even if the frontend tab is never refocused (e.g. the
    popup lingers on a success screen through a slow 2FA login).

    Tenant + user identity is recovered from the "name" field we set on the
    hosted-auth request (f"{account_id}:{user_id}:{nonce}" in initiate_hosted_auth),
    since Unipile echoes it back verbatim.

    **This callback is not signed.** Unipile's hosted-auth link accepts a bare
    ``notify_url`` with no way to attach a shared secret or signing key — only
    webhooks created through ``/webhooks`` carry the headers that
    ``_verify_unipile_signature`` checks. Requiring a signature here therefore
    rejected every real callback in any environment with UNIPILE_WEBHOOK_SECRET
    set (which config.py makes mandatory in production): the account linked fine
    at Unipile and then never appeared in the dashboard, because /sync is retired
    and the notify callback is the only path that creates the record.

    Provenance is instead proven by the single-use nonce minted in
    initiate_hosted_auth: an unsigned body is trusted only when its nonce matches
    a pending request we issued, and the tenant is taken from that stored request
    rather than from the request body. A validly signed request is still accepted
    without a nonce so links issued before this change (and any future signed
    delivery) keep working.
    """
    settings = get_settings()
    body = await request.body()
    x_webhook_secret = request.headers.get("X-Webhook-Secret", "")
    signature_header = getattr(
        settings, "unipile_webhook_signature_header", "X-Unipile-Signature"
    )
    x_unipile_signature = request.headers.get(signature_header, "")
    x_unipile_timestamp = request.headers.get("X-Unipile-Timestamp", "")
    sig = x_unipile_signature or x_webhook_secret
    signature_ok = _verify_unipile_signature(
        settings.unipile_webhook_secret, body, sig, x_unipile_timestamp
    )

    try:
        payload = json.loads(body)
    except Exception:
        raise HTTPException(status_code=400, detail="Invalid JSON")

    unipile_status = str(payload.get("status") or "")
    unipile_account_id = payload.get("account_id")
    name = payload.get("name") or ""

    # Only a successful (re)connection creates/updates a record — everything else
    # (CREATION_FAILED, etc.) is ack'd so Unipile doesn't retry. Checked before the
    # nonce is consumed so a failed attempt doesn't burn the user's pending link.
    if unipile_status.upper() not in ("CREATION_SUCCESS", "RECONNECTED") or not unipile_account_id:
        logger.info(f"Ignoring Unipile connect/notify status={unipile_status!r}")
        return {"status": "ignored"}

    account_id, _, rest = name.partition(":")
    user_id, _, nonce = rest.partition(":")

    pending = await _consume_hosted_auth_nonce(nonce)
    if pending is not None:
        # Trust the stored request, not the body: the body only proved it holds
        # the nonce, so the nonce decides which tenant this account belongs to.
        account_id = pending["account_id"]
        user_id = pending.get("user_id") or ""
    elif not signature_ok:
        # Unipile retries a notify it couldn't confirm, and the first (successful)
        # delivery already burned the nonce. Ack a retry that names an account we
        # have already linked so the provider stops retrying — it changes no state,
        # so it grants a forged body nothing.
        already_linked = await database.linkedin_accounts_collection.find_one(
            {"unipile_account_id": unipile_account_id}, {"_id": 1}
        )
        if already_linked:
            logger.info(
                "Acking replayed Unipile connect/notify for already-linked account %s",
                unipile_account_id,
            )
            return {"status": "ignored", "reason": "already linked"}
        logger.warning(
            "Rejecting Unipile connect/notify: no valid signature and no pending "
            "hosted-auth request for name=%r",
            name,
        )
        raise HTTPException(status_code=403, detail="Unrecognized hosted-auth callback")

    if not account_id:
        logger.warning(f"Unipile connect/notify missing account_id in name={name!r}")
        return {"status": "ignored", "reason": "missing account_id in name"}

    await _upsert_linkedin_account_from_unipile(
        unipile_account_id=unipile_account_id,
        account_id=account_id,
        user_id=user_id,  # "" if the name had no ":" (stale/malformed link) — account_id is still correct
        fallback_status=unipile_status or "OK",
        settings=settings,
        background_tasks=background_tasks,
    )

    return {"status": "ok"}


# ---------------------------------------------------------------------------
# GET /api/linkedin-accounts/{linkedin_account_id}
# ---------------------------------------------------------------------------

@router.get("/{linkedin_account_id}", response_model=LinkedInAccountResponse)
async def get_linkedin_account(
    linkedin_account_id: str,
    account_ctx: dict = Depends(get_account_context),
):
    """Get a specific LinkedIn account by its MongoDB _id."""
    account_id = account_ctx["account"]["_id"]
    doc = await _get_linkedin_account_or_404(linkedin_account_id, account_id)
    return _serialize_linkedin_account(doc)


class LinkedInAccountUpdateRequest(BaseModel):
    daily_connection_limit: Optional[int] = Field(default=None, ge=1, le=80)
    daily_inmail_limit: Optional[int] = Field(default=None, ge=1, le=20)
    daily_message_limit: Optional[int] = Field(default=None, ge=1, le=50)
    warmup_enabled: Optional[bool] = None
    warmup_status: Optional[Literal["warming", "active", "paused"]] = None


@router.patch("/{linkedin_account_id}", response_model=LinkedInAccountResponse)
async def update_linkedin_account(
    linkedin_account_id: str,
    body: LinkedInAccountUpdateRequest,
    account_ctx: dict = Depends(get_account_context),
):
    """Configure channel-specific safety limits and sender warm-up state."""
    account_id = account_ctx["account"]["_id"]
    doc = await _get_linkedin_account_or_404(linkedin_account_id, account_id)
    update_fields = {
        key: value
        for key, value in body.model_dump(exclude_none=True).items()
    }
    update_fields["updated_at"] = datetime.now(timezone.utc)
    if body.warmup_enabled is True and not doc.get("warmup_started_at"):
        update_fields["warmup_started_at"] = datetime.now(timezone.utc)
        update_fields.setdefault("warmup_status", "warming")
    elif body.warmup_enabled is False:
        update_fields["warmup_status"] = "active"
    if body.warmup_status == "warming" and not doc.get("warmup_started_at"):
        update_fields["warmup_started_at"] = datetime.now(timezone.utc)

    await database.linkedin_accounts_collection.update_one(
        {"_id": doc["_id"], "account_id": account_id},
        {"$set": update_fields},
    )
    updated = await database.linkedin_accounts_collection.find_one(
        {"_id": doc["_id"], "account_id": account_id}
    )
    return _serialize_linkedin_account(updated)


# ---------------------------------------------------------------------------
# DELETE /api/linkedin-accounts/{linkedin_account_id}
# ---------------------------------------------------------------------------

@router.delete("/{linkedin_account_id}")
async def delete_linkedin_account(
    linkedin_account_id: str,
    account_ctx: dict = Depends(get_account_context),
):
    """Remove a LinkedIn account record."""
    account_id = account_ctx["account"]["_id"]
    doc = await _get_linkedin_account_or_404(linkedin_account_id, account_id)

    await database.linkedin_accounts_collection.delete_one({"_id": doc["_id"]})
    return {"message": "LinkedIn account disconnected"}


# ---------------------------------------------------------------------------
# POST /api/linkedin-accounts/{linkedin_account_id}/sync-profile
# ---------------------------------------------------------------------------

@router.post("/{linkedin_account_id}/sync-profile")
async def sync_linkedin_profile(
    linkedin_account_id: str,
    account_ctx: dict = Depends(get_account_context),
):
    """Fetch the latest profile data from Unipile and update the stored record."""
    settings = get_settings()
    account_id = account_ctx["account"]["_id"]
    doc = await _get_linkedin_account_or_404(linkedin_account_id, account_id)

    unipile_account_id = doc.get("unipile_account_id")
    if not unipile_account_id:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="No Unipile account ID stored for this LinkedIn account",
        )

    now = datetime.now(timezone.utc)
    profile_data = await _fetch_unipile_profile(unipile_account_id, settings)
    # First pass to get public_id, then fetch full user profile for photo/stats
    partial = _extract_profile_fields(profile_data)
    # Prefer stored public_id if fresh fetch didn't return one
    public_id = partial.get("public_id") or doc.get("public_id")
    user_profile: dict = {}
    if public_id:
        user_profile = await _fetch_unipile_user_profile(unipile_account_id, public_id, settings)
    profile_fields = _extract_profile_fields(profile_data, user_profile)

    update_fields = {
        **profile_fields,
        "last_profile_sync_at": now,
        "updated_at": now,
    }
    await database.linkedin_accounts_collection.update_one(
        {"_id": doc["_id"]},
        {"$set": update_fields},
    )
    updated = await database.linkedin_accounts_collection.find_one({"_id": doc["_id"]})
    return {
        "message": "Profile synced",
        "account": _serialize_linkedin_account(updated),
    }


# ---------------------------------------------------------------------------
# POST /api/linkedin-accounts/{linkedin_account_id}/set-default
# ---------------------------------------------------------------------------

@router.post("/{linkedin_account_id}/set-default")
async def set_default_linkedin_account(
    linkedin_account_id: str,
    account_ctx: dict = Depends(get_account_context),
):
    """
    Mark this LinkedIn account as the default for the current account.
    Clears is_default on all other LinkedIn accounts for this account.
    """
    account_id = account_ctx["account"]["_id"]
    doc = await _get_linkedin_account_or_404(linkedin_account_id, account_id)

    now = datetime.now(timezone.utc)

    # Clear is_default on all other accounts for this account
    await database.linkedin_accounts_collection.update_many(
        {"account_id": account_id, "_id": {"$ne": doc["_id"]}},
        {"$set": {"is_default": False, "updated_at": now}},
    )

    # Set is_default on the target account
    await database.linkedin_accounts_collection.update_one(
        {"_id": doc["_id"]},
        {"$set": {"is_default": True, "updated_at": now}},
    )

    return {"message": "Set as default"}


# ---------------------------------------------------------------------------
# GET /api/linkedin-accounts/{linkedin_account_id}/connection-requests
# ---------------------------------------------------------------------------

@router.get("/{linkedin_account_id}/connection-requests")
async def list_connection_requests(
    linkedin_account_id: str,
    status_filter: Optional[str] = Query(default=None, alias="status"),
    page: int = Query(default=1, ge=1),
    page_size: int = Query(default=50, ge=1, le=200),
    account_ctx: dict = Depends(get_account_context),
):
    """
    List connection requests associated with a LinkedIn account.
    Supports optional status filtering and pagination.
    """
    account_id = account_ctx["account"]["_id"]

    # Validate the LinkedIn account exists and belongs to this account
    await _get_linkedin_account_or_404(linkedin_account_id, account_id)

    query: dict = {"linkedin_account_id": linkedin_account_id}
    if status_filter:
        query["status"] = status_filter

    skip = (page - 1) * page_size
    cursor = (
        database.linkedin_connection_requests_collection.find(query)
        .sort("created_at", -1)
        .skip(skip)
        .limit(page_size)
    )
    docs = await cursor.to_list(length=page_size)

    # Serialize ObjectIds
    for doc in docs:
        doc["_id"] = str(doc["_id"])

    return docs


# ---------------------------------------------------------------------------
# Hosted auth completion signal (polled by frontend after popup closes)
# ---------------------------------------------------------------------------

@router.get("/pending-auth/{account_name}")
async def pending_auth_status(
    account_name: str,
    account_ctx: dict = Depends(get_account_context),
):
    """
    Lightweight check if a LinkedIn account connected via hosted auth.
    The frontend polls this after the hosted-auth popup returns.

    Returns {connected: bool, account_id: str | null}.
    """
    doc = await database.linkedin_accounts_collection.find_one(
        {"account_id": account_ctx["account"]["_id"], "unipile_account_id": account_name},
    )
    if doc:
        return {"connected": True, "account_id": str(doc["_id"])}
    return {"connected": False, "account_id": None}
