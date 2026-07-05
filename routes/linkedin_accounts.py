"""
LinkedIn account management routes.

Connects LinkedIn accounts via Unipile's hosted auth flow.
Router prefix: /api/linkedin-accounts
"""

from datetime import datetime, timezone, timedelta
from typing import Optional

import httpx
from bson import ObjectId
from fastapi import APIRouter, BackgroundTasks, Depends, HTTPException, Query, status
from pydantic import BaseModel

from auth import get_account_context
from config import get_settings
import database
from models.linkedin_account import LinkedInAccountDocument, LinkedInAccountResponse

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
    Import any Unipile LinkedIn accounts not yet in MongoDB for this organization.

    Fetches all accounts from Unipile, filters to LinkedIn type, and creates
    local records for any that are unclaimed (not yet linked to any organization).
    Idempotent — safe to call multiple times.
    """
    from services.unipile_service import UnipileClient, UnipileAPIError as _UnipileAPIError

    settings = get_settings()
    account_id = account_ctx["account"]["_id"]
    user_id = account_ctx["user"]["_id"]
    now = datetime.now(timezone.utc)

    # Fetch all Unipile accounts
    try:
        client = UnipileClient()
        all_accounts = await client.get_accounts()
    except Exception as exc:
        raise HTTPException(
            status_code=status.HTTP_502_BAD_GATEWAY,
            detail=f"Failed to fetch Unipile accounts: {exc}",
        )

    # Filter to LinkedIn-type accounts only
    linkedin_accounts = [
        acc for acc in all_accounts
        if (acc.get("type") or acc.get("provider") or "").upper() == "LINKEDIN"
    ]

    if not linkedin_accounts:
        return {"message": "No LinkedIn accounts found in Unipile", "synced": 0}

    # Find which unipile_account_ids are already claimed by ANY organization
    all_unipile_ids = [acc["id"] for acc in linkedin_accounts]
    cursor = database.linkedin_accounts_collection.find(
        {"unipile_account_id": {"$in": all_unipile_ids}},
        {"unipile_account_id": 1},
    )
    claimed_ids = {doc["unipile_account_id"] async for doc in cursor}

    # Import unclaimed accounts into this organization
    synced = 0
    for acc in linkedin_accounts:
        unipile_id = acc["id"]
        if unipile_id in claimed_ids:
            continue

        # Fetch profile details from Unipile (account status + user profile)
        try:
            profile_data = await _fetch_unipile_profile(unipile_id, settings)
            # First pass to extract public_id so we can call the users endpoint
            partial = _extract_profile_fields(profile_data)
            user_profile: dict = {}
            if partial.get("public_id"):
                user_profile = await _fetch_unipile_user_profile(unipile_id, partial["public_id"], settings)
            profile_fields = _extract_profile_fields(profile_data, user_profile)
        except Exception:
            profile_fields = {"unipile_status": acc.get("status", "OK")}

        doc = LinkedInAccountDocument(
            account_id=account_id,
            user_id=user_id,
            unipile_account_id=unipile_id,
            unipile_status=profile_fields.pop("unipile_status", acc.get("status", "OK")),
            profile_id=profile_fields.get("profile_id"),
            public_id=profile_fields.get("public_id"),
            name=profile_fields.get("name"),
            headline=profile_fields.get("headline"),
            profile_photo_url=profile_fields.get("profile_photo_url"),
            profile_url=profile_fields.get("profile_url"),
            connections_count=profile_fields.get("connections_count"),
            followers_count=profile_fields.get("followers_count"),
            last_profile_sync_at=now,
            created_at=now,
            updated_at=now,
        )
        await database.linkedin_accounts_collection.insert_one(doc.model_dump())
        synced += 1

    return {"message": f"Synced {synced} new LinkedIn account(s)", "synced": synced}


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

    # Build redirect URLs — the frontend can override these via query params if needed
    frontend_url = settings.frontend_url
    success_redirect_url = f"{frontend_url}/linkedin?status=success"
    failure_redirect_url = f"{frontend_url}/linkedin?status=failed"

    # Unipile hosted-auth v2 schema: type="create", providers=["LINKEDIN"],
    # expiresOn (ISO 8601 UTC with ms), api_url (server root, no /api/v1 suffix)
    expires_on = (datetime.now(timezone.utc) + timedelta(hours=24)).strftime(
        "%Y-%m-%dT%H:%M:%S.000Z"
    )
    api_url = settings.unipile_base_url.removesuffix("/api/v1")

    payload = {
        "type": "create",
        "providers": ["LINKEDIN"],
        "expiresOn": expires_on,
        "api_url": api_url,
        "name": str(account_id),
        "success_redirect_url": success_redirect_url,
        "failure_redirect_url": failure_redirect_url,
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
    Called by Unipile after a successful LinkedIn OAuth flow.
    Creates or updates the linkedin_accounts record and fetches the profile.
    """
    settings = get_settings()
    account_id = account_ctx["account"]["_id"]
    user_id = account_ctx["user"]["_id"]
    now = datetime.now(timezone.utc)

    # Fetch profile from Unipile (account status + user profile for photo/stats)
    profile_data = await _fetch_unipile_profile(body.unipile_account_id, settings)
    partial = _extract_profile_fields(profile_data)
    user_profile: dict = {}
    if partial.get("public_id"):
        user_profile = await _fetch_unipile_user_profile(body.unipile_account_id, partial["public_id"], settings)
    profile_fields = _extract_profile_fields(profile_data, user_profile)

    existing = await database.linkedin_accounts_collection.find_one(
        {"unipile_account_id": body.unipile_account_id}
    )

    if existing:
        update_fields = {
            **profile_fields,
            "last_profile_sync_at": now,
            "updated_at": now,
        }
        await database.linkedin_accounts_collection.update_one(
            {"_id": existing["_id"]},
            {"$set": update_fields},
        )
    else:
        doc = LinkedInAccountDocument(
            account_id=account_id,
            user_id=user_id,
            unipile_account_id=body.unipile_account_id,
            unipile_status=profile_fields.pop("unipile_status", body.status),
            profile_id=profile_fields.get("profile_id"),
            public_id=profile_fields.get("public_id"),
            name=profile_fields.get("name"),
            headline=profile_fields.get("headline"),
            profile_photo_url=profile_fields.get("profile_photo_url"),
            profile_url=profile_fields.get("profile_url"),
            connections_count=profile_fields.get("connections_count"),
            followers_count=profile_fields.get("followers_count"),
            last_profile_sync_at=now,
            created_at=now,
            updated_at=now,
        )
        await database.linkedin_accounts_collection.insert_one(doc.model_dump())
        from services.campaign_launch_service import replan_channels_on_sender_add
        background_tasks.add_task(replan_channels_on_sender_add, account_id, "linkedin")

    return {"message": "LinkedIn account connected"}


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
