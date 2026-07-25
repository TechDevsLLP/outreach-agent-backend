"""
Email account management routes.

Supports Gmail API (Google OAuth), Zoho Mail API (Zoho OAuth), and custom
SMTP+IMAP providers. Legacy Microsoft records remain visible/disconnectable,
but new Microsoft connections are disabled until delivery is implemented.
Router prefix: /api/email-accounts
"""

import logging
import secrets
from datetime import datetime, timezone
from typing import Literal, Optional
from urllib.parse import urlsplit

import httpx
from jose import JWTError, jwt
from bson import ObjectId
from fastapi import APIRouter, BackgroundTasks, Body, Depends, HTTPException, Query, status
from fastapi.responses import JSONResponse
from pydantic import BaseModel, EmailStr, Field

from auth import get_account_context
from config import get_settings
import database
from models.email_account import (
    EmailAccountDocument,
    EmailAccountResponse,
    SmtpAccountRequest,
)
from services.email_account_crypto import encrypt_account_fields

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/email-accounts", tags=["Email Accounts"])

MICROSOFT_LAUNCH_DISABLED_DETAIL = (
    "Microsoft 365 connections are not supported in this launch. "
    "Use Google, Zoho, or SMTP/IMAP instead."
)
OAUTH_STATE_TTL_SECONDS = 600
OAUTH_STATE_ISSUER = "outflo-oauth-state"
OAUTH_STATE_AUDIENCE = "outflo-oauth-callback"


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _validated_return_to(return_to: Optional[str]) -> Optional[str]:
    """Allow only a local absolute path in optional OAuth navigation state."""
    if not return_to:
        return None
    parsed = urlsplit(return_to)
    if (
        not return_to.startswith("/")
        or return_to.startswith("//")
        or parsed.scheme
        or parsed.netloc
    ):
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="OAuth return_to must be a same-origin relative path",
        )
    return return_to


async def _issue_oauth_state(
    *, account_id: str, provider: str, redirect_uri: str, return_to: Optional[str]
) -> str:
    """Issue signed, expiring OAuth state and persist its one-time nonce."""
    settings = get_settings()
    now = datetime.now(timezone.utc)
    expires_at = datetime.fromtimestamp(
        now.timestamp() + OAUTH_STATE_TTL_SECONDS, tz=timezone.utc
    )
    jti = secrets.token_urlsafe(24)
    normalized_return_to = _validated_return_to(return_to)
    payload = {
        "iss": OAUTH_STATE_ISSUER,
        "aud": OAUTH_STATE_AUDIENCE,
        "purpose": "email_account_oauth",
        "sub": str(account_id),
        "provider": provider,
        "redirect_uri": redirect_uri,
        "return_to": normalized_return_to,
        "jti": jti,
        "iat": now,
        "exp": expires_at,
    }
    await database.oauth_state_nonces_collection.insert_one(
        {
            "_id": jti,
            "account_id": str(account_id),
            "provider": provider,
            "redirect_uri": redirect_uri,
            "created_at": now,
            "expires_at": expires_at,
            "consumed_at": None,
        }
    )
    return jwt.encode(payload, settings.jwt_secret_key, algorithm="HS256")


async def _consume_oauth_state(
    state_token: str,
    *,
    account_id: str,
    provider: str,
    redirect_uri: str,
) -> dict:
    """Validate all OAuth bindings and atomically reject state replay."""
    settings = get_settings()
    invalid_state = HTTPException(
        status_code=status.HTTP_400_BAD_REQUEST,
        detail="Invalid, expired, or already used OAuth state",
    )
    try:
        payload = jwt.decode(
            state_token,
            settings.jwt_secret_key,
            algorithms=["HS256"],
            audience=OAUTH_STATE_AUDIENCE,
            issuer=OAUTH_STATE_ISSUER,
            options={"require_exp": True, "require_iat": True, "require_sub": True},
        )
    except JWTError:
        raise invalid_state

    if (
        payload.get("purpose") != "email_account_oauth"
        or payload.get("sub") != str(account_id)
        or payload.get("provider") != provider
        or payload.get("redirect_uri") != redirect_uri
        or not payload.get("jti")
    ):
        raise invalid_state

    now = datetime.now(timezone.utc)
    result = await database.oauth_state_nonces_collection.update_one(
        {
            "_id": payload["jti"],
            "account_id": str(account_id),
            "provider": provider,
            "redirect_uri": redirect_uri,
            "consumed_at": None,
            "expires_at": {"$gt": now},
        },
        {"$set": {"consumed_at": now}},
    )
    if result.matched_count != 1:
        raise invalid_state
    return payload


def _serialize_email_account(doc: dict) -> EmailAccountResponse:
    """Convert a raw MongoDB document to an EmailAccountResponse."""
    doc["_id"] = str(doc["_id"])
    return EmailAccountResponse(**doc)


async def _get_email_account_or_404(email_account_id: str, account_id: str) -> dict:
    """Fetch an email account by its _id, ensuring it belongs to the given account."""
    try:
        oid = ObjectId(email_account_id)
    except Exception:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Email account not found")

    doc = await database.email_accounts_collection.find_one(
        {"_id": oid, "account_id": account_id}
    )
    if doc is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Email account not found")
    return doc


# ---------------------------------------------------------------------------
# GET /api/email-accounts
# ---------------------------------------------------------------------------

@router.get("", response_model=list[EmailAccountResponse])
async def list_email_accounts(
    account_ctx: dict = Depends(get_account_context),
):
    """List all email accounts for the current account, sorted newest first."""
    account_id = account_ctx["account"]["_id"]
    cursor = database.email_accounts_collection.find(
        {"account_id": account_id}
    ).sort("created_at", -1)
    docs = await cursor.to_list(length=200)
    return [_serialize_email_account(doc) for doc in docs]


# ---------------------------------------------------------------------------
# POST /api/email-accounts/oauth/google/url
# ---------------------------------------------------------------------------

@router.get("/oauth/google/url")
async def google_oauth_url(
    redirect_uri: Optional[str] = Query(default=None),
    return_to: Optional[str] = Query(default=None),
    account_ctx: dict = Depends(get_account_context),
):
    """Generate a Google OAuth authorization URL."""
    settings = get_settings()
    account_id = str(account_ctx["account"]["_id"])
    effective_redirect_uri = redirect_uri or settings.google_redirect_uri
    oauth_state = await _issue_oauth_state(
        account_id=account_id,
        provider="google",
        redirect_uri=effective_redirect_uri,
        return_to=return_to,
    )

    import urllib.parse

    params = {
        "client_id": settings.google_client_id,
        "redirect_uri": effective_redirect_uri,
        "response_type": "code",
        "scope": "https://www.googleapis.com/auth/gmail.send https://www.googleapis.com/auth/gmail.readonly https://www.googleapis.com/auth/userinfo.email",
        "access_type": "offline",
        "prompt": "consent",
        "state": oauth_state,
    }
    auth_url = "https://accounts.google.com/o/oauth2/v2/auth?" + urllib.parse.urlencode(params)
    return {"auth_url": auth_url}


# ---------------------------------------------------------------------------
# POST /api/email-accounts/oauth/google/exchange
# ---------------------------------------------------------------------------

class GoogleExchangeRequest(BaseModel):
    code: str
    redirect_uri: str
    state: str


@router.post("/oauth/google/exchange", response_model=EmailAccountResponse)
async def google_oauth_exchange(
    body: GoogleExchangeRequest,
    background_tasks: BackgroundTasks,
    account_ctx: dict = Depends(get_account_context),
):
    """Exchange a Google OAuth code for tokens and create/update an email account."""
    settings = get_settings()
    account_id = account_ctx["account"]["_id"]
    user_id = account_ctx["user"]["_id"]

    await _consume_oauth_state(
        body.state,
        account_id=str(account_id),
        provider="google",
        redirect_uri=body.redirect_uri,
    )

    async with httpx.AsyncClient() as client:
        # Exchange code for tokens
        token_resp = await client.post(
            "https://oauth2.googleapis.com/token",
            data={
                "code": body.code,
                "client_id": settings.google_client_id,
                "client_secret": settings.google_client_secret,
                "redirect_uri": body.redirect_uri,
                "grant_type": "authorization_code",
            },
        )
        if token_resp.status_code != 200:
            google_error = token_resp.text
            logger.error(
                f"Google OAuth token exchange failed: HTTP {token_resp.status_code} — {google_error}"
            )
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail=f"Google token exchange failed: {google_error}",
            )
        token_data = token_resp.json()

        # Fetch user email
        userinfo_resp = await client.get(
            "https://www.googleapis.com/oauth2/v2/userinfo",
            headers={"Authorization": f"Bearer {token_data['access_token']}"},
        )
        if userinfo_resp.status_code != 200:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="Failed to fetch Google user info",
            )
        userinfo = userinfo_resp.json()

    email = userinfo.get("email", "")
    now = datetime.now(timezone.utc)

    # Calculate token expiry
    expires_in = token_data.get("expires_in", 3600)
    token_expiry = datetime.fromtimestamp(
        datetime.now(timezone.utc).timestamp() + expires_in, tz=timezone.utc
    )

    # Build update payload
    update_fields = {
        "account_id": account_id,
        "user_id": user_id,
        "email": email,
        "display_name": email,
        "provider": "google",
        "oauth_access_token": token_data.get("access_token"),
        "oauth_refresh_token": token_data.get("refresh_token"),
        "oauth_token_expiry": token_expiry,
        "oauth_scopes": token_data.get("scope", "").split(),
        "status": "connected",
        "updated_at": now,
    }
    update_fields = encrypt_account_fields(update_fields)

    existing = await database.email_accounts_collection.find_one(
        {"account_id": account_id, "email": email, "provider": "google"}
    )

    from services.campaign_launch_service import replan_channels_on_sender_add
    if existing:
        await database.email_accounts_collection.update_one(
            {"_id": existing["_id"]},
            {"$set": update_fields},
        )
        updated = await database.email_accounts_collection.find_one({"_id": existing["_id"]})
        return _serialize_email_account(updated)
    else:
        update_fields.update({
            "daily_send_limit": 50,
            "warmup_enabled": True,
            "warmup_status": "warming",
            "warmup_day": 0,
            "warmup_started_at": now,
        })
        update_fields["created_at"] = now
        result = await database.email_accounts_collection.insert_one(update_fields)
        created = await database.email_accounts_collection.find_one({"_id": result.inserted_id})
        background_tasks.add_task(replan_channels_on_sender_add, account_id, "email")
        return _serialize_email_account(created)


# ---------------------------------------------------------------------------
# GET /api/email-accounts/oauth/zoho/url
# ---------------------------------------------------------------------------

@router.get("/oauth/zoho/url")
async def zoho_oauth_url(
    redirect_uri: Optional[str] = Query(default=None),
    return_to: Optional[str] = Query(default=None),
    account_ctx: dict = Depends(get_account_context),
):
    """
    Generate a Zoho Mail OAuth authorization URL.

    Always starts at accounts.zoho.com — Zoho transparently redirects to the
    user's actual data center and returns an `accounts-server` query param on
    the callback, which the frontend must forward to the exchange endpoint.
    """
    from services.email_providers.zoho import ZOHO_ACCOUNTS_DOMAIN_DEFAULT, ZOHO_SCOPES

    settings = get_settings()
    account_id = str(account_ctx["account"]["_id"])
    effective_redirect_uri = redirect_uri or settings.zoho_redirect_uri
    oauth_state = await _issue_oauth_state(
        account_id=account_id,
        provider="zoho",
        redirect_uri=effective_redirect_uri,
        return_to=return_to,
    )

    import urllib.parse

    params = {
        "client_id": settings.zoho_client_id,
        "redirect_uri": effective_redirect_uri,
        "response_type": "code",
        "scope": ZOHO_SCOPES,
        "access_type": "offline",
        "prompt": "consent",
        "state": oauth_state,
    }
    auth_url = f"{ZOHO_ACCOUNTS_DOMAIN_DEFAULT}/oauth/v2/auth?" + urllib.parse.urlencode(params)
    return {"auth_url": auth_url}


# ---------------------------------------------------------------------------
# POST /api/email-accounts/oauth/zoho/exchange
# ---------------------------------------------------------------------------

class ZohoExchangeRequest(BaseModel):
    code: str
    redirect_uri: str
    state: str
    accounts_server: Optional[str] = None  # from the OAuth callback's `accounts-server` query param


@router.post("/oauth/zoho/exchange", response_model=EmailAccountResponse)
async def zoho_oauth_exchange(
    body: ZohoExchangeRequest,
    background_tasks: BackgroundTasks,
    account_ctx: dict = Depends(get_account_context),
):
    """Exchange a Zoho OAuth code for tokens and create/update an email account."""
    from services.email_providers.zoho import (
        get_zoho_accounts,
        get_zoho_inbox_folder_id,
        resolve_zoho_domains,
    )

    settings = get_settings()
    account_id = account_ctx["account"]["_id"]
    user_id = account_ctx["user"]["_id"]
    await _consume_oauth_state(
        body.state,
        account_id=str(account_id),
        provider="zoho",
        redirect_uri=body.redirect_uri,
    )
    try:
        accounts_domain, api_domain = resolve_zoho_domains(body.accounts_server)
    except ValueError:
        # `accounts_server` is callback input. Reject before constructing an
        # HTTP client so the platform client secret can only reach Zoho.
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Unsupported Zoho accounts server",
        )

    async with httpx.AsyncClient() as client:
        token_resp = await client.post(
            f"{accounts_domain}/oauth/v2/token",
            data={
                "code": body.code,
                "client_id": settings.zoho_client_id,
                "client_secret": settings.zoho_client_secret,
                "redirect_uri": body.redirect_uri,
                "grant_type": "authorization_code",
            },
        )
        if token_resp.status_code != 200:
            zoho_error = token_resp.text
            logger.error(
                f"Zoho OAuth token exchange failed: HTTP {token_resp.status_code} — {zoho_error}"
            )
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail=f"Zoho token exchange failed: {zoho_error}",
            )
        token_data = token_resp.json()
        if "error" in token_data:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail=f"Zoho token exchange failed: {token_data['error']}",
            )

    access_token = token_data["access_token"]
    accounts = await get_zoho_accounts(access_token, api_domain)
    if not accounts:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Failed to fetch Zoho account details after token exchange",
        )
    zoho_account = accounts[0]
    zoho_account_id = str(zoho_account.get("accountId", ""))
    email = (
        zoho_account.get("primaryEmailAddress")
        or (zoho_account.get("emailAddress") or [None])[0]
        or ""
    )
    send_details = zoho_account.get("sendMailDetails") or []
    from_address = send_details[0].get("fromAddress") if send_details else email

    inbox_folder_id = None
    if zoho_account_id:
        try:
            inbox_folder_id = await get_zoho_inbox_folder_id(access_token, api_domain, zoho_account_id)
        except Exception as e:
            logger.warning(f"Zoho inbox folder lookup failed for {email}: {e}")

    now = datetime.now(timezone.utc)
    expires_in = token_data.get("expires_in", 3600)
    token_expiry = datetime.fromtimestamp(now.timestamp() + expires_in, tz=timezone.utc)

    update_fields = {
        "account_id": account_id,
        "user_id": user_id,
        "email": email,
        "display_name": email,
        "provider": "zoho",
        "oauth_access_token": token_data.get("access_token"),
        "oauth_refresh_token": token_data.get("refresh_token"),
        "oauth_token_expiry": token_expiry,
        "oauth_scopes": [s for s in token_data.get("scope", "").replace(",", " ").split() if s],
        "zoho_account_id": zoho_account_id,
        "zoho_api_domain": api_domain,
        "zoho_accounts_domain": accounts_domain,
        "zoho_from_address": from_address,
        "zoho_inbox_folder_id": inbox_folder_id,
        "status": "connected",
        "updated_at": now,
    }
    update_fields = encrypt_account_fields(update_fields)

    existing = await database.email_accounts_collection.find_one(
        {"account_id": account_id, "email": email, "provider": "zoho"}
    )

    from services.campaign_launch_service import replan_channels_on_sender_add
    if existing:
        await database.email_accounts_collection.update_one(
            {"_id": existing["_id"]},
            {"$set": update_fields},
        )
        updated = await database.email_accounts_collection.find_one({"_id": existing["_id"]})
        return _serialize_email_account(updated)
    else:
        update_fields.update({
            "daily_send_limit": 40,
            "warmup_enabled": True,
            "warmup_status": "warming",
            "warmup_day": 0,
            "warmup_started_at": now,
        })
        update_fields["created_at"] = now
        result = await database.email_accounts_collection.insert_one(update_fields)
        created = await database.email_accounts_collection.find_one({"_id": result.inserted_id})
        background_tasks.add_task(replan_channels_on_sender_add, account_id, "email")
        return _serialize_email_account(created)


# ---------------------------------------------------------------------------
# POST /api/email-accounts/oauth/microsoft/url
# ---------------------------------------------------------------------------

@router.get("/oauth/microsoft/url")
async def microsoft_oauth_url(
    redirect_uri: Optional[str] = Query(default=None),
    account_ctx: dict = Depends(get_account_context),
):
    """Reject new Microsoft connections until a send provider is certified."""
    raise HTTPException(
        status_code=status.HTTP_410_GONE,
        detail=MICROSOFT_LAUNCH_DISABLED_DETAIL,
    )


# ---------------------------------------------------------------------------
# POST /api/email-accounts/oauth/microsoft/exchange
# ---------------------------------------------------------------------------

class MicrosoftExchangeRequest(BaseModel):
    code: str
    redirect_uri: str


@router.post("/oauth/microsoft/exchange", response_model=EmailAccountResponse)
async def microsoft_oauth_exchange(
    body: MicrosoftExchangeRequest,
    background_tasks: BackgroundTasks,
    account_ctx: dict = Depends(get_account_context),
):
    """Reject code exchange while Microsoft delivery remains unsupported."""
    raise HTTPException(
        status_code=status.HTTP_410_GONE,
        detail=MICROSOFT_LAUNCH_DISABLED_DETAIL,
    )


# ---------------------------------------------------------------------------
# POST /api/email-accounts/smtp
# ---------------------------------------------------------------------------

@router.post("/smtp", response_model=EmailAccountResponse, status_code=status.HTTP_201_CREATED)
async def connect_smtp(
    body: SmtpAccountRequest,
    background_tasks: BackgroundTasks,
    account_ctx: dict = Depends(get_account_context),
):
    """Connect a custom SMTP+IMAP email account. IMAP credentials are required."""
    account_id = account_ctx["account"]["_id"]
    user_id = account_ctx["user"]["_id"]
    now = datetime.now(timezone.utc)

    doc = EmailAccountDocument(
        account_id=account_id,
        user_id=user_id,
        email=body.email,
        display_name=body.display_name,
        provider="smtp",
        smtp_host=body.smtp_host,
        smtp_port=body.smtp_port,
        smtp_encryption=body.smtp_encryption,
        smtp_username=body.smtp_username,
        smtp_password=body.smtp_password,
        imap_host=body.imap_host,
        imap_port=body.imap_port,
        imap_encryption=body.imap_encryption,
        imap_username=body.imap_username,
        imap_password=body.imap_password,
        status="connected",
        warmup_enabled=True,
        warmup_status="warming",
        warmup_started_at=now,
        created_at=now,
        updated_at=now,
    )

    fields = encrypt_account_fields(doc.model_dump())
    result = await database.email_accounts_collection.insert_one(fields)
    created = await database.email_accounts_collection.find_one({"_id": result.inserted_id})
    from services.campaign_launch_service import replan_channels_on_sender_add
    background_tasks.add_task(replan_channels_on_sender_add, account_id, "email")
    return _serialize_email_account(created)


# ---------------------------------------------------------------------------
# GET /api/email-accounts/{email_account_id}
# ---------------------------------------------------------------------------

@router.get("/{email_account_id}", response_model=EmailAccountResponse)
async def get_email_account(
    email_account_id: str,
    account_ctx: dict = Depends(get_account_context),
):
    """Get a specific email account by its ID."""
    account_id = account_ctx["account"]["_id"]
    doc = await _get_email_account_or_404(email_account_id, account_id)
    return _serialize_email_account(doc)


# ---------------------------------------------------------------------------
# PATCH /api/email-accounts/{email_account_id}
# ---------------------------------------------------------------------------

class EmailAccountUpdateRequest(BaseModel):
    display_name: Optional[str] = None
    daily_send_limit: Optional[int] = Field(default=None, ge=1, le=500)
    warmup_enabled: Optional[bool] = None
    warmup_status: Optional[Literal["warming", "active", "paused"]] = None


@router.patch("/{email_account_id}", response_model=EmailAccountResponse)
async def update_email_account(
    email_account_id: str,
    body: EmailAccountUpdateRequest,
    account_ctx: dict = Depends(get_account_context),
):
    """Update mutable fields on an email account."""
    account_id = account_ctx["account"]["_id"]
    doc = await _get_email_account_or_404(email_account_id, account_id)

    update_fields: dict = {"updated_at": datetime.now(timezone.utc)}
    if body.display_name is not None:
        update_fields["display_name"] = body.display_name
    if body.daily_send_limit is not None:
        update_fields["daily_send_limit"] = body.daily_send_limit
    if body.warmup_enabled is not None:
        update_fields["warmup_enabled"] = body.warmup_enabled
        if body.warmup_enabled and not doc.get("warmup_started_at"):
            update_fields["warmup_started_at"] = datetime.now(timezone.utc)
            update_fields.setdefault("warmup_status", "warming")
        elif not body.warmup_enabled:
            update_fields["warmup_status"] = "active"
    if body.warmup_status is not None:
        update_fields["warmup_status"] = body.warmup_status
        if body.warmup_status == "warming" and not doc.get("warmup_started_at"):
            update_fields["warmup_started_at"] = datetime.now(timezone.utc)

    await database.email_accounts_collection.update_one(
        {"_id": doc["_id"]},
        {"$set": update_fields},
    )
    updated = await database.email_accounts_collection.find_one({"_id": doc["_id"]})
    return _serialize_email_account(updated)


# ---------------------------------------------------------------------------
# DELETE /api/email-accounts/{email_account_id}
# ---------------------------------------------------------------------------

@router.delete("/{email_account_id}")
async def delete_email_account(
    email_account_id: str,
    account_ctx: dict = Depends(get_account_context),
):
    """Remove an email account record."""
    account_id = account_ctx["account"]["_id"]
    doc = await _get_email_account_or_404(email_account_id, account_id)

    await database.email_accounts_collection.delete_one({"_id": doc["_id"]})
    return {"message": "Email account removed"}


# ---------------------------------------------------------------------------
# POST /api/email-accounts/{email_account_id}/test
# ---------------------------------------------------------------------------

class TestEmailRequest(BaseModel):
    to_email: Optional[EmailStr] = None
    subject: Optional[str] = None
    body: Optional[str] = None  # HTML allowed


@router.post("/{email_account_id}/test")
async def test_email_account(
    email_account_id: str,
    payload: Optional[TestEmailRequest] = Body(default=None),
    account_ctx: dict = Depends(get_account_context),
):
    """Send a real test email via the connected email account."""
    account_id = account_ctx["account"]["_id"]
    user = account_ctx["user"]
    doc = await _get_email_account_or_404(email_account_id, account_id)

    req = payload or TestEmailRequest()
    to_email = str(req.to_email) if req.to_email else user.get("email", "")
    if not to_email:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="No recipient email — provide to_email in the request body",
        )
    subject = req.subject or "Test email from OutFlo"
    html_body = req.body or (
        f"<p>This is a test email from OutFlo sent via "
        f"<b>{doc.get('email', '')}</b> ({doc.get('provider', '')}).</p>"
    )

    from services.email_delivery_service import send_email
    result = await send_email(doc, to_email, subject, html_body)

    provider = doc.get("provider", "unknown")
    if result is None:
        return JSONResponse(
            status_code=502,
            content={
                "success": False,
                "message": "Email send failed — check provider credentials/logs",
                "provider": provider,
                "message_id": None,
                "to_email": to_email,
            },
        )
    return {
        "success": True,
        "message": f"Test email sent via {result.get('provider', provider)}",
        "provider": result.get("provider", provider),
        "message_id": result.get("message_id"),
        "to_email": to_email,
    }


# ---------------------------------------------------------------------------
# GET /api/email-accounts/{email_account_id}/health
# ---------------------------------------------------------------------------

@router.get("/{email_account_id}/health")
async def get_email_account_health(
    email_account_id: str,
    account_ctx: dict = Depends(get_account_context),
):
    """Get health and usage metrics for an email account."""
    account_id = account_ctx["account"]["_id"]
    doc = await _get_email_account_or_404(email_account_id, account_id)

    return {
        "status": doc.get("status", "connected"),
        "health": doc.get("health", "healthy"),
        "emails_sent_today": doc.get("emails_sent_today", 0),
        "daily_send_limit": doc.get("daily_send_limit", 50),
        "bounce_rate": doc.get("bounce_rate", 0.0),
    }


# ---------------------------------------------------------------------------
# POST /api/email-accounts/{email_account_id}/refresh-token
# ---------------------------------------------------------------------------

@router.post("/{email_account_id}/refresh-token")
async def refresh_oauth_token(
    email_account_id: str,
    account_ctx: dict = Depends(get_account_context),
):
    """Force-refresh the OAuth access token for a Google or Zoho email account."""
    from utils.crypto import decrypt

    settings = get_settings()
    account_id = account_ctx["account"]["_id"]
    doc = await _get_email_account_or_404(email_account_id, account_id)

    provider = doc.get("provider")
    if provider == "microsoft":
        raise HTTPException(
            status_code=status.HTTP_410_GONE,
            detail=MICROSOFT_LAUNCH_DISABLED_DETAIL,
        )
    if provider not in ("google", "zoho"):
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"Token refresh is only supported for Google and Zoho accounts, not '{provider}'",
        )

    refresh_token = decrypt(doc.get("oauth_refresh_token"))
    if not refresh_token:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="No refresh token stored for this account",
        )

    async with httpx.AsyncClient() as client:
        if provider == "google":
            resp = await client.post(
                "https://oauth2.googleapis.com/token",
                data={
                    "client_id": settings.google_client_id,
                    "client_secret": settings.google_client_secret,
                    "refresh_token": refresh_token,
                    "grant_type": "refresh_token",
                },
            )
        elif provider == "zoho":
            from services.email_providers.zoho import resolve_zoho_domains

            try:
                accounts_domain, _ = resolve_zoho_domains(doc.get("zoho_accounts_domain"))
            except ValueError:
                raise HTTPException(
                    status_code=status.HTTP_400_BAD_REQUEST,
                    detail="Unsupported Zoho accounts server",
                )
            resp = await client.post(
                f"{accounts_domain}/oauth/v2/token",
                data={
                    "client_id": settings.zoho_client_id,
                    "client_secret": settings.zoho_client_secret,
                    "refresh_token": refresh_token,
                    "grant_type": "refresh_token",
                },
            )
    if resp.status_code != 200:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"Token refresh failed: {resp.text}",
        )

    token_data = resp.json()
    if "error" in token_data:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"Token refresh failed: {token_data['error']}",
        )

    expires_in = token_data.get("expires_in", 3600)
    now = datetime.now(timezone.utc)
    token_expiry = datetime.fromtimestamp(now.timestamp() + expires_in, tz=timezone.utc)

    update_fields: dict = {
        "oauth_access_token": token_data["access_token"],
        "oauth_token_expiry": token_expiry,
        "updated_at": now,
    }
    # Google may issue a new refresh token; Zoho usually does not.
    if token_data.get("refresh_token"):
        update_fields["oauth_refresh_token"] = token_data["refresh_token"]
    update_fields = encrypt_account_fields(update_fields)

    await database.email_accounts_collection.update_one(
        {"_id": doc["_id"]},
        {"$set": update_fields},
    )

    return {"message": "Token refreshed", "expires_at": token_expiry}


# ---------------------------------------------------------------------------
# POST /api/email-accounts/{email_account_id}/mailbox-draft
# ---------------------------------------------------------------------------

class MailboxDraftRequest(BaseModel):
    to_email: EmailStr
    subject: str
    body: str  # HTML allowed


@router.post("/{email_account_id}/mailbox-draft")
async def create_email_account_draft(
    email_account_id: str,
    payload: MailboxDraftRequest,
    account_ctx: dict = Depends(get_account_context),
):
    """Create a standalone native draft (Gmail/Zoho/IMAP Drafts folder) via this account."""
    account_id = account_ctx["account"]["_id"]
    doc = await _get_email_account_or_404(email_account_id, account_id)

    from services.email_delivery_service import create_mailbox_draft
    result = await create_mailbox_draft(doc, str(payload.to_email), payload.subject, payload.body)

    if result is None:
        raise HTTPException(
            status_code=status.HTTP_502_BAD_GATEWAY,
            detail="Draft creation failed — check provider credentials/logs",
        )
    return {
        "success": True,
        "draft_id": result.get("draft_id"),
        "provider": result.get("provider"),
    }
