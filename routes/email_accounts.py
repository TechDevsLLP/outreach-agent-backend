"""
Email account management routes.

Supports SendGrid, Google OAuth, Microsoft OAuth, and SMTP providers.
Router prefix: /api/email-accounts
"""

import logging
from datetime import datetime, timezone
from typing import Optional

import httpx

logger = logging.getLogger(__name__)
from bson import ObjectId
from fastapi import APIRouter, BackgroundTasks, Body, Depends, HTTPException, Query, status
from fastapi.responses import JSONResponse
from pydantic import BaseModel, EmailStr

from auth import get_account_context
from config import get_settings
import database
from models.email_account import (
    EmailAccountDocument,
    EmailAccountResponse,
    SendGridAccountRequest,
    SmtpAccountRequest,
)

router = APIRouter(prefix="/api/email-accounts", tags=["Email Accounts"])


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

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
# POST /api/email-accounts/sendgrid
# ---------------------------------------------------------------------------

@router.post("/sendgrid", response_model=EmailAccountResponse, status_code=status.HTTP_201_CREATED)
async def connect_sendgrid(
    body: SendGridAccountRequest,
    background_tasks: BackgroundTasks,
    account_ctx: dict = Depends(get_account_context),
):
    """Connect a SendGrid email account."""
    account_id = account_ctx["account"]["_id"]
    user_id = account_ctx["user"]["_id"]
    now = datetime.now(timezone.utc)

    doc = EmailAccountDocument(
        account_id=account_id,
        user_id=user_id,
        email=body.sendgrid_sender_email,
        display_name=body.display_name,
        provider="sendgrid",
        sendgrid_api_key=body.sendgrid_api_key,
        sendgrid_sender_name=body.sendgrid_sender_name,
        sendgrid_sender_email=body.sendgrid_sender_email,
        sendgrid_reply_to=body.sendgrid_reply_to,
        status="connected",
        created_at=now,
        updated_at=now,
    )

    result = await database.email_accounts_collection.insert_one(doc.model_dump())
    created = await database.email_accounts_collection.find_one({"_id": result.inserted_id})
    from services.campaign_launch_service import replan_channels_on_sender_add
    background_tasks.add_task(replan_channels_on_sender_add, account_id, "email")
    return _serialize_email_account(created)


# ---------------------------------------------------------------------------
# POST /api/email-accounts/oauth/google/url
# ---------------------------------------------------------------------------

@router.get("/oauth/google/url")
async def google_oauth_url(
    redirect_uri: Optional[str] = Query(default=None),
    account_ctx: dict = Depends(get_account_context),
):
    """Generate a Google OAuth authorization URL."""
    settings = get_settings()
    account_id = account_ctx["account"]["_id"]
    effective_redirect_uri = redirect_uri or settings.google_redirect_uri

    import urllib.parse

    params = {
        "client_id": settings.google_client_id,
        "redirect_uri": effective_redirect_uri,
        "response_type": "code",
        "scope": "https://www.googleapis.com/auth/gmail.send https://www.googleapis.com/auth/gmail.readonly https://www.googleapis.com/auth/userinfo.email",
        "access_type": "offline",
        "prompt": "consent",
        "state": account_id,
    }
    auth_url = "https://accounts.google.com/o/oauth2/v2/auth?" + urllib.parse.urlencode(params)
    return {"auth_url": auth_url}


# ---------------------------------------------------------------------------
# POST /api/email-accounts/oauth/google/exchange
# ---------------------------------------------------------------------------

class _GoogleExchangeBody(SendGridAccountRequest):
    """Reuse pydantic for the exchange body."""
    pass


from pydantic import BaseModel


class GoogleExchangeRequest(BaseModel):
    code: str
    redirect_uri: str


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
    """Generate a Microsoft OAuth authorization URL."""
    settings = get_settings()
    account_id = account_ctx["account"]["_id"]
    effective_redirect_uri = redirect_uri or settings.microsoft_redirect_uri

    import urllib.parse

    params = {
        "client_id": settings.microsoft_client_id,
        "redirect_uri": effective_redirect_uri,
        "response_type": "code",
        "scope": "openid email Mail.Send offline_access",
        "response_mode": "query",
        "state": account_id,
    }
    auth_url = (
        "https://login.microsoftonline.com/common/oauth2/v2.0/authorize?"
        + urllib.parse.urlencode(params)
    )
    return {"auth_url": auth_url}


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
    """Exchange a Microsoft OAuth code for tokens and create/update an email account."""
    settings = get_settings()
    account_id = account_ctx["account"]["_id"]
    user_id = account_ctx["user"]["_id"]

    async with httpx.AsyncClient() as client:
        # Exchange code for tokens
        token_resp = await client.post(
            "https://login.microsoftonline.com/common/oauth2/v2.0/token",
            data={
                "code": body.code,
                "client_id": settings.microsoft_client_id,
                "client_secret": settings.microsoft_client_secret,
                "redirect_uri": body.redirect_uri,
                "grant_type": "authorization_code",
            },
        )
        if token_resp.status_code != 200:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail=f"Microsoft token exchange failed: {token_resp.text}",
            )
        token_data = token_resp.json()

        # Fetch user email from Microsoft Graph
        graph_resp = await client.get(
            "https://graph.microsoft.com/v1.0/me",
            headers={"Authorization": f"Bearer {token_data['access_token']}"},
        )
        if graph_resp.status_code != 200:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="Failed to fetch Microsoft user info",
            )
        graph_data = graph_resp.json()

    email = graph_data.get("mail") or graph_data.get("userPrincipalName", "")
    tenant_id = token_data.get("tenant") or token_data.get("tid", "")
    now = datetime.now(timezone.utc)

    expires_in = token_data.get("expires_in", 3600)
    token_expiry = datetime.fromtimestamp(
        datetime.now(timezone.utc).timestamp() + expires_in, tz=timezone.utc
    )

    update_fields = {
        "account_id": account_id,
        "user_id": user_id,
        "email": email,
        "display_name": graph_data.get("displayName", email),
        "provider": "microsoft",
        "oauth_access_token": token_data.get("access_token"),
        "oauth_refresh_token": token_data.get("refresh_token"),
        "oauth_token_expiry": token_expiry,
        "oauth_scopes": token_data.get("scope", "").split(),
        "microsoft_tenant_id": tenant_id,
        "status": "connected",
        "updated_at": now,
    }

    existing = await database.email_accounts_collection.find_one(
        {"account_id": account_id, "email": email, "provider": "microsoft"}
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
        update_fields["created_at"] = now
        result = await database.email_accounts_collection.insert_one(update_fields)
        created = await database.email_accounts_collection.find_one({"_id": result.inserted_id})
        background_tasks.add_task(replan_channels_on_sender_add, account_id, "email")
        return _serialize_email_account(created)


# ---------------------------------------------------------------------------
# POST /api/email-accounts/smtp
# ---------------------------------------------------------------------------

@router.post("/smtp", response_model=EmailAccountResponse, status_code=status.HTTP_201_CREATED)
async def connect_smtp(
    body: SmtpAccountRequest,
    background_tasks: BackgroundTasks,
    account_ctx: dict = Depends(get_account_context),
):
    """Connect an SMTP/IMAP email account."""
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
        status="connected",
        created_at=now,
        updated_at=now,
    )

    result = await database.email_accounts_collection.insert_one(doc.model_dump())
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
    daily_send_limit: Optional[int] = None
    warmup_enabled: Optional[bool] = None


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

    from services.campaign_engine import send_email_via_account
    result = await send_email_via_account(
        doc, to_email, subject, html_body,
        tracking_token=None, prospect_id=None, campaign_id=None,
    )

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
    """Refresh the OAuth access token for a Google or Microsoft email account."""
    settings = get_settings()
    account_id = account_ctx["account"]["_id"]
    doc = await _get_email_account_or_404(email_account_id, account_id)

    provider = doc.get("provider")
    if provider not in ("google", "microsoft"):
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"Token refresh is only supported for Google and Microsoft accounts, not '{provider}'",
        )

    refresh_token = doc.get("oauth_refresh_token")
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
        else:  # microsoft
            resp = await client.post(
                "https://login.microsoftonline.com/common/oauth2/v2.0/token",
                data={
                    "client_id": settings.microsoft_client_id,
                    "client_secret": settings.microsoft_client_secret,
                    "refresh_token": refresh_token,
                    "grant_type": "refresh_token",
                    "scope": "openid email Mail.Send offline_access",
                },
            )

    if resp.status_code != 200:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"Token refresh failed: {resp.text}",
        )

    token_data = resp.json()
    expires_in = token_data.get("expires_in", 3600)
    now = datetime.now(timezone.utc)
    token_expiry = datetime.fromtimestamp(now.timestamp() + expires_in, tz=timezone.utc)

    update_fields: dict = {
        "oauth_access_token": token_data["access_token"],
        "oauth_token_expiry": token_expiry,
        "updated_at": now,
    }
    # Google may issue a new refresh token; Microsoft usually does not
    if token_data.get("refresh_token"):
        update_fields["oauth_refresh_token"] = token_data["refresh_token"]

    await database.email_accounts_collection.update_one(
        {"_id": doc["_id"]},
        {"$set": update_fields},
    )

    return {"message": "Token refreshed", "expires_at": token_expiry}
