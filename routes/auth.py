import re
import secrets
from datetime import datetime
from typing import Optional

from bson import ObjectId
from fastapi import APIRouter, BackgroundTasks, Depends, HTTPException, Request, status
from pydantic import BaseModel
from pymongo.errors import PyMongoError
from rate_limit import limiter

import database
from config import get_settings
from auth import (
    create_access_token,
    get_current_user,
    verify_password,
)
from models.user import TokenResponse
from services.user_provisioning import create_user_with_account

settings = get_settings()
router = APIRouter(prefix="/api/auth", tags=["Auth"])
users_router = APIRouter(prefix="/api/users", tags=["Users"])
onboarding_router = APIRouter(prefix="/api/onboarding", tags=["Onboarding"])


# ---------------------------------------------------------------------------
# Request / Response schemas
# ---------------------------------------------------------------------------

class RegisterRequest(BaseModel):
    email: str
    password: str
    name: str
    company_name: Optional[str] = None


class LoginRequest(BaseModel):
    email: str
    password: str


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _slugify(name: str) -> str:
    """Convert a display name to a URL-safe slug with a 4-hex-char suffix."""
    base = re.sub(r"[^a-z0-9-]", "", name.lower().replace(" ", "-"))
    suffix = secrets.token_hex(2)  # 4 hex chars
    return f"{base}-{suffix}"


def _user_to_dict(user: dict) -> dict:
    return {
        "_id": str(user["_id"]),
        "email": user["email"],
        "name": user["name"],
        "plan": user.get("plan", "free"),
        "current_account_id": (
            str(user["current_account_id"]) if user.get("current_account_id") else None
        ),
        "onboarding_complete": user.get("onboarding_complete", False),
        "booking_link": user.get("booking_link"),
    }


def _account_to_dict(account: dict) -> dict:
    return {
        "_id": str(account["_id"]),
        "name": account["name"],
        "slug": account["slug"],
        "plan": account.get("plan", "free"),
        "status": account.get("status", "active"),
        "trial_ends_at": account.get("trial_ends_at"),
        "sender_name": account.get("sender_name"),
        "sender_email": account.get("sender_email"),
        "reply_to_email": account.get("reply_to_email"),
        "daily_email_quota": account.get("daily_email_quota", 50),
        "daily_linkedin_connection_quota": account.get("daily_linkedin_connection_quota", 20),
        "daily_linkedin_inmail_quota": account.get("daily_linkedin_inmail_quota", 5),
        "enrichment_batch_limit": account.get("enrichment_batch_limit", 50),
        "created_at": account.get("created_at"),
        "updated_at": account.get("updated_at"),
    }


# ---------------------------------------------------------------------------
# POST /api/auth/register
# ---------------------------------------------------------------------------

@router.post("/register", response_model=TokenResponse, status_code=status.HTTP_201_CREATED)
async def register(body: RegisterRequest):
    """Register a new user and create their first account (owner role)."""
    user_id, account_id = await create_user_with_account(
        email=body.email,
        password=body.password,
        name=body.name,
        company_name=body.company_name,
    )

    access_token = create_access_token(
        data={
            "sub": str(user_id),
            "account_id": str(account_id),
            "email": body.email.strip().lower(),
        }
    )
    return {"access_token": access_token, "token_type": "bearer"}


# ---------------------------------------------------------------------------
# POST /api/auth/login
# ---------------------------------------------------------------------------

@router.post("/login", response_model=TokenResponse)
@limiter.limit("10/minute")
async def login(request: Request, body: LoginRequest):
    """
    Authenticate a user and return a JWT access token.
    Body: {email, password}  — application/json, NOT form data.
    """
    email = body.email.strip().lower()

    try:
        user = await database.users_collection.find_one({"email": email})
    except PyMongoError:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="Database temporarily unavailable. Please try again.",
        )
    if user is None or not verify_password(body.password, user["password_hash"]):
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid email or password",
        )

    account_id_str = str(user["current_account_id"]) if user.get("current_account_id") else ""

    access_token = create_access_token(
        data={
            "sub": str(user["_id"]),
            "account_id": account_id_str,
            "email": email,
        }
    )
    return {"access_token": access_token, "token_type": "bearer"}


# ---------------------------------------------------------------------------
# GET /api/auth/me
# ---------------------------------------------------------------------------

@router.get("/me")
async def me(user: dict = Depends(get_current_user)):
    """Return the current user and their active account."""
    account = None
    if user.get("current_account_id"):
        try:
            account_oid = ObjectId(user["current_account_id"])
            account_doc = await database.accounts_collection.find_one({"_id": account_oid})
            if account_doc:
                account = _account_to_dict(account_doc)
        except Exception:
            pass

    return {
        "user": _user_to_dict(user),
        "account": account,
    }


# ---------------------------------------------------------------------------
# POST /api/auth/refresh
# ---------------------------------------------------------------------------

@router.post("/refresh", response_model=TokenResponse)
async def refresh(user: dict = Depends(get_current_user)):
    """Issue a fresh JWT with the same claims."""
    account_id_str = str(user["current_account_id"]) if user.get("current_account_id") else ""

    access_token = create_access_token(
        data={
            "sub": user["_id"],  # already a string from get_current_user
            "account_id": account_id_str,
            "email": user["email"],
        }
    )
    return {"access_token": access_token, "token_type": "bearer"}


# ---------------------------------------------------------------------------
# PATCH /api/auth/me  (update user profile fields)
# ---------------------------------------------------------------------------

class UpdateMeRequest(BaseModel):
    name: Optional[str] = None
    booking_link: Optional[str] = None
    onboarding_complete: Optional[bool] = None


@router.patch("/me")
async def update_me(body: UpdateMeRequest, user: dict = Depends(get_current_user)):
    """Update the current user's profile (name, booking_link, onboarding_complete)."""
    update_data = body.model_dump(exclude_none=True)
    if not update_data:
        raise HTTPException(status_code=400, detail="No fields to update")
    update_data["updated_at"] = datetime.utcnow()
    await database.users_collection.update_one(
        {"_id": user["_id"] if isinstance(user["_id"], ObjectId) else ObjectId(user["_id"])},
        {"$set": update_data},
    )
    updated = await database.users_collection.find_one(
        {"_id": user["_id"] if isinstance(user["_id"], ObjectId) else ObjectId(user["_id"])}
    )
    return {"user": _user_to_dict(updated)}


# ---------------------------------------------------------------------------
# POST /api/auth/logout
# ---------------------------------------------------------------------------

@router.post("/logout")
async def logout():
    """Logout — client is responsible for clearing the stored token."""
    return {"message": "Logged out"}


# ---------------------------------------------------------------------------
# POST /api/auth/password-reset/request
# POST /api/auth/password-reset/confirm
# ---------------------------------------------------------------------------

class PasswordResetRequestBody(BaseModel):
    email: str


class PasswordResetConfirmBody(BaseModel):
    token: str
    new_password: str


@router.post("/password-reset/request")
@limiter.limit("5/minute")
async def password_reset_request(request: Request, body: PasswordResetRequestBody):
    """
    Send a password-reset link to the given email (if an account exists).
    Always returns 200 to avoid leaking which emails are registered.
    """
    from datetime import timedelta, timezone
    import logging as _log

    email = body.email.strip().lower()
    user = await database.users_collection.find_one({"email": email})

    if user:
        token = secrets.token_urlsafe(32)
        expires = datetime.now(timezone.utc) + timedelta(hours=1)
        # Delete any previous tokens for this user (one active reset at a time)
        await database.password_reset_tokens_collection.delete_many({"user_id": user["_id"]})
        await database.password_reset_tokens_collection.insert_one({
            "token": token,
            "user_id": user["_id"],
            "email": email,
            "expires_at": expires,
            "used": False,
        })

        reset_url = f"{settings.frontend_url}/reset-password/{token}"

        # Try to send via SendGrid; fall back to log in dev
        if settings.sendgrid_api_key:
            try:
                from sendgrid import SendGridAPIClient
                from sendgrid.helpers.mail import Mail
                msg = Mail(
                    from_email=(settings.sender_email or "noreply@outflo.io"),
                    to_emails=email,
                    subject="Reset your Outflo password",
                    html_content=(
                        f"<p>Click the link below to reset your password (valid for 1 hour):</p>"
                        f"<p><a href='{reset_url}'>{reset_url}</a></p>"
                        f"<p>If you didn't request this, you can safely ignore this email.</p>"
                    ),
                )
                sg = SendGridAPIClient(settings.sendgrid_api_key)
                sg.send(msg)
            except Exception as exc:
                _log.getLogger(__name__).warning(f"Password reset email failed: {exc}. URL: {reset_url}")
        else:
            _log.getLogger(__name__).info(f"[DEV] Password reset URL for {email}: {reset_url}")

    return {"message": "If that email is registered, you'll receive a reset link shortly."}


@router.post("/password-reset/confirm")
async def password_reset_confirm(body: PasswordResetConfirmBody):
    """Validate the token and update the user's password."""
    from datetime import timezone

    if len(body.new_password) < 8:
        raise HTTPException(status_code=400, detail="Password must be at least 8 characters")

    token_doc = await database.password_reset_tokens_collection.find_one({"token": body.token})
    if not token_doc:
        raise HTTPException(status_code=400, detail="Invalid or expired reset token")
    if token_doc.get("used"):
        raise HTTPException(status_code=400, detail="Reset token has already been used")

    from datetime import timezone as _tz
    expires = token_doc["expires_at"]
    if expires.tzinfo is None:
        expires = expires.replace(tzinfo=_tz.utc)
    if expires < datetime.now(_tz.utc):
        raise HTTPException(status_code=400, detail="Reset token has expired")

    # Mark used and update password atomically
    await database.password_reset_tokens_collection.update_one(
        {"token": body.token},
        {"$set": {"used": True}},
    )
    await database.users_collection.update_one(
        {"_id": token_doc["user_id"]},
        {"$set": {"password_hash": hash_password(body.new_password), "updated_at": datetime.utcnow()}},
    )
    # Delete the consumed token
    await database.password_reset_tokens_collection.delete_one({"token": body.token})

    return {"message": "Password updated successfully"}


# ---------------------------------------------------------------------------
# /api/users/me  — alias for settings page compatibility
# ---------------------------------------------------------------------------

@users_router.patch("/me")
async def update_user_me(body: UpdateMeRequest, user: dict = Depends(get_current_user)):
    """Alias for PATCH /api/auth/me — update user profile."""
    update_data = body.model_dump(exclude_none=True)
    if not update_data:
        raise HTTPException(status_code=400, detail="No fields to update")
    update_data["updated_at"] = datetime.utcnow()
    user_oid = user["_id"] if isinstance(user["_id"], ObjectId) else ObjectId(user["_id"])
    await database.users_collection.update_one({"_id": user_oid}, {"$set": update_data})
    updated = await database.users_collection.find_one({"_id": user_oid})
    return {"user": _user_to_dict(updated)}


# ---------------------------------------------------------------------------
# POST /api/onboarding/analyze  — start AI analysis job
# GET  /api/onboarding/analyze/{job_id}  — poll job status
# ---------------------------------------------------------------------------

class AnalyzeRequest(BaseModel):
    company_name: str
    url: str
    description: Optional[str] = None


@onboarding_router.post("/analyze")
async def start_analyze(
    body: AnalyzeRequest,
    background_tasks: BackgroundTasks,
    user: dict = Depends(get_current_user),
):
    """Start background company analysis job. Returns job_id for polling."""
    from services.onboarding_analyzer_service import create_job, run_analysis

    job_id = create_job()
    background_tasks.add_task(
        run_analysis,
        job_id,
        body.company_name,
        body.url,
        body.description or "",
    )
    return {"job_id": job_id}


@onboarding_router.get("/analyze/{job_id}")
async def get_analyze_status(job_id: str, user: dict = Depends(get_current_user)):
    """Poll the status of a company analysis job."""
    from services.onboarding_analyzer_service import get_job

    job = get_job(job_id)
    if job is None:
        raise HTTPException(status_code=404, detail="Job not found")
    return job


# ---------------------------------------------------------------------------
# POST /api/onboarding  — save company profile + mark onboarding complete
# ---------------------------------------------------------------------------

class OnboardingRequest(BaseModel):
    company_name: str
    website_url: Optional[str] = None
    description: str = ""
    pain_points: list[str] = []
    # AI-enriched fields (all optional — backward compatible)
    target_market: Optional[str] = None
    services: Optional[list[str]] = None
    industries: Optional[list[str]] = None
    differentiators: Optional[list[str]] = None
    value_propositions: Optional[list[str]] = None
    icp_description: Optional[str] = None
    sender_role: Optional[str] = None
    case_studies: Optional[list[dict]] = None


@onboarding_router.post("")
async def complete_onboarding(body: OnboardingRequest, user: dict = Depends(get_current_user)):
    """Save company profile and mark onboarding complete in one call."""
    from datetime import timezone
    from database import company_profiles_collection, accounts_collection

    account_oid = ObjectId(user["current_account_id"])
    user_oid = user["_id"] if isinstance(user["_id"], ObjectId) else ObjectId(user["_id"])
    now = datetime.now(timezone.utc)

    # Upsert company profile
    existing = await company_profiles_collection.find_one({"account_id": account_oid})
    profile_fields: dict = {
        "company_name": body.company_name,
        "description": body.description,
        "pain_points": body.pain_points,
        "updated_at": now,
    }
    if body.website_url:
        profile_fields["website_url"] = body.website_url
    # Write AI-enriched fields when provided
    if body.target_market is not None:
        profile_fields["target_market"] = body.target_market
    if body.services is not None:
        profile_fields["services"] = body.services
    if body.industries is not None:
        profile_fields["industries"] = body.industries
    if body.differentiators is not None:
        profile_fields["differentiators"] = body.differentiators
    if body.value_propositions is not None:
        profile_fields["value_propositions"] = body.value_propositions
    if body.icp_description is not None:
        profile_fields["icp_description"] = body.icp_description
    if body.sender_role is not None:
        profile_fields["sender_role"] = body.sender_role
    if body.case_studies is not None:
        profile_fields["case_studies"] = body.case_studies

    if existing is None:
        await company_profiles_collection.insert_one({
            "account_id": account_oid,
            "user_id": user_oid,
            "website_url": body.website_url or "",
            "services": [],
            "target_market": "",
            "differentiators": [],
            "value_propositions": [],
            "tone_of_voice": "professional",
            "case_studies": [],
            "sender_name": "",
            "sender_role": "",
            "outreach_strategy": "email_first",
            "connection_request_guidance": None,
            "email_guidance": None,
            "inmail_guidance": None,
            "icp_description": "",
            "scoring_weights": None,
            "created_at": now,
            **profile_fields,
        })
    else:
        await company_profiles_collection.update_one(
            {"account_id": account_oid},
            {"$set": profile_fields},
        )

    # Update account name if provided
    await accounts_collection.update_one(
        {"_id": account_oid},
        {"$set": {"name": body.company_name, "updated_at": now}},
    )

    # Mark user onboarding complete
    await database.users_collection.update_one(
        {"_id": user_oid},
        {"$set": {"onboarding_complete": True, "updated_at": now}},
    )

    return {"success": True}
