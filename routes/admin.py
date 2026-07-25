"""Super-admin routes — protected by get_super_admin dependency.

Gate: SUPER_ADMIN_EMAIL env var must be set AND caller's email must match.
All data-modifying operations are non-destructive (suspend, not delete).
"""
import asyncio
import json
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Optional

from bson import ObjectId
from fastapi import APIRouter, Depends, HTTPException, Query, Request, Response, status
from fastapi.responses import FileResponse, JSONResponse
from pydantic import BaseModel

import database
from config import get_settings
from auth import (
    ADMIN_SESSION_COOKIE_NAME,
    clear_admin_session_cookie,
    create_impersonation_token,
    decode_access_token,
    get_current_user,
    get_super_admin,
    hash_password,
    is_browser_session_request,
    set_session_cookies,
)
from services.admin_audit_service import log_admin_action
from services.user_provisioning import create_user_with_account

router = APIRouter(prefix="/api/admin", tags=["Admin"])
settings = get_settings()


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _str_id(v) -> str | None:
    if v is None:
        return None
    return str(v)


def _serialize_doc(doc: dict) -> dict:
    """Recursively convert ObjectIds to strings in a Mongo document."""
    result = {}
    for k, v in doc.items():
        if isinstance(v, ObjectId):
            result[k] = str(v)
        elif isinstance(v, dict):
            result[k] = _serialize_doc(v)
        elif isinstance(v, list):
            result[k] = [
                _serialize_doc(i) if isinstance(i, dict)
                else str(i) if isinstance(i, ObjectId)
                else i
                for i in v
            ]
        else:
            result[k] = v
    return result


async def _load_user(user_id: str) -> dict:
    try:
        oid = ObjectId(user_id)
    except Exception:
        raise HTTPException(status_code=404, detail="User not found")
    user = await database.users_collection.find_one({"_id": oid})
    if not user:
        raise HTTPException(status_code=404, detail="User not found")
    return user


async def _get_account_id(user: dict) -> ObjectId:
    raw = user.get("current_account_id")
    if not raw:
        raise HTTPException(status_code=400, detail="User has no account")
    return raw if isinstance(raw, ObjectId) else ObjectId(raw)


def _redact_email_account(doc: dict) -> dict:
    """Strip sensitive credential fields from email account docs."""
    sensitive = {"api_key", "access_token", "refresh_token", "smtp_password", "client_secret"}
    return {k: v for k, v in doc.items() if k not in sensitive}


# ---------------------------------------------------------------------------
# Request / Response schemas
# ---------------------------------------------------------------------------

class CreateUserRequest(BaseModel):
    email: str
    password: str
    name: str
    account_name: Optional[str] = None
    plan: str = "free"


class UpdateUserRequest(BaseModel):
    name: Optional[str] = None
    plan: Optional[str] = None
    onboarding_complete: Optional[bool] = None
    password: Optional[str] = None


# ---------------------------------------------------------------------------
# GET /api/admin/users — list all users with stats
# ---------------------------------------------------------------------------

@router.get("/users")
async def list_users(
    page: int = Query(1, ge=1),
    page_size: int = Query(50, ge=1, le=100),
    _admin: dict = Depends(get_super_admin),
):
    skip = (page - 1) * page_size

    pipeline = [
        {"$lookup": {
            "from": "accounts",
            "localField": "current_account_id",
            "foreignField": "_id",
            "as": "_acct",
        }},
        {"$unwind": {"path": "$_acct", "preserveNullAndEmptyArrays": True}},
        {"$lookup": {
            "from": "campaigns",
            "localField": "current_account_id",
            "foreignField": "account_id",
            "as": "_camps",
        }},
        {"$lookup": {
            "from": "linkedin_accounts",
            "localField": "current_account_id",
            "foreignField": "account_id",
            "as": "_li",
        }},
        {"$lookup": {
            "from": "email_accounts",
            "localField": "current_account_id",
            "foreignField": "account_id",
            "as": "_em",
        }},
        {"$addFields": {
            "campaign_count": {"$size": "$_camps"},
            "linkedin_count": {"$size": "$_li"},
            "email_account_count": {"$size": "$_em"},
            "account_id": {"$ifNull": [{"$toString": "$_acct._id"}, None]},
            "account_name": "$_acct.name",
            "account_status": "$_acct.status",
        }},
        {"$project": {
            "password_hash": 0,
            "_camps": 0,
            "_li": 0,
            "_em": 0,
            "_acct": 0,
        }},
        {"$sort": {"created_at": -1}},
    ]

    count_result = await database.users_collection.aggregate(
        pipeline + [{"$count": "n"}]
    ).to_list(1)
    total = count_result[0]["n"] if count_result else 0

    users = await database.users_collection.aggregate(
        pipeline + [{"$skip": skip}, {"$limit": page_size}]
    ).to_list(page_size)

    return {
        "users": [_serialize_doc(u) for u in users],
        "total": total,
        "page": page,
        "page_size": page_size,
    }


# ---------------------------------------------------------------------------
# POST /api/admin/users — create a new user
# ---------------------------------------------------------------------------

@router.post("/users", status_code=status.HTTP_201_CREATED)
async def create_user(
    body: CreateUserRequest,
    _admin: dict = Depends(get_super_admin),
):
    user_id, account_id = await create_user_with_account(
        email=body.email,
        password=body.password,
        name=body.name,
        company_name=body.account_name,
        plan=body.plan,
    )
    user = await database.users_collection.find_one({"_id": user_id})
    return _serialize_doc({k: v for k, v in user.items() if k != "password_hash"})


# ---------------------------------------------------------------------------
# GET /api/admin/users/{user_id} — get user detail
# ---------------------------------------------------------------------------

@router.get("/users/{user_id}")
async def get_user(
    user_id: str,
    _admin: dict = Depends(get_super_admin),
):
    user = await _load_user(user_id)
    account_id = await _get_account_id(user)

    account = await database.accounts_collection.find_one({"_id": account_id})
    members = await database.account_members_collection.find(
        {"account_id": account_id}
    ).to_list(None)

    campaign_count = await database.campaigns_collection.count_documents({"account_id": account_id})
    linkedin_count = await database.linkedin_accounts_collection.count_documents({"account_id": account_id})
    email_count = await database.email_accounts_collection.count_documents({"account_id": account_id})

    return {
        "user": _serialize_doc({k: v for k, v in user.items() if k != "password_hash"}),
        "account": _serialize_doc(account) if account else None,
        "members": [_serialize_doc(m) for m in members],
        "stats": {
            "campaign_count": campaign_count,
            "linkedin_count": linkedin_count,
            "email_count": email_count,
        },
    }


# ---------------------------------------------------------------------------
# PATCH /api/admin/users/{user_id} — update user
# ---------------------------------------------------------------------------

@router.patch("/users/{user_id}")
async def update_user(
    user_id: str,
    body: UpdateUserRequest,
    _admin: dict = Depends(get_super_admin),
):
    user = await _load_user(user_id)
    updates: dict = {"updated_at": datetime.utcnow()}
    if body.name is not None:
        updates["name"] = body.name.strip()
    if body.plan is not None:
        updates["plan"] = body.plan
    if body.onboarding_complete is not None:
        updates["onboarding_complete"] = body.onboarding_complete
    if body.password is not None:
        updates["password_hash"] = hash_password(body.password)

    await database.users_collection.update_one({"_id": user["_id"]}, {"$set": updates})
    updated = await database.users_collection.find_one({"_id": user["_id"]})
    return _serialize_doc({k: v for k, v in updated.items() if k != "password_hash"})


# ---------------------------------------------------------------------------
# DELETE /api/admin/users/{user_id} — soft-disable (suspend account)
# ---------------------------------------------------------------------------

@router.delete("/users/{user_id}", status_code=status.HTTP_204_NO_CONTENT)
async def disable_user(
    user_id: str,
    admin: dict = Depends(get_super_admin),
):
    user = await _load_user(user_id)
    account_id = await _get_account_id(user)
    now = datetime.utcnow()
    await database.accounts_collection.update_one(
        {"_id": account_id},
        {"$set": {"status": "suspended", "updated_at": now}},
    )
    await database.users_collection.update_one(
        {"_id": user["_id"]},
        {"$set": {"is_active": False, "updated_at": now}},
    )
    await log_admin_action(
        admin["email"], "user.disable", "user", user_id,
        {"email": user.get("email"), "account_id": str(account_id)},
    )


# ---------------------------------------------------------------------------
# GET /api/admin/users/{user_id}/campaigns
# ---------------------------------------------------------------------------

@router.get("/users/{user_id}/campaigns")
async def get_user_campaigns(
    user_id: str,
    _admin: dict = Depends(get_super_admin),
):
    user = await _load_user(user_id)
    account_id = await _get_account_id(user)
    campaigns = await database.campaigns_collection.find(
        {"account_id": account_id},
        {"generated_messages": 0, "icp_params": 0},
    ).sort("created_at", -1).to_list(200)
    return {"campaigns": [_serialize_doc(c) for c in campaigns]}


# ---------------------------------------------------------------------------
# GET /api/admin/users/{user_id}/linkedin-accounts
# ---------------------------------------------------------------------------

@router.get("/users/{user_id}/linkedin-accounts")
async def get_user_linkedin_accounts(
    user_id: str,
    _admin: dict = Depends(get_super_admin),
):
    user = await _load_user(user_id)
    account_id = await _get_account_id(user)
    accounts = await database.linkedin_accounts_collection.find(
        {"account_id": account_id}
    ).to_list(50)
    return {"linkedin_accounts": [_serialize_doc(a) for a in accounts]}


# ---------------------------------------------------------------------------
# GET /api/admin/users/{user_id}/email-accounts
# ---------------------------------------------------------------------------

@router.get("/users/{user_id}/email-accounts")
async def get_user_email_accounts(
    user_id: str,
    _admin: dict = Depends(get_super_admin),
):
    user = await _load_user(user_id)
    account_id = await _get_account_id(user)
    accounts = await database.email_accounts_collection.find(
        {"account_id": account_id}
    ).to_list(50)
    redacted = [_redact_email_account(_serialize_doc(a)) for a in accounts]
    return {"email_accounts": redacted}


# ---------------------------------------------------------------------------
# GET /api/admin/users/{user_id}/activity
# ---------------------------------------------------------------------------

@router.get("/users/{user_id}/activity")
async def get_user_activity(
    user_id: str,
    page: int = Query(1, ge=1),
    page_size: int = Query(30, ge=1, le=100),
    _admin: dict = Depends(get_super_admin),
):
    user = await _load_user(user_id)
    account_id = await _get_account_id(user)
    from services.activity_feed_service import get_activity_feed
    result = await get_activity_feed(
        page=page,
        page_size=page_size,
        account_id=str(account_id),
    )
    return result


# ---------------------------------------------------------------------------
# POST /api/admin/impersonate/{user_id}
# ---------------------------------------------------------------------------

@router.post("/impersonate/{user_id}")
async def impersonate_user(
    user_id: str,
    request: Request,
    response: Response,
    admin: dict = Depends(get_super_admin),
):
    user = await _load_user(user_id)
    account_id_raw = user.get("current_account_id")
    account_id_str = str(account_id_raw) if account_id_raw else ""

    expires_at = datetime.now(timezone.utc) + timedelta(minutes=30)
    token = create_impersonation_token(
        target_user_id=str(user["_id"]),
        target_account_id=account_id_str,
        target_email=user["email"],
        admin_user_id=admin["_id"],
    )
    await log_admin_action(
        admin["email"], "user.impersonate", "user", user_id,
        {"target_email": user["email"], "account_id": account_id_str, "ttl_minutes": 30},
    )
    if is_browser_session_request(request):
        admin_token = request.cookies.get("auth_token")
        if not admin_token:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="Browser impersonation requires an active cookie session",
            )
        set_session_cookies(
            response,
            token,
            ttl_minutes=30,
            admin_token=admin_token,
        )
        response.headers["Cache-Control"] = "no-store"
        return {
            "impersonating": True,
            "user": {
                "_id": str(user["_id"]),
                "email": user["email"],
                "name": user["name"],
            },
            "expires_at": expires_at.isoformat(),
        }
    return {
        "token": token,
        "user": {
            "_id": str(user["_id"]),
            "email": user["email"],
            "name": user["name"],
        },
        "expires_at": expires_at.isoformat(),
    }


@router.post("/impersonation/stop")
async def stop_impersonating(
    request: Request,
    response: Response,
    current_user: dict = Depends(get_current_user),
):
    """Restore the original superadmin session without exposing either JWT."""
    if not current_user.get("_is_impersonating"):
        raise HTTPException(status_code=409, detail="No active impersonation session")

    admin_token = request.cookies.get(ADMIN_SESSION_COOKIE_NAME)
    if not admin_token:
        raise HTTPException(status_code=401, detail="Original admin session is unavailable")

    try:
        payload = decode_access_token(admin_token)
        admin_id = payload.get("sub")
        if not admin_id or admin_id != current_user.get("_impersonated_by"):
            raise ValueError("impersonation session mismatch")
        admin = await database.users_collection.find_one({"_id": ObjectId(admin_id)})
    except Exception:
        raise HTTPException(status_code=401, detail="Original admin session is invalid")

    configured_admin = (settings.super_admin_email or "").lower()
    if not admin or not configured_admin or admin.get("email", "").lower() != configured_admin:
        raise HTTPException(status_code=403, detail="Original admin access is no longer valid")

    set_session_cookies(response, admin_token)
    clear_admin_session_cookie(response)
    response.headers["Cache-Control"] = "no-store"
    await log_admin_action(
        admin["email"],
        "user.impersonation.stop",
        "user",
        current_user["_id"],
    )
    return {"impersonating": False}


# ---------------------------------------------------------------------------
# GET /api/admin/campaigns/{campaign_id}/discovery-log
# ---------------------------------------------------------------------------

@router.get("/campaigns/{campaign_id}/discovery-log")
async def get_campaign_discovery_log(
    campaign_id: str,
    offset: int = Query(0, ge=0),
    limit: int = Query(200, ge=1, le=1000),
    _admin: dict = Depends(get_super_admin),
):
    """Return paginated JSONL discovery log for a campaign."""
    from config import get_settings
    _settings = get_settings()
    log_base = Path(_settings.discovery_log_dir)
    log_files = list(log_base.glob(f"*/{campaign_id}/discovery.jsonl"))
    if not log_files:
        return {"lines": [], "total_lines": 0, "has_more": False, "status": "not_found"}

    log_path = log_files[0]
    lines = log_path.read_text(encoding="utf-8").splitlines()
    total = len(lines)
    page_lines = lines[offset:offset + limit]
    parsed = []
    for line in page_lines:
        try:
            parsed.append(json.loads(line))
        except Exception:
            parsed.append({"raw": line, "parse_error": True})

    return {
        "lines": parsed,
        "total_lines": total,
        "has_more": (offset + limit) < total,
    }


# ---------------------------------------------------------------------------
# GET /api/admin/campaigns/{campaign_id}/discovery-log/raw
# ---------------------------------------------------------------------------

@router.get("/campaigns/{campaign_id}/discovery-log/raw")
async def get_campaign_discovery_log_raw(
    campaign_id: str,
    _admin: dict = Depends(get_super_admin),
):
    """Stream raw JSONL discovery log."""
    from config import get_settings
    _settings = get_settings()
    log_base = Path(_settings.discovery_log_dir)
    log_files = list(log_base.glob(f"*/{campaign_id}/discovery.jsonl"))
    if not log_files:
        return JSONResponse({"detail": "Log not found"}, status_code=404)
    return FileResponse(str(log_files[0]), media_type="application/x-ndjson")


# ---------------------------------------------------------------------------
# GET /api/admin/campaigns/{campaign_id}/discovery-summary
# ---------------------------------------------------------------------------

@router.get("/campaigns/{campaign_id}/discovery-summary")
async def get_campaign_discovery_summary(
    campaign_id: str,
    _admin: dict = Depends(get_super_admin),
):
    """Return parsed discovery summary JSON for a campaign."""
    from config import get_settings
    _settings = get_settings()
    log_base = Path(_settings.discovery_log_dir)
    summary_files = list(log_base.glob(f"*/{campaign_id}/summary.json"))
    if not summary_files:
        return {"status": "not_found", "campaign_id": campaign_id}

    try:
        data = json.loads(summary_files[0].read_text(encoding="utf-8"))
        data.setdefault("status", "completed")
        return data
    except Exception as e:
        return {"status": "error", "campaign_id": campaign_id, "error": str(e)}


# ---------------------------------------------------------------------------
# GET /api/admin/usage/apify
# ---------------------------------------------------------------------------

@router.get("/usage/apify")
async def get_apify_usage(
    account_id: Optional[str] = Query(None),
    campaign_id: Optional[str] = Query(None),
    actor_id: Optional[str] = Query(None),
    from_date: Optional[str] = Query(None, alias="from"),
    to_date: Optional[str] = Query(None, alias="to"),
    page: int = Query(1, ge=1),
    page_size: int = Query(50, ge=1, le=200),
    _admin: dict = Depends(get_super_admin),
):
    query: dict = {}
    if account_id:
        query["account_id"] = account_id
    if campaign_id:
        query["campaign_id"] = campaign_id
    if actor_id:
        query["actor_id"] = actor_id
    if from_date or to_date:
        date_filter: dict = {}
        if from_date:
            date_filter["$gte"] = datetime.fromisoformat(from_date)
        if to_date:
            date_filter["$lte"] = datetime.fromisoformat(to_date)
        query["started_at"] = date_filter

    total = await database.apify_usage_collection.count_documents(query)
    skip = (page - 1) * page_size
    cursor = database.apify_usage_collection.find(query).sort("started_at", -1).skip(skip).limit(page_size)
    rows = []
    async for doc in cursor:
        rows.append(_serialize_doc(doc))

    pipeline = [{"$match": query}, {"$group": {"_id": None, "total": {"$sum": "$cost_usd"}}}]
    agg = await database.apify_usage_collection.aggregate(pipeline).to_list(1)
    total_cost = agg[0]["total"] if agg else 0.0

    return {"rows": rows, "total": total, "total_cost_usd": round(total_cost, 6)}


# ---------------------------------------------------------------------------
# GET /api/admin/usage/openrouter
# ---------------------------------------------------------------------------

@router.get("/usage/openrouter")
async def get_openrouter_usage(
    account_id: Optional[str] = Query(None),
    campaign_id: Optional[str] = Query(None),
    feature: Optional[str] = Query(None),
    model: Optional[str] = Query(None),
    from_date: Optional[str] = Query(None, alias="from"),
    to_date: Optional[str] = Query(None, alias="to"),
    page: int = Query(1, ge=1),
    page_size: int = Query(50, ge=1, le=200),
    _admin: dict = Depends(get_super_admin),
):
    query: dict = {}
    if account_id:
        query["account_id"] = account_id
    if campaign_id:
        query["campaign_id"] = campaign_id
    if feature:
        query["feature"] = feature
    if model:
        query["model"] = model
    if from_date or to_date:
        date_filter: dict = {}
        if from_date:
            date_filter["$gte"] = datetime.fromisoformat(from_date)
        if to_date:
            date_filter["$lte"] = datetime.fromisoformat(to_date)
        query["requested_at"] = date_filter

    total = await database.openrouter_usage_collection.count_documents(query)
    skip = (page - 1) * page_size
    cursor = database.openrouter_usage_collection.find(query).sort("requested_at", -1).skip(skip).limit(page_size)
    rows = []
    async for doc in cursor:
        rows.append(_serialize_doc(doc))

    pipeline = [{"$match": query}, {"$group": {"_id": None, "total": {"$sum": "$cost_usd"}}}]
    agg = await database.openrouter_usage_collection.aggregate(pipeline).to_list(1)
    total_cost = agg[0]["total"] if agg else 0.0

    return {"rows": rows, "total": total, "total_cost_usd": round(total_cost, 6)}


# ---------------------------------------------------------------------------
# GET /api/admin/usage/summary
# ---------------------------------------------------------------------------

@router.get("/usage/summary")
async def get_usage_summary(
    _admin: dict = Depends(get_super_admin),
):
    now = datetime.now(timezone.utc)
    today_start = now.replace(hour=0, minute=0, second=0, microsecond=0)
    week_start = today_start - timedelta(days=now.weekday())
    month_start = today_start.replace(day=1)

    async def _sum_cost(collection, date_field: str, start: datetime) -> float:
        pipeline = [
            {"$match": {date_field: {"$gte": start}}},
            {"$group": {"_id": None, "total": {"$sum": "$cost_usd"}}},
        ]
        agg = await collection.aggregate(pipeline).to_list(1)
        return round(agg[0]["total"] if agg else 0.0, 6)

    async def _sum_gt_credits(start: datetime) -> int:
        """GrowthToolkit usage is tracked in credits (no USD constant exists)."""
        pipeline = [
            {"$match": {"created_at": {"$gte": start}}},
            {"$group": {"_id": None, "total": {"$sum": {"$ifNull": ["$credits_used", 0]}}}},
        ]
        try:
            agg = await database.growthtoolkit_usage_collection.aggregate(pipeline).to_list(1)
        except Exception:
            return 0
        return agg[0]["total"] if agg else 0

    (
        apify_today, or_today, gt_today,
        apify_week, or_week, gt_week,
        apify_month, or_month, gt_month,
    ) = await asyncio.gather(
        _sum_cost(database.apify_usage_collection, "started_at", today_start),
        _sum_cost(database.openrouter_usage_collection, "requested_at", today_start),
        _sum_gt_credits(today_start),
        _sum_cost(database.apify_usage_collection, "started_at", week_start),
        _sum_cost(database.openrouter_usage_collection, "requested_at", week_start),
        _sum_gt_credits(week_start),
        _sum_cost(database.apify_usage_collection, "started_at", month_start),
        _sum_cost(database.openrouter_usage_collection, "requested_at", month_start),
        _sum_gt_credits(month_start),
    )

    top_accounts_pipeline = [
        {"$match": {"requested_at": {"$gte": month_start}}},
        {"$group": {"_id": "$account_id", "total_cost": {"$sum": "$cost_usd"}}},
        {"$sort": {"total_cost": -1}},
        {"$limit": 10},
    ]
    top_accounts = await database.openrouter_usage_collection.aggregate(top_accounts_pipeline).to_list(10)

    top_campaigns_pipeline = [
        {"$match": {"requested_at": {"$gte": month_start}, "campaign_id": {"$ne": None}}},
        {"$group": {"_id": "$campaign_id", "total_cost": {"$sum": "$cost_usd"}}},
        {"$sort": {"total_cost": -1}},
        {"$limit": 10},
    ]
    top_campaigns = await database.openrouter_usage_collection.aggregate(top_campaigns_pipeline).to_list(10)

    return {
        "today": {"apify_cost": apify_today, "openrouter_cost": or_today, "growthtoolkit_credits": gt_today},
        "this_week": {"apify_cost": apify_week, "openrouter_cost": or_week, "growthtoolkit_credits": gt_week},
        "this_month": {"apify_cost": apify_month, "openrouter_cost": or_month, "growthtoolkit_credits": gt_month},
        "top_accounts_by_cost": [{"account_id": r["_id"], "total_cost": r["total_cost"]} for r in top_accounts],
        "top_campaigns_by_cost": [{"campaign_id": r["_id"], "total_cost": r["total_cost"]} for r in top_campaigns],
    }
