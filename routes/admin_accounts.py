"""
Superadmin — tenant & account management.

All endpoints require get_super_admin. Prefix /api/admin (registered in main.py).
Mutations write to admin_audit_log via services/admin_audit_service.
"""
import asyncio
import re
from datetime import datetime, timedelta, timezone
from typing import Optional

from bson import ObjectId
from fastapi import APIRouter, Depends, HTTPException, Query
from pydantic import BaseModel, Field

import database
from auth import get_super_admin
from services.admin_audit_service import log_admin_action

router = APIRouter(prefix="/api/admin", tags=["Admin"])

# Account statuses understood by auth.get_account_context:
#   allowed through: "active", "trial", ""   |   blocked (402): anything else
SUSPENDED_STATUS = "suspended"
ACTIVE_STATUS = "active"

# Campaign statuses counted as "active" in list rollups
ACTIVE_CAMPAIGN_STATUSES = ["active"]

# Keys accepted as per-account quota overrides (read by daily_cap_service)
QUOTA_OVERRIDE_KEYS = ("email", "linkedin_connection", "linkedin_inmail", "linkedin_message")

_SENSITIVE_KEY_RE = re.compile(r"token|secret|password|api_key|apikey|credential", re.IGNORECASE)


def _serialize(doc):
    """Recursively convert ObjectIds to strings."""
    if isinstance(doc, ObjectId):
        return str(doc)
    if isinstance(doc, dict):
        return {k: _serialize(v) for k, v in doc.items()}
    if isinstance(doc, list):
        return [_serialize(v) for v in doc]
    return doc


def _redact(doc: dict) -> dict:
    """Drop any credential-shaped fields from a connected-account doc."""
    return {k: v for k, v in doc.items() if not _SENSITIVE_KEY_RE.search(k)}


def _account_oid(account_id: str) -> ObjectId:
    try:
        return ObjectId(account_id)
    except Exception:
        raise HTTPException(status_code=404, detail="Account not found")


async def _load_account(account_id: str) -> dict:
    account = await database.accounts_collection.find_one({"_id": _account_oid(account_id)})
    if not account:
        raise HTTPException(status_code=404, detail="Account not found")
    return account


# ---------------------------------------------------------------------------
# GET /api/admin/accounts — paginated list with rollups
# ---------------------------------------------------------------------------

@router.get("/accounts")
async def list_accounts(
    page: int = Query(1, ge=1),
    page_size: int = Query(25, ge=1, le=100),
    search: Optional[str] = Query(None, description="Substring match on name/slug"),
    status: Optional[str] = Query(None, description="Filter by account status"),
    _admin: dict = Depends(get_super_admin),
):
    match: dict = {}
    if search:
        rx = {"$regex": re.escape(search), "$options": "i"}
        match["$or"] = [{"name": rx}, {"slug": rx}]
    if status:
        match["status"] = status

    skip = (page - 1) * page_size
    pipeline = [
        {"$match": match},
        {"$sort": {"created_at": -1}},
        {"$skip": skip},
        {"$limit": page_size},
        # Member count
        {"$lookup": {
            "from": "account_members",
            "localField": "_id",
            "foreignField": "account_id",
            "as": "_members",
        }},
        # Campaigns (status + updated_at only)
        {"$lookup": {
            "from": "campaigns",
            "let": {"aid": "$_id"},
            "pipeline": [
                {"$match": {"$expr": {"$eq": ["$account_id", "$$aid"]}}},
                {"$project": {"status": 1, "updated_at": 1}},
            ],
            "as": "_camps",
        }},
        # Enrolled prospect count
        {"$lookup": {
            "from": "campaign_enrollments",
            "let": {"aid": "$_id"},
            "pipeline": [
                {"$match": {"$expr": {"$eq": ["$account_id", "$$aid"]}}},
                {"$count": "n"},
            ],
            "as": "_enr",
        }},
        {"$addFields": {
            "member_count": {"$size": "$_members"},
            "campaign_count_total": {"$size": "$_camps"},
            "campaign_count_active": {
                "$size": {
                    "$filter": {
                        "input": "$_camps",
                        "cond": {"$in": ["$$this.status", ACTIVE_CAMPAIGN_STATUSES]},
                    }
                }
            },
            "enrolled_prospect_count": {"$ifNull": [{"$first": "$_enr.n"}, 0]},
            "last_activity_at": {
                "$max": {
                    "$concatArrays": [
                        [{"$ifNull": ["$updated_at", None]}],
                        "$_camps.updated_at",
                    ]
                }
            },
        }},
        {"$project": {"_members": 0, "_camps": 0, "_enr": 0}},
    ]

    total = await database.accounts_collection.count_documents(match)
    rows = await database.accounts_collection.aggregate(pipeline).to_list(page_size)

    accounts = []
    for row in rows:
        accounts.append(_serialize({
            "_id": row["_id"],
            "name": row.get("name"),
            "slug": row.get("slug"),
            "status": row.get("status"),
            "plan": row.get("plan"),
            "trial_ends_at": row.get("trial_ends_at"),
            "created_at": row.get("created_at"),
            "member_count": row.get("member_count", 0),
            "campaign_count_total": row.get("campaign_count_total", 0),
            "campaign_count_active": row.get("campaign_count_active", 0),
            "enrolled_prospect_count": row.get("enrolled_prospect_count", 0),
            "last_activity_at": row.get("last_activity_at"),
            "quota_overrides": row.get("quota_overrides"),
        }))

    return {"accounts": accounts, "total": total, "page": page, "page_size": page_size}


# ---------------------------------------------------------------------------
# GET /api/admin/accounts/{account_id} — full detail
# ---------------------------------------------------------------------------

@router.get("/accounts/{account_id}")
async def get_account_detail(
    account_id: str,
    _admin: dict = Depends(get_super_admin),
):
    account = await _load_account(account_id)
    oid = account["_id"]
    aid_str = str(oid)

    # Members + user info
    members = await database.account_members_collection.find({"account_id": oid}).to_list(100)
    user_ids = [m["user_id"] for m in members if m.get("user_id")]
    users = await database.users_collection.find(
        {"_id": {"$in": user_ids}}, {"email": 1, "name": 1, "is_active": 1, "created_at": 1}
    ).to_list(100)
    users_by_id = {u["_id"]: u for u in users}
    member_rows = []
    for m in members:
        u = users_by_id.get(m.get("user_id")) or {}
        member_rows.append({
            "user_id": str(m.get("user_id")),
            "role": m.get("role"),
            "joined_at": m.get("joined_at"),
            "email": u.get("email"),
            "name": u.get("name"),
            "is_active": u.get("is_active", True),
        })

    # Connected channel accounts (redacted)
    linkedin_accounts = await database.linkedin_accounts_collection.find({"account_id": oid}).to_list(50)
    email_accounts = await database.email_accounts_collection.find({"account_id": oid}).to_list(50)

    # Campaign summaries
    campaigns = await database.campaigns_collection.find(
        {"account_id": oid},
        {
            "name": 1, "status": 1, "type": 1, "is_smart_campaign": 1,
            "discovery_status": 1, "message_gen_status": 1,
            "total_enrolled": 1, "emails_sent": 1, "replied_count": 1,
            "created_at": 1, "updated_at": 1,
        },
    ).sort("created_at", -1).to_list(100)

    # 30-day usage/cost rollup
    since = datetime.now(timezone.utc) - timedelta(days=30)

    async def _sum(collection, date_field, value_field, extra_match=None):
        match = {"account_id": aid_str, date_field: {"$gte": since}}
        if extra_match:
            match.update(extra_match)
        agg = await collection.aggregate([
            {"$match": match},
            {"$group": {"_id": None, "total": {"$sum": f"${value_field}"}, "count": {"$sum": 1}}},
        ]).to_list(1)
        return (round(agg[0]["total"], 6), agg[0]["count"]) if agg else (0.0, 0)

    (apify_cost, apify_runs), (or_cost, or_calls), (gt_credits, gt_calls) = await asyncio.gather(
        _sum(database.apify_usage_collection, "started_at", "cost_usd"),
        _sum(database.openrouter_usage_collection, "requested_at", "cost_usd"),
        _sum(database.growthtoolkit_usage_collection, "created_at", "credits_used"),
    )
    messages_sent_30d = await database.campaign_messages_collection.count_documents(
        {"account_id": oid, "status": "sent", "sent_at": {"$gte": since}}
    )

    return {
        "account": _serialize(account),
        "members": _serialize(member_rows),
        "quotas": {
            "daily_email_quota": account.get("daily_email_quota"),
            "daily_linkedin_connection_quota": account.get("daily_linkedin_connection_quota"),
            "daily_linkedin_inmail_quota": account.get("daily_linkedin_inmail_quota"),
            "enrichment_batch_limit": account.get("enrichment_batch_limit"),
            "monthly_apify_budget_usd": account.get("monthly_apify_budget_usd"),
            "quota_overrides": account.get("quota_overrides") or {},
        },
        "linkedin_accounts": [_redact(_serialize(a)) for a in linkedin_accounts],
        "email_accounts": [_redact(_serialize(a)) for a in email_accounts],
        "campaigns": _serialize(campaigns),
        "usage_30d": {
            "apify_cost_usd": apify_cost,
            "apify_runs": apify_runs,
            "openrouter_cost_usd": or_cost,
            "openrouter_calls": or_calls,
            "growthtoolkit_credits": gt_credits,
            "growthtoolkit_calls": gt_calls,
            "messages_sent": messages_sent_30d,
            "total_cost_usd": round(apify_cost + or_cost, 6),
        },
    }


# ---------------------------------------------------------------------------
# POST /api/admin/accounts/{account_id}/suspend | /reactivate
# ---------------------------------------------------------------------------

@router.post("/accounts/{account_id}/suspend")
async def suspend_account(
    account_id: str,
    admin: dict = Depends(get_super_admin),
):
    account = await _load_account(account_id)
    await database.accounts_collection.update_one(
        {"_id": account["_id"]},
        {"$set": {"status": SUSPENDED_STATUS, "updated_at": datetime.utcnow()}},
    )
    await log_admin_action(
        admin["email"], "account.suspend", "account", account_id,
        {"previous_status": account.get("status")},
    )
    return {"account_id": account_id, "status": SUSPENDED_STATUS}


@router.post("/accounts/{account_id}/reactivate")
async def reactivate_account(
    account_id: str,
    admin: dict = Depends(get_super_admin),
):
    account = await _load_account(account_id)
    await database.accounts_collection.update_one(
        {"_id": account["_id"]},
        {"$set": {"status": ACTIVE_STATUS, "updated_at": datetime.utcnow()}},
    )
    await log_admin_action(
        admin["email"], "account.reactivate", "account", account_id,
        {"previous_status": account.get("status")},
    )
    return {"account_id": account_id, "status": ACTIVE_STATUS}


# ---------------------------------------------------------------------------
# POST /api/admin/accounts/{account_id}/extend-trial
# ---------------------------------------------------------------------------

class ExtendTrialRequest(BaseModel):
    days: int = Field(..., ge=1, le=365)


@router.post("/accounts/{account_id}/extend-trial")
async def extend_trial(
    account_id: str,
    body: ExtendTrialRequest,
    admin: dict = Depends(get_super_admin),
):
    account = await _load_account(account_id)
    now = datetime.utcnow()
    current_end = account.get("trial_ends_at")
    # Extend from the later of (now, current end) so expired trials restart from today
    base = current_end if isinstance(current_end, datetime) and current_end > now else now
    new_end = base + timedelta(days=body.days)

    await database.accounts_collection.update_one(
        {"_id": account["_id"]},
        {"$set": {"trial_ends_at": new_end, "updated_at": now}},
    )
    await log_admin_action(
        admin["email"], "account.extend_trial", "account", account_id,
        {"days": body.days, "previous_trial_ends_at": current_end, "new_trial_ends_at": new_end},
    )
    return {"account_id": account_id, "trial_ends_at": new_end.isoformat()}


# ---------------------------------------------------------------------------
# PATCH /api/admin/accounts/{account_id}/quotas — per-account cap overrides
# ---------------------------------------------------------------------------

class QuotaOverrideRequest(BaseModel):
    """Set a value to override; explicit null clears that override."""
    email: Optional[int] = Field(None, ge=0, le=1000)
    linkedin_connection: Optional[int] = Field(None, ge=0, le=1000)
    linkedin_inmail: Optional[int] = Field(None, ge=0, le=1000)
    linkedin_message: Optional[int] = Field(None, ge=0, le=1000)


# Legacy display fields on the account doc, kept in sync for visibility
_LEGACY_QUOTA_FIELDS = {
    "email": "daily_email_quota",
    "linkedin_connection": "daily_linkedin_connection_quota",
    "linkedin_inmail": "daily_linkedin_inmail_quota",
}


@router.patch("/accounts/{account_id}/quotas")
async def update_account_quotas(
    account_id: str,
    body: QuotaOverrideRequest,
    admin: dict = Depends(get_super_admin),
):
    account = await _load_account(account_id)
    provided = body.model_dump(exclude_unset=True)
    if not provided:
        raise HTTPException(status_code=400, detail="No quota fields provided")

    sets: dict = {"updated_at": datetime.utcnow()}
    unsets: dict = {}
    for key, value in provided.items():
        if key not in QUOTA_OVERRIDE_KEYS:
            continue
        if value is None:
            unsets[f"quota_overrides.{key}"] = ""
        else:
            sets[f"quota_overrides.{key}"] = int(value)
            legacy = _LEGACY_QUOTA_FIELDS.get(key)
            if legacy:
                sets[legacy] = int(value)

    update: dict = {"$set": sets}
    if unsets:
        update["$unset"] = unsets
    await database.accounts_collection.update_one({"_id": account["_id"]}, update)

    updated = await database.accounts_collection.find_one(
        {"_id": account["_id"]}, {"quota_overrides": 1}
    )
    await log_admin_action(
        admin["email"], "account.quota_override", "account", account_id,
        {"changes": provided, "previous": account.get("quota_overrides") or {}},
    )
    return {
        "account_id": account_id,
        "quota_overrides": (updated or {}).get("quota_overrides") or {},
    }
