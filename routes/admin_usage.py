"""
Superadmin — cost & usage observability.

Merges apify_usage + openrouter_usage + growthtoolkit_usage (+ email sends
from campaign_messages, across Gmail/Zoho/SMTP providers) into per-account
per-day rollups.

Notes on data shapes:
- apify_usage / openrouter_usage: account_id stored as *string*, cost in cost_usd.
- growthtoolkit_usage: {account_id: str|None, endpoint, credits_used, success,
  code, duration_ms, prospect_id, created_at} — written by growthtoolkit_service.
  The collection may not exist yet; aggregations return empty gracefully.
- campaign_messages: account_id stored as ObjectId; email sends costed at
  COST_PER_EMAIL (same constant as routes/analytics.py) — a rough per-send
  estimate, not provider-specific billing (Gmail/Zoho/SMTP costs vary by plan).
"""
import asyncio
from datetime import datetime, timedelta, timezone
from typing import Optional

from fastapi import APIRouter, Depends, HTTPException, Query

import database
from auth import get_super_admin

router = APIRouter(prefix="/api/admin", tags=["Admin"])

# Rough per-email cost estimate for the cost dashboard (mirrors routes/analytics.py).
# Not tied to a specific provider's billing — Gmail/Zoho/SMTP costs vary by plan.
COST_PER_EMAIL = 0.0004

# GrowthToolkit bills in credits; there is no per-credit USD constant in the
# codebase, so credits are reported as-is (no invented dollar conversion).


def _parse_range(from_date: Optional[str], to_date: Optional[str], default_days: int = 30):
    now = datetime.now(timezone.utc)
    try:
        start = datetime.fromisoformat(from_date) if from_date else now - timedelta(days=default_days)
        end = datetime.fromisoformat(to_date) if to_date else now
    except ValueError:
        raise HTTPException(status_code=400, detail="Invalid from/to date (use ISO format)")
    if start.tzinfo is None:
        start = start.replace(tzinfo=timezone.utc)
    if end.tzinfo is None:
        end = end.replace(tzinfo=timezone.utc)
    return start, end


# ---------------------------------------------------------------------------
# GET /api/admin/usage/growthtoolkit
# ---------------------------------------------------------------------------

@router.get("/usage/growthtoolkit")
async def get_growthtoolkit_usage(
    from_date: Optional[str] = Query(None, alias="from"),
    to_date: Optional[str] = Query(None, alias="to"),
    account_id: Optional[str] = Query(None),
    _admin: dict = Depends(get_super_admin),
):
    """Per-account per-day rollup of growthtoolkit_usage, broken down by endpoint."""
    start, end = _parse_range(from_date, to_date)
    match: dict = {"created_at": {"$gte": start, "$lte": end}}
    if account_id:
        match["account_id"] = account_id

    pipeline = [
        {"$match": match},
        {"$group": {
            "_id": {
                "account_id": "$account_id",
                "day": {"$dateToString": {"format": "%Y-%m-%d", "date": "$created_at"}},
                "endpoint": "$endpoint",
            },
            "calls": {"$sum": 1},
            "credits_used": {"$sum": {"$ifNull": ["$credits_used", 0]}},
            "successes": {"$sum": {"$cond": [{"$eq": ["$success", True]}, 1, 0]}},
            "avg_duration_ms": {"$avg": "$duration_ms"},
        }},
        {"$sort": {"_id.day": -1, "_id.account_id": 1, "_id.endpoint": 1}},
        {"$limit": 5000},
    ]
    try:
        rows = await database.growthtoolkit_usage_collection.aggregate(pipeline).to_list(5000)
    except Exception:
        rows = []  # collection missing / not yet created

    out = []
    total_credits = 0
    total_calls = 0
    total_successes = 0
    for r in rows:
        calls = r["calls"]
        successes = r["successes"]
        total_credits += r["credits_used"]
        total_calls += calls
        total_successes += successes
        out.append({
            "account_id": r["_id"].get("account_id"),
            "day": r["_id"]["day"],
            "endpoint": r["_id"].get("endpoint"),
            "calls": calls,
            "credits_used": r["credits_used"],
            "success_rate": round(successes / calls, 4) if calls else None,
            "avg_duration_ms": round(r["avg_duration_ms"], 1) if r.get("avg_duration_ms") is not None else None,
        })

    return {
        "rows": out,
        "totals": {
            "calls": total_calls,
            "credits_used": total_credits,
            "success_rate": round(total_successes / total_calls, 4) if total_calls else None,
        },
        "from": start.isoformat(),
        "to": end.isoformat(),
    }


# ---------------------------------------------------------------------------
# GET /api/admin/usage/unified
# ---------------------------------------------------------------------------

@router.get("/usage/unified")
async def get_unified_usage(
    from_date: Optional[str] = Query(None, alias="from"),
    to_date: Optional[str] = Query(None, alias="to"),
    account_id: Optional[str] = Query(None),
    _admin: dict = Depends(get_super_admin),
):
    """One response merging apify + openrouter + growthtoolkit + email sends
    per account per day, with cost estimates where pricing is known."""
    from bson import ObjectId

    start, end = _parse_range(from_date, to_date)

    def _day_group(date_field: str, account_expr: str = "$account_id"):
        return {
            "account_id": account_expr,
            "day": {"$dateToString": {"format": "%Y-%m-%d", "date": f"${date_field}"}},
        }

    async def _agg(collection, date_field: str, sums: dict, extra_match: Optional[dict] = None,
                   account_is_oid: bool = False):
        match: dict = {date_field: {"$gte": start, "$lte": end}}
        if account_id:
            if account_is_oid:
                try:
                    match["account_id"] = ObjectId(account_id)
                except Exception:
                    return []
            else:
                match["account_id"] = account_id
        if extra_match:
            match.update(extra_match)
        account_expr = {"$toString": "$account_id"} if account_is_oid else "$account_id"
        pipeline = [
            {"$match": match},
            {"$group": {"_id": _day_group(date_field, account_expr), **sums}},
            {"$limit": 5000},
        ]
        try:
            return await collection.aggregate(pipeline).to_list(5000)
        except Exception:
            return []

    apify_rows, or_rows, gt_rows, email_rows = await asyncio.gather(
        _agg(database.apify_usage_collection, "started_at",
             {"apify_cost_usd": {"$sum": "$cost_usd"}, "apify_runs": {"$sum": 1}}),
        _agg(database.openrouter_usage_collection, "requested_at",
             {"openrouter_cost_usd": {"$sum": "$cost_usd"}, "openrouter_calls": {"$sum": 1}}),
        _agg(database.growthtoolkit_usage_collection, "created_at",
             {"growthtoolkit_credits": {"$sum": {"$ifNull": ["$credits_used", 0]}},
              "growthtoolkit_calls": {"$sum": 1}}),
        _agg(database.campaign_messages_collection, "sent_at",
             {"emails_sent": {"$sum": 1}},
             extra_match={"channel": "email", "status": "sent"},
             account_is_oid=True),
    )

    merged: dict[tuple, dict] = {}

    def _fold(rows: list[dict]):
        for r in rows:
            key = (r["_id"].get("account_id"), r["_id"]["day"])
            bucket = merged.setdefault(key, {
                "account_id": key[0], "day": key[1],
                "apify_cost_usd": 0.0, "apify_runs": 0,
                "openrouter_cost_usd": 0.0, "openrouter_calls": 0,
                "growthtoolkit_credits": 0, "growthtoolkit_calls": 0,
                "emails_sent": 0,
            })
            for k, v in r.items():
                if k != "_id":
                    bucket[k] = bucket.get(k, 0) + v

    for rows in (apify_rows, or_rows, gt_rows, email_rows):
        _fold(rows)

    out = []
    for bucket in merged.values():
        bucket["apify_cost_usd"] = round(bucket["apify_cost_usd"], 6)
        bucket["openrouter_cost_usd"] = round(bucket["openrouter_cost_usd"], 6)
        bucket["email_cost_usd"] = round(bucket["emails_sent"] * COST_PER_EMAIL, 6)
        bucket["total_cost_usd"] = round(
            bucket["apify_cost_usd"] + bucket["openrouter_cost_usd"] + bucket["email_cost_usd"], 6
        )
        out.append(bucket)
    out.sort(key=lambda b: (b["day"], str(b["account_id"] or "")), reverse=True)

    return {
        "rows": out,
        "from": start.isoformat(),
        "to": end.isoformat(),
        "notes": "growthtoolkit reported in credits (no USD pricing constant); "
                 "total_cost_usd = apify + openrouter + email estimate",
    }


# ---------------------------------------------------------------------------
# GET /api/admin/costs/top-accounts
# ---------------------------------------------------------------------------

@router.get("/costs/top-accounts")
async def get_top_cost_accounts(
    days: int = Query(30, ge=1, le=365),
    limit: int = Query(20, ge=1, le=100),
    _admin: dict = Depends(get_super_admin),
):
    """Top spenders (apify + openrouter USD) with cost-per-enrolled-prospect."""
    from bson import ObjectId

    start = datetime.now(timezone.utc) - timedelta(days=days)

    async def _cost_by_account(collection, date_field: str):
        pipeline = [
            {"$match": {date_field: {"$gte": start}}},
            {"$group": {"_id": "$account_id", "cost": {"$sum": "$cost_usd"}}},
        ]
        return await collection.aggregate(pipeline).to_list(None)

    async def _credits_by_account():
        pipeline = [
            {"$match": {"created_at": {"$gte": start}}},
            {"$group": {"_id": "$account_id", "credits": {"$sum": {"$ifNull": ["$credits_used", 0]}}}},
        ]
        try:
            return await database.growthtoolkit_usage_collection.aggregate(pipeline).to_list(None)
        except Exception:
            return []

    async def _enrolled_by_account():
        pipeline = [
            {"$match": {"enrolled_at": {"$gte": start}}},
            {"$group": {"_id": {"$toString": "$account_id"}, "enrolled": {"$sum": 1}}},
        ]
        return await database.campaign_enrollments_collection.aggregate(pipeline).to_list(None)

    apify_rows, or_rows, gt_rows, enrolled_rows = await asyncio.gather(
        _cost_by_account(database.apify_usage_collection, "started_at"),
        _cost_by_account(database.openrouter_usage_collection, "requested_at"),
        _credits_by_account(),
        _enrolled_by_account(),
    )

    totals: dict[str, dict] = {}

    def _bucket(aid) -> dict:
        key = str(aid) if aid is not None else "unattributed"
        return totals.setdefault(key, {
            "account_id": key if key != "unattributed" else None,
            "apify_cost_usd": 0.0, "openrouter_cost_usd": 0.0,
            "growthtoolkit_credits": 0, "enrolled_prospects": 0,
        })

    for r in apify_rows:
        _bucket(r["_id"])["apify_cost_usd"] += r["cost"]
    for r in or_rows:
        _bucket(r["_id"])["openrouter_cost_usd"] += r["cost"]
    for r in gt_rows:
        _bucket(r["_id"])["growthtoolkit_credits"] += r["credits"]
    for r in enrolled_rows:
        _bucket(r["_id"])["enrolled_prospects"] += r["enrolled"]

    rows = []
    for b in totals.values():
        total_cost = round(b["apify_cost_usd"] + b["openrouter_cost_usd"], 6)
        b["apify_cost_usd"] = round(b["apify_cost_usd"], 6)
        b["openrouter_cost_usd"] = round(b["openrouter_cost_usd"], 6)
        b["total_cost_usd"] = total_cost
        b["cost_per_enrolled_prospect"] = (
            round(total_cost / b["enrolled_prospects"], 4) if b["enrolled_prospects"] else None
        )
        rows.append(b)
    rows.sort(key=lambda b: b["total_cost_usd"], reverse=True)
    rows = rows[:limit]

    # Attach account names
    oids = []
    for b in rows:
        if b["account_id"]:
            try:
                oids.append(ObjectId(b["account_id"]))
            except Exception:
                pass
    accounts = await database.accounts_collection.find(
        {"_id": {"$in": oids}}, {"name": 1, "slug": 1, "plan": 1, "status": 1}
    ).to_list(None)
    by_id = {str(a["_id"]): a for a in accounts}
    for b in rows:
        acct = by_id.get(b["account_id"] or "")
        b["account_name"] = acct.get("name") if acct else None
        b["plan"] = acct.get("plan") if acct else None
        b["status"] = acct.get("status") if acct else None

    return {"rows": rows, "days": days}
