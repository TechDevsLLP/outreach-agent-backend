"""
Superadmin — the cost portal.

One place that answers "what is this system costing us, and who is spending it":

    GET /api/admin/costs/overview          totals + daily trend + provider split
    GET /api/admin/costs/by-campaign       spend per campaign, with unit economics
    GET /api/admin/costs/by-account        spend per account/user, with unit economics
    GET /api/admin/costs/by-driver         spend per model / actor / endpoint
    GET /api/admin/costs/campaign/{id}     full drill-down for one campaign

Cost sources and their quirks — every one of these is folded into the same
per-(account, campaign, day) shape by `_collect()`:

| Source                | date field   | account_id | cost                          |
|-----------------------|--------------|------------|-------------------------------|
| apify_usage           | started_at   | string     | cost_usd (real, from Apify)   |
| openrouter_usage      | requested_at | string     | cost_usd (tokens × price map) |
| growthtoolkit_usage   | created_at   | string     | credits × COST_PER_GT_CREDIT  |
| send_attempts         | sent_at      | string     | sends × COST_PER_EMAIL        |

Direct Gemini SDK calls are written into `openrouter_usage` too (the model name
identifies the provider), so they are covered by the OpenRouter source.

Email sends come from `send_attempts` (the authoritative dispatch record) —
NOT `campaign_messages`, which is unpopulated in the current pipeline. Send
attempts carry `enrollment_id` rather than `campaign_id`, so campaign
attribution goes through a lookup into campaign_enrollments.

Estimated vs. metered: Apify and OpenRouter costs are derived from real usage
records. GrowthToolkit credits and email sends are converted with the flat
constants below, and every response labels them as estimates rather than
presenting them as billed amounts.
"""
import asyncio
from datetime import datetime, timedelta, timezone
from typing import Optional

from bson import ObjectId
from fastapi import APIRouter, Depends, HTTPException, Query

import database
from auth import get_super_admin

router = APIRouter(prefix="/api/admin/costs", tags=["Admin"])

# ── Estimation constants ─────────────────────────────────────────────────────
# Neither provider exposes a per-call dollar amount we can record, so these are
# flat conversion rates used purely for the cost dashboard. They are surfaced
# in every response under `assumptions` so the numbers are never mistaken for
# billed amounts.
COST_PER_EMAIL = 0.0004          # mirrors routes/analytics.py and admin_usage.py
COST_PER_GT_CREDIT = 0.01        # GrowthToolkit bills in credits, not dollars

PROVIDERS = ("apify", "ai", "growthtoolkit", "email")

MAX_RANGE_DAYS = 400


# ── Range handling ───────────────────────────────────────────────────────────

def _parse_range(from_date: Optional[str], to_date: Optional[str], default_days: int):
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
    if end < start:
        raise HTTPException(status_code=400, detail="`to` must be after `from`")
    if (end - start).days > MAX_RANGE_DAYS:
        raise HTTPException(
            status_code=400, detail=f"Date range too wide (max {MAX_RANGE_DAYS} days)"
        )
    return start, end


def _account_match_value(account_id: str, as_oid: bool):
    if not as_oid:
        return account_id
    try:
        return ObjectId(account_id)
    except Exception:
        return None


# ── The one aggregation everything else is built on ──────────────────────────

async def _collect(
    start: datetime,
    end: datetime,
    account_id: Optional[str] = None,
    campaign_id: Optional[str] = None,
) -> list[dict]:
    """Fold every cost source into rows of
    {account_id, campaign_id, day, provider, cost_usd, units, ...}.

    Returning flat rows (rather than a pre-grouped shape) lets each endpoint
    group by whatever dimension it needs without re-querying Mongo.
    """

    async def _agg(
        collection,
        *,
        provider: str,
        date_field: str,
        cost_expr: dict,
        unit_expr: dict,
        account_is_oid: bool = False,
        campaign_is_oid: bool = False,
        extra_match: Optional[dict] = None,
        driver_field: Optional[str] = None,
    ) -> list[dict]:
        match: dict = {date_field: {"$gte": start, "$lte": end}}
        if account_id:
            value = _account_match_value(account_id, account_is_oid)
            if value is None:
                return []
            match["account_id"] = value
        if campaign_id:
            if campaign_is_oid:
                try:
                    match["campaign_id"] = ObjectId(campaign_id)
                except Exception:
                    return []
            else:
                match["campaign_id"] = campaign_id
        if extra_match:
            match.update(extra_match)

        group_id: dict = {
            "account_id": {"$toString": "$account_id"},
            "campaign_id": {"$toString": "$campaign_id"},
            "day": {"$dateToString": {"format": "%Y-%m-%d", "date": f"${date_field}"}},
        }
        if driver_field:
            group_id["driver"] = f"${driver_field}"

        pipeline = [
            {"$match": match},
            {"$group": {
                "_id": group_id,
                "cost_usd": {"$sum": cost_expr},
                "units": {"$sum": unit_expr},
                "calls": {"$sum": 1},
            }},
            {"$limit": 20000},
        ]
        try:
            rows = await collection.aggregate(pipeline).to_list(20000)
        except Exception:
            # A usage collection may not exist yet on a fresh deployment.
            return []

        out = []
        for r in rows:
            key = r["_id"]
            out.append({
                "provider": provider,
                "account_id": key.get("account_id") or None,
                "campaign_id": key.get("campaign_id") or None,
                "day": key["day"],
                "driver": key.get("driver") or provider,
                "cost_usd": float(r.get("cost_usd") or 0.0),
                "units": float(r.get("units") or 0),
                "calls": int(r.get("calls") or 0),
            })
        return out

    async def _email_agg() -> list[dict]:
        """Email sends, with campaign attribution resolved via the enrollment.

        `send_attempts` records the enrollment it belongs to, not the campaign,
        so a $lookup pulls the campaign_id across before grouping.
        """
        match: dict = {
            "sent_at": {"$gte": start, "$lte": end},
            "channel": "email",
            "state": "sent",
        }
        if account_id:
            match["account_id"] = account_id

        pipeline: list[dict] = [
            {"$match": match},
            {"$lookup": {
                "from": "campaign_enrollments",
                "let": {"eid": "$enrollment_id"},
                "pipeline": [
                    {"$match": {"$expr": {"$eq": [{"$toString": "$_id"}, "$$eid"]}}},
                    {"$project": {"campaign_id": 1}},
                ],
                "as": "_enrollment",
            }},
            {"$addFields": {
                "campaign_id": {"$toString": {"$arrayElemAt": ["$_enrollment.campaign_id", 0]}}
            }},
        ]
        if campaign_id:
            pipeline.append({"$match": {"campaign_id": campaign_id}})
        pipeline += [
            {"$group": {
                "_id": {
                    "account_id": {"$toString": "$account_id"},
                    "campaign_id": "$campaign_id",
                    "day": {"$dateToString": {"format": "%Y-%m-%d", "date": "$sent_at"}},
                    "driver": "$provider",
                },
                "cost_usd": {"$sum": COST_PER_EMAIL},
                "units": {"$sum": 1},
                "calls": {"$sum": 1},
            }},
            {"$limit": 20000},
        ]
        try:
            rows = await database.send_attempts_collection.aggregate(pipeline).to_list(20000)
        except Exception:
            return []

        return [{
            "provider": "email",
            "account_id": r["_id"].get("account_id") or None,
            "campaign_id": r["_id"].get("campaign_id") or None,
            "day": r["_id"]["day"],
            "driver": r["_id"].get("driver") or "email",
            "cost_usd": float(r.get("cost_usd") or 0.0),
            "units": float(r.get("units") or 0),
            "calls": int(r.get("calls") or 0),
        } for r in rows]

    apify, ai, growthtoolkit, email = await asyncio.gather(
        _agg(
            database.apify_usage_collection,
            provider="apify",
            date_field="started_at",
            cost_expr={"$ifNull": ["$cost_usd", 0]},
            unit_expr={"$ifNull": ["$items_returned", 0]},
            driver_field="actor_id",
        ),
        _agg(
            database.openrouter_usage_collection,
            provider="ai",
            date_field="requested_at",
            cost_expr={"$ifNull": ["$cost_usd", 0]},
            unit_expr={"$ifNull": ["$total_tokens", 0]},
            driver_field="model",
        ),
        _agg(
            database.growthtoolkit_usage_collection,
            provider="growthtoolkit",
            date_field="created_at",
            cost_expr={"$multiply": [{"$ifNull": ["$credits_used", 0]}, COST_PER_GT_CREDIT]},
            unit_expr={"$ifNull": ["$credits_used", 0]},
            driver_field="endpoint",
        ),
        _email_agg(),
    )

    return [*apify, *ai, *growthtoolkit, *email]


def _empty_providers() -> dict:
    return {p: 0.0 for p in PROVIDERS}


def _blank_bucket(**extra) -> dict:
    return {
        "cost_usd": 0.0,
        "by_provider": _empty_providers(),
        "calls": 0,
        **extra,
    }


def _add(bucket: dict, row: dict) -> None:
    bucket["cost_usd"] += row["cost_usd"]
    bucket["by_provider"][row["provider"]] += row["cost_usd"]
    bucket["calls"] += row["calls"]


def _round_bucket(bucket: dict) -> dict:
    bucket["cost_usd"] = round(bucket["cost_usd"], 6)
    bucket["by_provider"] = {k: round(v, 6) for k, v in bucket["by_provider"].items()}
    return bucket


ASSUMPTIONS = {
    "metered": ["apify", "ai"],
    "estimated": ["growthtoolkit", "email"],
    "cost_per_email_usd": COST_PER_EMAIL,
    "cost_per_growthtoolkit_credit_usd": COST_PER_GT_CREDIT,
    "note": (
        "Apify and AI costs come from recorded usage. GrowthToolkit credits and "
        "email sends are converted at the flat rates above and are estimates."
    ),
}


# ── Name resolution ──────────────────────────────────────────────────────────

async def _account_names(account_ids: list[str]) -> dict[str, dict]:
    oids = []
    for aid in account_ids:
        try:
            oids.append(ObjectId(aid))
        except Exception:
            continue
    if not oids:
        return {}
    docs = await database.accounts_collection.find(
        {"_id": {"$in": oids}},
        {"name": 1, "slug": 1, "plan": 1, "status": 1},
    ).to_list(None)
    return {str(d["_id"]): d for d in docs}


async def _account_owners(account_ids: list[str]) -> dict[str, dict]:
    """Primary user per account, so the portal can show a person, not just a tenant."""
    oids = []
    for aid in account_ids:
        try:
            oids.append(ObjectId(aid))
        except Exception:
            continue
    if not oids:
        return {}
    members = await database.account_members_collection.find(
        {"account_id": {"$in": oids + [str(o) for o in oids]}},
        {"account_id": 1, "user_id": 1, "role": 1},
    ).to_list(None)

    # Prefer an owner/admin membership when there is more than one member.
    best: dict[str, dict] = {}
    for m in members:
        aid = str(m.get("account_id"))
        role = (m.get("role") or "").lower()
        current = best.get(aid)
        if current is None or (role in ("owner", "admin") and (current.get("role") or "").lower() not in ("owner", "admin")):
            best[aid] = m

    user_ids = []
    for m in best.values():
        try:
            user_ids.append(ObjectId(str(m.get("user_id"))))
        except Exception:
            continue
    if not user_ids:
        return {}
    users = await database.users_collection.find(
        {"_id": {"$in": user_ids}}, {"name": 1, "email": 1}
    ).to_list(None)
    by_user = {str(u["_id"]): u for u in users}

    out: dict[str, dict] = {}
    for aid, m in best.items():
        user = by_user.get(str(m.get("user_id")))
        if user:
            out[aid] = {"user_id": str(user["_id"]), "name": user.get("name"), "email": user.get("email")}
    return out


async def _campaign_meta(campaign_ids: list[str]) -> dict[str, dict]:
    oids = []
    for cid in campaign_ids:
        try:
            oids.append(ObjectId(cid))
        except Exception:
            continue
    if not oids:
        return {}
    docs = await database.campaigns_collection.find(
        {"_id": {"$in": oids}},
        {"name": 1, "status": 1, "account_id": 1, "created_at": 1, "campaign_type": 1},
    ).to_list(None)
    return {str(d["_id"]): d for d in docs}


# ── Unit economics ───────────────────────────────────────────────────────────

async def _enrolled_counts(start: datetime, end: datetime, group: str) -> dict[str, int]:
    """Prospects enrolled in the window, grouped by account or campaign.

    Used as the denominator for cost-per-enrolled-prospect — the single number
    that makes spend comparable across accounts of very different sizes.
    """
    field = "$account_id" if group == "account" else "$campaign_id"
    pipeline = [
        {"$match": {"enrolled_at": {"$gte": start, "$lte": end}}},
        {"$group": {"_id": {"$toString": field}, "n": {"$sum": 1}}},
    ]
    try:
        rows = await database.campaign_enrollments_collection.aggregate(pipeline).to_list(None)
    except Exception:
        return {}
    return {r["_id"]: r["n"] for r in rows if r.get("_id")}


def _unit_costs(cost: float, enrolled: int, messages: int) -> dict:
    return {
        "enrolled_prospects": enrolled,
        "messages_sent": messages,
        "cost_per_enrolled_prospect": round(cost / enrolled, 4) if enrolled else None,
        "cost_per_message_sent": round(cost / messages, 4) if messages else None,
    }


# ---------------------------------------------------------------------------
# GET /api/admin/costs/overview
# ---------------------------------------------------------------------------

@router.get("/overview")
async def costs_overview(
    from_date: Optional[str] = Query(None, alias="from"),
    to_date: Optional[str] = Query(None, alias="to"),
    days: int = Query(30, ge=1, le=365),
    _admin: dict = Depends(get_super_admin),
):
    """System-wide spend for the window: total, provider split, and a daily
    series for the trend chart. Also returns today / week / month totals so the
    portal's headline cards don't need three extra round trips."""
    start, end = _parse_range(from_date, to_date, days)
    now = datetime.now(timezone.utc)
    today_start = now.replace(hour=0, minute=0, second=0, microsecond=0)
    week_start = today_start - timedelta(days=now.weekday())
    month_start = today_start.replace(day=1)

    # Widen the fetch to cover month-to-date so the headline cards are exact
    # even when the selected window is shorter.
    fetch_start = min(start, month_start)
    rows = await _collect(fetch_start, max(end, now))

    total = _blank_bucket()
    by_day: dict[str, dict] = {}
    periods = {"today": _blank_bucket(), "this_week": _blank_bucket(), "this_month": _blank_bucket()}
    accounts_seen: set[str] = set()
    campaigns_seen: set[str] = set()

    today_key = today_start.strftime("%Y-%m-%d")
    week_key = week_start.strftime("%Y-%m-%d")
    month_key = month_start.strftime("%Y-%m-%d")
    start_key = start.strftime("%Y-%m-%d")
    end_key = end.strftime("%Y-%m-%d")

    for row in rows:
        day = row["day"]

        if day >= month_key:
            _add(periods["this_month"], row)
        if day >= week_key:
            _add(periods["this_week"], row)
        if day == today_key:
            _add(periods["today"], row)

        # The selected window drives the total, series, and coverage counts.
        if not (start_key <= day <= end_key):
            continue
        _add(total, row)
        _add(by_day.setdefault(day, _blank_bucket(day=day)), row)
        if row["account_id"]:
            accounts_seen.add(row["account_id"])
        if row["campaign_id"]:
            campaigns_seen.add(row["campaign_id"])

    series = [_round_bucket(b) for b in sorted(by_day.values(), key=lambda b: b["day"])]
    window_days = max(1, (end - start).days or 1)

    return {
        "from": start.isoformat(),
        "to": end.isoformat(),
        "total": _round_bucket(total),
        "daily_average_usd": round(total["cost_usd"] / window_days, 6),
        "series": series,
        "periods": {k: _round_bucket(v) for k, v in periods.items()},
        "coverage": {
            "accounts_with_spend": len(accounts_seen),
            "campaigns_with_spend": len(campaigns_seen),
        },
        "assumptions": ASSUMPTIONS,
    }


# ---------------------------------------------------------------------------
# GET /api/admin/costs/by-campaign
# ---------------------------------------------------------------------------

@router.get("/by-campaign")
async def costs_by_campaign(
    from_date: Optional[str] = Query(None, alias="from"),
    to_date: Optional[str] = Query(None, alias="to"),
    days: int = Query(30, ge=1, le=365),
    account_id: Optional[str] = Query(None),
    limit: int = Query(50, ge=1, le=200),
    _admin: dict = Depends(get_super_admin),
):
    """Spend per campaign, most expensive first, with unit economics."""
    start, end = _parse_range(from_date, to_date, days)
    rows, enrolled_by_campaign = await asyncio.gather(
        _collect(start, end, account_id=account_id),
        _enrolled_counts(start, end, group="campaign"),
    )

    buckets: dict[str, dict] = {}
    unattributed = _blank_bucket()

    for row in rows:
        cid = row["campaign_id"]
        if not cid:
            # Platform-level work (onboarding scrapes, voice sync, sweeps) has no
            # campaign. Reported separately instead of being silently dropped.
            _add(unattributed, row)
            continue
        bucket = buckets.setdefault(cid, _blank_bucket(campaign_id=cid, messages_sent=0))
        _add(bucket, row)
        if row["provider"] == "email":
            bucket["messages_sent"] += int(row["units"])

    ranked = sorted(buckets.values(), key=lambda b: b["cost_usd"], reverse=True)[:limit]
    meta = await _campaign_meta([b["campaign_id"] for b in ranked])
    account_ids = [str(m.get("account_id")) for m in meta.values() if m.get("account_id")]
    names = await _account_names(account_ids)

    out = []
    for bucket in ranked:
        cid = bucket["campaign_id"]
        campaign = meta.get(cid) or {}
        acct_id = str(campaign.get("account_id")) if campaign.get("account_id") else None
        account = names.get(acct_id or "") or {}
        enrolled = enrolled_by_campaign.get(cid, 0)
        out.append({
            **_round_bucket(bucket),
            "campaign_name": campaign.get("name"),
            "campaign_status": campaign.get("status"),
            "campaign_type": campaign.get("campaign_type"),
            "created_at": campaign.get("created_at"),
            "account_id": acct_id,
            "account_name": account.get("name"),
            **_unit_costs(bucket["cost_usd"], enrolled, bucket["messages_sent"]),
        })

    return {
        "from": start.isoformat(),
        "to": end.isoformat(),
        "rows": out,
        "unattributed": _round_bucket(unattributed),
        "assumptions": ASSUMPTIONS,
    }


# ---------------------------------------------------------------------------
# GET /api/admin/costs/by-account
# ---------------------------------------------------------------------------

@router.get("/by-account")
async def costs_by_account(
    from_date: Optional[str] = Query(None, alias="from"),
    to_date: Optional[str] = Query(None, alias="to"),
    days: int = Query(30, ge=1, le=365),
    limit: int = Query(50, ge=1, le=200),
    _admin: dict = Depends(get_super_admin),
):
    """Spend per account, with the owning user and unit economics attached."""
    start, end = _parse_range(from_date, to_date, days)
    rows, enrolled_by_account = await asyncio.gather(
        _collect(start, end),
        _enrolled_counts(start, end, group="account"),
    )

    buckets: dict[str, dict] = {}
    unattributed = _blank_bucket()

    for row in rows:
        aid = row["account_id"]
        if not aid:
            _add(unattributed, row)
            continue
        bucket = buckets.setdefault(
            aid, _blank_bucket(account_id=aid, messages_sent=0, campaign_ids=set())
        )
        _add(bucket, row)
        if row["provider"] == "email":
            bucket["messages_sent"] += int(row["units"])
        if row["campaign_id"]:
            bucket["campaign_ids"].add(row["campaign_id"])

    ranked = sorted(buckets.values(), key=lambda b: b["cost_usd"], reverse=True)[:limit]
    account_ids = [b["account_id"] for b in ranked]
    names, owners = await asyncio.gather(
        _account_names(account_ids), _account_owners(account_ids)
    )

    out = []
    for bucket in ranked:
        aid = bucket["account_id"]
        account = names.get(aid) or {}
        owner = owners.get(aid) or {}
        campaign_count = len(bucket.pop("campaign_ids"))
        enrolled = enrolled_by_account.get(aid, 0)
        out.append({
            **_round_bucket(bucket),
            "account_name": account.get("name"),
            "plan": account.get("plan"),
            "status": account.get("status"),
            "owner_name": owner.get("name"),
            "owner_email": owner.get("email"),
            "owner_user_id": owner.get("user_id"),
            "campaigns": campaign_count,
            **_unit_costs(bucket["cost_usd"], enrolled, bucket["messages_sent"]),
        })

    return {
        "from": start.isoformat(),
        "to": end.isoformat(),
        "rows": out,
        "unattributed": _round_bucket(unattributed),
        "assumptions": ASSUMPTIONS,
    }


# ---------------------------------------------------------------------------
# GET /api/admin/costs/by-driver
# ---------------------------------------------------------------------------

@router.get("/by-driver")
async def costs_by_driver(
    from_date: Optional[str] = Query(None, alias="from"),
    to_date: Optional[str] = Query(None, alias="to"),
    days: int = Query(30, ge=1, le=365),
    account_id: Optional[str] = Query(None),
    campaign_id: Optional[str] = Query(None),
    _admin: dict = Depends(get_super_admin),
):
    """Spend per individual cost driver — a specific AI model, Apify actor, or
    GrowthToolkit endpoint. This is the view that says *what* is expensive."""
    start, end = _parse_range(from_date, to_date, days)
    rows = await _collect(start, end, account_id=account_id, campaign_id=campaign_id)

    buckets: dict[tuple, dict] = {}
    for row in rows:
        key = (row["provider"], row["driver"])
        bucket = buckets.setdefault(key, {
            "provider": row["provider"],
            "driver": row["driver"],
            "cost_usd": 0.0,
            "calls": 0,
            "units": 0.0,
        })
        bucket["cost_usd"] += row["cost_usd"]
        bucket["calls"] += row["calls"]
        bucket["units"] += row["units"]

    total = sum(b["cost_usd"] for b in buckets.values())
    out = []
    for b in sorted(buckets.values(), key=lambda b: b["cost_usd"], reverse=True):
        cost = round(b["cost_usd"], 6)
        out.append({
            **b,
            "cost_usd": cost,
            "units": round(b["units"], 2),
            "cost_per_call": round(b["cost_usd"] / b["calls"], 6) if b["calls"] else None,
            "share_pct": round(b["cost_usd"] / total * 100, 1) if total else 0.0,
        })

    return {
        "from": start.isoformat(),
        "to": end.isoformat(),
        "total_cost_usd": round(total, 6),
        "rows": out,
        "assumptions": ASSUMPTIONS,
    }


# ---------------------------------------------------------------------------
# GET /api/admin/costs/campaign/{campaign_id}
# ---------------------------------------------------------------------------

@router.get("/campaign/{campaign_id}")
async def campaign_cost_detail(
    campaign_id: str,
    from_date: Optional[str] = Query(None, alias="from"),
    to_date: Optional[str] = Query(None, alias="to"),
    days: int = Query(90, ge=1, le=365),
    _admin: dict = Depends(get_super_admin),
):
    """Everything spent on one campaign: provider split, daily series, and the
    individual drivers behind it."""
    start, end = _parse_range(from_date, to_date, days)
    meta = await _campaign_meta([campaign_id])
    campaign = meta.get(campaign_id)
    if not campaign:
        raise HTTPException(status_code=404, detail="Campaign not found")

    rows, enrolled_by_campaign = await asyncio.gather(
        _collect(start, end, campaign_id=campaign_id),
        _enrolled_counts(start, end, group="campaign"),
    )

    total = _blank_bucket(messages_sent=0)
    by_day: dict[str, dict] = {}
    drivers: dict[tuple, dict] = {}

    for row in rows:
        _add(total, row)
        if row["provider"] == "email":
            total["messages_sent"] += int(row["units"])
        _add(by_day.setdefault(row["day"], _blank_bucket(day=row["day"])), row)

        key = (row["provider"], row["driver"])
        d = drivers.setdefault(key, {
            "provider": row["provider"], "driver": row["driver"],
            "cost_usd": 0.0, "calls": 0,
        })
        d["cost_usd"] += row["cost_usd"]
        d["calls"] += row["calls"]

    account_id = str(campaign.get("account_id")) if campaign.get("account_id") else None
    names = await _account_names([account_id] if account_id else [])
    messages_sent = total["messages_sent"]

    return {
        "campaign": {
            "_id": campaign_id,
            "name": campaign.get("name"),
            "status": campaign.get("status"),
            "campaign_type": campaign.get("campaign_type"),
            "created_at": campaign.get("created_at"),
            "account_id": account_id,
            "account_name": (names.get(account_id or "") or {}).get("name"),
        },
        "from": start.isoformat(),
        "to": end.isoformat(),
        "total": _round_bucket(total),
        "series": [_round_bucket(b) for b in sorted(by_day.values(), key=lambda b: b["day"])],
        "drivers": [
            {**d, "cost_usd": round(d["cost_usd"], 6)}
            for d in sorted(drivers.values(), key=lambda d: d["cost_usd"], reverse=True)
        ],
        **_unit_costs(total["cost_usd"], enrolled_by_campaign.get(campaign_id, 0), messages_sent),
        "assumptions": ASSUMPTIONS,
    }
