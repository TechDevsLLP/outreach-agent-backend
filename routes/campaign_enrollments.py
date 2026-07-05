"""
Campaign enrollment routes — prospect lifecycle within a campaign sequence.

All routes are prefixed /api/campaigns/{campaign_id}/enrollments.
No router-level prefix is set so that the campaign_id path parameter
is naturally part of every route string.
"""

from datetime import datetime
from typing import Optional

from bson import ObjectId
from fastapi import APIRouter, Depends, HTTPException, Query

from auth import get_account_context
from database import (
    campaign_enrollments_collection,
    campaigns_collection,
    campaign_daily_schedules_collection,
    prospects_collection,
)

router = APIRouter(tags=["Campaign Enrollments"])

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

_PROSPECT_PROJECTION = {
    "_id": 1,
    "full_name": 1,
    "email": 1,
    "company_name": 1,
    "job_title": 1,
    "linkedin_url": 1,
    "priority_tier": 1,
    "enhanced_score": 1,
}


def serialize_enrollment(doc: dict) -> dict:
    """Convert ObjectId fields in an enrollment document to strings."""
    result = dict(doc)
    for field in ("_id", "campaign_id", "account_id", "prospect_id"):
        if field in result and isinstance(result[field], ObjectId):
            result[field] = str(result[field])
    return result


def serialize_prospect_embed(doc: dict) -> dict:
    """Return a slim prospect dict with string _id."""
    return {
        "_id": str(doc["_id"]),
        "full_name": doc.get("full_name"),
        "email": doc.get("email"),
        "company_name": doc.get("company_name"),
        "job_title": doc.get("job_title"),
        "linkedin_url": doc.get("linkedin_url"),
        "priority_tier": doc.get("priority_tier"),
        "enhanced_score": doc.get("enhanced_score"),
    }


async def _get_campaign_or_404(campaign_id: str, account_id: ObjectId) -> dict:
    """Fetch campaign scoped to account_id, raise 404/403 as appropriate."""
    try:
        oid = ObjectId(campaign_id)
    except Exception:
        raise HTTPException(status_code=404, detail="Campaign not found")

    doc = await campaigns_collection.find_one({"_id": oid})
    if doc is None:
        raise HTTPException(status_code=404, detail="Campaign not found")
    if doc.get("account_id") != account_id:
        raise HTTPException(status_code=403, detail="Access denied")
    return doc


async def _get_enrollment_or_404(
    enrollment_id: str, campaign_id: str, account_id: ObjectId
) -> dict:
    """Fetch an enrollment by ID, verify campaign and account ownership."""
    try:
        oid = ObjectId(enrollment_id)
    except Exception:
        raise HTTPException(status_code=404, detail="Enrollment not found")

    doc = await campaign_enrollments_collection.find_one({"_id": oid})
    if doc is None:
        raise HTTPException(status_code=404, detail="Enrollment not found")
    if doc.get("campaign_id") != campaign_id:
        raise HTTPException(status_code=403, detail="Enrollment does not belong to this campaign")
    if doc.get("account_id") != account_id and doc.get("account_id") != str(account_id):
        raise HTTPException(status_code=403, detail="Access denied")
    return doc


# ---------------------------------------------------------------------------
# List enrollments
# ---------------------------------------------------------------------------

@router.get("/api/campaigns/{campaign_id}/enrollments")
async def list_enrollments(
    campaign_id: str,
    status: Optional[str] = None,
    page: int = Query(1, ge=1),
    page_size: int = Query(50, ge=1, le=200),
    account_ctx: dict = Depends(get_account_context),
):
    """List enrollments for a campaign with embedded prospect info."""
    account_id = ObjectId(account_ctx["account"]["_id"])
    await _get_campaign_or_404(campaign_id, account_id)

    query: dict = {"campaign_id": ObjectId(campaign_id)}
    if status:
        query["status"] = status

    skip = (page - 1) * page_size
    total = await campaign_enrollments_collection.count_documents(query)
    cursor = (
        campaign_enrollments_collection.find(query)
        .sort("enrolled_at", -1)
        .skip(skip)
        .limit(page_size)
    )
    enrollments = await cursor.to_list(page_size)

    # Collect unique prospect IDs for a batch lookup
    prospect_id_strs = list({e.get("prospect_id") for e in enrollments if e.get("prospect_id")})
    prospect_oids = []
    for pid in prospect_id_strs:
        try:
            prospect_oids.append(ObjectId(pid))
        except Exception:
            pass

    prospect_map: dict = {}
    if prospect_oids:
        p_cursor = prospects_collection.find(
            {"_id": {"$in": prospect_oids}}, _PROSPECT_PROJECTION
        )
        for p in await p_cursor.to_list(len(prospect_oids)):
            prospect_map[str(p["_id"])] = serialize_prospect_embed(p)

    result = []
    for e in enrollments:
        serialized = serialize_enrollment(e)
        serialized["prospect"] = prospect_map.get(serialized.get("prospect_id"))
        result.append(serialized)

    return {"enrollments": result, "total": total, "page": page, "page_size": page_size}


# ---------------------------------------------------------------------------
# Status counts
# ---------------------------------------------------------------------------

@router.get("/api/campaigns/{campaign_id}/enrollments/status-counts")
async def enrollment_status_counts(
    campaign_id: str,
    account_ctx: dict = Depends(get_account_context),
):
    """Return enrollment counts grouped by status for a campaign."""
    account_id = ObjectId(account_ctx["account"]["_id"])
    await _get_campaign_or_404(campaign_id, account_id)

    pipeline = [
        {"$match": {"campaign_id": ObjectId(campaign_id)}},
        {"$group": {"_id": "$status", "count": {"$sum": 1}}},
    ]
    rows = await campaign_enrollments_collection.aggregate(pipeline).to_list(50)

    # Build a default dict so all known statuses are always present
    counts: dict = {
        "enrolled": 0,
        "active": 0,
        "paused": 0,
        "completed": 0,
        "replied": 0,
        "bounced": 0,
        "opted_out": 0,
    }
    for row in rows:
        status_key = row["_id"]
        if status_key:
            counts[status_key] = row["count"]

    return counts


# ---------------------------------------------------------------------------
# Bulk enroll
# ---------------------------------------------------------------------------

@router.post("/api/campaigns/{campaign_id}/enrollments/bulk")
async def bulk_enroll(
    campaign_id: str,
    body: dict,
    account_ctx: dict = Depends(get_account_context),
):
    """Bulk-enroll prospects into a campaign, skipping duplicates.

    Body: {prospect_ids: list[str], start_immediately: bool = True}
    """
    account_id = ObjectId(account_ctx["account"]["_id"])
    await _get_campaign_or_404(campaign_id, account_id)

    prospect_ids: list[str] = body.get("prospect_ids", [])
    start_immediately: bool = body.get("start_immediately", True)

    if not prospect_ids:
        raise HTTPException(status_code=400, detail="prospect_ids must not be empty")

    now = datetime.utcnow()
    enrolled_docs = []
    skipped_duplicates = 0

    for pid in prospect_ids:
        try:
            pid_oid = ObjectId(pid)
        except Exception:
            continue
            
        # Check for existing enrollment in this campaign
        existing = await campaign_enrollments_collection.find_one(
            {"campaign_id": ObjectId(campaign_id), "prospect_id": pid_oid}
        )
        if existing:
            skipped_duplicates += 1
            continue

        enrollment_status = "active" if start_immediately else "enrolled"
        doc = {
            "campaign_id": ObjectId(campaign_id),
            "account_id": account_id,
            "prospect_id": pid_oid,
            "status": enrollment_status,
            "current_step": 0,
            "next_action_at": now if start_immediately else None,
            "step_history": [],
            "enrolled_at": now,
            "completed_at": None,
            "last_activity_at": None,
        }
        result = await campaign_enrollments_collection.insert_one(doc)
        doc["_id"] = result.inserted_id
        enrolled_docs.append(doc)

    # Atomically increment total_enrolled on the campaign
    if enrolled_docs:
        await campaigns_collection.update_one(
            {"_id": ObjectId(campaign_id)},
            {
                "$inc": {"total_enrolled": len(enrolled_docs)},
                "$set": {"updated_at": now},
            },
        )

    return {
        "enrolled": len(enrolled_docs),
        "skipped_duplicates": skipped_duplicates,
        "enrollments": [serialize_enrollment(e) for e in enrolled_docs],
    }


# ---------------------------------------------------------------------------
# Add single prospect (respects active-campaign schedule)
# ---------------------------------------------------------------------------

@router.post("/api/campaigns/{campaign_id}/enrollments/add-prospect")
async def add_single_prospect(
    campaign_id: str,
    body: dict,
    account_ctx: dict = Depends(get_account_context),
):
    """Add one prospect to a campaign.

    - Active campaign: enroll onto the last existing scheduled day
      (next_action_at = that day's date). The campaign engine picks it up.
    - Otherwise (draft/paused/completed): enroll as not-yet-active
      (next_action_at=None), same semantics as bulkEnroll(start_immediately=False).
    """
    account_id = ObjectId(account_ctx["account"]["_id"])
    campaign = await _get_campaign_or_404(campaign_id, account_id)

    prospect_id = body.get("prospect_id")
    if not prospect_id:
        raise HTTPException(status_code=400, detail="prospect_id is required")

    try:
        pid_oid = ObjectId(prospect_id)
    except Exception:
        raise HTTPException(status_code=400, detail="Invalid prospect_id")

    # Duplicate check
    existing = await campaign_enrollments_collection.find_one(
        {"campaign_id": ObjectId(campaign_id), "prospect_id": pid_oid}
    )
    if existing:
        return {
            "enrolled": 0,
            "already_enrolled": True,
            "enrollment": serialize_enrollment(existing),
        }

    now = datetime.utcnow()
    campaign_status = campaign.get("status")
    is_active = campaign_status == "active"

    next_action_at = None
    enrollment_status = "enrolled"
    scheduled_for = None

    if is_active:
        last_day = await campaign_daily_schedules_collection.find_one(
            {"campaign_id": ObjectId(campaign_id)},
            sort=[("schedule_date", -1)],
        )
        if last_day and last_day.get("schedule_date"):
            scheduled_for = last_day["schedule_date"]
            # schedule_date is an ISO string (e.g. "2026-04-21"); engine treats
            # a UTC midnight of that date as the trigger.
            try:
                next_action_at = datetime.fromisoformat(str(scheduled_for))
            except Exception:
                next_action_at = now
            enrollment_status = "active"
        else:
            next_action_at = now
            enrollment_status = "active"

    doc = {
        "campaign_id": ObjectId(campaign_id),
        "account_id": account_id,
        "prospect_id": pid_oid,
        "status": enrollment_status,
        "current_step": 0,
        "next_action_at": next_action_at,
        "step_history": [],
        "enrolled_at": now,
        "completed_at": None,
        "last_activity_at": None,
    }
    result = await campaign_enrollments_collection.insert_one(doc)
    doc["_id"] = result.inserted_id

    await campaigns_collection.update_one(
        {"_id": ObjectId(campaign_id)},
        {"$inc": {"total_enrolled": 1}, "$set": {"updated_at": now}},
    )

    return {
        "enrolled": 1,
        "already_enrolled": False,
        "campaign_status": campaign_status,
        "scheduled_for": str(scheduled_for) if scheduled_for else None,
        "enrollment": serialize_enrollment(doc),
    }


# ---------------------------------------------------------------------------
# Single enrollment detail
# ---------------------------------------------------------------------------

@router.get("/api/campaigns/{campaign_id}/enrollments/{enrollment_id}")
async def get_enrollment(
    campaign_id: str,
    enrollment_id: str,
    account_ctx: dict = Depends(get_account_context),
):
    """Get a single enrollment with step history and embedded prospect."""
    account_id = ObjectId(account_ctx["account"]["_id"])
    await _get_campaign_or_404(campaign_id, account_id)
    doc = await _get_enrollment_or_404(enrollment_id, campaign_id, account_id)

    serialized = serialize_enrollment(doc)
    step_history = serialized.pop("step_history", [])

    # Fetch prospect
    prospect = None
    pid = doc.get("prospect_id")
    if pid:
        try:
            p_doc = await prospects_collection.find_one(
                {"_id": ObjectId(pid)}, _PROSPECT_PROJECTION
            )
            if p_doc:
                prospect = serialize_prospect_embed(p_doc)
        except Exception:
            pass

    return {
        "enrollment": serialized,
        "step_history": step_history,
        "prospect": prospect,
    }


# ---------------------------------------------------------------------------
# Enrollment status transitions
# ---------------------------------------------------------------------------

@router.post("/api/campaigns/{campaign_id}/enrollments/{enrollment_id}/pause")
async def pause_enrollment(
    campaign_id: str,
    enrollment_id: str,
    account_ctx: dict = Depends(get_account_context),
):
    """Pause a single enrollment."""
    account_id = ObjectId(account_ctx["account"]["_id"])
    await _get_campaign_or_404(campaign_id, account_id)
    await _get_enrollment_or_404(enrollment_id, campaign_id, account_id)

    oid = ObjectId(enrollment_id)
    await campaign_enrollments_collection.update_one(
        {"_id": oid},
        {"$set": {"status": "paused", "last_activity_at": datetime.utcnow()}},
    )
    updated = await campaign_enrollments_collection.find_one({"_id": oid})
    return {"enrollment": serialize_enrollment(updated)}


@router.post("/api/campaigns/{campaign_id}/enrollments/{enrollment_id}/resume")
async def resume_enrollment(
    campaign_id: str,
    enrollment_id: str,
    account_ctx: dict = Depends(get_account_context),
):
    """Resume a paused enrollment, scheduling next action immediately."""
    account_id = ObjectId(account_ctx["account"]["_id"])
    await _get_campaign_or_404(campaign_id, account_id)
    await _get_enrollment_or_404(enrollment_id, campaign_id, account_id)

    now = datetime.utcnow()
    oid = ObjectId(enrollment_id)
    await campaign_enrollments_collection.update_one(
        {"_id": oid},
        {"$set": {"status": "active", "next_action_at": now, "last_activity_at": now}},
    )
    updated = await campaign_enrollments_collection.find_one({"_id": oid})
    return {"enrollment": serialize_enrollment(updated)}


# ---------------------------------------------------------------------------
# Unenroll (soft delete → opted_out)
# ---------------------------------------------------------------------------

@router.delete("/api/campaigns/{campaign_id}/enrollments/{enrollment_id}")
async def unenroll(
    campaign_id: str,
    enrollment_id: str,
    account_ctx: dict = Depends(get_account_context),
):
    """Opt a prospect out of the campaign and decrement active_count."""
    account_id = ObjectId(account_ctx["account"]["_id"])
    await _get_campaign_or_404(campaign_id, account_id)
    doc = await _get_enrollment_or_404(enrollment_id, campaign_id, account_id)

    now = datetime.utcnow()
    oid = ObjectId(enrollment_id)
    await campaign_enrollments_collection.update_one(
        {"_id": oid},
        {"$set": {"status": "opted_out", "last_activity_at": now}},
    )

    # Only decrement active_count if the enrollment was previously active
    if doc.get("status") == "active":
        await campaigns_collection.update_one(
            {"_id": ObjectId(campaign_id)},
            {
                "$inc": {"active_count": -1, "opted_out_count": 1},
                "$set": {"updated_at": now},
            },
        )
    else:
        await campaigns_collection.update_one(
            {"_id": ObjectId(campaign_id)},
            {
                "$inc": {"opted_out_count": 1},
                "$set": {"updated_at": now},
            },
        )

    return {"message": "Unenrolled"}


# ---------------------------------------------------------------------------
# Company cascade enrollment
# ---------------------------------------------------------------------------

import uuid as _uuid

# Seniority rank for cascade ordering (lower = higher priority)
_SENIORITY_RANK = {
    "c-suite": 0, "cxo": 0, "founder": 0, "owner": 0, "partner": 0,
    "vp": 1, "vice president": 1,
    "director": 2,
    "head": 3,
    "manager": 4,
    "senior": 5,
}

def _seniority_score(job_title: str) -> int:
    """Lower number = higher seniority = enrolled first."""
    if not job_title:
        return 99
    title_lower = job_title.lower()
    for keyword, rank in _SENIORITY_RANK.items():
        if keyword in title_lower:
            return rank
    return 10


@router.post("/api/campaigns/{campaign_id}/enrollments/company-cascade")
async def company_cascade_enroll(
    campaign_id: str,
    body: dict,
    account_ctx: dict = Depends(get_account_context),
):
    """
    Enroll prospects grouped by company with cascade logic.

    Body:
    {
      "company_groups": [
        {
          "company_name": "Acme Corp",
          "prospect_ids": ["id1", "id2", "id3"],
          "cascade_delay_days": 5   // days after primary enrollment to activate next if no reply
        }
      ],
      "start_immediately": true
    }

    For each company group:
    - Prospects are sorted by seniority (CEO/Founder first)
    - Position 0 → enrolled as "active" (primary)
    - Positions 1+ → enrolled as "cascade_waiting" with cascade metadata
    The campaign engine will activate cascade prospects when the primary has no reply
    after cascade_delay_days.
    """
    account_id = ObjectId(account_ctx["account"]["_id"])
    await _get_campaign_or_404(campaign_id, account_id)

    company_groups: list = body.get("company_groups", [])
    start_immediately: bool = body.get("start_immediately", True)

    if not company_groups:
        raise HTTPException(status_code=400, detail="company_groups must not be empty")

    now = datetime.utcnow()
    total_enrolled = 0
    total_cascade = 0
    total_skipped = 0
    all_enrolled = []

    for group in company_groups:
        prospect_ids: list[str] = group.get("prospect_ids", [])
        cascade_delay_days: int = group.get("cascade_delay_days", 5)

        if not prospect_ids:
            continue

        # Fetch prospects to sort by seniority
        prospect_oids = []
        for pid in prospect_ids:
            try:
                prospect_oids.append(ObjectId(pid))
            except Exception:
                pass

        fetched = await prospects_collection.find(
            {"_id": {"$in": prospect_oids}},
            {"_id": 1, "job_title": 1, "enhanced_score": 1, "seniority_level": 1},
        ).to_list(len(prospect_oids))

        # Sort: first by seniority rank, then by enhanced_score desc
        fetched.sort(key=lambda p: (
            _seniority_score(p.get("job_title") or p.get("seniority_level") or ""),
            -(p.get("enhanced_score") or 0),
        ))

        group_id = str(_uuid.uuid4())
        cascade_from = now

        for position, p_doc in enumerate(fetched):
            pid_oid = p_doc["_id"]

            # Skip already enrolled
            existing = await campaign_enrollments_collection.find_one(
                {"campaign_id": ObjectId(campaign_id), "prospect_id": pid_oid}
            )
            if existing:
                total_skipped += 1
                continue

            is_primary = position == 0
            # cascade prospects activate after primary's cascade_delay_days
            cascade_activate_at = None
            if not is_primary:
                # Each subsequent prospect gets an extra delay multiplier
                from datetime import timedelta
                cascade_activate_at = now + timedelta(days=cascade_delay_days * position)

            doc = {
                "campaign_id": ObjectId(campaign_id),
                "account_id": account_id,
                "prospect_id": pid_oid,
                "status": "active" if (is_primary and start_immediately) else ("cascade_waiting" if not is_primary else "enrolled"),
                "current_step": 0,
                "next_action_at": now if (is_primary and start_immediately) else None,
                "step_history": [],
                "enrolled_at": now,
                "completed_at": None,
                "last_activity_at": None,
                # Cascade metadata
                "cascade_group_id": group_id,
                "cascade_position": position,
                "cascade_delay_days": cascade_delay_days,
                "cascade_activate_at": cascade_activate_at,
                "cascade_status": "primary" if is_primary else "waiting",
            }

            result = await campaign_enrollments_collection.insert_one(doc)
            doc["_id"] = result.inserted_id
            all_enrolled.append(doc)

            if is_primary:
                total_enrolled += 1
            else:
                total_cascade += 1

    # Update campaign counter
    new_count = total_enrolled + total_cascade
    if new_count > 0:
        await campaigns_collection.update_one(
            {"_id": ObjectId(campaign_id)},
            {
                "$inc": {"total_enrolled": new_count},
                "$set": {"updated_at": now},
            },
        )

    return {
        "enrolled_primary": total_enrolled,
        "enrolled_cascade": total_cascade,
        "skipped_duplicates": total_skipped,
        "total": new_count,
    }
