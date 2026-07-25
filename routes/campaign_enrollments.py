"""
Campaign enrollment routes — prospect lifecycle within a campaign sequence.

All routes are prefixed /api/campaigns/{campaign_id}/enrollments.
No router-level prefix is set so that the campaign_id path parameter
is naturally part of every route string.
"""

import logging
from datetime import datetime
from typing import Optional

from bson import ObjectId
from fastapi import APIRouter, BackgroundTasks, Depends, HTTPException, Query

from auth import get_account_context
from database import (
    campaign_enrollments_collection,
    campaigns_collection,
    campaign_daily_schedules_collection,
    prospect_state_collection,
    prospects_collection,
)

logger = logging.getLogger(__name__)

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
    """Fetch campaign scoped to account_id, raise 404 if missing.
    Cross-tenant access also returns 404 (no existence leak)."""
    try:
        oid = ObjectId(campaign_id)
    except Exception:
        raise HTTPException(status_code=404, detail="Campaign not found")

    doc = await campaigns_collection.find_one(
        {
            "_id": oid,
            "account_id": {"$in": [str(account_id), account_id]},
        }
    )
    if doc is None:
        raise HTTPException(status_code=404, detail="Campaign not found")
    return doc


def _tenant_filter(account_id: ObjectId) -> dict:
    return {"$in": [str(account_id), account_id]}


def _campaign_key_filter(campaign_id: str) -> dict:
    return {"$in": [campaign_id, ObjectId(campaign_id)]}


def _enrollment_scope_filter(
    enrollment_id: str, campaign_id: str, account_id: ObjectId
) -> dict:
    return {
        "_id": ObjectId(enrollment_id),
        "campaign_id": _campaign_key_filter(campaign_id),
        "account_id": _tenant_filter(account_id),
    }


async def _get_accessible_prospects_or_404(
    prospect_ids: list[str], account_id: ObjectId
) -> dict[str, dict]:
    """Resolve prospects only when every requested ID belongs to the account.

    Access comes from the tenant overlay, never from knowledge that a shared
    prospect ID exists.  The all-or-nothing check prevents partial enrollment
    and avoids revealing which IDs belong to another tenant.
    """
    if not isinstance(prospect_ids, list) or not prospect_ids:
        raise HTTPException(status_code=400, detail="prospect_ids must not be empty")
    if len(prospect_ids) > 1000:
        raise HTTPException(status_code=400, detail="Maximum 1000 prospects per request")

    ordered_ids: list[str] = []
    object_ids: list[ObjectId] = []
    seen: set[str] = set()
    for raw_id in prospect_ids:
        try:
            oid = ObjectId(str(raw_id))
        except Exception:
            raise HTTPException(status_code=400, detail="Invalid prospect_id")
        key = str(oid)
        if key not in seen:
            seen.add(key)
            ordered_ids.append(key)
            object_ids.append(oid)

    overlay_keys: list[object] = [*ordered_ids, *object_ids]
    state_rows = await prospect_state_collection.find(
        {
            "account_id": _tenant_filter(account_id),
            "prospect_id": {"$in": overlay_keys},
        },
        {"prospect_id": 1},
    ).to_list(len(ordered_ids))
    accessible_ids = {str(row.get("prospect_id")) for row in state_rows}
    if accessible_ids != set(ordered_ids):
        raise HTTPException(status_code=404, detail="One or more prospects were not found")

    prospect_rows = await prospects_collection.find(
        {"_id": {"$in": object_ids}}
    ).to_list(len(object_ids))
    prospect_map = {str(row["_id"]): row for row in prospect_rows}
    if set(prospect_map) != set(ordered_ids):
        raise HTTPException(status_code=404, detail="One or more prospects were not found")
    return prospect_map


async def _get_enrollment_or_404(
    enrollment_id: str, campaign_id: str, account_id: ObjectId
) -> dict:
    """Fetch an enrollment by ID, verify campaign and account ownership."""
    try:
        oid = ObjectId(enrollment_id)
    except Exception:
        raise HTTPException(status_code=404, detail="Enrollment not found")

    doc = await campaign_enrollments_collection.find_one(
        {
            "_id": oid,
            "campaign_id": _campaign_key_filter(campaign_id),
            "account_id": _tenant_filter(account_id),
        }
    )
    if doc is None:
        raise HTTPException(status_code=404, detail="Enrollment not found")
    return doc


def _smart_enrollment_fields(campaign: dict) -> dict:
    """Extra fields a new enrollment into an is_smart_campaign needs so the
    campaign engine (services/campaign_engine.py execute_enrollment) routes it
    to the sequence/smart-channel path instead of falling through to the
    classic per-step path — smart campaigns carry no `steps`, so that path
    marks the enrollment completed with zero sends.

    Mirrors the shape services/cascade_rotation_service.activate_next_backup
    sets for a freshly-activated cascade backup (same underlying contract).
    """
    fields: dict = {
        "smart_campaign_channel": None,
        "smart_campaign_send_day": None,
        "smart_campaign_scheduled_utc": None,
        "generated_messages": None,
        "message_gen_status": "pending",
        "message_gen_error": None,
    }
    graph = campaign.get("sequence_graph") or {}
    if graph:
        from services import sequence_service as seq
        start_node_id = seq.get_start_node_id(graph)
        fields["sequence_state"] = seq.build_initial_sequence_state(graph, start_node_id=start_node_id)
        start_node = seq.get_node(graph, start_node_id) or {}
        fields["smart_campaign_channel"] = seq.engine_channel(start_node.get("channel"))
    return fields


async def _generate_first_touch_and_activate(enrollment_id: ObjectId) -> None:
    """Background task: generate the first-touch message for a freshly
    API-enrolled smart-campaign prospect, then set next_action_at so the
    campaign engine picks it up.

    Deliberately left inert (next_action_at stays None) on failure rather than
    risk the engine sending a blank message (sequence start nodes don't lazily
    generate — see campaign_engine._execute_sequence_enrollment) or falling
    through to the classic per-step path.
    """
    try:
        enrollment = await campaign_enrollments_collection.find_one({"_id": enrollment_id})
        if not enrollment or enrollment.get("status") != "active" or enrollment.get("next_action_at"):
            return
        campaign = await campaigns_collection.find_one({"_id": enrollment["campaign_id"]})
        prospect = await prospects_collection.find_one({"_id": enrollment["prospect_id"]})
        if not campaign or not prospect:
            return

        from services.campaign_message_generator_service import generate_messages_for_enrollment
        from services.openrouter_service import OpenRouterClient
        from services.campaign_launch_service import clamp_to_send_window

        client = OpenRouterClient()
        try:
            await generate_messages_for_enrollment(enrollment, prospect, campaign, client)
        finally:
            await client.close()

        now = datetime.utcnow()
        next_action_at = clamp_to_send_window(now, campaign)
        update: dict = {"next_action_at": next_action_at, "last_activity_at": now}
        if enrollment.get("sequence_state"):
            update["sequence_state.next_action_at"] = next_action_at
            update["sequence_state.phase"] = "pending_send"
        await campaign_enrollments_collection.update_one(
            {"_id": enrollment_id},
            {"$set": update},
        )
    except Exception as e:
        logger.warning(f"Smart-campaign first-touch generation failed for enrollment {enrollment_id}: {e}")


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

    query: dict = {
        "campaign_id": _campaign_key_filter(campaign_id),
        "account_id": _tenant_filter(account_id),
    }
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
        {
            "$match": {
                "campaign_id": _campaign_key_filter(campaign_id),
                "account_id": _tenant_filter(account_id),
            }
        },
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
    background_tasks: BackgroundTasks,
    account_ctx: dict = Depends(get_account_context),
):
    """Bulk-enroll prospects into a campaign, skipping duplicates.

    Body: {prospect_ids: list[str], start_immediately: bool = True}
    """
    account_id = ObjectId(account_ctx["account"]["_id"])
    campaign = await _get_campaign_or_404(campaign_id, account_id)
    campaign_oid = ObjectId(campaign_id)

    prospect_ids: list[str] = body.get("prospect_ids", [])
    start_immediately: bool = body.get("start_immediately", True)

    prospect_map = await _get_accessible_prospects_or_404(prospect_ids, account_id)

    now = datetime.utcnow()
    candidate_oids = [ObjectId(pid) for pid in prospect_map]

    # Single $in dup-check across every candidate instead of one find_one per
    # prospect.
    existing_cursor = campaign_enrollments_collection.find(
        {
            "campaign_id": _campaign_key_filter(campaign_id),
            "account_id": _tenant_filter(account_id),
            "prospect_id": {"$in": [*candidate_oids, *[str(o) for o in candidate_oids]]},
        },
        {"prospect_id": 1},
    )
    existing_pids = {str(doc["prospect_id"]) async for doc in existing_cursor}

    enrollment_status = "active" if start_immediately else "enrolled"
    is_smart = bool(campaign.get("is_smart_campaign"))
    # Shape fields are set regardless of start_immediately so a not-yet-active
    # enrollment doesn't reproduce the classic-path zero-sends bug if it's
    # activated later; only an immediate start also triggers scheduling.
    smart_fields = _smart_enrollment_fields(campaign) if is_smart else None

    docs = []
    for pid in prospect_map:
        if pid in existing_pids:
            continue
        doc = {
            "campaign_id": campaign_oid,
            "account_id": str(account_id),
            "prospect_id": ObjectId(pid),
            "status": enrollment_status,
            "current_step": 0,
            # Smart-campaign first touches wait on message generation before
            # next_action_at is set (see _generate_first_touch_and_activate);
            # classic campaigns keep sending immediately off their templates.
            "next_action_at": (now if (start_immediately and not smart_fields) else None),
            "step_history": [],
            "enrolled_at": now,
            "completed_at": None,
            "last_activity_at": None,
        }
        if smart_fields is not None:
            doc.update(smart_fields)
        docs.append(doc)

    skipped_duplicates = len(prospect_map) - len(docs)

    enrolled_docs = []
    if docs:
        result = await campaign_enrollments_collection.insert_many(docs, ordered=False)
        for doc, inserted_id in zip(docs, result.inserted_ids):
            doc["_id"] = inserted_id
        enrolled_docs = docs

    if enrolled_docs:
        active_count_delta = sum(1 for d in enrolled_docs if d["status"] == "active")
        inc_fields: dict = {"total_enrolled": len(enrolled_docs)}
        if active_count_delta:
            inc_fields["active_count"] = active_count_delta
        await campaigns_collection.update_one(
            {"_id": campaign_oid, "account_id": _tenant_filter(account_id)},
            {
                "$inc": inc_fields,
                "$set": {"updated_at": now},
            },
        )
        if smart_fields is not None:
            for d in enrolled_docs:
                if d["status"] == "active":
                    background_tasks.add_task(_generate_first_touch_and_activate, d["_id"])

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
    background_tasks: BackgroundTasks,
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

    await _get_accessible_prospects_or_404([str(pid_oid)], account_id)

    # Duplicate check
    existing = await campaign_enrollments_collection.find_one(
        {
            "campaign_id": _campaign_key_filter(campaign_id),
            "account_id": _tenant_filter(account_id),
            "prospect_id": {"$in": [str(pid_oid), pid_oid]},
        }
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

    is_smart = bool(campaign.get("is_smart_campaign"))
    smart_fields = None
    if is_smart and enrollment_status == "active":
        smart_fields = _smart_enrollment_fields(campaign)
        # A smart-campaign first touch waits on message generation before
        # next_action_at is set (see _generate_first_touch_and_activate) —
        # the classic "last scheduled day" timing below doesn't apply.
        next_action_at = None

    doc = {
        "campaign_id": ObjectId(campaign_id),
        "account_id": str(account_id),
        "prospect_id": pid_oid,
        "status": enrollment_status,
        "current_step": 0,
        "next_action_at": next_action_at,
        "step_history": [],
        "enrolled_at": now,
        "completed_at": None,
        "last_activity_at": None,
    }
    if smart_fields is not None:
        doc.update(smart_fields)
    result = await campaign_enrollments_collection.insert_one(doc)
    doc["_id"] = result.inserted_id

    inc_fields: dict = {"total_enrolled": 1}
    if enrollment_status == "active":
        inc_fields["active_count"] = 1
    await campaigns_collection.update_one(
        {"_id": ObjectId(campaign_id), "account_id": _tenant_filter(account_id)},
        {"$inc": inc_fields, "$set": {"updated_at": now}},
    )
    if smart_fields is not None:
        background_tasks.add_task(_generate_first_touch_and_activate, doc["_id"])

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

    await campaign_enrollments_collection.update_one(
        _enrollment_scope_filter(enrollment_id, campaign_id, account_id),
        {"$set": {"status": "paused", "last_activity_at": datetime.utcnow()}},
    )
    updated = await campaign_enrollments_collection.find_one(
        _enrollment_scope_filter(enrollment_id, campaign_id, account_id)
    )
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
    await campaign_enrollments_collection.update_one(
        _enrollment_scope_filter(enrollment_id, campaign_id, account_id),
        {"$set": {"status": "active", "next_action_at": now, "last_activity_at": now}},
    )
    updated = await campaign_enrollments_collection.find_one(
        _enrollment_scope_filter(enrollment_id, campaign_id, account_id)
    )
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
    await campaign_enrollments_collection.update_one(
        _enrollment_scope_filter(enrollment_id, campaign_id, account_id),
        {"$set": {"status": "opted_out", "last_activity_at": now}},
    )

    # Only decrement active_count if the enrollment was previously active
    if doc.get("status") == "active":
        await campaigns_collection.update_one(
            {"_id": ObjectId(campaign_id), "account_id": _tenant_filter(account_id)},
            {
                "$inc": {"active_count": -1, "opted_out_count": 1},
                "$set": {"updated_at": now},
            },
        )
    else:
        await campaigns_collection.update_one(
            {"_id": ObjectId(campaign_id), "account_id": _tenant_filter(account_id)},
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
    background_tasks: BackgroundTasks,
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
          "cascade_delay_days": 5   // days after primary enrollment to activate next if no reply;
                                    // null → event-driven (backup activates only when the
                                    // primary's sequence ends "not_replied")
        }
      ],
      "start_immediately": true
    }

    For each company group:
    - Prospects are sorted by seniority (CEO/Founder first)
    - Position 0 → enrolled as "active" (primary)
    - Positions 1+ → enrolled as "cascade_waiting" with cascade metadata
    With cascade_delay_days set, the engine activates backups time-based after
    that many days without a primary reply. With cascade_delay_days null the
    backups are event-driven: they wait (cascade_activate_at = None) until the
    primary finishes its sequence with status "not_replied".
    """
    account_id = ObjectId(account_ctx["account"]["_id"])
    campaign = await _get_campaign_or_404(campaign_id, account_id)
    campaign_oid = ObjectId(campaign_id)

    company_groups: list = body.get("company_groups", [])
    start_immediately: bool = body.get("start_immediately", True)

    if not company_groups:
        raise HTTPException(status_code=400, detail="company_groups must not be empty")

    now = datetime.utcnow()
    is_smart = bool(campaign.get("is_smart_campaign"))

    # Validate + fetch every group's prospects up front so the dup-check below
    # can run as a single $in query across all groups instead of one
    # find_one per prospect.
    groups_fetched: list[tuple[str, int | None, list[dict]]] = []
    all_candidate_oids: list[ObjectId] = []
    for group in company_groups:
        prospect_ids: list[str] = group.get("prospect_ids", [])
        # None → event-driven cascade (no time-based activation).
        cascade_delay_days: int | None = group.get("cascade_delay_days", 5)
        if not prospect_ids:
            continue

        prospect_oids = []
        for pid in prospect_ids:
            try:
                prospect_oids.append(ObjectId(pid))
            except Exception:
                raise HTTPException(status_code=400, detail="Invalid prospect_id")

        accessible = await _get_accessible_prospects_or_404(
            [str(oid) for oid in prospect_oids], account_id
        )

        fetched = await prospects_collection.find(
            {"_id": {"$in": [ObjectId(pid) for pid in accessible]}},
            {"_id": 1, "job_title": 1, "enhanced_score": 1, "seniority_level": 1},
        ).to_list(len(prospect_oids))

        # Sort: first by seniority rank, then by enhanced_score desc
        fetched.sort(key=lambda p: (
            _seniority_score(p.get("job_title") or p.get("seniority_level") or ""),
            -(p.get("enhanced_score") or 0),
        ))

        groups_fetched.append((str(_uuid.uuid4()), cascade_delay_days, fetched))
        all_candidate_oids.extend(p_doc["_id"] for p_doc in fetched)

    existing_pids: set[str] = set()
    if all_candidate_oids:
        existing_cursor = campaign_enrollments_collection.find(
            {
                "campaign_id": _campaign_key_filter(campaign_id),
                "account_id": _tenant_filter(account_id),
                "prospect_id": {
                    "$in": [*all_candidate_oids, *[str(o) for o in all_candidate_oids]]
                },
            },
            {"prospect_id": 1},
        )
        existing_pids = {str(doc["prospect_id"]) async for doc in existing_cursor}

    total_enrolled = 0
    total_cascade = 0
    total_skipped = 0
    total_activated = 0
    all_enrolled: list[dict] = []
    docs: list[dict] = []

    for group_id, cascade_delay_days, fetched in groups_fetched:
        for position, p_doc in enumerate(fetched):
            pid_oid = p_doc["_id"]

            if str(pid_oid) in existing_pids:
                total_skipped += 1
                continue

            is_primary = position == 0
            # cascade prospects activate after primary's cascade_delay_days;
            # cascade_delay_days=None keeps cascade_activate_at=None → the
            # backup is event-driven (woken when the primary ends not_replied).
            cascade_activate_at = None
            if not is_primary and cascade_delay_days is not None:
                # Each subsequent prospect gets an extra delay multiplier
                from datetime import timedelta
                cascade_activate_at = now + timedelta(days=cascade_delay_days * position)

            is_active_primary = is_primary and start_immediately
            doc = {
                "campaign_id": campaign_oid,
                "account_id": str(account_id),
                "prospect_id": pid_oid,
                "status": "active" if is_active_primary else ("cascade_waiting" if not is_primary else "enrolled"),
                "current_step": 0,
                # Smart-campaign primaries wait on message generation before
                # next_action_at is set (see _generate_first_touch_and_activate).
                "next_action_at": (now if (is_active_primary and not is_smart) else None),
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
            # Shape fields are set for any primary regardless of
            # start_immediately so a not-yet-active primary doesn't reproduce
            # the classic-path zero-sends bug if it's activated later; only an
            # immediate start also triggers scheduling (below).
            if is_primary and is_smart:
                doc.update(_smart_enrollment_fields(campaign))

            docs.append(doc)
            if is_primary:
                total_enrolled += 1
                if is_active_primary:
                    total_activated += 1
            else:
                total_cascade += 1

    if docs:
        result = await campaign_enrollments_collection.insert_many(docs, ordered=False)
        for doc, inserted_id in zip(docs, result.inserted_ids):
            doc["_id"] = inserted_id
        all_enrolled = docs

    # Update campaign counters — cascade_waiting backups are NOT counted as
    # enrolled/active until they are actually activated (cascade_rotation_service
    # increments both total_enrolled and active_count at that point).
    new_count = total_enrolled + total_cascade
    if total_enrolled > 0:
        inc_fields: dict = {"total_enrolled": total_enrolled}
        if total_activated:
            inc_fields["active_count"] = total_activated
        await campaigns_collection.update_one(
            {"_id": campaign_oid, "account_id": _tenant_filter(account_id)},
            {
                "$inc": inc_fields,
                "$set": {"updated_at": now},
            },
        )
        if is_smart:
            for doc in all_enrolled:
                if doc.get("cascade_position") == 0 and doc["status"] == "active":
                    background_tasks.add_task(_generate_first_touch_and_activate, doc["_id"])

    return {
        "enrolled_primary": total_enrolled,
        "enrolled_cascade": total_cascade,
        "skipped_duplicates": total_skipped,
        "total": new_count,
    }
