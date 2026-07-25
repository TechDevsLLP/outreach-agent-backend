"""
Campaign schedule routes — draft, review, edit, approve, and cancel daily outreach schedules.

All routes are scoped to /api/campaigns/{campaign_id}/schedules/...
and require an authenticated account context.
"""

import re
import logging
from datetime import datetime, date
from typing import Optional

from bson import ObjectId
from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel

from auth import get_account_context
import database

logger = logging.getLogger(__name__)

router = APIRouter(tags=["Campaign Schedules"])


# ---------------------------------------------------------------------------
# Request bodies
# ---------------------------------------------------------------------------

class GenerateScheduleRequest(BaseModel):
    schedule_date: Optional[str] = None   # YYYY-MM-DD, defaults to today
    force_regenerate: bool = False


class EditItemRequest(BaseModel):
    subject: Optional[str] = None
    body: Optional[str] = None


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def personalize_simple(template: str, prospect: dict) -> str:
    """Replace {{variable}} placeholders with prospect field values."""
    vars = {
        "firstName": prospect.get("first_name", ""),
        "lastName": prospect.get("last_name", ""),
        "fullName": prospect.get("full_name", ""),
        "companyName": prospect.get("company_name", ""),
        "jobTitle": prospect.get("job_title", ""),
        "industry": prospect.get("industry", ""),
    }
    return re.sub(
        r'\{\{(\w+)\}\}',
        lambda m: vars.get(m.group(1), f'{{{{{m.group(1)}}}}}'),
        template,
    )


def _today_str() -> str:
    return date.today().isoformat()


def _serialize_oids(doc: dict) -> dict:
    """Recursively stringify any ObjectId values in a dict."""
    result = {}
    for k, v in doc.items():
        if isinstance(v, ObjectId):
            result[k] = str(v)
        elif isinstance(v, dict):
            result[k] = _serialize_oids(v)
        elif isinstance(v, list):
            result[k] = [
                _serialize_oids(i) if isinstance(i, dict) else (str(i) if isinstance(i, ObjectId) else i)
                for i in v
            ]
        else:
            result[k] = v
    return result


async def _get_campaign_or_404(campaign_id: str, account_oid: ObjectId) -> dict:
    """Fetch campaign scoped to account, raise 404 if missing.
    Cross-tenant access also returns 404 (no existence leak)."""
    try:
        oid = ObjectId(campaign_id)
    except Exception:
        raise HTTPException(status_code=404, detail="Campaign not found")

    doc = await database.campaigns_collection.find_one({"_id": oid})
    if doc is None or doc.get("account_id") != account_oid:
        raise HTTPException(status_code=404, detail="Campaign not found")
    return doc


async def _get_schedule_or_404(campaign_oid: ObjectId, schedule_date: str) -> dict:
    """Fetch a campaign_daily_schedules doc by campaign + date, raise 404 if missing."""
    doc = await database.campaign_daily_schedules_collection.find_one(
        {"campaign_id": campaign_oid, "schedule_date": schedule_date}
    )
    if doc is None:
        raise HTTPException(
            status_code=404,
            detail=f"No schedule found for date {schedule_date}",
        )
    return doc


async def _embed_prospect_info(items: list[dict]) -> list[dict]:
    """Attach a 'prospect' sub-document to each schedule item."""
    prospect_ids = [i["prospect_id"] for i in items if i.get("prospect_id")]
    if not prospect_ids:
        return items

    prospects = await database.prospects_collection.find(
        {"_id": {"$in": prospect_ids}}
    ).to_list(length=len(prospect_ids))
    by_id = {p["_id"]: p for p in prospects}

    enriched = []
    for item in items:
        item = _serialize_oids(item)
        pid = item.get("prospect_id")
        if pid:
            raw = by_id.get(ObjectId(pid))
            if raw:
                p = _serialize_oids(raw)
                item["prospect"] = {
                    "first_name": p.get("first_name", ""),
                    "last_name": p.get("last_name", ""),
                    "full_name": p.get("full_name", ""),
                    "email": p.get("email", ""),
                    "company_name": p.get("company_name", ""),
                    "job_title": p.get("job_title", ""),
                    "linkedin": p.get("linkedin") or p.get("linkedin_url", ""),
                    "industry": p.get("industry", ""),
                }
        enriched.append(item)
    return enriched


async def _load_schedule_with_items(schedule_doc: dict) -> dict:
    """Return a serialized schedule dict with embedded items and prospect info."""
    schedule = _serialize_oids(schedule_doc)
    schedule_oid = schedule_doc["_id"]

    items_cursor = database.campaign_schedule_items_collection.find(
        {"schedule_id": schedule_oid}
    )
    raw_items = await items_cursor.to_list(length=None)
    schedule["items"] = await _embed_prospect_info(raw_items)
    return schedule


# ---------------------------------------------------------------------------
# Routes
# ---------------------------------------------------------------------------

@router.get("/api/campaigns/{campaign_id}/schedules/today")
async def get_today_schedule(
    campaign_id: str,
    account_ctx: dict = Depends(get_account_context),
):
    """Return today's schedule (if it exists) with embedded prospect info."""
    account_oid = ObjectId(account_ctx["account"]["_id"])
    await _get_campaign_or_404(campaign_id, account_oid)

    today = _today_str()
    campaign_oid = ObjectId(campaign_id)

    doc = await database.campaign_daily_schedules_collection.find_one(
        {"campaign_id": campaign_oid, "schedule_date": today}
    )
    if doc is None:
        return {"schedule": None, "schedule_date": today, "message": "No schedule generated for today"}

    return {"schedule": await _load_schedule_with_items(doc)}


@router.post("/api/campaigns/{campaign_id}/schedules/generate")
async def generate_schedule(
    campaign_id: str,
    body: GenerateScheduleRequest,
    account_ctx: dict = Depends(get_account_context),
):
    """
    Generate a draft daily schedule for the given date.

    Finds active enrollments whose next_action_at falls on that date,
    personalizes each step's message, and creates campaign_daily_schedules +
    campaign_schedule_items documents with status='pending_approval'.
    """
    account_oid = ObjectId(account_ctx["account"]["_id"])
    campaign = await _get_campaign_or_404(campaign_id, account_oid)
    campaign_oid = ObjectId(campaign_id)

    schedule_date = body.schedule_date or _today_str()

    # Validate date format
    try:
        datetime.strptime(schedule_date, "%Y-%m-%d")
    except ValueError:
        raise HTTPException(status_code=400, detail="schedule_date must be YYYY-MM-DD")

    # Check for existing schedule
    existing = await database.campaign_daily_schedules_collection.find_one(
        {"campaign_id": campaign_oid, "schedule_date": schedule_date}
    )
    if existing and not body.force_regenerate:
        raise HTTPException(
            status_code=409,
            detail=f"Schedule already exists for {schedule_date}. Use force_regenerate=true to overwrite.",
        )

    # If force-regenerating, remove old schedule and its items
    if existing and body.force_regenerate:
        old_oid = existing["_id"]
        await database.campaign_schedule_items_collection.delete_many({"schedule_id": old_oid})
        await database.campaign_daily_schedules_collection.delete_one({"_id": old_oid})

    # Date window for next_action_at
    day_start = datetime.strptime(f"{schedule_date} 00:00:00", "%Y-%m-%d %H:%M:%S")
    day_end = datetime.strptime(f"{schedule_date} 23:59:59", "%Y-%m-%d %H:%M:%S")

    # Find active enrollments due on this date
    enrollments = await database.campaign_enrollments_collection.find({
        "campaign_id": campaign_oid,
        "status": "active",
        "next_action_at": {"$gte": day_start, "$lte": day_end},
    }).to_list(length=None)

    if not enrollments:
        raise HTTPException(
            status_code=404,
            detail=f"No active enrollments due on {schedule_date}",
        )

    # Batch-load prospects
    prospect_ids = [e["prospect_id"] for e in enrollments]
    prospects = await database.prospects_collection.find(
        {"_id": {"$in": prospect_ids}}
    ).to_list(length=len(prospect_ids))
    prospects_by_id = {p["_id"]: p for p in prospects}

    steps = campaign.get("steps", [])
    now = datetime.utcnow()

    # Create the parent schedule document
    schedule_doc = {
        "campaign_id": campaign_oid,
        "account_id": account_oid,
        "schedule_date": schedule_date,
        "status": "pending_approval",
        "total_items": 0,
        "approved_at": None,
        "approved_by": None,
        "created_at": now,
        "updated_at": now,
    }
    schedule_result = await database.campaign_daily_schedules_collection.insert_one(schedule_doc)
    schedule_oid = schedule_result.inserted_id

    # Build schedule items
    items_to_insert = []
    for enrollment in enrollments:
        step_idx = enrollment.get("current_step", 0)
        if step_idx >= len(steps):
            continue

        step_def = steps[step_idx]
        prospect = prospects_by_id.get(enrollment["prospect_id"])
        if not prospect:
            continue

        body_text = personalize_simple(step_def.get("body_template", ""), prospect)
        subject_text = personalize_simple(step_def.get("subject_template", ""), prospect)

        item_doc = {
            "schedule_id": schedule_oid,
            "campaign_id": campaign_oid,
            "account_id": account_oid,
            "enrollment_id": enrollment["_id"],
            "prospect_id": enrollment["prospect_id"],
            "step_number": step_idx,
            "channel": step_def.get("channel", "email"),
            "action": step_def.get("action", "email"),
            "subject": subject_text,
            "body": body_text,
            "status": "pending",   # pending / scheduled / sent / failed / removed
            "edited_at": None,
            "created_at": now,
        }
        items_to_insert.append(item_doc)

    if items_to_insert:
        await database.campaign_schedule_items_collection.insert_many(items_to_insert)

    # Update total_items count on schedule
    await database.campaign_daily_schedules_collection.update_one(
        {"_id": schedule_oid},
        {"$set": {"total_items": len(items_to_insert), "updated_at": datetime.utcnow()}},
    )

    created_schedule = await database.campaign_daily_schedules_collection.find_one({"_id": schedule_oid})
    return {
        "schedule": _serialize_oids(created_schedule),
        "items_count": len(items_to_insert),
    }


@router.get("/api/campaigns/{campaign_id}/schedules/{schedule_date}")
async def get_schedule(
    campaign_id: str,
    schedule_date: str,
    account_ctx: dict = Depends(get_account_context),
):
    """Return a schedule for a specific date with all items and embedded prospect info."""
    account_oid = ObjectId(account_ctx["account"]["_id"])
    await _get_campaign_or_404(campaign_id, account_oid)

    campaign_oid = ObjectId(campaign_id)
    doc = await _get_schedule_or_404(campaign_oid, schedule_date)

    return {"schedule": await _load_schedule_with_items(doc)}


@router.patch("/api/campaigns/{campaign_id}/schedules/{schedule_date}/items/{item_id}")
async def edit_schedule_item(
    campaign_id: str,
    schedule_date: str,
    item_id: str,
    body: EditItemRequest,
    account_ctx: dict = Depends(get_account_context),
):
    """
    Edit a schedule item's subject and/or body before approving.
    Sets edited_at = now.
    """
    account_oid = ObjectId(account_ctx["account"]["_id"])
    await _get_campaign_or_404(campaign_id, account_oid)

    campaign_oid = ObjectId(campaign_id)
    schedule_doc = await _get_schedule_or_404(campaign_oid, schedule_date)

    if schedule_doc.get("status") not in ("pending_approval",):
        raise HTTPException(
            status_code=400,
            detail=f"Cannot edit items on a schedule with status '{schedule_doc.get('status')}'",
        )

    try:
        item_oid = ObjectId(item_id)
    except Exception:
        raise HTTPException(status_code=404, detail="Item not found")

    item = await database.campaign_schedule_items_collection.find_one(
        {"_id": item_oid, "schedule_id": schedule_doc["_id"]}
    )
    if item is None:
        raise HTTPException(status_code=404, detail="Item not found")

    if item.get("status") == "removed":
        raise HTTPException(status_code=400, detail="Cannot edit a removed item")

    updates: dict = {"edited_at": datetime.utcnow()}
    if body.subject is not None:
        updates["subject"] = body.subject
    if body.body is not None:
        updates["body"] = body.body

    if len(updates) == 1:
        raise HTTPException(status_code=400, detail="No fields provided to update")

    await database.campaign_schedule_items_collection.update_one(
        {"_id": item_oid},
        {"$set": updates},
    )

    updated_item = await database.campaign_schedule_items_collection.find_one({"_id": item_oid})
    return {"message": "Item updated", "item": _serialize_oids(updated_item)}


@router.delete("/api/campaigns/{campaign_id}/schedules/{schedule_date}/items/{item_id}")
async def remove_schedule_item(
    campaign_id: str,
    schedule_date: str,
    item_id: str,
    account_ctx: dict = Depends(get_account_context),
):
    """Mark a schedule item as 'removed' so it will be skipped during execution."""
    account_oid = ObjectId(account_ctx["account"]["_id"])
    await _get_campaign_or_404(campaign_id, account_oid)

    campaign_oid = ObjectId(campaign_id)
    schedule_doc = await _get_schedule_or_404(campaign_oid, schedule_date)

    if schedule_doc.get("status") not in ("pending_approval", "approved"):
        raise HTTPException(
            status_code=400,
            detail=f"Cannot remove items from a schedule with status '{schedule_doc.get('status')}'",
        )

    try:
        item_oid = ObjectId(item_id)
    except Exception:
        raise HTTPException(status_code=404, detail="Item not found")

    item = await database.campaign_schedule_items_collection.find_one(
        {"_id": item_oid, "schedule_id": schedule_doc["_id"]}
    )
    if item is None:
        raise HTTPException(status_code=404, detail="Item not found")

    await database.campaign_schedule_items_collection.update_one(
        {"_id": item_oid},
        {"$set": {"status": "removed"}},
    )

    return {"message": "Item removed"}


@router.post("/api/campaigns/{campaign_id}/schedules/{schedule_date}/approve")
async def approve_schedule(
    campaign_id: str,
    schedule_date: str,
    account_ctx: dict = Depends(get_account_context),
):
    """
    Approve a draft schedule.

    Sets schedule status = 'approved' and all pending items to 'scheduled'.
    The campaign engine will pick up 'scheduled' items automatically.
    """
    account_oid = ObjectId(account_ctx["account"]["_id"])
    await _get_campaign_or_404(campaign_id, account_oid)

    campaign_oid = ObjectId(campaign_id)
    schedule_doc = await _get_schedule_or_404(campaign_oid, schedule_date)

    if schedule_doc.get("status") != "pending_approval":
        raise HTTPException(
            status_code=400,
            detail=f"Schedule is already '{schedule_doc.get('status')}', cannot approve",
        )

    user_id = account_ctx["user"]["_id"]
    now = datetime.utcnow()

    # Approve the schedule
    await database.campaign_daily_schedules_collection.update_one(
        {"_id": schedule_doc["_id"]},
        {"$set": {
            "status": "approved",
            "approved_at": now,
            "approved_by": user_id,
            "updated_at": now,
        }},
    )

    # Move all pending items to 'scheduled'
    update_result = await database.campaign_schedule_items_collection.update_many(
        {"schedule_id": schedule_doc["_id"], "status": "pending"},
        {"$set": {"status": "scheduled"}},
    )

    total_scheduled = update_result.modified_count

    updated_schedule = await database.campaign_daily_schedules_collection.find_one(
        {"_id": schedule_doc["_id"]}
    )

    return {
        "message": "Schedule approved",
        "total_items": total_scheduled,
        "schedule": _serialize_oids(updated_schedule),
    }


@router.post("/api/campaigns/{campaign_id}/schedules/{schedule_date}/cancel")
async def cancel_schedule(
    campaign_id: str,
    schedule_date: str,
    account_ctx: dict = Depends(get_account_context),
):
    """
    Cancel a schedule.

    Sets schedule status = 'cancelled' and all pending/scheduled items to 'removed'.
    """
    account_oid = ObjectId(account_ctx["account"]["_id"])
    await _get_campaign_or_404(campaign_id, account_oid)

    campaign_oid = ObjectId(campaign_id)
    schedule_doc = await _get_schedule_or_404(campaign_oid, schedule_date)

    if schedule_doc.get("status") in ("cancelled", "completed"):
        raise HTTPException(
            status_code=400,
            detail=f"Schedule is already '{schedule_doc.get('status')}', cannot cancel",
        )

    now = datetime.utcnow()

    await database.campaign_daily_schedules_collection.update_one(
        {"_id": schedule_doc["_id"]},
        {"$set": {"status": "cancelled", "updated_at": now}},
    )

    await database.campaign_schedule_items_collection.update_many(
        {"schedule_id": schedule_doc["_id"], "status": {"$in": ["pending", "scheduled"]}},
        {"$set": {"status": "removed"}},
    )

    return {"message": "Schedule cancelled"}
