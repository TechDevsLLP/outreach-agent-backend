"""
Campaign routes for OutFlo multi-step outreach campaigns.
"""

import asyncio
import logging
from datetime import datetime
from typing import Optional

logger = logging.getLogger(__name__)

from bson import ObjectId
from fastapi import APIRouter, BackgroundTasks, Depends, HTTPException, Query
from pydantic import BaseModel, Field
from pymongo import ReturnDocument

from auth import get_account_context
from config import get_settings
import database
from database import (
    campaigns_collection,
    campaign_daily_stats_collection,
    campaign_enrollments_collection,
    campaign_messages_collection,
    campaign_schedule_items_collection,
    campaign_daily_schedules_collection,
    prospects_collection,
)
from models.campaign import CampaignCreateRequest, CampaignUpdateRequest, SmartCampaignCreateRequest
from utils.serialization import serialize_doc

settings = get_settings()

router = APIRouter(prefix="/api/campaigns", tags=["Campaigns"])


class BulkCampaignIdsRequest(BaseModel):
    ids: list[str]


class ScrapeMoreRequest(BaseModel):
    """ICP overrides for re-running prospect discovery on a smart campaign.

    All fields optional — any field left None is not overwritten on the campaign doc.
    """
    icp_industry_label: Optional[str] = None
    icp_industries: Optional[list[str]] = None
    icp_keywords: Optional[list[str]] = None
    icp_exclude_keywords: Optional[list[str]] = None
    icp_job_titles: Optional[list[str]] = None
    icp_seniority_levels: Optional[list[str]] = None
    icp_countries: Optional[list[str]] = None
    icp_company_size_min: Optional[int] = None
    icp_company_size_max: Optional[int] = None
    prospect_count_target: Optional[int] = Field(None, ge=25, le=500)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _to_object_ids(ids: list[str]) -> list[ObjectId]:
    out = []
    for s in ids:
        try:
            out.append(ObjectId(s))
        except Exception:
            continue
    return out


def serialize_campaign(doc: dict) -> dict:
    """Convert all ObjectId / datetime fields in a campaign document to JSON-safe types.

    Delegates to ``serialize_doc`` so naive UTC datetimes get a ``Z`` suffix —
    otherwise the frontend ``new Date()`` parses them as local time and the UI
    shows "created N hours ago" right after creation.
    """
    safe = dict(doc)
    # Internal vector-search embedding (BSON Binary subtype 9): large and not
    # JSON-serializable. Strip it from API responses; serialize_doc would only
    # render it as null anyway.
    safe.pop("title_query_vec", None)
    result = serialize_doc(safe)
    result["discovery_companies_found"] = doc.get("discovery_companies_found", 0)
    result["discovery_apify_triggered"] = doc.get("discovery_apify_triggered", False)
    return result


async def _get_campaign_or_404(campaign_id: str, account_id: ObjectId) -> dict:
    """Fetch a campaign by ID scoped to account_id, raise 404 if missing.

    Cross-tenant access also returns 404 (not 403) so another tenant's
    campaign IDs cannot be probed for existence — same semantics as the
    prospects routes."""
    try:
        oid = ObjectId(campaign_id)
    except Exception:
        raise HTTPException(status_code=404, detail="Campaign not found")

    doc = await campaigns_collection.find_one({"_id": oid})
    if doc is None or doc.get("account_id") != account_id:
        raise HTTPException(status_code=404, detail="Campaign not found")
    return doc


async def _canonicalize_campaign_icp(campaign_id: str) -> None:
    """Background: canonicalize the campaign ICP free-text into structured filters."""
    try:
        from services.icp_canonicalizer import canonicalize_icp
        campaign = await campaigns_collection.find_one({"_id": ObjectId(campaign_id)})
        if not campaign:
            return
        canonical = await canonicalize_icp(campaign)
        await campaigns_collection.update_one(
            {"_id": ObjectId(campaign_id)},
            {"$set": canonical},
        )
        logger.info(f"[campaigns] ICP canonicalized for campaign {campaign_id}")
    except Exception as e:
        logger.warning(f"[campaigns] ICP canonicalization failed for {campaign_id}: {e}")


async def _enqueue_discovery_or_fail(
    campaign_id: str,
    account_id: ObjectId,
    generation: int,
):
    """Persist durable discovery work or make the campaign failure explicit."""
    from services.enrichment_job_service import enqueue_campaign_discovery

    try:
        return await enqueue_campaign_discovery(
            account_id=str(account_id),
            campaign_id=campaign_id,
            generation=int(generation),
        )
    except Exception as exc:
        await campaigns_collection.update_one(
            {"_id": ObjectId(campaign_id), "account_id": account_id},
            {"$set": {
                "discovery_status": "failed",
                "discovery_error": "Could not queue discovery work",
                "updated_at": datetime.utcnow(),
            }},
        )
        logger.error("Could not queue discovery for campaign %s: %s", campaign_id, exc)
        raise HTTPException(
            status_code=503, detail="Could not queue campaign discovery"
        ) from exc


async def _recompute_day_totals(campaign_oid):
    """Recompute discovery_day_totals from current enrollment assignments."""
    pipeline = [
        {"$match": {
            "campaign_id": campaign_oid,
            "status": {"$nin": ["archived", "skipped_no_channel", "cascade_waiting"]},
            "smart_campaign_send_day": {"$ne": None},
        }},
        {"$group": {
            "_id": {"day": "$smart_campaign_send_day", "channel": "$smart_campaign_channel"},
            "count": {"$sum": 1},
        }},
    ]
    results = await campaign_enrollments_collection.aggregate(pipeline).to_list(length=500)
    day_totals: dict = {}
    for r in results:
        d = str(r["_id"]["day"])
        ch = r["_id"]["channel"] or "unknown"
        day_totals.setdefault(d, {})[ch] = r["count"]
    await campaigns_collection.update_one(
        {"_id": campaign_oid},
        {"$set": {"discovery_day_totals": day_totals}},
    )


# ---------------------------------------------------------------------------
# List & Create
# ---------------------------------------------------------------------------

@router.get("")
async def list_campaigns(
    status: Optional[str] = None,
    type: Optional[str] = None,
    search: Optional[str] = None,
    sort_by: str = "created_at",
    sort_order: str = "desc",
    page: int = Query(1, ge=1),
    page_size: int = Query(20, ge=1, le=200),
    account_ctx: dict = Depends(get_account_context),
):
    """List campaigns for the current account with search, sort, and pagination."""
    account_id = ObjectId(account_ctx["account"]["_id"])

    query: dict = {"account_id": account_id}
    if status:
        query["status"] = status
    if type:
        query["type"] = type
    if search:
        query["$or"] = [
            {"name": {"$regex": search, "$options": "i"}},
            {"description": {"$regex": search, "$options": "i"}},
        ]

    sort_direction = -1 if sort_order == "desc" else 1
    allowed_sorts = {"name", "created_at", "status"}
    sort_field = sort_by if sort_by in allowed_sorts else "created_at"

    skip = (page - 1) * page_size
    total = await campaigns_collection.count_documents(query)
    cursor = (
        campaigns_collection.find(query)
        .sort(sort_field, sort_direction)
        .skip(skip)
        .limit(page_size)
    )
    docs = await cursor.to_list(page_size)

    return {
        "campaigns": [serialize_campaign(d) for d in docs],
        "total": total,
        "total_pages": (total + page_size - 1) // page_size,
        "page": page,
        "page_size": page_size,
    }


@router.post("")
async def create_campaign(
    body: CampaignCreateRequest,
    account_ctx: dict = Depends(get_account_context),
):
    """Create a new campaign draft."""
    account_id = ObjectId(account_ctx["account"]["_id"])
    user_id = account_ctx["user"]["_id"]

    now = datetime.utcnow()
    doc = {
        "account_id": account_id,
        "created_by": user_id,
        "name": body.name,
        "description": body.description,
        "type": body.type,
        "status": "draft",
        "steps": [s.model_dump() for s in body.steps],
        "email_account_id": body.email_account_id,
        "linkedin_account_id": body.linkedin_account_id,
        "daily_email_limit": body.daily_email_limit,
        "daily_linkedin_limit": body.daily_linkedin_limit,
        "timezone": body.timezone or "America/New_York",
        "send_days": body.send_days or ["monday", "tuesday", "wednesday", "thursday", "friday"],
        "send_hour_start": body.send_hour_start,
        "send_hour_end": body.send_hour_end,
        # Counters
        "total_enrolled": 0,
        "active_count": 0,
        "completed_count": 0,
        "replied_count": 0,
        "bounced_count": 0,
        "opted_out_count": 0,
        "meetings_booked": 0,
        # Email stats
        "emails_sent": 0,
        "emails_delivered": 0,
        "emails_opened": 0,
        "emails_clicked": 0,
        "emails_replied": 0,
        "emails_bounced": 0,
        # LinkedIn stats
        "linkedin_connections_sent": 0,
        "linkedin_connections_accepted": 0,
        "linkedin_inmails_sent": 0,
        "linkedin_replies": 0,
        # Timestamps
        "created_at": now,
        "updated_at": now,
        "started_at": None,
        "completed_at": None,
    }

    result = await campaigns_collection.insert_one(doc)
    created = await campaigns_collection.find_one({"_id": result.inserted_id})
    return {"campaign": serialize_campaign(created)}


# ---------------------------------------------------------------------------
# Account-level aggregate stats  (must be before /{campaign_id} routes)
# ---------------------------------------------------------------------------

@router.get("/account-stats")
async def get_account_stats(
    account_ctx: dict = Depends(get_account_context),
):
    """Aggregate stats across all campaigns for this account."""
    account_id = ObjectId(account_ctx["account"]["_id"])

    pipeline = [
        {"$match": {"account_id": account_id}},
        {
            "$group": {
                "_id": None,
                "total_campaigns": {"$sum": 1},
                "active_campaigns": {
                    "$sum": {"$cond": [{"$eq": ["$status", "active"]}, 1, 0]}
                },
                "total_enrolled": {"$sum": "$total_enrolled"},
                "total_replied": {"$sum": "$replied_count"},
                "total_meetings_booked": {"$sum": "$meetings_booked"},
                "emails_sent": {"$sum": "$emails_sent"},
                "emails_opened": {"$sum": "$emails_opened"},
                "linkedin_connections_sent": {"$sum": "$linkedin_connections_sent"},
                "linkedin_connections_accepted": {"$sum": "$linkedin_connections_accepted"},
                "linkedin_inmails_sent": {"$sum": "$linkedin_inmails_sent"},
            }
        },
    ]

    rows = await campaigns_collection.aggregate(pipeline).to_list(1)
    if not rows:
        return {
            "total_campaigns": 0,
            "active_campaigns": 0,
            "total_enrolled": 0,
            "total_replied": 0,
            "total_meetings_booked": 0,
            "emails_sent": 0,
            "emails_opened": 0,
            "linkedin_connections_sent": 0,
            "linkedin_connections_accepted": 0,
            "linkedin_inmails_sent": 0,
        }

    row = rows[0]
    row.pop("_id", None)
    return row


# ---------------------------------------------------------------------------
# Smart Campaign: validate ICP params (must be before /{campaign_id})
# ---------------------------------------------------------------------------
# NOTE: POST /generate-params (natural language → ApifyParams) was removed with
# the Apollo prospect actors. ICP capture now goes through /validate-target and
# /prefill-from-prompt.


class ValidateTargetRequest(BaseModel):
    conversation: list[dict]  # [{"role": "user"|"assistant", "content": str}]


@router.post("/validate-target")
async def validate_target_market(
    body: ValidateTargetRequest,
    account_ctx: dict = Depends(get_account_context),
):
    """
    Conversational ICP validation endpoint. Accepts a multi-turn conversation
    and either asks a follow-up question or returns structured targeting params.
    """
    from services.openrouter_service import OpenRouterClient, get_free_model

    conversation = body.conversation
    if not conversation:
        raise HTTPException(status_code=422, detail="Conversation cannot be empty")

    # Count user turns
    user_turns = sum(1 for msg in conversation if msg.get("role") == "user")

    system_prompt = (
        "You are an expert B2B sales targeting specialist. Analyze the conversation to determine "
        "if there's enough information to define a target market for prospecting.\n\n"
        "REQUIRED information (need at least these):\n"
        "- WHO to target: job titles, roles, or seniority levels (e.g., \"CTOs\", \"VP of Sales\", \"decision-makers\")\n"
        "- WHAT industry/sector (e.g., \"SaaS\", \"manufacturing\", \"fintech\")\n"
        "- At least ONE qualifier: company size (employees or revenue), funding stage, geography, or company keywords\n\n"
        "If the conversation has enough info, generate structured targeting parameters.\n"
        "If NOT enough info, ask ONE specific follow-up question to get the most critical missing piece.\n\n"
        "Return JSON in one of these formats:\n\n"
        "If sufficient:\n"
        "{\n"
        '  "sufficient": true,\n'
        '  "params": {\n'
        '    "contactJobTitle": ["string"],\n'
        '    "contactNotJobTitle": [],\n'
        '    "seniorityLevel": ["Director" | "Manager" | "Owner" | "Partner" | "C-Suite" | "VP" | "Head" | "Senior"],\n'
        '    "companyIndustry": ["string"],\n'
        '    "companyKeywords": ["string"],\n'
        '    "minRevenue": "string or null",\n'
        '    "maxRevenue": "string or null",\n'
        '    "funding": ["Seed" | "Series A" | "Series B" | "Series C" | "Series D+" | "IPO"],\n'
        '    "regions": [{"name": "string", "locations": ["string"], "fetchCount": 25, "startPage": 0}]\n'
        "  },\n"
        '  "summary": "Single clear sentence describing the target market in plain English"\n'
        "}\n\n"
        "If not sufficient:\n"
        "{\n"
        '  "sufficient": false,\n'
        '  "question": "One specific follow-up question"\n'
        "}\n\n"
        "Rules for params:\n"
        "- seniorityLevel must use ONLY these exact values: Director, Manager, Owner, Partner, C-Suite, VP, Head, Senior\n"
        "- funding must use ONLY: Seed, Series A, Series B, Series C, Series D+, IPO\n"
        "- regions.locations should use country names (e.g., \"United States\", \"United Kingdom\")\n"
        "- Default to United States region if no geography specified\n"
        "- fetchCount per region: 25 for primary, 15 for secondary\n"
        "- When sufficient=true, include a `summary` field: a single clear sentence describing the target market in plain English for user confirmation "
        "(e.g. 'We\\'ll reach out to CTOs and VPs at Series A-B SaaS companies in the US with 50-200 employees')."
    )

    # If user has already had 3+ turns, force generation regardless
    if user_turns >= 3:
        messages_to_send = list(conversation) + [
            {
                "role": "user",
                "content": (
                    "[System note: Enough context has been gathered. "
                    "Generate the structured targeting params now even if some details are missing. "
                    "Make reasonable assumptions and return sufficient=true with your best params.]"
                ),
            }
        ]
    else:
        messages_to_send = list(conversation)

    client = OpenRouterClient()
    try:
        result = await client.chat_completion(
            messages=[
                {"role": "system", "content": system_prompt},
                *messages_to_send,
            ],
            model=get_free_model(0),
            temperature=0.3,
            max_tokens=1500,
            response_format={"type": "json_object"},
        )
    finally:
        await client.close()

    if not isinstance(result, dict):
        raise HTTPException(status_code=500, detail="AI returned an unexpected response format")

    sufficient = result.get("sufficient", False)

    if not sufficient:
        return {
            "sufficient": False,
            "question": result.get("question", "Could you tell me more about your target market?"),
        }

    params = result.get("params", {})

    if not params.get("regions"):
        params["regions"] = [
            {"name": "United States", "locations": ["United States"], "fetchCount": 25, "startPage": 0}
        ]

    response: dict = {"sufficient": True, "params": params}
    summary = result.get("summary")
    if summary:
        response["summary"] = summary
    return response


# ---------------------------------------------------------------------------
# Campaign prefill from natural-language prompt
# ---------------------------------------------------------------------------

@router.post("/prefill-from-prompt")
async def prefill_campaign_from_prompt(
    body: dict,
    account_ctx: dict = Depends(get_account_context),
):
    """
    Convert a natural-language conversation into a structured campaign draft.
    Returns either a clarification question or a complete campaign config.
    """
    from services.campaign_prefill_flow import (
        next_missing_step,
        render_question,
        count_required_satisfied,
        count_required_total,
        strip_empty,
        llm_extract_step,
        llm_compose_campaign,
    )

    conversation = body.get("conversation", [])
    if not conversation:
        raise HTTPException(status_code=400, detail="conversation is required")
    captured = body.get("captured") or {}

    account_id = ObjectId(account_ctx["account"]["_id"])
    company_profile = await database.company_profiles_collection.find_one({"account_id": account_id})

    # Phase 1: extract values from the latest user turn (free-text or widget answer)
    last_user_msg = next(
        (m for m in reversed(conversation) if m.get("role") == "user"),
        None,
    )
    if last_user_msg and last_user_msg.get("content"):
        expected = next_missing_step(captured)
        captured = await llm_extract_step(
            captured=captured,
            last_user_text=last_user_msg["content"],
            expected_field=expected.id if expected else None,
            company_profile=company_profile,
        )

    # Phase 2: deterministically pick next missing step OR compose
    step = next_missing_step(captured)
    if step:
        return {
            "needs_clarification": {
                "question": render_question(step, captured),
                "field": step.id,
                "widget": step.widget,
                "options": step.options or [],
                "allow_free_text": step.allow_free_text,
                "progress": {
                    "captured": count_required_satisfied(captured),
                    "total": count_required_total(),
                },
            },
            "captured": strip_empty(captured),
        }

    # Phase 3: all steps satisfied — compose the campaign
    campaign = await llm_compose_campaign(captured, company_profile)

    # Validate and clean industry fields (preserve existing normalization logic)
    from services.industry_param_generator import VALID_INDUSTRIES, generate_industry_params
    valid_set = {ind.lower().strip() for ind in VALID_INDUSTRIES}
    target = campaign.get("target", {})

    raw_industries = target.get("icp_industries", target.get("industries", []))
    validated = [ind for ind in raw_industries if ind.lower().strip() in valid_set]
    if len(validated) < len(raw_industries):
        dropped = set(raw_industries) - set(validated)
        logger.warning(f"Dropped invalid industries from prefill: {dropped}")
    if not validated and target.get("industry_label"):
        try:
            fallback = await generate_industry_params(target["industry_label"])
            validated = fallback.get("company_industry", [])
            if not target.get("keywords"):
                target["keywords"] = fallback.get("company_keywords", [])
        except Exception as e:
            logger.warning(f"Industry fallback failed: {e}")
    target["icp_industries"] = validated
    target["keywords"] = (target.get("keywords") or [])[:5]
    target["exclude_keywords"] = target.get("exclude_keywords") or []
    target["exclude_industries"] = target.get("exclude_industries") or []

    # Ensure functional_departments flows through (LLM may omit it)
    target["functional_departments"] = (
        target.get("functional_departments")
        or target.get("icp_functional_departments")
        or [d for d in (captured.get("icp_functional_departments") or []) if d]
    )
    campaign["target"] = target

    return {"campaign": campaign, "captured": strip_empty(captured)}


# ---------------------------------------------------------------------------
# Single campaign CRUD
# ---------------------------------------------------------------------------

@router.get("/{campaign_id}")
async def get_campaign(
    campaign_id: str,
    account_ctx: dict = Depends(get_account_context),
):
    """Get a single campaign by ID."""
    account_id = ObjectId(account_ctx["account"]["_id"])
    doc = await _get_campaign_or_404(campaign_id, account_id)
    return {"campaign": serialize_campaign(doc)}


@router.patch("/{campaign_id}")
async def update_campaign(
    campaign_id: str,
    body: CampaignUpdateRequest,
    account_ctx: dict = Depends(get_account_context),
):
    """Update mutable fields on a campaign."""
    account_id = ObjectId(account_ctx["account"]["_id"])
    await _get_campaign_or_404(campaign_id, account_id)

    updates = body.model_dump(exclude_none=True)
    if not updates:
        raise HTTPException(status_code=400, detail="No fields provided to update")

    # Serialize steps if present
    if "steps" in updates:
        updates["steps"] = [
            s.model_dump() if hasattr(s, "model_dump") else s for s in updates["steps"]
        ]

    updates["updated_at"] = datetime.utcnow()
    oid = ObjectId(campaign_id)
    await campaigns_collection.update_one({"_id": oid}, {"$set": updates})
    updated = await campaigns_collection.find_one({"_id": oid})
    return {"campaign": serialize_campaign(updated)}


@router.delete("/{campaign_id}")
async def archive_campaign(
    campaign_id: str,
    account_ctx: dict = Depends(get_account_context),
):
    """Soft-delete a campaign by setting its status to 'archived'."""
    account_id = ObjectId(account_ctx["account"]["_id"])
    await _get_campaign_or_404(campaign_id, account_id)

    oid = ObjectId(campaign_id)
    await campaigns_collection.update_one(
        {"_id": oid},
        {"$set": {"status": "archived", "updated_at": datetime.utcnow()}},
    )
    return {"message": "Campaign archived"}


# ---------------------------------------------------------------------------
# Bulk operations
# ---------------------------------------------------------------------------

@router.post("/bulk/archive")
async def bulk_archive_campaigns(
    body: BulkCampaignIdsRequest,
    account_ctx: dict = Depends(get_account_context),
):
    """Soft-archive many campaigns (status → archived)."""
    account_id = ObjectId(account_ctx["account"]["_id"])
    oids = _to_object_ids(body.ids)
    if not oids:
        return {"updated": 0}
    result = await campaigns_collection.update_many(
        {"_id": {"$in": oids}, "account_id": account_id},
        {"$set": {"status": "archived", "updated_at": datetime.utcnow()}},
    )
    return {"updated": result.modified_count}


@router.post("/bulk/unarchive")
async def bulk_unarchive_campaigns(
    body: BulkCampaignIdsRequest,
    account_ctx: dict = Depends(get_account_context),
):
    """Unarchive campaigns — lands them in paused so user must explicitly Resume."""
    account_id = ObjectId(account_ctx["account"]["_id"])
    oids = _to_object_ids(body.ids)
    if not oids:
        return {"updated": 0}
    result = await campaigns_collection.update_many(
        {"_id": {"$in": oids}, "account_id": account_id, "status": "archived"},
        {"$set": {"status": "paused", "updated_at": datetime.utcnow()}},
    )
    return {"updated": result.modified_count}


@router.post("/bulk/pause")
async def bulk_pause_campaigns(
    body: BulkCampaignIdsRequest,
    account_ctx: dict = Depends(get_account_context),
):
    """Pause many active campaigns."""
    account_id = ObjectId(account_ctx["account"]["_id"])
    oids = _to_object_ids(body.ids)
    if not oids:
        return {"updated": 0}
    result = await campaigns_collection.update_many(
        {"_id": {"$in": oids}, "account_id": account_id, "status": "active"},
        {"$set": {"status": "paused", "updated_at": datetime.utcnow()}},
    )
    return {"updated": result.modified_count}


@router.post("/bulk/delete")
async def bulk_delete_campaigns(
    body: BulkCampaignIdsRequest,
    account_ctx: dict = Depends(get_account_context),
):
    """Hard-delete campaigns and all related records (enrollments, messages, schedules, stats)."""
    account_id = ObjectId(account_ctx["account"]["_id"])
    oids = _to_object_ids(body.ids)
    if not oids:
        return {"deleted": 0, "cascade_deleted": {}}

    owned = await campaigns_collection.find(
        {"_id": {"$in": oids}, "account_id": account_id}, {"_id": 1}
    ).to_list(length=None)
    owned_ids = [d["_id"] for d in owned]
    if not owned_ids:
        return {"deleted": 0, "cascade_deleted": {}}

    scope = {"campaign_id": {"$in": owned_ids}}
    enroll_res = await campaign_enrollments_collection.delete_many(scope)
    sched_res = await campaign_schedule_items_collection.delete_many(scope)
    msg_res = await campaign_messages_collection.delete_many(scope)
    stats_res = await campaign_daily_stats_collection.delete_many(scope)
    daily_res = await campaign_daily_schedules_collection.delete_many(scope)
    del_res = await campaigns_collection.delete_many({"_id": {"$in": owned_ids}})

    return {
        "deleted": del_res.deleted_count,
        "cascade_deleted": {
            "campaign_enrollments": enroll_res.deleted_count,
            "campaign_schedule_items": sched_res.deleted_count,
            "campaign_messages": msg_res.deleted_count,
            "campaign_daily_stats": stats_res.deleted_count,
            "campaign_daily_schedules": daily_res.deleted_count,
        },
    }


# ---------------------------------------------------------------------------
# Status transitions
# ---------------------------------------------------------------------------

@router.post("/{campaign_id}/activate")
async def activate_campaign(
    campaign_id: str,
    account_ctx: dict = Depends(get_account_context),
):
    """Activate a campaign (requires at least one step)."""
    account_id = ObjectId(account_ctx["account"]["_id"])
    doc = await _get_campaign_or_404(campaign_id, account_id)

    from services.campaign_launch_service import (
        SequenceLaunchValidationError,
        ensure_sequence_ready_for_launch,
    )
    try:
        ensure_sequence_ready_for_launch(doc)
    except SequenceLaunchValidationError as exc:
        raise HTTPException(status_code=409, detail=str(exc)) from exc

    if not doc.get("steps"):
        raise HTTPException(
            status_code=400, detail="Campaign must have at least one step before activating"
        )

    now = datetime.utcnow()
    oid = ObjectId(campaign_id)
    await campaigns_collection.update_one(
        {"_id": oid},
        {"$set": {"status": "active", "started_at": now, "updated_at": now}},
    )
    updated = await campaigns_collection.find_one({"_id": oid})
    return {"campaign": serialize_campaign(updated)}


@router.post("/{campaign_id}/pause")
async def pause_campaign(
    campaign_id: str,
    account_ctx: dict = Depends(get_account_context),
):
    """Pause an active campaign."""
    account_id = ObjectId(account_ctx["account"]["_id"])
    await _get_campaign_or_404(campaign_id, account_id)

    oid = ObjectId(campaign_id)
    await campaigns_collection.update_one(
        {"_id": oid},
        {"$set": {"status": "paused", "updated_at": datetime.utcnow()}},
    )
    updated = await campaigns_collection.find_one({"_id": oid})
    return {"campaign": serialize_campaign(updated)}


@router.post("/{campaign_id}/resume")
async def resume_campaign(
    campaign_id: str,
    account_ctx: dict = Depends(get_account_context),
):
    """Resume a paused campaign."""
    account_id = ObjectId(account_ctx["account"]["_id"])
    await _get_campaign_or_404(campaign_id, account_id)

    oid = ObjectId(campaign_id)
    await campaigns_collection.update_one(
        {"_id": oid},
        {"$set": {"status": "active", "updated_at": datetime.utcnow()}},
    )
    updated = await campaigns_collection.find_one({"_id": oid})
    return {"campaign": serialize_campaign(updated)}


# ---------------------------------------------------------------------------
# Duplicate
# ---------------------------------------------------------------------------

@router.post("/{campaign_id}/duplicate")
async def duplicate_campaign(
    campaign_id: str,
    new_name: Optional[str] = None,
    account_ctx: dict = Depends(get_account_context),
):
    """Duplicate a campaign, resetting all counters and status to draft."""
    account_id = ObjectId(account_ctx["account"]["_id"])
    original = await _get_campaign_or_404(campaign_id, account_id)

    now = datetime.utcnow()
    copy = dict(original)

    # Remove the original _id so MongoDB generates a new one
    copy.pop("_id", None)

    copy["name"] = new_name if new_name else f"Copy of {original['name']}"
    copy["status"] = "draft"
    copy["started_at"] = None
    copy["completed_at"] = None
    copy["created_at"] = now
    copy["updated_at"] = now

    # Reset all counters
    counter_fields = [
        "total_enrolled", "active_count", "completed_count", "replied_count",
        "bounced_count", "opted_out_count", "meetings_booked",
        "emails_sent", "emails_delivered", "emails_opened", "emails_clicked",
        "emails_replied", "emails_bounced",
        "linkedin_connections_sent", "linkedin_connections_accepted",
        "linkedin_inmails_sent", "linkedin_replies",
    ]
    for field in counter_fields:
        copy[field] = 0

    result = await campaigns_collection.insert_one(copy)
    created = await campaigns_collection.find_one({"_id": result.inserted_id})
    return {"campaign": serialize_campaign(created)}


# ---------------------------------------------------------------------------
# Stats endpoints
# ---------------------------------------------------------------------------

@router.get("/{campaign_id}/stats")
async def get_campaign_stats(
    campaign_id: str,
    account_ctx: dict = Depends(get_account_context),
):
    """Return campaign stats — live counts from campaign_messages where possible."""
    from database import campaign_messages_collection
    account_id = ObjectId(account_ctx["account"]["_id"])
    doc = await _get_campaign_or_404(campaign_id, account_id)
    oid = ObjectId(campaign_id)

    # Live counts from campaign_messages collection (source of truth)
    li_sent = await campaign_messages_collection.count_documents({
        "campaign_id": oid,
        "channel": "linkedin",
        "action": "connection_request",
        "status": {"$in": ["sent", "delivered", "opened", "replied"]},
    })
    emails_sent_live = await campaign_messages_collection.count_documents({
        "campaign_id": oid,
        "channel": "email",
        "status": {"$in": ["sent", "delivered", "opened", "clicked", "replied"]},
    })
    emails_opened_live = await campaign_messages_collection.count_documents({
        "campaign_id": oid,
        "channel": "email",
        "status": {"$in": ["opened", "clicked", "replied"]},
    })
    emails_clicked_live = await campaign_messages_collection.count_documents({
        "campaign_id": oid,
        "channel": "email",
        "status": {"$in": ["clicked", "replied"]},
    })
    emails_replied_live = await campaign_messages_collection.count_documents({
        "campaign_id": oid,
        "channel": "email",
        "status": "replied",
    })
    emails_bounced_live = await campaign_messages_collection.count_documents({
        "campaign_id": oid,
        "channel": "email",
        "status": "bounced",
    })
    inmails_sent = await campaign_messages_collection.count_documents({
        "campaign_id": oid,
        "channel": "linkedin",
        "action": "inmail",
        "status": {"$in": ["sent", "delivered", "opened", "replied"]},
    })

    # Use live counts; fall back to denormalized counter if live is 0 (backcompat)
    def _pick(live: int, fallback: int) -> int:
        return live if live > 0 else fallback

    return {
        "total_enrolled": doc.get("total_enrolled", 0),
        "active_count": doc.get("active_count", 0),
        "completed_count": doc.get("completed_count", 0),
        "replied_count": doc.get("replied_count", 0),
        "bounced_count": doc.get("bounced_count", 0),
        "opted_out_count": doc.get("opted_out_count", 0),
        "meetings_booked": doc.get("meetings_booked", 0),
        "emails_sent": _pick(emails_sent_live, doc.get("emails_sent", 0)),
        "emails_delivered": doc.get("emails_delivered", 0),
        "emails_opened": _pick(emails_opened_live, doc.get("emails_opened", 0)),
        "emails_clicked": _pick(emails_clicked_live, doc.get("emails_clicked", 0)),
        "emails_replied": _pick(emails_replied_live, doc.get("emails_replied", 0)),
        "emails_bounced": _pick(emails_bounced_live, doc.get("emails_bounced", 0)),
        "linkedin_connections_sent": _pick(li_sent, doc.get("linkedin_connections_sent", 0)),
        "linkedin_connections_accepted": doc.get("linkedin_connections_accepted", 0),
        "linkedin_inmails_sent": _pick(inmails_sent, doc.get("linkedin_inmails_sent", 0)),
        "linkedin_replies": doc.get("linkedin_replies", 0),
    }


@router.get("/{campaign_id}/daily-stats")
async def get_campaign_daily_stats(
    campaign_id: str,
    start_date: Optional[str] = Query(None, description="YYYY-MM-DD"),
    end_date: Optional[str] = Query(None, description="YYYY-MM-DD"),
    account_ctx: dict = Depends(get_account_context),
):
    """Return daily stats for a campaign between start_date and end_date."""
    account_id = ObjectId(account_ctx["account"]["_id"])
    await _get_campaign_or_404(campaign_id, account_id)

    query: dict = {"campaign_id": campaign_id}

    if start_date or end_date:
        date_filter: dict = {}
        if start_date:
            try:
                date_filter["$gte"] = datetime.strptime(start_date, "%Y-%m-%d")
            except ValueError:
                raise HTTPException(status_code=400, detail="start_date must be YYYY-MM-DD")
        if end_date:
            try:
                # Include the full end day
                end_dt = datetime.strptime(end_date, "%Y-%m-%d").replace(
                    hour=23, minute=59, second=59
                )
                date_filter["$lte"] = end_dt
            except ValueError:
                raise HTTPException(status_code=400, detail="end_date must be YYYY-MM-DD")
        query["date"] = date_filter

    cursor = campaign_daily_stats_collection.find(query).sort("date", 1)
    rows = await cursor.to_list(None)

    dates = []
    stats = []
    for row in rows:
        row.pop("_id", None)
        # Normalise campaign_id ObjectId if present
        if "campaign_id" in row and isinstance(row["campaign_id"], ObjectId):
            row["campaign_id"] = str(row["campaign_id"])
        if "account_id" in row and isinstance(row["account_id"], ObjectId):
            row["account_id"] = str(row["account_id"])
        date_val = row.get("date")
        dates.append(date_val.strftime("%Y-%m-%d") if isinstance(date_val, datetime) else str(date_val))
        stats.append(row)

    return {"dates": dates, "stats": stats}


# ---------------------------------------------------------------------------
# Campaign Inbox
# ---------------------------------------------------------------------------

@router.get("/{campaign_id}/inbox")
async def get_campaign_inbox(
    campaign_id: str,
    account_ctx: dict = Depends(get_account_context),
):
    """
    Return campaign inbox activity: emails opened, connections accepted, replies.
    Scoped to prospects enrolled in this campaign.
    """
    from database import campaign_enrollments_collection, prospects_collection

    account_id = ObjectId(account_ctx["account"]["_id"])
    await _get_campaign_or_404(campaign_id, account_id)

    # Fetch enrollments for this campaign
    enrollments = await campaign_enrollments_collection.find(
        {"campaign_id": campaign_id},
        {"prospect_id": 1},
    ).to_list(None)

    prospect_ids = []
    for e in enrollments:
        pid = e.get("prospect_id")
        if pid:
            try:
                prospect_ids.append(ObjectId(str(pid)))
            except Exception:
                pass

    if not prospect_ids:
        return {
            "email_opens": [],
            "connections_accepted": [],
            "replies": [],
            "summary": {
                "total_enrolled": 0,
                "email_opens": 0,
                "connections_accepted": 0,
                "replies": 0,
            },
        }

    prospects_data = await prospects_collection.find(
        {"_id": {"$in": prospect_ids}},
        {
            "full_name": 1,
            "email": 1,
            "company_name": 1,
            "job_title": 1,
            "status": 1,
            "last_outreach_opened": 1,
            "connection_accepted_at": 1,
            "outreach_history": 1,
        },
    ).to_list(None)

    opens = []
    connections_accepted = []
    replies = []

    for p in prospects_data:
        pid = str(p["_id"])
        name = p.get("full_name") or ""
        company = p.get("company_name") or ""
        email = p.get("email") or ""
        job_title = p.get("job_title") or ""

        base = {
            "prospect_id": pid,
            "name": name,
            "company": company,
            "email": email,
            "job_title": job_title,
        }

        # Email opens
        if p.get("last_outreach_opened"):
            opens.append({**base, "opened_at": p["last_outreach_opened"]})

        # LinkedIn connection accepted
        if p.get("connection_accepted_at"):
            connections_accepted.append({**base, "accepted_at": p["connection_accepted_at"]})

        # Replies: status == "replied" or outreach_history has reply event
        if p.get("status") == "replied":
            replied_at = None
            for h in p.get("outreach_history", []):
                evt = h.get("event") or h.get("event_type") or ""
                if evt in ("email_reply_received", "replied", "linkedin_reply", "reply"):
                    replied_at = h.get("timestamp")
                    break
            replies.append({**base, "replied_at": replied_at})

    # Sort by time descending
    opens.sort(key=lambda x: str(x.get("opened_at") or ""), reverse=True)
    connections_accepted.sort(key=lambda x: str(x.get("accepted_at") or ""), reverse=True)
    replies.sort(key=lambda x: str(x.get("replied_at") or ""), reverse=True)

    return {
        "email_opens": opens,
        "connections_accepted": connections_accepted,
        "replies": replies,
        "summary": {
            "total_enrolled": len(prospect_ids),
            "email_opens": len(opens),
            "connections_accepted": len(connections_accepted),
            "replies": len(replies),
        },
    }


# ---------------------------------------------------------------------------
# Smart Campaign: edit enrollment messages request model
# ---------------------------------------------------------------------------

class EditEnrollmentMessagesRequest(BaseModel):
    message_type: str  # "cold_email" | "linkedin_connection" | "linkedin_inmail"
    subject: Optional[str] = None
    body: str


# ---------------------------------------------------------------------------
# Smart Campaign: creation, discovery, review, and launch
# ---------------------------------------------------------------------------

def _smart_campaign_defaults(campaign_type: str) -> dict:
    from services.daily_cap_service import DEFAULT_CAPS
    caps = dict(DEFAULT_CAPS)
    if campaign_type == "email":
        caps["linkedin_connection"] = 0
        caps["linkedin_inmail"] = 0
        caps["linkedin_message"] = 0
    elif campaign_type == "linkedin":
        caps["email"] = 0
    return {"daily_caps": caps}


@router.post("/smart")
async def create_smart_campaign(
    body: SmartCampaignCreateRequest,
    background_tasks: BackgroundTasks,
    account_ctx: dict = Depends(get_account_context),
):
    """
    Create a Smart Campaign and immediately trigger AI prospect discovery.
    Returns the created campaign. Frontend should poll /discovery-status.
    """
    account_id = ObjectId(account_ctx["account"]["_id"])
    user_id = account_ctx["user"]["_id"]

    now = datetime.utcnow()
    campaign_type = (body.type or "custom").lower()
    defaults = _smart_campaign_defaults(campaign_type)
    caps = defaults["daily_caps"]
    email_limit = caps.get("email", 20)
    linkedin_limit = caps.get("linkedin_connection", 20) + caps.get("linkedin_inmail", 0)

    # Validate required sender accounts for the chosen campaign type. The
    # frontend wizard does not surface an account picker today — discovery
    # auto-picks the first connected account per channel. So we check that
    # the user actually HAS a connected account of each required type rather
    # than requiring an explicit ID in the request body.
    #
    # account_id can be stored as either a string OR an ObjectId in these
    # collections (historical inconsistency between insert paths), so match both.
    needs_email = campaign_type in {"email", "hybrid", "custom"}
    needs_linkedin = campaign_type in {"linkedin", "hybrid"}
    account_id_filter = {"$in": [account_id, str(account_id)]}

    if needs_email:
        ea_exists = await database.email_accounts_collection.find_one(
            {"account_id": account_id_filter, "status": {"$in": ["connected", "active"]}},
            {"_id": 1},
        )
        if not ea_exists:
            raise HTTPException(
                status_code=400,
                detail=f"An email account is required for {campaign_type} campaigns. Connect one in Settings → Email Accounts.",
            )

    if needs_linkedin:
        # linkedin_accounts uses `unipile_status` (OK/CREDENTIALS/ERROR/STOPPED/CONNECTING/DELETED).
        la_exists = await database.linkedin_accounts_collection.find_one(
            {"account_id": account_id_filter, "unipile_status": {"$in": ["OK"]}},
            {"_id": 1},
        )
        if not la_exists:
            raise HTTPException(
                status_code=400,
                detail=f"A LinkedIn account is required for {campaign_type} campaigns. Connect one in Settings → LinkedIn.",
            )

    # If the request DID specify explicit account IDs, verify they belong to
    # this account and are connected. (Future-proofing for when the wizard
    # adds a picker.)
    if body.email_account_id:
        ea = await database.email_accounts_collection.find_one(
            {"_id": ObjectId(body.email_account_id), "account_id": account_id, "status": {"$in": ["connected", "active"]}},
            {"_id": 1},
        )
        if not ea:
            raise HTTPException(status_code=400, detail="The selected email account is not connected.")

    if body.linkedin_account_id:
        try:
            li_account_oid = ObjectId(body.linkedin_account_id)
        except Exception:
            raise HTTPException(status_code=400, detail="Invalid linkedin_account_id format")
        la = await database.linkedin_accounts_collection.find_one(
            {"_id": li_account_oid, "account_id": account_id, "unipile_status": {"$in": ["OK"]}},
            {"_id": 1},
        )
        if not la:
            raise HTTPException(status_code=400, detail="The selected LinkedIn account is not connected.")

    # ── Upload-a-Lead-List (BYOL): validate the attached batch ───────────────
    is_upload = (body.discovery_mode or "").lower() == "upload"
    if is_upload:
        if not body.upload_batch_id or not ObjectId.is_valid(body.upload_batch_id):
            raise HTTPException(
                status_code=400,
                detail="A confirmed lead upload is required for upload campaigns.",
            )
        upload_batch = await database.lead_upload_batches_collection.find_one(
            {"_id": ObjectId(body.upload_batch_id), "account_id": str(account_id)},
            {"status": 1, "mapping": 1},
        )
        if not upload_batch:
            raise HTTPException(status_code=404, detail="Lead upload batch not found for this account.")
        if upload_batch.get("status") != "ready" or not upload_batch.get("mapping"):
            raise HTTPException(
                status_code=400,
                detail="The lead upload column mapping has not been confirmed yet.",
            )

    doc = {
        "account_id": account_id,
        "created_by": user_id,
        "name": body.name,
        "description": body.description,
        "type": body.type,
        "status": "draft",
        "steps": [],
        "email_account_id": body.email_account_id,
        "linkedin_account_id": body.linkedin_account_id,
        "daily_email_limit": email_limit,
        "daily_linkedin_limit": linkedin_limit,
        "daily_caps": defaults["daily_caps"],
        "timezone": body.timezone or "America/New_York",
        "send_days": body.send_days or ["monday", "tuesday", "wednesday", "thursday", "friday"],
        "send_hour_start": body.send_hour_start,
        "send_hour_end": body.send_hour_end,
        # Counters
        "total_enrolled": 0,
        "active_count": 0,
        "completed_count": 0,
        "replied_count": 0,
        "bounced_count": 0,
        "opted_out_count": 0,
        "meetings_booked": 0,
        # Email stats
        "emails_sent": 0,
        "emails_delivered": 0,
        "emails_opened": 0,
        "emails_clicked": 0,
        "emails_replied": 0,
        "emails_bounced": 0,
        # LinkedIn stats
        "linkedin_connections_sent": 0,
        "linkedin_connections_accepted": 0,
        "linkedin_inmails_sent": 0,
        "linkedin_replies": 0,
        # Smart Campaign fields
        "is_smart_campaign": True,
        # Company-first: prospect target is DERIVED from the company count (~3/company),
        # not a user input. Stored as a derived echo so the doc stays self-consistent.
        "prospect_count_target": body.prospect_count_target or (body.curated_company_count_target * 3),
        "icp_industries": body.icp_industries,
        "icp_job_titles": body.icp_job_titles,
        "icp_seniority_levels": body.icp_seniority_levels,
        "icp_company_size_min": body.icp_company_size_min,
        "icp_company_size_max": body.icp_company_size_max,
        "icp_countries": body.icp_countries,
        "icp_apify_params": body.icp_apify_params,
        "icp_industry_label": body.icp_industry_label,
        "icp_keywords": body.icp_keywords,
        "icp_exclude_keywords": body.icp_exclude_keywords,
        "icp_exclude_industries": body.icp_exclude_industries,
        "icp_functional_departments": body.icp_functional_departments,
        "max_prospects_per_company": body.max_prospects_per_company,
        "message_tone": body.message_tone,
        "value_proposition": body.value_proposition,
        "pain_point": body.pain_point,
        "cta_type": body.cta_type,
        "cta_url": body.cta_url,
        "message_guidance": body.message_guidance,
        "email_guidance": body.email_guidance or (body.message_guidance or {}).get("email_guidance"),
        "connection_request_guidance": body.connection_request_guidance or (body.message_guidance or {}).get("connection_request_guidance"),
        "inmail_guidance": body.inmail_guidance or (body.message_guidance or {}).get("inmail_guidance"),
        # Curated discovery mode fields
        "discovery_mode": body.discovery_mode,
        "curated_icp_prompt": body.curated_icp_prompt,
        "curated_company_count_target": body.curated_company_count_target,
        "curated_companies_sourced": 0,
        "curated_companies_approved": 0,
        "curated_companies_scraped": 0,
        # BYOL / Upload-a-Lead-List fields
        "discovery_source": "upload" if is_upload else "discovery",
        "upload_batch_id": body.upload_batch_id if is_upload else None,
        # BYOL enrolls all uploaded leads — never drop for a low fit score
        # (score is display/sort only). Curated mode leaves this None (default 25).
        "discovery_min_enroll_score": 0 if is_upload else None,
        "upload_rows_total": 0,
        "upload_rows_person": 0,
        "upload_rows_company": 0,
        "upload_rows_email_only": 0,
        "upload_rows_unresolvable": 0,
        "upload_unresolvable_rows": [],
        "upload_skipped_rows": [],
        "discovery_status": "queued",
        "discovery_generation": 1,
        "discovery_started_at": None,
        "discovery_completed_at": None,
        "discovery_error": None,
        "discovery_prospects_found": 0,
        "discovery_prospects_enrolled": 0,
        "message_gen_status": "idle",
        "message_gen_started_at": None,
        "message_gen_completed_at": None,
        "message_gen_prospects_done": 0,
        "approval_status": "pending",
        "launched_at": None,
        "launch_day1_date": None,
        # Timestamps
        "created_at": now,
        "updated_at": now,
        "started_at": None,
        "completed_at": None,
    }

    # Auto-populate pain_point / value_proposition from company_profile if not provided
    company_profile = await database.company_profiles_collection.find_one({"account_id": account_id})
    if company_profile:
        pain_points = company_profile.get("pain_points", [])
        value_props = company_profile.get("differentiators", [])
        if pain_points and not doc.get("pain_point"):
            doc["pain_point"] = "; ".join(pain_points) if isinstance(pain_points, list) else str(pain_points)
        if value_props and not doc.get("value_proposition"):
            doc["value_proposition"] = "; ".join(value_props) if isinstance(value_props, list) else str(value_props)

    if body.messaging_config:
        doc["messaging_config"] = body.messaging_config

    # L6: Reject campaigns with no ICP signal at all
    has_any_icp = any([
        body.icp_industries,
        body.icp_keywords if hasattr(body, "icp_keywords") else None,
        body.icp_seniority_levels,
        body.icp_countries,
        getattr(body, "icp_job_titles", None),
    ])
    # Upload campaigns bring their own leads, so no ICP is required. The ICP
    # fields, when present, still drive per-row person-fit gating of scraped
    # company employees.
    if not is_upload and not (body.curated_icp_prompt or has_any_icp):
        raise HTTPException(
            status_code=422,
            detail="At least one ICP field (industries, keywords, seniority, countries, or a company description) must be provided.",
        )

    # M1: Validate send window
    send_hour_start = body.send_hour_start if body.send_hour_start is not None else 9
    send_hour_end = body.send_hour_end if body.send_hour_end is not None else 17
    if send_hour_start >= send_hour_end:
        raise HTTPException(
            status_code=422,
            detail="send_hour_start must be less than send_hour_end",
        )

    # M2: Validate send_days whitelist
    _VALID_DAYS = {"monday", "tuesday", "wednesday", "thursday", "friday", "saturday", "sunday"}
    if body.send_days:
        invalid_days = [d for d in body.send_days if d.lower() not in _VALID_DAYS]
        if invalid_days:
            raise HTTPException(
                status_code=422,
                detail=f"Invalid send_days values: {invalid_days}",
            )

    # Always persist follow_up_flow — default to campaign-type-appropriate preset if not provided
    from services.flow_engine import get_default_flow, validate_flow
    follow_up_flow = body.follow_up_flow or get_default_flow({}, {"type": body.type or "custom"})
    flow_errors = validate_flow(follow_up_flow)
    if flow_errors:
        raise HTTPException(status_code=422, detail={"follow_up_flow_errors": flow_errors})
    doc["follow_up_flow"] = follow_up_flow

    # ── Branching sequence graph (new sequence campaigns only) ───────────────
    # The frontend React Flow editor sends sequence_graph for multi-touch
    # campaigns. Validate against the contract and persist; when absent the
    # campaign uses the legacy single-touch / follow_up_flow path unchanged.
    if body.sequence_graph is not None:
        from services.sequence_service import validate_sequence_graph
        seq_errors = validate_sequence_graph(body.sequence_graph)
        if seq_errors:
            raise HTTPException(status_code=400, detail={"sequence_graph_errors": seq_errors})
        doc["sequence_graph"] = body.sequence_graph
        doc["sequence_contract"] = "sequence_graph_v1"

    # ── Discovery tuning knobs + mock flag (None → absent → module default) ───
    _knob_fields = (
        "discovery_scrape_depth",
        "discovery_dropout_buffer",
        "discovery_enrollment_cap",
        "discovery_sourcing_concurrency",
        "discovery_scrape_concurrency",
        "discovery_enable_company_research",
        "discovery_skip_message_gen",
    )
    for _k in _knob_fields:
        _v = getattr(body, _k, None)
        if _v is not None:
            doc[_k] = _v

    # Mock mode — only honoured outside production (hard safety gate)
    if getattr(body, "discovery_mock_mode", None) and settings.app_env != "production":
        doc["discovery_mock_mode"] = True

    result = await campaigns_collection.insert_one(doc)
    campaign_id = str(result.inserted_id)

    # Log user inputs as the very first entry in the campaign log
    try:
        from services.campaign_discovery_logger import CampaignDiscoveryLogger
        async with CampaignDiscoveryLogger(campaign_id, str(account_id), settings.discovery_log_dir) as _input_log:
            await _input_log.log(
                phase="user_input",
                event="campaign_submitted",
                smart_prompt=getattr(body, "smart_prompt", None),
                chat_history=getattr(body, "chat_history", None),
                captured=getattr(body, "captured", None),
                prospect_count_target=body.prospect_count_target,
                campaign_type=body.type,
                timezone=body.timezone,
                icp_industries=body.icp_industries,
                icp_seniority_levels=body.icp_seniority_levels,
                icp_countries=body.icp_countries,
                icp_job_titles=getattr(body, "icp_job_titles", None),
                icp_keywords=body.icp_keywords,
                icp_exclude_keywords=body.icp_exclude_keywords,
                icp_functional_departments=getattr(body, "icp_functional_departments", None),
                icp_company_size_min=body.icp_company_size_min,
                icp_company_size_max=body.icp_company_size_max,
                cta_type=body.cta_type,
                cta_url=body.cta_url,
                message_tone=body.message_tone,
                value_proposition=body.value_proposition,
                pain_point=body.pain_point,
                email_account_id=str(body.email_account_id) if body.email_account_id else None,
                linkedin_account_id=str(body.linkedin_account_id) if body.linkedin_account_id else None,
                submitted_by_user_id=str(account_ctx["user"]["_id"]),
                submitted_by_email=account_ctx["user"].get("email"),
            )
    except Exception as _log_err:
        logger.warning(f"[Campaign {campaign_id}] Failed to write user input log: {_log_err}")

    await _enqueue_discovery_or_fail(campaign_id, account_id, generation=1)

    created = await campaigns_collection.find_one({"_id": result.inserted_id})
    return {"campaign": serialize_campaign(created)}


class SequencePutRequest(BaseModel):
    """Body for PUT /api/campaigns/{id}/sequence."""
    sequence_graph: dict


@router.get("/{campaign_id}/sequence")
async def get_campaign_sequence(
    campaign_id: str,
    account_ctx: dict = Depends(get_account_context),
):
    """Return the campaign's stored sequence graph.

    When the campaign has no stored graph yet, returns the default aggressive
    template with ``is_default: true`` so the editor can seed the canvas.
    """
    account_id = ObjectId(account_ctx["account"]["_id"])
    campaign = await _get_campaign_or_404(campaign_id, account_id)

    from services.sequence_service import build_default_sequence_graph

    stored = campaign.get("sequence_graph")
    if stored:
        return {"sequence_graph": stored, "is_default": False}
    return {"sequence_graph": build_default_sequence_graph(), "is_default": True}


@router.put("/{campaign_id}/sequence")
async def put_campaign_sequence(
    campaign_id: str,
    body: SequencePutRequest,
    account_ctx: dict = Depends(get_account_context),
):
    """Validate and persist a sequence graph on the campaign.

    Rejects edits while the campaign is active or completed (409). On validation
    failure returns 400 with a per-error list.
    """
    account_id = ObjectId(account_ctx["account"]["_id"])
    campaign = await _get_campaign_or_404(campaign_id, account_id)

    if campaign.get("status") in ("active", "completed"):
        raise HTTPException(
            status_code=409,
            detail="Cannot edit the sequence while the campaign is active or completed.",
        )

    from services.sequence_service import validate_sequence_graph

    errors = validate_sequence_graph(body.sequence_graph)
    if errors:
        raise HTTPException(status_code=400, detail={"errors": errors})

    await campaigns_collection.update_one(
        {"_id": campaign["_id"]},
        {"$set": {
            "sequence_graph": body.sequence_graph,
            "sequence_contract": "sequence_graph_v1",
            "updated_at": datetime.utcnow(),
        }},
    )
    return {"sequence_graph": body.sequence_graph, "is_default": False}


@router.post("/{campaign_id}/discover-prospects")
async def trigger_prospect_discovery(
    campaign_id: str,
    background_tasks: BackgroundTasks,
    account_ctx: dict = Depends(get_account_context),
):
    """
    Trigger (or re-trigger) AI prospect discovery for a smart campaign.
    Returns immediately. Poll /discovery-status to check progress.
    """
    account_id = ObjectId(account_ctx["account"]["_id"])
    doc = await _get_campaign_or_404(campaign_id, account_id)

    if not doc.get("is_smart_campaign"):
        raise HTTPException(status_code=400, detail="This endpoint is only for smart campaigns")

    if doc.get("discovery_status") in ("searching_db", "scraping", "enriching", "scoring", "sourcing_companies", "scraping_employees"):
        raise HTTPException(status_code=409, detail="Discovery is already running")

    has_icp = any([
        doc.get("icp_industries"),
        doc.get("icp_job_titles"),
        doc.get("icp_seniority_levels"),
        doc.get("icp_countries"),
        doc.get("icp_apify_params"),
    ])
    if not has_icp and not doc.get("curated_icp_prompt"):
        raise HTTPException(status_code=400, detail="Campaign must have at least one ICP field set")

    # Atomically advance the generation so each explicit retrigger gets a new
    # deterministic job while concurrent requests cannot share mutable work.
    queued_campaign = await campaigns_collection.find_one_and_update(
        {
            "_id": ObjectId(campaign_id),
            "account_id": account_id,
            "discovery_status": {"$nin": [
                "queued", "searching_db", "scraping", "enriching", "scoring",
                "sourcing_companies", "scraping_employees",
            ]},
        },
        {"$set": {
            "discovery_status": "queued",
            "discovery_error": None,
            "message_gen_status": "idle",
            "updated_at": datetime.utcnow(),
        }, "$inc": {"discovery_generation": 1}},
        return_document=ReturnDocument.AFTER,
    )
    if not queued_campaign:
        raise HTTPException(status_code=409, detail="Discovery is already queued or running")

    try:
        from services.campaign_discovery_logger import CampaignDiscoveryLogger
        async with CampaignDiscoveryLogger(campaign_id, str(account_id), settings.discovery_log_dir) as _input_log:
            await _input_log.log(
                phase="user_input",
                event="discovery_retriggered",
                triggered_by_user_id=str(account_ctx["user"]["_id"]),
                triggered_by_email=account_ctx["user"].get("email"),
            )
    except Exception as _log_err:
        logger.warning(f"[Campaign {campaign_id}] Failed to write retrigger log: {_log_err}")

    await _enqueue_discovery_or_fail(
        campaign_id,
        account_id,
        generation=int(queued_campaign.get("discovery_generation") or 1),
    )

    return {"status": "queued", "campaign_id": campaign_id}


@router.post("/{campaign_id}/scrape-more")
async def scrape_more_prospects(
    campaign_id: str,
    body: ScrapeMoreRequest,
    background_tasks: BackgroundTasks,
    account_ctx: dict = Depends(get_account_context),
):
    """Re-run prospect discovery with edited ICP params and auto-enroll the results.

    Only provided fields overwrite the campaign's ICP. Per-run discovery counters
    are reset so the UI shows just-this-run progress. Existing enrollments in this
    campaign are deduplicated by the service.
    """
    account_id = ObjectId(account_ctx["account"]["_id"])
    doc = await _get_campaign_or_404(campaign_id, account_id)

    if not doc.get("is_smart_campaign"):
        raise HTTPException(status_code=400, detail="This endpoint is only for smart campaigns")

    if doc.get("discovery_status") in ("searching_db", "scraping", "enriching", "scoring"):
        raise HTTPException(status_code=409, detail="Discovery is already running")

    _ALLOWED_ICP_FIELDS = {
        "icp_industry_label", "icp_industries", "icp_keywords", "icp_exclude_keywords",
        "icp_job_titles", "icp_seniority_levels", "icp_countries",
        "icp_company_size_min", "icp_company_size_max", "prospect_count_target",
    }
    icp_updates = {k: v for k, v in body.model_dump(exclude_none=True).items() if k in _ALLOWED_ICP_FIELDS}

    # Require at least one ICP signal after merge (either in the override or already on the doc).
    has_icp = any([
        icp_updates.get("icp_industries") or doc.get("icp_industries"),
        icp_updates.get("icp_job_titles") or doc.get("icp_job_titles"),
        icp_updates.get("icp_seniority_levels") or doc.get("icp_seniority_levels"),
        icp_updates.get("icp_countries") or doc.get("icp_countries"),
        doc.get("icp_apify_params"),
    ])
    if not has_icp:
        raise HTTPException(status_code=400, detail="Campaign must have at least one ICP field set")

    now = datetime.utcnow()
    updates: dict = {**icp_updates, "updated_at": now}

    # Reset per-run counters so the UI reflects this run's deltas, not cumulative totals.
    updates.update({
        "discovery_status": "queued",
        "discovery_error": None,
        "discovery_failure_reason": None,
        "discovery_prospects_found": 0,
        "discovery_prospects_enrolled": 0,
        "discovery_prospects_scraped": 0,
        "discovery_prospects_from_db": 0,
        "discovery_prospects_from_apify": 0,
        "discovery_companies_found": 0,
        "discovery_apify_triggered": False,
        "discovery_enrichment_total": 0,
        "discovery_enrichment_done": 0,
        "discovery_enrichment_failed": 0,
        "message_gen_status": "idle",
        "message_gen_prospects_done": 0,
    })

    queued_campaign = await campaigns_collection.find_one_and_update(
        {
            "_id": ObjectId(campaign_id),
            "account_id": account_id,
            "discovery_status": {"$nin": [
                "queued", "searching_db", "scraping", "enriching", "scoring",
                "sourcing_companies", "scraping_employees",
            ]},
        },
        {"$set": updates, "$inc": {"discovery_generation": 1}},
        return_document=ReturnDocument.AFTER,
    )
    if not queued_campaign:
        raise HTTPException(status_code=409, detail="Discovery is already queued or running")

    try:
        from services.campaign_discovery_logger import CampaignDiscoveryLogger
        async with CampaignDiscoveryLogger(campaign_id, str(account_id), settings.discovery_log_dir) as _input_log:
            await _input_log.log(
                phase="user_input",
                event="icp_updated_and_retriggered",
                icp_changes=icp_updates,
                triggered_by_user_id=str(account_ctx["user"]["_id"]),
                triggered_by_email=account_ctx["user"].get("email"),
            )
    except Exception as _log_err:
        logger.warning(f"[Campaign {campaign_id}] Failed to write ICP update log: {_log_err}")

    await _enqueue_discovery_or_fail(
        campaign_id,
        account_id,
        generation=int(queued_campaign.get("discovery_generation") or 1),
    )

    return {"status": "queued", "campaign_id": campaign_id}


# ---------------------------------------------------------------------------
# Curated discovery: sourced-companies review endpoints
# ---------------------------------------------------------------------------

@router.get("/{campaign_id}/sourced-companies")
async def list_sourced_companies(
    campaign_id: str,
    account_ctx: dict = Depends(get_account_context),
):
    """List companies sourced for a curated-mode campaign (for the approval UI)."""
    account_id = ObjectId(account_ctx["account"]["_id"])
    doc = await _get_campaign_or_404(campaign_id, account_id)
    if doc.get("discovery_mode") != "curated":
        raise HTTPException(status_code=400, detail="Not a curated-mode campaign")

    cursor = database.sourced_companies_collection.find(
        {"campaign_id": campaign_id}
    ).sort("user_excluded", 1)
    items = await cursor.to_list(length=None)
    for it in items:
        it["_id"] = str(it["_id"])
    return {
        "companies": items,
        "total": len(items),
        "approved_count": sum(1 for c in items if not c.get("user_excluded")),
        "excluded_count": sum(1 for c in items if c.get("user_excluded")),
        "discovery_status": doc.get("discovery_status"),
    }


class SourcedCompanyPatchRequest(BaseModel):
    user_excluded: Optional[bool] = None
    company_linkedin_url: Optional[str] = None


@router.patch("/{campaign_id}/sourced-companies/{company_id}")
async def patch_sourced_company(
    campaign_id: str,
    company_id: str,
    body: SourcedCompanyPatchRequest,
    account_ctx: dict = Depends(get_account_context),
):
    """Toggle exclusion or fix the LinkedIn URL for one sourced company."""
    account_id = ObjectId(account_ctx["account"]["_id"])
    doc = await _get_campaign_or_404(campaign_id, account_id)
    if doc.get("discovery_mode") != "curated":
        raise HTTPException(status_code=400, detail="Not a curated-mode campaign")
    if doc.get("discovery_status") in ("sourcing_companies", "scraping_employees"):
        raise HTTPException(status_code=409, detail="Discovery is still in progress; wait until it completes.")

    set_doc: dict = {"updated_at": datetime.utcnow()}
    if body.user_excluded is not None:
        set_doc["user_excluded"] = body.user_excluded
    if body.company_linkedin_url is not None:
        from services.company_sourcing_service import _normalize_linkedin_url
        normalized = _normalize_linkedin_url(body.company_linkedin_url)
        if not normalized:
            raise HTTPException(status_code=422, detail="LinkedIn URL must be a /company/ URL")
        set_doc["company_linkedin_url"] = normalized
        set_doc["linkedin_url_validated"] = True

    result = await database.sourced_companies_collection.update_one(
        {"_id": ObjectId(company_id), "campaign_id": campaign_id},
        {"$set": set_doc},
    )
    if result.matched_count == 0:
        raise HTTPException(status_code=404, detail="Sourced company not found")
    return {"updated": True}


@router.post("/{campaign_id}/sourced-companies/approve")
async def approve_sourced_companies(
    campaign_id: str,
    account_ctx: dict = Depends(get_account_context),
):
    """Legacy endpoint — curated discovery now runs end-to-end without a mid-flow gate."""
    raise HTTPException(
        status_code=410,
        detail="Company approval step no longer required. Discovery runs end-to-end automatically.",
    )


class ApproveIndustriesRequest(BaseModel):
    industry_ids: list[str]


class BulkSourcedCompaniesRequest(BaseModel):
    company_ids: list[str]
    user_excluded: bool


@router.post("/{campaign_id}/sourced-companies/bulk")
async def bulk_patch_sourced_companies(
    campaign_id: str,
    body: BulkSourcedCompaniesRequest,
    account_ctx: dict = Depends(get_account_context),
):
    """Bulk include/exclude sourced companies in one round-trip."""
    account_id = ObjectId(account_ctx["account"]["_id"])
    await _get_campaign_or_404(campaign_id, account_id)

    oids = []
    for cid in body.company_ids:
        try:
            oids.append(ObjectId(cid))
        except Exception:
            continue

    if not oids:
        raise HTTPException(status_code=400, detail="No valid company_ids provided")

    result = await database.sourced_companies_collection.update_many(
        {"_id": {"$in": oids}, "campaign_id": campaign_id},
        {"$set": {"user_excluded": body.user_excluded, "updated_at": datetime.utcnow()}},
    )

    return {
        "matched_count": result.matched_count,
        "modified_count": result.modified_count,
    }


@router.post("/{campaign_id}/industries/approve")
async def approve_industry_expansions(
    campaign_id: str,
    body: ApproveIndustriesRequest,
    account_ctx: dict = Depends(get_account_context),
):
    """
    Merge user-approved industry ids (previously surfaced as
    `suggested_industry_ids` from a loose group expansion — see
    services/industry_canonicalizer.suggest_industry_expansions) into the
    campaign's strict `industry_ids`.

    Approved ids are validated against the canonical industries_taxonomy
    table before being merged, removed from `suggested_industry_ids`, and
    the request is a no-op (not an error) for ids already present in either
    list or not currently suggested.
    """
    from services.industry_canonicalizer import get_taxonomy_entry

    account_id = ObjectId(account_ctx["account"]["_id"])
    campaign = await _get_campaign_or_404(campaign_id, account_id)

    requested_ids = [iid for iid in body.industry_ids if iid and iid.strip()]
    if not requested_ids:
        raise HTTPException(status_code=400, detail="industry_ids must not be empty")

    # Validate each id against the canonical taxonomy: static list first,
    # falling back to the DB collection for entries not in the static seed.
    valid_ids: list[str] = []
    unknown_ids: list[str] = []
    for industry_id in requested_ids:
        if get_taxonomy_entry(industry_id) is not None:
            valid_ids.append(industry_id)
            continue
        db_entry = await database.industries_taxonomy_collection.find_one(
            {"industry_id": industry_id}
        )
        if db_entry is not None:
            valid_ids.append(industry_id)
        else:
            unknown_ids.append(industry_id)

    if unknown_ids:
        raise HTTPException(
            status_code=400,
            detail=f"Unknown industry_id(s): {', '.join(unknown_ids)}",
        )

    existing_industry_ids: list[str] = list(campaign.get("industry_ids") or [])
    existing_suggested: list[str] = list(campaign.get("suggested_industry_ids") or [])

    merged_industry_ids = list(existing_industry_ids)
    for industry_id in valid_ids:
        if industry_id not in merged_industry_ids:
            merged_industry_ids.append(industry_id)

    remaining_suggested = [sid for sid in existing_suggested if sid not in valid_ids]

    update_fields: dict = {
        "industry_ids": merged_industry_ids,
        "suggested_industry_ids": remaining_suggested,
    }

    # ── Supplemental discovery decision ─────────────────────────────────────
    # Discovery runs exactly once, in run_fast_discovery(), at campaign creation.
    # If that has already completed, merging new industry_ids alone changes
    # nothing — no company sourcing/scraping/enrollment ever happens for the
    # newly-approved industries unless we do something here.
    #
    # We do NOT re-invoke run_fast_discovery() to fill the gap. That function
    # is a single monolithic, non-parameterized pass: it unconditionally wipes
    # `sourced_companies` for the whole campaign (delete_many at its top),
    # re-sources/re-scores/re-scrapes companies for the campaign's ENTIRE
    # industry_ids list (old + new, not just the newly-approved ones), and —
    # if any prospects end up assigned to send-day 1 — fires a background task
    # that regenerates Day-1 messages for every day-1 "active" enrollment,
    # including ones from the original run. `_upsert_curated_prospect` (upsert
    # by linkedin/email) and `_pre_enroll_prospects` (skips prospect_ids
    # already in `campaign_enrollments` for this campaign, see
    # services/prospect_enrollment_service.py `already_enrolled`/
    # `cross_enrolled` sets) do prevent duplicate prospect docs and duplicate
    # *enrollment* documents. But re-running the full pipeline is not scoped
    # to "the newly-approved industries only": it re-spends Apify/Gemini
    # budget re-processing industries that were already handled, discards the
    # sourced_companies audit trail for the whole campaign, and risks
    # clobbering already-generated Day-1 message content for existing
    # enrollees. That is not a safe additive rerun for this endpoint, so we
    # do not take it.
    #
    # Instead: mark the campaign as needing a (future, properly-scoped)
    # supplemental discovery pass and return that decision explicitly. Lazy
    # canonicalization already covers the two cases where no extra action is
    # needed: discovery hasn't completed yet (still running, or the campaign
    # is a pre-discovery draft) — the in-flight/next run will naturally pick
    # up the merged industry_ids — or the campaign is already active/completed,
    # where kicking off new sourcing would be surprising and out of scope here.
    discovery_status = campaign.get("discovery_status")
    campaign_status = campaign.get("status")
    rediscovery = "not_needed"
    if discovery_status == "completed" and campaign_status not in ("active", "completed"):
        existing_pending = list(campaign.get("pending_rediscovery_industry_ids") or [])
        pending_ids = list(dict.fromkeys(existing_pending + valid_ids))
        update_fields["pending_rediscovery"] = True
        update_fields["pending_rediscovery_industry_ids"] = pending_ids
        rediscovery = "deferred"

    await campaigns_collection.update_one(
        {"_id": campaign["_id"]},
        {"$set": update_fields},
    )

    return {
        "campaign_id": campaign_id,
        "industry_ids": merged_industry_ids,
        "suggested_industry_ids": remaining_suggested,
        "rediscovery": rediscovery,
    }


@router.get("/{campaign_id}/discovery-status")
async def get_discovery_status(
    campaign_id: str,
    account_ctx: dict = Depends(get_account_context),
):
    """Poll discovery and message generation status for a smart campaign."""
    account_id = ObjectId(account_ctx["account"]["_id"])
    doc = await _get_campaign_or_404(campaign_id, account_id)
    campaign_oid = ObjectId(campaign_id)

    # Compute enrichment counts from enrolled prospects
    enrollment_cursor = database.campaign_enrollments_collection.find(
        {"campaign_id": campaign_oid},
        {"prospect_id": 1},
    )
    enrollment_docs = await enrollment_cursor.to_list(length=2000)
    enrolled_ids = [e["prospect_id"] for e in enrollment_docs if e.get("prospect_id")]

    enrichment_done_count = 0
    if enrolled_ids:
        enrichment_done_count = await prospects_collection.count_documents({
            "_id": {"$in": enrolled_ids},
            "enrichment_status": "completed",
        })

    discovery_status_val = doc.get("discovery_status", "idle")
    # Live $inc counters are authoritative; prospect-doc recompute is a fallback for old campaigns.
    campaign_enr_total = doc.get("discovery_enrichment_total", 0)
    if campaign_enr_total > 0:
        enr_done = doc.get("discovery_enrichment_done", 0)
        enr_total = campaign_enr_total
    else:
        enr_done = enrichment_done_count
        enr_total = len(enrolled_ids)

    # Count failed message generations so the UI can surface a "Regenerate N
    # failed" prompt without a second round-trip.
    message_gen_failed_count = await campaign_enrollments_collection.count_documents({
        "campaign_id": campaign_oid,
        "message_gen_status": "failed",
    })

    from services.campaign_launch_service import compute_day1_preview
    day1_date, day1_is_today = compute_day1_preview(doc)

    return {
        "campaign_id": campaign_id,
        "discovery_status": discovery_status_val,
        "discovery_error": doc.get("discovery_error"),
        "discovery_prospects_found": doc.get("discovery_prospects_found", 0),
        "discovery_prospects_enrolled": doc.get("discovery_prospects_enrolled", 0),
        "discovery_companies_found": doc.get("discovery_companies_found", 0),
        # DB-first visibility: companies matched from the shared pool (Stage A).
        "discovery_companies_matched": doc.get("discovery_companies_matched", 0),
        # Curated (company-first) counters the review UI reads directly.
        "curated_companies_sourced": doc.get("curated_companies_sourced", 0),
        "curated_companies_approved": doc.get("curated_companies_approved", 0),
        "curated_companies_scraped": doc.get("curated_companies_scraped", 0),
        # Why-0-enrolled visibility.
        "discovery_skip_reasons": doc.get("discovery_skip_reasons") or {},
        "discovery_apify_triggered": doc.get("discovery_apify_triggered", False),
        "discovery_prospects_from_db": doc.get("discovery_prospects_from_db", 0),
        "discovery_prospects_from_apify": doc.get("discovery_prospects_from_apify", 0),
        "message_gen_status": doc.get("message_gen_status", "idle"),
        "message_gen_prospects_done": doc.get("message_gen_prospects_done", 0),
        "message_gen_failed_count": message_gen_failed_count,
        "discovery_day1_enrolled": doc.get("discovery_day1_enrolled", 0),
        "total_enrolled": doc.get("total_enrolled", 0),
        # Auto top-up: discovery re-runs itself when yield < target (see
        # curated_discovery_service). Surfaces a "finding more prospects…" banner.
        "discovery_topup_active": doc.get("discovery_topup_active", False),
        "discovery_topup_message": doc.get("discovery_topup_message"),
        "prospect_count_target": doc.get("prospect_count_target", 0),
        "approval_status": doc.get("approval_status", "pending"),
        "auto_launch_status": doc.get("auto_launch_status", "idle"),
        "auto_launch_error": doc.get("auto_launch_error"),
        # Legacy computed counts kept for backwards compat
        "enrichment_completed_count": enr_done,
        "enrichment_total_count": enr_total,
        "discovery_prospects_scraped": doc.get("discovery_prospects_scraped", 0),
        "discovery_enrichment_failed": doc.get("discovery_enrichment_failed", 0),
        "discovery_failure_reason": doc.get("discovery_failure_reason"),
        "discovery_prospects_rule_scored": doc.get("discovery_prospects_rule_scored", 0),
        "discovery_prospects_rule_scored_total": doc.get("discovery_prospects_rule_scored_total", 0),
        "discovery_prospects_total_seen": doc.get("discovery_prospects_total_seen", 0),
        "discovery_prospects_eligible": doc.get("discovery_prospects_eligible"),
        "discovery_prospects_planned": doc.get("discovery_prospects_planned"),
        # AI enrichment progress — incremented by _run_ai_enrichment_background
        "ai_enrichment_done": doc.get("ai_enrichment_done", 0),
        "ai_enrichment_total": doc.get("ai_enrichment_total", 0),
        # Day-1 send date preview
        "day1_date": day1_date.isoformat() if day1_date else None,
        "day1_is_today": bool(day1_is_today),
        "timezone": doc.get("timezone", "America/New_York"),
        # ── Upload-a-Lead-List (BYOL) counters + review arrays ──
        "discovery_source": doc.get("discovery_source", "discovery"),
        "upload_batch_id": str(doc["upload_batch_id"]) if doc.get("upload_batch_id") else None,
        "upload_rows_total": doc.get("upload_rows_total", 0),
        "upload_rows_person": doc.get("upload_rows_person", 0),
        "upload_rows_company": doc.get("upload_rows_company", 0),
        "upload_rows_email_only": doc.get("upload_rows_email_only", 0),
        "upload_rows_unresolvable": doc.get("upload_rows_unresolvable", 0),
        "upload_unresolvable_rows": doc.get("upload_unresolvable_rows") or [],
        "upload_skipped_rows": doc.get("upload_skipped_rows") or [],
    }


@router.get("/{campaign_id}/enrolled-prospects")
async def list_enrolled_prospects(
    campaign_id: str,
    page: int = Query(1, ge=1),
    page_size: int = Query(20, ge=1, le=200),
    sort: str = Query("score_desc", pattern="^(score_desc|score_asc|enrolled_at|day_asc)$"),
    channel_filter: Optional[str] = Query(
        None, pattern="^(email|linkedin_connection|linkedin_inmail|skipped|day_1|day_2_plus)?$"
    ),
    include_skipped: bool = Query(False),
    status_filter: Optional[str] = None,
    has_email: Optional[bool] = Query(None),
    has_linkedin: Optional[bool] = Query(None),
    enriched: Optional[bool] = Query(None),
    company: Optional[str] = Query(None),
    account_ctx: dict = Depends(get_account_context),
):
    """
    Paginated list of enrolled prospects for the campaign review UI.

    Returns every enrollment in the campaign (not just Day-1) with per-row
    fields the UI needs: score, priority_tier, channel, send_day, scheduled_at,
    message_gen_status. Supports sorting and channel/day filtering so the UI
    doesn't have to client-side filter a large result set.

    include_skipped=True additionally surfaces prospects that were scraped for
    this campaign but filtered out as low-fit, letting users see who was
    excluded and why.

    status_filter filters by enrollment status (e.g. active, replied, bounced, opted_out).
    """
    account_id = ObjectId(account_ctx["account"]["_id"])
    await _get_campaign_or_404(campaign_id, account_id)

    campaign_oid = ObjectId(campaign_id)
    skip = (page - 1) * page_size

    match: dict = {"campaign_id": campaign_oid}
    if status_filter:
        match["status"] = status_filter
    if channel_filter == "email":
        match["smart_campaign_channel"] = "email"
    elif channel_filter == "linkedin_connection":
        match["smart_campaign_channel"] = "linkedin_connection"
    elif channel_filter == "linkedin_inmail":
        match["smart_campaign_channel"] = "linkedin_inmail"
    elif channel_filter == "day_1":
        match["smart_campaign_send_day"] = 1
    elif channel_filter == "day_2_plus":
        match["smart_campaign_send_day"] = {"$gt": 1}
    # "skipped" filter is handled via the include_skipped union below

    # Sort order
    if sort == "score_desc":
        sort_stage = {"ai_prospect_score_effective": -1, "enrolled_at": -1}
    elif sort == "score_asc":
        sort_stage = {"ai_prospect_score_effective": 1, "enrolled_at": -1}
    elif sort == "day_asc":
        sort_stage = {"smart_campaign_send_day_effective": 1, "ai_prospect_score_effective": -1}
    else:
        sort_stage = {"enrolled_at": -1}

    # campaign_prospect_state join key: the enrollment stores account_id/prospect_id
    # as ObjectId, so stringify them for the match; campaign_id is the string route
    # param. Highest scoring_version wins (most recent scoring pass).
    cps_lookup = {
        "$lookup": {
            "from": "campaign_prospect_state",
            "let": {
                "pid": {"$toString": "$prospect_id"},
                "aid": {"$toString": "$account_id"},
            },
            "pipeline": [
                {
                    "$match": {
                        "$expr": {
                            "$and": [
                                {"$eq": ["$prospect_id", "$$pid"]},
                                {"$eq": ["$account_id", "$$aid"]},
                                {"$eq": ["$campaign_id", campaign_id]},
                            ]
                        }
                    }
                },
                {"$sort": {"scoring_version": -1}},
                {"$limit": 1},
            ],
            "as": "cps_data",
        }
    }

    # Filters that depend on the joined docs (email/linkedin from the shared prospect
    # doc, enriched from the cps overlay, company from either) must run AFTER the
    # $lookups. Absent-truthiness: {$in: [None, ""]} matches null/missing/"".
    post_lookup_match: dict = {}
    if has_email is not None:
        post_lookup_match["prospect_data.email"] = (
            {"$nin": [None, ""]} if has_email else {"$in": [None, ""]}
        )
    if has_linkedin is not None:
        post_lookup_match["prospect_data.linkedin"] = (
            {"$nin": [None, ""]} if has_linkedin else {"$in": [None, ""]}
        )
    if enriched is not None:
        post_lookup_match["cps_data.enrichment.state"] = (
            "succeeded" if enriched else {"$ne": "succeeded"}
        )
    if company:
        post_lookup_match["$or"] = [
            {"prospect_data.company_id": company},
            {"prospect_data.company_name": company},
        ]

    base_stages: list = [
        {"$match": match},
        {
            "$lookup": {
                "from": "prospects",
                "localField": "prospect_id",
                "foreignField": "_id",
                "as": "prospect_data",
            }
        },
        {"$unwind": {"path": "$prospect_data", "preserveNullAndEmptyArrays": True}},
        {
            "$lookup": {
                "from": "prospect_state",
                "let": {
                    "pid": {"$toString": "$prospect_id"},
                    "aid": {"$toString": "$account_id"},
                },
                "pipeline": [
                    {
                        "$match": {
                            "$expr": {
                                "$and": [
                                    {"$eq": ["$prospect_id", "$$pid"]},
                                    {"$eq": ["$account_id", "$$aid"]},
                                ]
                            }
                        }
                    },
                    {"$limit": 1},
                ],
                "as": "state_data",
            }
        },
        {"$addFields": {"state_data": {"$arrayElemAt": ["$state_data", 0]}}},
        cps_lookup,
        {"$addFields": {"cps_data": {"$arrayElemAt": ["$cps_data", 0]}}},
        # Compute sort-friendly effective values so we can sort by score with
        # null coalescing (unscored → -1) instead of mongo's default null
        # sorting behaviour which interleaves them with low scores. The campaign
        # fit score (campaign_prospect_state.score.value) is the primary signal so
        # the "Fit" column is populated for everyone; it coalesces to the
        # enrollment rule score, then the tenant-scoped ai_score, then -1.
        {
            "$addFields": {
                "ai_prospect_score_effective": {
                    "$ifNull": [
                        "$cps_data.score.value",
                        {
                            "$ifNull": [
                                "$campaign_rule_score",
                                {"$ifNull": ["$state_data.ai_score", -1]},
                            ]
                        },
                    ]
                },
                "smart_campaign_send_day_effective": {
                    "$ifNull": ["$smart_campaign_send_day", 9999]
                },
            }
        },
    ]
    if post_lookup_match:
        base_stages.append({"$match": post_lookup_match})

    # total must reflect post-lookup filters, so count through the same base pipeline
    # (a plain count_documents(match) would ignore email/linkedin/enriched/company).
    count_res = await campaign_enrollments_collection.aggregate(
        base_stages + [{"$count": "n"}]
    ).to_list(length=1)
    total = count_res[0]["n"] if count_res else 0

    pipeline = base_stages + [
        {"$sort": sort_stage},
        {"$skip": skip},
        {"$limit": page_size},
        {
            "$project": {
                "_id": 0,
                "enrollment_id": {"$toString": "$_id"},
                "prospect_id": {"$toString": "$prospect_id"},
                "message_gen_status": 1,
                "message_gen_error": 1,
                "message_gen_attempts": {"$ifNull": ["$message_gen_attempts", 0]},
                "smart_campaign_channel": 1,
                "smart_campaign_send_day": 1,
                "smart_campaign_scheduled_utc": 1,
                "enrolled_at": 1,
                "status": 1,
                "has_generated_message": {
                    "$cond": [{"$ifNull": ["$generated_messages", False]}, True, False]
                },
                "full_name": "$prospect_data.full_name",
                "first_name": "$prospect_data.first_name",
                "job_title": "$prospect_data.job_title",
                "company_name": "$prospect_data.company_name",
                "industry": "$prospect_data.industry",
                "country": "$prospect_data.country",
                "seniority_level": "$prospect_data.seniority_level",
                # Score/tier: the campaign fit score (campaign_prospect_state overlay)
                # is the authoritative per-campaign fit; ai_prospect_score (tenant
                # prospect_state) and campaign_rule_score are also projected so the
                # frontend can choose which to display. Tier prefers the cps overlay,
                # falling back to the tenant prospect_state tier.
                "ai_prospect_score": "$state_data.ai_score",
                "campaign_fit_score": "$cps_data.score.value",
                "priority_tier": {
                    "$ifNull": [
                        "$cps_data.score.priority_tier",
                        "$state_data.priority_tier",
                    ]
                },
                # campaign_rule_score lives on the enrollment (rule-based fit
                # snapshot), not the shared pool — retained.
                "campaign_rule_score": 1,
                "linkedin": "$prospect_data.linkedin",
                "email": "$prospect_data.email",
                "has_email": {"$cond": [{"$ifNull": ["$prospect_data.email", False]}, True, False]},
                "has_linkedin": {"$cond": [{"$ifNull": ["$prospect_data.linkedin", False]}, True, False]},
                # Enrichment status reads the campaign_prospect_state overlay
                # (enrichment.state) first — succeeded→"completed" — falling back to
                # the shared prospect doc's enrichment_status only when the overlay
                # has no state yet.
                "enrichment_status": {
                    "$cond": [
                        {"$eq": ["$cps_data.enrichment.state", "succeeded"]},
                        "completed",
                        {"$ifNull": [
                            "$cps_data.enrichment.state",
                            "$prospect_data.enrichment_status",
                        ]},
                    ]
                },
                "enrichment_completed_at": "$prospect_data.enrichment_completed_at",
            }
        },
    ]

    rows = await campaign_enrollments_collection.aggregate(pipeline).to_list(length=page_size)

    # Optionally surface scraped-but-skipped prospects (source="search", tagged to this
    # campaign's industry but never enrolled). Only join them on page 1 since they are
    # a separate data set and the UI renders them in a different section.
    skipped_rows: list = []
    skipped_total = 0
    if include_skipped and page == 1 and channel_filter in (None, "skipped"):
        # Find prospects tagged via the campaign's auto-created industry that have
        # no enrollment in this campaign — these were scraped and rejected.
        enrolled_ids_cursor = campaign_enrollments_collection.find(
            {"campaign_id": campaign_oid}, {"prospect_id": 1}
        )
        enrolled_pids = [d["prospect_id"] async for d in enrolled_ids_cursor]

        camp_doc = await database.campaigns_collection.find_one(
            {"_id": campaign_oid},
            {"icp_industries": 1, "_id": 1},
        )
        industry_ids_cursor = database.industries_collection.find(
            {"source_campaign_id": campaign_oid}, {"_id": 1}
        )
        industry_id_strs = [str(d["_id"]) async for d in industry_ids_cursor]

        skipped_filter: dict = {"_id": {"$nin": enrolled_pids}}
        if industry_id_strs:
            skipped_filter["$or"] = [
                {"industry_id": {"$in": industry_id_strs}},
                {"source_industry_ids": {"$in": industry_id_strs}},
            ]
        else:
            # No industry tag — fall back to industry name match
            industries = (camp_doc or {}).get("icp_industries") or []
            if industries:
                skipped_filter["industry"] = {"$in": industries}
            else:
                skipped_filter = None  # nothing useful to match on

        if skipped_filter is not None:
            skipped_total = await prospects_collection.count_documents(skipped_filter)
            # Pull tenant-neutral canonical fields only from the shared pool.
            skipped_prospects = await prospects_collection.find(
                skipped_filter,
                {
                    "_id": 1,
                    "full_name": 1,
                    "first_name": 1,
                    "job_title": 1,
                    "company_name": 1,
                    "industry": 1,
                    "country": 1,
                    "seniority_level": 1,
                    "enrichment_status": 1,
                    "enrichment_error": 1,
                },
            ).limit(200).to_list(length=200)

            # Overlay campaign/tenant-scoped scores from prospect_state (the same
            # collection the enrollment ranking trusts) instead of reading legacy
            # shared-pool score copies.
            account_id_str = str(account_ctx["account"]["_id"])
            skipped_pids = [str(p["_id"]) for p in skipped_prospects]
            score_by_pid: dict = {}
            if skipped_pids:
                async for st in database.prospect_state_collection.find(
                    {"account_id": account_id_str, "prospect_id": {"$in": skipped_pids}},
                    {"prospect_id": 1, "ai_score": 1, "priority_tier": 1},
                ):
                    score_by_pid[st["prospect_id"]] = st

            def _skipped_score(pid: str):
                return score_by_pid.get(pid, {}).get("ai_score")

            # Highest-fit skipped prospects first; unscored (null) sort last.
            skipped_prospects.sort(
                key=lambda p: (
                    _skipped_score(str(p["_id"])) is not None,
                    _skipped_score(str(p["_id"])) if _skipped_score(str(p["_id"])) is not None else 0,
                ),
                reverse=True,
            )
            for p in skipped_prospects[:50]:
                pid = str(p["_id"])
                st = score_by_pid.get(pid, {})
                skipped_rows.append({
                    "prospect_id": pid,
                    "enrollment_id": None,
                    "status": "skipped_low_fit",
                    "smart_campaign_channel": None,
                    "smart_campaign_send_day": None,
                    "full_name": p.get("full_name"),
                    "first_name": p.get("first_name"),
                    "job_title": p.get("job_title"),
                    "company_name": p.get("company_name"),
                    "industry": p.get("industry"),
                    "country": p.get("country"),
                    "seniority_level": p.get("seniority_level"),
                    "ai_prospect_score": st.get("ai_score"),
                    "priority_tier": st.get("priority_tier"),
                    "enrichment_status": p.get("enrichment_status"),
                    "enrichment_error": p.get("enrichment_error"),
                    "prospect_score": st.get("ai_score"),
                    "message_gen_status": None,
                    "has_generated_message": False,
                })

    return {
        "prospects": rows,
        "total": total,
        "page": page,
        "page_size": page_size,
        "pages": (total + page_size - 1) // page_size,
        "sort": sort,
        "channel_filter": channel_filter,
        "status_filter": status_filter,
        "skipped_prospects": skipped_rows,
        "skipped_total": skipped_total,
    }


@router.get("/{campaign_id}/enrolled-prospects/stats")
async def enrolled_prospects_stats(
    campaign_id: str,
    account_ctx: dict = Depends(get_account_context),
):
    """
    Aggregate stats over ALL campaign_enrollments for a campaign (not paginated).

    Semantics match list_enrolled_prospects: every enrollment counts toward the
    main totals regardless of enrollment status (scraped-but-skipped prospects
    have no enrollment document at all, so — like the list's main result set —
    they are excluded here; enrollment statuses are broken out in by_status).

    Response contract (frontend builds against this exactly):
      {
        "total": int,
        "with_email": int,       # prospect.email truthy (same as list's has_email)
        "with_linkedin": int,    # prospect.linkedin truthy (same as list's has_linkedin)
        "enriched": int,         # prospect.enrichment_status == "completed"
        "messages_ready": int,   # enrollment.message_gen_status == "completed"
        "by_status": {"active": n, "replied": n, "bounced": n, "opted_out": n, ...}
      }
    """
    account_id = ObjectId(account_ctx["account"]["_id"])
    await _get_campaign_or_404(campaign_id, account_id)

    campaign_oid = ObjectId(campaign_id)

    pipeline = [
        {"$match": {"campaign_id": campaign_oid}},
        {
            "$facet": {
                # Summary counts need the shared-pool prospect doc for
                # email/linkedin/enrichment_status — same $lookup the list
                # endpoint uses.
                "summary": [
                    {
                        "$lookup": {
                            "from": "prospects",
                            "localField": "prospect_id",
                            "foreignField": "_id",
                            "as": "prospect_data",
                        }
                    },
                    {
                        "$unwind": {
                            "path": "$prospect_data",
                            "preserveNullAndEmptyArrays": True,
                        }
                    },
                    # Join the campaign_prospect_state overlay so "enriched" reflects
                    # the per-campaign enrichment.state (not the shared prospect doc's
                    # enrichment_status). Same join key as the list endpoint.
                    {
                        "$lookup": {
                            "from": "campaign_prospect_state",
                            "let": {
                                "pid": {"$toString": "$prospect_id"},
                                "aid": {"$toString": "$account_id"},
                            },
                            "pipeline": [
                                {
                                    "$match": {
                                        "$expr": {
                                            "$and": [
                                                {"$eq": ["$prospect_id", "$$pid"]},
                                                {"$eq": ["$account_id", "$$aid"]},
                                                {"$eq": ["$campaign_id", campaign_id]},
                                            ]
                                        }
                                    }
                                },
                                {"$sort": {"scoring_version": -1}},
                                {"$limit": 1},
                            ],
                            "as": "cps_data",
                        }
                    },
                    {"$addFields": {"cps_data": {"$arrayElemAt": ["$cps_data", 0]}}},
                    # Company grouping key: prefer the shared-pool company_id, fall
                    # back to company_name so companies without a canonical id still
                    # count distinctly.
                    {
                        "$addFields": {
                            "company_key": {
                                "$ifNull": [
                                    "$prospect_data.company_id",
                                    "$prospect_data.company_name",
                                ]
                            }
                        }
                    },
                    {
                        "$group": {
                            "_id": None,
                            "total": {"$sum": 1},
                            # Truthiness mirrors the list endpoint's has_email /
                            # has_linkedin projection ($ifNull inside $cond:
                            # null/missing/"" all count as absent).
                            "with_email": {
                                "$sum": {
                                    "$cond": [
                                        {"$ifNull": ["$prospect_data.email", False]},
                                        1,
                                        0,
                                    ]
                                }
                            },
                            "with_linkedin": {
                                "$sum": {
                                    "$cond": [
                                        {"$ifNull": ["$prospect_data.linkedin", False]},
                                        1,
                                        0,
                                    ]
                                }
                            },
                            "enriched": {
                                "$sum": {
                                    "$cond": [
                                        {
                                            "$eq": [
                                                "$cps_data.enrichment.state",
                                                "succeeded",
                                            ]
                                        },
                                        1,
                                        0,
                                    ]
                                }
                            },
                            "messages_ready": {
                                "$sum": {
                                    "$cond": [
                                        {"$eq": ["$message_gen_status", "completed"]},
                                        1,
                                        0,
                                    ]
                                }
                            },
                            "companies_set": {"$addToSet": "$company_key"},
                        }
                    },
                ],
                # Enrollment-status breakdown (no prospect lookup needed).
                "by_status": [
                    {
                        "$group": {
                            "_id": {"$ifNull": ["$status", "unknown"]},
                            "n": {"$sum": 1},
                        }
                    }
                ],
            }
        },
    ]

    facets = await campaign_enrollments_collection.aggregate(pipeline).to_list(length=1)
    summary_rows = (facets[0].get("summary") if facets else None) or []
    summary = summary_rows[0] if summary_rows else {}
    by_status_rows = (facets[0].get("by_status") if facets else None) or []

    # Distinct company count over the enrolled prospects (null/empty keys dropped).
    companies_count = len([c for c in (summary.get("companies_set") or []) if c])

    return {
        "total": summary.get("total", 0),
        "with_email": summary.get("with_email", 0),
        "with_linkedin": summary.get("with_linkedin", 0),
        "enriched": summary.get("enriched", 0),
        "messages_ready": summary.get("messages_ready", 0),
        "companies": companies_count,
        "by_status": {str(row["_id"]): row["n"] for row in by_status_rows},
    }


@router.get("/{campaign_id}/companies")
async def list_campaign_companies(
    campaign_id: str,
    account_ctx: dict = Depends(get_account_context),
):
    """
    Distinct companies represented among a campaign's enrolled prospects.

    Groups every enrollment by the shared-pool company_id (falling back to
    company_name) and returns per-company metadata + prospect_count plus a small
    sample of enrolled prospects, sorted by prospect_count desc, so the review UI
    can render a company-level rollup.
    """
    account_id = ObjectId(account_ctx["account"]["_id"])
    await _get_campaign_or_404(campaign_id, account_id)

    campaign_oid = ObjectId(campaign_id)

    pipeline = [
        {"$match": {"campaign_id": campaign_oid}},
        {
            "$lookup": {
                "from": "prospects",
                "localField": "prospect_id",
                "foreignField": "_id",
                "as": "prospect_data",
            }
        },
        {"$unwind": {"path": "$prospect_data", "preserveNullAndEmptyArrays": True}},
        {
            "$group": {
                "_id": {
                    "$ifNull": [
                        "$prospect_data.company_id",
                        "$prospect_data.company_name",
                    ]
                },
                "company_name": {"$first": "$prospect_data.company_name"},
                "company_domain": {"$first": "$prospect_data.company_domain"},
                "company_linkedin": {"$first": "$prospect_data.company_linkedin"},
                "company_industry_group": {
                    "$first": "$prospect_data.company_industry_group"
                },
                "prospect_count": {"$sum": 1},
                "prospects": {
                    "$push": {
                        "prospect_id": {"$toString": "$prospect_id"},
                        "full_name": "$prospect_data.full_name",
                        "job_title": "$prospect_data.job_title",
                    }
                },
            }
        },
        {"$sort": {"prospect_count": -1}},
        {
            "$project": {
                "_id": 0,
                "company_id": "$_id",
                "company_name": 1,
                "company_domain": 1,
                "company_linkedin": 1,
                "company_industry_group": 1,
                "prospect_count": 1,
                # Cap the sample so a huge company doesn't bloat the payload.
                "prospects": {"$slice": ["$prospects", 10]},
            }
        },
    ]

    companies = await campaign_enrollments_collection.aggregate(pipeline).to_list(
        length=2000
    )
    # Drop the null/empty company bucket (prospects with neither id nor name).
    companies = [c for c in companies if c.get("company_id")]
    return {"companies": companies, "total_companies": len(companies)}


@router.get("/{campaign_id}/message-preview/{prospect_id}")
async def get_message_preview(
    campaign_id: str,
    prospect_id: str,
    account_ctx: dict = Depends(get_account_context),
):
    """Return generated messages for a specific prospect in this campaign."""
    account_id = ObjectId(account_ctx["account"]["_id"])
    await _get_campaign_or_404(campaign_id, account_id)

    campaign_oid = ObjectId(campaign_id)
    try:
        prospect_oid = ObjectId(prospect_id)
    except Exception:
        raise HTTPException(status_code=400, detail="Invalid prospect ID")

    enrollment = await campaign_enrollments_collection.find_one({
        "campaign_id": campaign_oid,
        "prospect_id": prospect_oid,
    })
    if not enrollment:
        raise HTTPException(status_code=404, detail="Enrollment not found for this prospect in this campaign")

    prospect = await prospects_collection.find_one(
        {"_id": prospect_oid},
        {"full_name": 1, "first_name": 1, "job_title": 1, "company_name": 1, "email": 1, "linkedin": 1},
    )

    def _str_id(doc):
        if doc and "_id" in doc:
            doc["_id"] = str(doc["_id"])
        return doc

    return {
        "enrollment_id": str(enrollment["_id"]),
        "prospect": _str_id(dict(prospect)) if prospect else None,
        "generated_messages": enrollment.get("generated_messages"),
        "message_gen_status": enrollment.get("message_gen_status", "pending"),
    }


@router.post("/{campaign_id}/approve-and-launch")
async def approve_and_launch(
    campaign_id: str,
    background_tasks: BackgroundTasks,
    account_ctx: dict = Depends(get_account_context),
):
    """
    Legacy: approve Day-1 and launch the campaign. Now a thin wrapper around
    POST /approve-day/1 so older frontends keep working. Prefer
    /approve-day/{day} for new callers.
    """
    return await approve_day_endpoint(campaign_id, 1, background_tasks, account_ctx=account_ctx)


@router.post("/{campaign_id}/approve-day/{day_n}")
async def approve_day_endpoint(
    campaign_id: str,
    day_n: int,
    background_tasks: BackgroundTasks,
    account_ctx: dict = Depends(get_account_context),
):
    """
    Approve a single day's outreach.

    - Computes calendar dates relative to the moment of approval (Day-1 = today
      if in window else next eligible; Day N≥2 = tomorrow from now).
    - Sets per-enrollment scheduled_utc + next_action_at for that day.
    - Flips the campaign to status=active + approval_status=launched on first
      approval (Day-1).
    - Background-generates messages for Day N+1 immediately after approval so
      the user can review the next day's drafts without waiting for sends to
      finish.
    """
    from services.campaign_launch_service import (
        SequenceLaunchValidationError,
        approve_day,
    )

    account_id = ObjectId(account_ctx["account"]["_id"])
    campaign = await _get_campaign_or_404(campaign_id, account_id)

    if not campaign.get("is_smart_campaign"):
        raise HTTPException(status_code=400, detail="This endpoint is only for smart campaigns")

    if day_n < 1:
        raise HTTPException(status_code=400, detail="day_n must be >= 1")

    try:
        result = await approve_day(campaign, day_n)
    except SequenceLaunchValidationError as e:
        raise HTTPException(status_code=409, detail=str(e)) from e
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))

    # Fire day N+1 message generation in background so the user can review the next day's
    # drafts without waiting. This is idempotent — if day N+1 is already generated it's
    # a cheap no-op. Works for all days (unbounded); approve-day endpoint handles last day
    # by returning empty (count=0) from ensure_day_ready_then_generate.
    # Durable pre-fill: enqueue day N+1 message generation as a leased job so it
    # survives a process restart instead of dying with an in-process task. The
    # enqueue is fast, so the approval response still returns promptly. This is a
    # best-effort pre-fill — a queue hiccup must not fail the approval the user
    # just made, so failures are logged rather than surfaced.
    from services.enrichment_job_service import (
        MESSAGE_GEN_MODE_ENSURE_DAY,
        enqueue_campaign_message_generation,
    )
    try:
        await enqueue_campaign_message_generation(
            account_id=str(account_id),
            campaign_id=campaign_id,
            day=day_n + 1,
            mode=MESSAGE_GEN_MODE_ENSURE_DAY,
        )
    except Exception as exc:
        logger.warning(
            f"[campaigns] failed to enqueue day {day_n + 1} pre-fill "
            f"for campaign {campaign_id}: {exc}"
        )

    # Return updated campaign for the frontend
    updated = await campaigns_collection.find_one({"_id": campaign["_id"]})
    return {
        **result,
        "campaign": serialize_campaign(updated) if updated else None,
    }


@router.post("/{campaign_id}/generate-messages")
async def generate_messages_for_day(
    campaign_id: str,
    background_tasks: BackgroundTasks,
    day: int = Query(..., ge=1, description="Send day to generate messages for"),
    account_ctx: dict = Depends(get_account_context),
):
    """Manually trigger message generation for a specific campaign day."""
    account_id = ObjectId(account_ctx["account"]["_id"])
    campaign = await _get_campaign_or_404(campaign_id, account_id)

    if not campaign.get("is_smart_campaign"):
        raise HTTPException(status_code=400, detail="This endpoint is only for smart campaigns")

    # Count enrollments awaiting generation on this day
    pending_count = await campaign_enrollments_collection.count_documents({
        "campaign_id": campaign["_id"],
        "smart_campaign_send_day": day,
        "message_gen_status": {"$in": ["scheduled_later", "failed", "pending"]},
        "status": {"$nin": ["archived", "skipped_no_channel", "cascade_waiting"]},
    })

    if pending_count == 0:
        return {"status": "no_pending", "day": day, "enrollment_count": 0}

    # Enqueue a durable, leased job so generation survives a process restart.
    # The enqueue is fast, so the response still returns promptly. This endpoint
    # exists to trigger generation, so fail closed if the work cannot be queued.
    from services.enrichment_job_service import (
        MESSAGE_GEN_MODE_GENERATE_DAY,
        enqueue_campaign_message_generation,
    )
    try:
        job = await enqueue_campaign_message_generation(
            account_id=str(account_id),
            campaign_id=campaign_id,
            day=day,
            mode=MESSAGE_GEN_MODE_GENERATE_DAY,
        )
    except Exception as exc:
        raise HTTPException(
            status_code=503, detail="Could not queue message generation"
        ) from exc

    return {
        "status": "queued",
        "job_id": str(job.id),
        "day": day,
        "enrollment_count": pending_count,
    }


@router.patch("/{campaign_id}/enrollments/{enrollment_id}/schedule")
async def update_enrollment_schedule(
    campaign_id: str,
    enrollment_id: str,
    body: dict,
    account_ctx: dict = Depends(get_account_context),
):
    """
    Change an enrollment's send day and/or channel.
    - Channel change invalidates previously generated messages.
    - Day change preserves messages if channel is unchanged.
    """
    from bson import ObjectId as OID
    account_id = ObjectId(account_ctx["account"]["_id"])
    campaign = await _get_campaign_or_404(campaign_id, account_id)

    enr = await campaign_enrollments_collection.find_one({
        "_id": OID(enrollment_id),
        "campaign_id": campaign["_id"],
    })
    if not enr:
        raise HTTPException(status_code=404, detail="Enrollment not found")

    if enr.get("status") in ("archived", "opted_out", "bounced"):
        raise HTTPException(status_code=400, detail="Cannot reschedule enrollment in terminal status")

    # Check if the day being moved from/to is already approved
    approved_days = set(campaign.get("approved_send_days") or [])
    current_day = enr.get("smart_campaign_send_day")
    new_day = body.get("day")
    new_channel = body.get("channel")

    if new_day and current_day in approved_days:
        raise HTTPException(status_code=400, detail=f"Day {current_day} is already approved and cannot be rescheduled")
    if new_day and new_day in approved_days:
        raise HTTPException(status_code=400, detail=f"Day {new_day} is already approved; cannot move into an approved day")

    update: dict = {}
    warning: str | None = None

    if new_day and new_day != current_day:
        update["smart_campaign_send_day"] = new_day
        update["next_action_at"] = None
        update["smart_campaign_scheduled_utc"] = None

    if new_channel and new_channel != enr.get("smart_campaign_channel"):
        valid_channels = {"email", "linkedin_connection", "linkedin_inmail"}
        if new_channel not in valid_channels:
            raise HTTPException(status_code=400, detail=f"Invalid channel: {new_channel}")
        update["smart_campaign_channel"] = new_channel
        # Invalidate generated messages — channel change requires different copy
        update["generated_messages"] = None
        update["message_gen_status"] = "scheduled_later"
        update["message_gen_error"] = None

    if not update:
        return {"status": "no_change", "warning": None}

    await campaign_enrollments_collection.update_one(
        {"_id": enr["_id"]},
        {"$set": update},
    )

    # Update campaign day totals
    await _recompute_day_totals(campaign["_id"])

    return {"status": "updated", "warning": warning}


@router.post("/{campaign_id}/schedule/bulk-update")
async def bulk_update_schedule(
    campaign_id: str,
    body: dict,
    account_ctx: dict = Depends(get_account_context),
):
    """Bulk reassign day and/or channel for multiple enrollments."""
    from pymongo import UpdateOne as _BulkUpdate
    account_id = ObjectId(account_ctx["account"]["_id"])
    campaign = await _get_campaign_or_404(campaign_id, account_id)

    enrollment_ids = [ObjectId(eid) for eid in (body.get("enrollment_ids") or [])]
    if not enrollment_ids:
        raise HTTPException(status_code=400, detail="enrollment_ids required")

    new_day = body.get("day")
    new_channel = body.get("channel")
    if new_day is None and new_channel is None:
        raise HTTPException(status_code=400, detail="Provide day or channel to update")

    approved_days = set(campaign.get("approved_send_days") or [])
    if new_day and new_day in approved_days:
        raise HTTPException(status_code=400, detail=f"Day {new_day} is already approved")

    valid_channels = {"email", "linkedin_connection", "linkedin_inmail"}
    if new_channel and new_channel not in valid_channels:
        raise HTTPException(status_code=400, detail=f"Invalid channel: {new_channel}")

    # Fetch enrollments to validate current state
    enrollments = await campaign_enrollments_collection.find({
        "_id": {"$in": enrollment_ids},
        "campaign_id": campaign["_id"],
    }).to_list(length=len(enrollment_ids))

    ops = []
    skipped = 0
    for enr in enrollments:
        if enr.get("status") in ("archived", "opted_out", "bounced"):
            skipped += 1
            continue
        current_day = enr.get("smart_campaign_send_day")
        if current_day in approved_days:
            skipped += 1
            continue
        update_fields: dict = {}
        if new_day and new_day != current_day:
            update_fields["smart_campaign_send_day"] = new_day
            update_fields["next_action_at"] = None
            update_fields["smart_campaign_scheduled_utc"] = None
        if new_channel and new_channel != enr.get("smart_campaign_channel"):
            update_fields["smart_campaign_channel"] = new_channel
            update_fields["generated_messages"] = None
            update_fields["message_gen_status"] = "scheduled_later"
            update_fields["message_gen_error"] = None
        if update_fields:
            ops.append(_BulkUpdate({"_id": enr["_id"]}, {"$set": update_fields}))

    updated = 0
    if ops:
        result = await campaign_enrollments_collection.bulk_write(ops, ordered=False)
        updated = result.modified_count

    # Recompute day totals
    await _recompute_day_totals(campaign["_id"])

    return {"status": "updated", "updated": updated, "skipped": skipped}


@router.patch("/{campaign_id}/enrollments/{enrollment_id}/messages")
async def edit_enrollment_messages(
    campaign_id: str,
    enrollment_id: str,
    body: EditEnrollmentMessagesRequest,
    account_ctx: dict = Depends(get_account_context),
):
    """
    Edit generated messages for a specific enrollment before campaign launch.
    Supports updating cold_email, linkedin_connection, and linkedin_inmail message types.
    """
    account_id = ObjectId(account_ctx["account"]["_id"])

    # Validate campaign belongs to account
    await _get_campaign_or_404(campaign_id, account_id)

    campaign_oid = ObjectId(campaign_id)
    try:
        enrollment_oid = ObjectId(enrollment_id)
    except Exception:
        raise HTTPException(status_code=400, detail="Invalid enrollment ID")

    # Fetch enrollment and verify it belongs to this campaign and account
    enrollment = await campaign_enrollments_collection.find_one({
        "_id": enrollment_oid,
        "campaign_id": campaign_oid,
        "account_id": account_id,
    })
    if not enrollment:
        raise HTTPException(status_code=404, detail="Enrollment not found")

    # Build the update fields based on message type
    update_fields: dict = {
        "messages_edited_at": datetime.utcnow(),
    }

    message_type = body.message_type
    if message_type == "cold_email":
        update_fields[f"generated_messages.cold_email.body"] = body.body
        if body.subject is not None:
            update_fields[f"generated_messages.cold_email.subject_a"] = body.subject
    elif message_type == "linkedin_connection":
        update_fields[f"generated_messages.linkedin_connection.note"] = body.body
    elif message_type == "linkedin_inmail":
        update_fields[f"generated_messages.linkedin_inmail.body"] = body.body
        if body.subject is not None:
            update_fields[f"generated_messages.linkedin_inmail.subject"] = body.subject
    else:
        raise HTTPException(
            status_code=400,
            detail=f"Invalid message_type '{message_type}'. Must be one of: cold_email, linkedin_connection, linkedin_inmail",
        )

    await campaign_enrollments_collection.update_one(
        {"_id": enrollment_oid},
        {"$set": update_fields},
    )

    return {"success": True, "enrollment_id": enrollment_id}


# ---------------------------------------------------------------------------
# Draft approval endpoints (sequence campaigns, draft mode)
# ---------------------------------------------------------------------------

@router.get("/pending-drafts/mine")
async def get_my_pending_drafts(account_ctx: dict = Depends(get_account_context)):
    """Return all pending AI drafts across all active campaigns for this account."""
    account_id = ObjectId(account_ctx["account"]["_id"])

    # Find enrollments with pending AI drafts across all campaigns in this account
    pipeline = [
        {"$match": {
            "account_id": account_id,
            "status": {"$in": ["active", "paused"]},
            "ai_draft_status": "pending",
            "ai_draft_text": {"$exists": True, "$ne": None},
        }},
        {"$lookup": {
            "from": "prospects",
            "localField": "prospect_id",
            "foreignField": "_id",
            "as": "prospect",
        }},
        {"$unwind": {"path": "$prospect", "preserveNullAndEmpty": False}},
        {"$lookup": {
            "from": "campaigns",
            "localField": "campaign_id",
            "foreignField": "_id",
            "as": "campaign",
        }},
        {"$unwind": {"path": "$campaign", "preserveNullAndEmpty": False}},
        {"$project": {
            "_id": 1,
            "campaign_id": 1,
            "campaign_name": "$campaign.name",
            "prospect_id": 1,
            "prospect_name": {"$ifNull": ["$prospect.full_name", "$prospect.name"]},
            "prospect_title": "$prospect.title",
            "prospect_company": "$prospect.company_name",
            "ai_draft_text": 1,
            "ai_draft_generated_at": 1,
        }},
        {"$sort": {"ai_draft_generated_at": -1}},
        {"$limit": 50},
    ]

    results = await campaign_enrollments_collection.aggregate(pipeline).to_list(50)
    return [serialize_doc(r) for r in results]


@router.get("/{campaign_id}/pending-drafts")
async def get_pending_drafts(
    campaign_id: str,
    account_ctx: dict = Depends(get_account_context),
):
    """
    Return all enrollments in this campaign that have a pending AI-generated draft
    awaiting user approval.
    """
    account_id = ObjectId(account_ctx["account"]["_id"])
    await _get_campaign_or_404(campaign_id, account_id)

    campaign_oid = ObjectId(campaign_id)
    enrollments = await campaign_enrollments_collection.find({
        "campaign_id": campaign_oid,
        "account_id": account_id,
        "waiting_for": "user_approval",
        "pending_draft": {"$exists": True, "$ne": None},
    }).to_list(200)

    # Batch-fetch prospect names for context
    prospect_ids = [e.get("prospect_id") for e in enrollments if e.get("prospect_id")]
    try:
        prospect_ids_oid = [ObjectId(str(pid)) for pid in prospect_ids]
        prospects_list = await database.prospects_collection.find(
            {"_id": {"$in": prospect_ids_oid}},
            {"full_name": 1, "company_name": 1, "linkedin": 1, "email": 1},
        ).to_list(len(prospect_ids_oid))
        prospects_map = {str(p["_id"]): p for p in prospects_list}
    except Exception:
        prospects_map = {}

    results = []
    for enr in enrollments:
        pid_str = str(enr.get("prospect_id", ""))
        prospect_info = prospects_map.get(pid_str, {})
        results.append({
            "enrollment_id": str(enr["_id"]),
            "prospect_id": pid_str,
            "prospect_name": prospect_info.get("full_name", ""),
            "prospect_company": prospect_info.get("company_name", ""),
            "current_step_id": enr.get("current_step_id"),
            "pending_draft": enr.get("pending_draft"),
            "waiting_since": enr.get("waiting_since"),
        })

    return {"drafts": results, "total": len(results)}


@router.post("/{campaign_id}/enrollments/{enrollment_id}/approve-draft")
async def approve_draft(
    campaign_id: str,
    enrollment_id: str,
    body: dict,
    account_ctx: dict = Depends(get_account_context),
):
    """
    Approve (and optionally edit) a pending AI-generated draft, then send it.

    Body:
        draft_text (optional str): Override the AI-generated text before sending.
    """
    account_id = ObjectId(account_ctx["account"]["_id"])
    campaign = await _get_campaign_or_404(campaign_id, account_id)

    try:
        enrollment_oid = ObjectId(enrollment_id)
    except Exception:
        raise HTTPException(status_code=400, detail="Invalid enrollment ID")

    enrollment = await campaign_enrollments_collection.find_one({
        "_id": enrollment_oid,
        "campaign_id": ObjectId(campaign_id),
        "account_id": account_id,
        "waiting_for": "user_approval",
    })
    if not enrollment:
        raise HTTPException(status_code=404, detail="Enrollment not found or not awaiting approval")

    pending_draft = enrollment.get("pending_draft")
    if not pending_draft:
        raise HTTPException(status_code=400, detail="No pending draft on this enrollment")

    draft_text = body.get("draft_text") or pending_draft.get("draft_text", "")
    channel = pending_draft.get("channel", "linkedin")

    # Fetch prospect for sending
    prospect_id = enrollment.get("prospect_id")
    try:
        prospect_oid = ObjectId(str(prospect_id))
        prospect = await database.prospects_collection.find_one({"_id": prospect_oid})
    except Exception:
        prospect = None

    if not prospect:
        raise HTTPException(status_code=404, detail="Prospect not found")

    now = datetime.utcnow()

    # Send the draft via the appropriate channel
    try:
        if channel == "linkedin":
            from services.unipile_service import UnipileClient
            linkedin_url = prospect.get("linkedin", "")
            if not linkedin_url:
                raise HTTPException(status_code=400, detail="Prospect has no LinkedIn URL")
            account_values = [account_id, str(account_id)]
            linked_query: dict = {
                "account_id": {"$in": account_values},
                "unipile_status": "OK",
                "unipile_account_id": {"$exists": True, "$nin": [None, ""]},
            }
            if campaign.get("linkedin_account_id"):
                try:
                    linked_query["_id"] = ObjectId(str(campaign["linkedin_account_id"]))
                except Exception:
                    raise HTTPException(status_code=400, detail="Campaign has invalid LinkedIn sender")
            linked_senders = await database.linkedin_accounts_collection.find(
                linked_query,
                {"unipile_account_id": 1},
            ).limit(2).to_list(2)
            if len(linked_senders) != 1:
                raise HTTPException(
                    status_code=400,
                    detail="Campaign requires one explicit connected LinkedIn sender",
                )
            unipile = UnipileClient(
                account_id=str(linked_senders[0]["unipile_account_id"])
            )
            await unipile.start_new_chat(linkedin_url, draft_text)
        elif channel in ("email", "cold_email", "followup_email"):
            from services.email_delivery_service import send_email as delivery_send_email
            email_address = prospect.get("email", "")
            if not email_address:
                raise HTTPException(status_code=400, detail="Prospect has no email address")
            subject = pending_draft.get("subject", "Following up")
            email_account_id = campaign.get("email_account_id")
            if not email_account_id:
                raise HTTPException(status_code=400, detail="Campaign has no connected email account")
            email_account = await database.email_accounts_collection.find_one(
                {
                    "_id": ObjectId(str(email_account_id)),
                    "account_id": {"$in": [account_id, str(account_id)]},
                }
            )
            if not email_account:
                raise HTTPException(status_code=400, detail="Connected email account not found")
            send_result = await delivery_send_email(
                email_account,
                email_address,
                subject,
                draft_text,
                prospect_id=str(prospect["_id"]),
                campaign_id=campaign_id,
            )
            if not send_result:
                raise HTTPException(status_code=502, detail="Email send failed — check provider credentials/logs")
        else:
            raise HTTPException(status_code=400, detail=f"Unsupported channel: {channel}")
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to send message: {e}")

    # Record the sent message
    await database.campaign_messages_collection.insert_one({
        "campaign_id": ObjectId(campaign_id),
        "enrollment_id": enrollment_oid,
        "prospect_id": prospect_id,
        "account_id": account_id,
        "channel": channel,
        "direction": "outbound",
        "content": draft_text,
        "step_id": enrollment.get("current_step_id"),
        "approved_by_user": True,
        "sent_at": now,
        "created_at": now,
    })

    # Advance enrollment state — flow-engine campaigns transition via 'sent', others complete
    flow_state = enrollment.get("flow_state")
    if flow_state:
        from services import flow_engine as _flow_engine
        _campaign_doc = await campaigns_collection.find_one(
            {"_id": ObjectId(campaign_id)},
            {"follow_up_flow": 1},
        )
        _flow = _campaign_doc.get("follow_up_flow") if _campaign_doc else None
        if _flow:
            new_flow_state = _flow_engine.transition(flow_state, _flow, "sent", prospect)
            _flow_update: dict = {
                "flow_state": new_flow_state,
                "pending_draft": None,
                "waiting_for": None,
            }
            if new_flow_state.get("next_action_at"):
                try:
                    _flow_update["next_action_at"] = datetime.fromisoformat(new_flow_state["next_action_at"])
                except Exception:
                    pass
            await campaign_enrollments_collection.update_one(
                {"_id": enrollment_oid},
                {"$set": _flow_update},
            )
        else:
            await campaign_enrollments_collection.update_one(
                {"_id": enrollment_oid},
                {"$set": {
                    "status": "completed",
                    "pending_draft": None,
                    "waiting_for": None,
                    "completed_at": now,
                    "last_activity_at": now,
                }},
            )
    else:
        # No flow_state — complete the enrollment
        await campaign_enrollments_collection.update_one(
            {"_id": enrollment_oid},
            {"$set": {
                "status": "completed",
                "pending_draft": None,
                "waiting_for": None,
                "completed_at": now,
                "last_activity_at": now,
            }},
        )

    return {"success": True, "message": "Draft approved and sent", "enrollment_id": enrollment_id}


@router.post("/{campaign_id}/enrollments/{enrollment_id}/reject-draft")
async def reject_draft(
    campaign_id: str,
    enrollment_id: str,
    account_ctx: dict = Depends(get_account_context),
):
    """
    Reject (discard) a pending AI-generated draft without sending.
    The enrollment stays active but the draft is cleared — user can reply manually.
    """
    account_id = ObjectId(account_ctx["account"]["_id"])
    await _get_campaign_or_404(campaign_id, account_id)

    try:
        enrollment_oid = ObjectId(enrollment_id)
    except Exception:
        raise HTTPException(status_code=400, detail="Invalid enrollment ID")

    enrollment = await campaign_enrollments_collection.find_one({
        "_id": enrollment_oid,
        "campaign_id": ObjectId(campaign_id),
        "account_id": account_id,
        "waiting_for": "user_approval",
    })
    if not enrollment:
        raise HTTPException(status_code=404, detail="Enrollment not found or not awaiting approval")

    now = datetime.utcnow()
    await campaign_enrollments_collection.update_one(
        {"_id": enrollment_oid},
        {"$set": {
            "pending_draft": None,
            "waiting_for": None,
            "last_activity_at": now,
        }},
    )

    return {"success": True, "message": "Draft rejected and cleared", "enrollment_id": enrollment_id}


@router.post("/{campaign_id}/enrollments/{enrollment_id}/archive")
async def archive_enrollment(
    campaign_id: str,
    enrollment_id: str,
    account_ctx=Depends(get_account_context),
):
    """Archive a single enrollment — stops future sends for this prospect in this campaign."""
    account_id = account_ctx["account"]["_id"]
    campaign_oid = ObjectId(campaign_id)
    enrollment_oid = ObjectId(enrollment_id)

    enrollment = await database.campaign_enrollments_collection.find_one({
        "_id": enrollment_oid,
        "campaign_id": campaign_oid,
        "account_id": account_id,
    })
    if not enrollment:
        raise HTTPException(status_code=404, detail="Enrollment not found")

    now = datetime.utcnow()
    flow_state = enrollment.get("flow_state") or {}
    flow_state["stopped"] = True
    flow_state["stopped_reason"] = "archived"

    await database.campaign_enrollments_collection.update_one(
        {"_id": enrollment_oid},
        {"$set": {
            "status": "archived",
            "flow_state": flow_state,
            "next_action_at": None,
            "last_activity_at": now,
        }},
    )

    # Decrement active count if was active
    if enrollment.get("status") == "active":
        await database.campaigns_collection.update_one(
            {"_id": campaign_oid},
            {"$inc": {"active_count": -1}},
        )

    return {"status": "archived", "enrollment_id": enrollment_id}


# ---------------------------------------------------------------------------
# Follow-up flow endpoints
# ---------------------------------------------------------------------------

@router.get("/{campaign_id}/follow-up-flow")
async def get_follow_up_flow(
    campaign_id: str,
    account_ctx: dict = Depends(get_account_context),
):
    """Return the follow-up flow for a campaign."""
    account_id = ObjectId(account_ctx["account"]["_id"])
    campaign = await _get_campaign_or_404(campaign_id, account_id)
    from services.flow_engine import DEFAULT_FLOW_LINKEDIN_FIRST
    return {"follow_up_flow": campaign.get("follow_up_flow") or DEFAULT_FLOW_LINKEDIN_FIRST}


@router.put("/{campaign_id}/follow-up-flow")
async def update_follow_up_flow(
    campaign_id: str,
    body: dict,
    account_ctx: dict = Depends(get_account_context),
):
    """Update the follow-up flow for a campaign (only before launch)."""
    account_id = ObjectId(account_ctx["account"]["_id"])
    campaign = await _get_campaign_or_404(campaign_id, account_id)
    if campaign.get("approval_status") == "launched":
        raise HTTPException(status_code=400, detail="Cannot update flow of a launched campaign")
    flow = body.get("follow_up_flow", {})
    from services.flow_engine import validate_flow
    errors = validate_flow(flow)
    if errors:
        raise HTTPException(status_code=422, detail={"errors": errors})
    await campaigns_collection.update_one(
        {"_id": ObjectId(campaign_id)},
        {"$set": {"follow_up_flow": flow}},
    )
    return {"follow_up_flow": flow}


# ---------------------------------------------------------------------------
# Schedule range endpoints
# ---------------------------------------------------------------------------

@router.get("/{campaign_id}/schedules/range")
async def get_schedule_range(
    campaign_id: str,
    start: str,
    end: str,
    account_ctx: dict = Depends(get_account_context),
):
    """Return ScheduleDay summaries for a date range."""
    from datetime import timezone, timedelta
    account_id = ObjectId(account_ctx["account"]["_id"])
    await _get_campaign_or_404(campaign_id, account_id)

    try:
        start_dt = datetime.strptime(start, "%Y-%m-%d").replace(tzinfo=timezone.utc)
        end_dt = datetime.strptime(end, "%Y-%m-%d").replace(tzinfo=timezone.utc) + timedelta(days=1)
    except ValueError:
        raise HTTPException(status_code=400, detail="Invalid date format, use YYYY-MM-DD")

    # Look up campaign_schedules documents for this range
    schedule_map = {}
    cursor = database.db["campaign_schedules"].find({
        "campaign_id": str(campaign_id),
        "date": {"$gte": start, "$lte": end},
    })
    async for doc in cursor:
        schedule_map[doc["date"]] = doc

    # Build response for each day in range
    days = []
    current = start_dt
    while current < end_dt:
        date_str = current.strftime("%Y-%m-%d")
        sched = schedule_map.get(date_str)
        if sched:
            items = sched.get("items", [])
            sent = sum(1 for i in items if i.get("status") in ("sent", "completed"))
            approved = sum(1 for i in items if i.get("status") in ("approved", "sending", "sent", "completed"))
            days.append({
                "date": date_str,
                "status": sched.get("status", "empty"),
                "total_items": len(items),
                "sent_items": sent,
                "approved_items": approved,
            })
        else:
            days.append({"date": date_str, "status": "empty", "total_items": 0, "sent_items": 0, "approved_items": 0})
        current += timedelta(days=1)
    return {"days": days}


@router.get("/{campaign_id}/schedule")
async def get_campaign_schedule(
    campaign_id: str,
    day: Optional[int] = Query(None, ge=1, description="Return enrollments for only this day"),
    account_ctx=Depends(get_account_context),
):
    """
    Return per-day schedule aggregated from campaign_enrollments.

    When ``day`` is provided, returns only that day's items plus summary
    metadata for every day (counts, approval state, message-gen state). This
    lets the frontend render a day switcher without fetching every day's
    full item list.
    """
    from datetime import date as _date, timedelta as _timedelta

    account_id = account_ctx["account"]["_id"]

    try:
        camp_oid = ObjectId(campaign_id)
    except Exception:
        raise HTTPException(status_code=400, detail="Invalid campaign ID")

    # Verify campaign belongs to account. Pull the fields compute_day1_preview
    # needs alongside the ones used for the nth_business_day math below.
    campaign = await database.campaigns_collection.find_one(
        {"_id": camp_oid, "account_id": ObjectId(account_id)},
        {
            "_id": 1,
            "launch_day1_date": 1,
            "timezone": 1,
            "send_days": 1,
            "send_hour_start": 1,
            "send_hour_end": 1,
            "approval_status": 1,
            "approved_send_days": 1,
            "day_approvals": 1,
        },
    )
    if not campaign:
        raise HTTPException(status_code=404, detail="Campaign not found")

    # Compute Day-1 preview (same logic used at approve-and-launch time) so the
    # UI can show "Sends on {day1_date}" in the banner without duplicating
    # timezone / send-window logic in the frontend.
    from services.campaign_launch_service import compute_day1_preview
    day1_date_obj, day1_is_today = compute_day1_preview(campaign)

    approved_days: list[int] = campaign.get("approved_send_days") or []
    day_approvals: dict = campaign.get("day_approvals") or {}

    # --- Pass 1: lightweight per-day summaries (no message bodies) ---
    summary_cursor = database.campaign_enrollments_collection.find(
        {
            "campaign_id": camp_oid,
            "status": {"$in": ["active", "enrolled", "completed", "replied", "converted", "paused"]},
            "smart_campaign_channel": {"$exists": True, "$ne": None},
        },
        {
            "smart_campaign_channel": 1,
            "smart_campaign_send_day": 1,
            "message_gen_status": 1,
        },
    )
    summary_enrollments = await summary_cursor.to_list(length=5000)

    day_summary_map: dict = {}
    for enr in summary_enrollments:
        sd = enr.get("smart_campaign_send_day") or 1
        ch = enr.get("smart_campaign_channel")
        msg_status = enr.get("message_gen_status")
        d = day_summary_map.setdefault(sd, {
            "send_day": sd,
            "total": 0,
            "totals": {"linkedin_connection": 0, "email": 0, "linkedin_inmail": 0},
            "message_status_counts": {},
        })
        d["total"] += 1
        if ch in d["totals"]:
            d["totals"][ch] += 1
        else:
            d["totals"][ch] = 1
        key = msg_status or "unknown"
        d["message_status_counts"][key] = d["message_status_counts"].get(key, 0) + 1

    # --- Compute calendar dates for every day ---
    def _date_from(value):
        if isinstance(value, str):
            return _date.fromisoformat(value[:10])
        if hasattr(value, "date"):
            return value.date()
        return value

    launch_date = campaign.get("launch_day1_date") or day1_date_obj
    send_days_list = campaign.get("send_days", ["monday", "tuesday", "wednesday", "thursday", "friday"])

    def _nth_business_day(start_date, n: int):
        current = start_date
        days_added = 0
        while days_added < n - 1:
            current += _timedelta(days=1)
            if current.strftime("%A").lower() in send_days_list:
                days_added += 1
        return current

    sorted_summaries = sorted(day_summary_map.values(), key=lambda d: d["send_day"])
    for s in sorted_summaries:
        send_day = s["send_day"]

        # Prefer the explicit approval date for already-approved days.
        explicit = day_approvals.get(str(send_day)) or day_approvals.get(send_day)
        if explicit and explicit.get("target_date"):
            s["date"] = explicit["target_date"]
        elif launch_date:
            try:
                base = _date_from(launch_date)
                s["date"] = _nth_business_day(base, send_day).isoformat()
            except Exception:
                s["date"] = None
        else:
            s["date"] = None

        s["approved"] = send_day in approved_days
        s["approved_at"] = (explicit or {}).get("approved_at") if explicit else None
        # Derive a single message-gen state per day from the distribution.
        counts = s["message_status_counts"]
        if counts.get("done", 0) == s["total"]:
            s["message_gen_state"] = "completed"
        elif counts.get("failed", 0) > 0:
            s["message_gen_state"] = "failed"
        elif counts.get("running", 0) > 0 or counts.get("pending", 0) > 0:
            s["message_gen_state"] = "running"
        elif counts.get("scheduled_later", 0) == s["total"]:
            s["message_gen_state"] = "not_started"
        else:
            s["message_gen_state"] = "partial"

    # --- Pass 2: detailed items for the requested day (or all days if no day specified) ---
    item_query = {
        "campaign_id": camp_oid,
        "status": {"$in": ["active", "enrolled", "completed", "replied", "converted", "paused"]},
        "smart_campaign_channel": {"$exists": True, "$ne": None},
    }
    if day is not None:
        item_query["smart_campaign_send_day"] = day

    enr_cursor = database.campaign_enrollments_collection.find(
        item_query,
        {
            "_id": 1,
            "prospect_id": 1,
            "smart_campaign_channel": 1,
            "smart_campaign_send_day": 1,
            "smart_campaign_scheduled_utc": 1,
            "generated_messages": 1,
            "status": 1,
            "message_gen_status": 1,
            "message_gen_error": 1,
        },
    )
    enrollments = await enr_cursor.to_list(length=5000)

    if not enrollments and not sorted_summaries:
        return {
            "days": [],
            "day_summaries": [],
            "day1_date": day1_date_obj.isoformat() if day1_date_obj else None,
            "day1_is_today": bool(day1_is_today),
            "timezone": campaign.get("timezone", "America/New_York"),
            "approval_status": campaign.get("approval_status", "pending"),
            "approved_send_days": approved_days,
        }

    prospect_ids = [e["prospect_id"] for e in enrollments if e.get("prospect_id")]
    prospects_map: dict = {}
    if prospect_ids:
        # Canonical identity fields come from the tenant-neutral shared pool;
        # score/tier come from the campaign/tenant-scoped prospect_state overlay.
        pid_strs = [str(pid) for pid in prospect_ids]
        state_by_pid: dict = {}
        async for st in database.prospect_state_collection.find(
            {"account_id": str(account_id), "prospect_id": {"$in": pid_strs}},
            {"prospect_id": 1, "ai_score": 1, "priority_tier": 1},
        ):
            state_by_pid[st["prospect_id"]] = st

        plist = await database.prospects_collection.find(
            {"_id": {"$in": prospect_ids}},
            {
                "_id": 1,
                "full_name": 1,
                "first_name": 1,
                "last_name": 1,
                "company_name": 1,
                "job_title": 1,
            },
        ).to_list(length=len(prospect_ids))
        for p in plist:
            full_name = p.get("full_name") or f"{p.get('first_name', '')} {p.get('last_name', '')}".strip()
            st = state_by_pid.get(str(p["_id"]), {})
            prospects_map[str(p["_id"])] = {
                "name": full_name,
                "company": p.get("company_name", ""),
                "job_title": p.get("job_title", ""),
                "ai_prospect_score": st.get("ai_score"),
                "priority_tier": st.get("priority_tier"),
            }

    days_map: dict = {}
    for enr in enrollments:
        send_day = enr.get("smart_campaign_send_day") or 1
        channel = enr.get("smart_campaign_channel")
        scheduled_at = enr.get("smart_campaign_scheduled_utc")
        prospect_info = prospects_map.get(str(enr.get("prospect_id", "")), {})
        msgs = enr.get("generated_messages") or {}

        # Preview + full body per channel so the UI can show-then-edit.
        subject = ""
        body_preview = ""
        full_body = ""
        if channel == "email":
            email_data = msgs.get("cold_email") or msgs.get("email") or {}
            subject = email_data.get("subject_a") or email_data.get("subject") or ""
            full_body = email_data.get("body", "") or ""
        elif channel == "linkedin_connection":
            li_data = msgs.get("linkedin_connection") or {}
            full_body = li_data.get("note", "") or ""
        elif channel == "linkedin_inmail":
            inmail_data = msgs.get("linkedin_inmail") or {}
            subject = inmail_data.get("subject", "") or ""
            full_body = inmail_data.get("body", "") or ""

        body_preview = (full_body[:140] + "…") if len(full_body) > 140 else full_body

        if send_day not in days_map:
            days_map[send_day] = {
                "send_day": send_day,
                "date": None,
                "items": [],
                "totals": {"linkedin_connection": 0, "email": 0, "linkedin_inmail": 0},
            }

        days_map[send_day]["totals"][channel] = days_map[send_day]["totals"].get(channel, 0) + 1
        days_map[send_day]["items"].append({
            "enrollment_id": str(enr["_id"]),
            "prospect_id": str(enr.get("prospect_id", "")),
            "prospect_name": prospect_info.get("name", ""),
            "prospect_company": prospect_info.get("company", ""),
            "prospect_job_title": prospect_info.get("job_title", ""),
            "ai_prospect_score": prospect_info.get("ai_prospect_score"),
            "priority_tier": prospect_info.get("priority_tier"),
            "channel": channel,
            "scheduled_at": scheduled_at.isoformat() if scheduled_at else None,
            "status": enr.get("status", "active"),
            "message_gen_status": enr.get("message_gen_status"),
            "message_gen_error": enr.get("message_gen_error"),
            "subject": subject,
            "body": full_body,
            "body_preview": body_preview,
        })

    sorted_days = sorted(days_map.values(), key=lambda d: d["send_day"])
    for day_data in sorted_days:
        sd = day_data["send_day"]
        explicit = day_approvals.get(str(sd)) or day_approvals.get(sd)
        if explicit and explicit.get("target_date"):
            day_data["date"] = explicit["target_date"]
        elif launch_date:
            try:
                base = _date_from(launch_date)
                day_data["date"] = _nth_business_day(base, sd).isoformat()
            except Exception:
                day_data["date"] = None
        # Sort items by scheduled_at (None-safe), then by score desc
        day_data["items"].sort(
            key=lambda x: (x.get("scheduled_at") or "", x.get("ai_prospect_score") is None, -(x.get("ai_prospect_score") or 0))
        )

    return {
        "days": sorted_days,
        "day_summaries": sorted_summaries,
        "day1_date": day1_date_obj.isoformat() if day1_date_obj else None,
        "day1_is_today": bool(day1_is_today),
        "timezone": campaign.get("timezone", "America/New_York"),
        "approval_status": campaign.get("approval_status", "pending"),
        "approved_send_days": approved_days,
        "requested_day": day,
    }


class RemoveEnrollmentsRequest(BaseModel):
    enrollment_ids: list[str] = Field(..., min_length=1)


@router.post("/{campaign_id}/schedule/remove-enrollments")
async def remove_enrollments_from_schedule(
    campaign_id: str,
    body: RemoveEnrollmentsRequest,
    account_ctx: dict = Depends(get_account_context),
):
    """
    Bulk-move selected prospects to the end of the campaign.

    The rest of the schedule stays untouched — freed slots are NOT backfilled.
    The selected enrollments are planned onto fresh days after the current
    last send day, respecting the existing per-channel daily caps.

    Only works on unapproved days — any selection containing an approved-day
    enrollment is rejected.
    """
    from services.campaign_launch_service import plan_channel_assignments
    from pymongo import UpdateOne as _UpdateOne

    account_id = ObjectId(account_ctx["account"]["_id"])

    try:
        enr_oids = [ObjectId(eid) for eid in body.enrollment_ids]
    except Exception:
        raise HTTPException(status_code=400, detail="Invalid enrollment_id format")

    campaign = await _get_campaign_or_404(campaign_id, account_id)
    camp_oid = campaign["_id"]

    selected_cursor = campaign_enrollments_collection.find({
        "_id": {"$in": enr_oids},
        "campaign_id": camp_oid,
    })
    selected = await selected_cursor.to_list(length=len(enr_oids))
    if len(selected) != len(enr_oids):
        raise HTTPException(status_code=404, detail="One or more enrollments not found in this campaign")

    approved_days: list[int] = campaign.get("approved_send_days") or []
    src_days: list[int] = []
    for enr in selected:
        sd = enr.get("smart_campaign_send_day")
        if sd is None:
            raise HTTPException(
                status_code=400,
                detail="One or more enrollments are not assigned to a send day",
            )
        if sd in approved_days:
            raise HTTPException(
                status_code=400,
                detail=f"Cannot remove from an approved day (Day {sd}). Approved days cannot be modified.",
            )
        src_days.append(sd)

    min_src_day = min(src_days)

    current_last_day_doc = await campaign_enrollments_collection.find_one(
        {
            "campaign_id": camp_oid,
            "smart_campaign_send_day": {"$ne": None},
            "status": {"$in": ["active", "enrolled"]},
        },
        sort=[("smart_campaign_send_day", -1)],
        projection={"smart_campaign_send_day": 1},
    )
    current_last_day = (
        current_last_day_doc.get("smart_campaign_send_day") if current_last_day_doc else 0
    ) or 0
    removed_start_day = current_last_day + 1

    prospect_ids = [e["prospect_id"] for e in selected if e.get("prospect_id")]
    prospects_list = await prospects_collection.find(
        {"_id": {"$in": prospect_ids}}
    ).to_list(length=len(prospect_ids))
    prospects_by_id = {p["_id"]: p for p in prospects_list}

    removed_for_planning = [{**e, "smart_campaign_channel": None} for e in selected]
    removed_assignments, _ = plan_channel_assignments(
        campaign, removed_for_planning, prospects_by_id, start_day=removed_start_day
    )
    new_last_day = max(
        (sd for _, _, sd in removed_assignments), default=removed_start_day
    )

    ops: list = []
    assigned_enr_ids: set = set()
    for enr, channel, send_day in removed_assignments:
        assigned_enr_ids.add(enr["_id"])
        ops.append(_UpdateOne(
            {"_id": enr["_id"]},
            {"$set": {
                "smart_campaign_send_day": send_day,
                "smart_campaign_channel": channel,
                "smart_campaign_scheduled_utc": None,
                "next_action_at": None,
            }},
        ))

    for enr in selected:
        if enr["_id"] not in assigned_enr_ids:
            ops.append(_UpdateOne(
                {"_id": enr["_id"]},
                {"$set": {
                    "smart_campaign_send_day": new_last_day,
                    "smart_campaign_scheduled_utc": None,
                    "next_action_at": None,
                }},
            ))

    if ops:
        await campaign_enrollments_collection.bulk_write(ops, ordered=False)

    return {
        "success": True,
        "removed_count": len(selected),
        "removed_start_day": removed_start_day,
        "new_last_day": new_last_day,
        "min_src_day": min_src_day,
    }


@router.post("/{campaign_id}/schedules/{date}/regenerate-item/{item_id}")
async def regenerate_schedule_item(
    campaign_id: str,
    date: str,
    item_id: str,
    background_tasks: BackgroundTasks,
    account_ctx: dict = Depends(get_account_context),
):
    """Re-generate the message for a single schedule item."""
    account_id = ObjectId(account_ctx["account"]["_id"])
    campaign = await _get_campaign_or_404(campaign_id, account_id)

    sched = await database.db["campaign_schedules"].find_one({"campaign_id": str(campaign_id), "date": date})
    if not sched:
        raise HTTPException(status_code=404, detail="Schedule not found")

    items = sched.get("items", [])
    item = next((i for i in items if i.get("id") == item_id or str(i.get("_id", "")) == item_id), None)
    if not item:
        raise HTTPException(status_code=404, detail="Item not found")

    enrollment_id = item.get("enrollment_id")
    if not enrollment_id:
        raise HTTPException(status_code=400, detail="Schedule item has no enrollment_id")

    try:
        enrollment = await campaign_enrollments_collection.find_one({"_id": ObjectId(enrollment_id)})
    except Exception:
        raise HTTPException(status_code=400, detail="Invalid enrollment_id in schedule item")
    if not enrollment:
        raise HTTPException(status_code=404, detail="Enrollment not found")

    prospect = await database.prospects_collection.find_one({"_id": enrollment["prospect_id"]})

    flow = campaign.get("follow_up_flow") or {}
    flow_state = enrollment.get("flow_state") or {}
    from services import flow_engine
    node = flow_engine.get_current_node(flow_state, flow)
    if not node:
        raise HTTPException(status_code=400, detail="No active flow node for this enrollment")

    from services.campaign_message_generator_service import generate_message_for_step
    draft = await generate_message_for_step(
        db=database.db,
        campaign_id=str(campaign_id),
        enrollment_id=str(enrollment["_id"]),
        node_id=node["id"],
        node=node,
        campaign=campaign,
        prospect=prospect or {},
    )

    return {"draft": draft}


# ---------------------------------------------------------------------------
# Smart Campaign — Regenerate outreach messages
# ---------------------------------------------------------------------------

class RegenerateMessagesRequest(BaseModel):
    additional_instructions: Optional[str] = None
    # If omitted, regenerates the enrollment's assigned channel (or all channels
    # for non-smart campaigns). When set, regenerates the full multi-channel
    # payload so callers can override the channel mix.
    regenerate_all_channels: Optional[bool] = False


class ChannelInstructions(BaseModel):
    email: Optional[str] = None
    linkedin_connection: Optional[str] = None
    linkedin_inmail: Optional[str] = None


class ChannelToggles(BaseModel):
    email: bool = True
    linkedin_connection: bool = True
    linkedin_inmail: bool = True


class EnrichAndGenerateRequest(BaseModel):
    instructions: ChannelInstructions = Field(default_factory=ChannelInstructions)
    regenerate_channels: ChannelToggles = Field(default_factory=ChannelToggles)
    send_empty_connection_request: bool = False


async def _regenerate_messages_for_enrollments(
    campaign: dict,
    enrollments: list[dict],
    additional_instructions: Optional[str],
    regenerate_all_channels: bool = False,
) -> dict:
    """
    Run message regeneration for a set of enrollments. Shared by the three
    regenerate endpoints. Uses the paid-primary fallback chain and the
    per-model rate limiter inside openrouter_service.
    """
    from services.openrouter_service import OpenRouterClient
    from services.campaign_message_generator_service import (
        generate_messages_for_enrollment,
        generate_single_channel_message,
        prepare_campaign_for_generation,
    )

    if not enrollments:
        return {"queued": 0, "succeeded": 0, "failed": 0}

    # Stamp sender identity + company profile onto the in-memory campaign dict
    # (same as generate_messages_for_campaign) so regenerated messages carry the
    # seller's name/case studies instead of falling back to unsigned prompts.
    await prepare_campaign_for_generation(campaign, campaign.get("account_id"))

    # Batch-fetch prospects
    prospect_ids = [e["prospect_id"] for e in enrollments]
    prospects_list = await prospects_collection.find(
        {"_id": {"$in": prospect_ids}}
    ).to_list(length=len(prospect_ids))
    prospects_by_id = {p["_id"]: p for p in prospects_list}

    client = OpenRouterClient()
    semaphore = asyncio.Semaphore(max(1, min(3, settings.ai_concurrency_limit)))
    succeeded = 0
    failed = 0

    async def _process(enrollment: dict):
        nonlocal succeeded, failed
        async with semaphore:
            prospect = prospects_by_id.get(enrollment["prospect_id"])
            if not prospect:
                failed += 1
                await campaign_enrollments_collection.update_one(
                    {"_id": enrollment["_id"]},
                    {"$set": {
                        "message_gen_status": "failed",
                        "message_gen_error": "Prospect not found during regenerate",
                    }, "$inc": {"message_gen_attempts": 1}},
                )
                return
            try:
                if (
                    not regenerate_all_channels
                    and campaign.get("is_smart_campaign")
                    and enrollment.get("smart_campaign_channel")
                ):
                    msgs = await generate_single_channel_message(
                        enrollment, prospect, campaign, client,
                        additional_instructions=additional_instructions,
                    )
                else:
                    msgs = await generate_messages_for_enrollment(
                        enrollment, prospect, campaign, client,
                        additional_instructions=additional_instructions,
                    )
                if msgs:
                    succeeded += 1
                    await campaign_enrollments_collection.update_one(
                        {"_id": enrollment["_id"]},
                        {"$inc": {"message_gen_attempts": 1}},
                    )
                else:
                    failed += 1
                    await campaign_enrollments_collection.update_one(
                        {"_id": enrollment["_id"]},
                        {"$set": {
                            "message_gen_status": "failed",
                            "message_gen_error": "Regenerate returned no messages",
                        }, "$inc": {"message_gen_attempts": 1}},
                    )
            except Exception as e:
                failed += 1
                await campaign_enrollments_collection.update_one(
                    {"_id": enrollment["_id"]},
                    {"$set": {
                        "message_gen_status": "failed",
                        "message_gen_error": str(e)[:500],
                    }, "$inc": {"message_gen_attempts": 1}},
                )

    try:
        await asyncio.gather(*[_process(e) for e in enrollments])
    finally:
        await client.close()

    return {"queued": len(enrollments), "succeeded": succeeded, "failed": failed}


@router.post("/{campaign_id}/enrollments/{enrollment_id}/regenerate-messages")
async def regenerate_enrollment_messages(
    campaign_id: str,
    enrollment_id: str,
    body: RegenerateMessagesRequest,
    account_ctx: dict = Depends(get_account_context),
):
    """Regenerate the outreach message(s) for a single enrollment, with optional extra instructions."""
    account_id = ObjectId(account_ctx["account"]["_id"])
    campaign = await _get_campaign_or_404(campaign_id, account_id)

    try:
        enr_oid = ObjectId(enrollment_id)
    except Exception:
        raise HTTPException(status_code=400, detail="Invalid enrollment_id")

    enrollment = await campaign_enrollments_collection.find_one(
        {"_id": enr_oid, "campaign_id": campaign["_id"]}
    )
    if not enrollment:
        raise HTTPException(status_code=404, detail="Enrollment not found")

    # Mark pending so UI can show a shimmer while we regenerate
    await campaign_enrollments_collection.update_one(
        {"_id": enr_oid},
        {"$set": {"message_gen_status": "pending", "message_gen_error": None}},
    )

    result = await _regenerate_messages_for_enrollments(
        campaign, [enrollment],
        additional_instructions=body.additional_instructions,
        regenerate_all_channels=bool(body.regenerate_all_channels),
    )

    # Return the updated enrollment so the UI can refresh immediately
    updated = await campaign_enrollments_collection.find_one({"_id": enr_oid})
    return {
        **result,
        "enrollment": serialize_doc(updated) if updated else None,
    }


@router.post("/{campaign_id}/regenerate-failed-messages")
async def regenerate_failed_messages(
    campaign_id: str,
    body: RegenerateMessagesRequest,
    account_ctx: dict = Depends(get_account_context),
):
    """Regenerate messages for every enrollment that is pending or failed."""
    account_id = ObjectId(account_ctx["account"]["_id"])
    campaign = await _get_campaign_or_404(campaign_id, account_id)

    cursor = campaign_enrollments_collection.find({
        "campaign_id": campaign["_id"],
        "$or": [
            {"message_gen_status": {"$in": ["failed", "pending"]}},
            {"generated_messages": None},
            {"generated_messages": {"$exists": False}},
        ],
    })
    enrollments = await cursor.to_list(length=2000)
    if not enrollments:
        return {"queued": 0, "succeeded": 0, "failed": 0}

    enr_ids = [e["_id"] for e in enrollments]
    await campaign_enrollments_collection.update_many(
        {"_id": {"$in": enr_ids}},
        {"$set": {"message_gen_status": "pending", "message_gen_error": None}},
    )

    return await _regenerate_messages_for_enrollments(
        campaign, enrollments,
        additional_instructions=body.additional_instructions,
        regenerate_all_channels=bool(body.regenerate_all_channels),
    )


@router.post("/{campaign_id}/regenerate-all-messages")
async def regenerate_all_messages(
    campaign_id: str,
    body: RegenerateMessagesRequest,
    account_ctx: dict = Depends(get_account_context),
):
    """Regenerate messages for every enrollment in the campaign (any status)."""
    account_id = ObjectId(account_ctx["account"]["_id"])
    campaign = await _get_campaign_or_404(campaign_id, account_id)

    cursor = campaign_enrollments_collection.find({
        "campaign_id": campaign["_id"],
        "status": {"$in": ["active", "enrolled", "enriching", "paused"]},
    })
    enrollments = await cursor.to_list(length=2000)
    if not enrollments:
        return {"queued": 0, "succeeded": 0, "failed": 0}

    enr_ids = [e["_id"] for e in enrollments]
    await campaign_enrollments_collection.update_many(
        {"_id": {"$in": enr_ids}},
        {"$set": {"message_gen_status": "pending", "message_gen_error": None}},
    )

    return await _regenerate_messages_for_enrollments(
        campaign, enrollments,
        additional_instructions=body.additional_instructions,
        regenerate_all_channels=bool(body.regenerate_all_channels),
    )


@router.post("/{campaign_id}/enrich-and-generate")
async def enrich_and_generate_for_day_endpoint(
    campaign_id: str,
    day: int,
    body: EnrichAndGenerateRequest,
    account_ctx=Depends(get_account_context),
):
    """Run full enrichment on day-N prospects (idempotent) then regenerate messages per-channel."""
    account_id = ObjectId(account_ctx["account"]["_id"])

    campaign = await campaigns_collection.find_one({"_id": ObjectId(campaign_id), "account_id": account_id})
    if not campaign:
        raise HTTPException(status_code=404, detail="Campaign not found")
    if not campaign.get("is_smart_campaign"):
        raise HTTPException(status_code=400, detail="Only smart campaigns support enrich-and-generate")

    # Reject if already running
    if campaign.get("message_gen_status") in {"queued", "running"}:
        raise HTTPException(status_code=409, detail="Message generation already in progress for this campaign")

    # Count scope
    all_enrollments = await campaign_enrollments_collection.find(
        {
            "account_id": {"$in": [account_id, str(account_id)]},
            "campaign_id": {"$in": [ObjectId(campaign_id), campaign_id]},
            "smart_campaign_send_day": day,
            "status": {"$nin": ["archived", "skipped_no_channel", "cascade_waiting"]},
        },
        {"_id": 1, "smart_campaign_channel": 1}
    ).to_list(length=5000)

    scope = {"email_count": 0, "linkedin_connection_count": 0, "linkedin_inmail_count": 0}
    for e in all_enrollments:
        ch = e.get("smart_campaign_channel", "")
        if ch == "email":
            scope["email_count"] += 1
        elif ch == "linkedin_connection":
            scope["linkedin_connection_count"] += 1
        elif ch == "linkedin_inmail":
            scope["linkedin_inmail_count"] += 1

    if not all_enrollments:
        raise HTTPException(status_code=422, detail=f"No prospects scheduled for Day {day}")

    # Persist per-channel instructions + toggle on campaign
    now = datetime.utcnow()
    updated_campaign = await campaigns_collection.find_one_and_update(
        {
            "_id": ObjectId(campaign_id), "account_id": account_id,
            "message_gen_status": {"$nin": ["queued", "running"]},
        },
        {
            "$set": {
            "per_channel_message_instructions": body.instructions.model_dump(),
            "send_empty_connection_request": body.send_empty_connection_request,
            "message_gen_status": "queued",
            "message_gen_started_at": now,
            "message_gen_completed_at": None,
            },
            "$inc": {"message_gen_generation": 1},
        },
        return_document=ReturnDocument.AFTER,
    )
    if updated_campaign is None:
        raise HTTPException(status_code=409, detail="Campaign changed before work could be queued")
    from services.enrichment_job_service import enqueue_campaign_day_run
    try:
        job = await enqueue_campaign_day_run(
            account_id=str(account_id), campaign_id=campaign_id, day=day,
            generation=int(updated_campaign.get("message_gen_generation", 1)),
            request=body.model_dump(mode="json"),
        )
    except Exception:
        await campaigns_collection.update_one(
            {"_id": ObjectId(campaign_id), "account_id": account_id},
            {"$set": {"message_gen_status": "failed", "message_gen_completed_at": datetime.utcnow()}},
        )
        raise HTTPException(status_code=503, detail="Could not queue campaign work")

    return {
        "status": "queued",
        "job_id": str(job.id),
        "day": day,
        "scope": scope,
        "total_prospects": len(all_enrollments),
    }


@router.get("/{campaign_id}/teammate-review")
async def get_teammate_review_enrollments(
    campaign_id: str,
    account_ctx: dict = Depends(get_account_context),
):
    """Return enrollments with status 'pending_teammate_review' for this campaign."""
    account_id = account_ctx["account"]["_id"]
    docs = await database.campaign_enrollments_collection.find(
        {
            "campaign_id": ObjectId(campaign_id),
            "account_id": {"$in": [account_id, str(account_id)]},
            "status": "pending_teammate_review",
        }
    ).to_list(length=500)
    return {"enrollments": [serialize_doc(d) for d in docs]}


@router.post("/{campaign_id}/teammate-review/{enrollment_id}/approve")
async def approve_teammate_review(
    campaign_id: str,
    enrollment_id: str,
    account_ctx: dict = Depends(get_account_context),
):
    """Advance an enrollment from 'pending_teammate_review' to 'scoring' for replan pickup."""
    now = datetime.utcnow()
    result = await database.campaign_enrollments_collection.update_one(
        {
            "_id": ObjectId(enrollment_id),
            "campaign_id": ObjectId(campaign_id),
            "status": "pending_teammate_review",
        },
        {"$set": {"status": "scoring", "last_activity_at": now, "teammate_conflict_approved_at": now}},
    )
    return {"updated": result.modified_count}
