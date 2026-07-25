"""
Enrichment pipeline API endpoints.
Handles triggering enrichment, tracking runs, and managing outreach.
"""

from fastapi import APIRouter, Depends, HTTPException, Query
from typing import Optional
from datetime import datetime
from bson import ObjectId

from auth import get_account_context
from database import (
    prospects_collection,
    enrichment_runs_collection,
    industries_collection,
    prospect_state_collection,
    campaign_enrollments_collection,
    campaign_prospect_state_collection,
    campaigns_collection,
)
from models.enrichment import EnrichmentTriggerRequest, EnrichByIndustryRequest
from services.enrichment_job_service import enqueue_enrichment_run

router = APIRouter(prefix="/api/enrichment", tags=["Enrichment"])


def _account_variants(account_id: str) -> list:
    return [str(account_id), ObjectId(str(account_id))]


async def _require_campaign(account_id: str, campaign_id: str | None) -> None:
    if not campaign_id:
        return
    if not ObjectId.is_valid(campaign_id):
        raise HTTPException(status_code=400, detail="Invalid campaign ID")
    campaign = await campaigns_collection.find_one(
        {"_id": ObjectId(campaign_id), "account_id": {"$in": _account_variants(account_id)}},
        {"_id": 1},
    )
    if campaign is None:
        raise HTTPException(status_code=404, detail="Campaign not found")


async def _authorized_prospect_ids(
    *, account_id: str, prospect_ids: list[str], campaign_id: str | None = None
) -> list[str]:
    """Return only IDs owned through the tenant overlay/campaign boundary."""
    normalized = []
    for value in prospect_ids:
        if not ObjectId.is_valid(value):
            raise HTTPException(status_code=400, detail=f"Invalid prospect ID: {value}")
        normalized.append(str(ObjectId(value)))
    if not normalized:
        return []

    await _require_campaign(account_id, campaign_id)
    state_query: dict = {
        "account_id": {"$in": _account_variants(account_id)},
        "prospect_id": {"$in": normalized + [ObjectId(value) for value in normalized]},
    }
    if campaign_id:
        state_query["campaign_id"] = str(campaign_id)
        state_cursor = campaign_prospect_state_collection.find(state_query, {"prospect_id": 1})
    else:
        state_cursor = prospect_state_collection.find(state_query, {"prospect_id": 1})
    authorized = {str(doc["prospect_id"]) async for doc in state_cursor}

    enrollment_query: dict = {
        "account_id": {"$in": _account_variants(account_id)},
        "prospect_id": {"$in": normalized + [ObjectId(value) for value in normalized]},
    }
    if campaign_id:
        enrollment_query["campaign_id"] = {"$in": [str(campaign_id), ObjectId(campaign_id)]}
    enrollment_cursor = campaign_enrollments_collection.find(enrollment_query, {"prospect_id": 1})
    async for doc in enrollment_cursor:
        authorized.add(str(doc["prospect_id"]))

    existing_cursor = prospects_collection.find(
        {"_id": {"$in": [ObjectId(value) for value in normalized]}}, {"_id": 1}
    )
    existing = {str(doc["_id"]) async for doc in existing_cursor}
    missing = [value for value in normalized if value not in authorized or value not in existing]
    if missing:
        # Do not reveal whether a shared prospect exists outside this tenant.
        raise HTTPException(status_code=404, detail="One or more prospects were not found")
    return normalized


async def _tenant_prospect_ids(
    *, account_id: str, campaign_id: str | None, max_prospects: int,
    statuses: list[str] | None = None, min_score: float | None = None,
    tenant_status: str | None = None,
) -> list[str]:
    await _require_campaign(account_id, campaign_id)
    if campaign_id:
        query: dict = {
            "account_id": str(account_id),
            "campaign_id": str(campaign_id),
        }
        if statuses:
            query["enrichment.state"] = {"$in": statuses}
        if min_score is not None:
            query["score.value"] = {"$gte": min_score}
        cursor = campaign_prospect_state_collection.find(query, {"prospect_id": 1}).sort("score.value", -1).limit(max_prospects)
    else:
        query = {"account_id": {"$in": _account_variants(account_id)}}
        if tenant_status:
            query["status"] = tenant_status
        if statuses:
            query["enrichment_status"] = {"$in": statuses}
        if min_score is not None:
            query["prospect_score"] = {"$gte": min_score}
        cursor = prospect_state_collection.find(query, {"prospect_id": 1}).sort("prospect_score", -1).limit(max_prospects)
    return [str(doc["prospect_id"]) async for doc in cursor]


async def _create_and_enqueue_run(
    *, account_id: str, prospect_ids: list[str], options: dict,
    triggered_by: str | None, campaign_id: str | None, trigger: str = "manual",
) -> str:
    now = datetime.utcnow()
    run_doc = {
        "account_id": str(account_id), "campaign_id": campaign_id,
        "status": "queued", "trigger": trigger,
        "total_prospects": len(prospect_ids), "prospects_processed": 0,
        "prospects_skipped": 0, "prospects_failed": 0,
        "profiles_scraped": 0, "companies_scraped": 0,
        "companies_deduplicated": 0, "ai_assessments_done": 0,
        "outreach_generated": 0, "prospect_ids": prospect_ids,
        "started_at": None, "created_at": now, "completed_at": None,
        "current_step": "queued", "error": None, "triggered_by": triggered_by,
    }
    result = await enrichment_runs_collection.insert_one(run_doc)
    run_id = str(result.inserted_id)
    await enqueue_enrichment_run(
        account_id=account_id, run_id=run_id, prospect_ids=prospect_ids,
        options=options, triggered_by=triggered_by, campaign_id=campaign_id,
    )
    return run_id


@router.post("/trigger")
async def trigger_enrichment(
    request: EnrichmentTriggerRequest,
    account_ctx: dict = Depends(get_account_context),
):
    """
    Start the enrichment pipeline as a background task.
    Returns run_id immediately; pipeline runs async.
    """
    account_id = str(account_ctx["account"]["_id"])
    triggered_by: str = account_ctx["user"]["_id"]

    # Resolve prospect IDs
    prospect_ids = []

    if request.prospect_ids:
        prospect_ids = await _authorized_prospect_ids(
            account_id=account_id,
            prospect_ids=request.prospect_ids,
            campaign_id=request.campaign_id,
        )

    elif request.enrich_all_unenriched:
        statuses = None if request.force_re_enrich else (
            ["queued", "retryable_failure", "not_found"]
            if request.campaign_id else ["not_started", "failed", None]
        )
        prospect_ids = await _tenant_prospect_ids(
            account_id=account_id,
            campaign_id=request.campaign_id,
            max_prospects=request.max_prospects,
            statuses=statuses,
            min_score=request.filter_min_score,
            tenant_status=request.filter_status,
        )

    if not prospect_ids:
        raise HTTPException(status_code=400, detail="No prospects found to enrich")

    # Cap at max_prospects
    if len(prospect_ids) > request.max_prospects:
        prospect_ids = prospect_ids[:request.max_prospects]

    # Build options
    options = {
        "skip_profile_scrape": request.skip_profile_scrape,
        "skip_company_scrape": request.skip_company_scrape,
        "skip_ai_assessment": request.skip_ai_assessment,
        "skip_outreach": request.skip_outreach,
        # User explicitly selected these prospects — always run the full pipeline.
        "skip_pre_enrichment_triage": True,
    }

    run_id = await _create_and_enqueue_run(
        account_id=account_id, prospect_ids=prospect_ids, options=options,
        triggered_by=triggered_by, campaign_id=request.campaign_id,
    )

    return {
        "enrichment_run_id": run_id,
        "total_prospects": len(prospect_ids),
        "status": "queued",
        "message": f"Enrichment queued for {len(prospect_ids)} prospects",
    }


@router.post("/assess")
async def trigger_ai_assessment(
    prospect_ids: Optional[list[str]] = None,
    include_outreach: bool = True,
    max_prospects: int = Query(default=100, ge=1, le=500),
    account_ctx: dict = Depends(get_account_context),
):
    """
    Run AI assessment (+ outreach) for prospects that already have scraped data.
    Skips profile and company scraping phases entirely.
    If no prospect_ids provided, finds all scraped-but-unassessed leads.
    """
    account_id = str(account_ctx["account"]["_id"])
    if prospect_ids:
        prospect_ids = await _authorized_prospect_ids(
            account_id=account_id, prospect_ids=prospect_ids
        )
    else:
        prospect_ids = await _tenant_prospect_ids(
            account_id=account_id, campaign_id=None, max_prospects=max_prospects,
            statuses=["profile_scraped", "company_scraped"],
        )

    if not prospect_ids:
        raise HTTPException(status_code=400, detail="No scraped prospects found to assess")

    prospect_ids = prospect_ids[:max_prospects]

    options = {
        "skip_profile_scrape": True,
        "skip_company_scrape": True,
        "skip_ai_assessment": False,
        "skip_outreach": not include_outreach,
        "skip_pre_enrichment_triage": True,
    }

    run_id = await _create_and_enqueue_run(
        account_id=account_id, prospect_ids=prospect_ids, options=options,
        triggered_by=str(account_ctx["user"]["_id"]), campaign_id=None,
        trigger="ai_assessment",
    )

    return {
        "enrichment_run_id": run_id,
        "total_prospects": len(prospect_ids),
        "status": "queued",
        "message": f"AI assessment queued for {len(prospect_ids)} prospects (outreach: {include_outreach})",
    }


@router.get("/runs")
async def list_enrichment_runs(
    page: int = Query(1, ge=1),
    page_size: int = Query(20, ge=1, le=100),
    status: Optional[str] = None,
    account_ctx: dict = Depends(get_account_context),
):
    """List enrichment runs with pagination."""
    account_id = str(account_ctx["account"]["_id"])
    query: dict = {"account_id": {"$in": _account_variants(account_id)}}
    if status:
        query["status"] = status

    total = await enrichment_runs_collection.count_documents(query)
    skip = (page - 1) * page_size

    cursor = enrichment_runs_collection.find(query).sort("started_at", -1).skip(skip).limit(page_size)
    runs = await cursor.to_list(page_size)

    for run in runs:
        run["_id"] = str(run["_id"])

    return {
        "total": total,
        "page": page,
        "page_size": page_size,
        "total_pages": (total + page_size - 1) // page_size,
        "runs": runs,
    }


@router.get("/runs/{run_id}")
async def get_enrichment_run(run_id: str, account_ctx: dict = Depends(get_account_context)):
    """Get enrichment run details and progress."""
    account_id = str(account_ctx["account"]["_id"])
    try:
        run = await enrichment_runs_collection.find_one({"_id": ObjectId(run_id), "account_id": {"$in": _account_variants(account_id)}})
    except Exception:
        raise HTTPException(status_code=400, detail="Invalid run ID")

    if not run:
        raise HTTPException(status_code=404, detail="Enrichment run not found")

    run["_id"] = str(run["_id"])
    return run


@router.get("/status")
async def get_enrichment_status(account_ctx: dict = Depends(get_account_context)):
    """Get summary of enrichment status counts across all leads."""
    account_id = str(account_ctx["account"]["_id"])
    pipeline = [
        {"$match": {"account_id": {"$in": _account_variants(account_id)}}},
        {"$group": {"_id": "$enrichment_status", "count": {"$sum": 1}}},
    ]
    results = await prospect_state_collection.aggregate(pipeline).to_list(20)

    status_counts = {}
    for r in results:
        key = r["_id"] if r["_id"] else "not_started"
        status_counts[key] = r["count"]

    total = sum(status_counts.values())

    return {
        "total_prospects": total,
        "status_counts": status_counts,
    }


@router.get("/leads/{prospect_id}")
async def get_lead_enrichment(prospect_id: str, account_ctx: dict = Depends(get_account_context)):
    """Get enrichment data for a specific prospect."""
    account_id = str(account_ctx["account"]["_id"])
    try:
        authorized_ids = await _authorized_prospect_ids(
            account_id=account_id, prospect_ids=[prospect_id]
        )
        prospect = await prospects_collection.find_one({"_id": ObjectId(authorized_ids[0])})
    except Exception:
        raise HTTPException(status_code=400, detail="Invalid prospect ID")

    if not prospect:
        raise HTTPException(status_code=404, detail="Prospect not found")

    state = await prospect_state_collection.find_one({
        "account_id": {"$in": _account_variants(account_id)},
        "prospect_id": {"$in": [prospect_id, ObjectId(prospect_id)]},
    }) or {}
    prospect["_id"] = str(prospect["_id"])

    return {
        "prospect_id": prospect["_id"],
        "full_name": prospect.get("full_name"),
        "email": prospect.get("email"),
        "company_name": prospect.get("company_name"),
        "enrichment_status": state.get("enrichment_status", "not_started"),
        "enrichment_error": state.get("enrichment_error"),
        "enrichment_started_at": state.get("enrichment_started_at"),
        "enrichment_completed_at": state.get("enrichment_completed_at"),
        "linkedin_profile_data": prospect.get("linkedin_profile_data"),
        "company_linkedin_data": prospect.get("company_linkedin_data"),
        "ai_assessment": state.get("ai_assessment"),
        "ai_prospect_score": state.get("ai_score", state.get("prospect_score")),
        "ai_score_breakdown": state.get("ai_score_breakdown"),
        "outreach_messages": state.get("outreach_messages"),
    }


# ---------------------------------------------------------------------------
# On-demand phone unlock (GrowthToolkit LinkedIn enrichment, 1 credit/call).
# USER-TRIGGERED ONLY: this endpoint must NEVER be called from enrollment or
# pipeline code — each call can spend a paid credit.
# ---------------------------------------------------------------------------

def _first_phone(phones: list) -> Optional[str]:
    """Extract a displayable number from a phone list entry (str or dict)."""
    for item in phones:
        if isinstance(item, str) and item.strip():
            return item.strip()
        if isinstance(item, dict):
            for key in ("number", "phone", "phone_number", "value"):
                val = item.get(key)
                if isinstance(val, str) and val.strip():
                    return val.strip()
    return None


@router.post("/prospects/{prospect_id}/unlock-phone")
async def unlock_prospect_phone(prospect_id: str, account_ctx: dict = Depends(get_account_context)):
    """Unlock a prospect's phone number via GrowthToolkit LinkedIn enrichment.

    Returns cached phone data without spending a credit if the prospect
    already has a phone number.
    """
    # GrowthToolkit is restricted to email finding only ($0.0075/lookup).
    # Phone unlock spends $0.50/credit on the same wallet, so it is disabled.
    raise HTTPException(
        status_code=403,
        detail="Phone unlock is disabled — GrowthToolkit is restricted to email finding only",
    )

    from services.growthtoolkit_service import (
        enrich_linkedin,
        CreditsExhausted,
        InvalidInput,
        GrowthToolkitError,
    )

    account_oid = ObjectId(account_ctx["account"]["_id"])
    account_id_str = str(account_oid)

    try:
        pid = ObjectId(prospect_id)
    except Exception:
        raise HTTPException(status_code=400, detail="Invalid prospect ID")

    prospect = await prospects_collection.find_one({"_id": pid})
    if not prospect:
        raise HTTPException(status_code=404, detail="Prospect not found")

    # Authorization: same pattern as routes/prospects.py — this account must
    # have a prospect_state or an enrollment for this prospect (fallback:
    # old schema with account_id directly on the prospect doc).
    authorized = await prospect_state_collection.find_one(
        {"account_id": {"$in": [account_id_str, account_oid]}, "prospect_id": prospect_id},
        {"_id": 1},
    )
    if not authorized:
        enr = await campaign_enrollments_collection.find_one(
            {"prospect_id": pid, "account_id": {"$in": [account_oid, account_id_str]}},
            {"_id": 1},
        )
        if not enr and prospect.get("account_id") not in (account_oid, account_id_str):
            raise HTTPException(status_code=404, detail="Prospect not found")

    linkedin_url = prospect.get("linkedin") or prospect.get("linkedin_url")
    if not linkedin_url:
        raise HTTPException(
            status_code=400,
            detail="Prospect has no LinkedIn URL — phone unlock requires a LinkedIn profile",
        )

    # Cached: don't spend a credit if we already have a phone number.
    existing_phones = prospect.get("phone_numbers") or []
    existing_mobile = prospect.get("mobile_number")
    if existing_mobile or existing_phones:
        return {
            "phone_numbers": existing_phones or ([existing_mobile] if existing_mobile else []),
            "mobile_number": existing_mobile or _first_phone(existing_phones),
            "cached": True,
        }

    try:
        result = await enrich_linkedin(
            linkedin_url,
            unlock_phone=True,
            account_id=account_id_str,
            prospect_id=prospect_id,
        )
    except CreditsExhausted:
        raise HTTPException(
            status_code=402,
            detail="GrowthToolkit credits exhausted — top up credits at appconnector.pro to unlock phone numbers",
        )
    except InvalidInput as e:
        raise HTTPException(status_code=400, detail=f"GrowthToolkit rejected the LinkedIn URL: {e}")
    except GrowthToolkitError as e:
        raise HTTPException(status_code=502, detail=f"GrowthToolkit request failed: {e}")

    if not result:
        raise HTTPException(status_code=404, detail="Phone not available for this prospect")

    unlock_details = result.get("unlock_details") or {}
    phones = unlock_details.get("phone_numbers") or result.get("phone_numbers") or []
    mobile = _first_phone(phones)
    if not mobile:
        raise HTTPException(status_code=404, detail="Phone not available for this prospect")

    await prospects_collection.update_one(
        {"_id": pid},
        {"$set": {
            "mobile_number": mobile,
            "phone_numbers": phones,
            "phone_source": "growthtoolkit",
            "phone_unlocked_at": datetime.utcnow(),
            "phone_unlocked_by_account": account_id_str,
        }},
    )

    return {
        "phone_numbers": phones,
        "mobile_number": mobile,
        "cached": False,
    }


@router.post("/enrich-by-industry")
async def enrich_top_prospects_by_industry(
    request: EnrichByIndustryRequest,
    account_ctx: dict = Depends(get_account_context),
):
    """
    Enrich top-scored unenriched prospects from each active industry.
    Launches one enrichment run per industry in parallel.

    - If industry_ids provided, only those industries are processed.
    - Otherwise, all active industries are used.
    - Selects up to max_per_industry unenriched prospects per industry,
      sorted by prospect_score descending.
    """
    account_id = str(account_ctx["account"]["_id"])
    # Resolve industries
    if request.industry_ids:
        for iid in request.industry_ids:
            try:
                ObjectId(iid)
            except Exception:
                raise HTTPException(status_code=400, detail=f"Invalid industry ID: {iid}")
        industries = await industries_collection.find(
            {"account_id": {"$in": _account_variants(account_id)}, "_id": {"$in": [ObjectId(iid) for iid in request.industry_ids]}}
        ).to_list(100)
    else:
        industries = await industries_collection.find({"account_id": {"$in": _account_variants(account_id)}, "is_active": True}).to_list(100)

    if not industries:
        raise HTTPException(status_code=404, detail="No industries found")

    options = {
        "skip_profile_scrape": request.skip_profile_scrape,
        "skip_company_scrape": request.skip_company_scrape,
        "skip_ai_assessment": request.skip_ai_assessment,
        "skip_outreach": request.skip_outreach,
        "skip_pre_enrichment_triage": request.skip_pre_enrichment_triage,
    }

    runs = []
    for industry in industries:
        ind_id = str(industry["_id"])

        query = {
            "account_id": {"$in": _account_variants(account_id)},
            "source_industry_ids": ind_id,
            "enrichment_status": {"$in": ["not_started", "failed", None]},
        }
        if request.min_score > 0:
            query["prospect_score"] = {"$gte": request.min_score}
        cursor = prospect_state_collection.find(query, {"prospect_id": 1}).sort("prospect_score", -1).limit(request.max_per_industry)
        prospect_ids = [str(doc["prospect_id"]) async for doc in cursor]
        prospect_ids = await _authorized_prospect_ids(
            account_id=account_id, prospect_ids=prospect_ids,
            campaign_id=request.campaign_id,
        ) if prospect_ids else []

        if not prospect_ids:
            runs.append({
                "industry_id": ind_id,
                "industry_name": industry["name"],
                "status": "skipped",
                "reason": "no unenriched prospects found",
            })
            continue

        run_id = await _create_and_enqueue_run(
            account_id=account_id, prospect_ids=prospect_ids, options=options,
            triggered_by=str(account_ctx["user"]["_id"]),
            campaign_id=request.campaign_id, trigger="enrich_by_industry",
        )

        runs.append({
            "industry_id": ind_id,
            "industry_name": industry["name"],
            "enrichment_run_id": run_id,
            "prospects_queued": len(prospect_ids),
            "status": "queued",
        })

    launched = [r for r in runs if r["status"] == "queued"]
    skipped = [r for r in runs if r["status"] == "skipped"]

    return {
        "total_industries": len(runs),
        "industries_launched": len(launched),
        "industries_skipped": len(skipped),
        "total_prospects_queued": sum(r.get("prospects_queued", 0) for r in launched),
        "runs": runs,
    }


@router.get("/status/by-industry")
async def get_enrichment_status_by_industry(account_ctx: dict = Depends(get_account_context)):
    """Get enrichment status breakdown per industry."""
    account_id = str(account_ctx["account"]["_id"])
    pipeline = [
        {"$match": {"account_id": {"$in": _account_variants(account_id)}}},
        {"$unwind": {"path": "$source_industry_ids", "preserveNullAndEmptyArrays": True}},
        {"$group": {
            "_id": {
                "industry_id": "$source_industry_ids",
                "enrichment_status": "$enrichment_status",
            },
            "count": {"$sum": 1},
            "avg_score": {"$avg": "$prospect_score"},
            "top_score": {"$max": "$prospect_score"},
        }},
    ]
    results = await prospect_state_collection.aggregate(pipeline).to_list(500)

    # Group by industry
    industry_map = {}
    for r in results:
        ind_id = r["_id"].get("industry_id") or "unknown"
        status = r["_id"].get("enrichment_status") or "not_started"
        if ind_id not in industry_map:
            industry_map[ind_id] = {
                "industry_id": ind_id,
                "total_prospects": 0,
                "enriched_count": 0,
                "enriching_count": 0,
                "not_enriched_count": 0,
                "failed_count": 0,
                "avg_score": 0,
                "top_score": 0,
                "_score_sum": 0,
                "_score_count": 0,
            }
        entry = industry_map[ind_id]
        entry["total_prospects"] += r["count"]
        entry["_score_sum"] += (r["avg_score"] or 0) * r["count"]
        entry["_score_count"] += r["count"]
        entry["top_score"] = max(entry["top_score"], r["top_score"] or 0)

        if status == "completed":
            entry["enriched_count"] += r["count"]
        elif status in ("in_progress", "profile_scraped", "company_scraped", "ai_assessed"):
            entry["enriching_count"] += r["count"]
        elif status in ("not_started", None):
            entry["not_enriched_count"] += r["count"]
        elif status in ("failed", "skipped"):
            entry["failed_count"] += r["count"]

    # Resolve industry names
    industry_ids = [iid for iid in industry_map.keys() if iid != "unknown"]
    industry_names = {}
    if industry_ids:
        valid_oids = []
        for iid in industry_ids:
            try:
                valid_oids.append(ObjectId(iid))
            except Exception:
                pass
        if valid_oids:
            industries = await industries_collection.find(
                {"account_id": {"$in": _account_variants(account_id)}, "_id": {"$in": valid_oids}}, {"name": 1}
            ).to_list(len(valid_oids))
            for ind in industries:
                industry_names[str(ind["_id"])] = ind["name"]

    # Build response
    industry_list = []
    for ind_id, entry in industry_map.items():
        entry["industry_name"] = industry_names.get(ind_id, "Unknown")
        if entry["_score_count"] > 0:
            entry["avg_score"] = round(entry["_score_sum"] / entry["_score_count"], 1)
        total = entry["total_prospects"]
        entry["enrichment_progress_pct"] = round(
            (entry["enriched_count"] / total * 100) if total > 0 else 0, 1
        )
        del entry["_score_sum"]
        del entry["_score_count"]
        industry_list.append(entry)

    industry_list.sort(key=lambda x: x["total_prospects"], reverse=True)

    return {
        "total_industries": len(industry_list),
        "industries": industry_list,
    }


@router.get("/status/by-industry/{industry_id}")
async def get_enrichment_status_for_industry(
    industry_id: str, account_ctx: dict = Depends(get_account_context)
):
    """Get enrichment status for a specific industry with top prospects."""
    account_id = str(account_ctx["account"]["_id"])
    try:
        ObjectId(industry_id)
    except Exception:
        raise HTTPException(status_code=400, detail="Invalid industry ID")

    # Get industry name
    industry = await industries_collection.find_one({"_id": ObjectId(industry_id), "account_id": {"$in": _account_variants(account_id)}})
    industry_name = industry["name"] if industry else "Unknown"

    # Aggregation for this industry
    pipeline = [
        {"$match": {"account_id": {"$in": _account_variants(account_id)}, "source_industry_ids": industry_id}},
        {"$group": {
            "_id": "$enrichment_status",
            "count": {"$sum": 1},
            "avg_score": {"$avg": "$prospect_score"},
            "top_score": {"$max": "$prospect_score"},
        }},
    ]
    results = await prospect_state_collection.aggregate(pipeline).to_list(20)

    status_counts = {}
    total = 0
    avg_score_sum = 0
    top_score = 0
    for r in results:
        key = r["_id"] if r["_id"] else "not_started"
        status_counts[key] = r["count"]
        total += r["count"]
        avg_score_sum += (r["avg_score"] or 0) * r["count"]
        top_score = max(top_score, r["top_score"] or 0)

    enriched = status_counts.get("completed", 0)

    top_states = await prospect_state_collection.find(
        {
            "account_id": {"$in": _account_variants(account_id)},
            "source_industry_ids": industry_id,
            "enrichment_status": "completed",
        },
        {"prospect_id": 1, "prospect_score": 1, "ai_score": 1},
    ).sort("prospect_score", -1).limit(10).to_list(10)
    state_by_id = {str(state["prospect_id"]): state for state in top_states}
    valid_top_ids = [ObjectId(value) for value in state_by_id if ObjectId.is_valid(value)]
    top_prospects = await prospects_collection.find(
        {"_id": {"$in": valid_top_ids}},
        {"full_name": 1, "email": 1, "company_name": 1},
    ).to_list(10)
    for p in top_prospects:
        state = state_by_id.get(str(p["_id"]), {})
        p["prospect_score"] = state.get("prospect_score")
        p["ai_prospect_score"] = state.get("ai_score")
        p["_id"] = str(p["_id"])

    return {
        "industry_id": industry_id,
        "industry_name": industry_name,
        "total_prospects": total,
        "status_counts": status_counts,
        "enriched_count": enriched,
        "enrichment_progress_pct": round((enriched / total * 100) if total > 0 else 0, 1),
        "avg_score": round(avg_score_sum / total, 1) if total > 0 else 0,
        "top_score": top_score,
        "top_enriched_prospects": top_prospects,
    }
