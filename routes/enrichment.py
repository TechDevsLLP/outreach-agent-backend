"""
Enrichment pipeline API endpoints.
Handles triggering enrichment, tracking runs, and managing outreach.
"""

from fastapi import APIRouter, Depends, HTTPException, BackgroundTasks, Query
from typing import Optional
from datetime import datetime
from bson import ObjectId

from auth import get_account_context
from database import prospects_collection, enrichment_runs_collection, industries_collection
from models.enrichment import EnrichmentTriggerRequest, EnrichByIndustryRequest
from services.enrichment_pipeline import run_enrichment_pipeline
from services.openrouter_service import OpenRouterClient

router = APIRouter(prefix="/api/enrichment", tags=["Enrichment"])


@router.post("/trigger")
async def trigger_enrichment(
    request: EnrichmentTriggerRequest,
    background_tasks: BackgroundTasks,
    account_ctx: dict = Depends(get_account_context),
):
    """
    Start the enrichment pipeline as a background task.
    Returns run_id immediately; pipeline runs async.
    """
    account_id = ObjectId(account_ctx["account"]["_id"])
    triggered_by: str = account_ctx["user"]["_id"]

    # Resolve prospect IDs
    prospect_ids = []

    if request.prospect_ids:
        # Validate provided prospect IDs exist
        for lid in request.prospect_ids:
            try:
                ObjectId(lid)
            except Exception:
                raise HTTPException(status_code=400, detail=f"Invalid prospect ID: {lid}")
        prospect_ids = request.prospect_ids

    elif request.enrich_all_unenriched:
        # Find unenriched leads
        query: dict = {"account_id": account_id}
        if request.force_re_enrich:
            pass  # Get all leads
        else:
            query["enrichment_status"] = {"$in": ["not_started", None]}

        if request.filter_min_score is not None:
            query["prospect_score"] = {"$gte": request.filter_min_score}
        if request.filter_status:
            query["status"] = request.filter_status

        cursor = prospects_collection.find(query, {"_id": 1}).limit(request.max_prospects)
        docs = await cursor.to_list(request.max_prospects)
        prospect_ids = [str(doc["_id"]) for doc in docs]

    if not prospect_ids:
        raise HTTPException(status_code=400, detail="No prospects found to enrich")

    # Cap at max_prospects
    if len(prospect_ids) > request.max_prospects:
        prospect_ids = prospect_ids[:request.max_prospects]

    # Create enrichment run document
    run_doc = {
        "account_id": account_id,
        "status": "running",
        "total_prospects": len(prospect_ids),
        "prospects_processed": 0,
        "prospects_skipped": 0,
        "prospects_failed": 0,
        "profiles_scraped": 0,
        "companies_scraped": 0,
        "companies_deduplicated": 0,
        "ai_assessments_done": 0,
        "outreach_generated": 0,
        "prospect_ids": prospect_ids,
        "started_at": datetime.utcnow(),
        "completed_at": None,
        "current_step": "initializing",
        "error": None,
        "triggered_by": triggered_by,
    }
    result = await enrichment_runs_collection.insert_one(run_doc)
    run_id = str(result.inserted_id)

    # Build options
    options = {
        "skip_profile_scrape": request.skip_profile_scrape,
        "skip_company_scrape": request.skip_company_scrape,
        "skip_ai_assessment": request.skip_ai_assessment,
        "skip_outreach": request.skip_outreach,
        # User explicitly selected these prospects — always run the full pipeline.
        "skip_pre_enrichment_triage": True,
    }

    # Launch pipeline as background task
    background_tasks.add_task(run_enrichment_pipeline, run_id, prospect_ids, options, triggered_by)

    return {
        "enrichment_run_id": run_id,
        "total_prospects": len(prospect_ids),
        "status": "running",
        "message": f"Enrichment pipeline started for {len(prospect_ids)} leads",
    }


@router.post("/assess")
async def trigger_ai_assessment(
    background_tasks: BackgroundTasks,
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
    account_id = ObjectId(account_ctx["account"]["_id"])
    if prospect_ids:
        for lid in prospect_ids:
            try:
                ObjectId(lid)
            except Exception:
                raise HTTPException(status_code=400, detail=f"Invalid prospect ID: {lid}")
    else:
        # Find prospects that have been scraped but not yet AI-assessed
        query = {
            "account_id": account_id,
            "enrichment_status": {"$in": ["profile_scraped", "company_scraped"]},
        }
        cursor = prospects_collection.find(query, {"_id": 1}).limit(max_prospects)
        docs = await cursor.to_list(max_prospects)
        prospect_ids = [str(doc["_id"]) for doc in docs]

    if not prospect_ids:
        raise HTTPException(status_code=400, detail="No scraped prospects found to assess")

    prospect_ids = prospect_ids[:max_prospects]

    run_doc = {
        "account_id": account_id,
        "status": "running",
        "total_prospects": len(prospect_ids),
        "prospects_processed": 0,
        "prospects_skipped": 0,
        "prospects_failed": 0,
        "profiles_scraped": 0,
        "companies_scraped": 0,
        "companies_deduplicated": 0,
        "ai_assessments_done": 0,
        "outreach_generated": 0,
        "prospect_ids": prospect_ids,
        "started_at": datetime.utcnow(),
        "completed_at": None,
        "current_step": "initializing",
        "error": None,
    }
    result = await enrichment_runs_collection.insert_one(run_doc)
    run_id = str(result.inserted_id)

    options = {
        "skip_profile_scrape": True,
        "skip_company_scrape": True,
        "skip_ai_assessment": False,
        "skip_outreach": not include_outreach,
        "skip_pre_enrichment_triage": True,
    }

    background_tasks.add_task(run_enrichment_pipeline, run_id, prospect_ids, options)

    return {
        "enrichment_run_id": run_id,
        "total_prospects": len(prospect_ids),
        "status": "running",
        "message": f"AI assessment started for {len(prospect_ids)} prospects (outreach: {include_outreach})",
    }


@router.get("/runs")
async def list_enrichment_runs(
    page: int = Query(1, ge=1),
    page_size: int = Query(20, ge=1, le=100),
    status: Optional[str] = None,
    account_ctx: dict = Depends(get_account_context),
):
    """List enrichment runs with pagination."""
    account_id = ObjectId(account_ctx["account"]["_id"])
    query: dict = {"account_id": account_id}
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
    account_id = ObjectId(account_ctx["account"]["_id"])
    try:
        run = await enrichment_runs_collection.find_one({"_id": ObjectId(run_id), "account_id": account_id})
    except Exception:
        raise HTTPException(status_code=400, detail="Invalid run ID")

    if not run:
        raise HTTPException(status_code=404, detail="Enrichment run not found")

    run["_id"] = str(run["_id"])
    return run


@router.get("/status")
async def get_enrichment_status(account_ctx: dict = Depends(get_account_context)):
    """Get summary of enrichment status counts across all leads."""
    account_id = ObjectId(account_ctx["account"]["_id"])
    pipeline = [
        {"$match": {"account_id": account_id}},
        {"$group": {"_id": "$enrichment_status", "count": {"$sum": 1}}},
    ]
    results = await prospects_collection.aggregate(pipeline).to_list(20)

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
    account_id = ObjectId(account_ctx["account"]["_id"])
    try:
        prospect = await prospects_collection.find_one({"_id": ObjectId(prospect_id), "account_id": account_id})
    except Exception:
        raise HTTPException(status_code=400, detail="Invalid prospect ID")

    if not prospect:
        raise HTTPException(status_code=404, detail="Prospect not found")

    prospect["_id"] = str(prospect["_id"])

    return {
        "prospect_id": prospect["_id"],
        "full_name": prospect.get("full_name"),
        "email": prospect.get("email"),
        "company_name": prospect.get("company_name"),
        "enrichment_status": prospect.get("enrichment_status", "not_started"),
        "enrichment_error": prospect.get("enrichment_error"),
        "enrichment_started_at": prospect.get("enrichment_started_at"),
        "enrichment_completed_at": prospect.get("enrichment_completed_at"),
        "linkedin_profile_data": prospect.get("linkedin_profile_data"),
        "company_linkedin_data": prospect.get("company_linkedin_data"),
        "ai_assessment": prospect.get("ai_assessment"),
        "ai_prospect_score": prospect.get("ai_prospect_score"),
        "ai_score_breakdown": prospect.get("ai_score_breakdown"),
        "outreach_messages": prospect.get("outreach_messages"),
    }


@router.post("/enrich-by-industry")
async def enrich_top_prospects_by_industry(
    request: EnrichByIndustryRequest,
    background_tasks: BackgroundTasks,
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
    account_id = ObjectId(account_ctx["account"]["_id"])
    # Resolve industries
    if request.industry_ids:
        for iid in request.industry_ids:
            try:
                ObjectId(iid)
            except Exception:
                raise HTTPException(status_code=400, detail=f"Invalid industry ID: {iid}")
        industries = await industries_collection.find(
            {"account_id": account_id, "_id": {"$in": [ObjectId(iid) for iid in request.industry_ids]}}
        ).to_list(100)
    else:
        industries = await industries_collection.find({"account_id": account_id, "is_active": True}).to_list(100)

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

        # Find top unenriched prospects for this industry
        query = {
            "account_id": account_id,
            "industry_id": ind_id,
            "enrichment_status": {"$in": ["not_started", None]},
            "linkedin": {"$ne": None},
        }
        if request.min_score > 0:
            query["prospect_score"] = {"$gte": request.min_score}

        cursor = prospects_collection.find(
            query, {"_id": 1}
        ).sort("prospect_score", -1).limit(request.max_per_industry)
        docs = await cursor.to_list(request.max_per_industry)
        prospect_ids = [str(doc["_id"]) for doc in docs]

        if not prospect_ids:
            runs.append({
                "industry_id": ind_id,
                "industry_name": industry["name"],
                "status": "skipped",
                "reason": "no unenriched prospects found",
            })
            continue

        # Create enrichment run for this industry
        run_doc = {
            "account_id": account_id,
            "status": "running",
            "trigger": "enrich_by_industry",
            "industry_id": ind_id,
            "total_prospects": len(prospect_ids),
            "prospects_processed": 0,
            "prospects_skipped": 0,
            "prospects_failed": 0,
            "profiles_scraped": 0,
            "companies_scraped": 0,
            "companies_deduplicated": 0,
            "ai_assessments_done": 0,
            "outreach_generated": 0,
            "prospect_ids": prospect_ids,
            "started_at": datetime.utcnow(),
            "completed_at": None,
            "current_step": "initializing",
            "error": None,
        }
        result = await enrichment_runs_collection.insert_one(run_doc)
        run_id = str(result.inserted_id)

        background_tasks.add_task(run_enrichment_pipeline, run_id, prospect_ids, options)

        runs.append({
            "industry_id": ind_id,
            "industry_name": industry["name"],
            "enrichment_run_id": run_id,
            "prospects_queued": len(prospect_ids),
            "status": "running",
        })

    launched = [r for r in runs if r["status"] == "running"]
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
    account_id = ObjectId(account_ctx["account"]["_id"])
    pipeline = [
        {"$match": {"account_id": account_id}},
        {"$group": {
            "_id": {
                "industry_id": "$industry_id",
                "enrichment_status": "$enrichment_status",
            },
            "count": {"$sum": 1},
            "avg_score": {"$avg": "$prospect_score"},
            "top_score": {"$max": "$prospect_score"},
        }},
    ]
    results = await prospects_collection.aggregate(pipeline).to_list(500)

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
                {"account_id": account_id, "_id": {"$in": valid_oids}}, {"name": 1}
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
    account_id = ObjectId(account_ctx["account"]["_id"])
    try:
        ObjectId(industry_id)
    except Exception:
        raise HTTPException(status_code=400, detail="Invalid industry ID")

    # Get industry name
    industry = await industries_collection.find_one({"_id": ObjectId(industry_id), "account_id": account_id})
    industry_name = industry["name"] if industry else "Unknown"

    # Aggregation for this industry
    pipeline = [
        {"$match": {"account_id": account_id, "industry_id": industry_id}},
        {"$group": {
            "_id": "$enrichment_status",
            "count": {"$sum": 1},
            "avg_score": {"$avg": "$prospect_score"},
            "top_score": {"$max": "$prospect_score"},
        }},
    ]
    results = await prospects_collection.aggregate(pipeline).to_list(20)

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

    # Top 10 enriched prospects
    top_prospects_cursor = prospects_collection.find(
        {"account_id": account_id, "industry_id": industry_id, "enrichment_status": "completed"},
        {"full_name": 1, "email": 1, "company_name": 1, "prospect_score": 1, "ai_prospect_score": 1},
    ).sort("prospect_score", -1).limit(10)
    top_prospects = await top_prospects_cursor.to_list(10)
    for p in top_prospects:
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
