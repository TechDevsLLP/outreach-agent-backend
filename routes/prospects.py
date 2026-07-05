import asyncio
import time
from fastapi import APIRouter, Depends, HTTPException, Query
from typing import Any, Optional
from bson import ObjectId
from auth import get_account_context
from database import (
    prospects_collection,
    prospect_state_collection,
    companies_collection,
    campaign_enrollments_collection,
    campaigns_collection,
)
from services.prospect_service import process_search
from models.search_run import SearchRunCreate, BulkSearchRequest
from utils.serialization import serialize_doc

router = APIRouter(prefix="/api/prospects", tags=["Prospects"])

_LIST_PROJECTION = {
    "_id": 1, "full_name": 1, "first_name": 1, "last_name": 1,
    "email": 1, "job_title": 1, "seniority": 1, "seniority_level": 1, "company_name": 1,
    "company_linkedin": 1, "linkedin": 1, "company_industry_id": 1, "company_industry_group": 1,
    "location": 1, "enrichment_status": 1, "stage": 1,
    "enrichment_started_at": 1, "enrichment_completed_at": 1,
    "ai_prospect_score": 1, "prospect_score": 1,
    "priority_tier": 1, "timezone": 1, "last_updated_at": 1,
    "first_seen_at": 1, "source": 1,
}

_stats_cache: dict[str, tuple[float, Any]] = {}
_STATS_CACHE_TTL = 60.0


@router.post("/search")
async def search_prospects(request: SearchRunCreate, account_ctx=Depends(get_account_context)):
    """
    Trigger a prospect search using an ICP profile or custom params.
    Calls the Apify Prospects Finder actor, deduplicates results,
    scores them, and saves to MongoDB.
    """
    # TODO Phase 1: process_search needs account_id passed in
    try:
        result = await process_search(
            industry_id=request.industry_id,
            custom_params=request.custom_params,
            fetch_count=request.fetch_count,
            start_page=request.start_page,
        )
        return result
    except ValueError as e:
        raise HTTPException(status_code=404, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Search failed: {str(e)}")


@router.post("/search/bulk")
async def bulk_search(request: BulkSearchRequest, account_ctx=Depends(get_account_context)):
    """
    Run multiple ICP profiles sequentially.
    Either provide industry_ids or set run_all_active=True.
    """
    from database import industries_collection
    account_id = ObjectId(account_ctx["account"]["_id"])

    if request.run_all_active:
        industries = await industries_collection.find({"account_id": account_id, "is_active": True}).to_list(100)
        industry_ids = [str(ind["_id"]) for ind in industries]
    else:
        industry_ids = request.industry_ids

    if not industry_ids:
        raise HTTPException(status_code=400, detail="No industries specified and no active industries found")

    results = []
    for ind_id in industry_ids:
        try:
            result = await process_search(industry_id=ind_id, fetch_count=request.fetch_count)
            results.append(result)
        except Exception as e:
            results.append({"industry_id": ind_id, "status": "failed", "error": str(e)})

    total_new = sum(r.get("new_prospects", 0) for r in results)
    total_fetched = sum(r.get("total_fetched", 0) for r in results)

    return {
        "industries_run": len(results),
        "total_fetched": total_fetched,
        "total_new_prospects": total_new,
        "results": results,
    }


@router.get("")
async def list_prospects(
    page: int = Query(1, ge=1),
    page_size: int = Query(50, ge=1, le=500),
    industry: Optional[str] = None,
    industry_id: Optional[str] = None,
    country: Optional[str] = None,
    seniority_level: Optional[str] = None,
    status: Optional[str] = None,
    min_score: Optional[float] = None,
    enrichment_status: Optional[str] = None,
    has_email: Optional[bool] = None,
    has_linkedin: Optional[bool] = None,
    search: Optional[str] = None,
    sort_by: str = "ai_prospect_score",
    sort_order: str = "desc",
    account_ctx=Depends(get_account_context),
):
    """List prospects scoped to account via prospect_state overlay."""
    account_id = ObjectId(account_ctx["account"]["_id"])
    account_id_str = str(account_id)

    # 1. Get prospect_ids (+ scores) from prospect_state for this account
    state_query: dict = {"account_id": {"$in": [account_id_str, account_id]}}
    if status:
        state_query["status"] = status
    if min_score is not None:
        state_query["ai_score"] = {"$gte": min_score}

    state_docs = await prospect_state_collection.find(
        state_query, {"prospect_id": 1, "ai_score": 1, "status": 1, "priority_tier": 1}
    ).to_list(None)

    # Fallback: old schema where account_id is directly on prospects
    if not state_docs:
        fallback_query: dict = {"account_id": {"$in": [account_id, account_id_str]}}
        if status:
            fallback_query["status"] = status
        if min_score is not None:
            fallback_query["ai_prospect_score"] = {"$gte": min_score}
        if enrichment_status:
            fallback_query["enrichment_status"] = enrichment_status if enrichment_status != "unenriched" else {"$nin": ["completed", "in_progress", "profile_scraped", "company_scraped", "ai_assessed"]}
        if has_email is True:
            fallback_query["email"] = {"$nin": [None, ""]}
        if has_linkedin is True:
            fallback_query["linkedin"] = {"$nin": [None, ""]}
        sort_direction = -1 if sort_order == "desc" else 1
        skip = (page - 1) * page_size
        total, prospects_raw = await asyncio.gather(
            prospects_collection.count_documents(fallback_query),
            prospects_collection.find(fallback_query, _LIST_PROJECTION)
                .sort(sort_by, sort_direction).skip(skip).limit(page_size).to_list(page_size),
        )
        return {
            "total": total, "page": page, "page_size": page_size,
            "total_pages": (total + page_size - 1) // page_size,
            "prospects": [serialize_doc(p) for p in prospects_raw],
        }

    pid_to_state: dict = {}
    for doc in state_docs:
        pid = doc.get("prospect_id")
        pid_to_state[str(pid)] = doc

    # 2. Build prospect filter
    def _oid(x):
        try:
            return ObjectId(x) if isinstance(x, str) else x
        except Exception:
            return None

    oids = [o for x in pid_to_state.keys() if (o := _oid(x))]
    p_query: dict = {"_id": {"$in": oids}}
    if search:
        p_query["$or"] = [
            {"full_name": {"$regex": search, "$options": "i"}},
            {"email": {"$regex": search, "$options": "i"}},
            {"company_name": {"$regex": search, "$options": "i"}},
            {"job_title": {"$regex": search, "$options": "i"}},
        ]
    if industry_id:
        p_query["company_industry_id"] = industry_id
    if industry:
        p_query["company_industry_id"] = {"$regex": industry, "$options": "i"}
    if country:
        p_query["$or"] = [
            {"location.country_code": {"$regex": country, "$options": "i"}},
            {"location.country": {"$regex": country, "$options": "i"}},
        ]
    if seniority_level:
        p_query["$or"] = p_query.get("$or", []) + [
            {"seniority": {"$regex": seniority_level, "$options": "i"}},
            {"seniority_level": {"$regex": seniority_level, "$options": "i"}},
        ]
    if enrichment_status:
        if enrichment_status == "unenriched":
            p_query["enrichment_status"] = {"$nin": ["completed", "in_progress", "profile_scraped", "company_scraped", "ai_assessed"]}
        else:
            p_query["enrichment_status"] = enrichment_status
    if has_email is True:
        p_query["email"] = {"$nin": [None, ""]}
    elif has_email is False:
        p_query["email"] = {"$in": [None, ""]}
    if has_linkedin is True:
        p_query["linkedin"] = {"$nin": [None, ""]}
    elif has_linkedin is False:
        p_query["linkedin"] = {"$in": [None, ""]}

    # 3. Sort by ai_score from state, paginate
    sorted_pids = sorted(
        pid_to_state.keys(),
        key=lambda x: pid_to_state[x].get("ai_score") or 0,
        reverse=(sort_order == "desc"),
    )
    skip = (page - 1) * page_size
    total = await prospects_collection.count_documents(p_query)
    paged_oids = [_oid(x) for x in sorted_pids[skip:skip + page_size] if _oid(x)]

    prospects_raw = await prospects_collection.find(
        {"_id": {"$in": paged_oids}}, _LIST_PROJECTION
    ).to_list(page_size)

    # Re-sort to match score order and annotate with state fields
    oid_to_p = {str(p["_id"]): p for p in prospects_raw}
    ordered = [oid_to_p[str(o)] for o in paged_oids if str(o) in oid_to_p]
    for p in ordered:
        state = pid_to_state.get(str(p["_id"])) or {}
        p["status"] = state.get("status", "new")
        p["ai_prospect_score"] = state.get("ai_score") or p.get("ai_prospect_score")
        p["priority_tier"] = state.get("priority_tier") or p.get("priority_tier")

    return {
        "total": total, "page": page, "page_size": page_size,
        "total_pages": (total + page_size - 1) // page_size,
        "prospects": [serialize_doc(p) for p in ordered],
    }


@router.get("/enriched-count")
async def get_enriched_count(account_ctx=Depends(get_account_context)):
    """Return the count of prospects enriched by the current account."""
    account_id = ObjectId(account_ctx["account"]["_id"])
    user_id = account_ctx["user"]["_id"]
    count = await prospects_collection.count_documents({"account_id": account_id, "enriched_by": user_id})
    return {"count": count, "user_id": user_id}


@router.get("/stats")
async def get_prospect_stats(account_ctx=Depends(get_account_context)):
    """Get aggregate statistics about all prospects for this account."""
    account_id = ObjectId(account_ctx["account"]["_id"])
    account_id_str = str(account_id)
    user_id = account_ctx["user"]["_id"]
    cache_key = f"{account_id_str}:{user_id}"

    now = time.monotonic()
    if cache_key in _stats_cache:
        ts, cached = _stats_cache[cache_key]
        if now - ts < _STATS_CACHE_TTL:
            return cached

    # Get all prospect_state docs for this account (new schema path)
    state_docs = await prospect_state_collection.find(
        {"account_id": {"$in": [account_id_str, account_id]}},
        {"prospect_id": 1, "ai_score": 1, "status": 1, "priority_tier": 1},
    ).to_list(None)

    if state_docs:
        # New schema: account scoping via prospect_state
        total = len(state_docs)
        by_status: dict = {}
        scores: list[float] = []
        hot = warm = cold = 0
        for d in state_docs:
            s = d.get("status") or "new"
            by_status[s] = by_status.get(s, 0) + 1
            sc = d.get("ai_score") or 0
            scores.append(sc)
            if sc >= 80: hot += 1
            elif sc >= 60: warm += 1
            else: cold += 1

        avg_score = round(sum(scores) / len(scores), 1) if scores else 0.0

        def _oid(x):
            try: return ObjectId(x) if isinstance(x, str) else x
            except Exception: return None

        oids = [o for x in (d.get("prospect_id") for d in state_docs) if (o := _oid(str(x)))]

        by_industry, by_country, by_seniority, enrichment_results, unique_companies_result = await asyncio.gather(
            prospects_collection.aggregate([
                {"$match": {"_id": {"$in": oids}, "company_industry_id": {"$ne": None}}},
                {"$group": {"_id": "$company_industry_id", "count": {"$sum": 1}}},
                {"$sort": {"count": -1}}, {"$limit": 15},
            ]).to_list(15),
            prospects_collection.aggregate([
                {"$match": {"_id": {"$in": oids}, "location.country_code": {"$ne": None}}},
                {"$group": {"_id": "$location.country_code", "count": {"$sum": 1}}},
                {"$sort": {"count": -1}}, {"$limit": 10},
            ]).to_list(10),
            prospects_collection.aggregate([
                {"$match": {"_id": {"$in": oids}, "$or": [{"seniority": {"$ne": None}}, {"seniority_level": {"$ne": None}}]}},
                {"$group": {"_id": {"$ifNull": ["$seniority", "$seniority_level"]}, "count": {"$sum": 1}}},
                {"$sort": {"count": -1}},
            ]).to_list(20),
            prospects_collection.aggregate([
                {"$match": {"_id": {"$in": oids}}},
                {"$group": {"_id": "$enrichment_status", "count": {"$sum": 1}}},
            ]).to_list(20),
            prospects_collection.aggregate([
                {"$match": {"_id": {"$in": oids}, "company_name": {"$nin": [None, ""]}}},
                {"$group": {"_id": "$company_name"}}, {"$count": "n"},
            ]).to_list(1),
        )

        by_enrichment_status = {r["_id"] or "not_started": r["count"] for r in enrichment_results}
        unique_companies = unique_companies_result[0]["n"] if unique_companies_result else 0
        score_distribution: dict = {}

    else:
        # Old schema fallback: account_id on prospects
        acct_filter = {"$in": [account_id, account_id_str]}
        total = await prospects_collection.count_documents({"account_id": acct_filter})
        if total == 0:
            result = {"total_prospects": 0, "unique_companies": 0, "enriched_by_me": 0}
            _stats_cache[cache_key] = (now, result)
            return result

        (status_raw, by_industry, by_country, by_seniority, avg_result, score_dist,
         enrichment_results, hot, warm, cold, unique_companies_result, _) = await asyncio.gather(
            prospects_collection.aggregate([{"$match": {"account_id": acct_filter}}, {"$group": {"_id": "$status", "count": {"$sum": 1}}}]).to_list(100),
            prospects_collection.aggregate([{"$match": {"account_id": acct_filter, "industry": {"$ne": None}}}, {"$group": {"_id": "$industry", "count": {"$sum": 1}}}, {"$sort": {"count": -1}}, {"$limit": 15}]).to_list(15),
            prospects_collection.aggregate([{"$match": {"account_id": acct_filter, "country": {"$ne": None}}}, {"$group": {"_id": "$country", "count": {"$sum": 1}}}, {"$sort": {"count": -1}}, {"$limit": 10}]).to_list(10),
            prospects_collection.aggregate([{"$match": {"account_id": acct_filter, "seniority_level": {"$ne": None}}}, {"$group": {"_id": "$seniority_level", "count": {"$sum": 1}}}, {"$sort": {"count": -1}}]).to_list(20),
            prospects_collection.aggregate([{"$match": {"account_id": acct_filter}}, {"$group": {"_id": None, "avg_score": {"$avg": "$prospect_score"}}}]).to_list(1),
            prospects_collection.aggregate([{"$match": {"account_id": acct_filter}}, {"$bucket": {"groupBy": "$prospect_score", "boundaries": [0, 20, 40, 60, 80, 101], "default": "other", "output": {"count": {"$sum": 1}}}}]).to_list(10),
            prospects_collection.aggregate([{"$match": {"account_id": acct_filter}}, {"$group": {"_id": "$enrichment_status", "count": {"$sum": 1}}}]).to_list(20),
            prospects_collection.count_documents({"account_id": acct_filter, "ai_prospect_score": {"$gte": 80}}),
            prospects_collection.count_documents({"account_id": acct_filter, "ai_prospect_score": {"$gte": 60, "$lt": 80}}),
            prospects_collection.count_documents({"account_id": acct_filter, "ai_prospect_score": {"$lt": 60}}),
            prospects_collection.aggregate([{"$match": {"account_id": acct_filter, "company_name": {"$nin": [None, ""]}}}, {"$group": {"_id": "$company_name"}}, {"$count": "n"}]).to_list(1),
            prospects_collection.count_documents({"account_id": acct_filter, "enriched_by": user_id}),
        )
        by_status = {r["_id"]: r["count"] for r in status_raw if r["_id"]}
        avg_score = round(avg_result[0]["avg_score"], 1) if avg_result else 0.0
        labels = {0: "0-19", 20: "20-39", 40: "40-59", 60: "60-79", 80: "80-100"}
        score_distribution = {labels.get(b["_id"], str(b["_id"])): b["count"] for b in score_dist}
        by_enrichment_status = {r["_id"] or "not_started": r["count"] for r in enrichment_results}
        unique_companies = unique_companies_result[0]["n"] if unique_companies_result else 0

    enriched_by_me = await prospects_collection.count_documents({"enriched_by": user_id})

    result = {
        "total_prospects": total,
        "unique_companies": unique_companies,
        "enriched_by_me": enriched_by_me,
        "by_status": by_status,
        "by_industry": by_industry,
        "by_country": by_country,
        "by_seniority": by_seniority,
        "avg_score": avg_score,
        "score_distribution": score_distribution,
        "by_enrichment_status": by_enrichment_status,
        "by_priority_tier": {"hot": hot, "warm": warm, "cold": cold},
    }
    _stats_cache[cache_key] = (now, result)
    return result


@router.get("/by-company")
async def prospects_by_company(
    industry: Optional[str] = None,
    keywords: Optional[str] = None,
    country: Optional[str] = None,
    sort_by: str = Query("total_count", pattern="^(total_count|avg_score|company_name)$"),
    page: int = Query(1, ge=1),
    page_size: int = Query(50, ge=1, le=200),
    account_ctx=Depends(get_account_context),
):
    """Group prospects by company name with aggregate stats per company.
    Also resolves company_id by joining with the companies collection where available.
    """
    from database import companies_collection as companies_col
    account_id = ObjectId(account_ctx["account"]["_id"])

    match_stage: dict = {
        "account_id": account_id,
        "company_name": {"$exists": True, "$ne": None, "$nin": ["", None]},
    }
    if industry:
        match_stage["industry"] = {"$regex": industry, "$options": "i"}
    if country:
        match_stage["$or"] = [
            {"country": {"$regex": country, "$options": "i"}},
            {"company_country": {"$regex": country, "$options": "i"}},
        ]
    if keywords:
        match_stage["$or"] = match_stage.get("$or", []) + [
            {"company_name": {"$regex": keywords, "$options": "i"}},
        ]

    sort_field = sort_by if sort_by in ("total_count", "avg_score", "company_name") else "total_count"
    sort_dir = 1 if sort_field == "company_name" else -1

    pipeline = [
        {"$match": match_stage},
        {"$group": {
            "_id": "$company_name",
            "total_count": {"$sum": 1},
            "hot_count": {
                "$sum": {"$cond": [{"$gte": [{"$ifNull": ["$ai_prospect_score", 0]}, 80]}, 1, 0]}
            },
            "warm_count": {
                "$sum": {"$cond": [
                    {"$and": [
                        {"$gte": [{"$ifNull": ["$ai_prospect_score", 0]}, 60]},
                        {"$lt": [{"$ifNull": ["$ai_prospect_score", 0]}, 80]},
                    ]}, 1, 0
                ]}
            },
            "cold_count": {
                "$sum": {"$cond": [{"$lt": [{"$ifNull": ["$ai_prospect_score", 0]}, 60]}, 1, 0]}
            },
            "contacted_count": {
                "$sum": {"$cond": [{"$in": ["$status", ["contacted", "replied", "meeting_booked"]]}, 1, 0]}
            },
            "replied_count": {
                "$sum": {"$cond": [{"$eq": ["$status", "replied"]}, 1, 0]}
            },
            "meeting_count": {
                "$sum": {"$cond": [{"$eq": ["$status", "meeting_booked"]}, 1, 0]}
            },
            "new_count": {
                "$sum": {"$cond": [{"$eq": ["$status", "new"]}, 1, 0]}
            },
            "enriched_count": {
                "$sum": {"$cond": [{"$eq": ["$enrichment_status", "completed"]}, 1, 0]}
            },
            "avg_score": {"$avg": "$ai_prospect_score"},
            "industry": {"$first": "$industry"},
            "company_linkedin": {"$first": "$company_linkedin"},
            "company_country": {"$first": "$company_country"},
            "country": {"$first": "$country"},
        }},
        {"$sort": {sort_field: sort_dir}},
    ]

    # Total count
    count_pipeline = pipeline + [{"$count": "total"}]
    count_result = await prospects_collection.aggregate(count_pipeline).to_list(1)
    total_companies = count_result[0]["total"] if count_result else 0

    # Paginated data
    skip = (page - 1) * page_size
    data_pipeline = pipeline + [{"$skip": skip}, {"$limit": page_size}]
    companies = await prospects_collection.aggregate(data_pipeline).to_list(page_size)

    # Bulk-resolve company IDs by looking up linkedin_url in companies collection
    linkedin_urls = [c.get("company_linkedin") for c in companies if c.get("company_linkedin")]
    company_id_map: dict = {}
    if linkedin_urls:
        async for doc in companies_col.find(
            {"linkedin_url": {"$in": linkedin_urls}},
            {"_id": 1, "linkedin_url": 1, "headquarters": 1, "description": 1, "tagline": 1},
        ):
            company_id_map[doc["linkedin_url"]] = {
                "id": str(doc["_id"]),
                "headquarters": doc.get("headquarters"),
                "description": doc.get("description"),
                "tagline": doc.get("tagline"),
            }

    result = []
    for c in companies:
        linkedin = c.get("company_linkedin")
        company_meta = company_id_map.get(linkedin, {}) if linkedin else {}
        region = (
            c.get("company_country")
            or c.get("country")
            or company_meta.get("headquarters")
            or ""
        )
        result.append({
            "company_name": c["_id"] or "Unknown",
            "company_id": company_meta.get("id"),
            "company_linkedin": linkedin,
            "total_count": c["total_count"],
            "hot_count": c["hot_count"],
            "warm_count": c["warm_count"],
            "cold_count": c["cold_count"],
            "contacted_count": c["contacted_count"],
            "replied_count": c["replied_count"],
            "meeting_count": c["meeting_count"],
            "new_count": c["new_count"],
            "enriched_count": c.get("enriched_count", 0),
            "avg_score": round(c["avg_score"] or 0, 1),
            "industry": c.get("industry") or "",
            "region": region,
            "headquarters": company_meta.get("headquarters") or region,
        })

    return {
        "companies": result,
        "total": total_companies,
        "page": page,
        "page_size": page_size,
    }


@router.get("/company/{company_name:path}")
async def get_prospects_for_company(
    company_name: str,
    page: int = Query(1, ge=1),
    page_size: int = Query(50, ge=1, le=200),
    status: Optional[str] = None,
    enrichment_status: Optional[str] = None,
    account_ctx=Depends(get_account_context),
):
    """Get all prospects for a specific company."""
    from urllib.parse import unquote
    account_id = ObjectId(account_ctx["account"]["_id"])
    decoded_name = unquote(company_name)

    query: dict = {"account_id": account_id, "company_name": decoded_name}
    if status:
        query["status"] = status
    if enrichment_status:
        if enrichment_status == "unenriched":
            query["enrichment_status"] = {"$nin": ["completed", "in_progress", "profile_scraped", "company_scraped", "ai_assessed"]}
        else:
            query["enrichment_status"] = enrichment_status

    skip = (page - 1) * page_size
    total, prospects_raw = await asyncio.gather(
        prospects_collection.count_documents(query),
        prospects_collection.find(query, _LIST_PROJECTION)
            .sort("ai_prospect_score", -1)
            .skip(skip)
            .limit(page_size)
            .to_list(page_size),
    )

    return {
        "total": total,
        "page": page,
        "page_size": page_size,
        "prospects": [serialize_doc(p) for p in prospects_raw],
    }


@router.post("/manual")
async def create_prospect_manual(data: dict, account_ctx=Depends(get_account_context)):
    """Manually create a prospect."""
    from datetime import datetime
    account_id = ObjectId(account_ctx["account"]["_id"])
    allowed = {
        "first_name", "last_name", "full_name", "email", "phone",
        "job_title", "seniority_level", "company_name", "company_domain",
        "company_linkedin", "linkedin_url", "industry", "country", "city",
        "notes", "tags", "status",
    }
    doc = {k: v for k, v in data.items() if k in allowed}
    doc["account_id"] = account_id
    doc.setdefault("status", "new")
    doc.setdefault("enrichment_status", "not_started")
    doc["created_at"] = datetime.utcnow()
    doc["last_updated_at"] = datetime.utcnow()
    if doc.get("first_name") and doc.get("last_name") and not doc.get("full_name"):
        doc["full_name"] = f"{doc['first_name']} {doc['last_name']}"
    result = await prospects_collection.insert_one(doc)
    doc["_id"] = str(result.inserted_id)
    return serialize_doc(doc)


@router.get("/{prospect_id}")
async def get_prospect(prospect_id: str, account_ctx=Depends(get_account_context)):
    """Get a single prospect by ID. Prospects are global; authorization via prospect_state or enrollment."""
    account_oid = ObjectId(account_ctx["account"]["_id"])
    account_id_str = str(account_oid)
    try:
        pid = ObjectId(prospect_id)
    except Exception:
        raise HTTPException(status_code=400, detail="Invalid prospect ID")

    # Primary: prospect is global — fetch directly
    prospect = await prospects_collection.find_one({"_id": pid})

    if not prospect:
        raise HTTPException(status_code=404, detail="Prospect not found")

    # Authorization: this account must have a prospect_state or an enrollment for this prospect
    authorized = await prospect_state_collection.find_one(
        {"account_id": {"$in": [account_id_str, account_oid]}, "prospect_id": prospect_id},
        {"_id": 1},
    )
    if not authorized:
        # Fallback: check enrollment
        acct_filter = {"$in": [account_oid, account_id_str]}
        enr = await campaign_enrollments_collection.find_one(
            {"prospect_id": pid, "account_id": acct_filter}, {"campaign_id": 1}
        )
        if not enr:
            # Last fallback: old schema where account_id is on prospect
            if prospect.get("account_id") not in (account_oid, account_id_str):
                raise HTTPException(status_code=404, detail="Prospect not found")

    # Overlay account-scoped fields from prospect_state
    state = await prospect_state_collection.find_one(
        {"account_id": account_id_str, "prospect_id": prospect_id},
        {"status": 1, "ai_score": 1, "ai_score_breakdown": 1, "priority_tier": 1, "tags": 1, "pitch": 1, "outreach_messages": 1},
    )
    prospect_state_overlay = state or {}
    if state:
        prospect["status"] = state.get("status", prospect.get("status", "new"))
        prospect["ai_prospect_score"] = state.get("ai_score", prospect.get("ai_prospect_score"))
        prospect["priority_tier"] = state.get("priority_tier", prospect.get("priority_tier"))
        if state.get("tags"):
            prospect["tags"] = state["tags"]
        if state.get("outreach_messages"):
            prospect["outreach_messages"] = state["outreach_messages"]
        if state.get("ai_score_breakdown"):
            prospect["ai_score_breakdown"] = state["ai_score_breakdown"]
        # Note: ai_assessment / company_news are not persisted per-tenant on prospect_state —
        # live pipelines (curated_discovery_service.py, enrichment_pipeline.py) write them only
        # to the shared `prospects` collection (ai_assessment is explicitly stripped from the
        # per-tenant write via the _skip set in curated_discovery_service._upsert_curated_prospect),
        # so no overlay is added here for those two fields.

    # Build merged intelligence for response:
    # - prospect_intelligence_base: global (written to prospects collection)
    # - pitch: per-tenant (written to prospect_state, adds pitch_angle / why_they_need_us)
    base_intel = prospect.get("prospect_intelligence_base") or {}
    pitch = prospect_state_overlay.get("pitch") or {}
    merged_intel = {**base_intel, **pitch}  # pitch overrides/adds pitch_angle, why_they_need_us
    prospect["prospect_intelligence"] = merged_intel if merged_intel else None

    # Merge competitors from company doc
    if prospect.get("company_linkedin") and not prospect.get("competitors"):
        try:
            comp_doc = await companies_collection.find_one({"linkedin_url": prospect["company_linkedin"]})
            if comp_doc and comp_doc.get("competitors"):
                prospect["competitors"] = comp_doc["competitors"]
                prospect["competitors_last_fetched"] = comp_doc.get("competitors_last_fetched")
        except Exception:
            pass

    return serialize_doc(prospect)


@router.patch("/{prospect_id}")
async def update_prospect(prospect_id: str, update: dict, account_ctx=Depends(get_account_context)):
    """Update prospect status or tags via prospect_state overlay."""
    account_id = ObjectId(account_ctx["account"]["_id"])
    account_id_str = str(account_id)
    try:
        oid = ObjectId(prospect_id)
    except Exception:
        raise HTTPException(status_code=400, detail="Invalid prospect ID")

    overlay_fields = {"status", "tags"}
    prospect_fields = {"notes"}
    allowed = overlay_fields | prospect_fields
    update_data = {k: v for k, v in update.items() if k in allowed}

    if "status" in update_data:
        if update_data["status"] not in ("new", "contacted", "qualified", "disqualified"):
            raise HTTPException(status_code=400, detail="Invalid status")

    if not update_data:
        raise HTTPException(status_code=400, detail="No valid fields to update")

    from datetime import datetime
    now = datetime.utcnow()

    # Verify prospect exists (global — no account_id filter on prospect)
    exists = await prospects_collection.find_one({"_id": oid}, {"_id": 1})
    if not exists:
        raise HTTPException(status_code=404, detail="Prospect not found")

    overlay_update = {k: v for k, v in update_data.items() if k in overlay_fields}
    prospect_update = {k: v for k, v in update_data.items() if k in prospect_fields}

    tasks = []
    if overlay_update:
        overlay_update["last_updated_at"] = now
        tasks.append(prospect_state_collection.update_one(
            {"account_id": account_id_str, "prospect_id": prospect_id},
            {"$set": overlay_update, "$setOnInsert": {
                "account_id": account_id_str, "prospect_id": prospect_id,
                "used_by": [], "tags": [], "created_at": now,
            }},
            upsert=True,
        ))
    if prospect_update:
        prospect_update["last_updated_at"] = now
        tasks.append(prospects_collection.update_one({"_id": oid}, {"$set": prospect_update}))

    if tasks:
        await asyncio.gather(*tasks)

    return {"message": "Prospect updated", "modified": len(tasks)}
