import asyncio
import time
from fastapi import APIRouter, Depends, HTTPException, Query
from typing import Any, Optional

# enrichment_status accepted values:
#   "not_started" | "in_progress" | "profile_scraped" | "company_scraped"
#   | "ai_assessed" | "completed"
# Magic token "unenriched" = any status other than completed/in_progress/profile_scraped/company_scraped/ai_assessed
from bson import ObjectId
from auth import get_account_context, get_super_admin
from database import (
    prospects_collection,
    prospect_state_collection,
    companies_collection,
    campaign_enrollments_collection,
    campaigns_collection,
)
from utils.serialization import serialize_doc
import database as database_module
from services.campaign_prospect_state_service import (
    DEFAULT_SCORING_VERSION,
    campaign_enrichment_projection,
    campaign_score_projection,
    campaign_state_page_pipeline,
)

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


async def _account_has_prospect_access(
    *,
    account_id: ObjectId,
    prospect_id: ObjectId,
    prospect: Optional[dict] = None,
) -> bool:
    """Return whether an account may read or mutate its overlay for a prospect.

    Shared prospect existence alone never grants tenant access.  During the
    schema transition, both canonical string keys and legacy ObjectId keys are
    accepted at the boundary; all new overlay writes remain string-keyed.
    """
    account_id_str = str(account_id)
    prospect_id_str = str(prospect_id)
    state = await prospect_state_collection.find_one(
        {
            "account_id": {"$in": [account_id_str, account_id]},
            "prospect_id": {"$in": [prospect_id_str, prospect_id]},
        },
        {"_id": 1},
    )
    if state:
        return True

    enrollment = await campaign_enrollments_collection.find_one(
        {
            "account_id": {"$in": [account_id_str, account_id]},
            "prospect_id": {"$in": [prospect_id_str, prospect_id]},
        },
        {"_id": 1},
    )
    if enrollment:
        return True

    # Temporary read compatibility for pre-migration documents.  New writes
    # must never add account_id to the canonical prospect pool.
    return bool(
        prospect
        and prospect.get("account_id") in (account_id, account_id_str)
    )

async def _mark_enrolled(
    docs: list[dict],
    campaign_id: str,
    account_id: str,
) -> list[dict]:
    """Add `already_enrolled: bool` to each prospect doc for a given campaign."""
    try:
        campaign_oid = ObjectId(campaign_id)
    except Exception:
        for d in docs:
            d["already_enrolled"] = False
        return docs

    enrolled_ids = await campaign_enrollments_collection.distinct(
        "prospect_id",
        {
            "campaign_id": {"$in": [campaign_oid, campaign_id]},
            "account_id": {"$in": [account_id, ObjectId(account_id)]},
        },
    )
    enrolled_set = {str(pid) for pid in enrolled_ids}

    for d in docs:
        pid = d.get("_id") or d.get("id")
        d["already_enrolled"] = str(pid) in enrolled_set if pid else False

    return docs


async def _overlay_campaign_scores(
    docs: list[dict], campaign_id: str, account_id: str
) -> list[dict]:
    """Project authoritative campaign fit without treating zero as missing."""
    prospect_ids = [str(d.get("_id") or d.get("id")) for d in docs if d.get("_id") or d.get("id")]
    if not prospect_ids:
        return docs
    try:
        campaign_oid = ObjectId(campaign_id)
    except Exception:
        return docs
    campaign = await campaigns_collection.find_one(
        {
            "_id": campaign_oid,
            "account_id": {"$in": [str(account_id), ObjectId(account_id)]},
        },
        {"scoring_version": 1},
    )
    scoring_version = (campaign or {}).get("scoring_version") or DEFAULT_SCORING_VERSION
    cursor = database_module.db["campaign_prospect_state"].find(
        {
            "account_id": str(account_id),
            "campaign_id": str(campaign_id),
            "prospect_id": {"$in": prospect_ids},
            "scoring_version": scoring_version,
        },
        {"prospect_id": 1, "score": 1, "enrichment": 1, "scoring_version": 1},
    )
    states = {str(s["prospect_id"]): s async for s in cursor}
    for doc in docs:
        state = states.get(str(doc.get("_id") or doc.get("id")))
        if not state:
            doc["campaign_score"] = campaign_score_projection(None)
            doc["campaign_fit_score"] = None
            doc["fit_score"] = None
            continue
        score = campaign_score_projection(state)
        raw_score = state.get("score") or {}
        value = score["value"]
        doc["campaign_score"] = score
        doc["campaign_fit_score"] = value
        doc["fit_score"] = value
        doc["ai_prospect_score"] = value
        doc["priority_tier"] = raw_score.get("priority_tier")
        doc["ai_score_breakdown"] = score["breakdown"]
        doc["scoring_version"] = score["version"]
        doc["campaign_enrichment"] = campaign_enrichment_projection(
            state, shared_source=doc.get("source")
        )
    return docs


async def _list_campaign_prospects(
    *,
    campaign_id: str,
    account_id: ObjectId,
    page: int,
    page_size: int,
    min_score: Optional[float],
    sort_order: str,
    status: Optional[str],
    overlay_filter: dict,
) -> dict:
    """List an owned campaign cohort using campaign state as the rank source.

    Enrollment is the membership authority.  Campaign state is then filtered,
    score-sorted, and paginated before the shared prospect and tenant overlay
    documents are fetched for the selected page.
    """
    account_id_str = str(account_id)
    try:
        campaign_oid = ObjectId(campaign_id)
    except Exception:
        raise HTTPException(status_code=400, detail="Invalid campaign ID")

    campaign = await campaigns_collection.find_one(
        {
            "_id": campaign_oid,
            "account_id": {"$in": [account_id_str, account_id]},
        },
        {"scoring_version": 1},
    )
    if not campaign:
        raise HTTPException(status_code=404, detail="Campaign not found")

    enrollment_query = {
        "campaign_id": {"$in": [campaign_oid, str(campaign_oid)]},
        "account_id": {"$in": [account_id_str, account_id]},
    }
    enrolled_ids = {
        str(value)
        for value in await campaign_enrollments_collection.distinct(
            "prospect_id", enrollment_query
        )
        if value is not None
    }

    # Prospect-facing filters use the denormalized tenant overlay keys, but
    # only to narrow membership IDs. They can never supply a score or rank.
    if enrolled_ids and (status or overlay_filter):
        legacy_member_ids = []
        for value in enrolled_ids:
            try:
                legacy_member_ids.append(ObjectId(value))
            except Exception:
                continue
        overlay_query: dict = {
            "account_id": {"$in": [account_id_str, account_id]},
            "prospect_id": {"$in": list(enrolled_ids) + legacy_member_ids},
        }
        if status:
            overlay_query["status"] = status
        if overlay_filter:
            overlay_query = {"$and": [overlay_query, overlay_filter]}
        matched_ids = await prospect_state_collection.distinct(
            "prospect_id", overlay_query
        )
        enrolled_ids.intersection_update(str(value) for value in matched_ids)

    scoring_version = campaign.get("scoring_version") or DEFAULT_SCORING_VERSION
    pipeline = campaign_state_page_pipeline(
        account_id=account_id_str,
        campaign_id=campaign_oid,
        scoring_version=scoring_version,
        prospect_ids=enrolled_ids,
        min_score=min_score,
        page=page,
        page_size=page_size,
        sort_order=sort_order,
    )
    result = await database_module.db["campaign_prospect_state"].aggregate(
        pipeline
    ).to_list(1)
    facet = result[0] if result else {"total": [], "page": []}
    total = facet["total"][0]["n"] if facet.get("total") else 0
    state_rows = facet.get("page") or []
    ordered_ids = [str(row["prospect_id"]) for row in state_rows]

    object_ids = []
    for value in ordered_ids:
        try:
            object_ids.append(ObjectId(value))
        except Exception:
            continue
    prospect_rows = await prospects_collection.find(
        {"_id": {"$in": object_ids}}, _LIST_PROJECTION
    ).to_list(len(object_ids))
    prospects_by_id = {str(row["_id"]): row for row in prospect_rows}

    overlay_rows = await prospect_state_collection.find(
        {
            "account_id": {"$in": [account_id_str, account_id]},
            "prospect_id": {"$in": ordered_ids + object_ids},
        },
        {
            "prospect_id": 1,
            "status": 1,
            "priority_tier": 1,
            "enrichment_status": 1,
            "enrichment_error": 1,
        },
    ).to_list(len(ordered_ids))
    overlays_by_id = {
        str(row.get("prospect_id")): row for row in overlay_rows
    }
    states_by_id = {str(row["prospect_id"]): row for row in state_rows}

    docs = []
    for prospect_id in ordered_ids:
        prospect = prospects_by_id.get(prospect_id)
        if not prospect:
            continue
        doc = dict(prospect)
        overlay = overlays_by_id.get(prospect_id) or {}
        state = states_by_id[prospect_id]
        score = campaign_score_projection(state)
        value = score["value"]

        doc["status"] = overlay.get("status", "new")
        doc["campaign_score"] = score
        # Compatibility aliases remain exact mirrors of the canonical value.
        doc["campaign_fit_score"] = value
        doc["fit_score"] = value
        doc["ai_prospect_score"] = value
        raw_score = state.get("score") or {}
        doc["priority_tier"] = raw_score.get("priority_tier")
        doc["ai_score_breakdown"] = score["breakdown"]
        doc["scoring_version"] = score["version"]
        doc["score_reasoning"] = score["reasoning"]
        doc["campaign_enrichment"] = campaign_enrichment_projection(
            state, shared_source=doc.get("source")
        )
        doc["already_enrolled"] = True
        docs.append(serialize_doc(doc))

    return {
        "total": total,
        "page": page,
        "page_size": page_size,
        "total_pages": (total + page_size - 1) // page_size,
        "prospects": docs,
    }

# Converts prospect_state.prospect_id (string or ObjectId) to an ObjectId
# inside an aggregation; invalid/missing ids become null (=> lookup no-match).
_PID_TO_OID = {
    "$convert": {"input": "$prospect_id", "to": "objectId", "onError": None, "onNull": None}
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
    campaign_id: Optional[str] = None,
    account_ctx=Depends(get_account_context),
):
    """List prospects scoped to account via prospect_state overlay.

    Single aggregation on prospect_state: match account (+ filters) -> sort by
    ai_score -> skip/limit -> $lookup the page's prospects. O(page) instead of
    the previous O(overlay size) pattern.

    Prospect-level filters (search/industry/country/seniority/enrichment/
    has_email/has_linkedin) match against the `pk` filter keys denormalized
    onto prospect_state at write time (utils/prospect_filter_keys.py), so both
    `total` AND the page slice reflect them. (Until July 2026 a legacy quirk
    made filters affect `total` only; a $lookup-based filter pass was measured
    at +~400 ms per request at a 2k overlay and rejected.)"""
    account_id = ObjectId(account_ctx["account"]["_id"])
    account_id_str = str(account_id)

    # 1. State-level match (account scoping + state-side filters)
    state_query: dict = {"account_id": {"$in": [account_id_str, account_id]}}
    if status:
        state_query["status"] = status
    if min_score is not None:
        state_query["ai_score"] = {"$gte": min_score}

    # 2. Prospect-level filters -> match on the denormalized pk.* keys.
    # Each param contributes one clause; clauses are ANDed so combining e.g.
    # search + country works (the legacy dict construction let a later $or
    # overwrite an earlier one).
    clauses: list[dict] = []
    if search:
        clauses.append({"$or": [
            {"pk.full_name": {"$regex": search, "$options": "i"}},
            {"pk.email": {"$regex": search, "$options": "i"}},
            {"pk.company_name": {"$regex": search, "$options": "i"}},
            {"pk.job_title": {"$regex": search, "$options": "i"}},
        ]})
    if industry_id:
        clauses.append({"pk.company_industry_id": industry_id})
    if industry:
        clauses.append({"pk.company_industry_id": {"$regex": industry, "$options": "i"}})
    if country:
        clauses.append({"$or": [
            {"pk.country_code": {"$regex": country, "$options": "i"}},
            {"pk.country": {"$regex": country, "$options": "i"}},
        ]})
    if seniority_level:
        clauses.append({"$or": [
            {"pk.seniority": {"$regex": seniority_level, "$options": "i"}},
            {"pk.seniority_level": {"$regex": seniority_level, "$options": "i"}},
        ]})
    if enrichment_status:
        if enrichment_status == "unenriched":
            clauses.append({"pk.enrichment_status": {"$nin": ["completed", "in_progress", "profile_scraped", "company_scraped", "ai_assessed"]}})
        else:
            clauses.append({"pk.enrichment_status": enrichment_status})
    if has_email is True:
        clauses.append({"pk.email": {"$nin": [None, ""]}})
    elif has_email is False:
        clauses.append({"pk.email": {"$in": [None, ""]}})
    if has_linkedin is True:
        clauses.append({"pk.linkedin": {"$nin": [None, ""]}})
    elif has_linkedin is False:
        clauses.append({"pk.linkedin": {"$in": [None, ""]}})

    p_filter: dict = {}
    if len(clauses) == 1:
        p_filter = clauses[0]
    elif clauses:
        p_filter = {"$and": clauses}

    if campaign_id:
        # Campaign views have a different scoring authority: membership comes
        # from enrollments and ordering comes exclusively from the versioned
        # campaign_prospect_state score. Account-level ai_score is never used.
        return await _list_campaign_prospects(
            campaign_id=campaign_id,
            account_id=account_id,
            page=page,
            page_size=page_size,
            min_score=min_score,
            sort_order=sort_order,
            status=status,
            overlay_filter=p_filter,
        )

    sort_direction = -1 if sort_order == "desc" else 1
    skip = (page - 1) * page_size

    pk_match = [{"$match": p_filter}] if p_filter else []
    facet: dict = {
        # count of overlay rows BEFORE prospect-level filters — this is the
        # old-schema fallback trigger (account has no overlay rows at all).
        "state_total": [{"$count": "n"}],
        "page": pk_match + [
            {"$sort": {"_sort_score": sort_direction, "_id": 1}},
            {"$skip": skip},
            {"$limit": page_size},
            {"$lookup": {
                "from": "prospects",
                "localField": "_pid_obj",
                "foreignField": "_id",
                "pipeline": [{"$project": _LIST_PROJECTION}],
                "as": "_prospect",
            }},
            {"$unwind": "$_prospect"},
        ],
    }
    if p_filter:
        facet["filtered_total"] = pk_match + [{"$count": "n"}]

    pipeline = [
        {"$match": state_query},
        {"$addFields": {
            "_sort_score": {"$ifNull": ["$ai_score", 0]},
            "_pid_obj": _PID_TO_OID,
        }},
        {"$facet": facet},
    ]
    agg = (await prospect_state_collection.aggregate(pipeline).to_list(1))[0]
    state_total = agg["state_total"][0]["n"] if agg["state_total"] else 0

    # Fallback: old schema where account_id is directly on prospects
    if state_total == 0:
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
        docs = [serialize_doc(p) for p in prospects_raw]
        if campaign_id:
            docs = await _overlay_campaign_scores(docs, campaign_id, account_id_str)
            docs = await _mark_enrolled(docs, campaign_id, account_id_str)
        return {
            "total": total, "page": page, "page_size": page_size,
            "total_pages": (total + page_size - 1) // page_size,
            "prospects": docs,
        }

    # 3. Assemble page (already sorted/paginated/joined by the aggregation)
    if p_filter:
        total = agg["filtered_total"][0]["n"] if agg.get("filtered_total") else 0
    else:
        total = state_total

    ordered = []
    for row in agg["page"]:
        p = row["_prospect"]
        p["status"] = row.get("status", "new")
        if row.get("ai_score") is not None:
            p["ai_prospect_score"] = row["ai_score"]
        p["priority_tier"] = row.get("priority_tier") or p.get("priority_tier")
        ordered.append(p)

    # If campaign_id provided, mark which prospects are already enrolled
    if campaign_id:
        ordered = await _overlay_campaign_scores(ordered, campaign_id, account_id_str)
        ordered = await _mark_enrolled(ordered, campaign_id, account_id_str)

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

    # New schema path: two aggregations on prospect_state (state-side summary
    # + prospect-side rollups via one $lookup pass) instead of pulling every
    # state doc into Python and running five $in aggregations over all ids.
    state_match = {"$match": {"account_id": {"$in": [account_id_str, account_id]}}}
    _score = {"$ifNull": ["$ai_score", 0]}

    summary_pipeline = [
        state_match,
        {"$facet": {
            "summary": [{"$group": {
                "_id": None,
                "total": {"$sum": 1},
                "avg_score": {"$avg": _score},
                "hot": {"$sum": {"$cond": [{"$gte": [_score, 80]}, 1, 0]}},
                "warm": {"$sum": {"$cond": [{"$and": [{"$gte": [_score, 60]}, {"$lt": [_score, 80]}]}, 1, 0]}},
                "cold": {"$sum": {"$cond": [{"$lt": [_score, 60]}, 1, 0]}},
            }}],
            "by_status": [{"$group": {
                # legacy parity: missing/None/"" status buckets as "new"
                "_id": {"$cond": [{"$in": [{"$ifNull": ["$status", None]}, [None, ""]]}, "new", "$status"]},
                "count": {"$sum": 1},
            }}],
        }},
    ]

    # ids-only overlay fetch (projection: prospect_id) feeding ONE prospects
    # aggregation with a $facet over all five rollups — measured ~4x faster
    # than a per-row $lookup and replaces five separate $in pipelines.
    summary_agg, state_id_docs = await asyncio.gather(
        prospect_state_collection.aggregate(summary_pipeline).to_list(1),
        prospect_state_collection.find(
            state_match["$match"], {"prospect_id": 1}
        ).to_list(None),
    )
    summary_rows = summary_agg[0]["summary"] if summary_agg else []

    if summary_rows:
        # New schema: account scoping via prospect_state
        s = summary_rows[0]
        total = s["total"]
        hot, warm, cold = s["hot"], s["warm"], s["cold"]
        avg_score = round(s["avg_score"] or 0, 1)
        by_status = {r["_id"]: r["count"] for r in summary_agg[0]["by_status"]}

        def _oid(x):
            try: return ObjectId(x) if isinstance(x, str) else x
            except Exception: return None

        oids = [o for d in state_id_docs if (o := _oid(d.get("prospect_id")))]
        rollups_agg = await prospects_collection.aggregate([
            {"$match": {"_id": {"$in": oids}}},
            {"$project": {
                "company_industry_id": 1, "location.country_code": 1,
                "seniority": 1, "seniority_level": 1,
                "enrichment_status": 1, "company_name": 1,
            }},
            {"$facet": {
                "by_industry": [
                    {"$match": {"company_industry_id": {"$ne": None}}},
                    {"$group": {"_id": "$company_industry_id", "count": {"$sum": 1}}},
                    {"$sort": {"count": -1}}, {"$limit": 15},
                ],
                "by_country": [
                    {"$match": {"location.country_code": {"$ne": None}}},
                    {"$group": {"_id": "$location.country_code", "count": {"$sum": 1}}},
                    {"$sort": {"count": -1}}, {"$limit": 10},
                ],
                "by_seniority": [
                    {"$match": {"$or": [{"seniority": {"$ne": None}}, {"seniority_level": {"$ne": None}}]}},
                    {"$group": {"_id": {"$ifNull": ["$seniority", "$seniority_level"]}, "count": {"$sum": 1}}},
                    {"$sort": {"count": -1}}, {"$limit": 20},
                ],
                "by_enrichment": [
                    {"$group": {"_id": "$enrichment_status", "count": {"$sum": 1}}},
                    {"$limit": 20},
                ],
                "unique_companies": [
                    {"$match": {"company_name": {"$nin": [None, ""]}}},
                    {"$group": {"_id": "$company_name"}}, {"$count": "n"},
                ],
            }},
        ]).to_list(1)

        rollups = rollups_agg[0]
        by_industry = rollups["by_industry"]
        by_country = rollups["by_country"]
        by_seniority = rollups["by_seniority"]
        by_enrichment_status = {r["_id"] or "not_started": r["count"] for r in rollups["by_enrichment"]}
        unique_companies = rollups["unique_companies"][0]["n"] if rollups["unique_companies"] else 0
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
    account_id_str = str(account_ctx["account"]["_id"])

    # Prospects are a SHARED pool with NO account_id field — the old
    # account_id match here always returned zero rows. Tenant-relative
    # score/status live in the prospect_state overlay and are joined below.
    match_stage: dict = {
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
        # Overlay tenant-scoped score/status from prospect_state (string-keyed
        # account_id + prospect_id). Inline ai_prospect_score/status fields on
        # the shared prospect doc are legacy and no longer trusted.
        {"$lookup": {
            "from": "prospect_state",
            "let": {"pid": {"$toString": "$_id"}},
            "pipeline": [
                {"$match": {
                    "account_id": account_id_str,
                    "$expr": {"$eq": ["$prospect_id", "$$pid"]},
                }},
                {"$limit": 1},
                {"$project": {"_id": 0, "ai_score": 1, "status": 1, "enrichment_status": 1}},
            ],
            "as": "_state",
        }},
        {"$addFields": {"_state": {"$arrayElemAt": ["$_state", 0]}}},
        {"$addFields": {
            "_score": "$_state.ai_score",
            "_status": {"$ifNull": ["$_state.status", "new"]},
            # enrichment_status: tenant overlay wins, shared doc is fallback.
            "_enrichment_status": {"$ifNull": ["$_state.enrichment_status", "$enrichment_status"]},
        }},
        {"$group": {
            "_id": "$company_name",
            "total_count": {"$sum": 1},
            "hot_count": {
                "$sum": {"$cond": [{"$gte": [{"$ifNull": ["$_score", 0]}, 80]}, 1, 0]}
            },
            "warm_count": {
                "$sum": {"$cond": [
                    {"$and": [
                        {"$gte": [{"$ifNull": ["$_score", 0]}, 60]},
                        {"$lt": [{"$ifNull": ["$_score", 0]}, 80]},
                    ]}, 1, 0
                ]}
            },
            "cold_count": {
                "$sum": {"$cond": [{"$lt": [{"$ifNull": ["$_score", 0]}, 60]}, 1, 0]}
            },
            "contacted_count": {
                "$sum": {"$cond": [{"$in": ["$_status", ["contacted", "replied", "meeting_booked"]]}, 1, 0]}
            },
            "replied_count": {
                "$sum": {"$cond": [{"$eq": ["$_status", "replied"]}, 1, 0]}
            },
            "meeting_count": {
                "$sum": {"$cond": [{"$eq": ["$_status", "meeting_booked"]}, 1, 0]}
            },
            "new_count": {
                "$sum": {"$cond": [{"$eq": ["$_status", "new"]}, 1, 0]}
            },
            "enriched_count": {
                "$sum": {"$cond": [{"$eq": ["$_enrichment_status", "completed"]}, 1, 0]}
            },
            "avg_score": {"$avg": "$_score"},
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
    search: Optional[str] = None,
    sort_by: str = "ai_prospect_score",
    sort_order: str = "desc",
    country: Optional[str] = None,
    seniority_level: Optional[str] = None,
    has_email: Optional[bool] = None,
    has_linkedin: Optional[bool] = None,
    min_score: Optional[float] = None,
    account_ctx=Depends(get_account_context),
):
    """Get all prospects for a specific company with search, sort, and filter."""
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
    if search:
        query["$or"] = [
            {"full_name": {"$regex": search, "$options": "i"}},
            {"email": {"$regex": search, "$options": "i"}},
            {"job_title": {"$regex": search, "$options": "i"}},
        ]
    if country:
        query["country"] = {"$regex": country, "$options": "i"}
    if seniority_level:
        query["seniority_level"] = {"$regex": seniority_level, "$options": "i"}
    if has_email is True:
        query["email"] = {"$nin": [None, ""]}
    elif has_email is False:
        query["email"] = {"$in": [None, ""]}
    if has_linkedin is True:
        query["linkedin"] = {"$nin": [None, ""]}
    elif has_linkedin is False:
        query["linkedin"] = {"$in": [None, ""]}
    if min_score is not None:
        query["ai_prospect_score"] = {"$gte": min_score}

    sort_direction = -1 if sort_order == "desc" else 1
    skip = (page - 1) * page_size
    total, prospects_raw = await asyncio.gather(
        prospects_collection.count_documents(query),
        prospects_collection.find(query, _LIST_PROJECTION)
            .sort(sort_by, sort_direction)
            .skip(skip)
            .limit(page_size)
            .to_list(page_size),
    )

    return {
        "total": total,
        "total_pages": (total + page_size - 1) // page_size,
        "page": page,
        "page_size": page_size,
        "prospects": [serialize_doc(p) for p in prospects_raw],
    }


@router.post("/manual")
async def create_prospect_manual(data: dict, _admin=Depends(get_super_admin)):
    """Create a tenant-neutral canonical prospect (superadmin only)."""
    from datetime import datetime
    allowed = {
        "first_name", "last_name", "full_name", "email", "phone",
        "job_title", "seniority_level", "company_name", "company_domain",
        "company_linkedin", "linkedin_url", "industry", "country", "city",
    }
    doc = {k: v for k, v in data.items() if k in allowed}
    doc.setdefault("enrichment_status", "not_started")
    doc["created_at"] = datetime.utcnow()
    doc["last_updated_at"] = datetime.utcnow()
    if doc.get("first_name") and doc.get("last_name") and not doc.get("full_name"):
        doc["full_name"] = f"{doc['first_name']} {doc['last_name']}"
    result = await prospects_collection.insert_one(doc)
    doc["_id"] = str(result.inserted_id)
    return serialize_doc(doc)


@router.get("/{prospect_id}")
async def get_prospect(
    prospect_id: str,
    campaign_id: Optional[str] = Query(None),
    account_ctx=Depends(get_account_context),
):
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

    if not await _account_has_prospect_access(
        account_id=account_oid, prospect_id=pid, prospect=prospect
    ):
        raise HTTPException(status_code=404, detail="Prospect not found")

    # Never expose legacy tenant-private fields that may still be present on a
    # shared document.  The requesting account's overlay is applied below.
    for private_field in (
        "notes",
        "tags",
        "status",
        "visible",
        "excluded",
        "exclusion_reason",
        "used_by",
        "outreach_messages",
        "prospect_intelligence",
        "pitch",
        "account_id",
        "enriched_by",
        "source_actor_id",
        "phone_unlocked_by_account",
        "ai_prospect_score",
        "prospect_score",
        "enhanced_score",
        "ai_score_breakdown",
        "priority_tier",
        "connection_request_sent_at",
        "connection_accepted_at",
        "connection_followup_sent_at",
        "connection_followup_message_id",
        "last_contacted_at",
        "scheduled_outreach_date",
        "optimal_send_time",
    ):
        prospect.pop(private_field, None)

    # Overlay account-scoped fields from prospect_state
    state = await prospect_state_collection.find_one(
        {
            "account_id": {"$in": [account_id_str, account_oid]},
            "prospect_id": {"$in": [prospect_id, pid]},
        },
        {
            "status": 1, "ai_score": 1, "ai_score_breakdown": 1,
            "priority_tier": 1, "notes": 1, "tags": 1, "pitch": 1,
            "visible": 1, "excluded": 1, "exclusion_reason": 1,
            "outreach_messages": 1, "prospect_intelligence": 1,
            "company_news": 1, "competitors": 1, "posts": 1,
            "enrichment_status": 1, "enrichment_error": 1,
            "ai_assessment": 1,
        },
    )
    prospect_state_overlay = state or {}
    if state:
        prospect["status"] = state.get("status", "new")
        if state.get("ai_score") is not None:
            prospect["ai_prospect_score"] = state["ai_score"]
        prospect["priority_tier"] = state.get("priority_tier", prospect.get("priority_tier"))
        if state.get("tags"):
            prospect["tags"] = state["tags"]
        for field in ("notes", "visible", "excluded", "exclusion_reason"):
            if field in state:
                prospect[field] = state[field]
        if state.get("outreach_messages"):
            prospect["outreach_messages"] = state["outreach_messages"]
        if state.get("ai_score_breakdown"):
            prospect["ai_score_breakdown"] = state["ai_score_breakdown"]
        if state.get("ai_assessment"):
            prospect["ai_assessment"] = state["ai_assessment"]
        for field in ("company_news", "competitors", "posts", "enrichment_status", "enrichment_error"):
            if state.get(field) is not None:
                prospect[field] = state[field]
    else:
        prospect["status"] = "new"

    campaign_state = None
    if campaign_id:
        try:
            campaign_oid = ObjectId(campaign_id)
        except Exception:
            raise HTTPException(status_code=400, detail="Invalid campaign ID")
        campaign = await campaigns_collection.find_one(
            {
                "_id": campaign_oid,
                "account_id": {"$in": [account_id_str, account_oid]},
            },
            {"scoring_version": 1},
        )
        if not campaign:
            raise HTTPException(status_code=404, detail="Campaign not found")
        enrollment = await campaign_enrollments_collection.find_one(
            {
                "account_id": {"$in": [account_id_str, account_oid]},
                "campaign_id": {"$in": [str(campaign_oid), campaign_oid]},
                "prospect_id": {"$in": [prospect_id, pid]},
            },
            {"_id": 1},
        )
        if not enrollment:
            raise HTTPException(status_code=404, detail="Prospect is not in this campaign")
        scoring_version = campaign.get("scoring_version") or DEFAULT_SCORING_VERSION
        campaign_state = await database_module.db["campaign_prospect_state"].find_one(
            {
                "account_id": account_id_str,
                "campaign_id": str(campaign_id),
                "prospect_id": prospect_id,
                "scoring_version": scoring_version,
            }
        )
        if not campaign_state:
            raise HTTPException(status_code=404, detail="Prospect is not in this campaign")
    else:
        # No campaign context (e.g. opened from the global Prospects list): fall
        # back to the prospect's most recent campaign score so the standalone
        # detail page shows fit + reasoning instead of a permanent "Unscored".
        # Lenient by design — no enrollment/404 gating here.
        campaign_state = await database_module.db["campaign_prospect_state"].find_one(
            {
                "account_id": account_id_str,
                "prospect_id": prospect_id,
            },
            sort=[
                ("score.scored_at", -1),
                ("updated_at", -1),
                ("created_at", -1),
            ],
        )

    if campaign_state:
        campaign_score = campaign_score_projection(campaign_state)
        score_value = campaign_score["value"]
        prospect["campaign_score"] = campaign_score
        prospect["campaign_fit_score"] = score_value
        prospect["fit_score"] = score_value
        prospect["ai_prospect_score"] = score_value
        raw_campaign_score = campaign_state.get("score") or {}
        prospect["priority_tier"] = raw_campaign_score.get("priority_tier")
        prospect["ai_score_breakdown"] = campaign_score["breakdown"]
        prospect["score_reasoning"] = campaign_score["reasoning"]
        prospect["scoring_version"] = campaign_score["version"]
        prospect["campaign_enrichment"] = campaign_enrichment_projection(
            campaign_state, shared_source=prospect.get("source")
        )
        # Surface the stored AI assessment (strengths / concerns / buying
        # signals / recommended services). Written to campaign_prospect_state by
        # the enrichment pipeline but previously never returned. Campaign-scoped
        # assessment wins over the account-overlay one attached above.
        if campaign_state.get("ai_assessment"):
            prospect["ai_assessment"] = campaign_state["ai_assessment"]

    # Build merged intelligence for response:
    # - prospect_intelligence_base: global (written to prospects collection)
    # - pitch: per-tenant (written to prospect_state, adds pitch_angle / why_they_need_us)
    base_intel = prospect.get("prospect_intelligence_base") or {}
    account_intel = prospect_state_overlay.get("prospect_intelligence") or {}
    campaign_intel = (
        ((campaign_state or {}).get("enrichment") or {}).get("result") or {}
    ).get("prospect_intelligence") or {}
    pitch = prospect_state_overlay.get("pitch") or {}
    merged_intel = {**base_intel, **account_intel, **campaign_intel, **pitch}
    prospect["prospect_intelligence"] = merged_intel if merged_intel else None

    # Fetch the company doc once for competitors merge + company research.
    # Canonical linkage: prospects.company_id == str(company._id);
    # company_linkedin is denormalized/unreliable and used only as fallback.
    comp_doc = None
    try:
        comp_id = prospect.get("company_id")
        if comp_id:
            try:
                comp_doc = await companies_collection.find_one({"_id": ObjectId(str(comp_id))})
            except Exception:
                comp_doc = None
        if comp_doc is None and prospect.get("company_linkedin"):
            comp_doc = await companies_collection.find_one(
                {"linkedin_url": prospect["company_linkedin"]}
            )
        if comp_doc is None and prospect.get("company_linkedin"):
            # Tolerant fallback: LinkedIn URLs are stored inconsistently
            # (scheme / www / trailing slash / case). Build the small set of
            # exact-match candidate strings from the normalized core and use
            # an indexed $in query — never a collection-scan regex.
            raw_url = str(prospect["company_linkedin"]).strip()
            core = raw_url.lower()
            for prefix in ("https://", "http://"):
                if core.startswith(prefix):
                    core = core[len(prefix):]
                    break
            if core.startswith("www."):
                core = core[len("www."):]
            core = core.rstrip("/")
            if core:
                candidates = set()
                for host_variant in (core, f"www.{core}"):
                    for scheme in ("", "https://", "http://"):
                        base = f"{scheme}{host_variant}"
                        candidates.add(base)
                        candidates.add(base + "/")
                candidates.discard(raw_url)  # exact form already tried above
                if candidates:
                    comp_doc = await companies_collection.find_one(
                        {"linkedin_url": {"$in": sorted(candidates)}}
                    )
    except Exception:
        comp_doc = None

    # Merge competitors from company doc (backward compat — unchanged shape)
    if comp_doc and comp_doc.get("competitors") and not prospect.get("competitors"):
        prospect["competitors"] = comp_doc["competitors"]
        prospect["competitors_last_fetched"] = comp_doc.get("competitors_last_fetched")

    # Attach company-level research written by the research pipeline. Shape
    # (when present — code defensively, the field may be absent or partial):
    #   {"competitors": [{name, website_url, linkedin_url, differentiation,
    #                     market_position, is_best_performer, why_winning}],
    #    "best_performer": {name, why_winning} | None,
    #    "news": [...], "buying_signals": [str],
    #    "company_posts": [{text, posted_at, url, reactions, comments}],
    #    "fetched_at": ...}
    # Passed through as-is; the existing company_news overlay (from
    # prospect_state) is kept separately for backward compat.
    research = comp_doc.get("research") if comp_doc else None
    prospect["company_research"] = research if isinstance(research, dict) else None

    return serialize_doc(prospect)


# ── Follow-up sequence / touch points ────────────────────────────────────────

# generated_messages is keyed by message_type, not channel.
_CHANNEL_TO_MSG_KEY = {
    "email": "cold_email",
    "cold_email": "cold_email",
    "linkedin_connection": "linkedin_connection",
    "linkedin_inmail": "linkedin_inmail",
}


def _extract_message(generated: Optional[dict], channel: Optional[str]) -> dict:
    """Pull {subject, body} for a channel out of an enrollment's generated_messages."""
    generated = generated or {}
    key = _CHANNEL_TO_MSG_KEY.get(channel or "", channel or "")
    msg = generated.get(key) or {}
    if not isinstance(msg, dict):
        return {"subject": None, "body": None}
    if channel in ("email", "cold_email"):
        return {"subject": msg.get("subject_a") or msg.get("subject"), "body": msg.get("body")}
    if channel == "linkedin_connection":
        return {"subject": None, "body": msg.get("note")}
    if channel == "linkedin_inmail":
        return {"subject": msg.get("subject"), "body": msg.get("body")}
    return {"subject": msg.get("subject"), "body": msg.get("body") or msg.get("note")}


def _build_touches(enr: dict) -> list[dict]:
    """Derive the ordered list of planned touch points for one enrollment.

    Prefers the multi-node hybrid sequence (``message_drafts``); falls back to
    the single smart-campaign touch (``smart_campaign_*`` + ``generated_messages``).
    """
    touches: list[dict] = []
    drafts = enr.get("message_drafts")
    if isinstance(drafts, dict) and drafts:
        for node_id, d in drafts.items():
            if not isinstance(d, dict):
                continue
            touches.append({
                "node_id": node_id,
                "channel": d.get("channel"),
                "subject": d.get("subject"),
                "body": d.get("body"),
                "status": d.get("status"),
                "scheduled_at": d.get("scheduled_date"),
                "generated_at": d.get("generated_at"),
            })
        # Order by scheduled date (missing dates sort last but keep insertion order).
        touches.sort(key=lambda t: (t.get("scheduled_at") is None, t.get("scheduled_at") or ""))
        return touches

    channel = enr.get("smart_campaign_channel")
    if channel:
        msg = _extract_message(enr.get("generated_messages"), channel)
        touches.append({
            "node_id": None,
            "channel": channel,
            "subject": msg["subject"],
            "body": msg["body"],
            "status": enr.get("message_gen_status"),
            "scheduled_at": enr.get("smart_campaign_scheduled_utc"),
            "send_day": enr.get("smart_campaign_send_day"),
            "generated_at": None,
        })
    return touches


@router.get("/{prospect_id}/touchpoints")
async def get_prospect_touchpoints(
    prospect_id: str,
    campaign_id: Optional[str] = Query(None, description="Restrict to a single campaign"),
    account_ctx=Depends(get_account_context),
):
    """Return the follow-up sequence (all touch points) for a prospect.

    Aggregates the prospect's enrollment(s) for this account into one timeline
    per campaign: the planned touches (drafted messages, channel, scheduled
    time, status) plus the executed step history (what actually went out and
    any replies).
    """
    account_oid = ObjectId(account_ctx["account"]["_id"])
    account_id_str = str(account_oid)
    try:
        pid = ObjectId(prospect_id)
    except Exception:
        raise HTTPException(status_code=400, detail="Invalid prospect ID")

    if not await _account_has_prospect_access(account_id=account_oid, prospect_id=pid):
        raise HTTPException(status_code=404, detail="Prospect not found")

    query: dict = {
        "account_id": {"$in": [account_id_str, account_oid]},
        "prospect_id": {"$in": [prospect_id, pid]},
    }
    if campaign_id:
        try:
            query["campaign_id"] = {"$in": [ObjectId(campaign_id), campaign_id]}
        except Exception:
            query["campaign_id"] = campaign_id

    cursor = campaign_enrollments_collection.find(query)
    enrollments = await cursor.to_list(length=200)

    # Resolve campaign names in one round trip.
    camp_oids = []
    for e in enrollments:
        cid = e.get("campaign_id")
        if isinstance(cid, ObjectId):
            camp_oids.append(cid)
        else:
            try:
                camp_oids.append(ObjectId(cid))
            except Exception:
                pass
    names: dict[str, str] = {}
    if camp_oids:
        async for c in campaigns_collection.find(
            {"_id": {"$in": camp_oids}}, {"name": 1}
        ):
            names[str(c["_id"])] = c.get("name") or "Untitled campaign"

    result: list[dict] = []
    for enr in enrollments:
        cid = str(enr.get("campaign_id"))
        result.append({
            "enrollment_id": str(enr.get("_id")),
            "campaign_id": cid,
            "campaign_name": names.get(cid, "Campaign"),
            "status": enr.get("status"),
            "current_step": enr.get("current_step", 0),
            "next_action_at": enr.get("next_action_at"),
            "enrolled_at": enr.get("enrolled_at"),
            "completed_at": enr.get("completed_at"),
            "last_activity_at": enr.get("last_activity_at"),
            "touches": _build_touches(enr),
            "history": enr.get("step_history") or [],
        })

    # Most recently enrolled campaign first.
    result.sort(key=lambda r: str(r.get("enrolled_at") or ""), reverse=True)

    return serialize_doc({"prospect_id": prospect_id, "enrollments": result})


@router.patch("/{prospect_id}")
async def update_prospect(prospect_id: str, update: dict, account_ctx=Depends(get_account_context)):
    """Update tenant-private prospect fields via prospect_state overlay."""
    account_id = ObjectId(account_ctx["account"]["_id"])
    account_id_str = str(account_id)
    try:
        oid = ObjectId(prospect_id)
    except Exception:
        raise HTTPException(status_code=400, detail="Invalid prospect ID")

    overlay_fields = {
        "status", "notes", "tags", "visible", "excluded", "exclusion_reason"
    }
    update_data = {k: v for k, v in update.items() if k in overlay_fields}

    if "status" in update_data:
        if update_data["status"] not in ("new", "contacted", "qualified", "disqualified"):
            raise HTTPException(status_code=400, detail="Invalid status")
    if "notes" in update_data and (
        not isinstance(update_data["notes"], str) or len(update_data["notes"]) > 5000
    ):
        raise HTTPException(status_code=400, detail="Invalid notes")
    if "tags" in update_data and (
        not isinstance(update_data["tags"], list)
        or len(update_data["tags"]) > 50
        or any(not isinstance(tag, str) or len(tag) > 100 for tag in update_data["tags"])
    ):
        raise HTTPException(status_code=400, detail="Invalid tags")
    for field in ("visible", "excluded"):
        if field in update_data and not isinstance(update_data[field], bool):
            raise HTTPException(status_code=400, detail=f"Invalid {field}")
    if "exclusion_reason" in update_data and (
        update_data["exclusion_reason"] is not None
        and (
            not isinstance(update_data["exclusion_reason"], str)
            or len(update_data["exclusion_reason"]) > 1000
        )
    ):
        raise HTTPException(status_code=400, detail="Invalid exclusion_reason")

    if not update_data:
        raise HTTPException(status_code=400, detail="No valid fields to update")

    from datetime import datetime
    now = datetime.utcnow()

    # Verify prospect exists (global — no account_id filter on prospect).
    # Fetch the filter-key projection so the overlay upsert can refresh `pk`.
    from utils.prospect_filter_keys import PK_PROJECTION, build_filter_keys
    exists = await prospects_collection.find_one({"_id": oid}, PK_PROJECTION)
    if not exists or not await _account_has_prospect_access(
        account_id=account_id, prospect_id=oid, prospect=exists
    ):
        raise HTTPException(status_code=404, detail="Prospect not found")

    update_data["last_updated_at"] = now
    update_data["pk"] = build_filter_keys(exists)
    # A field may not appear in both $set and $setOnInsert (MongoDB path
    # conflict). Only defaults absent from this request belong on insert.
    set_on_insert = {
        k: v for k, v in {
            "account_id": account_id_str,
            "prospect_id": prospect_id,
            "used_by": [],
            "tags": [],
            "created_at": now,
        }.items() if k not in update_data
    }
    await prospect_state_collection.update_one(
        {"account_id": account_id_str, "prospect_id": prospect_id},
        {"$set": update_data, "$setOnInsert": set_on_insert},
        upsert=True,
    )

    return {"message": "Prospect updated", "modified": 1}
