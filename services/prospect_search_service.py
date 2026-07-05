"""
Unified prospect search service.

Replaces the six near-duplicate _query_* functions and _apply_icp_filters() in
campaign_prospect_finder_service.py.

Two search modes:
  A. Structured-only: compound filter on company_industry_id + location.country_code
     + seniority + company_employee_band + stage (uses Atlas Search or simple
     Mongo find with compound index — fastest).
  B. Semantic + structured: $vectorSearch on title_vec with pre-filter fields
     (company_industry_id, country_code, seniority) then structural post-filter.

Both modes:
  - Pre-fetch the per-tenant exclusion set (used_by + 90-day cooldown) from
    prospect_state before the main query.
  - $lookup prospect_state for ai_score/status after the DB hit.
  - Filter out opted_out/bounced/disqualified statuses.
  - Sort by ai_score desc (then vectorSearchScore desc for mode B).

Usage:
    from services.prospect_search_service import search_prospects, build_exclusion_set

    excluded = await build_exclusion_set(account_id, user_id, cooldown_days=90, db=db)
    results = await search_prospects(
        db,
        account_id="...",
        industry_ids=["software_development", "saas"],
        country_codes=["US"],
        seniorities=["vp", "director", "c_suite"],
        title_query_vec=await embed_one("VP Sales VP Revenue Head of Sales"),
        exclude_ids=excluded,
        limit=200,
    )
"""

from __future__ import annotations

import logging
from datetime import datetime, timedelta
from typing import Optional

from bson import ObjectId

logger = logging.getLogger(__name__)

# Statuses that exclude a prospect from outreach regardless of tenant
_HARD_EXCLUDE_STATUSES = {"opted_out", "bounced", "disqualified", "unsubscribed"}

# Stages that are ready for outreach (already enriched)
_CONTACTABLE_STAGES = {"assessed", "contactable", "company_enriched", "profile_enriched"}


async def build_exclusion_set(
    db,
    *,
    account_id: str,
    user_id: Optional[str] = None,
    cooldown_days: int = 90,
    campaign_id: Optional[str] = None,
) -> set[str]:
    """
    Pre-fetch the set of prospect_ids this account/user should NOT contact.

    Excludes prospects where:
      - used_by contains an entry for this user (ownership exclusion)
      - used_by contains any entry enrolled within cooldown_days (global cooldown per account)
      - state.status is in _HARD_EXCLUDE_STATUSES

    Also excludes prospects already enrolled in campaign_id (if provided)
    from the campaign_enrollments collection.

    Returns a set of prospect_id strings for use in $nin filters.
    """
    cutoff = datetime.utcnow() - timedelta(days=cooldown_days)
    prospect_state_col = db["prospect_state"]
    campaign_enrollments_col = db["campaign_enrollments"]

    try:
        account_oid = ObjectId(account_id) if not isinstance(account_id, ObjectId) else account_id
    except Exception:
        account_oid = account_id

    # Build the exclusion query against prospect_state
    exclusion_filter: dict = {
        "account_id": {"$in": [str(account_oid), account_oid]},
        "$or": [],
    }

    or_clauses = []

    # Hard-exclude by status
    or_clauses.append({"status": {"$in": list(_HARD_EXCLUDE_STATUSES)}})

    # Owned by this user
    if user_id:
        or_clauses.append({"used_by.user_id": str(user_id)})

    # 90-day cooldown: blocks re-contact after sequence COMPLETION for this user
    if user_id:
        or_clauses.append({
            "used_by": {
                "$elemMatch": {
                    "user_id": str(user_id),
                    "status": "completed",
                    "completed_at": {"$gte": cutoff},
                }
            }
        })

    exclusion_filter["$or"] = or_clauses

    excluded: set[str] = set()

    # Query prospect_state
    async for doc in prospect_state_col.find(exclusion_filter, {"prospect_id": 1}):
        pid = doc.get("prospect_id")
        if pid:
            excluded.add(str(pid))

    # Also exclude from active campaign_enrollments (belt-and-suspenders)
    if campaign_id:
        try:
            cam_oid = ObjectId(campaign_id) if not isinstance(campaign_id, ObjectId) else campaign_id
        except Exception:
            cam_oid = campaign_id

        async for doc in campaign_enrollments_col.find(
            {
                "campaign_id": {"$in": [str(cam_oid), cam_oid]},
                "account_id": {"$in": [str(account_oid), account_oid]},
                "status": {"$in": ["active", "enrolled", "scoring", "enriching", "paused"]},
            },
            {"prospect_id": 1},
        ):
            pid = doc.get("prospect_id")
            if pid:
                excluded.add(str(pid))

    logger.debug("build_exclusion_set: %d prospects excluded for account %s", len(excluded), account_id)
    return excluded


def _build_structured_filter(
    *,
    industry_ids: Optional[list[str]] = None,
    industry_groups: Optional[list[str]] = None,
    country_codes: Optional[list[str]] = None,
    regions: Optional[list[str]] = None,
    seniorities: Optional[list[str]] = None,
    employee_bands: Optional[list[str]] = None,
    company_linkedin_urls: Optional[list[str]] = None,
    stages: Optional[list[str]] = None,
    exclude_ids: Optional[set[str]] = None,
) -> dict:
    """Build a MongoDB filter dict for structured prospect search."""
    filt: dict = {}

    # Industry filter (canonical industry_id or group)
    industry_or: list[dict] = []
    if industry_ids:
        industry_or.append({"company_industry_id": {"$in": industry_ids}})
    if industry_groups:
        industry_or.append({"company_industry_group": {"$in": industry_groups}})
    if industry_or:
        if len(industry_or) == 1:
            filt.update(industry_or[0])
        else:
            filt["$or"] = industry_or

    # Location filter
    location_or: list[dict] = []
    if country_codes:
        location_or.append({"location.country_code": {"$in": country_codes}})
    if regions:
        location_or.append({"location.region": {"$in": regions}})
    if location_or:
        if len(location_or) == 1:
            filt.update(location_or[0])
        else:
            existing_or = filt.pop("$or", [])
            filt["$or"] = existing_or + [{"$or": location_or}] if existing_or else location_or

    # Seniority filter (canonical tokens)
    if seniorities:
        filt["seniority"] = {"$in": seniorities}

    # Employee band filter
    if employee_bands:
        filt["company_employee_band"] = {"$in": employee_bands}

    # Company LinkedIn URL filter (for per-company reuse sub-step)
    if company_linkedin_urls:
        _co_urls_norm = [u.rstrip("/").lower() for u in company_linkedin_urls if u]
        if _co_urls_norm:
            filt["company_linkedin"] = {"$in": _co_urls_norm}

    # Stage filter (readiness for outreach)
    target_stages = stages or list(_CONTACTABLE_STAGES)
    filt["stage"] = {"$in": target_stages}

    # Exclusion set
    if exclude_ids:
        try:
            oids = [ObjectId(eid) for eid in exclude_ids if len(eid) == 24]
        except Exception:
            oids = []
        str_ids = list(exclude_ids)
        exclude_combined = oids + str_ids
        if exclude_combined:
            filt["_id"] = {"$nin": exclude_combined}

    return filt


async def search_prospects_structured(
    db,
    *,
    account_id: str,
    industry_ids: Optional[list[str]] = None,
    industry_groups: Optional[list[str]] = None,
    country_codes: Optional[list[str]] = None,
    regions: Optional[list[str]] = None,
    seniorities: Optional[list[str]] = None,
    employee_bands: Optional[list[str]] = None,
    company_linkedin_urls: Optional[list[str]] = None,
    stages: Optional[list[str]] = None,
    exclude_ids: Optional[set[str]] = None,
    limit: int = 200,
    sort_by_ai_score: bool = True,
) -> list[dict]:
    """
    Mode A: Pure structured search (no vector).

    Uses compound Mongo indexes on (company_industry_id, location.country_code, seniority).
    Fast, deterministic, appropriate when no title-similarity query is given.

    Returns prospect docs annotated with per-tenant state (ai_score, status)
    from prospect_state via $lookup.
    """
    prospects_col = db["prospects"]
    prospect_state_col = db["prospect_state"]

    try:
        account_oid = ObjectId(account_id) if not isinstance(account_id, ObjectId) else account_id
    except Exception:
        account_oid = account_id

    filt = _build_structured_filter(
        industry_ids=industry_ids,
        industry_groups=industry_groups,
        country_codes=country_codes,
        regions=regions,
        seniorities=seniorities,
        employee_bands=employee_bands,
        company_linkedin_urls=company_linkedin_urls,
        stages=stages,
        exclude_ids=exclude_ids,
    )

    # Fetch candidates (over-fetch to account for post-filter by status)
    fetch_limit = limit * 3
    sort = [("last_updated_at", -1)]  # recency as default sort (ai_score added below via overlay)

    candidates: list[dict] = []
    async for doc in prospects_col.find(filt).limit(fetch_limit):
        candidates.append(doc)

    if not candidates:
        return []

    # Bulk-lookup prospect_state for this account
    cand_ids = [doc["_id"] for doc in candidates]
    state_map: dict = {}
    async for state in prospect_state_col.find(
        {
            "account_id": {"$in": [str(account_oid), account_oid]},
            "prospect_id": {"$in": [str(oid) for oid in cand_ids]},
        },
        {"prospect_id": 1, "ai_score": 1, "status": 1, "priority_tier": 1},
    ):
        state_map[str(state.get("prospect_id", ""))] = state

    # Filter out hard-excluded statuses from overlay and annotate
    results: list[dict] = []
    for doc in candidates:
        pid = str(doc["_id"])
        state = state_map.get(pid, {})
        overlay_status = state.get("status", "new")
        if overlay_status in _HARD_EXCLUDE_STATUSES:
            continue
        doc["_state_ai_score"] = state.get("ai_score")
        doc["_state_status"] = overlay_status
        doc["_state_priority_tier"] = state.get("priority_tier")
        results.append(doc)

    # Sort by ai_score (desc) if available
    if sort_by_ai_score:
        results.sort(key=lambda d: (d.get("_state_ai_score") or 0), reverse=True)

    return results[:limit]


async def search_prospects_vector(
    db,
    *,
    account_id: str,
    title_query_vec: list[float],
    industry_ids: Optional[list[str]] = None,
    industry_groups: Optional[list[str]] = None,
    country_codes: Optional[list[str]] = None,
    regions: Optional[list[str]] = None,
    seniorities: Optional[list[str]] = None,
    employee_bands: Optional[list[str]] = None,
    stages: Optional[list[str]] = None,
    exclude_ids: Optional[set[str]] = None,
    limit: int = 200,
    num_candidates: Optional[int] = None,
) -> list[dict]:
    """
    Mode B: Atlas Vector Search on title_vec with structured pre-filters.

    Requires the `prospects_vec` Atlas Vector Search index to be configured on
    the `prospects` collection (see database.py comments for index definition).

    Pre-filter fields available in the vector index:
        company_industry_id, company_industry_group, location.country_code,
        location.region, seniority, company_employee_band, stage

    Over-fetches via numCandidates then post-filters for exclusion and status.
    Returns results annotated with vectorSearchScore and per-tenant ai_score.
    """
    prospects_col = db["prospects"]
    prospect_state_col = db["prospect_state"]

    try:
        account_oid = ObjectId(account_id) if not isinstance(account_id, ObjectId) else account_id
    except Exception:
        account_oid = account_id

    candidates_limit = limit * 3
    _num_candidates = num_candidates or max(candidates_limit * 4, 500)

    # Build the pre-filter for $vectorSearch
    pre_filter: dict = {}
    if industry_ids:
        pre_filter["company_industry_id"] = {"$in": industry_ids}
    elif industry_groups:
        pre_filter["company_industry_group"] = {"$in": industry_groups}
    if country_codes:
        pre_filter["location.country_code"] = {"$in": country_codes}
    elif regions:
        pre_filter["location.region"] = {"$in": regions}
    if seniorities:
        pre_filter["seniority"] = {"$in": seniorities}
    if employee_bands:
        pre_filter["company_employee_band"] = {"$in": employee_bands}

    target_stages = stages or list(_CONTACTABLE_STAGES)
    pre_filter["stage"] = {"$in": target_stages}

    # Build $vectorSearch stage
    vector_stage: dict = {
        "$vectorSearch": {
            "index": "prospects_vec",
            "path": "title_vec",
            "queryVector": title_query_vec,
            "numCandidates": _num_candidates,
            "limit": candidates_limit,
        }
    }
    if pre_filter:
        vector_stage["$vectorSearch"]["filter"] = pre_filter

    # Post-filter: exclusion by _id
    post_match: dict = {}
    if exclude_ids:
        try:
            oids = [ObjectId(eid) for eid in exclude_ids if len(eid) == 24]
        except Exception:
            oids = []
        str_ids = list(exclude_ids)
        exclude_combined = oids + str_ids
        if exclude_combined:
            post_match["_id"] = {"$nin": exclude_combined}

    pipeline = [
        vector_stage,
        {"$addFields": {"_vector_score": {"$meta": "vectorSearchScore"}}},
    ]
    if post_match:
        pipeline.append({"$match": post_match})

    candidates: list[dict] = []
    async for doc in prospects_col.aggregate(pipeline):
        candidates.append(doc)

    if not candidates:
        return []

    # Bulk-lookup prospect_state
    cand_ids = [doc["_id"] for doc in candidates]
    state_map: dict = {}
    async for state in prospect_state_col.find(
        {
            "account_id": {"$in": [str(account_oid), account_oid]},
            "prospect_id": {"$in": [str(oid) for oid in cand_ids]},
        },
        {"prospect_id": 1, "ai_score": 1, "status": 1, "priority_tier": 1},
    ):
        state_map[str(state.get("prospect_id", ""))] = state

    # Filter out hard-excluded statuses and annotate
    results: list[dict] = []
    for doc in candidates:
        pid = str(doc["_id"])
        state = state_map.get(pid, {})
        overlay_status = state.get("status", "new")
        if overlay_status in _HARD_EXCLUDE_STATUSES:
            continue
        doc["_state_ai_score"] = state.get("ai_score")
        doc["_state_status"] = overlay_status
        doc["_state_priority_tier"] = state.get("priority_tier")
        results.append(doc)

    # Sort: ai_score desc, then vector score desc
    results.sort(
        key=lambda d: (
            d.get("_state_ai_score") or 0,
            d.get("_vector_score") or 0,
        ),
        reverse=True,
    )
    return results[:limit]


async def search_prospects(
    db,
    *,
    account_id: str,
    industry_ids: Optional[list[str]] = None,
    industry_groups: Optional[list[str]] = None,
    country_codes: Optional[list[str]] = None,
    regions: Optional[list[str]] = None,
    seniorities: Optional[list[str]] = None,
    employee_bands: Optional[list[str]] = None,
    stages: Optional[list[str]] = None,
    title_query_vec: Optional[list[float]] = None,
    radius: Optional[tuple] = None,               # (lat, lng, meters) — future use
    exclude_used_by_user: Optional[str] = None,
    cooldown_days: int = 90,
    campaign_id: Optional[str] = None,
    limit: int = 200,
    pre_built_exclude_ids: Optional[set[str]] = None,
) -> list[dict]:
    """
    Main entry point: unified prospect search.

    Automatically selects Mode A (structured) or Mode B (vector) based on
    whether title_query_vec is provided.

    Args:
        db: Motor database instance.
        account_id: Tenant account ObjectId string.
        industry_ids: Canonical industry_id values (from industry_canonicalizer).
        industry_groups: Parent industry group names.
        country_codes: ISO-2 country codes (e.g. ["US", "CA"]).
        regions: State/province names (e.g. ["New York", "California"]).
        seniorities: Canonical seniority tokens (e.g. ["vp", "director", "c_suite"]).
        employee_bands: Company size bands (e.g. ["51-200", "201-1000"]).
        stages: Lifecycle stages to include (defaults to contactable stages).
        title_query_vec: 768-dim embedding for semantic title similarity (Mode B).
        radius: (lat, lng, meters) for geo radius filter — future implementation.
        exclude_used_by_user: User ObjectId string whose prospects to exclude.
        cooldown_days: Days since last contact before re-contacting is allowed.
        campaign_id: Campaign to exclude already-enrolled prospects from.
        limit: Max results to return.
        pre_built_exclude_ids: Pre-computed exclusion set (skip rebuild if provided).

    Returns:
        List of prospect dicts annotated with _state_ai_score, _state_status,
        _state_priority_tier (from per-tenant overlay).
    """
    # Build exclusion set (or reuse pre-built one)
    if pre_built_exclude_ids is not None:
        exclude_ids = pre_built_exclude_ids
    else:
        exclude_ids = await build_exclusion_set(
            db,
            account_id=account_id,
            user_id=exclude_used_by_user,
            cooldown_days=cooldown_days,
            campaign_id=campaign_id,
        )

    if title_query_vec:
        # Mode B: vector search
        try:
            return await search_prospects_vector(
                db,
                account_id=account_id,
                title_query_vec=title_query_vec,
                industry_ids=industry_ids,
                industry_groups=industry_groups,
                country_codes=country_codes,
                regions=regions,
                seniorities=seniorities,
                employee_bands=employee_bands,
                stages=stages,
                exclude_ids=exclude_ids,
                limit=limit,
            )
        except Exception as exc:
            logger.warning(
                "Vector search failed (index may not exist yet), falling back to structured: %s", exc
            )
            # Fall through to structured

    # Mode A: structured only
    return await search_prospects_structured(
        db,
        account_id=account_id,
        industry_ids=industry_ids,
        industry_groups=industry_groups,
        country_codes=country_codes,
        regions=regions,
        seniorities=seniorities,
        employee_bands=employee_bands,
        stages=stages,
        exclude_ids=exclude_ids,
        limit=limit,
    )


async def ensure_prospect_state(
    db,
    *,
    account_id: str,
    prospect_id: str,
    status: str = "new",
) -> dict:
    """
    Ensure a prospect_state overlay exists for this (account, prospect) pair.
    Creates it with status="new" if absent. Returns the overlay doc.
    """
    prospect_state_col = db["prospect_state"]
    now = datetime.utcnow()

    result = await prospect_state_col.find_one_and_update(
        {"account_id": str(account_id), "prospect_id": str(prospect_id)},
        {
            "$setOnInsert": {
                "account_id": str(account_id),
                "prospect_id": str(prospect_id),
                "status": status,
                "used_by": [],
                "tags": [],
                "created_at": now,
                "last_updated_at": now,
            }
        },
        upsert=True,
        return_document=True,
    )
    return result or {}


async def push_used_by(
    db,
    *,
    account_id: str,
    prospect_id: str,
    user_id: str,
    campaign_id: str,
    status: str = "active",
) -> None:
    """
    Append a used_by entry to the prospect_state overlay.
    Called alongside campaign_enrollments creation.
    """
    prospect_state_col = db["prospect_state"]
    now = datetime.utcnow()

    entry = {
        "user_id": str(user_id),
        "campaign_id": str(campaign_id),
        "status": status,
        "enrolled_at": now,
    }

    await prospect_state_col.update_one(
        {"account_id": str(account_id), "prospect_id": str(prospect_id)},
        {
            "$push": {"used_by": entry},
            "$set": {"last_updated_at": now},
            "$setOnInsert": {
                "status": "new",
                "tags": [],
                "created_at": now,
            },
        },
        upsert=True,
    )


async def update_used_by_status(
    db,
    *,
    account_id: str,
    prospect_id: str,
    campaign_id: str,
    new_status: str,
    completed_at: Optional[datetime] = None,
) -> None:
    """
    Update the status of a used_by entry for a specific campaign.
    Called when the campaign engine advances / completes the enrollment.
    """
    prospect_state_col = db["prospect_state"]
    now = datetime.utcnow()

    set_fields = {
        "used_by.$.status": new_status,
        "last_updated_at": now,
    }
    if completed_at is not None:
        set_fields["used_by.$.completed_at"] = completed_at

    await prospect_state_col.update_one(
        {
            "account_id": str(account_id),
            "prospect_id": str(prospect_id),
            "used_by.campaign_id": str(campaign_id),
        },
        {"$set": set_fields},
    )


async def search_companies_structured(
    db,
    *,
    industry_ids: Optional[list[str]] = None,
    country_codes: Optional[list[str]] = None,
    employee_bands: Optional[list[str]] = None,
    exclude_linkedin_urls: Optional[set[str]] = None,
    limit: int = 200,
) -> list[dict]:
    """
    Query the shared companies_collection by canonical ICP fields.

    Hard filters (applied only when non-empty): industry_ids, country_codes, employee_bands.
    Funding is never a hard filter — handled as a soft score boost upstream.

    Uses existing compound indexes:
      - industry.id + location.country_code
      - industry.id + employee_band

    Returns companies sorted by prospect_count desc (prefer companies that already
    yielded contactable prospects) then last_scraped_at desc.
    """
    companies_col = db["companies"]
    filt: dict = {}

    if industry_ids:
        filt["industry.id"] = {"$in": industry_ids}
    if country_codes:
        filt["location.country_code"] = {"$in": country_codes}
    if employee_bands:
        filt["employee_band"] = {"$in": employee_bands}
    if exclude_linkedin_urls:
        _excl = [u.rstrip("/").lower() for u in exclude_linkedin_urls if u]
        if _excl:
            filt["linkedin_url"] = {"$nin": _excl}

    companies: list[dict] = await companies_col.find(filt).sort([
        ("prospect_count", -1),
        ("last_scraped_at", -1),
    ]).limit(limit).to_list(length=limit)

    return companies
