"""
Superadmin — shared prospect/company pool operations.

- /pool/stats: counts + coverage percentages across the global pool
- /pool/quality: duplicates, missing-field breakdowns, stale records
- /pool/reenrich: enqueue re-enrichment via the existing pipeline (hard-capped)
- /suppression: global suppression list management
"""
import asyncio
import time
from datetime import datetime, timedelta, timezone
from typing import Optional

from bson import ObjectId
from fastapi import APIRouter, Depends, HTTPException, Query
from pydantic import BaseModel, Field

import database
from auth import get_super_admin
from models.suppression import SUPPRESSION_IDENTIFIER_TYPES, SUPPRESSION_REASONS
from services.admin_audit_service import log_admin_action

router = APIRouter(prefix="/api/admin", tags=["Admin"])

REENRICH_HARD_CAP = 500
STALE_DAYS = 180


def _serialize(doc):
    if isinstance(doc, ObjectId):
        return str(doc)
    if isinstance(doc, dict):
        return {k: _serialize(v) for k, v in doc.items()}
    if isinstance(doc, list):
        return [_serialize(v) for v in doc]
    return doc


def _pct(part: int, whole: int) -> Optional[float]:
    return round(100.0 * part / whole, 2) if whole else None


# ---------------------------------------------------------------------------
# GET /api/admin/pool/stats
# ---------------------------------------------------------------------------

def _sum_if_set(field: str) -> dict:
    """$group accumulator: count docs where `field` exists and is not null/''.
    Aggregation equivalent of count_documents({field: {"$exists": True,
    "$nin": [None, ""]}}) — $ifNull maps missing -> null."""
    return {"$sum": {"$cond": [{"$in": [{"$ifNull": [f"${field}", None]}, [None, ""]]}, 0, 1]}}


def _sum_if_not_null(field: str) -> dict:
    """$group accumulator: count docs where `field` exists and is not null.
    Aggregation equivalent of count_documents({field: {"$exists": True, "$ne": None}})."""
    return {"$sum": {"$cond": [{"$eq": [{"$ifNull": [f"${field}", None]}, None]}, 0, 1]}}


# The coverage aggregations scan prospects+companies (~340 ms warm on the real
# pool) and the numbers move slowly — cache in-process for 60 s, same pattern
# as /health/deep's search-index cache. ?refresh=true bypasses.
_POOL_STATS_CACHE_TTL = 60.0
_pool_stats_cache: dict = {"ts": 0.0, "value": None}


@router.get("/pool/stats")
async def get_pool_stats(
    refresh: bool = Query(False, description="Bypass the 60 s stats cache"),
    _admin: dict = Depends(get_super_admin),
):
    now_mono = time.monotonic()
    if (
        not refresh
        and _pool_stats_cache["value"] is not None
        and now_mono - _pool_stats_cache["ts"] < _POOL_STATS_CACHE_TTL
    ):
        return _pool_stats_cache["value"]

    p = database.prospects_collection
    c = database.companies_collection

    # One aggregation pass per collection computes all coverage numerators
    # (instead of N count_documents, each of which is its own scan). Raw
    # totals stay estimated_document_count (metadata-only, no scan).
    prospects_pipeline = [
        {"$group": {
            "_id": None,
            "location": _sum_if_set("location.country_code"),
            "industry": _sum_if_set("company_industry_id"),
            "embeddings": _sum_if_not_null("title_vec"),
            "email": _sum_if_set("email"),
            "phone": _sum_if_set("phone"),
            "company_linked": _sum_if_set("company_id"),
        }},
    ]
    companies_pipeline = [
        {"$group": {
            "_id": None,
            "location": _sum_if_set("location.country_code"),
            "industry": _sum_if_set("industry.id"),
            "embeddings": _sum_if_not_null("profile_vec"),
        }},
    ]

    (
        prospects_total,
        companies_total,
        p_cov_rows,
        c_cov_rows,
        overlay_rows,
    ) = await asyncio.gather(
        p.estimated_document_count(),
        c.estimated_document_count(),
        p.aggregate(prospects_pipeline).to_list(1),
        c.aggregate(companies_pipeline).to_list(1),
        # prospect_state overlay counts per account
        database.prospect_state_collection.aggregate([
            {"$group": {"_id": "$account_id", "count": {"$sum": 1}}},
            {"$sort": {"count": -1}},
            {"$limit": 200},
        ]).to_list(200),
    )

    _p_empty = {"location": 0, "industry": 0, "embeddings": 0, "email": 0, "phone": 0, "company_linked": 0}
    p_cov = p_cov_rows[0] if p_cov_rows else _p_empty
    c_cov = c_cov_rows[0] if c_cov_rows else {"location": 0, "industry": 0, "embeddings": 0}
    p_location, p_industry, p_embeddings = p_cov["location"], p_cov["industry"], p_cov["embeddings"]
    p_email, p_phone, p_company_linked = p_cov["email"], p_cov["phone"], p_cov["company_linked"]
    c_location, c_industry, c_embeddings = c_cov["location"], c_cov["industry"], c_cov["embeddings"]
    # attach account names (prospect_state.account_id is a string)
    oids = []
    for r in overlay_rows:
        try:
            oids.append(ObjectId(r["_id"]))
        except Exception:
            pass
    accounts = await database.accounts_collection.find(
        {"_id": {"$in": oids}}, {"name": 1}
    ).to_list(None)
    names = {str(a["_id"]): a.get("name") for a in accounts}

    result = {
        "prospects": {
            "total": prospects_total,
            "coverage": {
                "location": {"count": p_location, "pct": _pct(p_location, prospects_total)},
                "canonical_industry": {"count": p_industry, "pct": _pct(p_industry, prospects_total)},
                "embeddings": {"count": p_embeddings, "pct": _pct(p_embeddings, prospects_total)},
                "email": {"count": p_email, "pct": _pct(p_email, prospects_total)},
                "phone": {"count": p_phone, "pct": _pct(p_phone, prospects_total)},
                "company_linked": {"count": p_company_linked, "pct": _pct(p_company_linked, prospects_total)},
            },
        },
        "companies": {
            "total": companies_total,
            "coverage": {
                "location": {"count": c_location, "pct": _pct(c_location, companies_total)},
                "canonical_industry": {"count": c_industry, "pct": _pct(c_industry, companies_total)},
                "embeddings": {"count": c_embeddings, "pct": _pct(c_embeddings, companies_total)},
            },
        },
        "prospect_state_by_account": [
            {"account_id": r["_id"], "account_name": names.get(str(r["_id"])), "count": r["count"]}
            for r in overlay_rows
        ],
    }
    _pool_stats_cache["ts"] = now_mono
    _pool_stats_cache["value"] = result
    return result


# ---------------------------------------------------------------------------
# GET /api/admin/pool/quality
# ---------------------------------------------------------------------------

@router.get("/pool/quality")
async def get_pool_quality(_admin: dict = Depends(get_super_admin)):
    from services.duplicate_detection_service import get_duplication_stats

    p = database.prospects_collection
    stale_cutoff = datetime.utcnow() - timedelta(days=STALE_DAYS)
    _missing = lambda field: {"$or": [{field: {"$exists": False}}, {field: {"$in": [None, ""]}}]}  # noqa: E731

    (
        dup_stats,
        missing_email, missing_linkedin, missing_title,
        missing_company, missing_location, missing_seniority,
        stale_enriched, never_enriched, failed_enrichment,
    ) = await asyncio.gather(
        get_duplication_stats(),
        p.count_documents(_missing("email")),
        p.count_documents(_missing("linkedin")),
        p.count_documents(_missing("title")),
        p.count_documents(_missing("company_id")),
        p.count_documents(_missing("location.country_code")),
        p.count_documents(_missing("seniority")),
        p.count_documents({"enrichment_completed_at": {"$lt": stale_cutoff}}),
        p.count_documents({"enrichment_status": {"$in": ["not_started", None]}}),
        p.count_documents({"enrichment_status": "failed"}),
    )

    return {
        "duplicates": dup_stats,
        "missing_fields": {
            "email": missing_email,
            "linkedin": missing_linkedin,
            "title": missing_title,
            "company_id": missing_company,
            "location": missing_location,
            "seniority": missing_seniority,
        },
        "staleness": {
            "stale_days_threshold": STALE_DAYS,
            "enriched_older_than_threshold": stale_enriched,
            "never_enriched": never_enriched,
            "failed_enrichment": failed_enrichment,
        },
    }


# ---------------------------------------------------------------------------
# POST /api/admin/pool/reenrich
# ---------------------------------------------------------------------------

class ReenrichRequest(BaseModel):
    """At least one filter is required. dry_run defaults to True; enqueueing
    requires an explicit dry_run=false."""
    prospect_ids: Optional[list[str]] = None
    industry_id: Optional[str] = Field(None, description="prospects.company_industry_id")
    account_id: Optional[str] = Field(None, description="Limit to prospects in this account's prospect_state overlay")
    limit: int = Field(100, ge=1, le=REENRICH_HARD_CAP)
    dry_run: bool = True


@router.post("/pool/reenrich")
async def reenrich_pool(
    body: ReenrichRequest,
    admin: dict = Depends(get_super_admin),
):
    if not (body.prospect_ids or body.industry_id or body.account_id):
        raise HTTPException(
            status_code=400,
            detail="At least one filter (prospect_ids, industry_id, account_id) is required",
        )
    if not body.account_id:
        raise HTTPException(
            status_code=400,
            detail="account_id is required because enrichment state is tenant-owned",
        )
    if not ObjectId.is_valid(body.account_id):
        raise HTTPException(status_code=400, detail="Invalid account_id")

    limit = min(body.limit, REENRICH_HARD_CAP)
    query: dict = {}

    if body.prospect_ids:
        oids = []
        for pid in body.prospect_ids[:REENRICH_HARD_CAP]:
            try:
                oids.append(ObjectId(pid))
            except Exception:
                raise HTTPException(status_code=400, detail=f"Invalid prospect ID: {pid}")
        query["_id"] = {"$in": oids}

    if body.industry_id:
        query["company_industry_id"] = body.industry_id

    if body.account_id:
        # prospects are global; account scoping goes through the prospect_state overlay
        state_ids = await database.prospect_state_collection.find(
            {"account_id": body.account_id}, {"prospect_id": 1}
        ).to_list(REENRICH_HARD_CAP * 4)
        overlay_oids = []
        for s in state_ids:
            try:
                overlay_oids.append(ObjectId(s["prospect_id"]))
            except Exception:
                pass
        if "_id" in query:
            overlay_set = set(overlay_oids)
            query["_id"] = {"$in": [o for o in query["_id"]["$in"] if o in overlay_set]}
        else:
            query["_id"] = {"$in": overlay_oids}

    docs = await database.prospects_collection.find(query, {"_id": 1}).limit(limit).to_list(limit)
    prospect_ids = [str(d["_id"]) for d in docs]

    if body.dry_run:
        return {
            "dry_run": True,
            "matched": len(prospect_ids),
            "limit": limit,
            "sample_prospect_ids": prospect_ids[:20],
            "message": "No enrichment enqueued. Re-send with dry_run=false to enqueue.",
        }

    if not prospect_ids:
        raise HTTPException(status_code=400, detail="No prospects matched the filter")

    from services.enrichment_job_service import enqueue_enrichment_run

    run_doc = {
        "account_id": str(body.account_id),
        "status": "queued",
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
        "started_at": None,
        "created_at": datetime.utcnow(),
        "completed_at": None,
        "current_step": "queued",
        "error": None,
        "triggered_by": None,
        "triggered_via": "admin_reenrich",
    }
    result = await database.enrichment_runs_collection.insert_one(run_doc)
    run_id = str(result.inserted_id)

    options = {
        "skip_pre_enrichment_triage": True,  # admin explicitly selected these prospects
    }
    if body.account_id:
        options["account_id"] = body.account_id
    await enqueue_enrichment_run(
        account_id=str(body.account_id), run_id=run_id,
        prospect_ids=prospect_ids, options=options,
    )

    await log_admin_action(
        admin["email"], "pool.reenrich", "enrichment_run", run_id,
        {
            "filters": body.model_dump(exclude={"dry_run"}),
            "prospects_enqueued": len(prospect_ids),
        },
    )
    return {
        "dry_run": False,
        "enrichment_run_id": run_id,
        "prospects_enqueued": len(prospect_ids),
        "status": "queued",
    }


# ---------------------------------------------------------------------------
# Suppression management
# ---------------------------------------------------------------------------

@router.get("/suppression")
async def list_suppressions(
    account_id: Optional[str] = Query(None),
    identifier: Optional[str] = Query(None, description="Exact identifier match (lowercased)"),
    identifier_type: Optional[str] = Query(None),
    page: int = Query(1, ge=1),
    page_size: int = Query(50, ge=1, le=200),
    _admin: dict = Depends(get_super_admin),
):
    query: dict = {}
    if account_id:
        query["account_id"] = account_id
    if identifier:
        query["identifier"] = identifier.lower()
    if identifier_type:
        query["identifier_type"] = identifier_type

    total = await database.suppressions_collection.count_documents(query)
    skip = (page - 1) * page_size
    rows = await (
        database.suppressions_collection.find(query)
        .sort("created_at", -1).skip(skip).limit(page_size)
        .to_list(page_size)
    )
    return {
        "suppressions": [_serialize(r) for r in rows],
        "total": total,
        "page": page,
        "page_size": page_size,
    }


class SuppressionCreateRequest(BaseModel):
    account_id: str
    identifier_type: str  # email|linkedin_url|prospect_id
    identifier: str
    reason: str = "manual"
    notes: Optional[str] = None


@router.post("/suppression", status_code=201)
async def create_suppression(
    body: SuppressionCreateRequest,
    admin: dict = Depends(get_super_admin),
):
    if body.identifier_type not in SUPPRESSION_IDENTIFIER_TYPES:
        raise HTTPException(
            status_code=400,
            detail=f"identifier_type must be one of {SUPPRESSION_IDENTIFIER_TYPES}",
        )
    if body.reason not in SUPPRESSION_REASONS:
        raise HTTPException(
            status_code=400,
            detail=f"reason must be one of {SUPPRESSION_REASONS}",
        )

    identifier = body.identifier.strip().lower()
    if not identifier:
        raise HTTPException(status_code=400, detail="identifier is required")

    key = {
        "account_id": body.account_id,
        "identifier_type": body.identifier_type,
        "identifier": identifier,
    }
    await database.suppressions_collection.update_one(
        key,
        {"$setOnInsert": {
            **key,
            "reason": body.reason,
            "source": "human",
            "notes": body.notes,
            "created_by": admin["email"],
            "created_at": datetime.now(timezone.utc),
        }},
        upsert=True,
    )
    doc = await database.suppressions_collection.find_one(key)

    await log_admin_action(
        admin["email"], "suppression.create", "suppression", str(doc["_id"]),
        {"account_id": body.account_id, "identifier_type": body.identifier_type,
         "identifier": identifier, "reason": body.reason},
    )
    return _serialize(doc)


@router.delete("/suppression/{suppression_id}", status_code=204)
async def delete_suppression(
    suppression_id: str,
    admin: dict = Depends(get_super_admin),
):
    try:
        oid = ObjectId(suppression_id)
    except Exception:
        raise HTTPException(status_code=404, detail="Suppression not found")

    doc = await database.suppressions_collection.find_one({"_id": oid})
    if not doc:
        raise HTTPException(status_code=404, detail="Suppression not found")

    await database.suppressions_collection.delete_one({"_id": oid})
    await log_admin_action(
        admin["email"], "suppression.delete", "suppression", suppression_id,
        {"account_id": doc.get("account_id"), "identifier_type": doc.get("identifier_type"),
         "identifier": doc.get("identifier"), "reason": doc.get("reason")},
    )
