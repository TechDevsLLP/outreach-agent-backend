"""
Industries router - replaces icp_profiles.py.
Industries are DB-backed prospect-source tags. The legacy Apollo-actor scraping
endpoints (scrape / scrape-all / reset-pagination) and AI Apify-param generation
were removed along with the actors that consumed them.
"""

from fastapi import APIRouter, Depends, HTTPException, Query
from typing import Optional
from bson import ObjectId
from datetime import datetime
from auth import get_account_context
from database import industries_collection, prospects_collection, companies_collection
from utils.serialization import serialize_doc
from models.industry import (
    IndustryCreate, IndustryCreateFull, IndustryUpdate,
    ApifyBaseParams, DEFAULT_REGIONS,
)
import logging

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/industries", tags=["Industries"])


@router.get("")
async def list_industries(account_ctx=Depends(get_account_context)):
    """List all industries."""
    account_id = ObjectId(account_ctx["account"]["_id"])
    industries = await industries_collection.find({"account_id": account_id}).to_list(100)
    return {"industries": [serialize_doc(ind) for ind in industries]}


@router.post("")
async def create_industry(request: IndustryCreate, account_ctx=Depends(get_account_context)):
    """
    Simplified industry creation: admin provides a name.
    (AI Apify-param generation was retired with the Apollo prospect actors.)
    3 default regions (USA, Europe, India) are created automatically.
    """
    account_id = ObjectId(account_ctx["account"]["_id"])
    # Check for duplicate name
    existing = await industries_collection.find_one({"account_id": account_id, "name": request.name})
    if existing:
        raise HTTPException(status_code=409, detail=f"Industry '{request.name}' already exists")

    # Build default regions
    regions = [r.model_dump() for r in DEFAULT_REGIONS]

    doc = {
        "account_id": account_id,
        "name": request.name,
        "description": f"{request.name} industry prospects",
        "is_active": True,
        "apify_base_params": {},
        "regions": regions,
        "total_fetch_count": 100,
        "scrape_day": "saturday",
        "scrape_enabled": False,
        "created_at": datetime.utcnow(),
        "last_run_at": None,
        "total_runs": 0,
        "total_prospects_generated": 0,
        "ai_generated": False,
        "user_edited_params": False,
    }

    result = await industries_collection.insert_one(doc)
    doc["_id"] = str(result.inserted_id)

    logger.info(f"Created industry: {request.name} (ID: {doc['_id']})")
    return doc


@router.post("/full")
async def create_industry_full(request: IndustryCreateFull, account_ctx=Depends(get_account_context)):
    """
    Manual industry creation with explicit params and optional custom regions.
    If regions are not provided, 3 default regions are created.
    """
    account_id = ObjectId(account_ctx["account"]["_id"])
    existing = await industries_collection.find_one({"account_id": account_id, "name": request.name})
    if existing:
        raise HTTPException(status_code=409, detail=f"Industry '{request.name}' already exists")

    regions = (
        [r.model_dump() for r in request.regions]
        if request.regions
        else [r.model_dump() for r in DEFAULT_REGIONS]
    )

    doc = {
        "account_id": account_id,
        "name": request.name,
        "description": request.description,
        "is_active": request.is_active,
        "apify_base_params": request.apify_base_params.model_dump(exclude_none=True),
        "regions": regions,
        "total_fetch_count": request.total_fetch_count,
        "scrape_day": "saturday",
        "scrape_enabled": True,
        "created_at": datetime.utcnow(),
        "last_run_at": None,
        "total_runs": 0,
        "total_prospects_generated": 0,
        "ai_generated": False,
        "user_edited_params": False,
    }

    result = await industries_collection.insert_one(doc)
    doc["_id"] = str(result.inserted_id)
    return doc


@router.put("/{industry_id}")
async def update_industry(
    industry_id: str,
    update: IndustryUpdate,
    account_ctx=Depends(get_account_context),
):
    """Update an existing industry (regions, fetch counts, schedule, etc.)."""
    account_id = ObjectId(account_ctx["account"]["_id"])
    try:
        oid = ObjectId(industry_id)
    except Exception:
        raise HTTPException(status_code=400, detail="Invalid industry ID")

    update_data = {}
    if update.name is not None:
        update_data["name"] = update.name
    if update.description is not None:
        update_data["description"] = update.description
    if update.is_active is not None:
        update_data["is_active"] = update.is_active
    if update.apify_base_params is not None:
        update_data["apify_base_params"] = update.apify_base_params.model_dump(exclude_none=True)
        existing = await industries_collection.find_one({"_id": oid, "account_id": account_id})
        if existing and existing.get("ai_generated"):
            update_data["user_edited_params"] = True
    if update.regions is not None:
        update_data["regions"] = [r.model_dump() for r in update.regions]
    if update.total_fetch_count is not None:
        update_data["total_fetch_count"] = update.total_fetch_count
    if update.scrape_day is not None:
        update_data["scrape_day"] = update.scrape_day
    if update.scrape_enabled is not None:
        update_data["scrape_enabled"] = update.scrape_enabled

    if not update_data:
        raise HTTPException(status_code=400, detail="No fields to update")

    result = await industries_collection.update_one({"_id": oid, "account_id": account_id}, {"$set": update_data})
    if result.matched_count == 0:
        raise HTTPException(status_code=404, detail="Industry not found")

    updated = await industries_collection.find_one({"_id": oid, "account_id": account_id})
    updated["_id"] = str(updated["_id"])
    return updated


@router.delete("/{industry_id}")
async def delete_industry(industry_id: str, account_ctx=Depends(get_account_context)):
    """Delete an industry."""
    account_id = ObjectId(account_ctx["account"]["_id"])
    try:
        oid = ObjectId(industry_id)
    except Exception:
        raise HTTPException(status_code=400, detail="Invalid industry ID")

    result = await industries_collection.delete_one({"_id": oid, "account_id": account_id})
    if result.deleted_count == 0:
        raise HTTPException(status_code=404, detail="Industry not found")

    return {"message": "Industry deleted"}


@router.get("/{industry_id}/companies")
async def get_industry_companies(
    industry_id: str,
    page: int = Query(1, ge=1),
    page_size: int = Query(20, ge=1, le=100),
    search: Optional[str] = Query(None),
    min_prospects: int = Query(1, ge=1),
    account_ctx=Depends(get_account_context),
):
    """
    Return companies found within this industry by aggregating prospects.
    Groups by company_linkedin URL, returns company_name, prospect_count, top_prospect, etc.
    """
    account_id = ObjectId(account_ctx["account"]["_id"])
    # Validate industry_id
    try:
        ObjectId(industry_id)
    except Exception:
        raise HTTPException(status_code=400, detail="Invalid industry ID")

    industry_id_str = industry_id

    # Stage 1: match prospects belonging to this industry with a valid company_linkedin
    match_stage: dict = {
        "$match": {
            "account_id": account_id,
            "industry_id": industry_id_str,
            "company_linkedin": {"$exists": True, "$ne": None, "$ne": ""},
        }
    }

    pipeline: list = [match_stage]

    # Optional company name search (applied before grouping via a pre-filter)
    if search:
        pipeline.append({
            "$match": {
                "company_name": {"$regex": search, "$options": "i"}
            }
        })

    # Stage 2: group by company_linkedin
    decision_maker_titles = ["C-Suite", "VP", "Owner", "Director"]
    pipeline.append({
        "$group": {
            "_id": "$company_linkedin",
            "company_name": {"$first": "$company_name"},
            "company_domain": {"$first": "$company_domain"},
            "company_linkedin": {"$first": "$company_linkedin"},
            "prospect_count": {"$sum": 1},
            "enriched_count": {
                "$sum": {
                    "$cond": [{"$eq": ["$enrichment_status", "completed"]}, 1, 0]
                }
            },
            "max_score": {"$max": "$enhanced_score"},
            "statuses": {"$addToSet": "$status"},
            "has_decision_maker": {
                "$max": {
                    "$cond": [
                        {"$in": ["$seniority", decision_maker_titles]},
                        True,
                        False,
                    ]
                }
            },
        }
    })

    # Stage 3: filter by min_prospects
    pipeline.append({"$match": {"prospect_count": {"$gte": min_prospects}}})

    # Stage 4: sort by prospect_count desc
    pipeline.append({"$sort": {"prospect_count": -1}})

    # Stage 5: cross-reference companies_collection for enrichment data
    pipeline.append({
        "$lookup": {
            "from": "companies",
            "localField": "company_linkedin",
            "foreignField": "linkedin_url",
            "as": "company_data",
        }
    })
    pipeline.append({
        "$addFields": {
            "annual_revenue": {"$arrayElemAt": ["$company_data.annual_revenue_clean", 0]},
            "employee_count": {"$arrayElemAt": ["$company_data.employee_count", 0]},
            "company_db_id": {"$arrayElemAt": ["$company_data._id", 0]},
        }
    })
    pipeline.append({"$project": {"company_data": 0}})

    # Facet for total count + paginated results in one round-trip
    skip = (page - 1) * page_size
    pipeline.append({
        "$facet": {
            "total": [{"$count": "count"}],
            "companies": [{"$skip": skip}, {"$limit": page_size}],
        }
    })

    raw = await prospects_collection.aggregate(pipeline).to_list(length=1)
    if not raw:
        return {"companies": [], "total": 0, "page": page, "page_size": page_size}

    facet = raw[0]
    total = facet["total"][0]["count"] if facet["total"] else 0
    companies = []
    for doc in facet.get("companies", []):
        doc.pop("_id", None)
        # Stringify ObjectId on company_db_id if present
        if doc.get("company_db_id") and isinstance(doc["company_db_id"], ObjectId):
            doc["company_db_id"] = str(doc["company_db_id"])
        companies.append(doc)

    return {"companies": companies, "total": total, "page": page, "page_size": page_size}
