import asyncio
import logging
import re
from datetime import datetime
from io import BytesIO
from typing import List, Optional

import pandas as pd
from bson import ObjectId
from fastapi import APIRouter, BackgroundTasks, Depends, File, Form, HTTPException, Query, UploadFile
from pydantic import BaseModel, Field

from auth import get_account_context, get_super_admin
from database import (
    companies_collection,
    employees_collection,
    prospect_state_collection,
    prospects_collection,
)
logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/companies", tags=["Companies"])


async def scrape_companies_and_employees(*args, **kwargs):
    """Load the paid-provider scraper only when an authorized job executes."""
    from services.employee_scraper_service import (
        scrape_companies_and_employees as run_scraper,
    )

    return await run_scraper(*args, **kwargs)


def _str_id(doc: dict) -> dict:
    """Stringify ObjectId fields in a document (mutates in place)."""
    for key in ("_id", "account_id", "user_id", "company_id", "prospect_id", "industry_id"):
        if key in doc and isinstance(doc[key], ObjectId):
            doc[key] = str(doc[key])
    return doc


# ---------------------------------------------------------------------------
# GET /api/companies
# ---------------------------------------------------------------------------
@router.get("")
async def list_companies(
    industry: Optional[str] = Query(None),
    industry_id: Optional[str] = Query(None),
    search: Optional[str] = Query(None),
    min_prospects: int = Query(0, ge=0),
    min_employees: Optional[int] = Query(None, ge=0),
    max_employees: Optional[int] = Query(None, ge=0),
    has_website: Optional[bool] = Query(None),
    sort_by: str = "name",
    sort_order: str = "asc",
    page: int = Query(1, ge=1),
    page_size: int = Query(20, ge=1, le=100),
    ctx: dict = Depends(get_account_context),
):
    """Browse companies with optional filters, sort, and pagination.

    prospect_count is computed live via a $lookup against prospects.company_id
    (prospects store company_id as the stringified company _id) — there is no
    longer a denormalized prospect_count stored on the company doc.

    Search is a case-insensitive substring match on name / domain / website
    rather than a `$text` query: the list is a lookup tool, and users type
    partial names ("acme" should find "Acme Robotics"), which `$text`
    whole-word matching does not do.
    """
    filters: list[dict] = []

    if search and search.strip():
        pattern = re.escape(search.strip())
        filters.append({"$or": [
            {"name": {"$regex": pattern, "$options": "i"}},
            {"domain": {"$regex": pattern, "$options": "i"}},
            {"website": {"$regex": pattern, "$options": "i"}},
        ]})

    if industry:
        filters.append({"$or": [
            {"industry.label": {"$regex": re.escape(industry), "$options": "i"}},
            {"industry.raw": {"$regex": re.escape(industry), "$options": "i"}},
        ]})

    if industry_id:
        filters.append({"industry.id": industry_id})

    if min_employees is not None or max_employees is not None:
        size_range: dict = {}
        if min_employees is not None:
            size_range["$gte"] = min_employees
        if max_employees is not None:
            size_range["$lte"] = max_employees
        filters.append({"employee_count": size_range})

    if has_website is not None:
        site_present = {"$or": [
            {"website": {"$nin": [None, ""]}},
            {"domain": {"$nin": [None, ""]}},
        ]}
        filters.append(site_present if has_website else {"$nor": [site_present]})

    query: dict = {"$and": filters} if filters else {}

    sort_direction = -1 if sort_order == "desc" else 1
    # "industry" is a subdocument — sort on its label so the ordering is
    # alphabetical rather than by BSON field order.
    allowed_sorts = {
        "name": "name",
        "industry": "industry.label",
        "employee_count": "employee_count",
        "prospect_count": "prospect_count",
    }
    sort_field = allowed_sorts.get(sort_by, "name")

    skip = (page - 1) * page_size

    projection = {
        "_id": 1,
        "name": 1,
        "domain": 1,
        "linkedin_url": 1,
        "website": 1,
        "industry": 1,
        "employee_count": 1,
        "employee_band": 1,
        "prospect_count": 1,
        "location": 1,
        "founded_year": 1,
        "company_type": 1,
        "description": 1,
        "tagline": 1,
    }

    # Live prospect_count via $lookup against prospects.company_id.
    lookup_stages = [
        {
            "$lookup": {
                "from": "prospects",
                "let": {"cid": {"$toString": "$_id"}},
                "pipeline": [
                    {"$match": {"$expr": {"$eq": ["$company_id", "$$cid"]}}},
                    {"$count": "n"},
                ],
                "as": "_pc",
            }
        },
        {"$addFields": {"prospect_count": {"$ifNull": [{"$arrayElemAt": ["$_pc.n", 0]}, 0]}}},
    ]

    # A few dozen company docs are bare stubs — just an _id and a linkedin_url,
    # queued for a scrape that never completed. Mongo sorts missing values FIRST
    # ascending, so without this they'd monopolise page 1 and the list would
    # look completely empty. Rank rows with a value ahead of rows without one,
    # in either sort direction; the stubs still appear, just at the end.
    rank_stage = {
        "$addFields": {
            "_missing_sort_key": {
                "$cond": [
                    {"$in": [{"$ifNull": [f"${sort_field}", ""]}, [None, ""]]},
                    1,
                    0,
                ]
            }
        }
    }
    sort_stage = {"$sort": {"_missing_sort_key": 1, sort_field: sort_direction, "_id": 1}}

    needs_prospect_count_first = min_prospects > 0 or sort_field == "prospect_count"

    pipeline: list[dict] = [{"$match": query}]

    if needs_prospect_count_first:
        # Must compute counts before filtering/sorting/paginating on them.
        pipeline += lookup_stages
        if min_prospects > 0:
            pipeline.append({"$match": {"prospect_count": {"$gte": min_prospects}}})
        pipeline.append(
            {
                "$facet": {
                    "total": [{"$count": "count"}],
                    "rows": [
                        rank_stage,
                        sort_stage,
                        {"$skip": skip},
                        {"$limit": page_size},
                        {"$project": projection},
                    ],
                }
            }
        )
    else:
        # Paginate first, then look up counts for just the current page.
        pipeline.append(
            {
                "$facet": {
                    "total": [{"$count": "count"}],
                    "rows": [
                        rank_stage,
                        sort_stage,
                        {"$skip": skip},
                        {"$limit": page_size},
                        *lookup_stages,
                        {"$project": projection},
                    ],
                }
            }
        )

    result = await companies_collection.aggregate(pipeline).to_list(length=1)
    data = result[0] if result else {}
    total = (data.get("total") or [{}])[0].get("count", 0) if data.get("total") else 0
    companies = [_str_id(doc) for doc in data.get("rows", [])]

    return {"companies": companies, "total": total, "page": page, "page_size": page_size}


# ---------------------------------------------------------------------------
# GET /api/companies/by-industry  (public — no auth required)
# Must be declared BEFORE /{company_id} to avoid route collision
# ---------------------------------------------------------------------------
@router.get("/by-industry")
async def companies_by_industry():
    """Aggregate company and prospect counts grouped by industry (public endpoint).

    prospect_count is computed live via $lookup against prospects.company_id.
    """
    pipeline = [
        {
            "$lookup": {
                "from": "prospects",
                "let": {"cid": {"$toString": "$_id"}},
                "pipeline": [
                    {"$match": {"$expr": {"$eq": ["$company_id", "$$cid"]}}},
                    {"$count": "n"},
                ],
                "as": "_pc",
            }
        },
        {"$addFields": {"prospect_count": {"$ifNull": [{"$arrayElemAt": ["$_pc.n", 0]}, 0]}}},
        {
            "$group": {
                "_id": {"id": "$industry.id", "label": "$industry.label"},
                "company_count": {"$sum": 1},
                "total_prospects": {"$sum": {"$ifNull": ["$prospect_count", 0]}},
                "top_companies": {
                    "$push": {
                        "name": "$name",
                        "domain": "$domain",
                        "prospect_count": {"$ifNull": ["$prospect_count", 0]},
                    }
                },
            }
        },
        {"$sort": {"total_prospects": -1}},
        {
            "$project": {
                "_id": 0,
                "industry_id": "$_id.id",
                "industry": "$_id.label",
                "company_count": 1,
                "total_prospects": 1,
                "top_companies": {
                    "$slice": [
                        {
                            "$sortArray": {
                                "input": "$top_companies",
                                "sortBy": {"prospect_count": -1},
                            }
                        },
                        3,
                    ]
                },
            }
        },
    ]
    results = await companies_collection.aggregate(pipeline).to_list(length=None)
    return results


# ---------------------------------------------------------------------------
# GET /api/companies/stats  (must be declared BEFORE /{company_id})
# ---------------------------------------------------------------------------
@router.get("/stats")
async def get_company_stats(ctx: dict = Depends(get_account_context)):
    """Headline stats for the companies list plus the industry facet used by
    the industry filter dropdown.

    Deliberately does NOT report an average employee count: it mixes
    LinkedIn's self-reported headcount across every company in the pool and
    answers no question anyone asks. The numbers here are about coverage —
    how much of the pool is actually usable for outreach.
    """
    company_facets = [
        {
            "$facet": {
                "total": [{"$count": "count"}],
                "with_website": [
                    {"$match": {"$or": [
                        {"website": {"$nin": [None, ""]}},
                        {"domain": {"$nin": [None, ""]}},
                    ]}},
                    {"$count": "count"},
                ],
                "by_industry": [
                    {"$match": {"industry.label": {"$ne": None, "$exists": True}}},
                    {"$group": {"_id": "$industry.label", "count": {"$sum": 1}}},
                    {"$sort": {"count": -1}},
                    {"$limit": 50},
                ],
            }
        }
    ]

    # Companies that actually have people attached, and the total people count.
    # prospects.company_id is the stringified company _id.
    prospect_facets = [
        {"$match": {"company_id": {"$nin": [None, ""]}}},
        {"$group": {"_id": "$company_id", "n": {"$sum": 1}}},
        {"$group": {"_id": None, "companies": {"$sum": 1}, "prospects": {"$sum": "$n"}}},
    ]

    company_result, prospect_result, total_employees = await asyncio.gather(
        companies_collection.aggregate(company_facets).to_list(length=1),
        prospects_collection.aggregate(prospect_facets).to_list(length=1),
        employees_collection.estimated_document_count(),
    )

    data = company_result[0] if company_result else {}

    def _count(key: str) -> int:
        rows = data.get(key) or []
        return rows[0].get("count", 0) if rows else 0

    prospect_data = prospect_result[0] if prospect_result else {}

    return {
        "total_companies": _count("total"),
        "companies_with_prospects": prospect_data.get("companies", 0),
        "total_prospects": prospect_data.get("prospects", 0),
        "total_employees_scraped": total_employees,
        "companies_with_website": _count("with_website"),
        "by_industry": data.get("by_industry", []),
    }


# ---------------------------------------------------------------------------
# POST /api/companies/scrape
# ---------------------------------------------------------------------------

_VALID_SENIORITIES = {"120", "210", "220", "300", "310", "320"}


class ScrapeCompaniesRequest(BaseModel):
    company_urls: List[str]
    max_employees_per_company: int = 50
    seniority_levels: Optional[List[str]] = None
    scraper_mode: str = "Short ($4 per 1k)"


class BulkCompanyIdsRequest(BaseModel):
    company_ids: List[str] = Field(..., min_length=1, max_length=100)


@router.post("/scrape")
async def scrape_companies(
    body: ScrapeCompaniesRequest,
    _admin: dict = Depends(get_super_admin),
):
    """Scrape company pages and their employees from a list of LinkedIn URLs."""
    urls = [u.strip() for u in body.company_urls if u.strip()]
    invalid = [u for u in urls if not u.startswith("http") or "linkedin.com/company" not in u]
    if invalid:
        raise HTTPException(
            status_code=400,
            detail=f"Invalid LinkedIn company URLs: {invalid[:3]}{'...' if len(invalid) > 3 else ''}",
        )
    if not urls:
        raise HTTPException(status_code=400, detail="At least one company URL is required")
    if len(urls) > 100:
        raise HTTPException(status_code=400, detail="Maximum 100 URLs per request")

    seniority_levels = body.seniority_levels
    if seniority_levels:
        bad = [s for s in seniority_levels if s not in _VALID_SENIORITIES]
        if bad:
            raise HTTPException(status_code=400, detail=f"Invalid seniority levels: {bad}")

    result = await scrape_companies_and_employees(
        company_urls=urls,
        max_employees_per_company=max(1, min(500, body.max_employees_per_company)),
        seniority_levels=seniority_levels,
        scraper_mode=body.scraper_mode,
    )
    return result


# ---------------------------------------------------------------------------
# POST /api/companies/scrape/upload-excel
# ---------------------------------------------------------------------------

_URL_COLUMNS = {"company_url", "url", "company url", "linkedin_url", "linkedin url"}


@router.post("/scrape/upload-excel")
async def scrape_from_excel(
    file: UploadFile = File(...),
    max_employees_per_company: int = Form(50),
    seniority_levels: List[str] = Form(default=[]),
    scraper_mode: str = Form("Short ($4 per 1k)"),
    _admin: dict = Depends(get_super_admin),
):
    """Parse an Excel or CSV file to extract LinkedIn company URLs and start scraping."""
    filename = file.filename or ""
    ext = filename.rsplit(".", 1)[-1].lower() if "." in filename else ""
    if ext not in ("xlsx", "xls", "csv"):
        raise HTTPException(status_code=400, detail="Only .xlsx, .xls, and .csv files are supported")

    contents = await file.read()
    try:
        if ext == "csv":
            df = pd.read_csv(BytesIO(contents))
        else:
            df = pd.read_excel(BytesIO(contents))
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"Could not parse file: {e}")

    # Find the URL column (case-insensitive, whitespace-tolerant)
    col_map = {str(c).strip().lower(): c for c in df.columns}
    url_col = next((col_map[key] for key in _URL_COLUMNS if key in col_map), None)
    if url_col is None:
        raise HTTPException(
            status_code=400,
            detail=f"No URL column found. Expected one of: {', '.join(_URL_COLUMNS)}",
        )

    urls = (
        df[url_col]
        .dropna()
        .astype(str)
        .str.strip()
        .pipe(lambda s: s[s.str.startswith("http") & s.str.contains("linkedin.com/company", na=False)])
        .tolist()
    )

    if not urls:
        raise HTTPException(status_code=400, detail="No valid LinkedIn company URLs found in file. URLs must start with https:// and contain linkedin.com/company")
    if len(urls) > 100:
        raise HTTPException(status_code=400, detail="Maximum 100 URLs per upload")

    seniority = seniority_levels if seniority_levels else None

    result = await scrape_companies_and_employees(
        company_urls=urls,
        max_employees_per_company=max(1, min(500, max_employees_per_company)),
        seniority_levels=seniority,
        scraper_mode=scraper_mode,
    )
    result["source_file"] = filename
    return result


# ---------------------------------------------------------------------------
# Shared helper: delete one company + its employees (prospects kept)
# ---------------------------------------------------------------------------
async def _delete_company_and_employees(company_id_str: str) -> tuple[bool, int]:
    """Delete a company doc and its employees. Returns (found, employees_deleted)."""
    try:
        oid = ObjectId(company_id_str)
    except Exception:
        return False, 0
    company = await companies_collection.find_one({"_id": oid}, {"_id": 1})
    if not company:
        return False, 0
    del_result = await employees_collection.delete_many(
        {"company_id": {"$in": [oid, company_id_str]}}
    )
    await companies_collection.delete_one({"_id": oid})
    return True, del_result.deleted_count


# ---------------------------------------------------------------------------
# POST /api/companies/bulk-delete  (must be before /{company_id})
# ---------------------------------------------------------------------------
@router.post("/bulk-delete")
async def bulk_delete_companies(
    body: BulkCompanyIdsRequest,
    _admin: dict = Depends(get_super_admin),
):
    """Delete multiple companies and their scraped employees. Prospects are retained."""
    deleted = []
    not_found = []
    total_employees_deleted = 0
    for cid in body.company_ids:
        found, emp_count = await _delete_company_and_employees(cid)
        if found:
            deleted.append(cid)
            total_employees_deleted += emp_count
        else:
            not_found.append(cid)
    return {"deleted": deleted, "not_found": not_found, "employees_deleted": total_employees_deleted}


# ---------------------------------------------------------------------------
# Shared helper: background rescrape task
# ---------------------------------------------------------------------------
async def _run_rescrape(company_id_str: str, linkedin_url: str):
    try:
        oid = ObjectId(company_id_str)
    except Exception:
        return
    try:
        result = await scrape_companies_and_employees([linkedin_url])
        await companies_collection.update_one(
            {"_id": oid},
            {
                "$set": {
                    "enrichment_status": "completed",
                    "enrichment_completed_at": datetime.utcnow(),
                    "last_enrichment_result": result,
                }
            },
        )
    except Exception as e:
        logger.error(f"Bulk rescrape failed for {company_id_str}: {e}")
        await companies_collection.update_one(
            {"_id": oid},
            {"$set": {"enrichment_status": "failed", "enrichment_error": str(e)}},
        )


# ---------------------------------------------------------------------------
# POST /api/companies/bulk-rescrape  (must be before /{company_id})
# ---------------------------------------------------------------------------
@router.post("/bulk-rescrape")
async def bulk_rescrape_companies(
    body: BulkCompanyIdsRequest,
    background_tasks: BackgroundTasks,
    _admin: dict = Depends(get_super_admin),
):
    """Re-trigger employee scraping for multiple companies as background tasks."""
    queued = []
    not_found = []
    for cid in body.company_ids:
        try:
            oid = ObjectId(cid)
        except Exception:
            not_found.append(cid)
            continue
        company = await companies_collection.find_one({"_id": oid}, {"linkedin_url": 1})
        if not company or not company.get("linkedin_url"):
            not_found.append(cid)
            continue
        await companies_collection.update_one(
            {"_id": oid},
            {"$set": {"enrichment_status": "in_progress", "enrichment_started_at": datetime.utcnow()}},
        )
        background_tasks.add_task(_run_rescrape, cid, company["linkedin_url"])
        queued.append(cid)
    return {"queued": queued, "not_found": not_found}


# ---------------------------------------------------------------------------
# GET /api/companies/{company_id}
# ---------------------------------------------------------------------------
@router.get("/{company_id}")
async def get_company(
    company_id: str,
    ctx: dict = Depends(get_account_context),
):
    """Return a company with embedded top employees and top prospects."""
    try:
        oid = ObjectId(company_id)
    except Exception:
        raise HTTPException(status_code=400, detail="Invalid company_id")

    company = await companies_collection.find_one({"_id": oid})
    if company is None:
        raise HTTPException(status_code=404, detail="Company not found")
    company = _str_id(company)

    account_id_str = str(ctx["account"]["_id"])

    # Top 10 employees (match both ObjectId and string forms of company_id)
    emp_cursor = employees_collection.find(
        {"company_id": {"$in": [oid, company_id]}},
        {
            "_id": 1,
            "full_name": 1,
            "current_position.title": 1,
            "enriched": 1,
            "prospect_id": 1,
        },
    ).limit(10)
    employees = []
    async for emp in emp_cursor:
        e = _str_id(emp)
        employees.append(e)

    # Top 5 prospects. Canonical linkage: prospects.company_id == str(company._id)
    # (prospects are a SHARED pool with no account_id field; the old
    # account_id + company_linkedin query always returned zero rows).
    # Tenant-relative score/status/tier live in prospect_state — overlay them.
    prospect_docs = await prospects_collection.find(
        {"company_id": company_id},
        {
            "_id": 1,
            "full_name": 1,
            "job_title": 1,
            "email": 1,
            "linkedin": 1,
            "enrichment_status": 1,
            "prospect_score": 1,
        },
    ).limit(100).to_list(length=100)

    top_prospects = []
    if prospect_docs:
        pids = [str(p["_id"]) for p in prospect_docs]
        state_by_pid: dict = {}
        async for st in prospect_state_collection.find(
            {"account_id": account_id_str, "prospect_id": {"$in": pids}},
            {"prospect_id": 1, "ai_score": 1, "priority_tier": 1, "status": 1,
             "enrichment_status": 1},
        ):
            state_by_pid[str(st["prospect_id"])] = st

        for p in prospect_docs:
            _str_id(p)
            st = state_by_pid.get(p["_id"], {})
            ai_score = st.get("ai_score")
            # Tenant state score wins; legacy shared-pool base score is a
            # sort fallback only.
            p["score"] = ai_score if ai_score is not None else p.get("prospect_score")
            p["status"] = st.get("status")
            p["priority_tier"] = st.get("priority_tier")
            if st.get("enrichment_status"):
                p["enrichment_status"] = st["enrichment_status"]
            p.pop("prospect_score", None)

        # Highest score first; unscored (None) sort last.
        prospect_docs.sort(
            key=lambda p: (p.get("score") is not None, p.get("score") or 0),
            reverse=True,
        )
        top_prospects = prospect_docs[:5]

    return {"company": company, "employees": employees, "top_prospects": top_prospects}


# ---------------------------------------------------------------------------
# GET /api/companies/{company_id}/prospects
# ---------------------------------------------------------------------------
@router.get("/{company_id}/prospects")
async def get_company_prospects(
    company_id: str,
    page: int = Query(1, ge=1),
    page_size: int = Query(50, ge=1, le=200),
    status: Optional[str] = Query(None),
    enrichment_status: Optional[str] = Query(None),
    ctx: dict = Depends(get_account_context),
):
    """Paginated prospect list for a company.

    Canonical linkage: prospects.company_id == str(company._id) — the prospects
    collection is a SHARED pool with no account_id field. Tenant-relative
    score/status/tier come from the prospect_state overlay (account_id +
    prospect_id, string-keyed), which also drives the status filter and the
    score sort.
    """
    try:
        oid = ObjectId(company_id)
    except Exception:
        raise HTTPException(status_code=400, detail="Invalid company_id")

    company = await companies_collection.find_one({"_id": oid}, {"linkedin_url": 1})
    if company is None:
        raise HTTPException(status_code=404, detail="Company not found")

    account_id_str = str(ctx["account"]["_id"])

    # Canonical match is the string form; keep the ObjectId form for any
    # legacy docs written before the string-key migration.
    query: dict = {"company_id": {"$in": [company_id, oid]}}
    if enrichment_status:
        # enrichment_status is canonical on the shared prospect doc.
        query["enrichment_status"] = enrichment_status

    projection = {
        "_id": 1, "full_name": 1, "first_name": 1, "last_name": 1,
        "job_title": 1, "email": 1, "linkedin": 1, "company_name": 1,
        "company_id": 1, "seniority_level": 1, "industry": 1,
        "country": 1, "city": 1, "location": 1,
        "enrichment_status": 1, "enrichment_completed_at": 1,
    }
    # Companies have a bounded prospect set; fetch them all so we can overlay
    # tenant state, filter by state status, and sort by state score correctly.
    docs = await prospects_collection.find(query, projection).limit(2000).to_list(length=2000)

    state_by_pid: dict = {}
    if docs:
        pids = [str(d["_id"]) for d in docs]
        async for st in prospect_state_collection.find(
            {"account_id": account_id_str, "prospect_id": {"$in": pids}},
            {"prospect_id": 1, "ai_score": 1, "priority_tier": 1, "status": 1,
             "enrichment_status": 1},
        ):
            state_by_pid[str(st["prospect_id"])] = st

    rows = []
    for d in docs:
        _str_id(d)
        st = state_by_pid.get(d["_id"], {})
        d["score"] = st.get("ai_score")
        d["status"] = st.get("status", "new")
        d["priority_tier"] = st.get("priority_tier")
        if st.get("enrichment_status"):
            d["enrichment_status"] = st["enrichment_status"]
        if status and d["status"] != status:
            continue
        rows.append(d)

    # Highest tenant score first; unscored (None) sort last.
    rows.sort(
        key=lambda p: (p.get("score") is not None, p.get("score") or 0),
        reverse=True,
    )

    total = len(rows)
    skip = (page - 1) * page_size
    prospects = rows[skip:skip + page_size]

    return {"prospects": prospects, "total": total, "page": page, "page_size": page_size}


# ---------------------------------------------------------------------------
# GET /api/companies/{company_id}/employees
# ---------------------------------------------------------------------------
@router.get("/{company_id}/employees")
async def get_company_employees(
    company_id: str,
    page: int = Query(1, ge=1),
    page_size: int = Query(50, ge=1, le=200),
    enriched_only: bool = Query(False),
    sort_by: str = "name",
    sort_order: str = "desc",
    ctx: dict = Depends(get_account_context),
):
    """Paginated employee list for a company with optional sort and prospect info."""
    try:
        oid = ObjectId(company_id)
    except Exception:
        raise HTTPException(status_code=400, detail="Invalid company_id")

    company = await companies_collection.find_one({"_id": oid}, {"_id": 1})
    if company is None:
        raise HTTPException(status_code=404, detail="Company not found")

    # Match both ObjectId and string forms — older scraped employees may be stored
    # with company_id as a plain string rather than ObjectId.
    query: dict = {"company_id": {"$in": [oid, company_id]}}
    if enriched_only:
        query["enriched"] = True

    sort_direction = -1 if sort_order == "desc" else 1
    allowed_sorts = {"name", "title", "location", "enriched"}
    sort_field = sort_by if sort_by in allowed_sorts else "name"

    skip = (page - 1) * page_size
    total = await employees_collection.count_documents(query)
    cursor = employees_collection.find(query).sort(sort_field, sort_direction).skip(skip).limit(page_size)

    result_employees = []
    async for emp in cursor:
        emp = _str_id(emp)
        # Normalize enrichment_data: prospect_id may be stored as ObjectId
        if isinstance(emp.get("enrichment_data"), dict):
            ed = emp["enrichment_data"]
            if isinstance(ed.get("prospect_id"), ObjectId):
                ed["prospect_id"] = str(ed["prospect_id"])

        # Resolve prospect_id from either top-level or enrichment_data
        prospect_id_raw = emp.get("prospect_id") or (
            emp.get("enrichment_data") or {}
        ).get("prospect_id")

        prospect_info = None
        if prospect_id_raw:
            try:
                prospect_oid = ObjectId(str(prospect_id_raw))
                p = await prospects_collection.find_one(
                    {"_id": prospect_oid},
                    {"_id": 1, "status": 1, "enhanced_score": 1, "email": 1, "enrichment_status": 1},
                )
                if p:
                    prospect_info = _str_id(p)
            except Exception:
                pass
        emp["prospect"] = prospect_info
        # Ensure a unified prospect_id field on the employee response
        if prospect_id_raw and not emp.get("prospect_id"):
            emp["prospect_id"] = str(prospect_id_raw)
        result_employees.append(emp)

    return {"employees": result_employees, "total": total, "page": page, "page_size": page_size}


# ---------------------------------------------------------------------------
# GET /api/companies/{company_id}/employees/{employee_id}
# ---------------------------------------------------------------------------
@router.get("/{company_id}/employees/{employee_id}")
async def get_company_employee(
    company_id: str,
    employee_id: str,
    ctx: dict = Depends(get_account_context),
):
    """Return a single employee belonging to a company, with attached prospect info."""
    try:
        company_oid = ObjectId(company_id)
        emp_oid = ObjectId(employee_id)
    except Exception:
        raise HTTPException(status_code=400, detail="Invalid id")

    company = await companies_collection.find_one({"_id": company_oid})
    if not company:
        raise HTTPException(status_code=404, detail="Company not found")
    company = _str_id(company)

    emp = await employees_collection.find_one(
        {"_id": emp_oid, "company_id": {"$in": [company_oid, company_id]}}
    )
    if not emp:
        raise HTTPException(status_code=404, detail="Employee not found")

    emp = _str_id(emp)
    if isinstance(emp.get("enrichment_data"), dict):
        ed = emp["enrichment_data"]
        if isinstance(ed.get("prospect_id"), ObjectId):
            ed["prospect_id"] = str(ed["prospect_id"])

    prospect_id_raw = emp.get("prospect_id") or (emp.get("enrichment_data") or {}).get("prospect_id")
    prospect_info = None
    if prospect_id_raw:
        try:
            p = await prospects_collection.find_one(
                {"_id": ObjectId(str(prospect_id_raw))},
                {"_id": 1, "status": 1, "enhanced_score": 1, "email": 1, "enrichment_status": 1},
            )
            if p:
                prospect_info = _str_id(p)
        except Exception:
            pass
    emp["prospect"] = prospect_info
    if prospect_id_raw and not emp.get("prospect_id"):
        emp["prospect_id"] = str(prospect_id_raw)

    return {"employee": emp, "company": company}


# ---------------------------------------------------------------------------
# POST /api/companies/{company_id}/employees/promote-to-prospects
# ---------------------------------------------------------------------------

class PromoteEmployeesRequest(BaseModel):
    employee_ids: List[str]


@router.post("/{company_id}/employees/promote-to-prospects")
async def promote_employees_to_prospects(
    company_id: str,
    body: PromoteEmployeesRequest,
    _admin: dict = Depends(get_super_admin),
):
    """
    Convert selected scraped employees into prospect documents so they can
    be enrolled in campaigns. Idempotent — employees already promoted are
    returned as-is without creating duplicates.
    """
    try:
        company_oid = ObjectId(company_id)
    except Exception:
        raise HTTPException(status_code=400, detail="Invalid company_id")

    company = await companies_collection.find_one({"_id": company_oid})
    if not company:
        raise HTTPException(status_code=404, detail="Company not found")

    employee_oids = []
    for eid in body.employee_ids:
        try:
            employee_oids.append(ObjectId(eid))
        except Exception:
            pass

    if not employee_oids:
        raise HTTPException(status_code=400, detail="No valid employee IDs provided")

    employees = await employees_collection.find(
        {"_id": {"$in": employee_oids}, "company_id": {"$in": [company_oid, company_id]}}
    ).to_list(len(employee_oids))

    if not employees:
        raise HTTPException(status_code=404, detail="No matching employees found for this company")

    now = datetime.utcnow()
    prospect_ids = []
    promoted = 0
    already_existed = 0

    for emp in employees:
        # Already promoted → reuse existing prospect
        if emp.get("prospect_id"):
            prospect_ids.append(str(emp["prospect_id"]))
            already_existed += 1
            continue

        pos = emp.get("current_position") or {}
        new_prospect = {
            "full_name": emp.get("full_name"),
            "first_name": emp.get("first_name"),
            "last_name": emp.get("last_name"),
            "linkedin": emp.get("linkedin_url"),
            "email": None,
            "job_title": pos.get("title"),
            "company_name": emp.get("company_name") or company.get("name"),
            "company_linkedin": company.get("linkedin_url"),
            "company_domain": company.get("domain"),
            "company_website": company.get("website"),
            "industry": company.get("industry"),
            "company_size": company.get("employee_count"),
            "enrichment_status": "not_started",
            "source": "employee_scrape",
            "ai_prospect_score": None,
            "enhanced_score": None,
            "created_at": now,
            "last_updated_at": now,
        }
        result = await prospects_collection.insert_one(new_prospect)
        new_id = result.inserted_id
        prospect_ids.append(str(new_id))
        promoted += 1

        # Back-link the employee to this prospect
        await employees_collection.update_one(
            {"_id": emp["_id"]},
            {"$set": {"prospect_id": new_id, "enriched": True}},
        )

    return {"prospect_ids": prospect_ids, "promoted": promoted, "already_existed": already_existed}


# ---------------------------------------------------------------------------
# POST /api/companies/{company_id}/scrape-employees
# GET  /api/companies/{company_id}/scrape-employees/status
# ---------------------------------------------------------------------------

class ScrapeEmployeesRequest(BaseModel):
    """Knobs for a single-company employee top-up scrape."""
    max_employees: int = Field(50, ge=1, le=200)
    seniority_levels: Optional[List[str]] = None


async def _run_employee_scrape(
    company_id_str: str,
    linkedin_url: str,
    max_employees: int,
    seniority_levels: Optional[List[str]],
):
    """Background worker for /scrape-employees. Records the before/after
    employee count so the UI can report what the run actually added."""
    try:
        oid = ObjectId(company_id_str)
    except Exception:
        return

    before = await employees_collection.count_documents(
        {"company_id": {"$in": [oid, company_id_str]}}
    )
    try:
        result = await scrape_companies_and_employees(
            company_urls=[linkedin_url],
            max_employees_per_company=max_employees,
            seniority_levels=seniority_levels,
        )
        after = await employees_collection.count_documents(
            {"company_id": {"$in": [oid, company_id_str]}}
        )
        await companies_collection.update_one(
            {"_id": oid},
            {"$set": {
                "employee_scrape_status": "completed",
                "employee_scrape_completed_at": datetime.utcnow(),
                "employee_scrape_result": {
                    "employees_before": before,
                    "employees_after": after,
                    "employees_added": max(0, after - before),
                    "scraper_stats": result,
                },
                "employee_scrape_error": None,
            }},
        )
    except Exception as e:
        logger.error(f"Employee scrape failed for company {company_id_str}: {e}")
        await companies_collection.update_one(
            {"_id": oid},
            {"$set": {
                "employee_scrape_status": "failed",
                "employee_scrape_error": str(e),
                "employee_scrape_completed_at": datetime.utcnow(),
            }},
        )


@router.post("/{company_id}/scrape-employees")
async def scrape_more_employees(
    company_id: str,
    body: ScrapeEmployeesRequest,
    background_tasks: BackgroundTasks,
    ctx: dict = Depends(get_account_context),
):
    """Scrape more employees for a single company.

    This is the "find more people at this company" action on the company
    detail page. It runs in the background because the Apify actor takes
    tens of seconds; poll the companion status endpoint for progress.

    Concurrent runs for the same company are rejected rather than queued —
    each run costs Apify credits and a double-click should not double-charge.
    """
    try:
        oid = ObjectId(company_id)
    except Exception:
        raise HTTPException(status_code=400, detail="Invalid company_id")

    company = await companies_collection.find_one(
        {"_id": oid},
        {"linkedin_url": 1, "name": 1, "employee_scrape_status": 1},
    )
    if not company:
        raise HTTPException(status_code=404, detail="Company not found")

    linkedin_url = company.get("linkedin_url")
    if not linkedin_url:
        raise HTTPException(
            status_code=400,
            detail="This company has no LinkedIn URL, so its employees can't be scraped.",
        )

    if company.get("employee_scrape_status") == "in_progress":
        raise HTTPException(
            status_code=409,
            detail="An employee scrape is already running for this company.",
        )

    seniority_levels = body.seniority_levels or None
    if seniority_levels:
        bad = [s for s in seniority_levels if s not in _VALID_SENIORITIES]
        if bad:
            raise HTTPException(status_code=400, detail=f"Invalid seniority levels: {bad}")

    await companies_collection.update_one(
        {"_id": oid},
        {"$set": {
            "employee_scrape_status": "in_progress",
            "employee_scrape_started_at": datetime.utcnow(),
            "employee_scrape_started_by": str(ctx["account"]["_id"]),
            "employee_scrape_error": None,
        }},
    )

    background_tasks.add_task(
        _run_employee_scrape, company_id, linkedin_url, body.max_employees, seniority_levels
    )
    return {
        "status": "started",
        "company_id": company_id,
        "company_name": company.get("name"),
        "max_employees": body.max_employees,
    }


@router.get("/{company_id}/scrape-employees/status")
async def get_employee_scrape_status(
    company_id: str,
    ctx: dict = Depends(get_account_context),
):
    """Current state of the last employee scrape for a company."""
    try:
        oid = ObjectId(company_id)
    except Exception:
        raise HTTPException(status_code=400, detail="Invalid company_id")

    company = await companies_collection.find_one(
        {"_id": oid},
        {
            "employee_scrape_status": 1,
            "employee_scrape_started_at": 1,
            "employee_scrape_completed_at": 1,
            "employee_scrape_result": 1,
            "employee_scrape_error": 1,
        },
    )
    if not company:
        raise HTTPException(status_code=404, detail="Company not found")

    result = company.get("employee_scrape_result") or {}
    return {
        "status": company.get("employee_scrape_status") or "idle",
        "started_at": company.get("employee_scrape_started_at"),
        "completed_at": company.get("employee_scrape_completed_at"),
        "employees_added": result.get("employees_added"),
        "error": company.get("employee_scrape_error"),
    }


# ---------------------------------------------------------------------------
# POST /api/companies/{company_id}/enrich
# ---------------------------------------------------------------------------
@router.post("/{company_id}/enrich")
async def enrich_company(
    company_id: str,
    background_tasks: BackgroundTasks,
    _admin: dict = Depends(get_super_admin),
):
    """Trigger background employee scraping for a company."""
    try:
        oid = ObjectId(company_id)
    except Exception:
        raise HTTPException(status_code=400, detail="Invalid company_id")

    company = await companies_collection.find_one({"_id": oid}, {"linkedin_url": 1, "name": 1})
    if not company:
        raise HTTPException(status_code=404, detail="Company not found")

    linkedin_url = company.get("linkedin_url")
    if not linkedin_url:
        raise HTTPException(status_code=400, detail="Company has no LinkedIn URL")

    # Mark enrichment in progress
    await companies_collection.update_one(
        {"_id": oid},
        {"$set": {"enrichment_status": "in_progress", "enrichment_started_at": datetime.utcnow()}},
    )

    async def run_enrichment():
        try:
            result = await scrape_companies_and_employees([linkedin_url])
            await companies_collection.update_one(
                {"_id": oid},
                {
                    "$set": {
                        "enrichment_status": "completed",
                        "enrichment_completed_at": datetime.utcnow(),
                        "last_enrichment_result": result,
                    }
                },
            )
        except Exception as e:
            logger.error(f"Company enrichment failed for {company_id}: {e}")
            await companies_collection.update_one(
                {"_id": oid},
                {"$set": {"enrichment_status": "failed", "enrichment_error": str(e)}},
            )

    background_tasks.add_task(run_enrichment)
    return {"message": "Enrichment started", "company_id": company_id, "company_name": company.get("name")}


# ---------------------------------------------------------------------------
# POST /api/companies/from-linkedin
# ---------------------------------------------------------------------------

class FromLinkedinRequest(BaseModel):
    linkedin_url: str


@router.post("/from-linkedin")
async def add_company_from_linkedin(
    body: FromLinkedinRequest,
    background_tasks: BackgroundTasks,
    _admin: dict = Depends(get_super_admin),
):
    """Add a company (and discover its employees) from a LinkedIn URL."""
    linkedin_url = body.linkedin_url.strip().rstrip("/")

    # Normalise: ensure https://
    if not linkedin_url.startswith("http"):
        linkedin_url = "https://" + linkedin_url

    # Check if company already exists
    existing = await companies_collection.find_one({"linkedin_url": linkedin_url})
    if existing:
        return {
            "company_id": str(existing["_id"]),
            "company_name": existing.get("name", "Company"),
            "already_exists": True,
        }

    # Create a placeholder company document so we can return an ID immediately
    now = datetime.utcnow()
    new_doc = {
        "linkedin_url": linkedin_url,
        "name": linkedin_url.split("/company/")[-1].replace("-", " ").title(),
        "enrichment_status": "in_progress",
        "enrichment_started_at": now,
        "created_at": now,
        "last_updated_at": now,
        "enriched_prospect_count": 0,
    }
    result = await companies_collection.insert_one(new_doc)
    company_id = str(result.inserted_id)

    async def run_discovery():
        try:
            await scrape_companies_and_employees([linkedin_url])
            # Find the now-populated company record and mark complete
            company = await companies_collection.find_one({"linkedin_url": linkedin_url})
            if company:
                await companies_collection.update_one(
                    {"_id": company["_id"]},
                    {"$set": {"enrichment_status": "completed", "enrichment_completed_at": datetime.utcnow()}},
                )
        except Exception as e:
            logger.error(f"Company discovery failed for {linkedin_url}: {e}")
            await companies_collection.update_one(
                {"_id": result.inserted_id},
                {"$set": {"enrichment_status": "failed", "enrichment_error": str(e)}},
            )

    background_tasks.add_task(run_discovery)

    return {
        "company_id": company_id,
        "company_name": new_doc["name"],
        "already_exists": False,
    }


# ---------------------------------------------------------------------------
# DELETE /api/companies/{company_id}
# ---------------------------------------------------------------------------
@router.delete("/{company_id}")
async def delete_company(
    company_id: str,
    _admin: dict = Depends(get_super_admin),
):
    """Delete a company and its scraped employees. Enriched prospects are retained."""
    found, employees_deleted = await _delete_company_and_employees(company_id)
    if not found:
        raise HTTPException(status_code=404, detail="Company not found")
    return {"deleted": True, "company_id": company_id, "employees_deleted": employees_deleted}
