"""
Employee scraper service using Apify LinkedIn scrapers.
For each company URL:
1. Scrapes company information using Apify Company Scraper
2. Saves/updates company in MongoDB
3. Scrapes employees using Apify LinkedIn People Scraper
4. Links employees to the company
"""

import asyncio
import threading

from apify_client import ApifyClient
from config import get_settings
from database import employees_collection, companies_collection, prospects_collection
from datetime import datetime
import logging
from typing import Optional
from pymongo.errors import DuplicateKeyError
from bson import ObjectId

logger = logging.getLogger(__name__)

settings = get_settings()


class _LazyApifyClient:
    """Construct the synchronous Apify client only on first dataset access."""

    def __init__(self, api_key: str):
        self._api_key = api_key
        self._client = None
        self._lock = threading.Lock()

    def _get_client(self):
        if self._client is None:
            with self._lock:
                if self._client is None:
                    self._client = ApifyClient(self._api_key)
        return self._client

    def dataset(self, dataset_id: str):
        return self._get_client().dataset(dataset_id)


apify_client = _LazyApifyClient(settings.apify_api_key)

# Apify Actor IDs
COMPANY_SCRAPER_ACTOR_ID = settings.apify_company_scraper_id
EMPLOYEE_SCRAPER_ACTOR_ID = "Vb6LZkh4EqRlR0Ka9"


async def scrape_companies_and_employees(
    company_urls: list[str],
    max_employees_per_company: int = 50,
    seniority_levels: Optional[list[str]] = None,
    scraper_mode: str = "Short ($4 per 1k)"
) -> dict:
    """
    Main workflow:
    1. Scrape company information for all URLs
    2. Save companies to DB
    3. For each company, scrape employees
    4. Link employees to companies

    Returns summary with statistics.
    """
    logger.info(f"Starting scrape workflow for {len(company_urls)} companies")

    if seniority_levels is None:
        # Default to senior roles: VP, Director, C-Suite, Owner, Partner, Manager
        seniority_levels = ["210", "220", "300", "310", "320", "120"]

    stats = {
        "total_urls": len(company_urls),
        "companies_scraped": 0,
        "companies_new": 0,
        "companies_updated": 0,
        "employees_scraped": 0,
        "employees_new": 0,
        "employees_duplicate": 0,
    }

    # ──────────────────────────────────────────────────────────────
    # Phase 1: Scrape Company Information
    # ──────────────────────────────────────────────────────────────
    logger.info("Phase 1: Scraping company information")

    company_data = await _scrape_companies(company_urls)
    stats["companies_scraped"] = len(company_data)

    # Save companies to DB and get their IDs
    company_id_map = {}   # url -> company_id
    company_doc_map = {}  # url -> full company doc (for denorm)

    for company in company_data:
        result = await _save_company(company)
        if result:
            company_id, linkedin_url, company_doc = result
            company_id_map[linkedin_url] = company_id
            company_doc_map[linkedin_url] = company_doc

            if company_doc and company_doc.get("last_scraped_at") and company_doc.get("created_at") != company_doc.get("last_scraped_at"):
                stats["companies_updated"] += 1
            else:
                stats["companies_new"] += 1

    logger.info(f"Saved {len(company_id_map)} companies to database")

    # ──────────────────────────────────────────────────────────────
    # Phase 2: Scrape Employees for Each Company
    # ──────────────────────────────────────────────────────────────
    logger.info("Phase 2: Scraping employees for each company")

    for company_url, company_id in company_id_map.items():
        logger.info(f"Scraping employees for: {company_url}")

        employee_data = await _scrape_employees_for_company(
            company_url=company_url,
            max_items=max_employees_per_company,
            seniority_levels=seniority_levels,
            scraper_mode=scraper_mode
        )

        logger.info(f"Found {len(employee_data)} employees for {company_url}")
        stats["employees_scraped"] += len(employee_data)

        # Save employees as prospects and link to company
        for emp_data in employee_data:
            prospect_doc = transform_apify_employee(emp_data, company_id, company_doc_map.get(company_url))
            saved = await _save_employee(prospect_doc)

            if saved:
                stats["employees_new"] += 1
            else:
                stats["employees_duplicate"] += 1

    logger.info("Scrape workflow completed")
    logger.info(f"Summary: {stats}")

    return stats


async def _scrape_companies(company_urls: list[str]) -> list[dict]:
    """
    Scrape company information using Apify Company Scraper.
    Returns list of company data dictionaries.
    """
    if not company_urls:
        return []

    logger.info(f"Calling Apify Company Scraper for {len(company_urls)} URLs")

    run_input = {"companies": company_urls}

    # Retried, semaphore-bounded call to Apify (see services/apify_service.py)
    from services.apify_service import call_actor_with_retry
    run = await call_actor_with_retry(COMPANY_SCRAPER_ACTOR_ID, run_input)
    apify_run_id = run.get("id", "unknown")

    logger.info(f"Company scrape completed: {apify_run_id}")

    # Fetch results
    companies = []
    for item in apify_client.dataset(run["defaultDatasetId"]).iterate_items():
        companies.append(item)

    logger.info(f"Fetched {len(companies)} company results from Apify")
    return companies


async def _scrape_employees_for_company(
    company_url: str,
    max_items: int = 50,
    seniority_levels: list[str] | None = None,
    scraper_mode: str = "Short ($4 per 1k)"
) -> list[dict]:
    """
    Scrape employees for a single company using Apify People Scraper.
    Returns list of employee data dictionaries.
    """
    if seniority_levels is None:
        seniority_levels = ["210", "220", "300", "310", "320", "120"]

    logger.info(f"Calling Apify People Scraper for: {company_url}")

    run_input = {
        "companies": [company_url],
        "maxItems": max_items,
        "profileScraperMode": scraper_mode,
        "seniorityLevelIds": seniority_levels,
        "companyBatchMode": "all_at_once",
        "recentlyChangedJobs": False,
    }

    # Retried, semaphore-bounded call to Apify (see services/apify_service.py)
    from services.apify_service import call_actor_with_retry
    run = await call_actor_with_retry(EMPLOYEE_SCRAPER_ACTOR_ID, run_input)
    apify_run_id = run.get("id", "unknown")

    logger.info(f"Employee scrape completed: {apify_run_id}")

    # Fetch results
    employees = []
    for item in apify_client.dataset(run["defaultDatasetId"]).iterate_items():
        employees.append(item)

    return employees


async def _save_company(apify_data: dict) -> Optional[tuple[str, str, dict]]:
    """
    Save or update company using new shared schema with canonical industry + geo.
    Returns (company_id, linkedin_url, company_doc) or None.
    """
    from database import geo_places_collection, industries_taxonomy_collection
    from services.geo_resolver import resolve as geo_resolve
    from services.industry_canonicalizer import resolve as industry_resolve
    from services.embedding_service import embed_one, build_company_profile_text

    linkedin_url = _normalize_url(
        apify_data.get("linkedinUrl") or
        apify_data.get("url") or
        apify_data.get("companyUrl") or
        apify_data.get("linkedInUrl")
    )
    if not linkedin_url:
        logger.warning("No LinkedIn URL found in company data")
        return None

    raw_industry = apify_data.get("industry")
    raw_hq = apify_data.get("headquarters") or apify_data.get("hq")
    try:
        employee_count = int(apify_data.get("employeeCount") or apify_data.get("companySize") or 0) or None
    except (TypeError, ValueError):
        employee_count = None

    # Resolve industry + location in parallel
    industry_result, location_result = await asyncio.gather(
        industry_resolve(raw_industry, collection=industries_taxonomy_collection),
        geo_resolve(raw_hq, collection=geo_places_collection),
    )

    industry_doc = None
    if industry_result:
        industry_doc = {
            "id": industry_result.get("id"),
            "label": industry_result.get("label"),
            "group": industry_result.get("group"),
            "raw": raw_industry,
            "assign_method": industry_result.get("assign_method"),
            "confidence": industry_result.get("confidence"),
        }
    elif raw_industry:
        industry_doc = {"raw": raw_industry}

    location_doc = None
    geo_doc = None
    if location_result:
        location_doc = {
            "place_id": location_result.get("place_id"),
            "city": location_result.get("city"),
            "region": location_result.get("region"),
            "country": location_result.get("country"),
            "country_code": location_result.get("country_code"),
            "continent": location_result.get("continent"),
            "raw": raw_hq,
        }
        # coordinates come straight from the SQLite gazetteer result
        geo_doc = location_result.get("geo")
    elif raw_hq:
        location_doc = {"raw": raw_hq}

    specialties_raw = apify_data.get("specialties")
    if isinstance(specialties_raw, str):
        specialties = [s.strip() for s in specialties_raw.split(",") if s.strip()] or None
    else:
        specialties = specialties_raw or None

    now = datetime.utcnow()
    set_doc: dict = {
        "linkedin_url": linkedin_url,
        "linkedin_uid": apify_data.get("companyId") or apify_data.get("id"),
        "name": apify_data.get("name") or apify_data.get("companyName"),
        "website": apify_data.get("website") or apify_data.get("websiteUrl"),
        "domain": apify_data.get("domain"),
        "description": apify_data.get("description") or apify_data.get("about"),
        "tagline": apify_data.get("tagline"),
        "specialties": specialties,
        "employee_count": employee_count,
        "employee_band": _employee_band(employee_count),
        "follower_count": apify_data.get("followerCount"),
        "founded_year": apify_data.get("foundedYear") or apify_data.get("yearFounded"),
        "company_type": apify_data.get("companyType") or apify_data.get("type"),
        "phone": apify_data.get("phone"),
        "industry": industry_doc,
        "location": location_doc,
        "geo": geo_doc,
        "linkedin_data": apify_data,
        "last_updated_at": now,
        "last_scraped_at": now,
    }
    set_doc = {k: v for k, v in set_doc.items() if v is not None}
    set_doc["last_updated_at"] = now
    set_doc["last_scraped_at"] = now

    try:
        result = await companies_collection.update_one(
            {"linkedin_url": linkedin_url},
            {"$set": set_doc, "$setOnInsert": {"created_at": now}},
            upsert=True,
        )
        company_id = str(result.upserted_id) if result.upserted_id else None
        if not company_id:
            doc = await companies_collection.find_one({"linkedin_url": linkedin_url}, {"_id": 1})
            company_id = str(doc["_id"])

        # Best-effort profile embedding
        try:
            profile_text = build_company_profile_text({
                "name": set_doc.get("name"),
                "description": set_doc.get("description"),
                "specialties": specialties,
                "industry": industry_doc,
            })
            vec = await embed_one(profile_text)
            if vec:
                await companies_collection.update_one(
                    {"_id": ObjectId(company_id)},
                    {"$set": {
                        "profile_vec": vec,
                        "profile_vec_text": profile_text,
                        "profile_vec_model": "gemini-embedding-001",
                        "profile_vec_at": now,
                    }},
                )
        except Exception as ve:
            logger.warning(f"profile_vec failed for {linkedin_url}: {ve}")

        # Best-effort website scrape → structured website_data (shared pool,
        # tenant-neutral). Only when a website/domain exists and website_data
        # is missing or stale (>90 days). Failures are recorded, never raised.
        await _maybe_scrape_website_data(
            company_id,
            website=set_doc.get("website"),
            domain=set_doc.get("domain"),
            now=now,
        )

        full_doc = await companies_collection.find_one({"_id": ObjectId(company_id)})
        logger.info(f"Saved company: {set_doc.get('name')} ({company_id})")
        return (company_id, linkedin_url, full_doc or set_doc)

    except Exception as e:
        logger.error(f"Error saving company {linkedin_url}: {e}")
        return None


async def _maybe_scrape_website_data(
    company_id: str,
    *,
    website: str | None,
    domain: str | None,
    now: datetime,
) -> None:
    """
    Populate companies.website_data from the company's website/domain when it is
    missing or stale (>90 days). Tenant-neutral (shared pool) — never touches
    tenant fields. Errors are swallowed and recorded as a failure marker with a
    timestamp so a failed attempt is distinguishable from never-tried.
    """
    target = website or (f"https://{domain}" if domain else None)
    if not target:
        return

    try:
        from services.website_scraper_service import (
            fetch_website_data,
            is_website_data_stale,
            website_data_failure_marker,
        )

        existing = await companies_collection.find_one(
            {"_id": ObjectId(company_id)}, {"website_data": 1}
        )
        current = (existing or {}).get("website_data")
        if not is_website_data_stale(current):
            return

        data = await fetch_website_data(target)
        website_data = data if data is not None else website_data_failure_marker(target)

        await companies_collection.update_one(
            {"_id": ObjectId(company_id)},
            {"$set": {"website_data": website_data, "last_updated_at": now}},
        )
    except Exception as e:
        # Must never fail the enrichment pipeline.
        logger.warning(f"website_data scrape failed for company {company_id}: {e}")
        try:
            await companies_collection.update_one(
                {"_id": ObjectId(company_id)},
                {"$set": {"website_data": {
                    "url": target, "scraped_at": now, "error": f"exception:{type(e).__name__}",
                }}},
            )
        except Exception:
            pass


def transform_apify_employee(apify_data: dict, company_id: str, company_doc: dict | None = None) -> dict:
    """
    Transform Apify employee data to prospect schema.
    Links prospect to company via company_id with denormalized company facts.
    """
    current_positions = apify_data.get("currentPositions") or apify_data.get("currentPosition") or []
    current = current_positions[0] if current_positions else {}

    job_title = current.get("title") or current.get("position") or apify_data.get("headline")
    seniority = _map_seniority_canonical(
        apify_data.get("seniority") or apify_data.get("seniorityLevel"),
        job_title or "",
    )

    # Denormalize company facts
    company_industry_id = None
    company_industry_group = None
    company_employee_band = None
    company_size = None
    company_domain = None
    company_description = None
    company_linkedin_url = None
    company_name = current.get("companyName")
    if company_doc:
        ind = company_doc.get("industry") or {}
        company_industry_id = ind.get("id") if isinstance(ind, dict) else None
        company_industry_group = ind.get("group") if isinstance(ind, dict) else None
        company_employee_band = company_doc.get("employee_band")
        company_size = company_doc.get("employee_count")
        company_domain = company_doc.get("domain")
        company_description = company_doc.get("description")
        company_linkedin_url = company_doc.get("linkedin_url")
        company_name = company_name or company_doc.get("name")

    raw_location = (apify_data.get("location") or {}).get("linkedinText")
    first_name = apify_data.get("firstName") or ""
    last_name = apify_data.get("lastName") or ""

    return {
        "linkedin": _normalize_url(apify_data.get("linkedinUrl")),
        "linkedin_uid": apify_data.get("id"),
        "first_name": first_name or None,
        "last_name": last_name or None,
        "full_name": f"{first_name} {last_name}".strip() or None,
        "job_title": job_title,
        "headline": apify_data.get("headline"),
        "seniority": seniority,
        "company_id": company_id,
        "company_name": company_name,
        "company_linkedin": current.get("companyLinkedinUrl") or company_linkedin_url,
        "company_domain": company_domain,
        "company_industry_id": company_industry_id,
        "company_industry_group": company_industry_group,
        "company_employee_band": company_employee_band,
        "company_size": company_size,
        "company_description": company_description,
        "location": {"raw": raw_location} if raw_location else None,
        "linkedin_profile_data": apify_data,
        "source": "employee_discovery",
        "stage": "scraped",
        "enrichment_status": "not_started",
    }


async def _save_employee(prospect_doc: dict) -> bool:
    """
    Upsert prospect from employee scraper to prospects_collection.
    Returns True if new, False if duplicate/error.
    """
    linkedin = prospect_doc.get("linkedin")
    if not linkedin:
        return False

    now = datetime.utcnow()
    set_data = {k: v for k, v in prospect_doc.items() if v is not None and k != "first_seen_at"}
    set_data["last_updated_at"] = now

    try:
        await prospects_collection.find_one_and_update(
            {"linkedin": linkedin},
            {"$set": set_data, "$setOnInsert": {"first_seen_at": now}},
            upsert=True,
        )
        return True
    except DuplicateKeyError:
        logger.debug(f"Duplicate prospect: {linkedin}")
        return False
    except Exception as e:
        logger.error(f"Error saving prospect from employee: {e}")
        return False


def _normalize_url(url: Optional[str]) -> Optional[str]:
    """Normalize LinkedIn URL for consistency."""
    if not url:
        return None
    url = url.strip().rstrip("/").lower()
    if "?" in url:
        url = url.split("?")[0]
    return url


def _employee_band(count: int | None) -> str | None:
    """Map employee count to canonical band string."""
    if count is None:
        return None
    if count <= 10:
        return "1-10"
    if count <= 50:
        return "11-50"
    if count <= 200:
        return "51-200"
    if count <= 1000:
        return "201-1000"
    return "1000+"


def _map_seniority_canonical(raw: str | None, title: str = "") -> str | None:
    """Map raw seniority string or job title to canonical seniority token."""
    title_lower = title.lower()
    if not raw:
        if any(k in title_lower for k in ("ceo", "cto", "cfo", "coo", "cmo", "chief")):
            return "c_suite"
        if "founder" in title_lower or "co-founder" in title_lower or "owner" in title_lower:
            return "founder"
        if title_lower.startswith("vp ") or " vp " in title_lower or "vice president" in title_lower:
            return "vp"
        if "director" in title_lower or "head of" in title_lower:
            return "director"
        if "manager" in title_lower:
            return "manager"
        if any(k in title_lower for k in ("senior ", "sr.", " sr ")):
            return "senior"
        return None
    s = str(raw).lower()
    if any(k in s for k in ("c-suite", "csuite", "c_suite", "executive")):
        return "c_suite"
    if "founder" in s or "owner" in s:
        return "founder"
    if "vp" in s or "vice president" in s:
        return "vp"
    if "director" in s or "head" in s:
        return "director"
    if "manager" in s:
        return "manager"
    if "senior" in s:
        return "senior"
    if "mid" in s or "intermediate" in s:
        return "mid"
    if "junior" in s or "entry" in s:
        return "junior"
    return None


# ══════════════════════════════════════════════════════════════════
# Query Functions
# ══════════════════════════════════════════════════════════════════


async def get_company_stats() -> dict:
    """Get statistics about scraped companies."""
    total = await companies_collection.count_documents({})

    if total == 0:
        return {
            "total_companies": 0,
            "by_industry": [],
            "avg_employee_count": 0,
        }

    # By industry
    industry_pipeline = [
        {"$match": {"industry": {"$ne": None}}},
        {"$group": {"_id": "$industry", "count": {"$sum": 1}}},
        {"$sort": {"count": -1}},
        {"$limit": 20},
    ]
    by_industry = await companies_collection.aggregate(industry_pipeline).to_list(20)

    # Average employee count
    avg_pipeline = [
        {"$match": {"employee_count": {"$ne": None, "$gt": 0}}},
        {"$group": {"_id": None, "avg": {"$avg": "$employee_count"}}}
    ]
    avg_result = await companies_collection.aggregate(avg_pipeline).to_list(1)
    avg_employee_count = int(avg_result[0]["avg"]) if avg_result else 0

    return {
        "total_companies": total,
        "by_industry": by_industry,
        "avg_employee_count": avg_employee_count,
    }


# ══════════════════════════════════════════════════════════════════
# Curated-mode helpers — used by curated_discovery_service only
# ══════════════════════════════════════════════════════════════════


async def _run_one_employee_scrape(
    company_urls: list[str],
    *,
    max_items_per_company: int,
    max_items: int,
    seniority_level_ids: list[str] | None,
    functional_level_ids: list[str] | None,
    profile_scraper_mode: str,
    account_id: str | None,
    campaign_id: str | None,
    chunk_label: str = "",
    company_headcount_bands: list[str] | None = None,
) -> list[dict]:
    """One Apify actor run over a chunk of companies. Returns raw employee dicts.
    Isolated so a single failed chunk doesn't sink the whole scrape."""
    run_input: dict = {
        "companies": company_urls,
        "companyBatchMode": "one_by_one",
        "maxItemsPerCompany": max_items_per_company,
        "maxItems": max_items,
        "profileScraperMode": profile_scraper_mode,
        "recentlyChangedJobs": False,
    }
    if seniority_level_ids:
        run_input["seniorityLevelIds"] = seniority_level_ids
    if functional_level_ids:
        # Actor input key is `functionIds` — NOT `functionalLevelIds`, which the
        # actor silently ignores (unknown keys are dropped, not rejected).
        run_input["functionIds"] = functional_level_ids
    if company_headcount_bands:
        run_input["companyHeadcount"] = company_headcount_bands

    loop = asyncio.get_event_loop()
    from services.apify_service import call_actor_with_retry
    run = await call_actor_with_retry(EMPLOYEE_SCRAPER_ACTOR_ID, run_input)

    try:
        from services.apify_service import track_apify_run
        async with track_apify_run(
            EMPLOYEE_SCRAPER_ACTOR_ID,
            account_id=account_id,
            campaign_id=campaign_id,
        ) as tracker:
            tracker.set_run_id(run.get("id", ""))
    except Exception as e:
        logger.warning(f"[bulk_scrape{chunk_label}] track_apify_run failed: {e}")

    employees: list[dict] = []
    dataset_id = run.get("defaultDatasetId")
    if dataset_id:
        # Dataset iteration is blocking I/O — run it off the event loop.
        def _drain() -> list[dict]:
            return list(apify_client.dataset(dataset_id).iterate_items())
        employees = await loop.run_in_executor(None, _drain)

    logger.info(f"[bulk_scrape{chunk_label}] {len(employees)} employees from {len(company_urls)} companies")
    return employees


async def bulk_scrape_employees_for_companies(
    company_urls: list[str],
    *,
    max_items_per_company: int = 5,
    max_total_items: int | None = None,
    seniority_level_ids: list[str] | None = None,
    functional_level_ids: list[str] | None = None,
    profile_scraper_mode: str = "Short ($4 per 1k)",
    account_id: str | None = None,
    campaign_id: str | None = None,
    max_concurrency: int = 1,
    company_headcount_bands: list[str] | None = None,
) -> list[dict]:
    """
    Scrape employees from many companies. With max_concurrency > 1 the company list
    is split into ~equal chunks run as CONCURRENT Apify actor runs (the actor itself
    processes companies serially within a run via companyBatchMode=one_by_one, so
    splitting is what actually parallelizes the scrape). Datasets are merged.
    `company_headcount_bands` maps to the actor's `companyHeadcount` param (bands
    A-I; see _icp_size_to_headcount_bands in curated_discovery_service).
    Returns a flat list of raw Apify employee dicts.
    """
    if not company_urls:
        return []

    import math as _math
    _overall_max = (
        min(max_total_items, len(company_urls) * max_items_per_company)
        if max_total_items is not None
        else len(company_urls) * max_items_per_company
    )

    concurrency = max(1, min(max_concurrency, len(company_urls)))
    if concurrency == 1:
        chunks = [company_urls]
    else:
        chunk_size = _math.ceil(len(company_urls) / concurrency)
        chunks = [company_urls[i:i + chunk_size] for i in range(0, len(company_urls), chunk_size)]

    logger.info(
        f"[bulk_scrape] {len(company_urls)} companies → {len(chunks)} parallel run(s), "
        f"maxPerCo={max_items_per_company}, totalMaxItems={_overall_max}, mode={profile_scraper_mode}"
    )

    async def _chunk(idx: int, urls: list[str]) -> list[dict]:
        # Give each chunk its proportional slice of the global item budget.
        chunk_max = min(len(urls) * max_items_per_company, _math.ceil(_overall_max * len(urls) / len(company_urls)))
        try:
            return await _run_one_employee_scrape(
                urls,
                max_items_per_company=max_items_per_company,
                max_items=max(chunk_max, len(urls) * max_items_per_company if chunk_max == 0 else chunk_max),
                seniority_level_ids=seniority_level_ids,
                functional_level_ids=functional_level_ids,
                profile_scraper_mode=profile_scraper_mode,
                account_id=account_id,
                campaign_id=campaign_id,
                chunk_label=f":{idx + 1}/{len(chunks)}" if len(chunks) > 1 else "",
                company_headcount_bands=company_headcount_bands,
            )
        except Exception as e:
            logger.warning(f"[bulk_scrape:{idx + 1}/{len(chunks)}] chunk failed: {e}")
            return []

    results = await asyncio.gather(*[_chunk(i, c) for i, c in enumerate(chunks)])
    employees: list[dict] = [emp for chunk_result in results for emp in chunk_result]

    logger.info(f"[bulk_scrape] merged {len(employees)} employees from {len(company_urls)} companies across {len(chunks)} run(s)")
    return employees


def transform_employee_to_prospect(
    apify_employee: dict,
    sourced_company: dict,
    *,
    source_actor_id: str | None = None,
) -> dict:
    """
    Map raw Apify employee record to new prospect schema.
    `sourced_company` is the SourcedCompany dict from company_sourcing_service.

    Handles both actor response shapes:
    - Short mode (primary, $4/1k): `currentPositions` (plural), title in
      `position`, no email fields — email is filled separately via
      services/email_finder_service.find_emails.
    - Full / Full+email mode (legacy, $8-12/1k): `currentPosition` (singular)
      or `currentPositions`, title in `title`, plus email fields directly on
      the record — kept for backward compatibility only.
    """
    current_positions = (
        apify_employee.get("currentPositions")
        or apify_employee.get("currentPosition")
        or []
    )
    current = current_positions[0] if current_positions else {}

    # Full+email mode only — absent entirely in Short mode, so this naturally
    # falls through to None and the GrowthToolkit email finder fills it later.
    _emails_list = apify_employee.get("emails") or []
    email = (
        (_emails_list[0] if _emails_list else None)
        or apify_employee.get("email")
        or apify_employee.get("workEmail")
        or apify_employee.get("emailAddress")
        or (apify_employee.get("contactInfo") or {}).get("email")
    )

    first_name = apify_employee.get("firstName") or ""
    last_name = apify_employee.get("lastName") or ""
    full_name = f"{first_name} {last_name}".strip() or apify_employee.get("name") or ""

    job_title = current.get("title") or current.get("position") or apify_employee.get("headline")
    seniority = _map_seniority_canonical(
        apify_employee.get("seniority") or apify_employee.get("seniorityLevel"),
        job_title or "",
    )

    loc = apify_employee.get("location") or {}
    city = loc.get("city") or loc.get("linkedinText") or ""
    country_str = loc.get("country") or sourced_company.get("country") or ""
    raw_location = ", ".join(filter(None, [city, country_str])) or None

    # Canonical company denorm — works for both DB company docs (industry as dict)
    # and legacy Gemini dicts (industry as raw string). In the company-first rewrite
    # (Phase 2) sourced_company will always be a canonical DB doc, so these will be
    # populated. For the current Gemini path they remain None (best-effort).
    _ind = sourced_company.get("industry")
    if isinstance(_ind, dict):
        company_industry_id    = _ind.get("id")
        company_industry_group = _ind.get("group")
        industry_label         = _ind.get("label") or _ind.get("raw") or None
    else:
        company_industry_id    = None
        company_industry_group = None
        industry_label         = _ind  # raw string from Gemini

    # country_code: employee location first, then canonical company doc location
    _co_loc = sourced_company.get("location") or {}
    if isinstance(_co_loc, dict):
        country_code = _co_loc.get("country_code") or None
    else:
        country_code = None
    country_code = country_code or loc.get("country_code") or None

    # employee_band: from canonical company doc or computed from size estimate
    company_employee_band = (
        sourced_company.get("employee_band")
        or _employee_band(_curated_parse_size(sourced_company.get("employee_size_estimate")))
    )

    return {
        "first_name": first_name or None,
        "last_name": last_name or None,
        "full_name": full_name or None,
        "email": email,
        "linkedin": _normalize_url(apify_employee.get("linkedinUrl") or apify_employee.get("publicProfileUrl")),
        "job_title": job_title,
        "headline": apify_employee.get("headline"),
        "seniority": seniority,
        "location": ({"raw": raw_location, "country_code": country_code}
                     if (raw_location or country_code) else None),
        "company_name": current.get("companyName") or sourced_company.get("company_name"),
        "company_linkedin": _normalize_url(
            current.get("companyLinkedinUrl") or sourced_company.get("company_linkedin_url")
        ),
        "company_domain": sourced_company.get("company_domain") or sourced_company.get("company_website"),
        "industry": industry_label,
        "company_industry_id":    company_industry_id,
        "company_industry_group": company_industry_group,
        "company_employee_band":  company_employee_band,
        "company_size": _curated_parse_size(sourced_company.get("employee_size_estimate")),
        "company_description": sourced_company.get("description"),
        "source": "curated_discovery",
        "source_actor_id": source_actor_id,
        "stage": "contactable",   # was "scraped"; must be in _CONTACTABLE_STAGES for pool reuse
        "enrichment_status": "not_started",
        "discovered_from_company_name": sourced_company.get("company_name"),
    }


def _curated_parse_size(estimate: str | None) -> int | None:
    if not estimate:
        return None
    import re
    nums = [int(n) for n in re.findall(r"\d+", str(estimate))]
    return max(nums) if nums else None
