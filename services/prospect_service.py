"""
Prospect processing service.
Handles deduplication, scoring, and MongoDB operations.
"""

import asyncio
from datetime import datetime
from bson import ObjectId
from config import get_settings
from database import prospects_collection, search_runs_collection, industries_collection, enrichment_runs_collection
from utils.scoring import score_prospect, score_prospect_v2, generate_tags, build_icp_context
from services.apify_service import (
    build_actor_input, build_lead_scraper_input, run_lead_scraper,
    run_lead_scraper_async,
)
from pymongo import UpdateOne, InsertOne
from pymongo.errors import BulkWriteError
import logging

logger = logging.getLogger(__name__)


async def process_search(industry_id: str = None, custom_params: dict = None, fetch_count: int = 100, start_page: int = None) -> dict:
    """
    Main search pipeline:
    1. Load industry (or use custom params)
    2. Create search_run record
    3. Call Apify
    4. Deduplicate + score + insert prospects
    5. Update search_run with results
    """

    # 1. Resolve params
    industry_name = "custom"
    apify_params = custom_params or {}
    industry = None

    if industry_id:
        industry = await industries_collection.find_one({"_id": ObjectId(industry_id)})
        if not industry:
            raise ValueError(f"Industry not found: {industry_id}")
        industry_name = industry["name"]
        apify_params = industry.get("apify_base_params", {})

    # Auto-calculate start_page from previous search runs if not provided
    if start_page is None and industry_id:
        last_run = await search_runs_collection.find_one(
            {
                "industry_name": industry_name,
                "status": "completed",
                "region_name": {"$exists": False},
            },
            sort=[("started_at", -1)],
        )
        if last_run:
            last_page = last_run.get("page_number", 0)
            start_page = last_page + 1
            logger.info(f"Auto-paginating: last page was {last_page}, using page {start_page}")
        else:
            start_page = 0
    elif start_page is None:
        start_page = 0

    # Override fetch_count
    apify_params["fetch_count"] = fetch_count
    actor_input = build_actor_input(apify_params, fetch_count, start_page=start_page)

    # 2. Create search run record
    search_run = {
        "industry_name": industry_name,
        "apify_run_id": None,
        "input_params": actor_input,
        "status": "running",
        "page_number": start_page,
        "total_fetched": 0,
        "new_prospects": 0,
        "duplicates_skipped": 0,
        "updated_prospects": 0,
        "started_at": datetime.utcnow(),
        "completed_at": None,
        "error": None,
    }
    run_result = await search_runs_collection.insert_one(search_run)
    search_run_id = str(run_result.inserted_id)

    try:
        # 3. Call the Apify Lead Scraper (single-actor pipeline)
        locations = apify_params.get("contact_location", ["united states"])
        lead_scraper_input = build_lead_scraper_input(
            apify_params=apify_params,
            locations=locations,
            fetch_count=100,
        )

        # LEADS_FINDER disabled — consolidated to LEAD_SCRAPER only.
        # To re-enable, uncomment the asyncio.gather block below and restore
        # the raw_prospects + lead_scraper_prospects merge.
        # finder_result, scraper_result = await asyncio.gather(
        #     run_leads_finder_async(actor_input),
        #     run_lead_scraper_async(lead_scraper_input),
        #     return_exceptions=True,
        # )
        # if isinstance(finder_result, Exception):
        #     raise finder_result  # Leads Finder is critical
        # apify_run_id, raw_prospects = finder_result
        # logger.info(f"Leads Finder returned {len(raw_prospects)} prospects")
        # lead_scraper_run_id = None
        # lead_scraper_prospects = []
        # if isinstance(scraper_result, Exception):
        #     logger.warning(f"Lead Scraper failed (non-fatal, continuing with Leads Finder results): {scraper_result}")
        # else:
        #     lead_scraper_run_id, lead_scraper_prospects = scraper_result
        #     logger.info(f"Lead Scraper returned {len(lead_scraper_prospects)} prospects")
        # all_prospects = raw_prospects + lead_scraper_prospects

        lead_scraper_run_id, lead_scraper_prospects = await run_lead_scraper_async(lead_scraper_input)
        logger.info(f"Lead Scraper returned {len(lead_scraper_prospects)} prospects")

        # Preserve response shape: leads_finder_fetched is kept as 0 so
        # downstream consumers don't break.
        raw_prospects: list[dict] = []
        all_prospects = lead_scraper_prospects
        total_fetched = len(all_prospects)

        apify_run_id = lead_scraper_run_id
        run_ids = lead_scraper_run_id

        await search_runs_collection.update_one(
            {"_id": run_result.inserted_id},
            {"$set": {
                "apify_run_id": run_ids,
                "total_fetched": total_fetched,
                "leads_finder_fetched": 0,
                "lead_scraper_fetched": len(lead_scraper_prospects),
            }}
        )

        # 4. Batch process all prospects
        icp_ctx = None
        if industry_id and industry:
            icp_ctx = build_icp_context(industry)

        new_count, updated_count, dup_count = await _batch_upsert_prospects(
            all_prospects, icp_ctx, search_run_id, industry_id=industry_id,
        )

        # 5. Finalize search run
        now = datetime.utcnow()
        await search_runs_collection.update_one(
            {"_id": run_result.inserted_id},
            {"$set": {
                "status": "completed",
                "new_prospects": new_count,
                "duplicates_skipped": dup_count,
                "updated_prospects": updated_count,
                "completed_at": now,
            }}
        )

        # Update industry last_run_at
        if industry_id:
            await industries_collection.update_one(
                {"_id": ObjectId(industry_id)},
                {"$set": {"last_run_at": now}, "$inc": {"total_runs": 1, "total_prospects_generated": new_count}}
            )

        logger.info(
            f"Search completed: {new_count} new, {updated_count} updated, {dup_count} duplicates "
            f"(Lead Scraper: {len(lead_scraper_prospects)})"
        )

        # Auto-trigger enrichment for high-scoring prospects
        auto_enrich_result = await _trigger_auto_enrichment(search_run_id, industry_id)

        result = {
            "search_run_id": search_run_id,
            "total_fetched": total_fetched,
            "leads_finder_fetched": 0,
            "lead_scraper_fetched": len(lead_scraper_prospects),
            "new_prospects": new_count,
            "updated_prospects": updated_count,
            "duplicates_skipped": dup_count,
            "status": "completed",
        }
        if auto_enrich_result:
            result["auto_enrich"] = auto_enrich_result

        return result

    except Exception as e:
        logger.error(f"Search failed: {e}", exc_info=True)
        await search_runs_collection.update_one(
            {"_id": run_result.inserted_id},
            {"$set": {"status": "failed", "error": str(e), "completed_at": datetime.utcnow()}}
        )
        raise


async def process_search_for_region(
    industry_id: str,
    region_name: str,
    apify_params: dict,
    fetch_count: int,
    start_page: int = 0,
    apify_semaphore: asyncio.Semaphore = None,
) -> dict:
    """
    Run a search for a specific region of an industry.
    Used by the scheduler for per-region scraping.

    Args:
        industry_id: Industry document ID
        region_name: Region name (e.g., "usa", "europe", "india")
        apify_params: Full Apify params including location fields from region
        fetch_count: Number of results to fetch for this region
        start_page: Starting page for pagination

    Returns:
        Dict with search results and exhaustion status
    """
    industry = await industries_collection.find_one({"_id": ObjectId(industry_id)})
    if not industry:
        raise ValueError(f"Industry not found: {industry_id}")

    # Get current per-scraper exhaustion state from region
    current_region_state = {}
    for r in industry.get("regions", []):
        if r.get("name") == region_name:
            current_region_state = r.get("pagination_state", {})
            break

    actor_input = build_actor_input(apify_params, fetch_count, start_page=start_page)

    # Create search run record
    search_run = {
        "industry_name": industry["name"],
        "region_name": region_name,
        "apify_run_id": None,
        "input_params": actor_input,
        "status": "running",
        "page_number": start_page,
        "total_fetched": 0,
        "new_prospects": 0,
        "duplicates_skipped": 0,
        "updated_prospects": 0,
        "started_at": datetime.utcnow(),
        "completed_at": None,
        "error": None,
    }
    run_result = await search_runs_collection.insert_one(search_run)
    search_run_id = str(run_result.inserted_id)

    try:
        logger.info(f"Fetching page {start_page} for {industry['name']} / {region_name}")

        # Run the Apify Lead Scraper (single-actor pipeline)
        locations = apify_params.get("contact_location", [])
        lead_scraper_input = build_lead_scraper_input(
            apify_params=apify_params,
            locations=locations,
            fetch_count=100,
        )

        # LEADS_FINDER disabled — consolidated to LEAD_SCRAPER only.
        # To re-enable, uncomment the asyncio.gather block below and restore
        # the raw_prospects + lead_scraper_prospects merge.
        # finder_result, scraper_result = await asyncio.gather(
        #     run_leads_finder_async(actor_input, semaphore=apify_semaphore),
        #     run_lead_scraper_async(lead_scraper_input, semaphore=apify_semaphore),
        #     return_exceptions=True,
        # )
        # if isinstance(finder_result, Exception):
        #     raise finder_result
        # apify_run_id, raw_prospects = finder_result
        # logger.info(f"Leads Finder returned {len(raw_prospects)} for {industry['name']}/{region_name}")
        # lead_scraper_run_id = None
        # lead_scraper_prospects = []
        # if isinstance(scraper_result, Exception):
        #     logger.warning(f"Lead Scraper failed for {industry['name']}/{region_name} (non-fatal): {scraper_result}")
        # else:
        #     lead_scraper_run_id, lead_scraper_prospects = scraper_result
        #     logger.info(f"Lead Scraper returned {len(lead_scraper_prospects)} for {industry['name']}/{region_name}")
        # all_prospects = raw_prospects + lead_scraper_prospects

        lead_scraper_run_id, lead_scraper_prospects = await run_lead_scraper_async(
            lead_scraper_input, semaphore=apify_semaphore,
        )
        logger.info(
            f"Lead Scraper returned {len(lead_scraper_prospects)} for {industry['name']}/{region_name}"
        )

        # Preserve response shape: leads_finder has no results in single-actor mode.
        raw_prospects: list[dict] = []
        apify_run_id = lead_scraper_run_id
        all_prospects = lead_scraper_prospects
        total_fetched = len(all_prospects)

        too_few_results = len(lead_scraper_prospects) < fetch_count

        # Build ICP context for v2 scoring
        icp_ctx = build_icp_context(industry) if industry else None

        new_count, updated_count, dup_count = await _batch_upsert_prospects(
            all_prospects, icp_ctx, search_run_id, industry_id=industry_id,
        )

        # Approximate per-scraper new counts for exhaustion tracking
        leads_finder_new = 0
        lead_scraper_new = 0
        if new_count > 0 and len(raw_prospects) > 0:
            # Proportional estimate based on result set sizes
            total = len(raw_prospects) + len(lead_scraper_prospects)
            if total > 0:
                leads_finder_new = round(new_count * len(raw_prospects) / total)
                lead_scraper_new = new_count - leads_finder_new

        # Per-scraper exhaustion tracking
        leads_finder_exhausted = len(raw_prospects) < fetch_count

        # Lead Scraper exhaustion: track consecutive runs with 0 new prospects
        lead_scraper_failed = (len(lead_scraper_prospects) == 0 and lead_scraper_run_id is None)
        prev_consecutive_zeros = current_region_state.get("lead_scraper_consecutive_zero_runs", 0)

        if lead_scraper_failed:
            # Scraper failed/errored - don't change consecutive count
            lead_scraper_consecutive_zero_runs = prev_consecutive_zeros
        elif len(lead_scraper_prospects) > 0 and lead_scraper_new == 0:
            # Returned results but none were new
            lead_scraper_consecutive_zero_runs = prev_consecutive_zeros + 1
        elif lead_scraper_new > 0:
            # Got new prospects - reset counter
            lead_scraper_consecutive_zero_runs = 0
        else:
            # Returned empty (not failed) - increment
            lead_scraper_consecutive_zero_runs = prev_consecutive_zeros + 1

        lead_scraper_exhausted = lead_scraper_consecutive_zero_runs >= 2

        # Region only exhausted when BOTH scrapers are exhausted
        is_exhausted = leads_finder_exhausted and lead_scraper_exhausted

        run_ids = lead_scraper_run_id or apify_run_id

        # Update search run
        await search_runs_collection.update_one(
            {"_id": run_result.inserted_id},
            {"$set": {
                "status": "completed",
                "apify_run_id": run_ids,
                "total_fetched": total_fetched,
                "leads_finder_fetched": 0,
                "lead_scraper_fetched": len(lead_scraper_prospects),
                "new_prospects": new_count,
                "duplicates_skipped": dup_count,
                "updated_prospects": updated_count,
                "is_final_page": is_exhausted,
                "completed_at": datetime.utcnow(),
            }}
        )

        if is_exhausted and new_count == 0:
            logger.warning(
                f"Region {region_name} for {industry['name']} returned 0 new prospects "
                f"({dup_count} duplicates) — marking as exhausted."
            )
        else:
            logger.info(
                f"Region search completed ({industry['name']}/{region_name}): "
                f"{new_count} new, {updated_count} updated, {dup_count} duplicates "
                f"(Lead Scraper: {len(lead_scraper_prospects)})"
            )

        # Auto-trigger enrichment for high-scoring prospects
        auto_enrich_result = await _trigger_auto_enrichment(search_run_id, industry_id)

        result = {
            "status": "completed",
            "search_run_id": search_run_id,
            "region_name": region_name,
            "page_number": start_page,
            "new_prospects": new_count,
            "updated_prospects": updated_count,
            "total_fetched": total_fetched,
            "leads_finder_fetched": 0,
            "lead_scraper_fetched": len(lead_scraper_prospects),
            "is_exhausted": is_exhausted,
            "leads_finder_exhausted": leads_finder_exhausted,
            "lead_scraper_exhausted": lead_scraper_exhausted,
            "lead_scraper_consecutive_zero_runs": lead_scraper_consecutive_zero_runs,
        }
        if auto_enrich_result:
            result["auto_enrich"] = auto_enrich_result

        return result

    except Exception as e:
        logger.error(f"Region search failed ({industry['name']}/{region_name}): {e}", exc_info=True)
        await search_runs_collection.update_one(
            {"_id": run_result.inserted_id},
            {"$set": {"status": "failed", "error": str(e), "completed_at": datetime.utcnow()}}
        )
        raise


async def _trigger_auto_enrichment(search_run_id: str, industry_id: str | None = None) -> dict | None:
    """
    Auto-trigger enrichment for high-scoring prospects from a search run.
    Only runs if auto_enrich_enabled is True in config.

    Returns:
        Dict with auto-enrichment info or None if not triggered.
    """
    settings = get_settings()
    if not settings.auto_enrich_enabled:
        return None

    # Find newly created prospects from this search run that qualify
    query = {
        "source_search_run_ids": search_run_id,
        "prospect_score": {"$gte": settings.auto_enrich_threshold},
        "enrichment_status": {"$in": ["not_started", None]},
        "linkedin": {"$ne": None},
    }

    cursor = prospects_collection.find(
        query, {"_id": 1}
    ).sort("prospect_score", -1).limit(settings.auto_enrich_max_batch)

    docs = await cursor.to_list(settings.auto_enrich_max_batch)
    prospect_ids = [str(doc["_id"]) for doc in docs]

    if not prospect_ids:
        logger.info("Auto-enrich: No qualifying prospects found")
        return None

    # Create enrichment run document
    run_doc = {
        "status": "running",
        "trigger": "auto_enrich",
        "source_search_run_id": search_run_id,
        "industry_id": industry_id,
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

    # Fire-and-forget: launch enrichment pipeline
    from services.enrichment_pipeline import run_enrichment_pipeline

    options = {
        "skip_profile_scrape": False,
        "skip_company_scrape": False,
        "skip_ai_assessment": False,
        "skip_outreach": False,
    }

    asyncio.create_task(run_enrichment_pipeline(run_id, prospect_ids, options))

    logger.info(
        f"Auto-enrich triggered: {len(prospect_ids)} prospects "
        f"(threshold={settings.auto_enrich_threshold}, run_id={run_id})"
    )

    return {
        "auto_enrich_triggered": True,
        "enrichment_run_id": run_id,
        "prospects_queued": len(prospect_ids),
    }


async def _batch_upsert_prospects(
    all_prospects: list[dict],
    icp_ctx: dict | None,
    search_run_id: str,
    industry_id: str | None = None,
) -> tuple[int, int, int]:
    """
    Batch upsert prospects: pre-process, batch-fetch existing, bulk_write.
    Returns (new_count, updated_count, dup_count).
    """
    now = datetime.utcnow()

    # 1. Pre-process all prospects: score, tag, extract data, filter
    processed = []
    seen_emails = set()
    seen_linkedins = set()

    for raw_prospect in all_prospects:
        email = (raw_prospect.get("email") or "").strip().lower() or None
        linkedin = (raw_prospect.get("linkedin") or "").strip() or None

        if not email and not linkedin:
            continue

        # Intra-batch dedup
        if email and email in seen_emails:
            continue
        if linkedin and linkedin in seen_linkedins:
            continue
        if email:
            seen_emails.add(email)
        if linkedin:
            seen_linkedins.add(linkedin)

        score, breakdown = score_prospect_v2(raw_prospect, icp_context=icp_ctx)
        tags = generate_tags(raw_prospect)
        prospect_data = _extract_prospect_data(raw_prospect)

        processed.append({
            "email": email,
            "linkedin": linkedin,
            "prospect_data": prospect_data,
            "score": score,
            "breakdown": breakdown,
            "tags": tags,
        })

    if not processed:
        return 0, 0, 0

    # 2. Batch-fetch existing prospects by email or linkedin
    or_clauses = []
    all_emails = [p["email"] for p in processed if p["email"]]
    all_linkedins = [p["linkedin"] for p in processed if p["linkedin"]]
    if all_emails:
        or_clauses.append({"email": {"$in": all_emails}})
    if all_linkedins:
        or_clauses.append({"linkedin": {"$in": all_linkedins}})

    existing_map = {}  # key (email or linkedin) -> existing doc
    if or_clauses:
        cursor = prospects_collection.find({"$or": or_clauses})
        async for doc in cursor:
            if doc.get("email"):
                existing_map[("email", doc["email"])] = doc
            if doc.get("linkedin"):
                existing_map[("linkedin", doc["linkedin"])] = doc

    # 3. Build bulk operations
    operations = []
    new_count = 0
    updated_count = 0
    dup_count = 0

    for p in processed:
        email = p["email"]
        linkedin = p["linkedin"]
        prospect_data = p["prospect_data"]
        score = p["score"]
        breakdown = p["breakdown"]
        tags = p["tags"]

        # Find existing by email first, then linkedin
        existing = None
        if email:
            existing = existing_map.get(("email", email))
        if not existing and linkedin:
            existing = existing_map.get(("linkedin", linkedin))

        if existing:
            # Merge: only update fields that are currently null/empty
            update_fields = {"last_updated_at": now}
            has_updates = False

            for key, new_val in prospect_data.items():
                if new_val is not None and (existing.get(key) is None or existing.get(key) == ""):
                    update_fields[key] = new_val
                    has_updates = True

            if score > existing.get("prospect_score", 0):
                update_fields["prospect_score"] = score
                update_fields["score_breakdown"] = breakdown

            existing_tags = set(existing.get("tags", []))
            new_tags = existing_tags.union(set(tags))
            if new_tags != existing_tags:
                update_fields["tags"] = list(new_tags)

            if industry_id and not existing.get("industry_id"):
                update_fields["industry_id"] = industry_id

            add_to_set = {"source_search_run_ids": search_run_id}
            if industry_id:
                add_to_set["source_industry_ids"] = industry_id

            operations.append(UpdateOne(
                {"_id": existing["_id"]},
                {"$set": update_fields, "$addToSet": add_to_set},
            ))

            if has_updates:
                updated_count += 1
            else:
                dup_count += 1
        else:
            # New prospect
            prospect_data.update({
                "prospect_score": score,
                "score_breakdown": breakdown,
                "source_search_run_ids": [search_run_id],
                "source_industry_ids": [industry_id] if industry_id else [],
                "industry_id": industry_id,
                "tags": tags,
                "status": "new",
                "first_seen_at": now,
                "last_updated_at": now,
            })
            operations.append(InsertOne(prospect_data))
            new_count += 1

    # 4. Execute bulk write
    if operations:
        try:
            await prospects_collection.bulk_write(operations, ordered=False)
        except BulkWriteError as bwe:
            # Handle race-condition duplicates from InsertOne ops
            write_errors = bwe.details.get("writeErrors", [])
            dup_errors = [e for e in write_errors if e.get("code") == 11000]
            non_dup_errors = [e for e in write_errors if e.get("code") != 11000]

            # Adjust counts: duplicate inserts weren't actually new
            new_count -= len(dup_errors)
            dup_count += len(dup_errors)

            if dup_errors:
                logger.warning(f"Batch upsert: {len(dup_errors)} race-condition duplicates absorbed")
            if non_dup_errors:
                logger.error(f"Batch upsert: {len(non_dup_errors)} non-duplicate write errors: {non_dup_errors}")

    logger.info(f"Batch upsert complete: {new_count} new, {updated_count} updated, {dup_count} duplicates")
    return new_count, updated_count, dup_count


def _extract_prospect_data(raw_prospect: dict) -> dict:
    """Extract prospect fields from a raw Apify result."""
    raw_email = (raw_prospect.get("email") or "").strip().lower()
    return {
        "first_name": raw_prospect.get("first_name"),
        "last_name": raw_prospect.get("last_name"),
        "full_name": raw_prospect.get("full_name"),
        "email": raw_email or None,  # Store None instead of "" for sparse unique index
        "personal_email": raw_prospect.get("personal_email"),
        "mobile_number": raw_prospect.get("mobile_number"),
        "linkedin": (raw_prospect.get("linkedin") or "").strip() or None,
        "job_title": raw_prospect.get("job_title"),
        "headline": raw_prospect.get("headline"),
        "seniority_level": raw_prospect.get("seniority_level"),
        "functional_level": raw_prospect.get("functional_level"),
        "city": raw_prospect.get("city"),
        "state": raw_prospect.get("state"),
        "country": raw_prospect.get("country"),
        "company_name": raw_prospect.get("company_name"),
        "company_website": raw_prospect.get("company_website"),
        "company_domain": raw_prospect.get("company_domain"),
        "company_linkedin": raw_prospect.get("company_linkedin"),
        "company_linkedin_uid": raw_prospect.get("company_linkedin_uid"),
        "industry": raw_prospect.get("industry"),
        "company_size": raw_prospect.get("company_size"),
        "company_founded_year": raw_prospect.get("company_founded_year"),
        "company_phone": raw_prospect.get("company_phone"),
        "company_street_address": raw_prospect.get("company_street_address"),
        "company_full_address": raw_prospect.get("company_full_address"),
        "company_state": raw_prospect.get("company_state"),
        "company_city": raw_prospect.get("company_city"),
        "company_country": raw_prospect.get("company_country"),
        "company_postal_code": raw_prospect.get("company_postal_code"),
        "keywords": raw_prospect.get("keywords"),
        "company_description": raw_prospect.get("company_description"),
        "company_annual_revenue": raw_prospect.get("company_annual_revenue"),
        "company_annual_revenue_clean": raw_prospect.get("company_annual_revenue_clean"),
        "company_total_funding": raw_prospect.get("company_total_funding"),
        "company_total_funding_clean": raw_prospect.get("company_total_funding_clean"),
        "company_technologies": raw_prospect.get("company_technologies"),
    }


async def _upsert_prospect(
    email: str,
    linkedin: str | None,
    prospect_data: dict,
    score: float,
    breakdown: dict,
    tags: list[str],
    search_run_id: str,
    industry_id: str | None = None,
) -> str:
    """
    Upsert a prospect. Returns 'new', 'updated', or 'duplicate'.

    Dedup logic:
    - Primary key: email
    - Secondary key: linkedin (if email not found)
    - Merge: fill in null fields from new data, never overwrite existing values
    - Always append search_run_id to source list
    """
    now = datetime.utcnow()

    # Try to find existing prospect by email
    existing = None
    if email:
        existing = await prospects_collection.find_one({"email": email})

    # If not found by email, try linkedin
    if not existing and linkedin:
        existing = await prospects_collection.find_one({"linkedin": linkedin})

    if existing:
        # Merge: only update fields that are currently null/empty
        update_fields = {"last_updated_at": now}
        has_updates = False

        for key, new_val in prospect_data.items():
            if new_val is not None and (existing.get(key) is None or existing.get(key) == ""):
                update_fields[key] = new_val
                has_updates = True

        # Always update score if new score is higher
        if score > existing.get("prospect_score", 0):
            update_fields["prospect_score"] = score
            update_fields["score_breakdown"] = breakdown

        # Merge tags (union)
        existing_tags = set(existing.get("tags", []))
        new_tags = existing_tags.union(set(tags))
        if new_tags != existing_tags:
            update_fields["tags"] = list(new_tags)

        # Set industry_id if prospect doesn't have one yet
        if industry_id and not existing.get("industry_id"):
            update_fields["industry_id"] = industry_id

        add_to_set = {"source_search_run_ids": search_run_id}
        if industry_id:
            add_to_set["source_industry_ids"] = industry_id

        await prospects_collection.update_one(
            {"_id": existing["_id"]},
            {
                "$set": update_fields,
                "$addToSet": add_to_set,
            }
        )

        return "updated" if has_updates else "duplicate"

    else:
        # New prospect
        prospect_data.update({
            "prospect_score": score,
            "score_breakdown": breakdown,
            "source_search_run_ids": [search_run_id],
            "source_industry_ids": [industry_id] if industry_id else [],
            "industry_id": industry_id,
            "tags": tags,
            "status": "new",
            "first_seen_at": now,
            "last_updated_at": now,
        })

        try:
            await prospects_collection.insert_one(prospect_data)
            return "new"
        except Exception as e:
            # Handle race condition on unique index
            if "duplicate key" in str(e).lower():
                logger.warning(f"Race condition duplicate for {email}, skipping")
                return "duplicate"
            raise
