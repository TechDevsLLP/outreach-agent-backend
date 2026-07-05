# DEPRECATED — LEADS_FINDER disabled. Use services.prospect_service.process_search instead.
"""
Lead processing service.
Handles deduplication, scoring, and MongoDB operations.
"""

from datetime import datetime
from database import leads_collection, search_runs_collection, icp_profiles_collection
from utils.scoring import score_lead, generate_tags
from services.apify_service import build_actor_input, run_leads_finder
import logging

logger = logging.getLogger(__name__)


async def process_search(profile_id: str = None, custom_params: dict = None, fetch_count: int = 100) -> dict:
    """
    Main search pipeline:
    1. Load ICP profile (or use custom params)
    2. Create search_run record
    3. Call Apify
    4. Deduplicate + score + insert leads
    5. Update search_run with results
    """

    # 1. Resolve params
    profile_name = "custom"
    apify_params = custom_params or {}

    if profile_id:
        from bson import ObjectId
        profile = await icp_profiles_collection.find_one({"_id": ObjectId(profile_id)})
        if not profile:
            raise ValueError(f"ICP profile not found: {profile_id}")
        profile_name = profile["name"]
        apify_params = profile.get("apify_params", {})

    # Override fetch_count
    apify_params["fetch_count"] = fetch_count
    actor_input = build_actor_input(apify_params, fetch_count)

    # 2. Create search run record
    search_run = {
        "profile_name": profile_name,
        "apify_run_id": None,
        "input_params": actor_input,
        "status": "running",
        "total_fetched": 0,
        "new_leads": 0,
        "duplicates_skipped": 0,
        "updated_leads": 0,
        "started_at": datetime.utcnow(),
        "completed_at": None,
        "error": None,
    }
    run_result = await search_runs_collection.insert_one(search_run)
    search_run_id = str(run_result.inserted_id)

    try:
        # 3. Call Apify (blocking)
        raise NotImplementedError(
            "LEADS_FINDER actor is disabled. Use services.prospect_service.process_search "
            "which uses LEAD_SCRAPER."
        )
        apify_run_id, raw_leads = run_leads_finder(actor_input)

        await search_runs_collection.update_one(
            {"_id": run_result.inserted_id},
            {"$set": {"apify_run_id": apify_run_id, "total_fetched": len(raw_leads)}}
        )

        # 4. Process each lead
        new_count = 0
        dup_count = 0
        updated_count = 0

        for raw_lead in raw_leads:
            email = (raw_lead.get("email") or "").strip().lower()
            linkedin = (raw_lead.get("linkedin") or "").strip() or None

            if not email and not linkedin:
                continue  # Skip leads with no identifiers

            # Score the lead
            score, breakdown = score_lead(raw_lead)
            tags = generate_tags(raw_lead)

            # Build the lead document
            lead_data = {
                "first_name": raw_lead.get("first_name"),
                "last_name": raw_lead.get("last_name"),
                "full_name": raw_lead.get("full_name"),
                "email": email,
                "personal_email": raw_lead.get("personal_email"),
                "mobile_number": raw_lead.get("mobile_number"),
                "linkedin": linkedin,
                "job_title": raw_lead.get("job_title"),
                "headline": raw_lead.get("headline"),
                "seniority_level": raw_lead.get("seniority_level"),
                "functional_level": raw_lead.get("functional_level"),
                "city": raw_lead.get("city"),
                "state": raw_lead.get("state"),
                "country": raw_lead.get("country"),
                "company_name": raw_lead.get("company_name"),
                "company_website": raw_lead.get("company_website"),
                "company_domain": raw_lead.get("company_domain"),
                "company_linkedin": raw_lead.get("company_linkedin"),
                "company_linkedin_uid": raw_lead.get("company_linkedin_uid"),
                "industry": raw_lead.get("industry"),
                "company_size": raw_lead.get("company_size"),
                "company_founded_year": raw_lead.get("company_founded_year"),
                "company_phone": raw_lead.get("company_phone"),
                "company_street_address": raw_lead.get("company_street_address"),
                "company_full_address": raw_lead.get("company_full_address"),
                "company_state": raw_lead.get("company_state"),
                "company_city": raw_lead.get("company_city"),
                "company_country": raw_lead.get("company_country"),
                "company_postal_code": raw_lead.get("company_postal_code"),
                "keywords": raw_lead.get("keywords"),
                "company_description": raw_lead.get("company_description"),
                "company_annual_revenue": raw_lead.get("company_annual_revenue"),
                "company_annual_revenue_clean": raw_lead.get("company_annual_revenue_clean"),
                "company_total_funding": raw_lead.get("company_total_funding"),
                "company_total_funding_clean": raw_lead.get("company_total_funding_clean"),
                "company_technologies": raw_lead.get("company_technologies"),
            }

            # Dedup: try email first, then linkedin
            result = await _upsert_lead(email, linkedin, lead_data, score, breakdown, tags, search_run_id)

            if result == "new":
                new_count += 1
            elif result == "updated":
                updated_count += 1
            else:
                dup_count += 1

        # 5. Finalize search run
        now = datetime.utcnow()
        await search_runs_collection.update_one(
            {"_id": run_result.inserted_id},
            {"$set": {
                "status": "completed",
                "new_leads": new_count,
                "duplicates_skipped": dup_count,
                "updated_leads": updated_count,
                "completed_at": now,
            }}
        )

        # Update profile last_run_at
        if profile_id:
            from bson import ObjectId
            await icp_profiles_collection.update_one(
                {"_id": ObjectId(profile_id)},
                {"$set": {"last_run_at": now}, "$inc": {"total_runs": 1, "total_leads_generated": new_count}}
            )

        logger.info(f"Search completed: {new_count} new, {updated_count} updated, {dup_count} duplicates")

        return {
            "search_run_id": search_run_id,
            "total_fetched": len(raw_leads),
            "new_leads": new_count,
            "updated_leads": updated_count,
            "duplicates_skipped": dup_count,
            "status": "completed",
        }

    except Exception as e:
        logger.error(f"Search failed: {e}", exc_info=True)
        await search_runs_collection.update_one(
            {"_id": run_result.inserted_id},
            {"$set": {"status": "failed", "error": str(e), "completed_at": datetime.utcnow()}}
        )
        raise


async def _upsert_lead(
    email: str,
    linkedin: str | None,
    lead_data: dict,
    score: float,
    breakdown: dict,
    tags: list[str],
    search_run_id: str,
) -> str:
    """
    Upsert a lead. Returns 'new', 'updated', or 'duplicate'.

    Dedup logic:
    - Primary key: email
    - Secondary key: linkedin (if email not found)
    - Merge: fill in null fields from new data, never overwrite existing values
    - Always append search_run_id to source list
    """
    now = datetime.utcnow()

    # Try to find existing lead by email
    existing = None
    if email:
        existing = await leads_collection.find_one({"email": email})

    # If not found by email, try linkedin
    if not existing and linkedin:
        existing = await leads_collection.find_one({"linkedin": linkedin})

    if existing:
        # Merge: only update fields that are currently null/empty
        update_fields = {"last_updated_at": now}
        has_updates = False

        for key, new_val in lead_data.items():
            if new_val is not None and (existing.get(key) is None or existing.get(key) == ""):
                update_fields[key] = new_val
                has_updates = True

        # Always update score if new score is higher
        if score > existing.get("lead_score", 0):
            update_fields["lead_score"] = score
            update_fields["score_breakdown"] = breakdown

        # Merge tags (union)
        existing_tags = set(existing.get("tags", []))
        new_tags = existing_tags.union(set(tags))
        if new_tags != existing_tags:
            update_fields["tags"] = list(new_tags)

        await leads_collection.update_one(
            {"_id": existing["_id"]},
            {
                "$set": update_fields,
                "$addToSet": {"source_search_run_ids": search_run_id},
            }
        )

        return "updated" if has_updates else "duplicate"

    else:
        # New lead
        lead_data.update({
            "lead_score": score,
            "score_breakdown": breakdown,
            "source_search_run_ids": [search_run_id],
            "tags": tags,
            "status": "new",
            "first_seen_at": now,
            "last_updated_at": now,
        })

        try:
            await leads_collection.insert_one(lead_data)
            return "new"
        except Exception as e:
            # Handle race condition on unique index
            if "duplicate key" in str(e).lower():
                logger.warning(f"Race condition duplicate for {email}, skipping")
                return "duplicate"
            raise
