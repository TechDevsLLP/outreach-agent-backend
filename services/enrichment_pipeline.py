"""
Main enrichment pipeline orchestrator.
Coordinates profile scraping, company scraping, AI assessment, and outreach generation.
Each lead's failure is isolated — one prospect failing doesn't block others.

Optimizations applied:
- Bulk MongoDB writes instead of sequential update_one loops
- Concurrent Apify scrape batches (up to apify_actor_concurrency_limit)
- Parallel Phase 2.5 (competitors) + Phase 3 (AI assessment) since competitors are only used in Phase 4
- Concurrent contact discovery across companies
- Shared OpenRouterClient created once
- Concurrent outreach sequence initialization
- Extracted shared helpers for employee formatting and AI ranking
"""

import asyncio
import logging
from datetime import datetime
from bson import ObjectId

from pymongo import UpdateOne

from config import get_settings
from database import (
    prospects_collection,
    enrichment_runs_collection,
    companies_collection,
    prospect_state_collection,
    geo_places_collection,
    industries_taxonomy_collection,
)
from services.openrouter_service import OpenRouterClient
from services.linkedin_scraper_service import scrape_linkedin_profiles, match_profiles_to_urls
from services.company_scraper_service import (
    extract_unique_company_urls,
    scrape_company_pages,
    match_companies_to_urls,
)
from services.ai_assessment_service import assess_lead, batch_assess_leads, compute_ai_prospect_score
from services.timezone_service import infer_timezone, calculate_optimal_send_time
from utils.scoring import is_decision_maker_rule_based, score_company_fit_rule_based

logger = logging.getLogger(__name__)

settings = get_settings()


async def run_enrichment_pipeline(
    enrichment_run_id: str,
    prospect_ids: list[str],
    options: dict,
    triggered_by: str | None = None,
) -> dict:
    """
    Run the full enrichment pipeline for the given leads.

    Phases:
    0. Setup - fetch leads, filter invalid ones
    1. LinkedIn Profile Scrape
    2. Company Scrape with Deduplication
    2.5 + 3. Competitor Research (parallel with) AI Assessment
    3.5. Contact Discovery (for "good company, wrong person")
    4. Outreach Generation
    5. Finalize
    """
    run_oid = ObjectId(enrichment_run_id)
    pipeline_start = datetime.utcnow()
    skip_profile = options.get("skip_profile_scrape", False)
    skip_company = options.get("skip_company_scrape", False)
    skip_competitors = options.get("skip_competitor_research", False)
    skip_ai = options.get("skip_ai_assessment", False)
    skip_outreach = options.get("skip_outreach", False)
    skip_triage = options.get("skip_pre_enrichment_triage", False)

    stats = {
        "prospects_processed": 0,
        "prospects_skipped": 0,
        "prospects_failed": 0,
        "profiles_scraped": 0,
        "companies_scraped": 0,
        "companies_deduplicated": 0,
        "ai_assessments_done": 0,
        "outreach_generated": 0,
        "triage_decision_makers": 0,
        "triage_wrong_person": 0,
        "triage_companies_to_discover": 0,
        "triage_contacts_discovered": 0,
    }

    # Create OpenRouterClient once, reuse across all phases
    openrouter_client = None

    # ── Fetch company profile for personalised AI assessment ──
    company_profile: dict | None = None
    _account_id_for_profile = None
    if triggered_by:
        try:
            from database import users_collection
            user_doc = await users_collection.find_one({"_id": ObjectId(triggered_by)})
            if user_doc and user_doc.get("current_account_id"):
                _account_id_for_profile = user_doc["current_account_id"]
        except Exception as e:
            logger.warning(f"Could not resolve account from triggered_by={triggered_by}: {e}")
    elif options.get("account_id"):
        _account_id_for_profile = options["account_id"]

    if _account_id_for_profile:
        try:
            from database import company_profiles_collection
            _aid = _account_id_for_profile if isinstance(_account_id_for_profile, ObjectId) else ObjectId(_account_id_for_profile)
            company_profile = await company_profiles_collection.find_one({"account_id": _aid})
            if company_profile:
                logger.info(f"Using company profile '{company_profile.get('company_name')}' for personalised AI assessment")
        except Exception as e:
            logger.warning(f"Could not fetch company profile for account={_account_id_for_profile}: {e}")

    try:
        # ── Phase 0: Setup ──
        await _update_run(run_oid, {"current_step": "setup"})

        lead_oids = [ObjectId(lid) for lid in prospect_ids]
        prospects = await prospects_collection.find({"_id": {"$in": lead_oids}}).to_list(len(lead_oids))

        # Filter out prospects without LinkedIn URL → mark "skipped" via bulk write
        valid_leads = []
        bulk_ops = []
        for prospect in prospects:
            if not prospect.get("linkedin"):
                bulk_ops.append(UpdateOne(
                    {"_id": prospect["_id"]},
                    {"$set": {
                        "enrichment_status": "skipped",
                        "enrichment_error": "No LinkedIn URL",
                    }}
                ))
                stats["prospects_skipped"] += 1
            else:
                valid_leads.append(prospect)
                bulk_ops.append(UpdateOne(
                    {"_id": prospect["_id"]},
                    {"$set": {
                        "enrichment_status": "in_progress",
                        "enrichment_started_at": datetime.utcnow(),
                        "enrichment_run_id": enrichment_run_id,
                        "enrichment_error": None,
                    }}
                ))

        if bulk_ops:
            await prospects_collection.bulk_write(bulk_ops, ordered=False)

        await _update_run(run_oid, {
            "total_prospects": len(prospects),
            "prospects_skipped": stats["prospects_skipped"],
        })

        if not valid_leads:
            await _update_run(run_oid, {
                "status": "completed",
                "current_step": "completed",
                "completed_at": datetime.utcnow(),
                "error": "No valid prospects to enrich (all missing LinkedIn URL)",
            })
            return stats

        # ── Phase 0.5: Pre-Enrichment Triage (DISABLED — uncomment to re-enable) ──
        # if not skip_triage and settings.pre_enrichment_triage_enabled:
        #     await _update_run(run_oid, {"current_step": "pre_enrichment_triage"})
        #     valid_leads, stats = await _phase_pre_enrichment_triage(
        #         valid_leads, stats, enrichment_run_id
        #     )
        #     await _update_run(run_oid, {
        #         "triage_decision_makers": stats["triage_decision_makers"],
        #         "triage_wrong_person": stats["triage_wrong_person"],
        #         "triage_contacts_discovered": stats.get("triage_contacts_discovered", 0),
        #     })

        # ── Phase 1: LinkedIn Profile Scrape ──
        elapsed = (datetime.utcnow() - pipeline_start).total_seconds()
        if elapsed > 1200:
            logger.error(f"Enrichment pipeline exceeded 20-minute wall-clock limit after {elapsed:.0f}s, aborting before Phase 1")
            await _update_run(run_oid, {
                "status": "failed",
                "current_step": "failed",
                "error": f"Pipeline exceeded 20-minute time limit after {int(elapsed)}s",
                "completed_at": datetime.utcnow(),
                **stats,
            })
            return stats

        if not skip_profile:
            await _update_run(run_oid, {"current_step": "scraping_profiles"})
            valid_leads, stats = await _phase_profile_scrape(valid_leads, stats)
            await _update_run(run_oid, {
                "profiles_scraped": stats["profiles_scraped"],
                "prospects_failed": stats["prospects_failed"],
            })

        # ── Phase 2: Company Scrape with Dedup ──
        elapsed = (datetime.utcnow() - pipeline_start).total_seconds()
        if elapsed > 1200:
            logger.error(f"Enrichment pipeline exceeded 20-minute wall-clock limit after {elapsed:.0f}s, aborting before Phase 2")
            await _update_run(run_oid, {
                "status": "failed",
                "current_step": "failed",
                "error": f"Pipeline exceeded 20-minute time limit after {int(elapsed)}s",
                "completed_at": datetime.utcnow(),
                **stats,
            })
            return stats

        if not skip_company:
            await _update_run(run_oid, {"current_step": "scraping_companies"})
            valid_leads, stats = await _phase_company_scrape(valid_leads, stats)
            await _update_run(run_oid, {
                "companies_scraped": stats["companies_scraped"],
                "companies_deduplicated": stats["companies_deduplicated"],
            })

        # ── Phase 2.5 + 2.6 + Phase 3: Competitors + News + AI Assessment (PARALLEL) ──
        # Competitors and news both depend only on company_name; AI assessment runs concurrently.
        wrong_person_prospects = []

        if not skip_competitors and not skip_ai:
            openrouter_client = OpenRouterClient()
            await _update_run(run_oid, {"current_step": "ai_assessment_and_competitors"})

            # Run all three in parallel
            competitors_task = asyncio.create_task(
                _phase_competitors(valid_leads, stats)
            )
            news_task = asyncio.create_task(
                _phase_news(valid_leads, stats)
            )
            assessment_task = asyncio.create_task(
                _phase_ai_assessment(valid_leads, stats, openrouter_client, company_profile=company_profile, account_id=_account_id_for_profile)
            )

            # Wait for all to complete
            (valid_leads_from_competitors, stats), (valid_leads_from_news, stats), (valid_leads_from_ai, wrong_person_prospects, stats) = (
                await asyncio.gather(competitors_task, news_task, assessment_task)
            )

            # Merge competitor/news data into AI-assessed prospects (both update in-place and in DB)
            valid_leads = valid_leads_from_ai

            await _update_run(run_oid, {
                "competitors_researched": stats.get("competitors_researched", 0),
                "news_researched": stats.get("news_researched", 0),
                "ai_assessments_done": stats["ai_assessments_done"],
                "prospects_failed": stats["prospects_failed"],
                "wrong_person_prospects": len(wrong_person_prospects),
            })

        elif not skip_competitors:
            await _update_run(run_oid, {"current_step": "researching_competitors"})
            competitors_task = asyncio.create_task(_phase_competitors(valid_leads, stats))
            news_task = asyncio.create_task(_phase_news(valid_leads, stats))
            (valid_leads, stats), (_, stats) = await asyncio.gather(competitors_task, news_task)
            await _update_run(run_oid, {
                "competitors_researched": stats.get("competitors_researched", 0),
                "news_researched": stats.get("news_researched", 0),
            })

        elif not skip_ai:
            openrouter_client = OpenRouterClient()
            await _update_run(run_oid, {"current_step": "ai_assessment"})
            valid_leads, wrong_person_prospects, stats = await _phase_ai_assessment(valid_leads, stats, openrouter_client, company_profile=company_profile, account_id=_account_id_for_profile)
            await _update_run(run_oid, {
                "ai_assessments_done": stats["ai_assessments_done"],
                "prospects_failed": stats["prospects_failed"],
                "wrong_person_prospects": len(wrong_person_prospects),
            })

        # ── Phase 3.5: Contact Discovery for "wrong person" prospects ──
        if wrong_person_prospects:
            if not openrouter_client:
                openrouter_client = OpenRouterClient()
            await _update_run(run_oid, {"current_step": "contact_discovery"})
            discovered_prospects, stats = await _phase_contact_discovery(
                wrong_person_prospects, stats, openrouter_client
            )
            valid_leads.extend(discovered_prospects)
            await _update_run(run_oid, {
                "contacts_discovered": stats.get("contacts_discovered", 0),
                "employees_scraped_for_discovery": stats.get("employees_scraped_for_discovery", 0),
                "emails_found": stats.get("emails_found", 0),
            })

        # ── Phase 4: Outreach Generation (DISABLED — outreach is campaign-specific) ──
        # if not skip_outreach:
        #     if not openrouter_client:
        #         openrouter_client = OpenRouterClient()
        #     await _update_run(run_oid, {"current_step": "outreach_generation"})
        #     valid_leads, stats = await _phase_outreach(valid_leads, stats, openrouter_client)
        #     await _update_run(run_oid, {
        #         "outreach_generated": stats["outreach_generated"],
        #         "prospects_failed": stats["prospects_failed"],
        #     })

        # Mark ai_assessed prospects as completed
        if valid_leads:
            valid_oids = [p["_id"] for p in valid_leads]
            bulk_ops = [
                UpdateOne(
                    {"_id": oid, "enrichment_status": "ai_assessed"},
                    {"$set": {
                        "enrichment_status": "completed",
                        "enrichment_completed_at": datetime.utcnow(),
                    }}
                )
                for oid in valid_oids
            ]
            await prospects_collection.bulk_write(bulk_ops, ordered=False)

        # ── Phase 5: Finalize ──
        stats["prospects_processed"] = len(valid_leads)

        # Phase 5.0: Auto-trigger employee discovery (DISABLED — invoke explicitly via API)
        # if settings.auto_discover_contacts_enabled:
        #     stats["employee_discoveries_queued"] = 0
        #     from services.employee_discovery_service import discover_best_contacts
        #     for prospect in valid_leads:
        #         score = prospect.get("ai_prospect_score") or 0
        #         if score >= settings.auto_discover_contacts_threshold:
        #             try:
        #                 asyncio.create_task(discover_best_contacts(
        #                     prospect_id=str(prospect["_id"]),
        #                     max_contacts=3,
        #                     auto_enrich=False,
        #                 ))
        #                 stats["employee_discoveries_queued"] += 1
        #             except Exception as e:
        #                 logger.warning(f"Employee discovery queue failed for {prospect.get('email')}: {e}")

        # Phase 5.1: Auto-initialize outreach sequences (DISABLED — campaign-specific)
        # stats["outreach_sequences_initialized"] = 0
        # from services.followup_sequence_service import initialize_followup_sequence
        # eligible_prospects = [
        #     p for p in valid_leads
        #     if (p.get("ai_prospect_score") or 0) >= 50
        #     and not p.get("followup_sequence")
        # ]
        # if eligible_prospects:
        #     async def _init_sequence(prospect):
        #         try:
        #             await initialize_followup_sequence(str(prospect["_id"]))
        #             return True
        #         except Exception as e:
        #             logger.warning(f"Failed to init outreach for {prospect.get('email')}: {e}")
        #             return False
        #     results = await asyncio.gather(*[_init_sequence(p) for p in eligible_prospects])
        #     stats["outreach_sequences_initialized"] = sum(1 for r in results if r)
        # if stats["outreach_sequences_initialized"] > 0:
        #     logger.info(f"Auto-initialized {stats['outreach_sequences_initialized']} outreach sequences")

        # Set enriched_by on all prospects that completed successfully in this run
        if triggered_by and valid_leads:
            valid_oids = [p["_id"] for p in valid_leads]
            await prospects_collection.update_many(
                {"_id": {"$in": valid_oids}, "enrichment_status": "completed"},
                {"$set": {"enriched_by": triggered_by}},
            )

        await _update_run(run_oid, {
            "status": "completed",
            "current_step": "completed",
            "completed_at": datetime.utcnow(),
            "prospects_processed": stats["prospects_processed"],
            **{k: v for k, v in stats.items()},
        })

        logger.info(f"Enrichment pipeline completed: {stats}")
        return stats

    except Exception as e:
        logger.error(f"Enrichment pipeline failed: {e}", exc_info=True)
        await _update_run(run_oid, {
            "status": "failed",
            "current_step": "failed",
            "error": str(e),
            "completed_at": datetime.utcnow(),
            **stats,
        })
        return stats

    finally:
        if openrouter_client:
            await openrouter_client.close()


# ── Phase Implementations ──


async def _phase_profile_scrape(prospects: list[dict], stats: dict) -> tuple[list[dict], dict]:
    """Phase 1: Scrape LinkedIn profiles with concurrent batches."""
    batch_size = settings.enrichment_batch_size
    urls = [prospect["linkedin"] for prospect in prospects]

    if not urls:
        return prospects, stats

    # Split into batches and run concurrently (up to concurrency limit)
    batches = [urls[i:i + batch_size] for i in range(0, len(urls), batch_size)]
    scrape_semaphore = asyncio.Semaphore(settings.apify_actor_concurrency_limit)

    async def _scrape_batch(batch_urls: list[str]) -> dict:
        async with scrape_semaphore:
            try:
                _run_id, results = await scrape_linkedin_profiles(batch_urls)
                return match_profiles_to_urls(batch_urls, results)
            except Exception as e:
                logger.error(f"Profile scrape batch failed: {e}", exc_info=True)
                return {}

    batch_results = await asyncio.gather(*[_scrape_batch(b) for b in batches])

    # Merge all batch results
    all_matched = {}
    for result in batch_results:
        all_matched.update(result)

    # Map results back to prospects and bulk-update DB
    bulk_ops = []
    for prospect in prospects:
        linkedin_url = prospect["linkedin"]
        profile_data = all_matched.get(linkedin_url)
        if profile_data:
            prospect["linkedin_profile_data"] = profile_data
            db_set: dict = {
                "linkedin_profile_data": profile_data,
                "enrichment_status": "profile_scraped",
            }
            # Backfill email from profile if prospect doesn't have one yet
            extracted_email = profile_data.get("extracted_email")
            if extracted_email and not prospect.get("email"):
                prospect["email"] = extracted_email
                db_set["email"] = extracted_email
                db_set["email_source"] = "linkedin_profile"
            bulk_ops.append(UpdateOne({"_id": prospect["_id"]}, {"$set": db_set}))
            stats["profiles_scraped"] += 1
        else:
            logger.warning(f"No profile data for {linkedin_url}, continuing without profile")
            bulk_ops.append(UpdateOne(
                {"_id": prospect["_id"]},
                {"$set": {"enrichment_status": "profile_scraped"}}
            ))

    if bulk_ops:
        await prospects_collection.bulk_write(bulk_ops, ordered=False)

    return prospects, stats


async def _phase_company_scrape(prospects: list[dict], stats: dict) -> tuple[list[dict], dict]:
    """Phase 2: Scrape company pages with deduplication and concurrent batches."""
    batch_size = settings.company_scrape_batch_size

    # Extract and deduplicate company URLs
    company_to_prospects = extract_unique_company_urls(prospects)
    unique_urls = list(company_to_prospects.keys())
    total_prospect_company_pairs = sum(len(ids) for ids in company_to_prospects.values())
    stats["companies_deduplicated"] = total_prospect_company_pairs - len(unique_urls)

    if not unique_urls:
        return prospects, stats

    # Split into batches and run concurrently
    batches = [unique_urls[i:i + batch_size] for i in range(0, len(unique_urls), batch_size)]
    scrape_semaphore = asyncio.Semaphore(settings.apify_actor_concurrency_limit)

    async def _scrape_batch(batch_urls: list[str]) -> dict:
        async with scrape_semaphore:
            try:
                _run_id, results = await asyncio.to_thread(scrape_company_pages, batch_urls)
                return match_companies_to_urls(batch_urls, results)
            except Exception as e:
                logger.error(f"Company scrape batch failed: {e}", exc_info=True)
                return {}

    batch_results = await asyncio.gather(*[_scrape_batch(b) for b in batches])

    # Merge all batch results
    all_matched = {}
    for result in batch_results:
        all_matched.update(result)

    stats["companies_scraped"] = len(all_matched)

    # Build prospect_id -> company_data mapping
    prospect_id_to_company = {}
    for company_url, prospect_ids in company_to_prospects.items():
        company_data = all_matched.get(company_url)
        if company_data:
            for prospect_id in prospect_ids:
                prospect_id_to_company[prospect_id] = company_data

    # Upsert each scraped company into companies_collection with canonical schema
    from services.geo_resolver import resolve as geo_resolve
    from services.industry_canonicalizer import resolve as industry_resolve

    def _band(n: int | None) -> str | None:
        if n is None: return None
        if n <= 10: return "1-10"
        if n <= 50: return "11-50"
        if n <= 200: return "51-200"
        if n <= 1000: return "201-1000"
        return "1000+"

    async def _upsert_company(url: str, data: dict) -> tuple[str, dict] | None:
        raw_ind = data.get("industry")
        raw_hq = data.get("headquarters") or data.get("hq")
        try:
            ec = int(data.get("employeeCount") or data.get("companySize") or 0) or None
        except (TypeError, ValueError):
            ec = None
        ind_res, loc_res = await asyncio.gather(
            industry_resolve(raw_ind, collection=industries_taxonomy_collection),
            geo_resolve(raw_hq, collection=geo_places_collection),
        )
        ind_doc = ({k: ind_res.get(k) for k in ("id", "label", "group", "assign_method", "confidence")} | {"raw": raw_ind}) if ind_res else ({"raw": raw_ind} if raw_ind else None)
        loc_doc = None
        if loc_res:
            loc_doc = {k: loc_res.get(k) for k in ("place_id", "city", "region", "country", "country_code", "continent")}
            loc_doc["raw"] = raw_hq
        elif raw_hq:
            loc_doc = {"raw": raw_hq}
        now = datetime.utcnow()
        set_doc = {k: v for k, v in {
            "linkedin_url": url,
            "name": data.get("name") or data.get("companyName"),
            "domain": data.get("domain"),
            "description": data.get("description") or data.get("about"),
            "employee_count": ec,
            "employee_band": _band(ec),
            "industry": ind_doc,
            "location": loc_doc,
            "linkedin_data": data,
            "last_scraped_at": now,
            "last_updated_at": now,
        }.items() if v is not None}
        set_doc["last_updated_at"] = now
        try:
            res = await companies_collection.update_one(
                {"linkedin_url": url},
                {"$set": set_doc, "$setOnInsert": {"created_at": now, "prospect_count": 0}},
                upsert=True,
            )
            cid = str(res.upserted_id) if res.upserted_id else None
            if not cid:
                d = await companies_collection.find_one({"linkedin_url": url}, {"_id": 1})
                cid = str(d["_id"])
            return cid, set_doc
        except Exception as exc:
            logger.error(f"Company upsert failed for {url}: {exc}")
            return None

    company_url_to_doc: dict[str, tuple[str, dict]] = {}
    upsert_tasks = {url: _upsert_company(url, data) for url, data in all_matched.items()}
    results_list = await asyncio.gather(*upsert_tasks.values(), return_exceptions=True)
    for url, res in zip(upsert_tasks.keys(), results_list):
        if isinstance(res, tuple) and res:
            company_url_to_doc[url] = res  # (company_id, company_set_doc)

    # Denormalize company facts onto prospects + link company_id
    bulk_ops = []
    for prospect in prospects:
        prospect_id = str(prospect["_id"])
        company_data = prospect_id_to_company.get(prospect_id)
        url = prospect.get("company_linkedin")
        company_tuple = company_url_to_doc.get(url) if url else None

        if company_data:
            prospect["company_linkedin_data"] = company_data  # keep in-memory for AI phase

        update_set: dict = {"enrichment_status": "company_scraped", "stage": "company_enriched"}
        if company_tuple:
            company_id, company_doc = company_tuple
            ind = company_doc.get("industry") or {}
            prospect["company_id"] = company_id
            prospect["company_industry_id"] = ind.get("id") if isinstance(ind, dict) else None
            prospect["company_industry_group"] = ind.get("group") if isinstance(ind, dict) else None
            prospect["company_employee_band"] = company_doc.get("employee_band")
            prospect["company_size"] = company_doc.get("employee_count")
            prospect["company_domain"] = company_doc.get("domain") or prospect.get("company_domain")
            update_set.update({
                "company_id": company_id,
                "company_industry_id": prospect["company_industry_id"],
                "company_industry_group": prospect["company_industry_group"],
                "company_employee_band": prospect["company_employee_band"],
                "company_size": prospect["company_size"],
                "company_domain": prospect["company_domain"],
            })
        bulk_ops.append(UpdateOne({"_id": prospect["_id"]}, {"$set": {k: v for k, v in update_set.items() if v is not None}}))

    if bulk_ops:
        await prospects_collection.bulk_write(bulk_ops, ordered=False)

    return prospects, stats


async def _phase_ai_assessment(
    prospects: list[dict],
    stats: dict,
    client: OpenRouterClient,
    company_profile: dict | None = None,
    account_id: str | None = None,
) -> tuple[list[dict], list[dict], dict]:
    """Phase 3: Batch AI assessment — 3 prospects per call using Haiku 4.5.
    Returns (good_prospects, wrong_person_prospects, stats).
    When company_profile is provided the AI assessment is personalised to the
    triggering user's ICP definition.
    """
    BATCH_SIZE = 3
    company_threshold = settings.contact_discovery_company_threshold
    prospect_threshold = settings.contact_discovery_prospect_threshold

    # Build batches of 3; concurrency governed by settings (default 6 for paid Haiku tier)
    batches = [prospects[i:i + BATCH_SIZE] for i in range(0, len(prospects), BATCH_SIZE)]
    batch_semaphore = asyncio.Semaphore(settings.ai_assessment_concurrency_limit)
    assessment_model = settings.mini_enrichment_model  # claude-haiku-4-5 — fast, reliable JSON output

    async def _run_batch(batch: list[dict], batch_index: int) -> list[bool]:
        async with batch_semaphore:
            model = assessment_model
            tuples = [
                (p, p.get("linkedin_profile_data"), p.get("company_linkedin_data"), company_profile)
                for p in batch
            ]
            try:
                assessments = await asyncio.wait_for(
                    batch_assess_leads(tuples, client, model),
                    timeout=150.0,
                )
            except asyncio.TimeoutError:
                logger.error(f"batch_assess_leads timed out for batch {batch_index} (model={model})")
                for prospect in batch:
                    await _update_prospect_status(prospect["_id"], "failed", error="AI assessment: batch timed out")
                    stats["prospects_failed"] += 1
                return [False] * len(batch)
            except Exception as e:
                logger.error(f"batch_assess_leads failed for batch {batch_index} (model={model}): {e}")
                for prospect in batch:
                    await _update_prospect_status(prospect["_id"], "failed", error=f"AI assessment: {e}")
                    stats["prospects_failed"] += 1
                return [False] * len(batch)

            from pymongo import UpdateOne as MongoUpdateOne
            successes = []
            bulk_ops = []

            async def _flush_bulk_ops(ops: list) -> None:
                if not ops:
                    return
                try:
                    result = await prospects_collection.bulk_write(ops, ordered=False)
                    _ = result  # matched/modified counts available if needed
                except Exception as bwe:
                    # Correlate write errors back to prospects and mark them failed
                    write_errors = getattr(bwe, "details", {}).get("writeErrors", [])
                    failed_ids = {str(we.get("keyValue", {}).get("_id", "")) for we in write_errors}
                    if failed_ids:
                        logger.error(f"bulk_write had {len(failed_ids)} write error(s): {bwe}")
                        for we in write_errors:
                            pid = we.get("keyValue", {}).get("_id")
                            if pid:
                                await _update_prospect_status(pid, "failed", error=f"bulk_write error: {we.get('errmsg', '')}")

            for prospect, assessment in zip(batch, assessments):
                if assessment is None:
                    await _update_prospect_status(prospect["_id"], "failed", error="AI assessment: batch returned None")
                    stats["prospects_failed"] += 1
                    successes.append(False)
                    continue

                try:
                    profile = prospect.get("linkedin_profile_data")
                    company = prospect.get("company_linkedin_data")
                    score, breakdown = compute_ai_prospect_score(prospect, profile, company, assessment)

                    prospect["company_fit_score"] = assessment.get("company_fit_score", score)
                    prospect["prospect_fit_score"] = assessment.get("prospect_fit_score", score)
                    prospect["prospect_score"] = score

                    city = prospect.get("city")
                    country_val = prospect.get("country")
                    timezone_str = infer_timezone(city, country_val)
                    optimal_time = calculate_optimal_send_time(timezone_str) if timezone_str else None

                    from utils.scoring import tier_from_score
                    priority_tier = tier_from_score(score)
                    prospect["priority_tier"] = priority_tier
                    prospect["ai_assessment"] = assessment
                    prospect["ai_prospect_score"] = score
                    prospect["ai_score_breakdown"] = breakdown

                    bulk_ops.append(MongoUpdateOne(
                        {"_id": prospect["_id"]},
                        {"$set": {
                            "ai_assessment": assessment,
                            "ai_prospect_score": score,
                            "prospect_score": score,
                            "ai_score_breakdown": breakdown,
                            "company_fit_score": prospect["company_fit_score"],
                            "prospect_fit_score": prospect["prospect_fit_score"],
                            "timezone": timezone_str,
                            "optimal_send_time": optimal_time,
                            "priority_tier": priority_tier,
                            "enrichment_status": "ai_assessed",
                            "stage": "assessed",
                        }},
                    ))

                    # Write scores to prospect_state overlay (account-scoped)
                    if account_id:
                        try:
                            from services.prospect_search_service import ensure_prospect_state
                            from bson import ObjectId as _OID
                            _aid = account_id if isinstance(account_id, str) else str(account_id)
                            _pid = str(prospect["_id"])
                            await ensure_prospect_state(None, account_id=_aid, prospect_id=_pid)
                            await prospect_state_collection.update_one(
                                {"account_id": _aid, "prospect_id": _pid},
                                {"$set": {
                                    "ai_score": score,
                                    "ai_score_breakdown": breakdown,
                                    "prospect_score": score,
                                    "priority_tier": priority_tier,
                                    "last_updated_at": datetime.utcnow(),
                                }},
                            )
                        except Exception as _pse:
                            logger.debug(f"prospect_state overlay write failed: {_pse}")

                    stats["ai_assessments_done"] += 1
                    successes.append(True)

                    # Flush every 10 ops for durability (crash-safety mid-batch)
                    if len(bulk_ops) >= 10:
                        await _flush_bulk_ops(bulk_ops)
                        bulk_ops.clear()

                except Exception as e:
                    logger.error(f"AI assessment post-processing failed for {prospect.get('email', 'unknown')}: {e}", exc_info=True)
                    await _update_prospect_status(prospect["_id"], "failed", error=f"AI assessment: {e}")
                    stats["prospects_failed"] += 1
                    successes.append(False)

            await _flush_bulk_ops(bulk_ops)
            return successes

    batch_results = await asyncio.gather(*[
        _run_batch(batch, i) for i, batch in enumerate(batches)
    ])

    # Flatten results back to per-prospect
    flat_successes = [s for batch_s in batch_results for s in batch_s]

    good_prospects = []
    wrong_person_prospects = []

    for prospect, success in zip(prospects, flat_successes):
        if not success:
            continue
        company_score = prospect.get("company_fit_score", 0)
        prospect_score = prospect.get("prospect_fit_score", 0)

        if company_score >= company_threshold and prospect_score < prospect_threshold:
            wrong_person_prospects.append(prospect)
        else:
            good_prospects.append(prospect)

    logger.info(
        f"AI assessment split: {len(good_prospects)} good, "
        f"{len(wrong_person_prospects)} wrong-person (company good, prospect bad)"
    )

    return good_prospects, wrong_person_prospects, stats


async def _phase_contact_discovery(
    wrong_person_prospects: list[dict],
    stats: dict,
    client: OpenRouterClient,
) -> tuple[list[dict], dict]:
    """Phase 3.5: Discover better contacts at companies where the prospect was wrong person.

    Processes companies CONCURRENTLY for speed.
    """
    from services.employee_scraper_service import _scrape_employees_for_company
    from services.employee_discovery_service import _fallback_ranking
    from services.email_finder_service import find_emails_batch

    stats["contacts_discovered"] = 0
    stats["employees_scraped_for_discovery"] = 0
    stats["emails_found"] = 0

    # 1. Deduplicate by company LinkedIn URL
    company_groups: dict[str, list[dict]] = {}
    for prospect in wrong_person_prospects:
        company_url = prospect.get("company_linkedin") or prospect.get("company_linkedin_uid")
        if company_url:
            company_url = company_url.strip().rstrip("/")
            company_groups.setdefault(company_url, []).append(prospect)
        else:
            logger.warning(f"No company URL for wrong-person prospect {prospect.get('email', 'unknown')}, skipping discovery")

    if not company_groups:
        return [], stats

    logger.info(f"Phase 3.5: Discovering contacts at {len(company_groups)} unique companies")

    # Process companies concurrently with semaphore
    discovery_semaphore = asyncio.Semaphore(3)  # Limit concurrent company discoveries

    async def _discover_at_company(company_url: str, source_prospects: list[dict]) -> list[dict]:
        async with discovery_semaphore:
            company_name = source_prospects[0].get("company_name", "Unknown")
            try:
                # Scrape employees
                logger.info(f"Scraping employees for {company_name} ({company_url})")
                employees = await asyncio.to_thread(
                    _scrape_employees_for_company,
                    company_url,
                    max_items=50,
                )
                stats["employees_scraped_for_discovery"] += len(employees)

                if not employees:
                    logger.warning(f"No employees found for {company_name}")
                    return []

                # AI-rank employees
                ranked_contacts = await _ai_rank_employees(
                    employees, company_name, client
                )

                if not ranked_contacts:
                    return []

                # Create new prospect records — batch dedup then bulk insert
                source_prospect = source_prospects[0]
                new_prospects_for_company = []

                # Dedup: one $in query instead of N find_one calls
                candidate_urls = [c.get("linkedin_url") for c in ranked_contacts if c.get("linkedin_url")]
                existing_urls: set[str] = set()
                if candidate_urls:
                    async for ex in prospects_collection.find(
                        {"linkedin": {"$in": candidate_urls}},
                        {"_id": 0, "linkedin": 1},
                    ):
                        if ex.get("linkedin"):
                            existing_urls.add(ex["linkedin"])

                now_ts = datetime.utcnow()
                docs_to_insert = []
                for contact in ranked_contacts:
                    linkedin_url = contact.get("linkedin_url")
                    if not linkedin_url:
                        continue
                    if linkedin_url in existing_urls:
                        logger.info(f"Prospect already exists for {linkedin_url}, skipping")
                        continue
                    docs_to_insert.append({
                        "full_name": contact.get("full_name"),
                        "job_title": contact.get("title"),
                        "linkedin": linkedin_url,
                        "company_name": company_name,
                        "company_linkedin": company_url,
                        "company_domain": source_prospect.get("company_domain"),
                        "industry": source_prospect.get("industry"),
                        "industry_id": source_prospect.get("industry_id"),
                        "company_size": source_prospect.get("company_size"),
                        "company_annual_revenue": source_prospect.get("company_annual_revenue"),
                        "source": "contact_discovery",
                        "discovered_from_prospect_id": str(source_prospect["_id"]),
                        "discovery_reasoning": contact.get("reasoning"),
                        "tags": ["contact_discovery"],
                        "status": "new",
                        "enrichment_status": "in_progress",
                        "enrichment_started_at": now_ts,
                        "first_seen_at": now_ts,
                        "last_updated_at": now_ts,
                        "company_linkedin_data": source_prospect.get("company_linkedin_data"),
                        "competitors": source_prospect.get("competitors"),
                    })

                if docs_to_insert:
                    try:
                        insert_result = await prospects_collection.insert_many(docs_to_insert, ordered=False)
                        for doc, inserted_id in zip(docs_to_insert, insert_result.inserted_ids):
                            doc["_id"] = inserted_id
                            new_prospects_for_company.append(doc)
                        stats["contacts_discovered"] += len(new_prospects_for_company)
                    except Exception as e:
                        logger.warning(f"insert_many failed for contacts at {company_name}: {e}")

                if not new_prospects_for_company:
                    return []

                # Scrape LinkedIn profiles for new prospects
                new_urls = [p["linkedin"] for p in new_prospects_for_company]
                try:
                    _run_id, profile_results = await scrape_linkedin_profiles(new_urls)
                    matched = match_profiles_to_urls(new_urls, profile_results)

                    bulk_ops = []
                    for prospect in new_prospects_for_company:
                        profile_data = matched.get(prospect["linkedin"])
                        if profile_data:
                            prospect["linkedin_profile_data"] = profile_data
                            bulk_ops.append(UpdateOne(
                                {"_id": prospect["_id"]},
                                {"$set": {"linkedin_profile_data": profile_data}}
                            ))
                    if bulk_ops:
                        await prospects_collection.bulk_write(bulk_ops, ordered=False)
                except Exception as e:
                    logger.error(f"Profile scrape failed for discovered contacts at {company_name}: {e}")

                # Find emails for new prospects
                try:
                    email_results = await find_emails_batch(new_urls)
                    bulk_ops = []
                    for prospect in new_prospects_for_company:
                        email_data = email_results.get(prospect["linkedin"])
                        if email_data:
                            email = email_data.get("email") or email_data.get("emailAddress")
                            if email:
                                prospect["email"] = email
                                bulk_ops.append(UpdateOne(
                                    {"_id": prospect["_id"]},
                                    {"$set": {"email": email, "email_source": "apify_email_finder"}}
                                ))
                                stats["emails_found"] += 1
                    if bulk_ops:
                        await prospects_collection.bulk_write(bulk_ops, ordered=False)
                except Exception as e:
                    logger.error(f"Email finder failed for discovered contacts at {company_name}: {e}")

                # Run AI assessment on new prospects
                assessed_prospects, _, stats_update = await _phase_ai_assessment(
                    new_prospects_for_company, stats, client
                )

                return assessed_prospects

            except Exception as e:
                logger.error(f"Contact discovery failed for {company_name}: {e}", exc_info=True)
                return []

    # Run all company discoveries concurrently
    company_results = await asyncio.gather(*[
        _discover_at_company(url, prospects)
        for url, prospects in company_groups.items()
    ])

    all_new_prospects = []
    for result in company_results:
        all_new_prospects.extend(result)

    # Mark original wrong-person prospects as "replaced" via bulk write
    bulk_ops = [
        UpdateOne(
            {"_id": prospect["_id"]},
            {"$set": {
                "enrichment_status": "replaced",
                "replaced_reason": "good_company_wrong_person",
                "replaced_at": datetime.utcnow(),
            }}
        )
        for prospect in wrong_person_prospects
    ]
    if bulk_ops:
        try:
            await prospects_collection.bulk_write(bulk_ops, ordered=False)
        except Exception as e:
            logger.warning(f"Failed to mark prospects as replaced: {e}")

    logger.info(
        f"Phase 3.5 complete: {stats['contacts_discovered']} contacts discovered, "
        f"{stats['emails_found']} emails found, "
        f"{len(all_new_prospects)} prospects ready for outreach"
    )

    return all_new_prospects, stats


async def _phase_pre_enrichment_triage(
    prospects: list[dict],
    stats: dict,
    enrichment_run_id: str,
) -> tuple[list[dict], dict]:
    """Phase 0.5: Pre-enrichment triage using rule-based checks (no API calls).

    Categorizes prospects before expensive scraping:
    - Good company + decision maker → proceed to enrichment
    - Good company + NOT decision maker → discover better contacts, then proceed
    - Poor company + decision maker → proceed (let AI assess later)
    - Poor company + NOT decision maker → skip entirely
    """
    from services.employee_scraper_service import _scrape_employees_for_company
    from services.employee_discovery_service import _fallback_ranking

    threshold = settings.pre_enrichment_company_fit_threshold
    proceed_list = []
    discovery_candidates: dict[str, list[dict]] = {}  # company_url -> [prospects]

    # Triage all prospects and collect bulk DB updates
    bulk_ops = []
    for prospect in prospects:
        company_score, company_breakdown = score_company_fit_rule_based(prospect)
        is_dm, dm_reasoning = is_decision_maker_rule_based(prospect)
        good_company = company_score >= threshold

        triage_data = {
            "pre_enrichment_triage": {
                "company_score": company_score,
                "company_breakdown": company_breakdown,
                "is_decision_maker": is_dm,
                "dm_reasoning": dm_reasoning,
                "good_company": good_company,
                "threshold": threshold,
                "triaged_at": datetime.utcnow(),
            }
        }

        if good_company and is_dm:
            triage_data["pre_enrichment_triage"]["action"] = "proceed"
            bulk_ops.append(UpdateOne(
                {"_id": prospect["_id"]}, {"$set": triage_data}
            ))
            proceed_list.append(prospect)
            stats["triage_decision_makers"] += 1

        elif good_company and not is_dm:
            triage_data["pre_enrichment_triage"]["action"] = "discover"
            triage_data["enrichment_status"] = "triage_skipped"
            triage_data["triage_skip_reason"] = "good_company_not_decision_maker"
            bulk_ops.append(UpdateOne(
                {"_id": prospect["_id"]}, {"$set": triage_data}
            ))
            stats["triage_wrong_person"] += 1

            company_url = prospect.get("company_linkedin") or prospect.get("company_linkedin_uid")
            if company_url:
                company_url = company_url.strip().rstrip("/")
                discovery_candidates.setdefault(company_url, []).append(prospect)

        elif not good_company and is_dm:
            triage_data["pre_enrichment_triage"]["action"] = "proceed_low_company"
            bulk_ops.append(UpdateOne(
                {"_id": prospect["_id"]}, {"$set": triage_data}
            ))
            proceed_list.append(prospect)
            stats["triage_decision_makers"] += 1

        else:
            triage_data["pre_enrichment_triage"]["action"] = "skip"
            triage_data["enrichment_status"] = "triage_skipped"
            triage_data["triage_skip_reason"] = "poor_company_not_decision_maker"
            bulk_ops.append(UpdateOne(
                {"_id": prospect["_id"]}, {"$set": triage_data}
            ))
            stats["triage_wrong_person"] += 1
            stats["prospects_skipped"] += 1

    # Single bulk write for all triage updates
    if bulk_ops:
        await prospects_collection.bulk_write(bulk_ops, ordered=False)

    if not discovery_candidates:
        logger.info(
            f"Phase 0.5 triage: {stats['triage_decision_makers']} decision-makers, "
            f"{stats['triage_wrong_person']} filtered out, 0 companies to discover"
        )
        return proceed_list, stats

    # Discover better contacts at good companies (CONCURRENT)
    stats["triage_companies_to_discover"] = len(discovery_candidates)
    logger.info(f"Phase 0.5: Discovering contacts at {len(discovery_candidates)} companies")

    openrouter_client = None
    discovery_semaphore = asyncio.Semaphore(3)

    try:
        openrouter_client = OpenRouterClient()

        async def _discover_at_company(company_url: str, source_prospects: list[dict]) -> list[dict]:
            async with discovery_semaphore:
                company_name = source_prospects[0].get("company_name", "Unknown")
                try:
                    employees = await asyncio.to_thread(
                        _scrape_employees_for_company,
                        company_url,
                        max_items=50,
                    )

                    if not employees:
                        logger.warning(f"Phase 0.5: No employees found for {company_name}")
                        return []

                    # AI-rank employees using shared helper
                    ranked_contacts = await _ai_rank_employees(
                        employees, company_name, openrouter_client
                    )

                    if not ranked_contacts:
                        return []

                    # Create new prospect records
                    source_prospect = source_prospects[0]
                    new_prospects = []

                    for contact in ranked_contacts:
                        linkedin_url = contact.get("linkedin_url")
                        if not linkedin_url:
                            continue

                        existing = await prospects_collection.find_one({"linkedin": linkedin_url})
                        if existing:
                            logger.info(f"Phase 0.5: Prospect already exists for {linkedin_url}, skipping")
                            continue

                        prospect_data = {
                            "full_name": contact.get("full_name"),
                            "job_title": contact.get("title"),
                            "linkedin": linkedin_url,
                            "company_name": company_name,
                            "company_linkedin": company_url,
                            "company_domain": source_prospect.get("company_domain"),
                            "industry": source_prospect.get("industry"),
                            "industry_id": source_prospect.get("industry_id"),
                            "company_size": source_prospect.get("company_size"),
                            "company_annual_revenue": source_prospect.get("company_annual_revenue"),
                            "company_annual_revenue_clean": source_prospect.get("company_annual_revenue_clean"),
                            "city": source_prospect.get("city"),
                            "country": source_prospect.get("country"),
                            "source": "pre_enrichment_discovery",
                            "discovered_from_prospect_id": str(source_prospect["_id"]),
                            "discovery_reasoning": contact.get("reasoning"),
                            "tags": ["pre_enrichment_discovery"],
                            "status": "new",
                            "enrichment_status": "in_progress",
                            "enrichment_started_at": datetime.utcnow(),
                            "enrichment_run_id": enrichment_run_id,
                            "first_seen_at": datetime.utcnow(),
                            "last_updated_at": datetime.utcnow(),
                        }

                        try:
                            result = await prospects_collection.insert_one(prospect_data)
                            prospect_data["_id"] = result.inserted_id
                            new_prospects.append(prospect_data)
                            stats["triage_contacts_discovered"] += 1
                        except Exception as e:
                            logger.warning(f"Phase 0.5: Failed to create prospect for {linkedin_url}: {e}")

                    return new_prospects

                except Exception as e:
                    logger.error(f"Phase 0.5: Discovery failed for {company_name}: {e}", exc_info=True)
                    return []

        # Run all company discoveries concurrently
        company_results = await asyncio.gather(*[
            _discover_at_company(url, prospects)
            for url, prospects in discovery_candidates.items()
        ])

        for result in company_results:
            proceed_list.extend(result)

    finally:
        if openrouter_client:
            await openrouter_client.close()

    logger.info(
        f"Phase 0.5 triage complete: {stats['triage_decision_makers']} decision-makers proceed, "
        f"{stats['triage_wrong_person']} filtered, "
        f"{stats['triage_contacts_discovered']} new contacts discovered"
    )

    return proceed_list, stats


# ── Shared Helpers ──


async def _ai_rank_employees(
    employees: list[dict],
    company_name: str,
    client: OpenRouterClient,
    top_n: int = 3,
) -> list[dict]:
    """Shared helper: Format employees for AI and rank top N contacts.

    Used by both Phase 0.5 (pre-enrichment triage) and Phase 3.5 (contact discovery).
    Returns list of ranked contacts with full_name, title, linkedin_url, reasoning.
    """
    from services.employee_discovery_service import _fallback_ranking
    from utils.prompts import get_system_prompt

    # Format employee list for AI
    employee_list_str = ""
    for i, emp in enumerate(employees, 1):
        name = f"{emp.get('firstName', '')} {emp.get('lastName', '')}".strip()
        positions = emp.get("currentPositions") or []
        title = positions[0].get("title", "Unknown") if positions else "Unknown"
        linkedin_url = emp.get("linkedinUrl", "N/A")
        location_data = emp.get("location", {})
        location = location_data.get("linkedinText", "Unknown") if isinstance(location_data, dict) else "Unknown"
        employee_list_str += f"{i}. {name} | {title} | {location} | {linkedin_url}\n"

    user_prompt = (
        f"## Company: {company_name}\n\n"
        f"## Employees ({len(employees)} found)\n\n"
        f"{employee_list_str}\n\n"
        f"Rank the top {top_n} contacts for our outreach. Return as JSON."
    )

    try:
        response = await client.chat_completion(
            messages=[
                {"role": "system", "content": await get_system_prompt("employee_ranking")},
                {"role": "user", "content": user_prompt},
            ],
            model=settings.claude_model,
            temperature=0.3,
            max_tokens=2048,
            response_format={"type": "json_object"},
        )
        return response.get("ranked_contacts", [])[:top_n]
    except Exception as e:
        logger.error(f"AI ranking failed for {company_name}: {e}")
        return _fallback_ranking(
            [{"full_name": f"{emp.get('firstName', '')} {emp.get('lastName', '')}".strip(),
              "linkedin_url": emp.get("linkedinUrl"),
              "current_position": {"title": (emp.get("currentPositions") or [{}])[0].get("title", "Unknown") if emp.get("currentPositions") else "Unknown"}}
             for emp in employees],
            top_n
        )


async def _update_run(run_oid: ObjectId, update_data: dict):
    """Update enrichment run document."""
    await enrichment_runs_collection.update_one(
        {"_id": run_oid},
        {"$set": update_data}
    )


async def _update_prospect_status(prospect_oid: ObjectId, status: str, error: str | None = None):
    """Update a prospect's enrichment status."""
    update = {"enrichment_status": status}
    if error:
        update["enrichment_error"] = error
    await prospects_collection.update_one({"_id": prospect_oid}, {"$set": update})


async def _phase_competitors(prospects: list[dict], stats: dict) -> tuple[list[dict], dict]:
    """Phase 2.5: Research competitors for prospect companies.

    Uses a shared OpenRouterClient to avoid creating a new HTTP client per call.
    """
    from services.competitor_research_service import research_competitors

    stats["competitors_researched"] = 0

    semaphore = asyncio.Semaphore(3)
    shared_client = OpenRouterClient()

    try:
        async def _research_one(prospect: dict):
            async with semaphore:
                try:
                    company_name = prospect.get("company_name")
                    industry = prospect.get("industry")

                    if not company_name:
                        return

                    # Resolve the company doc for dedup check + storage
                    company_linkedin = prospect.get("company_linkedin")
                    comp_doc = None
                    if company_linkedin:
                        comp_doc = await companies_collection.find_one({"linkedin_url": company_linkedin})

                    # Reuse fresh competitors from company doc if available (< 30 days old)
                    if comp_doc and comp_doc.get("competitors"):
                        fetched_at = comp_doc.get("competitors_last_fetched")
                        if fetched_at and (datetime.utcnow() - fetched_at).days < 30:
                            prospect["competitors"] = comp_doc["competitors"]
                            logger.info(f"Reusing cached competitors for {company_name} from company doc")
                            return

                    # Website fallback chain: prospect.company_website → company_domain → company doc
                    website = (
                        prospect.get("company_website")
                        or prospect.get("company_domain")
                    )
                    if not website and comp_doc:
                        website = comp_doc.get("website") or comp_doc.get("domain")

                    competitors = await research_competitors(
                        company_name, website, industry, limit=3, client=shared_client
                    )

                    # Store competitors on the company doc (one Perplexity call per company)
                    if comp_doc and competitors:
                        await companies_collection.update_one(
                            {"_id": comp_doc["_id"]},
                            {"$set": {
                                "competitors": competitors,
                                "competitors_last_fetched": datetime.utcnow(),
                            }}
                        )

                    prospect["competitors"] = competitors

                    if competitors:
                        stats["competitors_researched"] += 1

                    logger.info(f"Competitor research completed for {company_name}: {len(competitors)} competitors")

                except Exception as e:
                    logger.error(f"Competitor research failed for prospect {prospect.get('email')}: {e}", exc_info=True)

        await asyncio.gather(*[_research_one(p) for p in prospects])
    finally:
        await shared_client.close()

    return prospects, stats


async def _phase_news(prospects: list[dict], stats: dict) -> tuple[list[dict], dict]:
    """Phase 2.6: Research recent company news using Perplexity Sonar Pro.

    Runs in parallel with _phase_competitors — both depend only on company_name.
    Skips prospects whose news was fetched within 14 days.
    Persists to prospects.company_news + news_last_fetched.
    """
    from services.news_research_service import research_company_news

    stats["news_researched"] = 0
    semaphore = asyncio.Semaphore(3)
    shared_client = OpenRouterClient()

    try:
        async def _research_one(prospect: dict):
            async with semaphore:
                try:
                    company_name = prospect.get("company_name")
                    if not company_name:
                        return

                    # Skip if news fetched within 14 days
                    news_last = prospect.get("news_last_fetched")
                    if news_last:
                        if isinstance(news_last, str):
                            from dateutil import parser as _dp
                            news_last = _dp.parse(news_last)
                        age_days = (datetime.utcnow() - news_last).days
                        if age_days < 14:
                            logger.info(f"Reusing cached news for {company_name} (fetched {age_days}d ago)")
                            return

                    news = await research_company_news(company_name, limit=5, days_back=90, client=shared_client)

                    if news:
                        prospect["company_news"] = news
                        await prospects_collection.update_one(
                            {"_id": prospect["_id"]},
                            {"$set": {
                                "company_news": news,
                                "news_last_fetched": datetime.utcnow(),
                            }}
                        )
                        stats["news_researched"] += 1
                        logger.info(f"News research: {len(news)} items for {company_name}")

                except Exception as e:
                    logger.error(f"News research failed for {prospect.get('email')}: {e}", exc_info=True)

        await asyncio.gather(*[_research_one(p) for p in prospects])
    finally:
        await shared_client.close()

    return prospects, stats
