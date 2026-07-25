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
from datetime import datetime, timedelta
from bson import ObjectId

from pymongo import UpdateOne

from config import get_settings
from database import (
    prospects_collection,
    enrichment_runs_collection,
    companies_collection,
    campaigns_collection,
    prospect_state_collection,
    campaign_prospect_state_collection,
    campaign_enrollments_collection,
    geo_places_collection,
    industries_taxonomy_collection,
)
from services.openrouter_service import OpenRouterClient
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

# How long a company-level research claim (competitors_fetching_at /
# news_fetching_at) is honored before it's treated as abandoned (e.g. the
# worker holding it crashed) and re-claimable.
_CLAIM_STALE_AFTER = timedelta(minutes=5)


async def run_enrichment_pipeline(
    enrichment_run_id: str,
    prospect_ids: list[str],
    options: dict,
    triggered_by: str | None = None,
    *,
    account_id: str,
    campaign_id: str | None = None,
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
    account_id = str(account_id)
    campaign_id = str(campaign_id) if campaign_id else None
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
    _account_id_for_profile = account_id

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
        owned_run = await enrichment_runs_collection.find_one(
            {"_id": run_oid, "account_id": {"$in": [account_id, ObjectId(account_id)]}},
            {"_id": 1},
        )
        if owned_run is None:
            raise PermissionError("enrichment run does not belong to account")
        await _update_run(run_oid, account_id, {
            "status": "running", "current_step": "setup",
            "started_at": datetime.utcnow(),
        })

        lead_oids = [ObjectId(lid) for lid in prospect_ids]
        state_ids: set[str] = set()
        if campaign_id:
            state_cursor = campaign_prospect_state_collection.find(
                {
                    "account_id": account_id,
                    "campaign_id": campaign_id,
                    "prospect_id": {"$in": [str(value) for value in lead_oids]},
                },
                {"prospect_id": 1},
            )
        else:
            state_cursor = prospect_state_collection.find(
                {
                    "account_id": {"$in": [account_id, ObjectId(account_id)]},
                    "prospect_id": {"$in": [str(value) for value in lead_oids] + lead_oids},
                },
                {"prospect_id": 1},
            )
        state_ids.update([str(doc["prospect_id"]) async for doc in state_cursor])
        enrollment_query = {
            "account_id": {"$in": [account_id, ObjectId(account_id)]},
            "prospect_id": {"$in": [str(value) for value in lead_oids] + lead_oids},
        }
        if campaign_id:
            enrollment_query["campaign_id"] = {"$in": [campaign_id, ObjectId(campaign_id)]}
        enrollment_cursor = campaign_enrollments_collection.find(enrollment_query, {"prospect_id": 1})
        async for doc in enrollment_cursor:
            state_ids.add(str(doc["prospect_id"]))
        unauthorized = [str(value) for value in lead_oids if str(value) not in state_ids]
        if unauthorized:
            raise PermissionError("one or more prospects are outside the enrichment tenant scope")
        scoring_version = None
        if campaign_id:
            from services.campaign_prospect_state_service import DEFAULT_SCORING_VERSION
            _campaign_for_scoring = await campaigns_collection.find_one(
                {"_id": ObjectId(str(campaign_id))}, {"scoring_version": 1}
            )
            scoring_version = (_campaign_for_scoring or {}).get("scoring_version") or DEFAULT_SCORING_VERSION
            for prospect_oid in lead_oids:
                await campaign_prospect_state_collection.update_one(
                    {
                        "account_id": account_id,
                        "campaign_id": campaign_id,
                        "prospect_id": str(prospect_oid),
                        "scoring_version": scoring_version,
                    },
                    {
                        "$setOnInsert": {
                            "account_id": account_id,
                            "campaign_id": campaign_id,
                            "prospect_id": str(prospect_oid),
                            "scoring_version": scoring_version,
                            "score": {"value": None, "version": scoring_version},
                            "enrichment": {"state": "queued", "attempt": 0, "queued_at": datetime.utcnow()},
                            "created_at": datetime.utcnow(),
                            "updated_at": datetime.utcnow(),
                        }
                    },
                    upsert=True,
                )
        prospects = await prospects_collection.find({"_id": {"$in": lead_oids}}).to_list(len(lead_oids))
        if len(prospects) != len(set(lead_oids)):
            raise LookupError("one or more prospects no longer exist")

        # Filter out prospects without LinkedIn URL. Runtime/progress belongs
        # to the tenant/campaign overlay, never the shared person document.
        valid_leads = []
        for prospect in prospects:
            if not prospect.get("linkedin"):
                await _update_prospect_status(
                    account_id, campaign_id, prospect["_id"], "skipped",
                    error="No LinkedIn URL", scoring_version=scoring_version,
                )
                stats["prospects_skipped"] += 1
            else:
                valid_leads.append(prospect)
                await _update_prospect_status(
                    account_id, campaign_id, prospect["_id"], "in_progress",
                    run_id=enrichment_run_id, scoring_version=scoring_version,
                )

        await _update_run(run_oid, account_id, {
            "total_prospects": len(prospects),
            "prospects_skipped": stats["prospects_skipped"],
        })

        if not valid_leads:
            await _update_run(run_oid, account_id, {
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
        #         valid_leads, stats, enrichment_run_id,
        #         account_id=account_id, campaign_id=campaign_id,
        #     )
        #     await _update_run(run_oid, {
        #         "triage_decision_makers": stats["triage_decision_makers"],
        #         "triage_wrong_person": stats["triage_wrong_person"],
        #         "triage_contacts_discovered": stats.get("triage_contacts_discovered", 0),
        #     })

        # ── Phase 1: LinkedIn Profile Scrape (RETIRED) ──
        # The Apify PROFILE_SCRAPER actor was removed. Prospects proceed to
        # company scraping + AI assessment without per-person profile data.
        if not skip_profile:
            logger.info("Phase 1 profile scrape skipped — profile scraper actor retired")

        # ── Phase 2: Company Scrape with Dedup ──
        elapsed = (datetime.utcnow() - pipeline_start).total_seconds()
        if elapsed > 1200:
            logger.error(f"Enrichment pipeline exceeded 20-minute wall-clock limit after {elapsed:.0f}s, aborting before Phase 2")
            await _update_run(run_oid, account_id, {
                "status": "failed",
                "current_step": "failed",
                "error": f"Pipeline exceeded 20-minute time limit after {int(elapsed)}s",
                "completed_at": datetime.utcnow(),
                **stats,
            })
            return stats

        if not skip_company:
            await _update_run(run_oid, account_id, {"current_step": "scraping_companies"})
            valid_leads, stats = await _phase_company_scrape(valid_leads, stats)
            for prospect in valid_leads:
                await _update_prospect_status(
                    account_id, campaign_id, prospect["_id"], "company_scraped",
                    scoring_version=scoring_version,
                )
            await _update_run(run_oid, account_id, {
                "companies_scraped": stats["companies_scraped"],
                "companies_deduplicated": stats["companies_deduplicated"],
            })

        # ── Phase 2.5 + 2.6 + Phase 3: Competitors + News + AI Assessment (PARALLEL) ──
        # Competitors and news both depend only on company_name; AI assessment runs concurrently.
        wrong_person_prospects = []

        if not skip_competitors and not skip_ai:
            openrouter_client = OpenRouterClient()
            await _update_run(run_oid, account_id, {"current_step": "ai_assessment_and_competitors"})

            # Run all three in parallel
            competitors_task = asyncio.create_task(
                _phase_competitors(valid_leads, stats)
            )
            news_task = asyncio.create_task(
                _phase_news(valid_leads, stats)
            )
            assessment_task = asyncio.create_task(
                _phase_ai_assessment(valid_leads, stats, openrouter_client, company_profile=company_profile, account_id=account_id, campaign_id=campaign_id)
            )

            # Wait for all to complete
            (valid_leads_from_competitors, stats), (valid_leads_from_news, stats), (valid_leads_from_ai, wrong_person_prospects, stats) = (
                await asyncio.gather(competitors_task, news_task, assessment_task)
            )

            # Merge competitor/news data into AI-assessed prospects (both update in-place and in DB)
            valid_leads = valid_leads_from_ai

            await _update_run(run_oid, account_id, {
                "competitors_researched": stats.get("competitors_researched", 0),
                "news_researched": stats.get("news_researched", 0),
                "ai_assessments_done": stats["ai_assessments_done"],
                "prospects_failed": stats["prospects_failed"],
                "wrong_person_prospects": len(wrong_person_prospects),
            })

        elif not skip_competitors:
            await _update_run(run_oid, account_id, {"current_step": "researching_competitors"})
            competitors_task = asyncio.create_task(_phase_competitors(valid_leads, stats))
            news_task = asyncio.create_task(_phase_news(valid_leads, stats))
            (valid_leads, stats), (_, stats) = await asyncio.gather(competitors_task, news_task)
            await _update_run(run_oid, account_id, {
                "competitors_researched": stats.get("competitors_researched", 0),
                "news_researched": stats.get("news_researched", 0),
            })

        elif not skip_ai:
            openrouter_client = OpenRouterClient()
            await _update_run(run_oid, account_id, {"current_step": "ai_assessment"})
            valid_leads, wrong_person_prospects, stats = await _phase_ai_assessment(valid_leads, stats, openrouter_client, company_profile=company_profile, account_id=account_id, campaign_id=campaign_id)
            await _update_run(run_oid, account_id, {
                "ai_assessments_done": stats["ai_assessments_done"],
                "prospects_failed": stats["prospects_failed"],
                "wrong_person_prospects": len(wrong_person_prospects),
            })

        # ── Phase 3.5: Contact Discovery for "wrong person" prospects ──
        if wrong_person_prospects:
            if not openrouter_client:
                openrouter_client = OpenRouterClient()
            await _update_run(run_oid, account_id, {"current_step": "contact_discovery"})
            discovered_prospects, stats = await _phase_contact_discovery(
                wrong_person_prospects, stats, openrouter_client,
                account_id=account_id, campaign_id=campaign_id,
            )
            valid_leads.extend(discovered_prospects)
            await _update_run(run_oid, account_id, {
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

        for prospect in valid_leads:
            await _update_prospect_status(
                account_id, campaign_id, prospect["_id"], "completed",
                scoring_version=scoring_version,
            )

        # ── Phase 5: Finalize ──
        stats["prospects_processed"] = len(valid_leads)

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

        await _update_run(run_oid, account_id, {
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
        await _update_run(run_oid, account_id, {
            "status": "failed",
            "current_step": "failed",
            "error": str(e),
            "completed_at": datetime.utcnow(),
            **stats,
        })
        raise

    finally:
        if openrouter_client:
            await openrouter_client.close()


# ── Phase Implementations ──


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
                _run_id, results = await scrape_company_pages(batch_urls)
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
                {"$set": set_doc, "$setOnInsert": {"created_at": now}},
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

        update_set: dict = {}
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
    campaign_id: str | None = None,
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

    # Load the campaign doc once so the assessment prompt gets Campaign Context
    # (name/description/value_prop/pain_point/ICP targeting). Fail-soft: any
    # load failure just means assessment proceeds without campaign context.
    campaign_doc: dict | None = None
    if campaign_id:
        try:
            campaign_doc = await campaigns_collection.find_one(
                {"_id": ObjectId(str(campaign_id))},
                {
                    "name": 1,
                    "description": 1,
                    "value_proposition": 1,
                    "pain_point": 1,
                    "icp_job_titles": 1,
                    "icp_seniority_levels": 1,
                    "icp_functional_departments": 1,
                    "account_id": 1,
                    "scoring_version": 1,
                },
            )
        except Exception as e:
            logger.warning(f"AI assessment: failed to load campaign {campaign_id} for prompt context: {e}")
            campaign_doc = None

    from services.campaign_prospect_state_service import DEFAULT_SCORING_VERSION
    pipeline_scoring_version = (campaign_doc or {}).get("scoring_version") or DEFAULT_SCORING_VERSION

    async def _run_batch(batch: list[dict], batch_index: int) -> list[bool]:
        async with batch_semaphore:
            model = assessment_model
            tuples = [
                (p, p.get("linkedin_profile_data"), p.get("company_linkedin_data"), company_profile)
                for p in batch
            ]
            try:
                assessments = await asyncio.wait_for(
                    batch_assess_leads(tuples, client, model, campaign=campaign_doc),
                    timeout=150.0,
                )
            except asyncio.TimeoutError:
                logger.error(f"batch_assess_leads timed out for batch {batch_index} (model={model})")
                for prospect in batch:
                    await _update_prospect_status(account_id, campaign_id, prospect["_id"], "failed", error="AI assessment: batch timed out", scoring_version=pipeline_scoring_version)
                    stats["prospects_failed"] += 1
                return [False] * len(batch)
            except Exception as e:
                logger.error(f"batch_assess_leads failed for batch {batch_index} (model={model}): {e}")
                for prospect in batch:
                    await _update_prospect_status(account_id, campaign_id, prospect["_id"], "failed", error=f"AI assessment: {e}", scoring_version=pipeline_scoring_version)
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
                                await _update_prospect_status(account_id, campaign_id, pid, "failed", error=f"bulk_write error: {we.get('errmsg', '')}", scoring_version=pipeline_scoring_version)

            for prospect, assessment in zip(batch, assessments):
                if assessment is None:
                    await _update_prospect_status(account_id, campaign_id, prospect["_id"], "failed", error="AI assessment: batch returned None", scoring_version=pipeline_scoring_version)
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
                            # Inferred timezone is a neutral person fact. Fit,
                            # optimal timing, assessment and workflow state are
                            # persisted only on tenant/campaign state below.
                            "timezone": timezone_str,
                        }},
                    ))

                    # Write all tenant-relative assessment to the proper
                    # campaign or account overlay.
                    if account_id:
                        try:
                            _aid = account_id if isinstance(account_id, str) else str(account_id)
                            _pid = str(prospect["_id"])
                            if campaign_id:
                                from services.campaign_prospect_state_service import persist_campaign_score
                                await persist_campaign_score(
                                    account_id=_aid, campaign_id=campaign_id,
                                    prospect_id=_pid,
                                    scoring_version=pipeline_scoring_version,
                                    result={
                                        "fit_score": score,
                                        "company_fit_score": prospect["company_fit_score"],
                                        "prospect_fit_score": prospect["prospect_fit_score"],
                                        "priority_tier": priority_tier,
                                        "reasoning": assessment.get("reasoning"),
                                        "breakdown": breakdown,
                                    },
                                )
                                await campaign_prospect_state_collection.update_one(
                                    {
                                        "account_id": _aid, "campaign_id": campaign_id, "prospect_id": _pid,
                                        "scoring_version": pipeline_scoring_version,
                                    },
                                    {"$set": {
                                        "ai_assessment": assessment,
                                        "optimal_send_time": optimal_time,
                                        "updated_at": datetime.utcnow(),
                                    }},
                                )
                            else:
                                await prospect_state_collection.update_one(
                                    {"account_id": _aid, "prospect_id": _pid},
                                    {"$set": {
                                        "ai_score": score, "prospect_score": score,
                                        "ai_score_breakdown": breakdown,
                                        "ai_assessment": assessment,
                                        "priority_tier": priority_tier,
                                        "optimal_send_time": optimal_time,
                                        "last_updated_at": datetime.utcnow(),
                                    }},
                                    upsert=False,
                                )
                        except Exception as _pse:
                            logger.error("tenant assessment write failed for %s: %s", _pid, _pse)
                            raise

                    stats["ai_assessments_done"] += 1
                    successes.append(True)

                    # Flush every 10 ops for durability (crash-safety mid-batch)
                    if len(bulk_ops) >= 10:
                        await _flush_bulk_ops(bulk_ops)
                        bulk_ops.clear()

                except Exception as e:
                    logger.error(f"AI assessment post-processing failed for {prospect.get('email', 'unknown')}: {e}", exc_info=True)
                    await _update_prospect_status(account_id, campaign_id, prospect["_id"], "failed", error=f"AI assessment: {e}", scoring_version=pipeline_scoring_version)
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
    *,
    account_id: str,
    campaign_id: str | None,
) -> tuple[list[dict], dict]:
    """Phase 3.5: Discover better contacts at companies where the prospect was wrong person.

    Processes companies CONCURRENTLY for speed.
    """
    from services.employee_scraper_service import _scrape_employees_for_company
    from services.email_finder_service import find_emails, EmailLookupEntry, _split_full_name
    from services.campaign_prospect_state_service import DEFAULT_SCORING_VERSION

    stats["contacts_discovered"] = 0
    stats["employees_scraped_for_discovery"] = 0
    stats["emails_found"] = 0

    pipeline_scoring_version = DEFAULT_SCORING_VERSION
    if campaign_id:
        _campaign_for_discovery = await campaigns_collection.find_one(
            {"_id": ObjectId(str(campaign_id))}, {"scoring_version": 1}
        )
        pipeline_scoring_version = (_campaign_for_discovery or {}).get("scoring_version") or DEFAULT_SCORING_VERSION

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
                            if campaign_id:
                                await campaign_prospect_state_collection.update_one(
                                    {
                                        "account_id": account_id, "campaign_id": campaign_id,
                                        "prospect_id": str(inserted_id),
                                        "scoring_version": pipeline_scoring_version,
                                    },
                                    {"$setOnInsert": {
                                        "account_id": account_id, "campaign_id": campaign_id,
                                        "prospect_id": str(inserted_id),
                                        "scoring_version": pipeline_scoring_version,
                                        "score": {"value": None, "version": pipeline_scoring_version},
                                        "enrichment": {"state": "running", "attempt": 1, "started_at": now_ts},
                                        "created_at": now_ts, "updated_at": now_ts,
                                    }},
                                    upsert=True,
                                )
                            else:
                                await prospect_state_collection.update_one(
                                    {"account_id": account_id, "prospect_id": str(inserted_id)},
                                    {"$setOnInsert": {
                                        "account_id": account_id, "prospect_id": str(inserted_id),
                                        "status": "new", "tags": ["contact_discovery"],
                                        "enrichment_status": "in_progress",
                                        "enrichment_started_at": now_ts,
                                        "created_at": now_ts, "last_updated_at": now_ts,
                                    }},
                                    upsert=True,
                                )
                            new_prospects_for_company.append(doc)
                        stats["contacts_discovered"] += len(new_prospects_for_company)
                    except Exception as e:
                        logger.warning(f"insert_many failed for contacts at {company_name}: {e}")

                if not new_prospects_for_company:
                    return []

                # NOTE: per-person profile scraping for discovered contacts was retired
                # with the Apify PROFILE_SCRAPER actor — contacts are assessed from
                # employee-scrape + company data only.

                # Find emails for new prospects
                try:
                    email_entries = []
                    for prospect in new_prospects_for_company:
                        first, last = _split_full_name(prospect.get("full_name") or "")
                        domain = prospect.get("company_domain") or source_prospect.get("company_domain")
                        if first and last and domain and prospect.get("linkedin"):
                            email_entries.append(EmailLookupEntry(
                                first_name=first, last_name=last, domain=domain,
                                key=prospect["linkedin"],
                            ))
                    if email_entries:
                        email_results = await find_emails(email_entries, account_id=account_id)
                        bulk_ops = []
                        for prospect in new_prospects_for_company:
                            email = email_results.get(prospect["linkedin"])
                            if email:
                                prospect["email"] = email
                                bulk_ops.append(UpdateOne(
                                    {"_id": prospect["_id"]},
                                    {"$set": {"email": email, "email_source": "growthtoolkit"}}
                                ))
                                stats["emails_found"] += 1
                        if bulk_ops:
                            await prospects_collection.bulk_write(bulk_ops, ordered=False)
                except Exception as e:
                    logger.error(f"Email finder failed for discovered contacts at {company_name}: {e}")

                # Run AI assessment on new prospects
                assessed_prospects, _, stats_update = await _phase_ai_assessment(
                    new_prospects_for_company, stats, client,
                    account_id=account_id, campaign_id=campaign_id,
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

    for prospect in wrong_person_prospects:
        await _update_prospect_status(
            account_id, campaign_id, prospect["_id"], "replaced",
            scoring_version=pipeline_scoring_version,
        )

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
    *,
    account_id: str,
    campaign_id: str | None = None,
) -> tuple[list[dict], dict]:
    """Phase 0.5: Pre-enrichment triage using rule-based checks (no API calls).

    Categorizes prospects before expensive scraping:
    - Good company + decision maker → proceed to enrichment
    - Good company + NOT decision maker → discover better contacts, then proceed
    - Poor company + decision maker → proceed (let AI assess later)
    - Poor company + NOT decision maker → skip entirely
    """
    from services.employee_scraper_service import _scrape_employees_for_company
    from services.campaign_prospect_state_service import DEFAULT_SCORING_VERSION

    threshold = settings.pre_enrichment_company_fit_threshold
    proceed_list = []
    discovery_candidates: dict[str, list[dict]] = {}  # company_url -> [prospects]

    pipeline_scoring_version = DEFAULT_SCORING_VERSION
    if campaign_id:
        _campaign_for_triage = await campaigns_collection.find_one(
            {"_id": ObjectId(str(campaign_id))}, {"scoring_version": 1}
        )
        pipeline_scoring_version = (_campaign_for_triage or {}).get("scoring_version") or DEFAULT_SCORING_VERSION

    async def _persist_triage(prospect_id, triage: dict) -> None:
        now = datetime.utcnow()
        if campaign_id:
            result = await campaign_prospect_state_collection.update_one(
                {
                    "account_id": str(account_id), "campaign_id": str(campaign_id),
                    "prospect_id": str(prospect_id),
                    "scoring_version": pipeline_scoring_version,
                },
                {"$set": {"pre_enrichment_triage": triage, "updated_at": now}},
            )
        else:
            result = await prospect_state_collection.update_one(
                {"account_id": str(account_id), "prospect_id": str(prospect_id)},
                {"$set": {"pre_enrichment_triage": triage, "last_updated_at": now}},
            )
        if result.matched_count != 1:
            raise PermissionError("triage prospect is outside tenant scope")

    # Triage all prospects and persist decisions only to tenant state.
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
            await _persist_triage(prospect["_id"], triage_data["pre_enrichment_triage"])
            proceed_list.append(prospect)
            stats["triage_decision_makers"] += 1

        elif good_company and not is_dm:
            triage_data["pre_enrichment_triage"]["action"] = "discover"
            triage_data["pre_enrichment_triage"]["skip_reason"] = "good_company_not_decision_maker"
            await _persist_triage(prospect["_id"], triage_data["pre_enrichment_triage"])
            await _update_prospect_status(account_id, campaign_id, prospect["_id"], "skipped", scoring_version=pipeline_scoring_version)
            stats["triage_wrong_person"] += 1

            company_url = prospect.get("company_linkedin") or prospect.get("company_linkedin_uid")
            if company_url:
                company_url = company_url.strip().rstrip("/")
                discovery_candidates.setdefault(company_url, []).append(prospect)

        elif not good_company and is_dm:
            triage_data["pre_enrichment_triage"]["action"] = "proceed_low_company"
            await _persist_triage(prospect["_id"], triage_data["pre_enrichment_triage"])
            proceed_list.append(prospect)
            stats["triage_decision_makers"] += 1

        else:
            triage_data["pre_enrichment_triage"]["action"] = "skip"
            triage_data["pre_enrichment_triage"]["skip_reason"] = "poor_company_not_decision_maker"
            await _persist_triage(prospect["_id"], triage_data["pre_enrichment_triage"])
            await _update_prospect_status(account_id, campaign_id, prospect["_id"], "skipped", scoring_version=pipeline_scoring_version)
            stats["triage_wrong_person"] += 1
            stats["prospects_skipped"] += 1

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
                            "company_size": source_prospect.get("company_size"),
                            "company_annual_revenue": source_prospect.get("company_annual_revenue"),
                            "company_annual_revenue_clean": source_prospect.get("company_annual_revenue_clean"),
                            "city": source_prospect.get("city"),
                            "country": source_prospect.get("country"),
                            "source": "pre_enrichment_discovery",
                            "first_seen_at": datetime.utcnow(),
                            "last_updated_at": datetime.utcnow(),
                        }

                        try:
                            result = await prospects_collection.insert_one(prospect_data)
                            prospect_data["_id"] = result.inserted_id
                            if campaign_id:
                                await campaign_prospect_state_collection.update_one(
                                    {
                                        "account_id": str(account_id), "campaign_id": str(campaign_id),
                                        "prospect_id": str(result.inserted_id),
                                        "scoring_version": pipeline_scoring_version,
                                    },
                                    {"$setOnInsert": {
                                        "account_id": str(account_id), "campaign_id": str(campaign_id),
                                        "prospect_id": str(result.inserted_id),
                                        "scoring_version": pipeline_scoring_version,
                                        "score": {"value": None, "version": pipeline_scoring_version},
                                        "enrichment": {"state": "running", "attempt": 1, "run_id": enrichment_run_id},
                                        "pre_enrichment_triage": {"action": "discovered_replacement"},
                                        "created_at": datetime.utcnow(), "updated_at": datetime.utcnow(),
                                    }},
                                    upsert=True,
                                )
                            else:
                                await prospect_state_collection.update_one(
                                    {"account_id": str(account_id), "prospect_id": str(result.inserted_id)},
                                    {"$setOnInsert": {
                                        "account_id": str(account_id), "prospect_id": str(result.inserted_id),
                                        "status": "new", "tags": ["pre_enrichment_discovery"],
                                        "enrichment_status": "in_progress", "enrichment_run_id": enrichment_run_id,
                                        "created_at": datetime.utcnow(), "last_updated_at": datetime.utcnow(),
                                    }},
                                    upsert=True,
                                )
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


async def _update_run(run_oid: ObjectId, account_id: str, update_data: dict):
    """Update enrichment run document."""
    await enrichment_runs_collection.update_one(
        {"_id": run_oid, "account_id": {"$in": [str(account_id), ObjectId(str(account_id))]}},
        {"$set": update_data}
    )


async def _update_prospect_status(
    account_id: str,
    campaign_id: str | None,
    prospect_oid: ObjectId | str,
    status: str,
    error: str | None = None,
    run_id: str | None = None,
    scoring_version: str | None = None,
):
    """Persist workflow status only inside the authorized tenant overlay."""
    now = datetime.utcnow()
    prospect_id = str(prospect_oid)
    if campaign_id:
        from services.campaign_prospect_state_service import DEFAULT_SCORING_VERSION
        scoring_version = scoring_version or DEFAULT_SCORING_VERSION
        state = "running"
        if status == "completed":
            state = "succeeded"
        elif status in {"failed"}:
            state = "retryable_failure"
        elif status in {"skipped"}:
            state = "not_found"
        fields = {
            "enrichment.state": state,
            "enrichment.outcome": status,
            "enrichment.error_message": error,
            "enrichment.run_id": run_id,
            "updated_at": now,
        }
        if status == "in_progress":
            fields["enrichment.started_at"] = now
        if state in {"succeeded", "retryable_failure", "not_found"}:
            fields["enrichment.completed_at"] = now
        result = await campaign_prospect_state_collection.update_one(
            {
                "account_id": str(account_id), "campaign_id": str(campaign_id),
                "prospect_id": prospect_id, "scoring_version": scoring_version,
            },
            {"$set": fields},
        )
    else:
        fields = {
            "enrichment_status": status,
            "enrichment_error": error,
            "enrichment_run_id": run_id,
            "last_updated_at": now,
        }
        if status == "in_progress":
            fields["enrichment_started_at"] = now
        if status in {"completed", "failed", "skipped"}:
            fields["enrichment_completed_at"] = now
        result = await prospect_state_collection.update_one(
            {"account_id": str(account_id), "prospect_id": prospect_id},
            {"$set": fields},
        )
    if result.matched_count != 1:
        # Bookkeeping only — a missing/mismatched state doc must never abort
        # the run and mask the real error (e.g. an AI timeout) that called us.
        logger.warning(
            f"prospect enrichment state bookkeeping miss for prospect={prospect_id} "
            f"account={account_id} campaign={campaign_id} status={status} "
            f"(matched_count={result.matched_count})"
        )


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
                comp_doc = None
                claimed_id = None
                try:
                    company_name = prospect.get("company_name")
                    industry = prospect.get("industry")

                    if not company_name:
                        return

                    # Resolve the company doc for dedup check + storage
                    company_linkedin = prospect.get("company_linkedin")
                    if company_linkedin:
                        comp_doc = await companies_collection.find_one({"linkedin_url": company_linkedin})

                    # Reuse fresh competitors from company doc if available (< 30 days old)
                    if comp_doc and comp_doc.get("competitors"):
                        fetched_at = comp_doc.get("competitors_last_fetched")
                        if fetched_at and (datetime.utcnow() - fetched_at).days < 30:
                            prospect["competitors"] = comp_doc["competitors"]
                            logger.info(f"Reusing cached competitors for {company_name} from company doc")
                            return

                    # Atomic claim: multiple prospects at the same company would
                    # otherwise all pass the freshness check above concurrently
                    # and each pay for a Perplexity call. Only the claim winner
                    # calls out; everyone else waits for its result.
                    if comp_doc:
                        claim_now = datetime.utcnow()
                        claim = await companies_collection.find_one_and_update(
                            {
                                "_id": comp_doc["_id"],
                                "$or": [
                                    {"competitors_fetching_at": {"$exists": False}},
                                    {"competitors_fetching_at": {"$lt": claim_now - _CLAIM_STALE_AFTER}},
                                ],
                            },
                            {"$set": {"competitors_fetching_at": claim_now}},
                        )
                        if claim is None:
                            latest = await companies_collection.find_one(
                                {"_id": comp_doc["_id"]}, {"competitors": 1}
                            )
                            if latest and latest.get("competitors"):
                                prospect["competitors"] = latest["competitors"]
                            return
                        claimed_id = comp_doc["_id"]

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
                finally:
                    if claimed_id is not None:
                        try:
                            await companies_collection.update_one(
                                {"_id": claimed_id}, {"$unset": {"competitors_fetching_at": ""}}
                            )
                        except Exception:
                            pass

        await asyncio.gather(*[_research_one(p) for p in prospects])
    finally:
        await shared_client.close()

    return prospects, stats


async def _phase_news(prospects: list[dict], stats: dict) -> tuple[list[dict], dict]:
    """Phase 2.6: Research recent company news using Perplexity Sonar Pro.

    Runs in parallel with _phase_competitors — both depend only on company_name.
    Cached on the COMPANY doc (one Perplexity call per company, not per
    prospect) with an atomic claim so concurrent prospects at the same company
    don't each pay for their own call. Persists to companies.news +
    news_last_fetched, and mirrors the result onto prospects.company_news for
    prospect-detail display / message personalization.
    """
    from services.news_research_service import research_company_news

    stats["news_researched"] = 0
    semaphore = asyncio.Semaphore(3)
    shared_client = OpenRouterClient()

    try:
        async def _research_one(prospect: dict):
            async with semaphore:
                comp_doc = None
                claimed_id = None
                try:
                    company_name = prospect.get("company_name")
                    if not company_name:
                        return

                    company_linkedin = prospect.get("company_linkedin")
                    if company_linkedin:
                        comp_doc = await companies_collection.find_one({"linkedin_url": company_linkedin})

                    now = datetime.utcnow()

                    # Reuse fresh news from the company doc if available (< 14 days old)
                    if comp_doc and comp_doc.get("news"):
                        fetched_at = comp_doc.get("news_last_fetched")
                        if fetched_at and (now - fetched_at).days < 14:
                            prospect["company_news"] = comp_doc["news"]
                            await prospects_collection.update_one(
                                {"_id": prospect["_id"]},
                                {"$set": {"company_news": comp_doc["news"], "news_last_fetched": fetched_at}},
                            )
                            logger.info(f"Reusing cached news for {company_name} from company doc")
                            return

                    # Atomic claim: multiple prospects at the same company would
                    # otherwise all pass the freshness check above concurrently
                    # and each pay for a Perplexity call.
                    if comp_doc:
                        claim = await companies_collection.find_one_and_update(
                            {
                                "_id": comp_doc["_id"],
                                "$or": [
                                    {"news_fetching_at": {"$exists": False}},
                                    {"news_fetching_at": {"$lt": now - _CLAIM_STALE_AFTER}},
                                ],
                            },
                            {"$set": {"news_fetching_at": now}},
                        )
                        if claim is None:
                            latest = await companies_collection.find_one(
                                {"_id": comp_doc["_id"]}, {"news": 1}
                            )
                            if latest and latest.get("news"):
                                prospect["company_news"] = latest["news"]
                                await prospects_collection.update_one(
                                    {"_id": prospect["_id"]},
                                    {"$set": {"company_news": latest["news"], "news_last_fetched": now}},
                                )
                            return
                        claimed_id = comp_doc["_id"]

                    news = await research_company_news(company_name, limit=5, days_back=90, client=shared_client)

                    if news:
                        fetched_now = datetime.utcnow()
                        prospect["company_news"] = news
                        await prospects_collection.update_one(
                            {"_id": prospect["_id"]},
                            {"$set": {
                                "company_news": news,
                                "news_last_fetched": fetched_now,
                            }}
                        )
                        if comp_doc:
                            await companies_collection.update_one(
                                {"_id": comp_doc["_id"]},
                                {"$set": {
                                    "news": news,
                                    "news_last_fetched": fetched_now,
                                }}
                            )
                        stats["news_researched"] += 1
                        logger.info(f"News research: {len(news)} items for {company_name}")

                except Exception as e:
                    logger.error(f"News research failed for {prospect.get('email')}: {e}", exc_info=True)
                finally:
                    if claimed_id is not None:
                        try:
                            await companies_collection.update_one(
                                {"_id": claimed_id}, {"$unset": {"news_fetching_at": ""}}
                            )
                        except Exception:
                            pass

        await asyncio.gather(*[_research_one(p) for p in prospects])
    finally:
        await shared_client.close()

    return prospects, stats


def _fallback_ranking(employees: list[dict], max_contacts: int) -> list[dict]:
    """Simple fallback ranking by title seniority when AI fails."""
    priority_keywords = [
        "chief", "ceo", "cmo", "cto", "cfo", "coo", "president",
        "vp", "vice president", "director", "head of",
        "marketing", "business development", "sales",
    ]

    scored = []
    for emp in employees:
        position = emp.get("current_position", {})
        title = (position.get("title", "") if isinstance(position, dict) else "").lower()
        name = emp.get("full_name") or f"{emp.get('first_name', '')} {emp.get('last_name', '')}".strip()
        score = sum(10 for kw in priority_keywords if kw in title)
        scored.append({
            "rank": 0,
            "linkedin_url": emp.get("linkedin_url"),
            "full_name": name,
            "title": position.get("title", "Unknown") if isinstance(position, dict) else "Unknown",
            "reasoning": "Ranked by title seniority (AI unavailable)",
            "approach_angle": "Generic outreach based on seniority level",
            "confidence_score": min(80, score * 5 + 30),
            "_score": score,
        })

    scored.sort(key=lambda x: x["_score"], reverse=True)
    result = scored[:max_contacts]
    for i, contact in enumerate(result, 1):
        contact["rank"] = i
        del contact["_score"]
    return result
