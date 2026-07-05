"""
Smart Campaign Prospect Finder Service.

Discovers and enrolls high-quality prospects for smart campaigns.
Phase 1: Query MongoDB for ICP-matching prospects (ai_score >= 60).
Phase 2: Apify scraping + mini-enrichment if DB count is insufficient.
Phase 3: Enroll top N prospects and trigger message generation.
"""

import asyncio
import logging
import math
import re
from datetime import datetime
from bson import ObjectId
from typing import Optional

import database
from config import get_settings
from services.openrouter_service import get_free_model
from services.daily_cap_service import DEFAULT_CAPS

logger = logging.getLogger(__name__)
settings = get_settings()

# Seniority levels that are eligible for InMail (premium outreach)
INMAIL_SENIORITY_LEVELS = {"c_suite", "c-suite", "csuite", "founder", "owner", "vp", "partner"}




async def _discover_and_enroll_prospects_legacy(campaign_id: str, account_id: str) -> dict:
    """
    DEPRECATED — kept only as reference. Not called by any code path.
    Main background task entry point.
    Reads campaign doc to get ICP params and prospect_count_target.
    Updates campaign.discovery_status throughout execution.

    Returns a summary dict with counts.
    """
    from services.campaign_discovery_logger import CampaignDiscoveryLogger

    now = datetime.utcnow()
    discovery_started_at = now
    campaign_oid = ObjectId(campaign_id)
    account_oid = ObjectId(account_id)

    # Only skip Apify if the DB already provides ≥80% of the target. Below that,
    # always supplement with Apify scraping to hit the target count.
    DB_COVERAGE_SKIP_APIFY = 0.80

    # Mark discovery as searching_db
    await database.campaigns_collection.update_one(
        {"_id": campaign_oid},
        {"$set": {
            "discovery_status": "searching_db",
            "discovery_started_at": now,
            "discovery_error": None,
            "discovery_apify_triggered": False,
            "discovery_prospects_scraped": 0,
            "discovery_enrichment_total": 0,
            "discovery_enrichment_done": 0,
            "discovery_enrichment_failed": 0,
            "discovery_failure_reason": None,
        }},
    )

    async with CampaignDiscoveryLogger(
        campaign_id=campaign_id,
        account_id=account_id,
        log_dir=settings.discovery_log_dir,
    ) as disc_log:
        try:
            # Tracks how many prospects Apify scraped during Phase 2 (if any) so we
            # can bump discovery_prospects_found progressively (DB count first, then
            # DB + Apify) as data becomes available.
            apify_scraped_count = 0

            campaign = await database.campaigns_collection.find_one({"_id": campaign_oid})
            if not campaign:
                raise ValueError(f"Campaign {campaign_id} not found")

            # QW-5: Pre-flight Apify budget check
            _account_doc = await database.accounts_collection.find_one({"_id": account_oid})
            if _account_doc and _account_doc.get("monthly_apify_budget_usd") is not None:
                _budget = float(_account_doc["monthly_apify_budget_usd"])
                _now_dt = datetime.utcnow()
                _month_start = datetime(_now_dt.year, _now_dt.month, 1)
                _mtd_agg = await database.apify_usage_collection.aggregate([
                    {"$match": {"account_id": str(account_oid), "started_at": {"$gte": _month_start}}},
                    {"$group": {"_id": None, "total": {"$sum": "$cost_usd"}}},
                ]).to_list(1)
                _mtd_spent = _mtd_agg[0]["total"] if _mtd_agg else 0.0
                if _mtd_spent >= _budget:
                    _msg = (
                        f"Monthly Apify budget of ${_budget:.2f} reached "
                        f"(${_mtd_spent:.2f} spent this month). Discovery aborted."
                    )
                    logger.warning(f"[Campaign {campaign_id}] {_msg}")
                    await database.campaigns_collection.update_one(
                        {"_id": campaign_oid},
                        {"$set": {
                            "discovery_status": "failed",
                            "discovery_error": _msg,
                            "discovery_failure_reason": "apify_budget_exceeded",
                        }},
                    )
                    return {"campaign_id": campaign_id, "found": 0, "enrolled": 0, "enrollment_ids": []}


            target_count = campaign.get("prospect_count_target", 100)
            await disc_log.log(
                phase="init", event="discovery_params",
                target_count=target_count,
                icp_industries=campaign.get("icp_industries"),
                icp_seniority_levels=campaign.get("icp_seniority_levels"),
                icp_countries=campaign.get("icp_countries"),
                icp_job_titles=campaign.get("icp_job_titles"),
                icp_keywords=campaign.get("icp_keywords"),
                icp_exclude_keywords=campaign.get("icp_exclude_keywords"),
                icp_functional_departments=campaign.get("icp_functional_departments"),
                icp_company_size_min=campaign.get("icp_company_size_min"),
                icp_company_size_max=campaign.get("icp_company_size_max"),
                cta_type=campaign.get("cta_type"),
                cta_url=campaign.get("cta_url"),
                message_tone=campaign.get("message_tone"),
            )

            # Auto-pick the account's first connected email + LinkedIn accounts when
            # the campaign was created without explicit IDs. plan_channel_assignments
            # gates eligibility on these fields, so without them every prospect ends
            # up "skipped_no_channel" and Day-1 cohort comes out empty.
            # account_id can be stored as either ObjectId or string in account-scoped
            # collections (historical inconsistency between insert paths), so match both.
            account_id_filter = {"$in": [account_oid, str(account_oid)]}

            auto_set: dict = {}
            if not campaign.get("email_account_id"):
                email_acc = await database.email_accounts_collection.find_one(
                    {"account_id": account_id_filter, "status": {"$in": ["connected", "active"]}}
                )
                if email_acc:
                    auto_set["email_account_id"] = email_acc["_id"]
                    campaign["email_account_id"] = email_acc["_id"]
            if not campaign.get("linkedin_account_id"):
                # linkedin_accounts uses `unipile_status` (OK/CREDENTIALS/ERROR/STOPPED/CONNECTING/DELETED).
                li_acc = await database.linkedin_accounts_collection.find_one(
                    {"account_id": account_id_filter, "unipile_status": {"$in": ["OK", "CONNECTING"]}}
                )
                if li_acc:
                    auto_set["linkedin_account_id"] = li_acc["_id"]
                    campaign["linkedin_account_id"] = li_acc["_id"]
            if auto_set:
                await database.campaigns_collection.update_one(
                    {"_id": campaign_oid}, {"$set": auto_set}
                )
                logger.info(
                    f"[Campaign {campaign_id}] Auto-picked sending accounts: "
                    f"{ {k: str(v) for k, v in auto_set.items()} }"
                )

            # Build ICP dict from campaign fields
            icp = _build_icp_from_campaign(campaign)

            # Exclude only prospects already enrolled IN THIS CAMPAIGN (previously was
            # account-wide which starved new campaigns that targeted the same pool).
            exclude_ids = await _get_already_enrolled_prospect_ids(account_oid, campaign_oid)

            # Phase 1: Query DB directly for matching prospects (full ICP filters, no score gate)
            logger.info(f"[Campaign {campaign_id}] Phase 1: querying DB for matching prospects")
            db_prospects = await _query_existing_prospects(account_oid, icp, target_count * 3, exclude_ids)
            logger.info(f"[Campaign {campaign_id}] Phase 1 found {len(db_prospects)} matching prospects in DB")
            db_coverage = len(db_prospects) / target_count if target_count > 0 else 1.0
            await disc_log.log(phase="phase1_db", event="db_query_complete",
                               found=len(db_prospects), target=target_count,
                               coverage_pct=round(db_coverage * 100, 1))
            if db_prospects:
                await disc_log.log(
                    phase="phase1_db", event="db_match_detail",
                    count=len(db_prospects),
                    prospects=[{
                        "id": str(p.get("_id")),
                        "name": p.get("full_name") or f"{p.get('first_name', '')} {p.get('last_name', '')}".strip(),
                        "email": p.get("email"),
                        "linkedin": p.get("linkedin"),
                        "title": p.get("job_title") or p.get("title"),
                        "company": p.get("company_name"),
                        "seniority": p.get("seniority_level"),
                        "country": p.get("country"),
                        "enrichment_status": p.get("enrichment_status"),
                        "ai_prospect_score": p.get("ai_prospect_score"),
                        "priority_tier": p.get("priority_tier"),
                        "last_enriched_at": str(p.get("last_enriched_at") or ""),
                    } for p in db_prospects],
                )
            # Set discovery_prospects_found to the DB count immediately so the UI's
            # Phase 1 ("Finding prospects") counter shows a non-zero value right away
            # instead of waiting for Apify + enrichment.
            await database.campaigns_collection.update_one(
                {"_id": campaign_oid},
                {"$set": {"discovery_prospects_found": len(db_prospects)}},
            )
            if db_prospects:
                await database.campaigns_collection.update_one(
                    {"_id": campaign_oid},
                    {"$inc": {"discovery_prospects_from_db": len(db_prospects)}},
                )

            # Phases 1b + 1c (blocking un-enriched DB enrichment) moved into the async
            # _enrich_and_finalize_discovery task so discovery returns to the caller quickly.

            await database.campaigns_collection.update_one(
                {"_id": campaign_oid},
                {"$set": {"discovery_prospects_found_in_db": len(db_prospects)}},
            )

            apify_triggered = False
            apify_scraped_count = 0
            industry_id: str | None = None

            # Phase 2: Apify if DB coverage is below threshold. If DB already covers
            # ≥80% of the target, skip Apify entirely — "use our pool first" behaviour.
            if len(db_prospects) < target_count and db_coverage < DB_COVERAGE_SKIP_APIFY:
                deficit = target_count - len(db_prospects)
                needed = math.ceil(deficit * 2.5)
                needed = max(needed, target_count * 3)  # Over-fetch to absorb company-cap + channel-filter drops

                logger.info(
                    f"[Campaign {campaign_id}] Phase 2: Apify scraping "
                    f"(DB matched={len(db_prospects)}, target={target_count}, "
                    f"coverage={db_coverage:.0%}, scraping {needed} via Apify)"
                )
                await disc_log.log(phase="phase2_apify", event="apify_scrape_started",
                                   needed=needed, db_matched=len(db_prospects),
                                   coverage_pct=round(db_coverage * 100, 1))

                await database.campaigns_collection.update_one(
                    {"_id": campaign_oid},
                    {"$set": {
                        "discovery_status": "scraping",
                        "discovery_apify_triggered": True,
                    }},
                )
                apify_triggered = True

                try:
                    new_prospects, _yields = await _run_apify_discovery(icp, needed, account_oid, campaign=campaign)
                    logger.info(f"[Campaign {campaign_id}] Phase 2 scraped {len(new_prospects)} new prospects")
                    apify_scraped_count = len(new_prospects)
                    await disc_log.log(
                        phase="phase2_apify", event="apify_scrape_finished",
                        scraped=len(new_prospects), yields=_yields,
                        db_matched=len(db_prospects),
                        combined_unique=len(db_prospects) + len(new_prospects),
                        prospects=[{
                            "id": str(p.get("_id")),
                            "name": p.get("full_name") or f"{p.get('first_name', '')} {p.get('last_name', '')}".strip(),
                            "email": p.get("email"),
                            "linkedin": p.get("linkedin"),
                            "title": p.get("job_title") or p.get("title"),
                            "seniority": p.get("seniority_level"),
                            "company": p.get("company_name"),
                            "industry": p.get("industry"),
                            "country": p.get("country"),
                            "source_actor": p.get("source_actor_id"),
                        } for p in new_prospects],
                    )
                    await database.campaigns_collection.update_one(
                        {"_id": campaign_oid},
                        {"$inc": {
                            "discovery_prospects_scraped": len(new_prospects),
                            "discovery_prospects_from_apify": len(new_prospects),
                        }},
                    )
                    await database.campaigns_collection.update_one(
                        {"_id": campaign_oid},
                        {"$set": {
                            "discovery_prospects_found": len(db_prospects) + apify_scraped_count,
                        }},
                    )

                    # Gate A3: Freshness check — validate that Apify prospects still work at their stated employer
                    if settings.quality_gates_enabled and settings.freshness_gate_enabled and new_prospects:
                        from services.freshness_validator_service import validate_employer_freshness
                        try:
                            freshness_results = await validate_employer_freshness(
                                new_prospects,
                                short_mode=settings.freshness_short_mode,
                                concurrency=settings.freshness_concurrency,
                            )
                            freshness_by_url = {r.linkedin_url: r for r in freshness_results if r.linkedin_url}
                            kept_prospects = []
                            stale_count = 0
                            invalid_count = 0
                            skipped_count = 0
                            from pymongo import UpdateOne as _UpdateOne_fresh
                            freshness_ops = []
                            for p in new_prospects:
                                li_url = p.get("linkedin") or ""
                                result = freshness_by_url.get(li_url)
                                if result is None or result.skipped:
                                    kept_prospects.append(p)
                                    skipped_count += 1
                                    if result and result.skipped and p.get("_id"):
                                        freshness_ops.append(_UpdateOne_fresh(
                                            {"_id": p["_id"]},
                                            {"$set": {"freshness_check_status": "skipped"}},
                                        ))
                                elif result.matches:
                                    kept_prospects.append(p)
                                    if p.get("_id"):
                                        freshness_ops.append(_UpdateOne_fresh(
                                            {"_id": p["_id"]},
                                            {"$set": {
                                                "freshness_check_status": "fresh",
                                                "linkedin_snapshot": result.profile_snapshot,
                                            }},
                                        ))
                                else:
                                    if result.linkedin_url_invalid:
                                        # Hard drop: broken LinkedIn URL, no point reaching out
                                        invalid_count += 1
                                        if p.get("_id"):
                                            freshness_ops.append(_UpdateOne_fresh(
                                                {"_id": p["_id"]},
                                                {"$set": {
                                                    "freshness_check_status": "invalid",
                                                    "linkedin_snapshot": result.profile_snapshot,
                                                    "status": "disqualified",
                                                    "disqualify_reason": "linkedin_invalid",
                                                }},
                                            ))
                                    else:
                                        # Soft filter: stale employer — stay in pool, penalized via rule_score
                                        stale_count += 1
                                        kept_prospects.append(p)
                                        p["freshness_check_status"] = "stale"
                                        if p.get("_id"):
                                            freshness_ops.append(_UpdateOne_fresh(
                                                {"_id": p["_id"]},
                                                {"$set": {
                                                    "freshness_check_status": "stale",
                                                    "stale_employer": True,
                                                    "linkedin_snapshot": result.profile_snapshot,
                                                }},
                                            ))
                            if freshness_ops:
                                try:
                                    await database.prospects_collection.bulk_write(freshness_ops, ordered=False)
                                except Exception as _fw_err:
                                    logger.warning(f"[Campaign {campaign_id}] Freshness bulk write error: {_fw_err}")
                            await disc_log.log(phase="gate_a3_freshness", event="freshness_gate_complete",
                                               total_checked=len(new_prospects),
                                               passed=len(new_prospects) - stale_count - invalid_count,
                                               penalized_stale=stale_count,
                                               dropped_invalid=invalid_count,
                                               skipped=skipped_count)
                            new_prospects = kept_prospects
                            apify_scraped_count = len(new_prospects)
                        except Exception as _fresh_err:
                            logger.warning(f"[Campaign {campaign_id}] Freshness gate error: {_fresh_err}", exc_info=True)
                            await disc_log.error("gate_a3_freshness", "freshness_gate_error", exc=_fresh_err)

                    if new_prospects:
                        # Pre-enroll scraped prospects immediately so enrolled-prospects API
                        # returns them during enrichment (status="enriching" = engine-safe placeholder)
                        await _pre_enroll_prospects(campaign, new_prospects)
                        await _recompute_companies_count(campaign_oid)

                    # Auto-create industry from campaign params and tag new prospects
                    if new_prospects:
                        industry_id = await _auto_create_industry_if_needed(campaign, account_oid, len(new_prospects))
                        if industry_id:
                            new_prospect_oids = [p["_id"] for p in new_prospects if p.get("_id")]
                            if new_prospect_oids:
                                from pymongo import UpdateOne as _UpdateOne
                                tag_ops = [
                                    _UpdateOne(
                                        {"_id": oid},
                                        {
                                            "$set": {"industry_id": industry_id},
                                            "$addToSet": {"source_industry_ids": industry_id},
                                        }
                                    )
                                    for oid in new_prospect_oids
                                ]
                                try:
                                    await database.prospects_collection.bulk_write(tag_ops, ordered=False)
                                except Exception as tag_err:
                                    logger.warning(f"Industry tagging bulk write error: {tag_err}")

                    # Mini-enrichment runs async in _enrich_and_finalize_discovery (non-blocking)

                except Exception as apify_err:
                    logger.error(f"[Campaign {campaign_id}] Apify phase error: {apify_err}", exc_info=True)
                    await disc_log.error("phase2_apify", "apify_phase_error", exc=apify_err)
                    # Continue with what we have from DB
            else:
                logger.info(
                    f"[Campaign {campaign_id}] Skipping Apify: DB coverage "
                    f"{db_coverage:.0%} (matched={len(db_prospects)}, target={target_count}) "
                    f"meets threshold ({DB_COVERAGE_SKIP_APIFY:.0%})"
                )
                await disc_log.log(phase="phase2_apify", event="apify_skipped",
                                   reason="db_coverage_sufficient", coverage_pct=round(db_coverage * 100, 1))

            # Fast-path complete: Apify scraped + pre-enrolled; hand off to async enrichment task.
            await database.campaigns_collection.update_one(
                {"_id": campaign_oid},
                {"$set": {
                    "discovery_status": "enriching",
                    "discovery_apify_triggered": apify_triggered,
                    "discovery_prospects_found": len(db_prospects) + apify_scraped_count,
                }},
            )

            # Enrichment, scoring, enrollment and message generation all run async.
            async def _safe_finalize(cid: str, aid: str, excl: list[str]) -> None:
                try:
                    await _enrich_and_finalize_discovery(cid, aid, excl)
                except Exception as _finalize_err:
                    logger.exception(f"[Campaign {cid}] _enrich_and_finalize_discovery crashed")
                    try:
                        await database.campaigns_collection.update_one(
                            {"_id": ObjectId(cid)},
                            {"$set": {
                                "discovery_status": "failed",
                                "discovery_failure_reason": "finalize_crashed",
                                "discovery_error": str(_finalize_err)[:500],
                            }},
                        )
                    except Exception:
                        pass

            asyncio.create_task(_safe_finalize(campaign_id, account_id, [str(e) for e in exclude_ids]))

            await disc_log.log(phase="handoff", event="finalize_task_created",
                               db_found=len(db_prospects), apify_scraped=apify_scraped_count)
            return {
                "campaign_id": campaign_id,
                "found": len(db_prospects),
                "enrolled": 0,
                "enrollment_ids": [],
            }

        except Exception as e:
            logger.error(f"[Campaign {campaign_id}] Discovery failed: {e}", exc_info=True)
            await disc_log.error("discovery", "discovery_failed", exc=e)
            await database.campaigns_collection.update_one(
                {"_id": campaign_oid},
                {"$set": {
                    "discovery_status": "failed",
                    "discovery_error": str(e),
                }},
            )
            raise


async def _enrich_and_finalize_discovery(
    campaign_id: str,
    account_id: str,
    exclude_ids_str: list[str],
) -> None:
    """
    Background task: scores all scraped prospects cheaply, selects Day-1 cohort,
    assigns channels, enriches ONLY the cohort, generates one message per enrollment.
    """
    from services.campaign_discovery_logger import CampaignDiscoveryLogger

    campaign_oid = ObjectId(campaign_id)
    account_oid = ObjectId(account_id)
    exclude_ids = {ObjectId(e) for e in exclude_ids_str if e}

    disc_log = CampaignDiscoveryLogger(campaign_id, account_id, settings.discovery_log_dir)
    await disc_log.__aenter__()

    try:
        await disc_log.log(phase="enrich", event="finalize_started")
        campaign = await database.campaigns_collection.find_one({"_id": campaign_oid})
        if not campaign:
            logger.error(f"[Campaign {campaign_id}] _enrich_and_finalize_discovery: campaign not found")
            return

        # --- Step A: Collect all prospects that need enrichment ---
        # a) Pre-enrolled prospects (status "scoring" from _pre_enroll_prospects,
        #    or legacy "enriching" from older code paths)
        enr_cursor = database.campaign_enrollments_collection.find(
            {"campaign_id": campaign_oid, "status": {"$in": ["enriching", "scoring"]}},
            {"prospect_id": 1},
        )
        apify_prospect_oids = [doc["prospect_id"] async for doc in enr_cursor]

        # b) Un-enriched DB prospects
        db_unenriched_exclude = exclude_ids | set(apify_prospect_oids)
        target_count = campaign.get("prospect_count_target", 100)
        # Track how many raw prospects Apify has already scraped (for top-up gate).
        apify_scraped_count = len(apify_prospect_oids)
        unenriched_db = await _query_unenriched_prospects(
            account_oid, _build_icp_from_campaign(campaign), target_count * 2, db_unenriched_exclude
        )
        if unenriched_db:
            logger.info(f"[Campaign {campaign_id}] Async: found {len(unenriched_db)} un-enriched DB prospects")
            await _pre_enroll_prospects(campaign, unenriched_db)

        all_candidate_oids = list({
            *apify_prospect_oids,
            *[p["_id"] for p in unenriched_db],
        })

        await disc_log.log(
            phase="enrich", event="candidates_collected",
            apify_count=len(apify_prospect_oids),
            unenriched_db_count=len(unenriched_db),
            total_candidates=len(all_candidate_oids),
        )

        # --- Step B: Campaign-aware rule-based scoring on ALL candidates ---
        # Rule-scoring is instant (no AI) and uses only fields already present
        # from the Apify scrape / DB row. The resulting score drives cohort
        # planning below; AI enrichment happens AFTER planning, scoped to the
        # Day-1 cohort only.
        from utils.scoring import score_prospect_for_campaign

        await database.campaigns_collection.update_one(
            {"_id": campaign_oid},
            {"$set": {"discovery_status": "scoring"}},
        )

        # Also include any already-enriched DB prospects from Phase 1
        icp = _build_icp_from_campaign(campaign)
        db_enriched = await _query_existing_prospects(account_oid, icp, target_count * 3, exclude_ids)

        # Fetch raw candidate docs for rule-based scoring — we need enough
        # fields to run score_prospect_for_campaign (seniority, email, linkedin,
        # company_size, industry, country).
        candidate_docs = []
        if all_candidate_oids:
            raw_cursor = database.prospects_collection.find(
                {"_id": {"$in": all_candidate_oids}},
                {"_id": 1, "seniority_level": 1, "email": 1, "linkedin": 1,
                 "company_size": 1, "company_annual_revenue_clean": 1,
                 "industry": 1, "country": 1,
                 "company_name": 1, "company_linkedin": 1,
                 "freshness_check_status": 1,
                 "job_title": 1, "headline": 1,
                 "company_description": 1, "company_keywords": 1},
            )
            candidate_docs = await raw_cursor.to_list(length=len(all_candidate_oids))

        # Rule-score everyone; de-dup across DB-enriched and scraped/un-enriched pools.
        all_for_scoring: list[tuple[dict, float]] = []
        rule_score_bulk_ops: list = []
        seen_ids: set = set()
        chunk_scored = 0
        for p in db_enriched + candidate_docs:
            pid = str(p["_id"])
            if pid in seen_ids:
                continue
            seen_ids.add(pid)
            try:
                rule_score = score_prospect_for_campaign(p, campaign)
            except Exception as _se:
                logger.warning(f"[Campaign {campaign_id}] score_prospect_for_campaign failed for {pid}: {_se}")
                continue
            all_for_scoring.append((p, rule_score))
            rule_score_bulk_ops.append((p["_id"], float(rule_score)))
            chunk_scored += 1
            if chunk_scored % 50 == 0:
                await database.campaigns_collection.update_one(
                    {"_id": campaign_oid},
                    {"$inc": {"discovery_prospects_rule_scored": chunk_scored}},
                )
                chunk_scored = 0
        if chunk_scored > 0:
            await database.campaigns_collection.update_one(
                {"_id": campaign_oid},
                {"$inc": {"discovery_prospects_rule_scored": chunk_scored}},
            )

        # Bulk-write rule scores back to prospect docs so the Prospects tab can surface them.
        from pymongo import UpdateOne as _UpdateOne_score
        _score_now = datetime.utcnow()
        score_prospect_ops = []
        for pid, rs in rule_score_bulk_ops:
            score_prospect_ops.append(_UpdateOne_score(
                {"_id": pid},
                {"$set": {"last_campaign_rule_score": rs, "last_scored_at": _score_now}},
            ))
        for i in range(0, len(score_prospect_ops), 100):
            chunk = score_prospect_ops[i:i + 100]
            if chunk:
                await database.prospects_collection.bulk_write(chunk, ordered=False)

        # Sort by rule score desc so top-ranked prospects land in Day 1.
        all_for_scoring.sort(key=lambda x: x[1], reverse=True)

        await disc_log.log(
            phase="enrich", event="rule_scoring_complete",
            scored_count=len(all_for_scoring),
            prospects=[{
                "id": str(p.get("_id")),
                "name": p.get("full_name") or f"{p.get('first_name', '')} {p.get('last_name', '')}".strip(),
                "email": p.get("email"),
                "title": p.get("job_title") or p.get("title"),
                "company": p.get("company_name"),
                "seniority": p.get("seniority_level"),
                "country": p.get("country"),
                "rule_score": round(float(rs), 2),
            } for p, rs in all_for_scoring],
        )

        # Gate A4: AI prefilter — batch LLM pass to eliminate poor ICP fits before enrollment
        if settings.quality_gates_enabled and settings.prefilter_gate_enabled and all_for_scoring:
            from services.ai_prefilter_service import prefilter_candidates
            _prefilter_candidates_list = [p for p, _s in all_for_scoring]
            try:
                prefilter_results = await prefilter_candidates(
                    _prefilter_candidates_list,
                    icp=campaign,
                    batch_size=settings.prefilter_batch_size,
                    concurrency=settings.prefilter_concurrency,
                    confidence_threshold=settings.prefilter_confidence_threshold,
                    account_id=str(account_id),
                    campaign_id=str(campaign_id),
                )
                _prefilter_key_to_result = {r.prospect_key: r for r in prefilter_results}
                prefilter_passed = 0
                prefilter_rejected = 0
                prefilter_errors = 0
                from pymongo import UpdateOne as _UpdateOne_pf
                pf_ops = []
                all_for_scoring_filtered = []
                for p, score in all_for_scoring:
                    key = p.get("linkedin") or p.get("email") or f"idx_{p.get('_id')}"
                    result = _prefilter_key_to_result.get(key)
                    if result is None or result.error:
                        all_for_scoring_filtered.append((p, score))
                        prefilter_errors += 1
                        if result and p.get("_id"):
                            pf_ops.append(_UpdateOne_pf(
                                {"_id": p["_id"]},
                                {"$set": {"prefilter_status": "error", "prefilter_confidence": result.confidence}},
                            ))
                    elif result.passes:
                        all_for_scoring_filtered.append((p, score))
                        prefilter_passed += 1
                        if p.get("_id"):
                            pf_ops.append(_UpdateOne_pf(
                                {"_id": p["_id"]},
                                {"$set": {"prefilter_status": "passed", "prefilter_confidence": result.confidence}},
                            ))
                    else:
                        prefilter_rejected += 1
                        if p.get("_id"):
                            pf_ops.append(_UpdateOne_pf(
                                {"_id": p["_id"]},
                                {"$set": {
                                    "prefilter_status": "rejected",
                                    "prefilter_confidence": result.confidence,
                                    "prefilter_reject_reason": result.reject_reason,
                                    "status": "disqualified",
                                    "disqualify_reason": "prefilter_rejected",
                                }},
                            ))
                if pf_ops:
                    try:
                        await database.prospects_collection.bulk_write(pf_ops, ordered=False)
                    except Exception as _pf_err:
                        logger.warning(f"[Campaign {campaign_id}] Prefilter bulk write error: {_pf_err}")
                logger.info(
                    f"[Campaign {campaign_id}] Gate A4 prefilter: "
                    f"passed={prefilter_passed}, rejected={prefilter_rejected}, errors={prefilter_errors}"
                )
                await disc_log.log(
                    phase="enrich", event="prefilter_complete",
                    passed=prefilter_passed,
                    rejected=prefilter_rejected,
                    errors=prefilter_errors,
                    decisions=[{
                        "id": str(p.get("_id")),
                        "name": p.get("full_name") or f"{p.get('first_name', '')} {p.get('last_name', '')}".strip(),
                        "email": p.get("email"),
                        "passes": _prefilter_key_to_result.get(
                            p.get("linkedin") or p.get("email") or f"idx_{p.get('_id')}"
                        ) and _prefilter_key_to_result[
                            p.get("linkedin") or p.get("email") or f"idx_{p.get('_id')}"
                        ].passes,
                        "confidence": getattr(
                            _prefilter_key_to_result.get(
                                p.get("linkedin") or p.get("email") or f"idx_{p.get('_id')}"
                            ), "confidence", None
                        ),
                        "reject_reason": getattr(
                            _prefilter_key_to_result.get(
                                p.get("linkedin") or p.get("email") or f"idx_{p.get('_id')}"
                            ), "reject_reason", None
                        ),
                    } for p, _ in all_for_scoring],
                )
                all_for_scoring = all_for_scoring_filtered
            except Exception as _pf_gate_err:
                logger.warning(f"[Campaign {campaign_id}] Prefilter gate error: {_pf_gate_err}", exc_info=True)

        # --- Step C: Compute cohort size and available channels ---
        caps = campaign.get("daily_caps") or DEFAULT_CAPS
        for _k, _v in DEFAULT_CAPS.items():
            if _k not in caps:
                caps[_k] = _v

        # Check which channels are actually available
        available_channels: set = set()
        has_linkedin_account = bool(campaign.get("linkedin_account_id"))
        has_email_account = bool(campaign.get("email_account_id"))
        if has_linkedin_account:
            # linkedin_accounts uses `unipile_status` (OK/CREDENTIALS/ERROR/STOPPED/CONNECTING/DELETED).
            li_account = await database.linkedin_accounts_collection.find_one(
                {"_id": ObjectId(campaign["linkedin_account_id"]), "unipile_status": {"$in": ["OK", "CONNECTING"]}},
                {"_id": 1},
            ) if campaign.get("linkedin_account_id") else None
            if li_account:
                available_channels.add("linkedin_connection")
                available_channels.add("linkedin_inmail")  # gated by seniority in build_day1_batch
        if has_email_account:
            email_account = await database.email_accounts_collection.find_one(
                {"_id": ObjectId(campaign["email_account_id"]), "status": {"$in": ["connected", "active"]}},
                {"_id": 1},
            ) if campaign.get("email_account_id") else None
            if email_account:
                available_channels.add("email")

        if not available_channels:
            # No accounts connected — use all channels (let launch validate later)
            available_channels = {"linkedin_connection", "email", "linkedin_inmail"}
            await database.campaigns_collection.update_one(
                {"_id": campaign_oid},
                {"$set": {"discovery_warnings": "No sending accounts verified; channel assignment is provisional"}},
            )

        # --- Step C.5: Per-company prospect cap ---
        # Prevent multiple prospects from the same company flooding the cohort.
        # Keep top N per company (by rule score desc, already sorted above).
        _max_per_company = icp.get("max_prospects_per_company", 3)
        _company_counts: dict = {}
        _capped_for_scoring: list = []
        _capped_out_ids: list = []
        for p, score in all_for_scoring:
            co_key = (
                (p.get("company_linkedin") or "").strip().lower()
                or (p.get("company_name") or "").strip().lower()
                or "__unknown__"
            )
            count = _company_counts.get(co_key, 0)
            if count < _max_per_company:
                _company_counts[co_key] = count + 1
                _capped_for_scoring.append((p, score))
            else:
                _capped_out_ids.append(p["_id"])

        if _capped_out_ids:
            logger.info(
                f"[Campaign {campaign_id}] Per-company cap ({_max_per_company}): "
                f"kept {len(_capped_for_scoring)}, dropped {len(_capped_out_ids)} excess"
            )
            # Mark capped-out prospects as disqualified so they don't re-enter the pool
            from pymongo import UpdateOne as _UpdateOne_cap
            cap_ops = [
                _UpdateOne_cap(
                    {"_id": oid},
                    {"$set": {"status": "disqualified", "disqualify_reason": "company_cap_exceeded"}},
                )
                for oid in _capped_out_ids
            ]
            await database.prospects_collection.bulk_write(cap_ops, ordered=False)

        # Publish canonical post-cap count — this is the single denominator all UI cards use
        await database.campaigns_collection.update_one(
            {"_id": campaign_oid},
            {"$set": {
                "discovery_prospects_eligible": len(_capped_for_scoring),
                "discovery_prospects_rule_scored_total": len(_capped_for_scoring),
                "discovery_prospects_rule_scored": len(_capped_for_scoring),
            }},
        )

        # --- Step D.0: Deterministic campaign scoring + cold gate ---
        # Score every candidate against the campaign ICP. Drop cold fits before
        # creating any enrollment rows — avoids wasted DB writes and scheduling
        # work for prospects we can immediately see are poor fits.
        from services.campaign_scoring_service import compute_campaign_score
        from pymongo import UpdateOne as _UpdateOne_cam_score
        _cam_score_ops: list = []
        _cam_passing: list = []
        _min_enroll_score = settings.min_score_to_enroll
        # QW-6: track near-miss prospects (10 pts below threshold) for relaxed-score recovery
        _near_miss_min_score = max(_min_enroll_score - 10, 0)
        _near_miss_candidates: list = []  # (prospect, rule_score) tuples
        for _p, _rs in _capped_for_scoring:
            _cam_result = compute_campaign_score(
                _p, campaign,
                profile=_p.get("linkedin_profile_data"),
                company=_p.get("company_data"),
            )
            _cam_score_ops.append(_UpdateOne_cam_score(
                {"_id": _p["_id"]},
                {"$set": {
                    "ai_prospect_score": _cam_result["fit_score"],
                    "prospect_score": _cam_result["fit_score"],
                    "priority_tier": _cam_result["priority_tier"],
                    "fit_score": _cam_result["fit_score"],
                    "company_fit_score": _cam_result["company_fit_score"],
                    "prospect_fit_score": _cam_result["prospect_fit_score"],
                    "ai_assessment": _cam_result,
                    "ai_score_breakdown": _cam_result["breakdown"],
                    "enrichment_status": "ai_assessed",
                }},
            ))
            # Keep the in-memory dict in sync so near_miss_recovery's prospects_by_id
            # sees the campaign score rather than falling back to rule_score as p_fit.
            _p["ai_prospect_score"] = _cam_result["fit_score"]
            _p["priority_tier"] = _cam_result["priority_tier"]
            if _cam_result["fit_score"] >= _min_enroll_score:
                _cam_passing.append((_p, _rs))
            elif _cam_result["fit_score"] >= _near_miss_min_score:
                _near_miss_candidates.append((_p, _rs))
        for _i in range(0, len(_cam_score_ops), 200):
            await database.prospects_collection.bulk_write(
                _cam_score_ops[_i:_i + 200], ordered=False
            )
        await disc_log.log(
            phase="enrich", event="internal_scoring_gate",
            total=len(_capped_for_scoring),
            passing=len(_cam_passing),
            cold_dropped=len(_capped_for_scoring) - len(_cam_passing),
            near_miss=len(_near_miss_candidates),
            min_score=_min_enroll_score,
            near_miss_min_score=_near_miss_min_score,
        )

        # --- Step D: Enroll ALL candidates and plan channel/day for each ---
        # We no longer trim to a Day-1 cohort. Every discovered prospect is
        # enrolled and assigned to a send_day (Day 1..N) based on score + caps.
        # Messages are only generated for Day 1 upfront; Day 2+ messages are
        # generated the moment the previous day is approved.
        all_candidates_scored = _cam_passing  # scored, above cold gate, company-capped
        if not all_candidates_scored:
            await database.campaigns_collection.update_one(
                {"_id": campaign_oid},
                {"$set": {
                    "discovery_status": "failed",
                    "discovery_error": "No prospects found to enrich. Try broadening your ICP filters.",
                    "discovery_failure_reason": "no_icp_match",
                }},
            )
            return

        all_candidate_docs = [p for p, _score in all_candidates_scored]
        rule_score_by_pid: dict = {p["_id"]: s for p, s in all_candidates_scored}
        enrollment_ids = await _enroll_prospects(campaign, all_candidate_docs)

        # Publish an interim enrolled count so polls during enriching/scoring see
        # a non-zero value. The final update further below overwrites this with
        # total_assigned once channel/day planning completes.
        await database.campaigns_collection.update_one(
            {"_id": campaign_oid},
            {"$set": {"discovery_prospects_enrolled": len(enrollment_ids)}},
        )

        # --- Step E: Plan channel + day for every enrollment (BEFORE AI) ---
        # Planning uses the rule-based score we just computed. AI enrichment
        # runs AFTER planning, scoped only to the Day-1 cohort — this saves
        # Apify + OpenRouter budget on prospects that won't be contacted for
        # days (their AI enrichment is triggered when their day is approved).
        from services.campaign_launch_service import plan_channel_assignments
        from pymongo import UpdateOne as _UpdateOne

        total_enrolled_count = len(enrollment_ids)

        # Stamp the rule score onto each enrollment so plan_channel_assignments
        # sorts correctly and so the Prospects tab can surface it directly.
        rule_score_ops = []
        all_enr_cursor = database.campaign_enrollments_collection.find(
            {"campaign_id": campaign_oid, "status": {"$in": ["active", "enriching", "enrolled"]}}
        )
        all_enrollments = await all_enr_cursor.to_list(length=total_enrolled_count + 50)
        for enr in all_enrollments:
            pid = enr.get("prospect_id")
            if pid is None:
                continue
            rs = rule_score_by_pid.get(pid)
            if rs is None:
                continue
            rule_score_ops.append(_UpdateOne(
                {"_id": enr["_id"]},
                {"$set": {"campaign_rule_score": float(rs)}},
            ))
            enr["campaign_rule_score"] = float(rs)
        if rule_score_ops:
            await database.campaign_enrollments_collection.bulk_write(rule_score_ops, ordered=False)

        # Refresh campaign doc so plan_channel_assignments sees the latest caps
        campaign_refreshed = await database.campaigns_collection.find_one({"_id": campaign_oid})
        campaign_refreshed = campaign_refreshed or campaign

        refreshed_prospect_ids = [e["prospect_id"] for e in all_enrollments]
        refreshed_prospects = await database.prospects_collection.find(
            {"_id": {"$in": refreshed_prospect_ids}}
        ).to_list(length=len(refreshed_prospect_ids))
        prospects_by_id = {p["_id"]: p for p in refreshed_prospects}

        # Stamp priority_tier from the freshly-scored prospect onto each enrollment so
        # analytics and the E2E report can read it directly from campaign_enrollments.
        tier_ops = []
        for enr in all_enrollments:
            pid = enr.get("prospect_id")
            if pid and pid in prospects_by_id:
                tier = prospects_by_id[pid].get("priority_tier")
                if tier:
                    tier_ops.append(_UpdateOne(
                        {"_id": enr["_id"]},
                        {"$set": {"priority_tier": tier}},
                    ))
                    enr["priority_tier"] = tier
        if tier_ops:
            await database.campaign_enrollments_collection.bulk_write(tier_ops, ordered=False)

        assignments, skip_reasons = plan_channel_assignments(campaign_refreshed, all_enrollments, prospects_by_id)
        if skip_reasons:
            logger.info(f"[Campaign {campaign_id}] Planner skipped {sum(skip_reasons.values())} prospects by reason: {skip_reasons}")

        await disc_log.log(
            phase="enrich", event="channel_planning_initial",
            assigned=len(assignments),
            skipped=sum(skip_reasons.values()) if skip_reasons else 0,
            skip_reasons=skip_reasons or {},
            assignments=[{
                "enrollment_id": str(enr.get("_id")),
                "prospect_id": str(enr.get("prospect_id")),
                "channel": ch,
                "send_day": sd,
                "rule_score": enr.get("campaign_rule_score"),
            } for enr, ch, sd in assignments],
        )

        # --- Top-up loop: if yield < target, scrape more from Apify (max 3 attempts) ---
        lead_scraper_exhausted = False
        leads_finder_exhausted = False
        topup_raw_scraped = 0
        RAW_POOL_MULTIPLIER = settings.score_raw_pool_multiplier
        EXHAUSTION_THRESHOLD = 5

        for _topup in range(settings.score_topup_max_iterations):
            if len(assignments) >= target_count:
                break

            raw_pool_target = math.ceil(target_count * RAW_POOL_MULTIPLIER)
            total_raw_scraped_so_far = apify_scraped_count + topup_raw_scraped
            if total_raw_scraped_so_far >= raw_pool_target:
                logger.info(
                    f"[Campaign {campaign_id}] Top-up gate: raw pool {total_raw_scraped_so_far} >= "
                    f"{raw_pool_target} (target×{RAW_POOL_MULTIPLIER}), no further Apify calls needed"
                )
                break

            if lead_scraper_exhausted and leads_finder_exhausted:
                logger.info(f"[Campaign {campaign_id}] Both Apify actors exhausted, stopping top-up")
                break

            deficit = target_count - len(assignments)
            topup_needed = max(math.ceil(deficit * 2), 50)
            logger.info(
                f"[Campaign {campaign_id}] Top-up {_topup + 1}: assigned={len(assignments)}/{target_count}, "
                f"raw_pool={total_raw_scraped_so_far}/{raw_pool_target}, need {topup_needed} more "
                f"(scraper={'skip' if lead_scraper_exhausted else 'run'}, "
                f"finder={'skip' if leads_finder_exhausted else 'run'})"
            )
            try:
                _topup_icp = _perturb_icp_for_topup(icp, _topup)
                more_prospects, actor_yields = await _run_apify_discovery(
                    _topup_icp, topup_needed, account_oid, leads_finder_page=_topup + 1,
                    run_lead_scraper=not lead_scraper_exhausted,
                    run_leads_finder=not leads_finder_exhausted,
                    campaign=campaign_refreshed,
                )

                if actor_yields.get("lead_scraper", 0) < EXHAUSTION_THRESHOLD:
                    lead_scraper_exhausted = True
                    logger.info(f"[Campaign {campaign_id}] LeadScraper exhausted (yielded {actor_yields.get('lead_scraper', 0)})")
                if actor_yields.get("leads_finder", 0) < EXHAUSTION_THRESHOLD:
                    leads_finder_exhausted = True
                    logger.info(f"[Campaign {campaign_id}] LeadsFinder exhausted (yielded {actor_yields.get('leads_finder', 0)})")

                if not more_prospects:
                    break

                topup_raw_scraped += len(more_prospects)
                await _pre_enroll_prospects(campaign_refreshed, more_prospects)
                await _recompute_companies_count(campaign_oid)
                await database.campaigns_collection.update_one(
                    {"_id": campaign_oid},
                    {"$inc": {
                        "discovery_prospects_scraped": len(more_prospects),
                        "discovery_prospects_from_apify": len(more_prospects),
                    }},
                )

                new_pids = [p["_id"] for p in more_prospects if p.get("_id")]
                new_docs = await database.prospects_collection.find(
                    {"_id": {"$in": new_pids}}
                ).to_list(length=len(new_pids))

                _topup_score_now = datetime.utcnow()
                topup_score_ops = []
                topup_enr_ops_inner = []
                topup_scored_with_score: list[tuple] = []
                for p in new_docs:
                    pid = p["_id"]
                    try:
                        rs = score_prospect_for_campaign(p, campaign_refreshed)
                    except Exception as _se:
                        logger.warning(f"[Campaign {campaign_id}] Top-up scoring failed for {pid}: {_se}")
                        continue
                    rule_score_by_pid[pid] = rs
                    prospects_by_id[pid] = p
                    topup_scored_with_score.append((p, rs))
                    topup_score_ops.append(_UpdateOne_score(
                        {"_id": pid},
                        {"$set": {
                            "last_campaign_rule_score": float(rs),
                            "last_scored_at": _topup_score_now,
                        }},
                    ))
                if topup_score_ops:
                    await database.prospects_collection.bulk_write(topup_score_ops, ordered=False)

                # Internal campaign scoring + cold gate for top-up batch
                from services.campaign_scoring_service import compute_campaign_score as _ccs
                from pymongo import UpdateOne as _UpdateOne_cam_tu
                _cam_tu_ops: list = []
                _topup_passing: list = []
                for _tp, _trs in topup_scored_with_score:
                    _cam_tu = _ccs(
                        _tp, campaign_refreshed,
                        profile=_tp.get("linkedin_profile_data"),
                        company=_tp.get("company_data"),
                    )
                    _cam_tu_ops.append(_UpdateOne_cam_tu(
                        {"_id": _tp["_id"]},
                        {"$set": {
                            "ai_prospect_score": _cam_tu["fit_score"],
                            "prospect_score": _cam_tu["fit_score"],
                            "priority_tier": _cam_tu["priority_tier"],
                            "fit_score": _cam_tu["fit_score"],
                            "company_fit_score": _cam_tu["company_fit_score"],
                            "prospect_fit_score": _cam_tu["prospect_fit_score"],
                            "ai_assessment": _cam_tu,
                            "ai_score_breakdown": _cam_tu["breakdown"],
                            "enrichment_status": "ai_assessed",
                        }},
                    ))
                    if _cam_tu["fit_score"] >= settings.min_score_to_enroll:
                        _topup_passing.append((_tp, _trs))
                if _cam_tu_ops:
                    for _ci in range(0, len(_cam_tu_ops), 200):
                        await database.prospects_collection.bulk_write(
                            _cam_tu_ops[_ci:_ci + 200], ordered=False
                        )
                topup_scored_with_score = _topup_passing

                # Apply per-company cap to top-up candidates (reuse _company_counts from outer scope)
                topup_scored_with_score.sort(key=lambda x: x[1], reverse=True)
                topup_survivors: list = []
                topup_capped_out_ids: list = []
                for p, _rs in topup_scored_with_score:
                    co_key = (
                        (p.get("company_linkedin") or "").strip().lower()
                        or (p.get("company_name") or "").strip().lower()
                        or "__unknown__"
                    )
                    count = _company_counts.get(co_key, 0)
                    if count < _max_per_company:
                        _company_counts[co_key] = count + 1
                        topup_survivors.append(p)
                    else:
                        topup_capped_out_ids.append(p["_id"])

                if topup_capped_out_ids:
                    from pymongo import UpdateOne as _UpdateOne_cap2
                    topup_cap_ops = [
                        _UpdateOne_cap2(
                            {"_id": oid},
                            {"$set": {"status": "disqualified", "disqualify_reason": "company_cap_exceeded"}},
                        )
                        for oid in topup_capped_out_ids
                    ]
                    await database.prospects_collection.bulk_write(topup_cap_ops, ordered=False)

                topup_survivors_count = len(topup_survivors)
                if topup_survivors_count > 0:
                    await database.campaigns_collection.update_one(
                        {"_id": campaign_oid},
                        {"$inc": {
                            "discovery_prospects_eligible": topup_survivors_count,
                            "discovery_prospects_rule_scored_total": topup_survivors_count,
                            "discovery_prospects_rule_scored": topup_survivors_count,
                        }},
                    )

                topup_survivor_pids = {p["_id"] for p in topup_survivors}
                new_enr_docs = await database.campaign_enrollments_collection.find(
                    {"campaign_id": campaign_oid, "prospect_id": {"$in": new_pids}}
                ).to_list(length=len(new_pids))
                # Only add survivors to all_enrollments; disqualify capped-out ones
                for enr in new_enr_docs:
                    pid_e = enr.get("prospect_id")
                    if pid_e not in topup_survivor_pids:
                        continue
                    rs_e = rule_score_by_pid.get(pid_e)
                    if rs_e is not None:
                        enr["campaign_rule_score"] = float(rs_e)
                        topup_enr_ops_inner.append(_UpdateOne(
                            {"_id": enr["_id"]},
                            {"$set": {"campaign_rule_score": float(rs_e)}},
                        ))
                if topup_enr_ops_inner:
                    await database.campaign_enrollments_collection.bulk_write(topup_enr_ops_inner, ordered=False)

                existing_enr_ids = {e["_id"] for e in all_enrollments}
                surviving_enr_docs = [e for e in new_enr_docs if e["_id"] not in existing_enr_ids and e.get("prospect_id") in topup_survivor_pids]
                all_enrollments = all_enrollments + surviving_enr_docs
                assignments, skip_reasons = plan_channel_assignments(campaign_refreshed, all_enrollments, prospects_by_id)
                if skip_reasons:
                    logger.info(f"[Campaign {campaign_id}] Top-up {_topup + 1} planner skips: {skip_reasons}")

                await disc_log.log(
                    phase="enrich", event="top_up_iteration",
                    iteration=_topup + 1,
                    new_scraped=len(more_prospects),
                    total_assigned_now=len(assignments),
                    target=target_count,
                    skip_reasons=skip_reasons or {},
                    actor_yields=actor_yields,
                )

            except Exception as _topup_err:
                logger.warning(f"[Campaign {campaign_id}] Top-up attempt {_topup + 1} error: {_topup_err}")
                await disc_log.warning("enrich", "top_up_error", iteration=_topup + 1, error=str(_topup_err))
                break

        # --- QW-6: Near-miss recovery on under-fill ---
        # After the main top-up loop exhausts, if we're still below 80% target,
        # enroll prospects that scored just below min_score_to_enroll (within 10pts).
        if len(assignments) < 0.8 * target_count and _near_miss_candidates:
            logger.info(
                f"[Campaign {campaign_id}] QW-6 near-miss recovery: "
                f"{len(assignments)}/{target_count} assigned, "
                f"adding {len(_near_miss_candidates)} near-miss prospects "
                f"(score {_near_miss_min_score}–{_min_enroll_score - 1})"
            )
            _nm_docs = [p for p, _ in _near_miss_candidates]
            await _enroll_prospects(campaign_refreshed, _nm_docs)
            for _nm_p, _nm_rs in _near_miss_candidates:
                rule_score_by_pid[_nm_p["_id"]] = _nm_rs
                prospects_by_id[_nm_p["_id"]] = _nm_p
            _nm_pids = [p["_id"] for p, _ in _near_miss_candidates]
            _nm_enr_docs = await database.campaign_enrollments_collection.find(
                {"campaign_id": campaign_oid, "prospect_id": {"$in": _nm_pids}}
            ).to_list(length=len(_nm_pids))
            _nm_enr_ops = []
            for _nm_enr in _nm_enr_docs:
                _nm_pid = _nm_enr.get("prospect_id")
                _nm_rs = rule_score_by_pid.get(_nm_pid)
                if _nm_rs is not None:
                    _nm_enr["campaign_rule_score"] = float(_nm_rs)
                    _nm_enr_ops.append(_UpdateOne(
                        {"_id": _nm_enr["_id"]},
                        {"$set": {"campaign_rule_score": float(_nm_rs)}},
                    ))
            if _nm_enr_ops:
                await database.campaign_enrollments_collection.bulk_write(_nm_enr_ops, ordered=False)
            _existing_ids = {e["_id"] for e in all_enrollments}
            all_enrollments = all_enrollments + [e for e in _nm_enr_docs if e["_id"] not in _existing_ids]
            assignments, skip_reasons = plan_channel_assignments(
                campaign_refreshed, all_enrollments, prospects_by_id,
                min_score=_near_miss_min_score,
            )
            await disc_log.log(
                phase="enrich", event="near_miss_recovery",
                near_miss_added=len(_nm_enr_docs),
                relaxed_min_score=_near_miss_min_score,
                new_assignments=len(assignments),
            )

        # --- MW-1: ICP relaxation ladder on persistent under-fill ---
        # After near-miss recovery, if still below 80%, progressively relax ICP
        # and run additional Apify rounds (max 3 relaxation steps).
        if len(assignments) < 0.8 * target_count:
            for _relax_step in range(1, 4):
                if len(assignments) >= 0.8 * target_count:
                    break
                _relaxed_icp = _build_relaxed_icp(icp, _relax_step)
                _relax_deficit = target_count - len(assignments)
                _relax_needed = max(math.ceil(_relax_deficit * 2), 50)
                logger.info(
                    f"[Campaign {campaign_id}] MW-1 ICP relaxation step {_relax_step}: "
                    f"assigned={len(assignments)}/{target_count}, fetching {_relax_needed}"
                )
                try:
                    _relax_prospects, _relax_yields = await _run_apify_discovery(
                        _relaxed_icp, _relax_needed, account_oid,
                        leads_finder_page=settings.score_topup_max_iterations + _relax_step,
                        campaign=campaign_refreshed,
                    )
                    if not _relax_prospects:
                        break
                    await _pre_enroll_prospects(campaign_refreshed, _relax_prospects)
                    _relax_pids = [p["_id"] for p in _relax_prospects if p.get("_id")]
                    _relax_docs = await database.prospects_collection.find(
                        {"_id": {"$in": _relax_pids}}
                    ).to_list(length=len(_relax_pids))

                    _relax_score_ops: list = []
                    _relax_cam_ops: list = []
                    _relax_passing: list = []
                    _relax_threshold = max(_min_enroll_score - 5 * _relax_step, 35)
                    for _rp in _relax_docs:
                        try:
                            _rrs = score_prospect_for_campaign(_rp, campaign_refreshed)
                        except Exception:
                            continue
                        rule_score_by_pid[_rp["_id"]] = _rrs
                        prospects_by_id[_rp["_id"]] = _rp
                        _rcr = compute_campaign_score(
                            _rp, campaign_refreshed,
                            profile=_rp.get("linkedin_profile_data"),
                            company=_rp.get("company_data"),
                        )
                        _relax_cam_ops.append(_UpdateOne_cam_score(
                            {"_id": _rp["_id"]},
                            {"$set": {
                                "ai_prospect_score": _rcr["fit_score"],
                                "prospect_score": _rcr["fit_score"],
                                "priority_tier": _rcr["priority_tier"],
                                "ai_assessment": _rcr,
                                "enrichment_status": "ai_assessed",
                                "relaxed_icp_step": _relax_step,
                            }},
                        ))
                        _relax_score_ops.append(_UpdateOne(
                            {"_id": _rp["_id"]},
                            {"$set": {"last_campaign_rule_score": float(_rrs), "last_scored_at": datetime.utcnow()}},
                        ))
                        if _rcr["fit_score"] >= _relax_threshold:
                            _relax_passing.append((_rp, _rrs))
                    if _relax_score_ops:
                        await database.prospects_collection.bulk_write(_relax_score_ops, ordered=False)
                    if _relax_cam_ops:
                        for _ci in range(0, len(_relax_cam_ops), 200):
                            await database.prospects_collection.bulk_write(
                                _relax_cam_ops[_ci:_ci + 200], ordered=False
                            )

                    if _relax_passing:
                        _relax_enr_docs = await database.campaign_enrollments_collection.find(
                            {"campaign_id": campaign_oid,
                             "prospect_id": {"$in": [p["_id"] for p, _ in _relax_passing]}}
                        ).to_list(length=len(_relax_passing))
                        _relax_enr_ops: list = []
                        for _re in _relax_enr_docs:
                            _re_pid = _re.get("prospect_id")
                            _re_rs = rule_score_by_pid.get(_re_pid)
                            if _re_rs is not None:
                                _re["campaign_rule_score"] = float(_re_rs)
                                _relax_enr_ops.append(_UpdateOne(
                                    {"_id": _re["_id"]},
                                    {"$set": {"campaign_rule_score": float(_re_rs)}},
                                ))
                        if _relax_enr_ops:
                            await database.campaign_enrollments_collection.bulk_write(_relax_enr_ops, ordered=False)
                        _existing_ids = {e["_id"] for e in all_enrollments}
                        _relax_pids_set = {p["_id"] for p, _ in _relax_passing}
                        all_enrollments = all_enrollments + [
                            e for e in _relax_enr_docs
                            if e["_id"] not in _existing_ids and e.get("prospect_id") in _relax_pids_set
                        ]
                        assignments, skip_reasons = plan_channel_assignments(campaign_refreshed, all_enrollments, prospects_by_id)

                    await disc_log.log(
                        phase="enrich", event="icp_relaxation_step",
                        step=_relax_step,
                        new_scraped=len(_relax_prospects),
                        passing=len(_relax_passing),
                        total_assigned_now=len(assignments),
                        target=target_count,
                        actor_yields=_relax_yields,
                        threshold_used=_relax_threshold,
                    )
                except Exception as _relax_err:
                    logger.warning(f"[Campaign {campaign_id}] ICP relaxation step {_relax_step} error: {_relax_err}")
                    await disc_log.warning("enrich", "icp_relaxation_error", step=_relax_step, error=str(_relax_err))
                    break

        await database.campaigns_collection.update_one(
            {"_id": campaign_oid},
            {"$set": {"discovery_prospects_planned": len(assignments)}},
        )

        # Safety check: if the planner over-assigned Day 1 beyond the per-channel caps,
        # trim the excess and push it to Day 2 so the first send day isn't blown.
        day1_cap_total = sum(caps.get(c, 0) for c in ("linkedin_connection", "email", "linkedin_inmail"))
        day1_assignments = [(enr, ch, sd) for enr, ch, sd in assignments if sd == 1]
        if len(day1_assignments) > day1_cap_total:
            logger.warning(
                f"[Campaign {campaign_id}] Day-1 over-assignment: "
                f"{len(day1_assignments)} > cap {day1_cap_total}; trimming to cap."
            )
            day1_assignments_sorted = sorted(
                day1_assignments,
                key=lambda x: rule_score_by_pid.get(x[0].get("prospect_id"), 0),
                reverse=True,
            )
            overflow_enr_ids = {x[0]["_id"] for x in day1_assignments_sorted[day1_cap_total:]}
            assignments = [
                (enr, ch, 2 if sd == 1 and enr["_id"] in overflow_enr_ids else sd)
                for enr, ch, sd in assignments
            ]

        plan_ops = []
        assigned_ids: set = set()
        for enr, channel, send_day in assignments:
            assigned_ids.add(enr["_id"])
            # Only Day-1 enrollments are queued for upfront message generation.
            # Day 2+ stay "scheduled_later" — the approval of Day N triggers
            # background generation for Day N+1.
            msg_status = "scheduled_later"
            plan_ops.append(_UpdateOne(
                {"_id": enr["_id"]},
                {"$set": {
                    "smart_campaign_channel": channel,
                    "smart_campaign_send_day": send_day,
                    "status": "active",
                    # Clear next_action_at — engine must NOT dispatch until
                    # that day is approved. approve_day will set this.
                    "next_action_at": None,
                    "smart_campaign_scheduled_utc": None,
                    "message_gen_status": msg_status,
                    "generated_messages": None,
                    "message_gen_error": None,
                    "campaign_rule_score": float(rule_score_by_pid.get(enr.get("prospect_id")) or 0),
                }},
            ))
        # Any enrollment that couldn't be assigned a channel/day gets marked
        # so the UI can surface it as skipped.
        for enr in all_enrollments:
            if enr["_id"] not in assigned_ids:
                plan_ops.append(_UpdateOne(
                    {"_id": enr["_id"]},
                    {"$set": {
                        "status": "skipped_no_channel",
                        "smart_campaign_channel": None,
                        "smart_campaign_send_day": None,
                        "next_action_at": None,
                        "smart_campaign_scheduled_utc": None,
                        # Prevent the message generator from picking these up:
                        # _enroll_prospects seeded them as "pending".
                        "message_gen_status": "skipped",
                    }},
                ))
        if plan_ops:
            await database.campaign_enrollments_collection.bulk_write(plan_ops, ordered=False)

        # Seed flow_state for every assigned enrollment (AI-free pass — just initializes
        # the state machine so the engine knows which node to dispatch on first send).
        from services.flow_engine import build_initial_state, get_default_flow
        _campaign_for_seed = await database.campaigns_collection.find_one({"_id": campaign_oid})
        _flow = (_campaign_for_seed or {}).get("follow_up_flow")
        if not _flow:
            _flow = get_default_flow({}, _campaign_for_seed or {})
            await database.campaigns_collection.update_one(
                {"_id": campaign_oid}, {"$set": {"follow_up_flow": _flow}}
            )
        _prospects_for_seed: dict = {}
        for enr, ch, sd in assignments:
            pid = enr.get("prospect_id")
            if pid and pid not in _prospects_for_seed:
                p = await database.prospects_collection.find_one({"_id": pid})
                if p:
                    _prospects_for_seed[pid] = p
        seed_ops = []
        for enr, ch, sd in assignments:
            pid = enr.get("prospect_id")
            p = _prospects_for_seed.get(pid, {})
            state = build_initial_state(enr, _flow, p)
            # If flow decided no viable channel, mark skipped
            if state.get("current_node_id") == "STOP" and state.get("stopped_reason") == "no_viable_channel":
                seed_ops.append(_UpdateOne(
                    {"_id": enr["_id"]},
                    {"$set": {"status": "skipped_no_channel", "flow_state": state}},
                ))
            else:
                seed_ops.append(_UpdateOne(
                    {"_id": enr["_id"]},
                    {"$set": {"flow_state": state, "current_step_index": 1}},
                ))
        if seed_ops:
            await database.campaign_enrollments_collection.bulk_write(seed_ops, ordered=False)

        # Count per-day totals for UI metadata
        day_totals: dict[str, dict] = {}
        day1_prospect_ids: list[str] = []
        for enr, ch, d in assignments:
            day_totals.setdefault(str(d), {}).setdefault(ch, 0)
            day_totals[str(d)][ch] += 1
            if d == 1:
                day1_prospect_ids.append(str(enr["prospect_id"]))
        logger.info(
            f"[Campaign {campaign_id}] Planned {len(assignments)} prospects across "
            f"{len(day_totals)} day(s): {day_totals}"
        )

        await disc_log.log(
            phase="enrich", event="schedule_finalized",
            total_assigned=len(assignments),
            day_count=len(day_totals),
            day_totals=day_totals,
            day1_count=len(day1_prospect_ids),
            schedule=[{
                "enrollment_id": str(enr.get("_id")),
                "prospect_id": str(enr.get("prospect_id")),
                "channel": ch,
                "send_day": sd,
                "rule_score": enr.get("campaign_rule_score"),
            } for enr, ch, sd in assignments],
        )

        # Scoring already completed synchronously above — no AI background task.
        total_scored_count = len(all_for_scoring)

        # --- Step G: Finalize ---
        # discovery_prospects_enrolled = every assigned prospect (not just Day 1).
        # The Prospects tab shows all of these; the Schedule tab paginates by day.
        day1_enrolled_count = len(day1_prospect_ids)
        total_assigned = len(assignments)
        prospect_count_target = campaign.get("prospect_count_target", 300)
        under_fill = total_assigned < 0.8 * prospect_count_target
        discovery_warning = (
            {"code": "icp_too_narrow", "found": total_assigned, "target": prospect_count_target}
            if under_fill
            else None
        )

        await database.campaigns_collection.update_one(
            {"_id": campaign_oid},
            {"$set": {
                "discovery_completed_at": datetime.utcnow(),
                "discovery_status": "completed",
                "approval_status": "pending_review",
                "discovery_prospects_enrolled": total_assigned,
                "discovery_day1_enrolled": day1_enrolled_count,
                "discovery_day_totals": day_totals,
                "discovery_failure_reason": "under_fill" if under_fill else None,
                "discovery_warning": discovery_warning,
                "approved_send_days": [],
            }},
        )
        await _recompute_companies_count(campaign_oid)
        if under_fill:
            logger.warning(
                f"[Campaign {campaign_id}] Under-fill: {total_assigned}/{prospect_count_target} "
                f"prospects assigned (< 80% target). Consider broadening ICP filters."
            )

        # Scoring ran synchronously above; no AI background task needed.
        logger.info(
            f"[Campaign {campaign_id}] Finalized: {total_assigned} enrolled across "
            f"{len(day_totals)} days, generating Day-1 messages for {day1_enrolled_count}"
        )
        await disc_log.log(
            phase="enrich", event="enrich_complete",
            total_enrolled=total_assigned,
            day1_enrolled=day1_enrolled_count,
            day_count=len(day_totals),
            under_fill=under_fill,
        )
        await disc_log.finalize({
            "status": "completed",
            "total_enrolled": total_assigned,
            "day1_enrolled": day1_enrolled_count,
            "day_totals": day_totals,
            "under_fill": under_fill,
        })

    except Exception as e:
        logger.error(f"[Campaign {campaign_id}] _enrich_and_finalize_discovery failed: {e}", exc_info=True)
        await disc_log.error("enrich", "enrich_failed", exc=e)
        await disc_log.finalize({"status": "failed", "phase": "enrich", "error": str(e)})
        await database.campaigns_collection.update_one(
            {"_id": campaign_oid},
            {"$set": {
                "discovery_status": "failed",
                "discovery_error": str(e),
            }},
        )
    finally:
        await disc_log.__aexit__(None, None, None)


async def _generate_messages_background(campaign_id: str, account_id: str):
    """Fire-and-forget wrapper for message generation. Sets pending_review when done."""
    campaign_oid = ObjectId(campaign_id)
    try:
        from services.campaign_message_generator_service import generate_messages_for_campaign
        await generate_messages_for_campaign(campaign_id, account_id)
    except Exception as e:
        logger.error(f"[Campaign {campaign_id}] Background message generation failed: {e}", exc_info=True)
        return

    # Check if any enrollments have generated messages
    done_count = await database.campaign_enrollments_collection.count_documents({
        "campaign_id": campaign_oid,
        "message_gen_status": "done",
    })
    fail_count = await database.campaign_enrollments_collection.count_documents({
        "campaign_id": campaign_oid,
        "message_gen_status": "failed",
    })
    total_count = await database.campaign_enrollments_collection.count_documents({
        "campaign_id": campaign_oid,
    })

    if done_count == 0:
        if total_count == 0:
            error_msg = "No prospects were found or enrolled for this campaign."
        else:
            error_msg = f"Message generation produced no results for {total_count} enrolled prospects."
        logger.warning(f"[Campaign {campaign_id}] Message generation failed: {error_msg}")
        await database.campaigns_collection.update_one(
            {"_id": campaign_oid},
            {"$set": {
                "message_gen_status": "failed",
                "message_gen_error": error_msg,
            }},
        )
        return

    # Only promote to pending_review when all messages done OR no failures recorded
    if done_count == total_count or fail_count == 0:
        logger.info(f"[Campaign {campaign_id}] Message generation complete ({done_count}/{total_count}). Ready for review.")
        await database.campaigns_collection.update_one(
            {"_id": campaign_oid},
            {"$set": {
                "approval_status": "pending_review",
                "updated_at": datetime.utcnow(),
            }},
        )
    else:
        logger.warning(
            f"[Campaign {campaign_id}] Partial message gen: {done_count}/{total_count} done, "
            f"{fail_count} failed — staying in current approval_status until retried."
        )


async def _get_already_enrolled_prospect_ids(
    account_id: ObjectId,
    campaign_id: ObjectId | None = None,
    cooldown_days: int = 90,
) -> set:
    """
    Return set of prospect ObjectIds to exclude from DB discovery:
    1. Prospects already enrolled in THIS campaign (to avoid duplicates).
    2. Prospects contacted in ANY campaign in the last `cooldown_days` days
       (to avoid re-messaging tired leads).
    """
    from datetime import timedelta
    result: set = set()

    # 1. Exclude prospects already in this specific campaign
    if campaign_id is not None:
        query: dict = {
            "account_id": account_id,
            "campaign_id": campaign_id,
            "status": {"$in": ["active", "enrolled", "scoring", "paused"]},
        }
        cursor = database.campaign_enrollments_collection.find(query, {"prospect_id": 1})
        async for doc in cursor:
            pid = doc.get("prospect_id")
            if pid:
                result.add(pid if isinstance(pid, ObjectId) else ObjectId(str(pid)))

    # 2. Exclude prospects contacted in any campaign within cooldown window
    cooldown_cutoff = datetime.utcnow() - timedelta(days=cooldown_days)
    contacted_cursor = database.prospects_collection.find(
        {
            "account_id": account_id,
            "last_contacted_at": {"$gte": cooldown_cutoff},
        },
        {"_id": 1},
    )
    async for doc in contacted_cursor:
        pid = doc.get("_id")
        if pid:
            result.add(pid if isinstance(pid, ObjectId) else ObjectId(str(pid)))

    return result


def _build_icp_from_campaign(campaign: dict) -> dict:
    """Extract ICP dict from campaign fields."""
    return {
        "industries": campaign.get("icp_industries", []),
        "job_titles": campaign.get("icp_job_titles", []),
        "seniority_levels": campaign.get("icp_seniority_levels", []),
        "company_size_min": campaign.get("icp_company_size_min"),
        "company_size_max": campaign.get("icp_company_size_max"),
        "countries": campaign.get("icp_countries", []),
        "apify_params": campaign.get("icp_apify_params") or {},
        "keywords": campaign.get("icp_keywords", []),
        "exclude_keywords": campaign.get("icp_exclude_keywords", []),
        "exclude_industries": campaign.get("icp_exclude_industries", []),
        "max_prospects_per_company": campaign.get("max_prospects_per_company", 3),
        "functional_departments": campaign.get("icp_functional_departments", []),
        "funding_stages": campaign.get("icp_funding_stages", []),
        "revenue_min": campaign.get("icp_revenue_min"),
        "revenue_max": campaign.get("icp_revenue_max"),
        "cities": campaign.get("icp_cities", []),
    }


def _apply_icp_filters(query: dict, icp: dict, exclude_ids: set) -> None:
    """Mutate query dict to add ICP-based filters using semantic synonym expansion."""
    from services.icp_synonyms import expand_seniorities, expand_industry_regexes, expand_job_title_query

    if exclude_ids:
        query["_id"] = {"$nin": list(exclude_ids)}

    # Industry filter — expand synonyms (SaaS → computer software, internet, etc.)
    if icp.get("industries"):
        try:
            industry_patterns = expand_industry_regexes(icp["industries"])
            query["industry"] = {"$in": industry_patterns}
        except Exception:
            query["industry"] = {"$in": icp["industries"]}

    # Seniority filter — expand equivalence groups (c_suite → founder, owner, ceo, ...)
    if icp.get("seniority_levels"):
        all_variants = expand_seniorities(icp["seniority_levels"])
        query["seniority_level"] = {"$in": all_variants}

    # Country filter
    if icp.get("countries"):
        query["country"] = {"$in": icp["countries"]}

    # Company size filter
    size_filter = {}
    if icp.get("company_size_min"):
        size_filter["$gte"] = icp["company_size_min"]
    if icp.get("company_size_max"):
        size_filter["$lte"] = icp["company_size_max"]
    if size_filter:
        query["company_size"] = size_filter

    # Job title filter — semantic expansion (Marketing Director → Head of Marketing, CMO, etc.)
    if icp.get("job_titles"):
        title_regex = expand_job_title_query(icp["job_titles"])
        if title_regex:
            query["job_title"] = {"$regex": title_regex, "$options": "i"}

    # Functional department filter — prefer functional_level exact match, fall back to job_title regex
    if icp.get("functional_departments"):
        from services.icp_synonyms import FUNCTION_KEYWORDS
        try:
            from services.icp_synonyms import to_leads_finder_functional
            fl_tokens = sorted({
                t for d in icp["functional_departments"]
                if (t := to_leads_finder_functional(d))
            })
        except (ImportError, AttributeError):
            fl_tokens = []

        kw_alts: list[str] = []
        for d in icp["functional_departments"]:
            norm = d.lower().strip().replace("-", "_").replace(" ", "_")
            for kw in FUNCTION_KEYWORDS.get(norm, [norm]):
                kw_alts.append(re.escape(kw))

        or_clauses: list[dict] = []
        if fl_tokens:
            or_clauses.append({"functional_level": {"$in": fl_tokens}})
        if kw_alts:
            or_clauses.append({"job_title": {"$regex": "|".join(kw_alts), "$options": "i"}})
        if or_clauses:
            query.setdefault("$and", []).append({"$or": or_clauses})

    # Keyword filter — positive signal: at least one ICP keyword must appear in
    # the prospect's title, headline, company_keywords, or company_description.
    if icp.get("keywords"):
        kw_pattern = "|".join(re.escape(k) for k in icp["keywords"] if k and k.strip())
        if kw_pattern:
            query.setdefault("$and", []).append({"$or": [
                {"job_title":           {"$regex": kw_pattern, "$options": "i"}},
                {"headline":            {"$regex": kw_pattern, "$options": "i"}},
                {"company_keywords":    {"$regex": kw_pattern, "$options": "i"}},
                {"company_description": {"$regex": kw_pattern, "$options": "i"}},
            ]})


async def _query_existing_prospects(
    account_id: ObjectId,
    icp: dict,
    limit: int,
    exclude_ids: set,
) -> list[dict]:
    """
    Phase 1: Query MongoDB for ICP-matching prospects.

    Intentionally permissive — we want to reach for the DB first before burning
    Apify credits on fresh scraping. No AI-score gate, and no enrichment-status
    gate either: un-enriched prospects will simply pick up enrichment in Phase
    3 alongside any Apify leads.
    """
    query: dict = {
        "account_id": account_id,
        "status": {"$nin": ["opted_out", "bounced", "disqualified"]},
    }
    _apply_icp_filters(query, icp, exclude_ids)

    # Sort so that the best-signal prospects come first: scored prospects sorted
    # by score desc, then un-scored fall to the tail sorted by most-recent update.
    cursor = (
        database.prospects_collection.find(query)
        .sort([("ai_prospect_score", -1), ("last_updated_at", -1)])
        .limit(limit)
    )
    return await cursor.to_list(length=limit)


async def _query_unenriched_prospects(
    account_id: ObjectId,
    icp: dict,
    limit: int,
    exclude_ids: set,
) -> list[dict]:
    """
    Phase 1 fallback: find ICP-matching prospects that exist in DB but were
    never enriched or had enrichment fail. These will be sent through
    mini-enrichment before being considered for enrollment.
    """
    query: dict = {
        "account_id": account_id,
        "enrichment_status": {"$in": ["not_started", "failed"]},
        "status": {"$nin": ["opted_out", "bounced", "disqualified"]},
    }
    _apply_icp_filters(query, icp, exclude_ids)

    cursor = (
        database.prospects_collection.find(query)
        .sort("created_at", -1)  # Most recent first (no score to sort by)
        .limit(limit)
    )
    return await cursor.to_list(length=limit)


async def _query_scraped_prospects_relaxed(
    account_id: ObjectId,
    icp: dict,
    limit: int,
    exclude_ids: set,
) -> list[dict]:
    """
    Relaxed fallback: return recently-scraped prospects regardless of AI score.
    Used when mini-enrichment fails (rate limits) to still populate the campaign.
    """
    query: dict = {
        "account_id": account_id,
        "enrichment_status": {"$in": ["profile_scraped", "failed", "not_started", "in_progress"]},
        "status": {"$nin": ["opted_out", "bounced", "disqualified"]},
    }
    _apply_icp_filters(query, icp, exclude_ids)
    cursor = (
        database.prospects_collection.find(query)
        .sort("created_at", -1)
        .limit(limit)
    )
    return await cursor.to_list(length=limit)


async def _query_industry_only_prospects(
    account_id: ObjectId,
    icp: dict,
    limit: int,
    exclude_ids: set,
) -> list[dict]:
    """
    Phase 1c fallback: match prospects by industry only — no country/seniority/size gate.
    Catches bare/CSV-imported prospects that lack structured ICP fields.
    """
    query: dict = {
        "account_id": account_id,
        "enrichment_status": {"$in": ["not_started", "failed"]},
        "status": {"$nin": ["opted_out", "bounced", "disqualified"]},
    }
    if exclude_ids:
        query["_id"] = {"$nin": list(exclude_ids)}
    if icp.get("industries"):
        from services.icp_synonyms import expand_industry_regexes
        try:
            industry_patterns = expand_industry_regexes(icp["industries"])
            query["industry"] = {"$in": industry_patterns}
        except Exception:
            query["industry"] = {"$in": icp["industries"]}
    cursor = (
        database.prospects_collection.find(query)
        .sort("created_at", -1)
        .limit(limit)
    )
    return await cursor.to_list(length=limit)


async def _query_companies_from_db(
    account_id: str,
    icp_industries: list,
    icp_countries: list,
    icp_company_size_min: int | None,
    icp_company_size_max: int | None,
) -> list[str]:
    """Returns list of distinct company_linkedin URLs matching company-level ICP criteria."""
    query: dict = {"account_id": ObjectId(account_id)}

    if icp_industries:
        from services.icp_synonyms import expand_industry_terms
        expanded = expand_industry_terms(icp_industries)
        industry_regexes = [{"industry": {"$regex": re.escape(ind), "$options": "i"}} for ind in expanded]
        query["$or"] = industry_regexes

    if icp_countries:
        country_regexes = [{"country": {"$regex": c, "$options": "i"}} for c in icp_countries]
        existing_and = query.get("$and", [])
        existing_and.append({"$or": country_regexes})
        query["$and"] = existing_and

    if icp_company_size_min or icp_company_size_max:
        size_filter: dict = {}
        if icp_company_size_min:
            size_filter["$gte"] = icp_company_size_min
        if icp_company_size_max:
            size_filter["$lte"] = icp_company_size_max
        query["company_size"] = size_filter

    # Get distinct company_linkedin values
    company_urls = await database.prospects_collection.distinct("company_linkedin", query)
    # Filter out None/empty values
    return [url for url in company_urls if url]


async def _query_prospects_from_companies(
    company_urls: list[str],
    account_id: str,
    campaign: dict,
    limit: int = 1000,
) -> list[dict]:
    """Query prospects within matched companies, applying person-level ICP filters."""
    query: dict = {
        "account_id": ObjectId(account_id),
        "company_linkedin": {"$in": company_urls},
        "ai_prospect_score": {"$gte": 60},
        "enrichment_status": {"$in": ["completed", "ai_assessed", "profile_scraped", "company_scraped"]},
        "status": {"$nin": ["opted_out", "bounced", "disqualified"]},
    }

    icp_seniority = campaign.get("icp_seniority_levels", [])
    if icp_seniority:
        query["seniority_level"] = {"$in": [s.lower().strip() for s in icp_seniority]}

    icp_job_titles = campaign.get("icp_job_titles", [])
    if icp_job_titles:
        title_regexes = [{"job_title": {"$regex": t, "$options": "i"}} for t in icp_job_titles]
        query.setdefault("$or", title_regexes)

    cursor = database.prospects_collection.find(query).sort("ai_prospect_score", -1).limit(limit)
    return await cursor.to_list(length=limit)


def _perturb_icp_for_topup(icp: dict, iteration: int) -> dict:
    """
    Return a copy of `icp` with slightly varied apify_params per top-up iteration
    so Lead Scraper (no pagination) returns a different Apollo slice each round.

    iteration=0: expand seniority by one adjacent level
    iteration=1: expand seniority + drop city restriction
    iteration≥2: rotate industry list + drop city
    """
    import copy
    varied = copy.deepcopy(icp)
    apify_params = dict(varied.get("apify_params") or {})

    _ADJACENT: dict[str, list[str]] = {
        "manager":   ["director", "senior"],
        "director":  ["manager", "vp"],
        "vp":        ["director", "c_suite"],
        "senior":    ["manager"],
        "c_suite":   ["vp", "founder"],
        "founder":   ["owner", "c_suite"],
        "owner":     ["founder", "partner"],
        "partner":   ["owner", "vp"],
        "head":      ["director", "vp"],
    }

    seniority = list(apify_params.get("seniority_level") or [])
    if seniority:
        expanded = list(seniority)
        for s in seniority:
            for adj in _ADJACENT.get(s, []):
                if adj not in expanded:
                    expanded.append(adj)
        apify_params = {**apify_params, "seniority_level": expanded}

    if iteration >= 1:
        apify_params.pop("contact_city", None)

    if iteration >= 2:
        industries = list(apify_params.get("company_industry") or [])
        if len(industries) > 1:
            offset = (iteration - 1) % len(industries)
            apify_params = {**apify_params, "company_industry": industries[offset:] + industries[:offset]}

    varied["apify_params"] = apify_params
    return varied


def _build_relaxed_icp(icp: dict, step: int) -> dict:
    """
    Return a genuinely relaxed copy of `icp` for MW-1 ICP relaxation ladder.
    Each step widens constraints further.

    step=1: drop city filter
    step=2: also widen seniority by ±1 level
    step=3: also widen country to APAC/regional equivalents (or drop industry)
    """
    import copy
    _REGION_EXPANSIONS: dict[str, list[str]] = {
        "australia":      ["australia", "new zealand", "singapore"],
        "united kingdom": ["united kingdom", "ireland"],
        "canada":         ["canada", "united states"],
        "germany":        ["germany", "austria", "switzerland"],
        "india":          ["india", "singapore", "united arab emirates"],
        "singapore":      ["singapore", "malaysia", "australia"],
        "united states":  ["united states", "canada"],
    }
    _ADJACENT: dict[str, list[str]] = {
        "manager":  ["director", "senior"],
        "director": ["manager", "vp"],
        "vp":       ["director", "c_suite"],
        "senior":   ["manager"],
        "c_suite":  ["vp", "founder"],
        "founder":  ["owner", "c_suite"],
        "head":     ["director", "vp"],
    }

    relaxed = copy.deepcopy(icp)
    apify_params = dict(relaxed.get("apify_params") or {})

    # step 1+: drop city
    apify_params.pop("contact_city", None)
    relaxed["cities"] = []

    if step >= 2:
        # widen seniority
        seniority = list(apify_params.get("seniority_level") or [])
        expanded = list(seniority)
        for s in seniority:
            for adj in _ADJACENT.get(s, []):
                if adj not in expanded:
                    expanded.append(adj)
        if expanded:
            apify_params["seniority_level"] = expanded

    if step >= 3:
        # widen country to regional peers or drop industry keyword restrictions
        countries = list(relaxed.get("countries") or [])
        expanded_countries: list[str] = []
        for c in countries:
            regional = _REGION_EXPANSIONS.get(c.lower())
            if regional:
                for r in regional:
                    if r not in expanded_countries:
                        expanded_countries.append(r)
            else:
                if c not in expanded_countries:
                    expanded_countries.append(c)
        if expanded_countries:
            relaxed["countries"] = expanded_countries
        # also drop keyword exclusions — they can be too restrictive
        apify_params.pop("company_not_keywords", None)
        relaxed["exclude_keywords"] = []

    relaxed["apify_params"] = apify_params
    return relaxed


async def _run_apify_discovery(
    icp: dict,
    needed_count: int,
    account_id: ObjectId,
    industry_id: str | None = None,
    leads_finder_page: int = 0,
    run_lead_scraper: bool = True,
    run_leads_finder: bool = True,
    campaign: dict | None = None,
) -> tuple[list[dict], dict]:  # returns (docs, per_actor_yields)
    """
    Phase 2: Run Lead Scraper (T1XDXWc1L92AfIJtd) + LEADS_FINDER (IoSHqwTR9YGhzccez)
    in parallel for diversified results and higher yield.
    - Lead Scraper: Apollo-style, verified emails, ~500/call, uses 'functional' (Title-Case)
    - LEADS_FINDER: broader title matching, supports startPage pagination, city-level targeting,
      funding/revenue filters, uses 'functional_level' (lowercase)
    Both actors are first-class and run on every discovery pass.
    Returns (upserted_docs, yield_tracker) where yield_tracker tracks per-actor unique yields.
    """
    import threading
    from services.apify_service import (
        build_lead_scraper_input, iter_lead_scraper,
        build_actor_input, iter_leads_finder,
    )

    sentinel_count = (1 if run_lead_scraper else 0) + (1 if run_leads_finder else 0)
    if sentinel_count == 0:
        return ([], {"lead_scraper": 0, "leads_finder": 0})

    apify_params = icp.get("apify_params") or _icp_to_apify_params(icp)
    locations = icp.get("countries") or []
    exclude_kw = [k.lower().strip() for k in (icp.get("exclude_keywords") or []) if k.strip()]

    per_actor = min(needed_count, 1200)

    lead_scraper_input = build_lead_scraper_input(
        apify_params, locations=locations, fetch_count=per_actor
    )
    leads_finder_input = build_actor_input(
        apify_params, fetch_count=per_actor, start_page=leads_finder_page
    )

    upserted_docs: list[dict] = []
    seen_keys: set = set()
    loop = asyncio.get_event_loop()
    yield_tracker: dict = {"lead_scraper": 0, "leads_finder": 0}

    # Shared queue; producers signal with (None, label) sentinel each
    queue: asyncio.Queue = asyncio.Queue()

    def make_producer(actor_fn, actor_input, label):
        def producer():
            try:
                for item in actor_fn(actor_input):
                    loop.call_soon_threadsafe(queue.put_nowait, (item, label))
            except Exception as exc:
                logger.exception(f"[{label}] Apify producer crashed: {exc}")
            finally:
                loop.call_soon_threadsafe(queue.put_nowait, (None, label))
        return producer

    started_threads = []
    if run_lead_scraper:
        t1 = threading.Thread(target=make_producer(iter_lead_scraper, lead_scraper_input, "lead_scraper"), daemon=True)
        t1.start()
        started_threads.append(t1)
    if run_leads_finder:
        t2 = threading.Thread(target=make_producer(iter_leads_finder, leads_finder_input, "leads_finder"), daemon=True)
        t2.start()
        started_threads.append(t2)

    sentinels_received = 0
    while sentinels_received < sentinel_count:
        item, label = await queue.get()
        if item is None:
            sentinels_received += 1
            continue

        email = (item.get("email") or "").strip().lower() or None
        linkedin = (item.get("linkedin") or item.get("linkedin_url") or "").strip() or None
        key = email or linkedin
        if not key or key in seen_keys:
            continue

        # Post-scrape exclude-keyword blocklist (for lead scraper which can't filter server-side)
        if exclude_kw:
            co_name = (item.get("company_name") or item.get("organization_name") or "").lower()
            co_industry = (item.get("industry") or item.get("organization_industry") or "").lower()
            if any(ek in co_name or ek in co_industry for ek in exclude_kw):
                continue

        seen_keys.add(key)
        yield_tracker[label] = yield_tracker.get(label, 0) + 1
        actor_id_for_label = settings.apify_lead_scraper_id if label == "lead_scraper" else settings.apify_actor_id
        doc = await _upsert_single_lead(item, account_id, industry_id=industry_id, campaign=campaign, source_actor_id=actor_id_for_label)
        if doc:
            upserted_docs.append(doc)

    for t in started_threads:
        t.join(timeout=5)
    logger.info(
        f"Apify discovery complete: {len(upserted_docs)} prospects saved (page={leads_finder_page}), "
        f"yields={yield_tracker}"
    )
    return (upserted_docs, yield_tracker)


def _icp_to_apify_params(icp: dict) -> dict:
    """Map campaign ICP dict to ApifyParams format for Apify actor inputs."""
    params: dict = {}

    if icp.get("job_titles"):
        params["contact_job_title"] = icp["job_titles"]

    if icp.get("seniority_levels"):
        from services.apify_service import build_actor_input  # noqa — import _SENIORITY_MAP indirectly
        params["seniority_level"] = [s.lower().strip() for s in icp["seniority_levels"]]

    if icp.get("industries"):
        params["company_industry"] = icp["industries"]

    if icp.get("exclude_industries"):
        params["company_not_industry"] = icp["exclude_industries"]

    if icp.get("keywords"):
        keywords = [k.strip() for k in icp["keywords"] if isinstance(k, str) and k.strip() and len(k.strip()) <= 60]
        if keywords:
            params["company_keywords"] = keywords[:5]

    if icp.get("exclude_keywords"):
        excl = [k.strip() for k in icp["exclude_keywords"] if isinstance(k, str) and k.strip() and len(k.strip()) <= 60]
        if excl:
            params["company_not_keywords"] = excl[:5]

    if icp.get("countries"):
        countries = list(dict.fromkeys(
            c.lower().strip() for c in icp["countries"]
            if isinstance(c, str) and c.strip()
        ))
        if countries:
            params["contact_location"] = countries

    if icp.get("company_size_min") or icp.get("company_size_max"):
        size_parts = []
        if icp.get("company_size_min"):
            size_parts.append(str(icp["company_size_min"]))
        if icp.get("company_size_max"):
            size_parts.append(str(icp["company_size_max"]))
        if size_parts:
            params["size"] = ["-".join(size_parts)]

    if icp.get("functional_departments"):
        from services.icp_synonyms import to_lead_scraper_functional, to_leads_finder_functional
        ls_func = [f for f in (to_lead_scraper_functional(d) for d in icp["functional_departments"]) if f]
        lf_func = [f for f in (to_leads_finder_functional(d) for d in icp["functional_departments"]) if f]
        if ls_func:
            params["functional"] = list(dict.fromkeys(ls_func))
        if lf_func:
            params["functional_level"] = list(dict.fromkeys(lf_func))
    elif icp.get("job_titles"):
        from services.icp_synonyms import infer_functional_from_titles
        ls_func, lf_func = infer_functional_from_titles(icp["job_titles"])
        if ls_func:
            params["functional"] = ls_func
        if lf_func:
            params["functional_level"] = lf_func

    _existing_ap = icp.get("apify_params") or {}
    if _existing_ap.get("functional") and "functional" not in params:
        params["functional"] = _existing_ap["functional"]
    if _existing_ap.get("functional_level") and "functional_level" not in params:
        params["functional_level"] = _existing_ap["functional_level"]

    if icp.get("revenue_min"):
        params["min_revenue"] = icp["revenue_min"]
    if icp.get("revenue_max"):
        params["max_revenue"] = icp["revenue_max"]
    if icp.get("funding_stages"):
        params["funding"] = icp["funding_stages"]
    if icp.get("cities"):
        params["contact_city"] = [c.lower() for c in icp["cities"]]
        # City-level targeting is mutually exclusive with country-level for LEADS_FINDER accuracy
        params.pop("contact_location", None)

    return params


async def _upsert_single_lead(lead: dict, account_id: ObjectId, industry_id: str | None = None, campaign: dict | None = None, source_actor_id: str | None = None) -> dict | None:
    """
    Upsert a single lead to prospects collection immediately.
    Returns the upserted/existing doc with _id, or None if lead has no identifier.
    Optionally tags the prospect with an industry_id on insert.
    """
    email = (lead.get("email") or "").strip().lower() or None
    linkedin = (lead.get("linkedin") or lead.get("linkedin_url") or "").strip() or None

    if not email and not linkedin:
        return None

    now = datetime.utcnow()

    or_clauses = []
    if email:
        or_clauses.append({"email": email})
    if linkedin:
        or_clauses.append({"linkedin": linkedin})
    upsert_filter = {"$or": or_clauses} if len(or_clauses) > 1 else or_clauses[0]

    prospect_data = {
        "email": email,
        "linkedin": linkedin,
        "full_name": lead.get("full_name") or f"{lead.get('first_name', '')} {lead.get('last_name', '')}".strip(),
        "first_name": lead.get("first_name"),
        "last_name": lead.get("last_name"),
        "job_title": lead.get("job_title") or lead.get("title"),
        "headline": lead.get("headline"),
        "company_name": lead.get("company_name") or lead.get("organization_name"),
        "company_domain": lead.get("company_domain") or lead.get("primary_domain"),
        "company_linkedin": lead.get("company_linkedin") or lead.get("organization_linkedin_url"),
        "company_size": lead.get("company_size"),
        "enrichment_status": "not_started",
        "source": "search",
        "stage": "scraped",
        "last_updated_at": now,
    }
    if source_actor_id:
        prospect_data["source_actor_id"] = source_actor_id

    # Title gate (Gate A2)
    title_gate_rejected = False
    if settings.quality_gates_enabled and settings.title_gate_enabled and campaign:
        from utils.scoring import passes_title_gate
        icp_seniority = campaign.get("icp_seniority_levels", []) if campaign else []
        exclude_kw_gate = campaign.get("icp_exclude_keywords", []) if campaign else []
        gate_pass, gate_reason = passes_title_gate(prospect_data, icp_seniority, exclude_kw_gate)
        if not gate_pass:
            prospect_data["title_gate_status"] = "rejected"
            prospect_data["disqualify_reason"] = gate_reason
            title_gate_rejected = True
        else:
            prospect_data["title_gate_status"] = "passed"

    if campaign:
        try:
            from utils.scoring import score_prospect_for_campaign as _score_fn_insert
            _initial_score = _score_fn_insert(prospect_data, campaign)
            prospect_data["last_campaign_rule_score"] = float(_initial_score)
            prospect_data["last_scored_at"] = now
        except Exception as _score_err:
            logger.debug(f"Initial rule score on insert failed: {_score_err}")

    set_on_insert = {k: v for k, v in prospect_data.items() if v is not None and k != "last_updated_at"}
    set_on_insert["created_at"] = now

    result = await database.prospects_collection.find_one_and_update(
        upsert_filter,
        {"$setOnInsert": set_on_insert, "$set": {"last_updated_at": now}},
        upsert=True,
        return_document=True,
    )

    if title_gate_rejected:
        return None

    # Ensure prospect_state overlay exists for this account
    if result and account_id:
        try:
            _aid = str(account_id)
            _pid = str(result["_id"])
            _state_status = "disqualified" if title_gate_rejected else "new"
            await database.prospect_state_collection.update_one(
                {"account_id": _aid, "prospect_id": _pid},
                {"$setOnInsert": {
                    "account_id": _aid, "prospect_id": _pid,
                    "status": _state_status, "tags": [], "used_by": [],
                    "created_at": now, "last_updated_at": now,
                }},
                upsert=True,
            )
        except Exception as _pse:
            logger.debug(f"prospect_state ensure failed: {_pse}")

    return result


async def _upsert_apify_leads(leads: list[dict], account_id: ObjectId) -> list[dict]:
    """
    Bulk-upsert Apify leads to prospects collection.
    Dedup key: email (primary) or LinkedIn URL.
    Returns list of upserted/existing prospect dicts with _id.
    """
    from pymongo import UpdateOne

    now = datetime.utcnow()
    operations = []
    processed_keys: set = set()
    lead_key_map = {}

    for lead in leads:
        email = (lead.get("email") or "").strip().lower() or None
        linkedin = (lead.get("linkedin") or lead.get("linkedin_url") or "").strip() or None

        if not email and not linkedin:
            continue

        key = email or linkedin
        if key in processed_keys:
            continue
        processed_keys.add(key)

        # Build filter (email OR linkedin for dedup)
        or_clauses = []
        if email:
            or_clauses.append({"email": email})
        if linkedin:
            or_clauses.append({"linkedin": linkedin})

        upsert_filter = {"$or": or_clauses} if len(or_clauses) > 1 else or_clauses[0]

        prospect_data = {
            "account_id": account_id,
            "email": email,
            "linkedin": linkedin,
            "full_name": lead.get("full_name") or f"{lead.get('first_name', '')} {lead.get('last_name', '')}".strip(),
            "first_name": lead.get("first_name"),
            "last_name": lead.get("last_name"),
            "job_title": lead.get("job_title") or lead.get("title"),
            "headline": lead.get("headline"),
            "company_name": lead.get("company_name") or lead.get("organization_name"),
            "company_domain": lead.get("company_domain") or lead.get("primary_domain"),
            "company_website": lead.get("company_website") or lead.get("website_url"),
            "company_linkedin": lead.get("company_linkedin") or lead.get("organization_linkedin_url"),
            "industry": lead.get("industry"),
            "seniority_level": (lead.get("seniority_level") or lead.get("seniority") or "").lower() or None,
            "country": lead.get("country"),
            "city": lead.get("city"),
            "state": lead.get("state"),
            "company_size": lead.get("company_size") or lead.get("num_employees"),
            "enrichment_status": "not_started",
            "status": "new",
            "source": "search",
            "first_seen_at": now,
            "last_updated_at": now,
        }
        # Remove None values from prospect_data for cleaner upsert
        prospect_data = {k: v for k, v in prospect_data.items() if v is not None}

        operations.append(UpdateOne(
            upsert_filter,
            {"$setOnInsert": prospect_data},
            upsert=True,
        ))
        lead_key_map[key] = upsert_filter

    if not operations:
        return []

    try:
        await database.prospects_collection.bulk_write(operations, ordered=False)
    except Exception as e:
        logger.warning(f"Bulk upsert error (some may be duplicates): {e}")

    # Fetch upserted/existing docs
    or_filters = list(lead_key_map.values())
    cursor = database.prospects_collection.find({"$or": or_filters[:500]})
    return await cursor.to_list(length=500)


APIFY_PROFILE_SCRAPE_LIMIT = 100  # Top N prospects by pre-score get LinkedIn profile scraping via Apify


def _rule_based_pre_score(prospect: dict) -> float:
    """Quick rule-based pre-score for LinkedIn profile scrape prioritization."""
    score = 0.0
    seniority = (prospect.get("seniority_level") or "").lower()
    if seniority in ("c-suite", "c_suite", "csuite", "owner", "founder", "vp"):
        score += 40
    elif seniority in ("director", "head"):
        score += 30
    elif seniority in ("manager", "senior"):
        score += 20
    if prospect.get("email"):
        score += 15
    size_raw = prospect.get("company_size") or 0
    try:
        company_size = int(size_raw) if isinstance(size_raw, str) else (size_raw or 0)
    except (ValueError, TypeError):
        company_size = 0
    if company_size >= 50:
        score += 10
    revenue_raw = prospect.get("company_annual_revenue_clean") or 0
    try:
        revenue = float(revenue_raw) if isinstance(revenue_raw, str) else revenue_raw
    except (ValueError, TypeError):
        revenue = 0
    if revenue >= 1_000_000:
        score += 5
    return score


async def _recompute_companies_count(campaign_oid) -> int:
    """Count unique companies across all pre-enrolled prospects for this campaign."""
    try:
        pipeline = [
            {"$match": {"campaign_id": campaign_oid}},
            {"$lookup": {
                "from": "prospects",
                "localField": "prospect_id",
                "foreignField": "_id",
                "as": "p",
            }},
            {"$unwind": {"path": "$p", "preserveNullAndEmptyArrays": False}},
            {"$group": {"_id": {
                "$ifNull": [
                    {"$toLower": {"$ifNull": ["$p.company_linkedin", ""]}},
                    {"$ifNull": [{"$toLower": {"$ifNull": ["$p.company_name", ""]}}, "__unknown__"]},
                ]
            }}},
            {"$count": "n"},
        ]
        result = await database.campaign_enrollments_collection.aggregate(pipeline).to_list(1)
        n = result[0]["n"] if result else 0
        await database.campaigns_collection.update_one(
            {"_id": campaign_oid},
            {"$set": {"discovery_companies_found": n}},
        )
        return n
    except Exception as e:
        logger.warning(f"_recompute_companies_count error: {e}")
        return 0


async def _auto_create_industry_if_needed(
    campaign: dict, account_id: ObjectId, prospect_count: int
) -> str | None:
    """
    Auto-create an industry document from campaign params for future prospect reuse.
    Returns the industry_id string, or None if creation skipped/failed.
    """
    icp_industries = campaign.get("icp_industries") or []
    if not icp_industries:
        return None

    industry_name = icp_industries[0]

    try:
        # Build regions from campaign countries
        countries = campaign.get("icp_countries") or ["united states"]
        regions = []
        for i, country in enumerate(countries):
            regions.append({
                "name": country.lower().replace(" ", "_"),
                "locations": [country.lower()],
                "fetch_count": 50,
                "priority": i + 1,
                "is_exhausted": False,
                "exhausted_at": None,
                "pagination_state": {
                    "last_page_fetched": -1,
                    "leads_finder_exhausted": False,
                    "lead_scraper_exhausted": False,
                    "lead_scraper_consecutive_zero_runs": 0,
                },
            })

        apify_params = campaign.get("icp_apify_params") or {}
        doc = {
            "account_id": account_id,
            "name": industry_name,
            "description": f"Auto-created from campaign: {campaign.get('name', '')}",
            "is_active": True,
            "apify_base_params": apify_params,
            "regions": regions,
            "total_fetch_count": 100,
            "scrape_day": "saturday",
            "scrape_enabled": False,
            "created_at": datetime.utcnow(),
            "last_run_at": None,
            "total_runs": 0,
            "total_prospects_generated": prospect_count,
            "ai_generated": False,
            "user_edited_params": False,
            "auto_created_from_campaign": True,
            "source_campaign_id": campaign["_id"],
        }

        # Upsert to avoid race conditions — $setOnInsert only fires when creating a new doc
        filter_q = {
            "account_id": account_id,
            "name": {"$regex": f"^{re.escape(industry_name)}$", "$options": "i"},
        }
        result = await database.industries_collection.update_one(
            filter_q,
            {"$setOnInsert": doc},
            upsert=True,
        )
        if result.upserted_id:
            industry_id = str(result.upserted_id)
            logger.info(f"Auto-created industry '{industry_name}' (id={industry_id}) from campaign {campaign['_id']}")
        else:
            existing = await database.industries_collection.find_one(filter_q, {"_id": 1})
            industry_id = str(existing["_id"]) if existing else None
            logger.info(f"Industry '{industry_name}' already exists (id={industry_id}), reusing")
        return industry_id
    except Exception as e:
        logger.warning(f"Failed to auto-create industry: {e}")
        return None




async def _pre_enroll_prospects(campaign: dict, prospects: list[dict]) -> None:
    """
    Create provisional enrollment records immediately after scraping so that
    enrolled-prospects API returns them while enrichment is still running.
    status='enriching' is engine-safe (engine only runs on status='active').
    _enroll_prospects() promotes these to 'active' for top-N; the rest are deleted.
    """
    if not prospects:
        return

    campaign_oid = campaign["_id"]
    account_oid = campaign["account_id"]
    now = datetime.utcnow()

    prospect_oids = [p["_id"] for p in prospects]
    existing_cursor = database.campaign_enrollments_collection.find(
        {"campaign_id": campaign_oid, "prospect_id": {"$in": prospect_oids}},
        {"prospect_id": 1},
    )
    already_enrolled = {doc["prospect_id"] async for doc in existing_cursor}

    # MW-3: Skip prospects actively enrolled in another campaign for this account
    cross_cursor = database.campaign_enrollments_collection.find(
        {
            "account_id": account_oid,
            "campaign_id": {"$ne": campaign_oid},
            "prospect_id": {"$in": prospect_oids},
            "status": {"$in": ["active", "enrolled"]},
        },
        {"prospect_id": 1},
    )
    cross_enrolled = {doc["prospect_id"] async for doc in cross_cursor}
    if cross_enrolled:
        logger.info(
            f"[_pre_enroll] Skipping {len(cross_enrolled)} prospects already active "
            f"in another campaign for account {account_oid}"
        )

    # Check for teammate conflicts: same account, DIFFERENT user, used_by within 90 days
    from datetime import timedelta
    _cooldown_cutoff = now - timedelta(days=90)
    _account_id_str = str(account_oid)

    # campaign.get("created_by") is the current user; fall back to account_id
    _current_user_id = str(campaign.get("created_by") or campaign.get("account_id") or account_oid)

    teammate_conflict_map: dict = {}  # prospect_id -> conflict metadata
    _p_id_strs = [str(p["_id"]) for p in prospects]

    async for state_doc in database.prospect_state_collection.find(
        {
            "account_id": _account_id_str,
            "prospect_id": {"$in": _p_id_strs},
            "used_by": {
                "$elemMatch": {
                    "user_id": {"$ne": _current_user_id},  # different user
                    "$or": [
                        {"status": {"$in": ["active", "paused", "enrolled", "scoring"]}},
                        {
                            "status": "completed",
                            "completed_at": {"$gte": _cooldown_cutoff},
                        },
                    ],
                }
            },
        },
        {"prospect_id": 1, "used_by": 1},
    ):
        _pid = str(state_doc.get("prospect_id", ""))
        # Find the conflicting used_by entry (teammate)
        for _entry in (state_doc.get("used_by") or []):
            if str(_entry.get("user_id", "")) != _current_user_id:
                teammate_conflict_map[_pid] = {
                    "teammate_user_id": str(_entry.get("user_id", "")),
                    "teammate_campaign_id": str(_entry.get("campaign_id", "")),
                    "teammate_status": _entry.get("status"),
                    "teammate_enrolled_at": _entry.get("enrolled_at"),
                }
                break

    if teammate_conflict_map:
        logger.info(
            f"[_pre_enroll] {len(teammate_conflict_map)} prospects have teammate conflicts "
            f"for campaign {campaign_oid}"
        )

    docs = []
    for p in prospects:
        if p["_id"] in already_enrolled or p["_id"] in cross_enrolled:
            continue
        _pid_str = str(p["_id"])
        _conflict = teammate_conflict_map.get(_pid_str)
        _doc = {
            "campaign_id": campaign_oid,
            "account_id": account_oid,
            "prospect_id": p["_id"],
            "status": "pending_teammate_review" if _conflict else "scoring",
            "current_step": 0,
            "next_action_at": None,
            "step_history": [],
            "enrolled_at": now,
            "completed_at": None,
            "last_activity_at": None,
            "smart_campaign_channel": None,
            "smart_campaign_send_day": None,
            "smart_campaign_scheduled_utc": None,
            "generated_messages": None,
            "message_gen_status": "pending",
            "message_gen_error": None,
            "campaign_rule_score": float(p.get("ai_prospect_score") or p.get("fit_score") or p.get("prospect_score") or 0),
        }
        if _conflict:
            _doc["teammate_conflict"] = _conflict
        docs.append(_doc)

    if docs:
        try:
            result = await database.campaign_enrollments_collection.insert_many(docs, ordered=False)
            inserted = len(result.inserted_ids) if result and result.inserted_ids else len(docs)
            logger.info(f"Pre-enrolled {inserted}/{len(docs)} provisional prospects for campaign {campaign['_id']}")
        except Exception as e:
            from pymongo.errors import BulkWriteError
            if isinstance(e, BulkWriteError):
                inserted = e.details.get("nInserted", 0)
                n_errors = len(e.details.get("writeErrors", []))
                logger.error(
                    f"_pre_enroll_prospects partial insert: {inserted}/{len(docs)} inserted, "
                    f"{n_errors} errors for campaign {campaign['_id']}: {e}"
                )
            else:
                logger.error(f"_pre_enroll_prospects insert_many failed for campaign {campaign['_id']}: {e}", exc_info=True)
            await database.campaigns_collection.update_one(
                {"_id": campaign["_id"]},
                {"$set": {"discovery_partial_enroll_error": str(e)[:200]}},
            )

        # Sync used_by on prospect_state overlay (best-effort)
        try:
            _aid = str(account_oid)
            _cid = str(campaign_oid)
            _now = now
            state_ops = []
            from pymongo import UpdateOne as _SUO
            from utils.scoring import tier_from_score as _tier_from_score
            for doc in docs:
                _pid = str(doc["prospect_id"])
                _score = float(doc.get("campaign_rule_score") or 0)
                state_ops.append(_SUO(
                    {"account_id": _aid, "prospect_id": _pid},
                    {
                        "$setOnInsert": {"account_id": _aid, "prospect_id": _pid, "status": "new", "tags": [], "created_at": _now},
                        # Write ai_score + tier on every path (DB-pool and Apify-sourced)
                        # so prospect_state.ai_score is always populated consistently.
                        "$set": {
                            "ai_score": _score,
                            "priority_tier": _tier_from_score(_score),
                            "last_updated_at": _now,
                        },
                        "$addToSet": {"used_by": {"user_id": _current_user_id, "campaign_id": _cid, "status": "scoring", "enrolled_at": _now}},
                    },
                    upsert=True,
                ))
            if state_ops:
                await database.prospect_state_collection.bulk_write(state_ops, ordered=False)
        except Exception as _se:
            logger.debug(f"_pre_enroll used_by sync failed: {_se}")


async def _enroll_prospects(campaign: dict, prospects: list[dict]) -> list[str]:
    """
    Enroll top N prospects into the campaign.
    Creates enrollment docs with next_action_at=None (set at approve-and-launch).
    Returns list of enrollment ID strings.
    """
    if not prospects:
        return []

    campaign_oid = campaign["_id"]
    account_oid = campaign["account_id"]
    now = datetime.utcnow()

    # Find existing enrollments for these prospects, noting their status
    prospect_oids = [p["_id"] for p in prospects]
    existing_cursor = database.campaign_enrollments_collection.find(
        {"campaign_id": campaign_oid, "prospect_id": {"$in": prospect_oids}},
        {"prospect_id": 1, "status": 1},
    )
    existing_map = {doc["prospect_id"]: doc["status"] async for doc in existing_cursor}

    to_promote = []   # Pre-enrolled as "enriching" or "scoring" — promote to active
    to_insert = []    # Truly new — create fresh enrollment

    for prospect in prospects:
        pid = prospect["_id"]
        existing_status = existing_map.get(pid)
        if existing_status is None:
            to_insert.append(prospect)
        elif existing_status in ("enriching", "scoring"):
            to_promote.append(pid)
        # else already active/completed — skip

    # Promote provisional enrollments to active
    if to_promote:
        await database.campaign_enrollments_collection.update_many(
            {"campaign_id": campaign_oid, "prospect_id": {"$in": to_promote}, "status": {"$in": ["enriching", "scoring"]}},
            {"$set": {"status": "active"}},
        )

    # Create enrollment docs for prospects not yet enrolled at all
    docs = []
    for prospect in to_insert:
        docs.append({
            "campaign_id": campaign_oid,
            "account_id": account_oid,
            "prospect_id": prospect["_id"],
            "status": "active",
            "current_step": 0,
            "next_action_at": None,
            "step_history": [],
            "enrolled_at": now,
            "completed_at": None,
            "last_activity_at": None,
            "smart_campaign_channel": None,
            "smart_campaign_send_day": None,
            "smart_campaign_scheduled_utc": None,
            "generated_messages": None,
            "message_gen_status": "pending",
            "message_gen_error": None,
        })

    if docs:
        await database.campaign_enrollments_collection.insert_many(docs, ordered=False)

    newly_enrolled = len(to_promote) + len(docs)
    if newly_enrolled > 0:
        await database.campaigns_collection.update_one(
            {"_id": campaign_oid},
            {"$inc": {"total_enrolled": newly_enrolled, "active_count": newly_enrolled}},
        )

    # Sync used_by on prospect_state for all enrolled prospects
    try:
        _aid = str(account_oid)
        _cid = str(campaign_oid)
        from pymongo import UpdateOne as _EUO
        state_ops = []
        for p in prospects:
            _pid = str(p["_id"])
            state_ops.append(_EUO(
                {"account_id": _aid, "prospect_id": _pid},
                {
                    "$setOnInsert": {"account_id": _aid, "prospect_id": _pid, "status": "new", "tags": [], "created_at": now, "last_updated_at": now},
                    "$addToSet": {"used_by": {"user_id": _aid, "campaign_id": _cid, "status": "active", "enrolled_at": now}},
                },
                upsert=True,
            ))
        if state_ops:
            await database.prospect_state_collection.bulk_write(state_ops, ordered=False)
    except Exception as _se:
        logger.debug(f"_enroll_prospects used_by sync failed: {_se}")

    # Return IDs for all selected prospects (promoted + newly inserted)
    if prospect_oids:
        cursor = database.campaign_enrollments_collection.find(
            {"campaign_id": campaign_oid, "prospect_id": {"$in": prospect_oids}},
            {"_id": 1},
        )
        return [str(doc["_id"]) async for doc in cursor]
    return []


async def _auto_launch_campaign(campaign_id: str, account_id: str):
    """
    Auto-approve and launch campaign after message generation completes.
    Assigns channels, computes timezone-aware send times, activates campaign.
    """
    campaign_oid = ObjectId(campaign_id)
    account_oid = ObjectId(account_id)

    logger.info(f"[Campaign {campaign_id}] Auto-launch starting")
    await database.campaigns_collection.update_one(
        {"_id": campaign_oid},
        {"$set": {"auto_launch_status": "running"}},
    )

    try:
        campaign = await database.campaigns_collection.find_one({"_id": campaign_oid})
        if not campaign:
            raise ValueError(f"Campaign {campaign_id} not found")

        # Auto-select accounts if not already set
        email_account_id = campaign.get("email_account_id")
        linkedin_account_id = campaign.get("linkedin_account_id")

        if not email_account_id:
            email_acc = await database.email_accounts_collection.find_one(
                {"account_id": account_oid, "status": {"$in": ["connected", "active"]}}
            )
            if email_acc:
                email_account_id = email_acc["_id"]
                await database.campaigns_collection.update_one(
                    {"_id": campaign_oid},
                    {"$set": {"email_account_id": email_account_id}},
                )

        if not linkedin_account_id:
            linkedin_acc = await database.linkedin_accounts_collection.find_one(
                {"account_id": account_oid, "unipile_status": "OK"}
            )
            if linkedin_acc:
                linkedin_account_id = linkedin_acc["_id"]
                await database.campaigns_collection.update_one(
                    {"_id": campaign_oid},
                    {"$set": {"linkedin_account_id": linkedin_account_id}},
                )

        # Re-fetch campaign with updated account IDs
        campaign = await database.campaigns_collection.find_one({"_id": campaign_oid})

        # Run the extracted approve-and-launch logic
        from services.campaign_launch_service import run_approve_and_launch
        await run_approve_and_launch(campaign, account_oid)

        await database.campaigns_collection.update_one(
            {"_id": campaign_oid},
            {"$set": {
                "auto_launch_status": "completed",
                "auto_launch_completed_at": datetime.utcnow(),
            }},
        )
        logger.info(f"[Campaign {campaign_id}] Auto-launch completed")

    except Exception as e:
        logger.error(f"[Campaign {campaign_id}] Auto-launch failed: {e}", exc_info=True)
        await database.campaigns_collection.update_one(
            {"_id": campaign_oid},
            {"$set": {
                "auto_launch_status": "failed",
                "auto_launch_error": str(e)[:500],
            }},
        )
