"""
Fast curated discovery — single end-to-end BackgroundTask.

run_fast_discovery(campaign_id, account_id)
  ① Gemini source (up to 3 iterations, hard cost cap)
  ② Haiku batch score companies → drop < 50
  ③ ONE bulk Apify call (companyBatchMode=one_by_one, maxItemsPerCompany=5, Short mode)
  ④ Haiku batch score employees → drop < 60
  ⑤ Unconditional recovery: re-scrape 0-employee companies with broadened IDs
  ⑥ Bulk email finder for all kept prospects
  ⑦ Upsert prospects + pre-enroll
  ⑧ Day-1 message generation (parallel, fail-soft)
  → discovery_status = completed, campaign.status = awaiting_approval
"""

import asyncio
import json
import logging
from datetime import datetime
from typing import Optional

from bson import ObjectId
from pymongo import ReturnDocument, UpdateOne

import database
from config import get_settings
from services.company_sourcing_service import source_companies
from services.employee_scraper_service import (
    bulk_scrape_employees_for_companies,
    transform_employee_to_prospect,
)

logger = logging.getLogger(__name__)
settings = get_settings()

_MAX_GEMINI_ITERATIONS = 10  # loop until _QUALITY_COMPANY_TARGET or budget exhausted
_QUALITY_COMPANY_TARGET = 120   # minimum kept (score ≥50) companies before stopping
_COMPANY_SCORE_THRESHOLD = 50
_EMPLOYEE_SCORE_THRESHOLD = 48
_SCORING_DROPOUT_BUFFER = 2.5   # scrape ~2.5x target raw employees to survive score gate + contactability + dedup
_COMPANY_BUFFER = 1.3           # source ~1.3x the companies strictly needed (company-score dropout)
_MIN_PER_COMPANY = 2            # floor so a company is worth a scrape
_MAX_PER_COMPANY_CAP = 10       # ceiling regardless of campaign setting
_SCRAPE_DEPTH = 8               # employees per company to scrape
_PER_COMPANY_ENROLLMENT_CAP = 3 # max enrolled per company after scoring
# Full mode returns proper vanity profile URLs (linkedinUrl = /in/slug) required by
# the email finder.  Short mode returns only internal ACw... IDs which the actor
# cannot resolve.  Full costs $8/1k vs Short $4/1k.
_PROFILE_SCRAPER_MODE = "Full ($8 per 1k)"


# ──────────────────────────────────────────────────────────────────────────────
# Channel planning helpers (public — called by onboarding wizard)
# ──────────────────────────────────────────────────────────────────────────────

async def finalize_channel_plan(campaign_id: str, account_id: str) -> dict:
    """
    Auto-pick sender accounts, run plan_channel_assignments, persist channel/day
    to enrollments, and write day_totals to the campaign doc.
    Called both at the end of run_fast_discovery and by replan_and_launch.
    """
    campaign_oid = ObjectId(campaign_id)
    account_oid = ObjectId(account_id) if (isinstance(account_id, str) and len(account_id) == 24) else ObjectId(account_id)
    account_id_filter = {"$in": [account_oid, str(account_oid)]}

    campaign = await database.campaigns_collection.find_one({"_id": campaign_oid})
    if not campaign:
        return {"assigned": 0, "skip_reasons": {}, "day_totals": {}}

    # Auto-pick sender accounts if not already set
    auto_set: dict = {}
    if not campaign.get("email_account_id"):
        email_acc = await database.email_accounts_collection.find_one(
            {"account_id": account_id_filter, "status": {"$in": ["connected", "active"]}}
        )
        if email_acc:
            auto_set["email_account_id"] = email_acc["_id"]
            campaign["email_account_id"] = email_acc["_id"]
    if not campaign.get("linkedin_account_id"):
        li_acc = await database.linkedin_accounts_collection.find_one(
            {"account_id": account_id_filter, "unipile_status": {"$in": ["OK", "CONNECTING"]}}
        )
        if li_acc:
            auto_set["linkedin_account_id"] = li_acc["_id"]
            campaign["linkedin_account_id"] = li_acc["_id"]
    if auto_set:
        await database.campaigns_collection.update_one({"_id": campaign_oid}, {"$set": auto_set})
        logger.info(f"[finalize_plan:{campaign_id}] auto-picked senders: { {k: str(v) for k, v in auto_set.items()} }")

    # Load enrollments for planning
    enrollments_for_plan = await database.campaign_enrollments_collection.find(
        {"campaign_id": campaign_oid, "status": "scoring"}
    ).to_list(length=5000)

    if not enrollments_for_plan:
        return {"assigned": 0, "skip_reasons": {}, "day_totals": {}}

    pids = list({e["prospect_id"] for e in enrollments_for_plan})
    prospects_by_id = {}
    if pids:
        for p in await database.prospects_collection.find({"_id": {"$in": pids}}).to_list(length=len(pids)):
            prospects_by_id[p["_id"]] = p

    from services.campaign_launch_service import plan_channel_assignments
    assignments, skip_reasons = plan_channel_assignments(campaign, enrollments_for_plan, prospects_by_id, min_score=0)
    logger.info(f"[finalize_plan:{campaign_id}] assigned={len(assignments)}, skip={skip_reasons}")

    from pymongo import UpdateOne as _PlanUpdateOne
    plan_ops = []
    assigned_ids = set()
    for enr, channel, send_day in assignments:
        assigned_ids.add(enr["_id"])
        plan_ops.append(_PlanUpdateOne(
            {"_id": enr["_id"]},
            {"$set": {
                "smart_campaign_channel": channel,
                "smart_campaign_send_day": send_day,
                "status": "active",
                "next_action_at": None,
                "smart_campaign_scheduled_utc": None,
                "message_gen_status": "pending" if send_day == 1 else "scheduled_later",
                "generated_messages": None,
                "message_gen_error": None,
            }},
        ))
    for enr in enrollments_for_plan:
        if enr["_id"] not in assigned_ids:
            plan_ops.append(_PlanUpdateOne(
                {"_id": enr["_id"]},
                {"$set": {
                    "status": "skipped_no_channel",
                    "smart_campaign_channel": None,
                    "smart_campaign_send_day": None,
                    "next_action_at": None,
                    "smart_campaign_scheduled_utc": None,
                    "message_gen_status": "skipped",
                }},
            ))
    if plan_ops:
        await database.campaign_enrollments_collection.bulk_write(plan_ops, ordered=False)

    # Sync used_by → "active" for assigned prospects on prospect_state overlay
    if assignments:
        try:
            from services.prospect_search_service import update_used_by_status as _upd_used_by
            _sync_ops = []
            for enr, channel, send_day in assignments:
                _sync_ops.append(
                    _upd_used_by(
                        database.db,
                        account_id=str(enr.get("account_id", "")),
                        prospect_id=str(enr.get("prospect_id", "")),
                        campaign_id=str(enr.get("campaign_id", "")),
                        new_status="active",
                    )
                )
            import asyncio as _asyncio
            await _asyncio.gather(*_sync_ops, return_exceptions=True)
        except Exception as _usync_e:
            logger.warning(f"[finalize_plan:{campaign_id}] used_by sync failed: {_usync_e}")

    # Compute day_totals
    day_totals: dict = {}
    for enr, ch, d in assignments:
        day_totals.setdefault(str(d), {}).setdefault(ch, 0)
        day_totals[str(d)][ch] += 1

    total_assigned = len(assignments)
    # NOTE: discovery_status is intentionally NOT set here.
    # The pipeline already wrote "completed" before calling finalize_channel_plan.
    # Overwriting it with "awaiting_approval" would create a terminal-signal race
    # where the frontend sees "awaiting_approval" instead of "completed".
    # campaign.status="awaiting_approval" (set at the same time as "completed") is
    # the authoritative signal that discovery is done and the review gate is open.
    await database.campaigns_collection.update_one(
        {"_id": campaign_oid},
        {"$set": {
            "discovery_prospects_eligible": total_assigned,
            "discovery_day_totals": day_totals,
        }},
    )

    return {"assigned": total_assigned, "skip_reasons": skip_reasons, "day_totals": day_totals}


async def replan_and_launch(campaign_id: str, account_id: str) -> dict:
    """
    Re-plan channel assignments now that sender accounts have been connected.
    Called at onboarding launch time (discovery fired at stage 3 before accounts existed).
    Re-queries enrollments in {scoring, skipped_no_channel} statuses, resets them
    to 'scoring', then calls finalize_channel_plan with now-connected senders.
    Also triggers Day-1 deep enrichment + message gen.
    """
    campaign_oid = ObjectId(campaign_id)

    # Re-query all enrollments that weren't successfully planned
    enrollments_to_replan = await database.campaign_enrollments_collection.find(
        {
            "campaign_id": campaign_oid,
            "status": {"$in": ["scoring", "skipped_no_channel"]},
        }
    ).to_list(length=5000)

    if not enrollments_to_replan:
        # Try active enrollments with no next_action_at (planned but not yet sent)
        enrollments_to_replan = await database.campaign_enrollments_collection.find(
            {
                "campaign_id": campaign_oid,
                "status": "active",
                "next_action_at": None,
            }
        ).to_list(length=5000)

    if enrollments_to_replan:
        reset_ids = [e["_id"] for e in enrollments_to_replan]
        await database.campaign_enrollments_collection.update_many(
            {"_id": {"$in": reset_ids}},
            {"$set": {
                "status": "scoring",
                "smart_campaign_channel": None,
                "smart_campaign_send_day": None,
                "message_gen_status": "pending",
                "generated_messages": None,
                "next_action_at": None,
            }},
        )
        logger.info(f"[replan:{campaign_id}] reset {len(reset_ids)} enrollments for re-planning")

    # Run finalize_channel_plan with now-connected sender accounts
    result = await finalize_channel_plan(campaign_id, account_id)

    # Trigger Day-1 deep enrichment + message gen if prospects were assigned
    if result.get("assigned", 0) > 0:
        campaign = await database.campaigns_collection.find_one({"_id": campaign_oid})
        if campaign:
            # Get day1 and all prospect oids
            day1_enrs = await database.campaign_enrollments_collection.find(
                {"campaign_id": campaign_oid, "smart_campaign_send_day": 1, "status": "active"}
            ).to_list(length=5000)
            day1_oids = [e["prospect_id"] for e in day1_enrs]

            all_enrs = await database.campaign_enrollments_collection.find(
                {"campaign_id": campaign_oid, "status": "active"}
            ).to_list(length=5000)
            all_oids = [e["prospect_id"] for e in all_enrs]

            asyncio.create_task(_run_deep_enrichment_then_messages(
                campaign_id=campaign_id,
                account_id=account_id,
                day1_prospect_oids=day1_oids,
                all_prospect_oids=all_oids,
            ))

    logger.info(f"[replan:{campaign_id}] done: assigned={result.get('assigned')}, days={list(result.get('day_totals', {}).keys())}")
    return result


# ──────────────────────────────────────────────────────────────────────────────
# Public entry point
# ──────────────────────────────────────────────────────────────────────────────

async def run_fast_discovery(campaign_id: str, account_id: str) -> dict:
    """End-to-end curated discovery. Single BackgroundTask, ~60–120s."""
    campaign_oid = ObjectId(campaign_id)
    now = datetime.utcnow()

    await database.sourced_companies_collection.delete_many({"campaign_id": campaign_id})
    await database.campaigns_collection.update_one(
        {"_id": campaign_oid},
        {"$set": {
            "discovery_status": "sourcing_companies",
            "discovery_started_at": now,
            "discovery_error": None,
            "curated_companies_sourced": 0,
            "curated_companies_approved": 0,
            "curated_companies_scraped": 0,
        }},
    )

    try:
        campaign = await database.campaigns_collection.find_one({"_id": campaign_oid})
        if not campaign:
            raise ValueError(f"Campaign {campaign_id} not found")

        icp_prompt = _build_icp_prompt_from_campaign(campaign)
        import math
        prospect_target = int(campaign.get("prospect_count_target") or settings.enrolled_target_first_campaign)
        per_company = int(campaign.get("max_prospects_per_company") or settings.max_prospects_per_company)
        per_company = max(_MIN_PER_COMPANY, min(per_company, _MAX_PER_COMPANY_CAP))

        # Derive company target from prospect target, buffered for company-score dropout.
        companies_needed = math.ceil(prospect_target / per_company)
        target = max(10, math.ceil(companies_needed * _COMPANY_BUFFER))   # companies to source
        max_companies = max(target, math.ceil(max(target, _QUALITY_COMPANY_TARGET) * 1.5))  # hard sourcing ceiling

        # Fetch sender context for employee scoring
        company_profile = await database.company_profiles_collection.find_one(
            {"account_id": ObjectId(account_id) if len(account_id) == 24 else account_id}
        )
        # sender_context no longer needed — LLM employee scoring replaced by deterministic scoring
        # sender_context = _build_sender_context(company_profile)

        # ── MOCK MODE: skip all paid calls, inject synthetic prospects ────────────
        # Enabled when campaign.discovery_mock_mode=True AND app_env != production.
        # This lets E2E harnesses exercise the real enroll/plan/message path for free.
        _is_mock = bool(campaign.get("discovery_mock_mode")) and settings.app_env != "production"
        if not _is_mock and settings.discovery_mock_mode and settings.app_env != "production":
            _is_mock = True

        if _is_mock:
            import uuid as _uuid_mod
            logger.info(f"[fast:{campaign_id}] *** MOCK MODE *** — generating synthetic prospects (no Apify/Gemini)")
            _mock_enroll_cap = int(campaign.get("discovery_enrollment_cap") or _PER_COMPANY_ENROLLMENT_CAP)
            _mock_scrape_depth = int(campaign.get("discovery_scrape_depth") or _SCRAPE_DEPTH)
            _mock_per_co = min(_mock_enroll_cap, per_company, max(1, _mock_scrape_depth))
            _mock_n_co = max(3, min(20, math.ceil(prospect_target / per_company)))
            _mock_industries = campaign.get("icp_industries") or ["Retail"]
            _mock_titles = campaign.get("icp_job_titles") or ["Owner"]
            _mock_seniorities = campaign.get("icp_seniority_levels") or ["owner"]
            _mock_countries = campaign.get("icp_countries") or ["United States"]

            new_prospect_oids: list[ObjectId] = []
            for _ci in range(_mock_n_co):
                _co_slug = f"mockco-{_ci}-{_uuid_mod.uuid4().hex[:6]}"
                for _pi in range(_mock_per_co):
                    _uid = _uuid_mod.uuid4().hex[:8]
                    _pt: dict = {
                        "full_name": f"Mock Prospect {_ci}-{_pi}",
                        "first_name": "Mock",
                        "last_name": f"Prospect{_ci}{_pi}",
                        "linkedin": f"https://www.linkedin.com/in/mock-{_uid}",
                        "email": f"mock-{_uid}@{_co_slug}.example.com",
                        "job_title": _mock_titles[_ci % len(_mock_titles)],
                        "headline": f"{_mock_titles[_ci % len(_mock_titles)]} at MockCo-{_ci}",
                        "company_name": f"MockCo-{_ci}",
                        "company_linkedin": f"https://www.linkedin.com/company/{_co_slug}",
                        "company_linkedin_url": f"https://www.linkedin.com/company/{_co_slug}",
                        "company_domain": f"{_co_slug}.example.com",
                        "industry": _mock_industries[_ci % len(_mock_industries)],
                        "company_employee_band": "51-200",
                        "company_size": 100 + _ci * 10,
                        "seniority": _mock_seniorities[_ci % len(_mock_seniorities)],
                        "function": "Operations",
                        "fit_score": 75.0,
                        "location": {
                            "city": "Atlanta", "region": "Georgia",
                            "country": _mock_countries[0], "country_code": "US",
                        },
                        "linkedin_profile_data": {
                            "posts": [{
                                "text": (
                                    f"Growing our {_mock_industries[_ci % len(_mock_industries)]} "
                                    "footprint — local discovery on Google Maps is everything."
                                ),
                                "likes": 12,
                            }]
                        },
                    }
                    _oid = await _upsert_curated_prospect(_pt, campaign_oid, account_id)
                    if _oid:
                        new_prospect_oids.append(_oid)

            await database.campaigns_collection.update_one(
                {"_id": campaign_oid},
                {"$set": {
                    "discovery_prospects_found": len(new_prospect_oids),
                    "discovery_prospects_from_apify": len(new_prospect_oids),
                    "discovery_prospects_from_db": 0,
                    "discovery_status": "enriching",
                }},
            )

            from services.campaign_prospect_finder_service import _pre_enroll_prospects
            _mock_prospects_full = await database.prospects_collection.find(
                {"_id": {"$in": new_prospect_oids}}
            ).to_list(length=None)
            if _mock_prospects_full:
                await _pre_enroll_prospects(campaign, _mock_prospects_full)

            await database.campaigns_collection.update_one(
                {"_id": campaign_oid},
                {"$set": {
                    "discovery_status": "completed",
                    "discovery_completed_at": datetime.utcnow(),
                    "status": "awaiting_approval",
                    "approved_send_days": [],
                }},
            )
            _mock_plan = await finalize_channel_plan(campaign_id, account_id)
            _mock_assigned = _mock_plan.get("assigned", 0)
            await database.campaigns_collection.update_one(
                {"_id": campaign_oid},
                {"$set": {
                    "discovery_prospects_planned": _mock_assigned,
                    "discovery_prospects_enrolled": _mock_assigned,
                }},
            )
            if _mock_assigned > 0:
                _mock_day1 = await database.campaign_enrollments_collection.find(
                    {"campaign_id": campaign_oid, "smart_campaign_send_day": 1, "status": "active"}
                ).to_list(length=5000)
                _mock_day1_oids = [e["prospect_id"] for e in _mock_day1]
                asyncio.create_task(_run_deep_enrichment_then_messages(
                    campaign_id=campaign_id,
                    account_id=str(account_id),
                    day1_prospect_oids=_mock_day1_oids,
                    all_prospect_oids=new_prospect_oids,
                ))
            logger.info(
                f"[fast:{campaign_id}] MOCK done — {len(new_prospect_oids)} synthetic prospects, "
                f"{_mock_assigned} assigned"
            )
            return {"campaign_id": campaign_id, "prospects_created": len(new_prospect_oids), "mock": True}
        # ── END MOCK MODE ─────────────────────────────────────────────────────────

        # ── Lazy ICP canonicalization ─────────────────────────────────────────────
        from services.prospect_search_service import (
            search_companies_structured as _search_cos,
            search_prospects_structured as _search_pool_structured,
            build_exclusion_set as _build_exclusion_set,
        )
        from services.icp_canonicalizer import canonicalize_icp as _canonicalize_icp

        _canon_fields = ("industry_ids", "country_codes", "seniorities", "employee_bands")
        if not any(campaign.get(f) for f in _canon_fields):
            try:
                _canonical = await _canonicalize_icp(campaign)
                if any(_canonical.values()):
                    await database.campaigns_collection.update_one(
                        {"_id": campaign_oid}, {"$set": _canonical}
                    )
                    campaign.update(_canonical)
                    logger.info(f"[fast:{campaign_id}] lazy-canonicalized ICP: {_canonical.get('industry_ids')}")
            except Exception as _ce:
                logger.warning(f"[fast:{campaign_id}] ICP canonicalization failed: {_ce}")

        # Company + prospect targets (company-first)
        _company_target = int(campaign.get("curated_company_count_target") or 100)

        # Build exclusion set (90-day cooldown from completion, per-user)
        _user_id = str(campaign.get("created_by") or account_id)
        _excluded = await _build_exclusion_set(
            database.db,
            account_id=str(account_id),
            user_id=_user_id,
            cooldown_days=90,
            campaign_id=campaign_id,
        )

        _icp_industry_ids = campaign.get("industry_ids") or []
        _icp_country_codes = campaign.get("country_codes") or []
        _icp_employee_bands = campaign.get("employee_bands") or []

        # ── STAGE A: DB company match ──────────────────────────────────────────────
        # Query the shared companies_collection by canonical ICP fields.
        # Returns up to _company_target companies sorted by prospect_count desc.
        _matched_cos: list[dict] = []
        if _icp_industry_ids or _icp_country_codes:
            try:
                _matched_cos = await _search_cos(
                    database.db,
                    industry_ids=_icp_industry_ids or None,
                    country_codes=_icp_country_codes or None,
                    employee_bands=_icp_employee_bands or None,
                    limit=_company_target,
                )
                logger.info(
                    f"[fast:{campaign_id}] Stage A: {len(_matched_cos)} companies matched in DB "
                    f"(target={_company_target})"
                )
                await database.campaigns_collection.update_one(
                    {"_id": campaign_oid},
                    {"$set": {"discovery_companies_matched": len(_matched_cos)}},
                )
            except Exception as _ca_e:
                logger.warning(f"[fast:{campaign_id}] Stage A company search failed: {_ca_e}")
                _matched_cos = []
        else:
            logger.info(f"[fast:{campaign_id}] Stage A: skipped (no canonical ICP industry/country)")

        def _db_company_to_sc(co_doc: dict) -> dict:
            """Normalize a DB company doc to the sourced-company dict format used downstream."""
            _loc = co_doc.get("location") or {}
            return {
                "company_linkedin_url": (co_doc.get("linkedin_url") or "").rstrip("/"),
                "company_name": co_doc.get("name"),
                "company_domain": co_doc.get("domain") or co_doc.get("website"),
                "company_website": co_doc.get("website"),
                # Keep as dict so transform_employee_to_prospect extracts canonical fields
                "industry": co_doc.get("industry"),
                "location": co_doc.get("location"),
                "employee_band": co_doc.get("employee_band"),
                "employee_size_estimate": (
                    str(co_doc["employee_count"]) if co_doc.get("employee_count") else None
                ),
                "description": co_doc.get("description"),
                "country": (_loc.get("country") if isinstance(_loc, dict) else None) or "",
                "_icp_score": 80.0,  # DB companies are pre-qualified; score higher than threshold
                "_db_company_id": str(co_doc["_id"]),
                "_source": "db",
            }

        _matched_sc_list: list[dict] = [_db_company_to_sc(co) for co in _matched_cos]
        _matched_urls: set[str] = {
            (sc.get("company_linkedin_url") or "").rstrip("/").lower()
            for sc in _matched_sc_list
            if sc.get("company_linkedin_url")
        }

        # ── STAGE B: Gap-fill sourcing (only if matched < target) ─────────────────
        # gap == 0 → skip Gemini entirely (the "≥100 companies → don't source" rule).
        _gap = max(0, _company_target - len(_matched_sc_list))
        logger.info(
            f"[fast:{campaign_id}] Stage B: gap={_gap} "
            f"(target={_company_target}, matched={len(_matched_sc_list)})"
        )

        _sourced_sc_list: list[dict] = []
        if _gap > 0:
            _sourcing_concurrency = int(campaign.get("discovery_sourcing_concurrency") or 1)
            _want = math.ceil(_gap * _COMPANY_BUFFER)
            logger.info(f"[fast:{campaign_id}] Stage B: sourcing ~{_want} companies (gap={_gap})")

            try:
                _gemini_raw, _gemini_meta = await _with_retries(
                    lambda: source_companies(
                        icp_prompt=icp_prompt,
                        target_count=_want,
                        exclude_names=[sc.get("company_name") or "" for sc in _matched_sc_list],
                        account_id=account_id,
                        campaign_id=campaign_id,
                        validate_urls=False,
                        max_concurrency=_sourcing_concurrency,
                    )
                )
            except Exception as _sb_e:
                logger.warning(f"[fast:{campaign_id}] Stage B Gemini sourcing failed: {_sb_e}")
                _gemini_raw, _gemini_meta = [], {}

            # Dedupe sourced companies against already-matched DB companies and each other
            _raw_co_urls = [
                (c.get("company_linkedin_url") or "").rstrip("/").lower()
                for c in _gemini_raw
                if c.get("company_linkedin_url")
            ]
            _db_url_existing: set[str] = set()
            if _raw_co_urls:
                async for _ex in database.companies_collection.find(
                    {"linkedin_url": {"$in": _raw_co_urls}},
                    {"linkedin_url": 1},
                ):
                    _db_url_existing.add((_ex.get("linkedin_url") or "").rstrip("/").lower())

            _seen_in_gap: set[str] = set()
            _deduped: list[dict] = []
            for _co in _gemini_raw:
                _co_url = (_co.get("company_linkedin_url") or "").rstrip("/").lower()
                if not _co_url:
                    continue
                if _co_url in _matched_urls or _co_url in _db_url_existing or _co_url in _seen_in_gap:
                    continue
                _seen_in_gap.add(_co_url)
                _co["_source"] = "gemini"
                _deduped.append(_co)

            # Score and keep above threshold, limited to the gap
            for _co in _deduped:
                _co["_icp_score"] = _score_company_deterministic(_co, icp_prompt)
            _sourced_sc_list = [
                _co for _co in _deduped
                if _co.get("_icp_score", 0) >= _COMPANY_SCORE_THRESHOLD
                and _co.get("company_linkedin_url")
            ][:_gap]

            await database.campaigns_collection.update_one(
                {"_id": campaign_oid},
                {"$set": {"curated_companies_sourced": len(_gemini_raw)}},
            )
            logger.info(
                f"[fast:{campaign_id}] Stage B: raw={len(_gemini_raw)}, "
                f"deduped={len(_deduped)}, kept={len(_sourced_sc_list)}"
            )
        else:
            logger.info(f"[fast:{campaign_id}] Stage B: skipped (DB already has enough companies)")

        # Merge DB-matched + Gemini-sourced into the working company set
        kept_companies: list[dict] = _matched_sc_list + _sourced_sc_list

        # Persist Gemini-sourced companies to sourced_companies collection for UI display
        if _sourced_sc_list:
            _sc_ui_docs = [{
                **c,
                "campaign_id": campaign_id,
                "account_id": account_id,
                "source": "gemini_grounded",
                "user_excluded": False,
                "employee_scrape_status": "pending",
                "employees_scraped_count": 0,
                "prospects_created_count": 0,
                "created_at": now,
                "updated_at": now,
            } for c in _sourced_sc_list]
            try:
                await database.sourced_companies_collection.insert_many(_sc_ui_docs, ordered=False)
            except Exception as _ui_e:
                logger.warning(f"[fast:{campaign_id}] sourced_companies UI persist failed: {_ui_e}")

        if not kept_companies:
            await database.campaigns_collection.update_one(
                {"_id": campaign_oid},
                {"$set": {
                    "discovery_status": "failed",
                    "discovery_error": (
                        "No companies found matching your criteria. "
                        "Try broadening your ICP description or removing location/industry filters."
                    ),
                }},
            )
            return {"campaign_id": campaign_id, "sourced": 0}

        await database.campaigns_collection.update_one(
            {"_id": campaign_oid},
            {"$set": {
                "curated_companies_approved": len(kept_companies),
                "discovery_companies_found": len(kept_companies),
            }},
        )
        logger.info(
            f"[fast:{campaign_id}] company set: {len(kept_companies)} "
            f"(db={len(_matched_sc_list)}, gemini={len(_sourced_sc_list)}), "
            f"icp={icp_prompt[:80]!r}"
        )

        # ── STAGE C: Per-company prospect reuse sub-step ───────────────────────────
        # For each company in the set, check if the shared prospect pool already has
        # contactable prospects there that this account hasn't used. Reuse them, skip
        # scraping that company. Companies without enough reusable prospects go to Apify.
        _enroll_cap = int(campaign.get("discovery_enrollment_cap") or _PER_COMPANY_ENROLLMENT_CAP)
        _co_urls_list = [
            (c.get("company_linkedin_url") or "").rstrip("/").lower()
            for c in kept_companies
            if c.get("company_linkedin_url")
        ]

        _reuse_by_co: dict[str, list[dict]] = {}
        if _co_urls_list:
            try:
                _reuse_candidates = await _search_pool_structured(
                    database.db,
                    account_id=str(account_id),
                    industry_ids=_icp_industry_ids or None,
                    country_codes=_icp_country_codes or None,
                    employee_bands=_icp_employee_bands or None,
                    company_linkedin_urls=_co_urls_list,
                    exclude_ids=_excluded,
                    limit=len(_co_urls_list) * _enroll_cap * 3,
                )
                for _rp in _reuse_candidates:
                    _rp_co_url = (_rp.get("company_linkedin") or "").rstrip("/").lower()
                    if _rp_co_url:
                        _reuse_by_co.setdefault(_rp_co_url, []).append(_rp)
                logger.info(
                    f"[fast:{campaign_id}] Stage C reuse: "
                    f"{sum(len(v) for v in _reuse_by_co.values())} prospects "
                    f"at {len(_reuse_by_co)}/{len(_co_urls_list)} companies"
                )
            except Exception as _rc_e:
                logger.warning(f"[fast:{campaign_id}] Stage C reuse query failed: {_rc_e}")

        # Partition companies into: fully/partially covered by pool vs. must scrape
        companies_to_scrape: list[dict] = []
        _reused_pairs: list[tuple[dict, dict]] = []  # (prospect_doc, sourced_company)

        for _co in kept_companies:
            _co_url = (_co.get("company_linkedin_url") or "").rstrip("/").lower()
            if not _co_url:
                companies_to_scrape.append(_co)
                continue
            _existing = _reuse_by_co.get(_co_url, [])[:_enroll_cap]
            if len(_existing) >= _enroll_cap:
                # Fully covered — skip scraping this company
                _reused_pairs.extend((_rp, _co) for _rp in _existing)
            elif _existing:
                # Partial — reuse what exists, still scrape for more
                _reused_pairs.extend((_rp, _co) for _rp in _existing)
                companies_to_scrape.append(_co)
            else:
                companies_to_scrape.append(_co)

        _db_enrolled_count = len(_reused_pairs)
        logger.info(
            f"[fast:{campaign_id}] Stage C: "
            f"reused={_db_enrolled_count}, scraping={len(companies_to_scrape)} companies"
        )

        # Reduce prospect_target by how many we already have from the reuse pool
        prospect_target = max(0, prospect_target - _db_enrolled_count)

        # ── ③ Bulk Apify employee scrape (companies_to_scrape only) ────────────
        await database.campaigns_collection.update_one(
            {"_id": campaign_oid},
            {"$set": {"discovery_status": "scraping_employees"}},
        )

        # ── Per-campaign tuning overrides (campaign doc keys; fall back to module constants) ──
        _scrape_depth = int(campaign.get("discovery_scrape_depth") or _SCRAPE_DEPTH)
        _dropout_buffer = float(campaign.get("discovery_dropout_buffer") or _SCORING_DROPOUT_BUFFER)
        # _enroll_cap already set in Stage C above
        _sourcing_concurrency = int(campaign.get("discovery_sourcing_concurrency") or 1)

        seniority_ids = _icp_seniority_to_actor_ids(campaign.get("icp_seniority_levels") or [])
        functional_ids = _icp_function_to_actor_ids(campaign.get("icp_functional_departments") or [])
        # Only scrape companies not covered by Stage C reuse pool
        company_urls = [c["company_linkedin_url"] for c in companies_to_scrape]

        logger.info(
            f"[fast:{campaign_id}] bulk Apify — {len(company_urls)} cos, "
            f"scrape_depth={_scrape_depth}, dropout_buffer={_dropout_buffer}, enroll_cap={_enroll_cap}, "
            f"seniority={seniority_ids}, function={functional_ids}"
        )

        raw_employees = await _with_retries(
            lambda: bulk_scrape_employees_for_companies(
                company_urls,
                max_items_per_company=_scrape_depth,
                max_total_items=math.ceil(prospect_target * _dropout_buffer),
                seniority_level_ids=seniority_ids or None,
                functional_level_ids=functional_ids or None,
                profile_scraper_mode=_PROFILE_SCRAPER_MODE,
                account_id=account_id,
                campaign_id=campaign_id,
            )
        )
        logger.info(f"[fast:{campaign_id}] bulk Apify returned {len(raw_employees)} employees")

        # Map each employee to its sourced company (by companyUrl from response).
        # FIXED: employees whose company URL is present but doesn't match any ICP company
        # are off-ICP contamination — drop them rather than mis-attributing to kept_companies[0].
        # Employees with NO company URL (scraper omission, not off-ICP) get the first company
        # as a fallback, since we can't determine their attribution.
        url_to_sc = {
            (_normalize_li_url(c.get("company_linkedin_url")) or ""): c
            for c in kept_companies
            if c.get("company_linkedin_url")
        }
        returned_company_urls: set[str] = set()
        employee_pairs: list[tuple[dict, dict]] = []  # (raw_employee, sourced_company)
        # Fallback attribution for employees missing a company URL — use first scraped company
        _fallback_sc = (
            companies_to_scrape[0] if companies_to_scrape
            else kept_companies[0] if kept_companies
            else None
        )
        _dropped_off_icp = 0
        _no_url_fallback = 0
        for emp in raw_employees:
            co_url = _extract_company_url_from_employee(emp)
            if co_url:
                returned_company_urls.add(co_url)
                sc = url_to_sc.get(co_url) or _find_closest_sc(co_url, url_to_sc)
                if sc is None:
                    # co_url present but off-ICP — drop to avoid polluting attribution
                    _dropped_off_icp += 1
                    continue
            else:
                # No company URL in scraper response — keep with fallback
                sc = _fallback_sc
                if sc is None:
                    continue
                _no_url_fallback += 1
            employee_pairs.append((emp, sc))

        if _dropped_off_icp or _no_url_fallback:
            logger.info(
                f"[fast:{campaign_id}] employee attribution: "
                f"dropped_off_icp={_dropped_off_icp}, no_url_fallback={_no_url_fallback}"
            )

        # ── ④ Score employees, then find emails for keepers only ────────────────────────
        logger.info(
            f"[fast:{campaign_id}] gates: companies_kept={len(kept_companies)}, "
            f"employees_scraped={len(raw_employees)}"
        )

        from services.email_finder_service import find_emails_bulk

        transformed = [transform_employee_to_prospect(emp, sc) for emp, sc in employee_pairs]

        # Re-resolve SC using each prospect's own company_linkedin field, which is populated
        # from the employee's currentPositions data and is reliable even in batch mode.
        # The actor's batch response often omits currentPositions[0].companyLinkedinUrl, causing
        # the attribution loop to fall back to kept_companies[0] for most employees.
        _url_to_sc_norm = {
            (c.get("company_linkedin_url") or "").rstrip("/").lower(): c
            for c in kept_companies
            if c.get("company_linkedin_url")
        }
        _improved_sc = 0
        for i in range(len(employee_pairs)):
            emp_i, sc_i = employee_pairs[i]
            t_co_url = (transformed[i].get("company_linkedin") or "").rstrip("/").lower()
            if not t_co_url:
                continue
            if (sc_i.get("company_linkedin_url") or "").rstrip("/").lower() == t_co_url:
                continue
            resolved = _url_to_sc_norm.get(t_co_url)
            if resolved:
                employee_pairs[i] = (emp_i, resolved)
                transformed[i] = transform_employee_to_prospect(emp_i, resolved)
                _improved_sc += 1
        if _improved_sc:
            logger.info(f"[fast:{campaign_id}] SC re-attribution: fixed {_improved_sc}/{len(employee_pairs)} employees")

        from utils.scoring import score_prospect_for_campaign as _score_for_campaign

        email_by_url: dict[str, str | None] = {}

        kept_employees: list[tuple[dict, dict]] = []  # (prospect_dict, sourced_company)
        for i, (t, (_, sc)) in enumerate(zip(transformed, employee_pairs)):
            score = _score_for_campaign(t, campaign)
            t["fit_score"] = score
            t["ai_prospect_score"] = float(score)
            if score >= _EMPLOYEE_SCORE_THRESHOLD and (t.get("linkedin") or t.get("email")):
                kept_employees.append((t, sc))

        logger.info(
            f"[fast:{campaign_id}] after employee score: "
            f"{len(kept_employees)}/{len(transformed)} pass threshold"
        )

        # Per-company enrollment cap: keep only top-N by score per company
        _company_buckets: dict[str, list] = {}
        for pair in kept_employees:
            t_pr, sc_pr = pair
            co_url = (t_pr.get("company_linkedin") or t_pr.get("company_linkedin_url") or sc_pr.get("company_linkedin_url") or "").rstrip("/").lower()
            _company_buckets.setdefault(co_url, []).append(pair)

        kept_employees = []
        for co_url, _pairs in _company_buckets.items():
            _pairs.sort(key=lambda p: p[0].get("fit_score", 0), reverse=True)
            kept_employees.extend(_pairs[:_enroll_cap])

        logger.info(
            f"[fast:{campaign_id}] after per-company cap ({_enroll_cap}/co): "
            f"{len(kept_employees)} prospects across {len(_company_buckets)} companies"
        )

        # Soft cap after first-pass scoring: keep only a buffered overshoot of the target
        # to bound the recovery pass and email-finder cost.
        _keep_cap = math.ceil(prospect_target * 1.3)
        if len(kept_employees) > _keep_cap:
            kept_employees.sort(key=lambda pair: pair[0].get("fit_score", 0), reverse=True)
            kept_employees = kept_employees[:_keep_cap]

        # ── ⑤ Recovery for low-yield companies ─────────────────────────────────
        # Short mode doesn't return companyLinkedinUrl, so per-company URL tracking
        # is unreliable. Trigger recovery on ALL companies if first-pass yield is
        # < 1 employee per company on average (indicates most got 0 results).
        zero_emp_urls = company_urls  # fall back to full list if URL matching fails
        if returned_company_urls:
            # URL matching worked (Full mode or actor returned companyLinkedinUrl)
            zero_emp_urls = [
                url for url in company_urls
                if not any(url == ret for ret in returned_company_urls)
            ]

        low_yield = len(raw_employees) < len(company_urls)  # < 1 emp per company
        remaining_needed = prospect_target - len(kept_employees)
        if zero_emp_urls and low_yield and remaining_needed > 0:
            logger.info(
                f"[fast:{campaign_id}] recovery: {len(zero_emp_urls)} companies, "
                f"first-pass yield={len(raw_employees)}/{len(company_urls)}, broadening seniority+function"
            )
            broad_seniority = _broaden_seniority_ids(seniority_ids)
            broad_function = _broaden_function_ids(functional_ids)

            try:
                recovery_employees = await _with_retries(
                    lambda: bulk_scrape_employees_for_companies(
                        zero_emp_urls,
                        max_items_per_company=_scrape_depth,
                        max_total_items=math.ceil(remaining_needed * _dropout_buffer),
                        seniority_level_ids=broad_seniority or None,
                        functional_level_ids=broad_function or None,
                        profile_scraper_mode=_PROFILE_SCRAPER_MODE,
                        account_id=account_id,
                        campaign_id=campaign_id,
                    )
                )
                if recovery_employees:
                    r_pairs = [
                        (emp, _find_closest_sc(_extract_company_url_from_employee(emp) or "", url_to_sc) or _fallback_sc)
                        for emp in recovery_employees
                    ]
                    r_transformed = [transform_employee_to_prospect(emp, sc) for emp, sc in r_pairs]
                    r_kept = 0
                    for i, (t, (_, sc)) in enumerate(zip(r_transformed, r_pairs)):
                        score = _score_for_campaign(t, campaign)
                        t["fit_score"] = score
                        t["ai_prospect_score"] = float(score)
                        if score >= _EMPLOYEE_SCORE_THRESHOLD and (t.get("linkedin") or t.get("email")):
                            kept_employees.append((t, sc))
                            r_kept += 1
                    logger.info(
                        f"[fast:{campaign_id}] recovery added {r_kept} prospects"
                    )
                    # Re-cap so recovery cannot blow past target.
                    if len(kept_employees) > prospect_target:
                        kept_employees.sort(key=lambda pair: pair[0].get("fit_score", 0), reverse=True)
                        kept_employees = kept_employees[:prospect_target]
            except Exception as e:
                logger.warning(f"[fast:{campaign_id}] recovery scrape failed (skipping): {e}")

        # Final hard cap: guarantees email-finder, upsert, and enrollment never exceed target.
        kept_employees.sort(key=lambda pair: pair[0].get("fit_score", 0), reverse=True)
        kept_employees = kept_employees[:prospect_target]

        # ── ⑥ Apply prefetched emails; residual find for late (recovery) prospects ──────
        # email_by_url was filled concurrently with scoring in phase ④.
        # Only run the actor again for recovery prospects added in phase ⑤ (rare).
        #
        # OLD ACTOR — to revert, uncomment the block below and delete the new code,
        # then remove the asyncio.gather + _resolve_emails in phase ④ above:
        # missing_email_urls = list({
        #     t["linkedin"] for t, _ in kept_employees
        #     if not t.get("email") and t.get("linkedin")
        # })
        # email_by_url: dict[str, str | None] = {}
        # if missing_email_urls:
        #     logger.info(f"[fast:{campaign_id}] email finder — {len(missing_email_urls)} URLs")
        #     try:
        #         from services.email_finder_service import find_emails_for_linkedin_urls
        #         email_by_url = await _with_retries(
        #             lambda: find_emails_for_linkedin_urls(
        #                 missing_email_urls,
        #                 account_id=account_id,
        #                 campaign_id=campaign_id,
        #             ),
        #             fail_sentinel={},
        #         )
        #     except Exception as e:
        #         logger.warning(f"[fast:{campaign_id}] email finder failed (continuing without): {e}")
        still_missing = list({
            t["linkedin"] for t, _ in kept_employees
            if not t.get("email") and t.get("linkedin") and t["linkedin"] not in email_by_url
        })
        if still_missing:
            logger.info(f"[fast:{campaign_id}] email finder — {len(still_missing)} kept prospects needing email")
            try:
                residual = await _with_retries(
                    lambda: find_emails_bulk(still_missing, account_id=account_id, campaign_id=campaign_id),
                    fail_sentinel={},
                )
                email_by_url.update(residual)
            except Exception as e:
                logger.warning(f"[fast:{campaign_id}] residual email finder failed (continuing): {e}")

        # Apply emails
        emails_applied = 0
        for t, _ in kept_employees:
            if not t.get("email") and t.get("linkedin"):
                found = email_by_url.get(t["linkedin"])
                if found:
                    t["email"] = found
                    emails_applied += 1

        with_email = sum(1 for t, _ in kept_employees if t.get("email"))
        logger.info(
            f"[fast:{campaign_id}] email fill: {with_email}/{len(kept_employees)} have email "
            f"({emails_applied} new from finder)"
        )

        # Re-score now that email + industry are populated (activates two previously-dead
        # components: 15-pt email component and 18-pt industry component).
        for t, _ in kept_employees:
            updated_score = _score_for_campaign(t, campaign)
            t["fit_score"] = updated_score
            t["ai_prospect_score"] = float(updated_score)

        if kept_employees:
            _scores = sorted(set(round(t["fit_score"], 1) for t, _ in kept_employees))
            _sc_vals = [t["fit_score"] for t, _ in kept_employees]
            _score_mean = sum(_sc_vals) / len(_sc_vals)
            logger.info(
                f"[fast:{campaign_id}] FINAL SCORES after re-score: "
                f"distinct={len(_scores)} min={min(_sc_vals):.1f} max={max(_sc_vals):.1f} "
                f"mean={_score_mean:.1f} values={_scores[:15]}"
            )
            _industries = sorted(set(t.get("industry") or "MISSING" for t, _ in kept_employees))
            _with_industry = sum(1 for t, _ in kept_employees if t.get("industry"))
            logger.info(
                f"[fast:{campaign_id}] INDUSTRY FILL: {_with_industry}/{len(kept_employees)} "
                f"have industry — values={_industries[:10]}"
            )
            _domains = sorted(set(t.get("company_domain") or "NONE" for t, _ in kept_employees))
            logger.info(
                f"[fast:{campaign_id}] DOMAIN DISTRIBUTION: {len(_domains)} distinct domains "
                f"— sample={_domains[:10]}"
            )

        # ── ⑦ Upsert prospects (scraped + reused) ──────────────────────────────
        new_prospect_oids: list[ObjectId] = []
        per_company_counts: dict = {}
        for t, sc in kept_employees:
            oid = await _upsert_curated_prospect(t, campaign_oid, account_id)
            if oid:
                new_prospect_oids.append(oid)
                sc_id = sc.get("_id") or sc.get("company_linkedin_url", "")
                per_company_counts[sc_id] = per_company_counts.get(sc_id, 0) + 1

        # Upsert Stage C reused prospects (already in DB — this ensures stage/score are current)
        for _rp, _rsc in _reused_pairs:
            _rp_oid = await _upsert_curated_prospect(_rp, campaign_oid, account_id)
            if _rp_oid:
                new_prospect_oids.append(_rp_oid)
                _rsc_id = _rsc.get("_id") or _rsc.get("company_linkedin_url", "")
                per_company_counts[_rsc_id] = per_company_counts.get(_rsc_id, 0) + 1

        # Update per-company stats in sourced_companies
        for sc in kept_companies:
            sc_id = sc.get("_id") or sc.get("company_linkedin_url", "")
            co_url = sc.get("company_linkedin_url")
            scrape_count = sum(
                1 for emp, _ in employee_pairs
                if _extract_company_url_from_employee(emp) == co_url
            )
            await database.sourced_companies_collection.update_one(
                {"campaign_id": campaign_id, "company_linkedin_url": co_url},
                {"$set": {
                    "employee_scrape_status": "completed",
                    "employees_scraped_count": scrape_count,
                    "prospects_created_count": per_company_counts.get(sc_id, 0),
                }},
            )

        total_prospects = len(new_prospect_oids)
        _scraped_count = total_prospects - _db_enrolled_count
        await database.campaigns_collection.update_one(
            {"_id": campaign_oid},
            {"$set": {
                "curated_companies_scraped": len(companies_to_scrape),
                "discovery_prospects_found": total_prospects,
                "discovery_prospects_from_apify": max(0, _scraped_count),
                "discovery_prospects_from_db": _db_enrolled_count,
                "discovery_status": "enriching",
            }},
        )

        # ── ⑧ Pre-enroll + channel planning + Day-1 message generation ─────────
        from services.campaign_prospect_finder_service import (
            _pre_enroll_prospects,
        )

        new_prospects_full = await database.prospects_collection.find(
            {"_id": {"$in": new_prospect_oids}}
        ).to_list(length=None)

        # Supplement with reused prospect docs (already have all fields; avoids re-fetch)
        _reused_ids_in_db = {str(p["_id"]) for p in new_prospects_full}
        for _rp, _ in _reused_pairs:
            if _rp.get("_id") and str(_rp["_id"]) not in _reused_ids_in_db:
                new_prospects_full.append(_rp)

        if new_prospects_full:
            await _pre_enroll_prospects(campaign, new_prospects_full)

        # ── Mark discovery complete ──────────────────────────────────────────────
        await database.campaigns_collection.update_one(
            {"_id": campaign_oid},
            {"$set": {
                "discovery_status": "completed",
                "discovery_completed_at": datetime.utcnow(),
                "status": "awaiting_approval",
                "approved_send_days": [],
            }},
        )

        # ── Channel planning (auto-pick senders + assign channel+day to enrollments) ──
        plan_result = await finalize_channel_plan(campaign_id, account_id)
        total_assigned = plan_result.get("assigned", 0)

        # Write UI metadata to match database-mode flow
        await database.campaigns_collection.update_one(
            {"_id": campaign_oid},
            {"$set": {
                "discovery_prospects_planned": total_assigned,
                "discovery_prospects_enrolled": total_assigned,
            }},
        )

        # ── Fire deep enrichment (post scrape + intelligence) then Day-1 messages ──
        # Day-1 cohort gets enriched first so messages use prospect_intelligence.
        # Remaining days are enriched + messaged in background after Day 1 is done.
        if total_assigned > 0:
            day1_enrs = await database.campaign_enrollments_collection.find(
                {"campaign_id": campaign_oid, "smart_campaign_send_day": 1, "status": "active"}
            ).to_list(length=5000)
            day1_oids = [e["prospect_id"] for e in day1_enrs]
            asyncio.create_task(
                _run_deep_enrichment_then_messages(
                    campaign_id=campaign_id,
                    account_id=str(account_id),
                    day1_prospect_oids=day1_oids,
                    all_prospect_oids=new_prospect_oids,
                )
            )
        else:
            # No prospects assigned (no sender or no contactable prospects) — resolve
            # the spinner explicitly so the UI shows the actionable "No schedule yet" state.
            await database.campaigns_collection.update_one(
                {"_id": campaign_oid},
                {"$set": {
                    "message_gen_status": "completed",
                    "message_gen_completed_at": datetime.utcnow(),
                }},
            )

        logger.info(
            f"[fast:{campaign_id}] complete — "
            f"companies={len(kept_companies)} (scraped={len(companies_to_scrape)}, reused_co={len(kept_companies)-len(companies_to_scrape)}), "
            f"prospects={total_prospects} (scraped={max(0,_scraped_count)}, reused={_db_enrolled_count}), "
            f"assigned={total_assigned}"
        )
        return {"campaign_id": campaign_id, "prospects_created": total_prospects, "prospects_from_db": _db_enrolled_count}

    except Exception as e:
        logger.exception(f"[fast:{campaign_id}] failed")
        await database.campaigns_collection.update_one(
            {"_id": campaign_oid},
            {"$set": {
                "discovery_status": "failed",
                "discovery_error": str(e)[:500],
            }},
        )
        raise


# ──────────────────────────────────────────────────────────────────────────────
# Day-1 message generation (background, fail-soft)
# ──────────────────────────────────────────────────────────────────────────────

async def _run_day1_message_gen(campaign_id: str, account_id: str) -> None:
    """
    Campaign-level Day-1 message generation.
    Replaces the old per-enrollment _generate_day1_messages.
    Uses generate_messages_for_campaign so that campaign-level message_gen_status
    advances running→completed (resolves the Schedule tab spinner).
    send_day=1 keeps generation scoped to Day-1 enrollments (message_gen_status:pending).
    """
    try:
        from services.campaign_message_generator_service import generate_messages_for_campaign
        await generate_messages_for_campaign(campaign_id, account_id, send_day=1)
        logger.info(f"[fast:{campaign_id}] Day-1 messages generated via campaign-level generator")
    except Exception as e:
        logger.warning(f"[fast:{campaign_id}] Day-1 message gen failed: {e}")
        try:
            await database.campaigns_collection.update_one(
                {"_id": ObjectId(campaign_id)},
                {"$set": {"message_gen_status": "failed", "message_gen_error": str(e)[:300]}},
            )
        except Exception:
            pass


async def _run_deep_enrichment_then_messages(
    campaign_id: str,
    account_id: str,
    day1_prospect_oids: list[ObjectId],
    all_prospect_oids: list[ObjectId],
) -> None:
    """
    Full deep enrichment pipeline:
    0. Scrape enrolled company LinkedIn pages (one-time)
    1. Scrape LinkedIn posts for Day-1 cohort (up to 45 prospects)
    2. Generate prospect_intelligence for Day-1 cohort in batches of 5
    3. Store intelligence on prospect docs
    4. Run Day-1 message generation (uses intelligence when available)
    5. In background: enrich remaining prospects (Days 2-3) and generate their messages
    """
    from services.campaign_message_generator_service import generate_messages_for_campaign

    try:
        # Read skip-message-gen flag from campaign doc (gated — default False so prod is unchanged)
        skip_message_gen = False
        try:
            _flag_doc = await database.campaigns_collection.find_one(
                {"_id": ObjectId(campaign_id)}, {"discovery_skip_message_gen": 1}
            )
            skip_message_gen = bool((_flag_doc or {}).get("discovery_skip_message_gen", False))
        except Exception:
            pass
        if skip_message_gen:
            logger.info(f"[fast:{campaign_id}] discovery_skip_message_gen=True — will skip message gen")

        # ── Company LinkedIn scrape (enrolled companies, one-time) ─────────────────
        try:
            # Get enrolled company URLs from the campaign's prospects
            all_enrolled_prospects = await database.prospects_collection.find(
                {"source_industry_ids": f"curated:{campaign_id}"}
            ).to_list(length=500)

            enrolled_co_urls = list({
                (p.get("company_linkedin") or "").rstrip("/")
                for p in all_enrolled_prospects
                if p.get("company_linkedin")
            })

            if enrolled_co_urls:
                logger.info(f"[fast:{campaign_id}] scraping {len(enrolled_co_urls)} enrolled company LinkedIn pages")
                from services.company_scraper_service import scrape_company_pages
                import asyncio as _asyncio_co
                loop = _asyncio_co.get_event_loop()
                _, co_pages = await loop.run_in_executor(None, scrape_company_pages, enrolled_co_urls)

                if co_pages:
                    from services.employee_scraper_service import _save_company
                    co_by_url: dict[str, dict] = {}
                    for cp in co_pages:
                        # Build url-keyed lookup for the research step (handles both camelCase and snake_case keys)
                        li_url = (
                            cp.get("linkedinUrl") or cp.get("linkedin_url")
                            or cp.get("url") or cp.get("companyUrl") or ""
                        ).rstrip("/")
                        if li_url:
                            co_by_url[li_url.lower()] = cp

                    # Canonicalize each company page (sets industry.id + location.country_code + employee_band)
                    # so companies are matchable by Stage-A DB search on the next campaign.
                    _save_results = await asyncio.gather(
                        *[_save_company(cp) for cp in co_pages],
                        return_exceptions=True,
                    )
                    _saved_count = sum(1 for r in _save_results if r and not isinstance(r, Exception))
                    logger.info(
                        f"[fast:{campaign_id}] saved {_saved_count}/{len(co_pages)} company pages "
                        f"to companies_collection (canonical)"
                    )

                    # company data is now stored on companies_collection (not inline on prospects)
        except Exception as _co_err:
            logger.warning(f"[fast:{campaign_id}] company LinkedIn scrape failed (non-fatal): {_co_err}")

        # ── Per-company news + competitor research (gated by campaign doc flag) ─────────────
        # Runs ONCE per company, cached in co_research_by_url, shared across all prospects.
        # Enable: set campaign.discovery_enable_company_research = True (script only for now).
        co_research_by_url: dict = {}
        campaign_doc_for_research = await database.campaigns_collection.find_one(
            {"_id": ObjectId(campaign_id)}, {"discovery_enable_company_research": 1}
        )
        enable_company_research = bool(
            (campaign_doc_for_research or {}).get("discovery_enable_company_research", True)
        )

        if enable_company_research:
            try:
                from services.competitor_research_service import research_competitors
                from services.news_research_service import research_company_news
                from services.openrouter_service import OpenRouterClient as _ORClient

                # Collect unique companies from all enrolled prospects
                _all_enrolled = await database.prospects_collection.find(
                    {"_id": {"$in": all_prospect_oids}},
                    {"company_linkedin": 1, "company_name": 1, "company_domain": 1, "industry": 1},
                ).to_list(length=len(all_prospect_oids))

                _companies_for_research: dict[str, dict] = {}
                for _p in _all_enrolled:
                    _li_url = (_p.get("company_linkedin") or "").rstrip("/")
                    if _li_url and _li_url not in _companies_for_research:
                        _companies_for_research[_li_url] = {
                            "name": _p.get("company_name") or "",
                            "domain": _p.get("company_domain") or "",
                            "industry": _p.get("industry") or "",
                        }

                logger.info(
                    f"[fast:{campaign_id}] company research: {len(_companies_for_research)} unique companies"
                )

                _research_sem = asyncio.Semaphore(3)  # max 3 concurrent Perplexity calls

                async def _research_one(li_url: str, co: dict) -> tuple[str, dict]:
                    async with _research_sem:
                        _res: dict = {"competitors": [], "news": []}
                        if not co.get("name"):
                            return li_url, _res
                        _rc = _ORClient()
                        try:
                            _res["competitors"] = await research_competitors(
                                co["name"],
                                company_website=co.get("domain") or None,
                                industry=co.get("industry") or None,
                                limit=3,
                                client=_rc,
                            )
                            _res["news"] = await research_company_news(
                                co["name"],
                                limit=3,
                                days_back=90,
                                client=_rc,
                            )
                        except Exception as _re:
                            logger.warning(
                                f"[fast:{campaign_id}] research failed for {co['name']}: {_re}"
                            )
                        finally:
                            await _rc.close()
                        return li_url, _res

                _research_results = await asyncio.gather(
                    *[_research_one(u, c) for u, c in _companies_for_research.items()],
                    return_exceptions=False,
                )

                # Cache and persist to companies_collection
                from pymongo import UpdateOne as _ResUpdateOne
                _res_ops = []
                for _li_url, _res in _research_results:
                    co_research_by_url[_li_url] = _res
                    if _li_url:
                        _res_ops.append(_ResUpdateOne(
                            {"linkedin_url": _li_url},
                            {"$set": {"research": {**_res, "fetched_at": datetime.utcnow()}}},
                            upsert=True,
                        ))
                if _res_ops:
                    await database.companies_collection.bulk_write(_res_ops, ordered=False)

                logger.info(
                    f"[fast:{campaign_id}] company research complete: "
                    f"{len(co_research_by_url)} companies, "
                    f"{sum(len(v.get('news', [])) for v in co_research_by_url.values())} news items, "
                    f"{sum(len(v.get('competitors', [])) for v in co_research_by_url.values())} competitors"
                )
            except Exception as _re:
                logger.warning(f"[fast:{campaign_id}] company research phase failed (non-fatal): {_re}")

        # ── Day-1 deep enrichment ─────────────────────────────────────────────
        if day1_prospect_oids:
            await _enrich_prospect_cohort(
                prospect_oids=day1_prospect_oids,
                campaign_id=campaign_id,
                account_id=account_id,
                label="day1",
                co_research_by_url=co_research_by_url or None,
            )

        # ── Day-1 message generation (now with intelligence) ──────────────────
        if skip_message_gen:
            # Mark day1 enrollments as skipped so the poll's pending count drains
            await database.campaign_enrollments_collection.update_many(
                {
                    "campaign_id": ObjectId(campaign_id),
                    "prospect_id": {"$in": day1_prospect_oids},
                    "message_gen_status": {"$in": [None, "pending"]},
                },
                {"$set": {"message_gen_status": "skipped"}},
            )
            logger.info(f"[fast:{campaign_id}] Day-1 message gen skipped ({len(day1_prospect_oids)} enrollments marked skipped)")
        else:
            await generate_messages_for_campaign(campaign_id, account_id, send_day=1)
            logger.info(f"[fast:{campaign_id}] Day-1 messages generated")

        # ── Days 2-5 enrichment + message gen (background, non-blocking) ──────
        remaining_oids = [oid for oid in all_prospect_oids if oid not in set(day1_prospect_oids)]
        if remaining_oids:
            asyncio.create_task(
                _enrich_remaining_days(
                    campaign_id=campaign_id,
                    account_id=account_id,
                    remaining_oids=remaining_oids,
                    co_research_by_url=co_research_by_url or None,
                    skip_message_gen=skip_message_gen,
                )
            )

    except Exception as e:
        logger.warning(f"[fast:{campaign_id}] deep enrichment+messages failed: {e}")
        try:
            await database.campaigns_collection.update_one(
                {"_id": ObjectId(campaign_id)},
                {"$set": {"message_gen_status": "failed", "message_gen_error": str(e)[:300]}},
            )
        except Exception:
            pass


async def _enrich_prospect_cohort(
    prospect_oids: list[ObjectId],
    campaign_id: str,
    account_id: str,
    label: str = "cohort",
    co_research_by_url: dict | None = None,
) -> None:
    """Scrape posts + generate intelligence for a batch of prospect OIDs.

    co_research_by_url: optional {company_linkedin_url: {competitors, news}} cache
    from _run_deep_enrichment_then_messages. When present, injected into each prospect
    dict before intelligence generation so news/competitors reach the AI prompt.
    """
    from services.linkedin_post_scraper_service import scrape_linkedin_posts_bulk
    from services.prospect_intelligence_service import (
        generate_base_intelligence_batch,
        store_base_intelligence,
        generate_pitch_batch,
        store_pitch_for_account,
    )

    if not prospect_oids:
        return

    try:
        prospects = await database.prospects_collection.find(
            {"_id": {"$in": prospect_oids}}
        ).to_list(length=len(prospect_oids))

        if not prospects:
            return

        company_profile = await database.company_profiles_collection.find_one(
            {"account_id": account_id}
        )

        # Scrape posts for all prospects with a LinkedIn URL
        li_urls = [p["linkedin"] for p in prospects if p.get("linkedin")]
        posts_by_url: dict = {}
        if li_urls:
            try:
                posts_by_url = await scrape_linkedin_posts_bulk(li_urls, posts_per_profile=5)
                logger.info(
                    f"[fast:{campaign_id}] {label} post scrape: "
                    f"{sum(1 for v in posts_by_url.values() if v)}/{len(li_urls)} profiles had posts"
                )
            except Exception as e:
                logger.warning(f"[fast:{campaign_id}] {label} post scrape failed (continuing): {e}")

        # Attach posts + company research to prospect dicts for intelligence generation
        enriched_prospects = []
        for p in prospects:
            p_copy = dict(p)
            p_copy["posts"] = posts_by_url.get(p.get("linkedin") or "", [])
            # Inject company-level research so AI intel can reference news + competitors
            if co_research_by_url:
                co_url = (p.get("company_linkedin") or "").rstrip("/")
                research = co_research_by_url.get(co_url) or {}
                if research:
                    p_copy["company_competitors"] = research.get("competitors", [])
                    p_copy["company_news"] = research.get("news", [])
                    # Also inject into fields the batch message prompt reads
                    p_copy["recent_news"] = research.get("news", [])
                    _comp_names = [c.get("name", "") for c in research.get("competitors", [])[:3] if c.get("name")]
                    if _comp_names:
                        p_copy.setdefault("ai_assessment", {})["competitor_summary"] = ", ".join(_comp_names)
            enriched_prospects.append(p_copy)

        # ── Base intelligence (tenant-agnostic, stored on prospects.prospect_intelligence_base) ──
        intelligence_list = await generate_base_intelligence_batch(
            enriched_prospects,
            account_id=account_id,
            campaign_id=campaign_id,
        )
        await store_base_intelligence(prospect_oids, intelligence_list)
        logger.info(
            f"[fast:{campaign_id}] {label} base intelligence stored for "
            f"{sum(1 for i in intelligence_list if i)}/{len(prospects)} prospects"
        )

        # Inject base intel back so pitch generation has it
        for p_copy, intel in zip(enriched_prospects, intelligence_list):
            p_copy["prospect_intelligence_base"] = intel

        # ── Per-tenant pitch (stored on prospect_state.pitch) ────────────────────────────────
        if company_profile:
            try:
                pitches = await generate_pitch_batch(
                    enriched_prospects,
                    company_profile,
                    account_id=account_id,
                    campaign_id=campaign_id,
                )
                await store_pitch_for_account(prospect_oids, pitches, account_id)
                logger.info(
                    f"[fast:{campaign_id}] {label} pitch stored for "
                    f"{sum(1 for p in pitches if p)}/{len(prospects)} prospects"
                )
            except Exception as e:
                logger.warning(f"[fast:{campaign_id}] {label} pitch generation failed (continuing): {e}")

        # Persist scraped posts to prospect documents
        if posts_by_url:
            from pymongo import UpdateOne as _UpdateOne
            _posts_ops = []
            for _li_url, _posts_list in posts_by_url.items():
                _p = next(
                    (p for p in prospects
                     if (p.get("linkedin") or "").rstrip("/").lower() == _li_url.rstrip("/").lower()),
                    None,
                )
                if _p and _p.get("_id") and _posts_list:
                    _posts_ops.append(_UpdateOne(
                        {"_id": _p["_id"]},
                        {"$set": {"posts": _posts_list}}
                    ))
            if _posts_ops:
                await database.prospects_collection.bulk_write(_posts_ops, ordered=False)
                logger.info(f"[fast:{campaign_id}] persisted posts for {len(_posts_ops)} prospects")

    except Exception as e:
        logger.warning(f"[fast:{campaign_id}] {label} enrichment failed (continuing): {e}")


async def _enrich_remaining_days(
    campaign_id: str,
    account_id: str,
    remaining_oids: list[ObjectId],
    co_research_by_url: dict | None = None,
    skip_message_gen: bool = False,
) -> None:
    """Enrich Days 2-5 prospects and generate their messages in background."""
    from services.campaign_message_generator_service import generate_messages_for_campaign

    try:
        await _enrich_prospect_cohort(
            prospect_oids=remaining_oids,
            campaign_id=campaign_id,
            account_id=account_id,
            label="days2-5",
            co_research_by_url=co_research_by_url,
        )
        if skip_message_gen:
            # Mark remaining enrollments as skipped so the poll's pending count drains
            await database.campaign_enrollments_collection.update_many(
                {
                    "campaign_id": ObjectId(campaign_id),
                    "prospect_id": {"$in": remaining_oids},
                    "message_gen_status": {"$in": [None, "pending"]},
                },
                {"$set": {"message_gen_status": "skipped"}},
            )
            logger.info(f"[fast:{campaign_id}] days2-5 message gen skipped ({len(remaining_oids)} enrollments marked skipped)")
        else:
            # Pre-generate day 2 messages so the user sees them immediately on the review page.
            # Days 3+ are generated on demand when the user approves each preceding day (D5).
            try:
                await generate_messages_for_campaign(campaign_id, account_id, send_day=2)
            except Exception as e:
                logger.warning(f"[fast:{campaign_id}] day 2 message gen failed: {e}")
    except Exception as e:
        logger.warning(f"[fast:{campaign_id}] remaining days enrichment failed: {e}")


# ──────────────────────────────────────────────────────────────────────────────
# LLM batch scoring
# ──────────────────────────────────────────────────────────────────────────────

def _score_company_deterministic(company: dict, icp_prompt: str) -> float:
    """Deterministic company score (0-100). No LLM calls."""
    score = 0.0
    icp_lower = icp_prompt.lower()

    # Industry match (0-50): check if company industry keywords appear in ICP prompt.
    # industry may be a string (Gemini-sourced) or a dict (DB company with canonical fields).
    _ind_raw = company.get("industry") or ""
    if isinstance(_ind_raw, dict):
        industry = (_ind_raw.get("label") or _ind_raw.get("raw") or "").lower().strip()
    else:
        industry = str(_ind_raw).lower().strip()
    if industry:
        industry_words = [w for w in industry.replace("-", " ").split() if len(w) > 3]
        if any(word in icp_lower for word in industry_words):
            score += 50
        elif industry and len(industry) > 3 and industry[:4] in icp_lower:
            score += 30
    else:
        score += 25  # unknown industry: partial credit to not over-exclude

    # Company size hint (0-30): prefer companies that have size info
    size_raw = company.get("employee_size_estimate") or company.get("company_size")
    if size_raw:
        score += 30
    else:
        score += 10  # no size data: partial credit

    # Has LinkedIn URL (0-20): required for employee scraping
    if company.get("company_linkedin_url"):
        score += 20

    # Location match (0-10): soft boost if company country appears in ICP prompt
    _co_loc = company.get("location") or {}
    _country = (
        (_co_loc.get("country") if isinstance(_co_loc, dict) else None)
        or company.get("country") or ""
    ).lower()
    if _country and _country in icp_lower:
        score += 10

    # Funding match (0-10): soft boost if company funding stage appears in ICP prompt
    _funding = (company.get("funding_stage") or company.get("funding_raw") or "").lower()
    if _funding and _funding in icp_lower:
        score += 10

    return min(100.0, score)


async def _score_companies_with_llm(
    companies: list[dict],
    icp_prompt: str,
    client,
) -> list[int]:
    """Haiku batch scores a list of companies. Returns list of ints (0-100), same order."""
    if not companies:
        return []

    from utils.prompts import COMPANY_BATCH_SCORE_SYSTEM_PROMPT, build_company_batch_score_prompt

    # Chunk at 100 to keep token count reasonable
    all_scores: list[int] = []
    for chunk in _chunk(companies, 100):
        try:
            resp = await client.chat_completion(
                messages=[
                    {"role": "system", "content": COMPANY_BATCH_SCORE_SYSTEM_PROMPT},
                    {"role": "user", "content": build_company_batch_score_prompt(chunk, icp_prompt)},
                ],
                model=settings.mini_enrichment_model,
                temperature=0.1,
                max_tokens=512,
                response_format={"type": "json_object"},
            )
            raw_scores = resp.get("scores", [])
            if not isinstance(raw_scores, list) or len(raw_scores) != len(chunk):
                raise ValueError(f"Expected {len(chunk)} scores, got {len(raw_scores)}")
            all_scores.extend(int(s) for s in raw_scores)
        except Exception as e:
            logger.warning(f"[fast] company scoring chunk failed ({e}), using rule-based fallback")
            all_scores.extend(_rule_score_companies(chunk))
    return all_scores


async def _score_employees_with_llm(
    employees: list[dict],
    icp_prompt: str,
    sender_context: str,
    client,
) -> list[int]:
    """Haiku batch scores a list of employee/prospect dicts. Returns list of ints."""
    if not employees:
        return []

    from utils.prompts import EMPLOYEE_BATCH_SCORE_SYSTEM_PROMPT, build_employee_batch_score_prompt

    all_scores: list[int] = []
    for chunk in _chunk(employees, 100):
        try:
            resp = await client.chat_completion(
                messages=[
                    {"role": "system", "content": EMPLOYEE_BATCH_SCORE_SYSTEM_PROMPT},
                    {"role": "user", "content": build_employee_batch_score_prompt(chunk, icp_prompt, sender_context)},
                ],
                model=settings.mini_enrichment_model,
                temperature=0.1,
                max_tokens=512,
                response_format={"type": "json_object"},
            )
            raw_scores = resp.get("scores", [])
            if not isinstance(raw_scores, list) or len(raw_scores) != len(chunk):
                raise ValueError(f"Expected {len(chunk)} scores, got {len(raw_scores)}")
            all_scores.extend(int(s) for s in raw_scores)
        except Exception as e:
            logger.warning(f"[fast] employee scoring chunk failed ({e}), using rule-based fallback")
            all_scores.extend(_rule_score_employees(chunk))
    return all_scores


# ──────────────────────────────────────────────────────────────────────────────
# Retry wrapper
# ──────────────────────────────────────────────────────────────────────────────

async def _with_retries(fn, retries: int = 3, backoffs: tuple = (1, 4, 16), fail_sentinel=None):
    """Retry an async callable on any exception, with exponential backoff."""
    last_exc = None
    for attempt in range(retries):
        try:
            return await fn()
        except Exception as e:
            last_exc = e
            if attempt < retries - 1:
                wait = backoffs[min(attempt, len(backoffs) - 1)]
                logger.warning(f"[fast] retry {attempt + 1}/{retries} after {wait}s: {e}")
                await asyncio.sleep(wait)
    if fail_sentinel is not None:
        logger.error(f"[fast] all {retries} retries exhausted, returning sentinel: {last_exc}")
        return fail_sentinel
    raise last_exc


# ──────────────────────────────────────────────────────────────────────────────
# Rule-based fallbacks (used when Haiku fails)
# ──────────────────────────────────────────────────────────────────────────────

def _rule_score_companies(companies: list[dict]) -> list[int]:
    return [50 for _ in companies]  # neutral score; let through


def _rule_score_employees(employees: list[dict]) -> list[int]:
    scores = []
    for e in employees:
        s = 0
        title = (e.get("job_title") or e.get("headline") or "").lower()
        seniority = (e.get("seniority") or e.get("seniority_level") or "").lower()
        if any(k in title for k in ("vp", "vice president", "director", "head of", "ceo", "cto", "cfo", "founder")):
            s += 35
        elif any(k in title for k in ("manager", "lead", "senior", "principal")):
            s += 20
        if seniority in ("vp", "director", "head", "c_suite", "founder", "owner"):
            s += 30
        elif seniority == "manager":
            s += 20
        if e.get("email"):
            s += 15
        scores.append(min(s, 100))
    return scores


# ──────────────────────────────────────────────────────────────────────────────
# Seniority / function broadening for recovery pass
# ──────────────────────────────────────────────────────────────────────────────

_SENIORITY_ORDERING = ["120", "220", "210", "300", "310", "320"]  # senior→manager→director→vp→csuite→founder

def _broaden_seniority_ids(ids: list[str]) -> list[str]:
    """Add 1 adjacent tier on each side of each selected ID."""
    if not ids:
        return _SENIORITY_ORDERING  # no filter → all levels
    result = set(ids)
    for sid in ids:
        if sid in _SENIORITY_ORDERING:
            idx = _SENIORITY_ORDERING.index(sid)
            if idx > 0:
                result.add(_SENIORITY_ORDERING[idx - 1])
            if idx < len(_SENIORITY_ORDERING) - 1:
                result.add(_SENIORITY_ORDERING[idx + 1])
    return sorted(result)


_FUNCTION_ADJACENCY: dict[str, list[str]] = {
    "4": ["24", "20"],    # engineering → product, operations
    "24": ["4", "25"],    # product → engineering, sales
    "25": ["15", "20"],   # sales → marketing, operations
    "15": ["25", "24"],   # marketing → sales, product
    "5": ["20", "10"],    # finance → operations, hr
    "10": ["20", "5"],    # hr → operations, finance
    "20": ["25", "5"],    # operations → sales, finance
}

def _broaden_function_ids(ids: list[str]) -> list[str]:
    """Add 1 adjacent function for each selected ID."""
    if not ids:
        return list(_FUNCTION_ADJACENCY.keys())  # all functions
    result = set(ids)
    for fid in ids:
        for adj in _FUNCTION_ADJACENCY.get(fid, [])[:1]:
            result.add(adj)
    return sorted(result)


# ──────────────────────────────────────────────────────────────────────────────
# Prospect upsert
# ──────────────────────────────────────────────────────────────────────────────

async def _upsert_curated_prospect(
    prospect_doc: dict,
    campaign_oid: ObjectId,
    account_id: str,
) -> Optional[ObjectId]:
    li = prospect_doc.get("linkedin")
    email = prospect_doc.get("email")

    if li:
        query = {"linkedin": li}
    elif email:
        query = {"email": email}
    else:
        return None

    now = datetime.utcnow()
    # Ensure stage is contactable so this prospect is visible to search_prospects on next campaign.
    # transform_employee_to_prospect already sets "contactable", but guard other call paths too.
    prospect_doc.setdefault("stage", "contactable")
    # Strip ICP-relative / tenant-scoped fields from the shared prospect doc
    _skip = {"account_id", "is_smart_campaign", "source_industry_ids", "status", "tags",
              "ai_prospect_score", "prospect_score", "ai_assessment", "ai_score_breakdown",
              "prospect_intelligence", "outreach_messages", "priority_tier",
              "enrichment_status", "first_seen_at"}  # reserved for $setOnInsert
    update_set = {k: v for k, v in prospect_doc.items() if v is not None and k not in _skip}
    update_set["last_updated_at"] = now

    from pymongo.errors import DuplicateKeyError as MongoDupKeyError
    try:
        result = await database.prospects_collection.find_one_and_update(
            query,
            {
                "$set": update_set,
                "$setOnInsert": {"first_seen_at": now, "enrichment_status": "not_started"},
            },
            upsert=True,
            return_document=ReturnDocument.AFTER,
        )
        prospect_oid = result["_id"] if result else None
    except MongoDupKeyError:
        fallback_query = {"email": email} if email and query.get("linkedin") else {"linkedin": li}
        result = await database.prospects_collection.find_one_and_update(
            fallback_query,
            {"$set": {"last_updated_at": now}},
            return_document=ReturnDocument.AFTER,
        )
        prospect_oid = result["_id"] if result else None

    # Ensure prospect_state overlay exists for this tenant
    if prospect_oid and account_id:
        try:
            _aid = str(account_id)
            _pid = str(prospect_oid)
            await database.prospect_state_collection.update_one(
                {"account_id": _aid, "prospect_id": _pid},
                {"$setOnInsert": {
                    "account_id": _aid,
                    "prospect_id": _pid,
                    "status": "new",
                    "tags": [],
                    "used_by": [],
                    "created_at": now,
                    "last_updated_at": now,
                }},
                upsert=True,
            )
        except Exception as _oe:
            logger.debug(f"prospect_state upsert failed: {_oe}")

    return prospect_oid


# ──────────────────────────────────────────────────────────────────────────────
# ICP prompt / ID mappings
# ──────────────────────────────────────────────────────────────────────────────

def _build_icp_prompt_from_campaign(campaign: dict) -> str:
    explicit = (campaign.get("curated_icp_prompt") or "").strip()
    if explicit:
        return explicit
    parts = []
    if campaign.get("icp_industries"):
        parts.append(f"Industries: {', '.join(campaign['icp_industries'])}")
    if campaign.get("icp_countries"):
        parts.append(f"Countries: {', '.join(campaign['icp_countries'])}")
    if campaign.get("icp_keywords"):
        parts.append(f"Keywords: {', '.join(campaign['icp_keywords'])}")
    if campaign.get("icp_company_size_min") or campaign.get("icp_company_size_max"):
        lo = campaign.get("icp_company_size_min") or "any"
        hi = campaign.get("icp_company_size_max") or "any"
        parts.append(f"Company size: {lo} to {hi} employees")
    if campaign.get("icp_funding_stages"):
        parts.append(f"Funding stages: {', '.join(campaign['icp_funding_stages'])}")
    return " | ".join(parts) or "B2B technology companies"


def _build_sender_context(company_profile: dict | None) -> str:
    if not company_profile:
        return ""
    parts = []
    if company_profile.get("company_name"):
        parts.append(company_profile["company_name"])
    if company_profile.get("value_proposition"):
        parts.append(company_profile["value_proposition"])
    if company_profile.get("target_customer_description"):
        parts.append(company_profile["target_customer_description"])
    return " | ".join(parts)


_SENIORITY_LABEL_TO_ACTOR_ID: dict[str, list[str]] = {
    "c_suite": ["310"],
    "csuite": ["310"],
    "founder": ["320"],
    "owner": ["320"],
    "partner": ["320"],
    "vp": ["300"],
    "director": ["210"],
    "head": ["210"],
    "manager": ["220"],
    "senior": ["120"],
}


def _icp_seniority_to_actor_ids(labels: list[str]) -> list[str]:
    out: set[str] = set()
    for label in labels:
        key = (label or "").lower().replace("-", "_").replace(" ", "_")
        for actor_id in _SENIORITY_LABEL_TO_ACTOR_ID.get(key, []):
            out.add(actor_id)
    return sorted(out)


def _icp_function_to_actor_ids(labels: list[str]) -> list[str]:
    mapping = {
        "engineering": "4",
        "sales": "25",
        "marketing": "15",
        "product": "24",
        "product_management": "24",
        "finance": "5",
        "hr": "10",
        "human_resources": "10",
        "operations": "20",
    }
    return [mapping[l.lower().replace(" ", "_")] for l in labels if l.lower().replace(" ", "_") in mapping]


# ──────────────────────────────────────────────────────────────────────────────
# Helpers
# ──────────────────────────────────────────────────────────────────────────────

def _chunk(lst: list, size: int):
    for i in range(0, len(lst), size):
        yield lst[i:i + size]


def _extract_company_url_from_employee(emp: dict) -> str | None:
    """Extract the company LinkedIn URL from an employee record returned by Apify.

    Priority order (highest reliability first):
    1. _meta.query.currentCompanies[0] — echoes the exact URL we passed to the actor (100% reliable)
    2. currentPosition[0].companyLinkedinUrl (singular list, present on ~87% of records)
    3. currentPositions[0] (plural, legacy harvestapi field — rarely populated)
    4. Top-level companyUrl / companyLinkedinUrl
    """
    # Primary: actor echoes the requested URL in _meta.query.currentCompanies[0]
    try:
        meta_companies = (emp.get("_meta") or {}).get("query", {}).get("currentCompanies") or []
        if meta_companies:
            url = meta_companies[0]
            if url:
                return _normalize_li_url(str(url))
    except Exception:
        pass

    # Secondary: currentPosition (singular) — actor returns this as a list
    positions = emp.get("currentPosition") or []
    if positions:
        url = positions[0].get("companyLinkedinUrl") or positions[0].get("companyUrl")
        if url:
            return _normalize_li_url(url)

    # Legacy: currentPositions (plural) — harvestapi used to return this
    legacy = emp.get("currentPositions") or []
    if legacy:
        url = legacy[0].get("companyLinkedinUrl") or legacy[0].get("companyUrl")
        if url:
            return _normalize_li_url(url)

    # Top-level fallback
    return (
        _normalize_li_url(emp.get("companyUrl"))
        or _normalize_li_url(emp.get("companyLinkedinUrl"))
    )


def _normalize_li_url(url: str | None) -> str | None:
    if not url:
        return None
    url = url.strip().rstrip("/")
    if not url.startswith("http"):
        url = "https://" + url.lstrip("/")
    return url.lower()


def _find_closest_sc(company_url: str, url_to_sc: dict) -> dict | None:
    """Find a sourced company by partial URL match when exact key misses."""
    if not company_url:
        return None
    norm = company_url.lower().rstrip("/")
    for key, sc in url_to_sc.items():
        if key and norm in key.lower() or (key and key.lower() in norm):
            return sc
    return None


# ──────────────────────────────────────────────────────────────────────────────
# Per-day approval: generate next day's messages on demand (D5)
# ──────────────────────────────────────────────────────────────────────────────

async def ensure_day_ready_then_generate(
    campaign_id: str,
    account_id: str,
    day: int,
) -> None:
    """
    Idempotent: generate messages for `day` of this campaign.

    Called by the approve-day endpoint after approving day N to pre-fill day N+1.
    Safe to call multiple times — `generate_messages_for_campaign` only generates
    'pending' / 'scheduled_later' enrollments, so already-generated days are a
    cheap no-op.

    Also guards against missing enrichment: if a day-N prospect somehow lacks
    `prospect_intelligence_base` (e.g. enrichment for that cohort failed earlier),
    runs a targeted enrichment pass before message generation.
    """
    from services.campaign_message_generator_service import generate_messages_for_campaign

    try:
        campaign_oid = ObjectId(campaign_id)

        # Check if this day has any enrollments
        enr_count = await database.campaign_enrollments_collection.count_documents({
            "campaign_id": campaign_oid,
            "smart_campaign_send_day": day,
            "status": {"$nin": ["archived", "skipped_no_channel", "failed"]},
        })
        if enr_count == 0:
            logger.info(f"[day_gen:{campaign_id}] day {day} has no eligible enrollments — no-op")
            return

        # Spot-check for missing intelligence (enrichment may have failed for some)
        enr_docs = await database.campaign_enrollments_collection.find(
            {
                "campaign_id": campaign_oid,
                "smart_campaign_send_day": day,
                "status": {"$nin": ["archived", "skipped_no_channel", "failed"]},
                "message_gen_status": {"$in": [None, "pending", "scheduled_later"]},
            },
            {"prospect_id": 1},
        ).to_list(length=500)

        if enr_docs:
            pids = [e["prospect_id"] for e in enr_docs]
            need_intel = await database.prospects_collection.find(
                {"_id": {"$in": pids}, "prospect_intelligence_base": {"$exists": False}},
                {"_id": 1},
            ).to_list(length=len(pids))

            if need_intel:
                need_oids = [d["_id"] for d in need_intel]
                logger.info(
                    f"[day_gen:{campaign_id}] day {day}: enriching {len(need_oids)} prospects "
                    f"missing intelligence before message gen"
                )
                try:
                    # Reload company research from DB for use in enrichment
                    co_research_by_url: dict = {}
                    enrolled_prospects = await database.prospects_collection.find(
                        {"_id": {"$in": need_oids}},
                        {"company_linkedin": 1},
                    ).to_list(length=len(need_oids))
                    co_urls = list({
                        (p.get("company_linkedin") or "").rstrip("/")
                        for p in enrolled_prospects
                        if p.get("company_linkedin")
                    })
                    if co_urls:
                        async for co_doc in database.companies_collection.find(
                            {"linkedin_url": {"$in": co_urls}},
                            {"linkedin_url": 1, "research": 1},
                        ):
                            url = (co_doc.get("linkedin_url") or "").rstrip("/")
                            research = co_doc.get("research")
                            if url and research:
                                co_research_by_url[url.lower()] = research

                    await _enrich_prospect_cohort(
                        prospect_oids=need_oids,
                        campaign_id=campaign_id,
                        account_id=account_id,
                        label=f"day{day}_on_demand",
                        co_research_by_url=co_research_by_url,
                    )
                except Exception as _enr_e:
                    logger.warning(
                        f"[day_gen:{campaign_id}] day {day} spot-enrichment failed (continuing): {_enr_e}"
                    )

        # Generate messages for this day (idempotent — skips already-generated enrollments)
        logger.info(f"[day_gen:{campaign_id}] generating messages for day {day}")
        await generate_messages_for_campaign(campaign_id, account_id, send_day=day)

    except Exception as e:
        logger.warning(f"[day_gen:{campaign_id}] day {day} generation failed: {e}")
