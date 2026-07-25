"""
Onboarding prospect preview + first campaign launch service.

Bridges the 6-stage onboarding wizard (ICP/profile capture) and the curated
campaign discovery engine (Gemini sourcing + Apify employee scraping) so the
user can confirm real target prospects BEFORE their first campaign starts.

Three public entry points:
  build_icp_prompt_from_profile   — profile doc → free-text ICP string
  source_preview_prospects        — ICP → 5 companies → 5 prospects w/ email
  launch_onboarding_first_campaign — inject confirmed prospects + fire campaign
"""
import asyncio
import logging
from datetime import datetime
from math import ceil
from typing import Optional

from bson import ObjectId
from pymongo import UpdateOne

import database
from config import get_settings

logger = logging.getLogger(__name__)
settings = get_settings()

_DEFAULT_DAILY_CAPS = {
    "linkedin_connection": 20,
    "email": 20,
    "linkedin_inmail": 5,
    "linkedin_message": 20,
}


# ─────────────────────────────────────────────────────────────────────────────
# ICP prompt builder
# ─────────────────────────────────────────────────────────────────────────────

def build_icp_prompt_from_profile(profile: dict) -> str:
    """
    Convert a company_profiles document into a free-text ICP description
    suitable for company_sourcing_service.source_companies(icp_prompt=...).

    Falls back to a generic prompt if the profile is empty.
    """
    parts: list[str] = []

    company_name = profile.get("company_name") or ""
    if company_name:
        parts.append(f"We are {company_name}.")

    services = profile.get("services") or []
    if services:
        svc_str = ", ".join(services) if isinstance(services, list) else str(services)
        parts.append(f"Our services: {svc_str}")

    pain_pts = profile.get("pain_points") or []
    if pain_pts:
        pp_str = "; ".join(pain_pts) if isinstance(pain_pts, list) else str(pain_pts)
        parts.append(f"Pain points we solve: {pp_str}")

    icp_desc = (profile.get("icp_description") or "").strip()
    if icp_desc:
        parts.append(f"ICP description: {icp_desc}")

    industries = profile.get("target_industries") or []
    if industries:
        ind_str = ", ".join(industries) if isinstance(industries, list) else str(industries)
        parts.append(f"Target industries: {ind_str}")

    titles = profile.get("target_job_titles") or []
    if titles:
        t_str = ", ".join(titles) if isinstance(titles, list) else str(titles)
        parts.append(f"Target job titles: {t_str}")

    seniority = profile.get("target_seniority") or []
    if seniority:
        s_str = ", ".join(seniority) if isinstance(seniority, list) else str(seniority)
        parts.append(f"Seniority levels: {s_str}")

    geos = profile.get("target_geographies") or []
    if geos:
        g_str = ", ".join(geos) if isinstance(geos, list) else str(geos)
        parts.append(f"Geographies: {g_str}")

    sizes = profile.get("target_company_sizes") or []
    if sizes:
        sz_str = ", ".join(str(s) for s in sizes) if isinstance(sizes, list) else str(sizes)
        parts.append(f"Company sizes: {sz_str}")

    if not parts:
        return "B2B software/tech companies, decision makers (CTO/CEO/Founder), North America"

    return "\n".join(parts)


# ─────────────────────────────────────────────────────────────────────────────
# Preview sourcing
# ─────────────────────────────────────────────────────────────────────────────

async def source_preview_prospects(
    profile: dict,
    *,
    count: int = 5,
    exclude_company_names: Optional[list[str]] = None,
    override_job_titles: Optional[list[str]] = None,
    override_industries: Optional[list[str]] = None,
    account_id: Optional[str] = None,
) -> list[dict]:
    """
    Source count*2 companies, scrape 1 employee/company, find emails, return
    the best `count` prospects (email-holders ranked first).

    Returns a list of prospect-preview dicts. Each dict has:
      full_name, linkedin, email, company_name, job_title,
      company_linkedin, company_domain, company_website, industry,
      ai_prospect_score, fit_score,
      _sourced_company   <- internal; stripped before DB write
    """
    from services.company_sourcing_service import source_companies
    from services.employee_scraper_service import (
        bulk_scrape_employees_for_companies,
        transform_employee_to_prospect,
    )
    from services.email_finder_service import find_emails, EmailLookupEntry

    # Apply per-reroll overrides without mutating the caller's profile
    working_profile = dict(profile)
    if override_job_titles is not None:
        working_profile["target_job_titles"] = override_job_titles
    if override_industries is not None:
        working_profile["target_industries"] = override_industries

    icp_prompt = build_icp_prompt_from_profile(working_profile)
    logger.info("[preview] ICP prompt:\n%s", icp_prompt)

    # Scrape from 2x companies so Apify slug failures still yield `count` good prospects
    scrape_count = count * 2
    target_fetch = max(ceil(scrape_count * 2.5), scrape_count + 5)
    companies, meta = await source_companies(
        icp_prompt=icp_prompt,
        target_count=target_fetch,
        exclude_names=exclude_company_names,
        validate_urls=True,
        account_id=account_id,
    )
    logger.info("[preview] Sourced %d companies (meta=%s)", len(companies), meta)

    # Only keep companies with a verified LinkedIn company URL
    valid_companies = [c for c in companies if c.get("company_linkedin_url")][:scrape_count]

    if not valid_companies:
        logger.warning("[preview] No valid companies with LinkedIn URLs found")
        return []

    company_urls = [c["company_linkedin_url"] for c in valid_companies]
    url_to_company = {c["company_linkedin_url"]: c for c in valid_companies}

    # Scrape 1 employee per company (Short mode, no emails yet)
    raw_employees = await bulk_scrape_employees_for_companies(
        company_urls,
        max_items_per_company=1,
        account_id=account_id,
    )
    logger.info("[preview] Got %d raw employees from %d companies", len(raw_employees), len(company_urls))

    # Match employees → companies.
    # Apify "Short" mode may or may not return companyLinkedinUrl on the employee.
    # Strategy: check currentPositions[0].companyLinkedinUrl, then fallback to
    # order-based assignment (first employee → first unmatched company).
    company_to_employee: dict[str, dict] = {}

    def _norm(url: str) -> str:
        """Normalise a LinkedIn company URL for fuzzy matching."""
        return url.rstrip("/").lower().split("?")[0]

    norm_company_urls = {_norm(u): u for u in company_urls}

    for emp in raw_employees:
        # Attempt to find company URL in the employee record
        positions = emp.get("currentPositions") or []
        emp_company_li = ""
        if positions:
            emp_company_li = positions[0].get("companyLinkedinUrl") or ""
        if not emp_company_li:
            emp_company_li = emp.get("companyLinkedinUrl") or ""

        matched_url = None
        if emp_company_li:
            norm_emp = _norm(emp_company_li)
            # Try exact match first
            if norm_emp in norm_company_urls:
                matched_url = norm_company_urls[norm_emp]
            else:
                # Substring match (handles /company/slug vs /company/slug/about)
                for ncu, orig in norm_company_urls.items():
                    if ncu in norm_emp or norm_emp in ncu:
                        matched_url = orig
                        break

        if matched_url and matched_url not in company_to_employee:
            company_to_employee[matched_url] = emp
        elif not matched_url:
            # Fallback: assign to the first company not yet matched
            for url in company_urls:
                if url not in company_to_employee:
                    company_to_employee[url] = emp
                    break

    # Transform employees → prospect dicts
    prospects: list[dict] = []
    for url in company_urls:
        emp = company_to_employee.get(url)
        if not emp:
            logger.warning("[preview] No employee returned for company %s", url)
            continue
        sourced_company = url_to_company[url]
        p = transform_employee_to_prospect(emp, sourced_company)
        p["ai_prospect_score"] = 90.0
        p["fit_score"] = 0.90
        p["_sourced_company"] = sourced_company  # retained for launch injection
        prospects.append(p)

    if not prospects:
        logger.warning("[preview] No prospects after transform — raw_employees=%d", len(raw_employees))
        return []

    # Find emails for prospects that don't have one (Short mode rarely returns emails)
    email_entries = [
        EmailLookupEntry(
            first_name=p["first_name"],
            last_name=p["last_name"],
            domain=p["company_domain"],
            key=p["linkedin"],
        )
        for p in prospects
        if not p.get("email") and p.get("linkedin")
        and p.get("first_name") and p.get("last_name") and p.get("company_domain")
    ]
    if email_entries:
        try:
            email_map = await find_emails(email_entries, account_id=str(account_id) if account_id else None)
            for p in prospects:
                if not p.get("email") and p.get("linkedin"):
                    found = email_map.get(p["linkedin"])
                    if found:
                        p["email"] = found
                        logger.info("[preview] Email found for %s: %s", p.get("full_name"), found)
        except Exception as exc:
            logger.warning("[preview] Email finder failed (non-fatal): %s", exc)

    # Preview must only show prospects with email + full details
    prospects = [
        p for p in prospects
        if p.get("email") and p.get("full_name") and p.get("job_title") and p.get("company_name")
    ]

    # Rank by email presence (desc), then full name completeness, keep Gemini order otherwise
    prospects.sort(
        key=lambda p: (
            1 if p.get("email") else 0,
            1 if len((p.get("full_name") or "").split()) > 1 else 0,
        ),
        reverse=True,
    )
    prospects = prospects[:count]

    logger.info(
        "[preview] Final %d prospects: %s",
        len(prospects),
        [(p.get("full_name"), p.get("company_name"), bool(p.get("email"))) for p in prospects],
    )
    return prospects


# ─────────────────────────────────────────────────────────────────────────────
# First campaign launch (DEPRECATED — kept as stub to avoid ImportError)
# The new flow: campaign is created at Stage 3 (onboarding_scrape_service).
# At Stage 5 launch, call replan_and_launch from curated_discovery_service.
# ─────────────────────────────────────────────────────────────────────────────

async def launch_onboarding_first_campaign(
    account_id: str,
    user_id: str,
    profile: dict,
    confirmed_prospects: list[dict],
    *,
    target_total: int = 50,
    campaign_name: Optional[str] = None,
) -> str:
    """
    DEPRECATED: Campaign is now created at Stage 3 ICP lock (onboarding_scrape_service).
    At Stage 5 launch, call replan_and_launch instead.
    This stub raises NotImplementedError to surface mis-use.
    """
    raise NotImplementedError(
        "launch_onboarding_first_campaign is deprecated. "
        "Use replan_and_launch from curated_discovery_service at Stage 5 launch."
    )


async def _launch_onboarding_first_campaign_legacy(
    account_id: str,
    user_id: str,
    profile: dict,
    confirmed_prospects: list[dict],
    *,
    target_total: int = 50,
    campaign_name: Optional[str] = None,
) -> str:
    """
    DEPRECATED — kept only as reference. Not called by any code path.
    Create the user's first curated campaign.

    - Upserts the `confirmed_prospects` (already scraped) into the DB.
    - Pre-enrolls them and pins them to Day 1.
    - Generates Day-1 messages synchronously (so approve-day/1 works immediately).
    - Sets campaign status = awaiting_approval.
    - Fires a background top-up discovery for the remaining slots.

    Returns the new campaign_id (string).
    """
    from services.curated_discovery_service import _upsert_curated_prospect, _run_day1_message_gen
    from services.prospect_enrollment_service import _pre_enroll_prospects

    account_oid = ObjectId(account_id)
    user_oid = ObjectId(user_id)
    now = datetime.utcnow()

    icp_prompt = build_icp_prompt_from_profile(profile)
    name = campaign_name or f"First Outreach — {profile.get('company_name', 'Target Accounts')}"

    # ── 1. Create campaign document ──────────────────────────────────────────
    campaign_oid = ObjectId()
    campaign_doc = {
        "_id": campaign_oid,
        "account_id": account_oid,
        "created_by": user_oid,
        "name": name,
        "type": "email",
        "description": "Created from onboarding prospect preview",
        "status": "draft",
        "daily_caps": dict(_DEFAULT_DAILY_CAPS),
        "daily_email_limit": _DEFAULT_DAILY_CAPS["email"],
        "daily_linkedin_limit": (
            _DEFAULT_DAILY_CAPS["linkedin_connection"] + _DEFAULT_DAILY_CAPS["linkedin_inmail"]
        ),
        "timezone": "America/New_York",
        "send_days": ["monday", "tuesday", "wednesday", "thursday", "friday"],
        "send_hour_start": 9,
        "send_hour_end": 17,
        # Smart campaign core
        "is_smart_campaign": True,
        "discovery_mode": "curated",
        "curated_icp_prompt": icp_prompt,
        "curated_company_count_target": target_total,
        "prospect_count_target": target_total,
        # ICP fields (for engine + plan_channel_assignments fallback)
        "icp_industries": profile.get("target_industries") or [],
        "icp_job_titles": profile.get("target_job_titles") or [],
        "icp_seniority_levels": profile.get("target_seniority") or [],
        "icp_countries": profile.get("target_geographies") or [],
        "icp_keywords": [],
        "icp_company_size_min": None,
        "icp_company_size_max": None,
        "icp_functional_departments": [],
        "max_prospects_per_company": 3,
        # Messaging
        "message_tone": "professional",
        "value_proposition": (
            "; ".join(profile.get("value_propositions") or [])
            if isinstance(profile.get("value_propositions"), list)
            else profile.get("value_propositions") or None
        ),
        "pain_point": (
            "; ".join(profile.get("pain_points") or [])
            if isinstance(profile.get("pain_points"), list)
            else profile.get("pain_points") or None
        ),
        "cta_type": "book_call",
        "cta_url": None,
        # Sending accounts (not connected yet — user sets up after onboarding)
        "email_account_id": None,
        "linkedin_account_id": None,
        # Discovery state
        "curated_companies_sourced": 0,
        "curated_companies_approved": 0,
        "curated_companies_scraped": 0,
        "discovery_status": "idle",
        "discovery_started_at": None,
        "discovery_completed_at": None,
        "discovery_error": None,
        "discovery_prospects_found": len(confirmed_prospects),
        "discovery_prospects_enrolled": 0,
        # Message gen state
        "message_gen_status": "idle",
        "message_gen_started_at": None,
        "message_gen_completed_at": None,
        "message_gen_prospects_done": 0,
        # Approval state
        "approval_status": "pending",
        "approved_send_days": [],
        "launched_at": None,
        "launch_day1_date": None,
        # Counters
        "total_enrolled": 0,
        "active_count": 0,
        "completed_count": 0,
        "replied_count": 0,
        "bounced_count": 0,
        "opted_out_count": 0,
        "meetings_booked": 0,
        "emails_sent": 0,
        "emails_delivered": 0,
        "emails_opened": 0,
        "emails_clicked": 0,
        "emails_replied": 0,
        "emails_bounced": 0,
        "linkedin_connections_sent": 0,
        "linkedin_connections_accepted": 0,
        "linkedin_inmails_sent": 0,
        "linkedin_replies": 0,
        # Timestamps
        "created_at": now,
        "updated_at": now,
        "started_at": None,
        "completed_at": None,
    }

    await database.campaigns_collection.insert_one(campaign_doc)
    campaign_id = str(campaign_oid)
    logger.info("[onboarding_launch] Created campaign %s", campaign_id)

    # ── 2. Upsert confirmed prospects ────────────────────────────────────────
    confirmed_company_names: list[str] = []
    new_prospect_oids: list[ObjectId] = []

    for raw_p in confirmed_prospects:
        # Strip the internal helper key before upserting
        p = {k: v for k, v in raw_p.items() if k != "_sourced_company"}
        p["ai_prospect_score"] = 90.0
        p["fit_score"] = 0.90
        oid = await _upsert_curated_prospect(p, campaign_oid, account_id)
        if oid:
            new_prospect_oids.append(oid)
        co = raw_p.get("company_name") or ""
        if co:
            confirmed_company_names.append(co)

    logger.info("[onboarding_launch] Upserted %d prospects", len(new_prospect_oids))

    if not new_prospect_oids:
        logger.error("[onboarding_launch] No prospects upserted — aborting")
        await database.campaigns_collection.update_one(
            {"_id": campaign_oid},
            {"$set": {"discovery_status": "failed", "discovery_error": "No prospects had linkedin or email"}},
        )
        return campaign_id

    # ── 3. Fetch full docs + pre-enroll ─────────────────────────────────────
    confirmed_full = await database.prospects_collection.find(
        {"_id": {"$in": new_prospect_oids}}
    ).to_list(length=None)

    if confirmed_full:
        await _pre_enroll_prospects(campaign_doc, confirmed_full)

    # ── 4. Pin confirmed enrollments to Day 1 ────────────────────────────────
    # approve_day(campaign, 1) queries for smart_campaign_send_day == 1.
    # _pre_enroll_prospects leaves it as None, so we set it explicitly.
    enrs = await database.campaign_enrollments_collection.find(
        {"campaign_id": campaign_oid, "prospect_id": {"$in": new_prospect_oids}},
    ).to_list(length=None)

    if enrs:
        ops = [
            UpdateOne(
                {"_id": enr["_id"]},
                {"$set": {
                    "smart_campaign_send_day": 1,
                    "smart_campaign_channel": "email",
                    # Keep status="scoring" — approve_day flips to "active"
                    "message_gen_status": "pending",
                }},
            )
            for enr in enrs
        ]
        await database.campaign_enrollments_collection.bulk_write(ops, ordered=False)
        logger.info("[onboarding_launch] Pinned %d enrollments to Day 1", len(enrs))

    # ── 5. Generate Day-1 messages (synchronous — must be done before approval) ─
    # _generate_day1_messages processes all enrollments with message_gen_status=pending
    # At this point that is exactly our 5 confirmed prospects.
    await _run_day1_message_gen(campaign_id, account_id)
    logger.info("[onboarding_launch] Day-1 message generation complete")

    # ── 6. Flip campaign to awaiting_approval ────────────────────────────────
    await database.campaigns_collection.update_one(
        {"_id": campaign_oid},
        {"$set": {
            "discovery_status": "completed",
            "discovery_completed_at": now,
            "discovery_prospects_enrolled": len(confirmed_full),
            "status": "awaiting_approval",
            "updated_at": now,
        }},
    )

    # ── 7. Background top-up discovery ──────────────────────────────────────
    # Finds target_total - 5 more prospects (excludes confirmed companies).
    # run_fast_discovery is safe to fire on an already-populated campaign:
    #   - It deletes old sourced_companies docs (we have none since we skipped that step)
    #   - _pre_enroll_prospects skips already-enrolled prospect IDs
    #   - It will again set status=awaiting_approval at end (idempotent)
    asyncio.create_task(
        _run_topup_discovery(campaign_id, account_id, confirmed_company_names)
    )

    logger.info(
        "[onboarding_launch] Campaign %s ready. %d Day-1 prospects enrolled. Top-up running.",
        campaign_id, len(confirmed_full),
    )
    return campaign_id


async def _run_topup_discovery(
    campaign_id: str,
    account_id: str,
    exclude_company_names: list[str],
) -> None:
    """
    DEPRECATED — no longer called. Kept to prevent reference errors in
    any existing asyncio tasks that may still be running.
    Background task: runs the standard curated discovery pipeline to top up
    the campaign beyond the 5 confirmed prospects.
    exclude_company_names are patched into the campaign's curated_icp_prompt
    temporarily so source_companies skips them.
    """
    try:
        # Patch: prepend an EXCLUDE block to the icp_prompt so Gemini avoids
        # re-sourcing the same companies. We update the DB, run, then restore.
        # This is safe because the main thread has already returned campaign_id.
        if exclude_company_names:
            exclude_block = "\nEXCLUDE these companies (already confirmed): " + ", ".join(exclude_company_names)
            campaign = await database.campaigns_collection.find_one({"_id": ObjectId(campaign_id)})
            original_prompt = (campaign or {}).get("curated_icp_prompt") or ""
            await database.campaigns_collection.update_one(
                {"_id": ObjectId(campaign_id)},
                {"$set": {"curated_icp_prompt": original_prompt + exclude_block}},
            )

        from services.curated_discovery_service import run_fast_discovery
        await run_fast_discovery(campaign_id, account_id)

    except Exception as exc:
        logger.warning("[onboarding_topup] Top-up discovery failed (non-fatal): %s", exc)
    finally:
        # Restore original prompt (without the EXCLUDE block) so future re-runs are clean
        if exclude_company_names:
            try:
                campaign = await database.campaigns_collection.find_one({"_id": ObjectId(campaign_id)})
                if campaign:
                    current_prompt = campaign.get("curated_icp_prompt") or ""
                    clean = current_prompt.split("\nEXCLUDE these companies")[0]
                    await database.campaigns_collection.update_one(
                        {"_id": ObjectId(campaign_id)},
                        {"$set": {"curated_icp_prompt": clean}},
                    )
            except Exception:
                pass
