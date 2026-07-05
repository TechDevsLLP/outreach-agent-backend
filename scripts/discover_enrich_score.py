"""
Discovery + enrichment + scoring script (NO message generation).

Full flow:
  Account bootstrap → company profile (ICP) → synthetic campaign →
  run_fast_discovery (skip_message_gen=True) → poll enrichment →
  verify companies → export

Outputs (backend/scripts/results/des_<ts>/):
  prospects.csv        — contact + ai_score + intel + pitch + posts_captured
  companies.csv        — enrolled companies with distinct domains
  cost_report.csv/.md  — Apify (real $) + Gemini costs (no message-gen line)
  run_summary.json     — counts, timings, target-hit status

Usage:
    cd backend
    python3 scripts/discover_enrich_score.py
"""

import asyncio
import csv
import json
import logging
import os
import sys
import time
from datetime import datetime
from pathlib import Path

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from bson import ObjectId

import database
from config import get_settings

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("discover_enrich_score")
settings = get_settings()

# ─── CONSTANTS ─────────────────────────────────────────────────────────────────

EMAIL = "prithvi@techdevs.in"
PASSWORD = "TechDevs2025!"
SENDER_NAME = "Prithvi Jadwani"
SENDER_ROLE = "Co-founder & CEO"

TARGET_COMPANIES = 25
MAX_DISCOVERY_ITERATIONS = 3
ENRICHMENT_POLL_TIMEOUT_S = 3600   # 60 min max
ENRICHMENT_POLL_INTERVAL_S = 15

RESULTS_DIR = Path(__file__).parent / "results"

TIER1_INDUSTRIES = [
    "Vape / smoke / tobacco retail",
    "Cannabis dispensaries",
    "CBD / hemp / Delta-8 retail",
    "Independent and regional pharmacy chains",
    "Firearms / gun stores and shooting ranges",
    "Liquor / package stores",
    "Retail",
    "Consumer Goods",
    "Wellness and Fitness Services",
    "Health, Wellness and Fitness",
    "Pharmaceuticals",
    "Hospitals and Health Care",
    "Sporting Goods",
    "Tobacco",
]

DECISION_MAKER_TITLES = [
    "Owner",
    "Founder",
    "Co-founder",
    "CEO",
    "President",
    "Chief Executive Officer",
    "VP Operations",
    "Vice President Operations",
    "Director of Operations",
    "Multi-unit franchisee",
    "Franchise owner",
    "Regional Director",
    "Managing Partner",
    "Head of Locations",
    "Director of Retail",
    "MSO operator",
]

COMPANY_PROFILE_DOC = {
    "company_name": "TechDevs",
    "company_url": "https://techdevs.in",
    "sender_name": SENDER_NAME,
    "sender_role": SENDER_ROLE,
    "sender_linkedin_url": "https://www.linkedin.com/in/prithvi-jadwani/",
    "services": [
        "Google Maps & Apple Maps optimization — rank #1 in every neighbourhood",
        "GMB (Google Business Profile) management at scale — $250/outlet/month",
        "AI SEO upsell — +$250/outlet/month",
        "One-time setup — $250/outlet",
        "Reputation management and review generation across all locations",
        "Centralized multi-outlet Maps dashboard",
    ],
    "description": (
        "We make multi-outlet brands the #1 result on Google Maps and Apple Maps "
        "in every neighbourhood they operate — driving footfall, calls, and bookings "
        "to physical stores. GMB/Local management: $250/outlet/month. "
        "Setup: $250/outlet (one-time). AI SEO upsell: +$250/outlet/month. "
        "Minimum term: 6–12 months."
    ),
    "icp_description": (
        "Multi-outlet US brands (10+ locations) in ad-restricted verticals — vape/tobacco, "
        "cannabis dispensaries, CBD/hemp, pharmacies, firearms, liquor — where Google Maps "
        "is the primary discovery channel because paid search is restricted or throttled. "
        "The ideal single buyer controls many locations: a franchisor, large multi-unit "
        "franchisee, regional chain, PE-backed roll-up, or cannabis MSO. Footfall, calls, "
        "or in-store bookings are the core revenue event. Disqualify: single-location, "
        "online-only, under 10 outlets. Our proof point: Xhale City vape chain, ranked #1 "
        "on Maps across all store neighbourhoods."
    ),
    "pain_points": [
        "Cannot run Google Ads due to platform restrictions — Maps IS the primary discovery channel",
        "Losing walk-in customers to competitors who rank higher on Google Maps",
        "No centralized GMB/Maps management across 10–500 outlets",
        "Inconsistent reviews across locations hurt local pack ranking",
        "AI search (SGE) is pulling organic visibility away from un-optimised listings",
        "Spending on local SEO agencies that don't understand multi-outlet at scale",
    ],
    "differentiators": [
        "Proven in the vape vertical — Xhale City case study (ranked #1 in every neighbourhood)",
        "Per-outlet pricing scales with their footprint (10 to 500+ locations)",
        "Google Maps + Apple Maps + AI SEO in one bundle",
        "One engagement for the entire network — single decision-maker sign-off",
        "We understand ad-restricted verticals and the Maps-first discovery dynamic",
    ],
    "case_studies": [
        {
            "client": "Xhale City",
            "outcome": "Ranked #1 on Google Maps and Apple Maps across all store neighbourhoods",
            "metric": "Significant increase in walk-in traffic, calls, and bookings from Maps",
            "industry": "Vape / tobacco retail",
        }
    ],
    "target_industries": TIER1_INDUSTRIES,
    "target_job_titles": DECISION_MAKER_TITLES,
    "target_seniority": ["Founder", "C-Level", "VP", "Director", "Owner"],
    "target_geographies": ["United States"],
    "target_company_sizes": ["51-200", "201-1000", "1000+"],
    "primary_cta": (
        "Book a free 20-min Maps audit — we show exactly where you're losing footfall "
        "to competitors who outrank you on Google Maps"
    ),
    "outreach_strategy": "email_first",
    "aggression_preset": "aggressive",
    "onboarding_stage": 3,
}


# ─── PHASE A: Bootstrap ────────────────────────────────────────────────────────

async def bootstrap_account() -> tuple[str, str]:
    """Find or create account for prithvi@techdevs.in. Returns (account_id_str, user_id_str)."""
    user = await database.users_collection.find_one({"email": EMAIL})
    if user:
        user_id = str(user["_id"])
        account_id = str(user.get("current_account_id") or "")
        if not account_id:
            member = await database.account_members_collection.find_one(
                {"user_id": ObjectId(user_id)}
            )
            account_id = str(member["account_id"]) if member else ""
        logger.info("Found existing user=%s account=%s", user_id, account_id)
    else:
        from services.user_provisioning import create_user_with_account
        user_oid, account_oid = await create_user_with_account(
            EMAIL, PASSWORD, SENDER_NAME, company_name="TechDevs"
        )
        user_id = str(user_oid)
        account_id = str(account_oid)
        logger.info("Created user=%s account=%s", user_id, account_id)

    return account_id, user_id


async def reset_account_state(account_id: str) -> None:
    """Delete per-account state only. Global prospects/companies pool untouched."""
    account_oid = ObjectId(account_id)
    account_id_str = str(account_id)

    campaigns_cursor = database.campaigns_collection.find(
        {"account_id": {"$in": [account_oid, account_id_str]}},
        {"_id": 1},
    )
    campaign_oids = [doc["_id"] async for doc in campaigns_cursor]
    campaign_strs = [str(oid) for oid in campaign_oids]

    r1 = await database.campaigns_collection.delete_many(
        {"account_id": {"$in": [account_oid, account_id_str]}}
    )
    r2 = await database.campaign_enrollments_collection.delete_many(
        {"account_id": {"$in": [account_oid, account_id_str]}}
    )
    r3 = await database.prospect_state_collection.delete_many(
        {"account_id": account_id_str}
    )
    r4 = await database.onboarding_sessions_collection.delete_many(
        {"account_id": account_id_str}
    )
    r5 = await database.onboarding_scrape_jobs_collection.delete_many(
        {"account_id": account_id_str}
    )
    r6_count = 0
    if campaign_oids:
        r6 = await database.sourced_companies_collection.delete_many(
            {"campaign_id": {"$in": campaign_oids + campaign_strs}}
        )
        r6_count = r6.deleted_count

    logger.info(
        "Reset complete — campaigns=%d enrollments=%d prospect_state=%d "
        "onboarding=%d sourced_companies=%d",
        r1.deleted_count, r2.deleted_count, r3.deleted_count,
        r4.deleted_count + r5.deleted_count, r6_count,
    )


# ─── PHASE B: Company profile + ICP ───────────────────────────────────────────

async def upsert_company_profile(account_id: str, user_id: str) -> str:
    """Upsert company profile with Maps SEO ICP, then canonicalize. Returns profile _id str."""
    now = datetime.utcnow()
    profile = {
        **COMPANY_PROFILE_DOC,
        "account_id": account_id,
        "user_id": user_id,
        "updated_at": now,
    }
    await database.company_profiles_collection.update_one(
        {"account_id": account_id},
        {"$set": profile, "$setOnInsert": {"created_at": now, "_id": ObjectId()}},
        upsert=True,
    )
    saved = await database.company_profiles_collection.find_one({"account_id": account_id})
    profile_id = str(saved["_id"])
    logger.info("Company profile saved: %s", profile_id)

    from services.icp_canonicalizer import canonicalize_icp_and_persist
    icp_fields = {k: profile[k] for k in (
        "target_industries", "target_job_titles", "target_seniority",
        "target_geographies", "target_company_sizes",
    ) if k in profile}
    canonical = await canonicalize_icp_and_persist(
        icp_fields, profile_id, database.company_profiles_collection
    )
    logger.info(
        "ICP canonicalized: industry_ids=%d country_codes=%s seniorities=%s employee_bands=%s",
        len(canonical.get("industry_ids") or []),
        canonical.get("country_codes"),
        canonical.get("seniorities"),
        canonical.get("employee_bands"),
    )
    return profile_id


# ─── PHASE C: Campaign ────────────────────────────────────────────────────────

async def create_campaign(account_id: str, user_id: str, profile: dict) -> str:
    """Build and insert the campaign doc. Returns campaign_id str."""
    from services.onboarding_prospect_service import build_icp_prompt_from_profile
    from services.icp_canonicalizer import canonicalize_icp_and_persist

    account_oid = ObjectId(account_id)
    icp_prompt = build_icp_prompt_from_profile(profile)
    now = datetime.utcnow()

    campaign_oid = ObjectId()
    doc = {
        "_id": campaign_oid,
        "account_id": account_oid,
        "created_by": user_id,
        "name": "DES — Tier-1 Ad-Restricted Multi-Outlet — Maps SEO",
        "type": "smart",
        "is_smart_campaign": True,
        "status": "draft",
        "discovery_status": "pending",
        "discovery_mode": "curated",
        "message_tone": "professional",
        "cta_type": "reply",
        "max_prospects_per_company": 2,
        # 25 companies × 2 prospects = 50 enrolled (with 1.3× buffer)
        "prospect_count_target": 65,
        "daily_caps": {"linkedin_connection": 20, "email": 20, "linkedin_inmail": 5},
        "icp_company_size_min": 1,
        "icp_company_size_max": 500000,
        "curated_icp_prompt": icp_prompt,
        "icp_industries": TIER1_INDUSTRIES,
        "icp_job_titles": DECISION_MAKER_TITLES,
        "icp_seniority_levels": profile.get("target_seniority", []),
        "icp_countries": profile.get("target_geographies", ["United States"]),
        # ── Tuning (script-only) ──────────────────────────────────────────────
        "discovery_scrape_depth": 3,
        "discovery_dropout_buffer": 1.5,
        "discovery_enrollment_cap": 2,
        "discovery_sourcing_concurrency": 4,
        "discovery_enable_company_research": True,
        # Skip message generation — enrichment + scoring only
        "discovery_skip_message_gen": True,
        # ─────────────────────────────────────────────────────────────────────
        "created_at": now,
        "updated_at": now,
    }
    await database.campaigns_collection.insert_one(doc)
    campaign_id = str(campaign_oid)
    logger.info("Campaign created: %s (discovery_skip_message_gen=True)", campaign_id)

    campaign_icp = {
        "icp_industries": TIER1_INDUSTRIES,
        "icp_job_titles": DECISION_MAKER_TITLES,
        "icp_seniority_levels": profile.get("target_seniority", []),
        "icp_countries": profile.get("target_geographies", ["United States"]),
        "target_company_sizes": profile.get("target_company_sizes", []),
    }
    await canonicalize_icp_and_persist(
        campaign_icp, campaign_id, database.campaigns_collection
    )
    logger.info("Campaign ICP canonicalized")

    return campaign_id


# ─── PHASE D: Discovery + poll ────────────────────────────────────────────────

async def run_discovery(campaign_id: str, account_id: str) -> None:
    """Run fast discovery and poll until all enrichment completes (no message-gen shortcut)."""
    from services.curated_discovery_service import run_fast_discovery

    logger.info("Calling run_fast_discovery ...")
    t0 = time.monotonic()
    await run_fast_discovery(campaign_id, account_id)
    elapsed = time.monotonic() - t0
    logger.info("run_fast_discovery returned in %.1fs — polling for enrichment ...", elapsed)

    # Poll until discovery_status=completed AND all enrollments have non-pending message_gen_status.
    # With discovery_skip_message_gen=True, the background task marks enrollments "skipped" instead
    # of generating messages. pending_gen reaches 0 once all are "skipped".
    # NOTE: do NOT short-circuit on camp_status=="awaiting_approval" — that is set before
    # enrichment runs, so exiting on it would skip waiting for post-scrape and intelligence.
    deadline = time.monotonic() + ENRICHMENT_POLL_TIMEOUT_S
    campaign_oid = ObjectId(campaign_id)
    account_oid = ObjectId(account_id)

    while time.monotonic() < deadline:
        await asyncio.sleep(ENRICHMENT_POLL_INTERVAL_S)

        camp = await database.campaigns_collection.find_one(
            {"_id": campaign_oid},
            {"status": 1, "discovery_status": 1, "message_gen_status": 1},
        )
        camp_status = (camp or {}).get("status", "")
        disc_status = (camp or {}).get("discovery_status", "")
        camp_msg_status = (camp or {}).get("message_gen_status", "")

        total = await database.campaign_enrollments_collection.count_documents({
            "campaign_id": campaign_oid,
            "account_id": {"$in": [account_oid, account_id]},
        })
        pending_gen = await database.campaign_enrollments_collection.count_documents({
            "campaign_id": campaign_oid,
            "account_id": {"$in": [account_oid, account_id]},
            "message_gen_status": {"$in": ["pending", None]},
            "status": {"$nin": ["skipped_no_channel", "pending_teammate_review"]},
        })
        logger.info(
            "Poll: campaign_status=%s disc_status=%s camp_msg_status=%s enrollments=%d pending=%d",
            camp_status, disc_status, camp_msg_status, total, pending_gen,
        )

        # Done when all enrollments have terminal message_gen_status.
        # NOTE: disc_status stays "awaiting_approval" because finalize_channel_plan resets it after
        # "completed" is written — we don't depend on it here.
        done = pending_gen == 0 and total > 0
        # Also exit if campaign-level enrichment flagged as failed (prevents timeout hang)
        failed = camp_msg_status == "failed"
        if done or failed:
            if failed:
                logger.warning("Enrichment failed at campaign level (message_gen_status=failed)")
            else:
                logger.info("Enrichment complete: %d enrolled, %d pending", total, pending_gen)
            break
    else:
        logger.warning("Enrichment poll timed out after %ds", ENRICHMENT_POLL_TIMEOUT_S)


# ─── PHASE E: Verify + mark primary ──────────────────────────────────────────

async def count_distinct_companies(campaign_id: str, account_id: str) -> int:
    campaign_oid = ObjectId(campaign_id)
    account_oid = ObjectId(account_id)
    pipeline = [
        {"$match": {
            "campaign_id": campaign_oid,
            "account_id": {"$in": [account_oid, account_id]},
            "status": {"$nin": ["skipped_no_channel"]},
        }},
        {"$lookup": {
            "from": "prospects",
            "localField": "prospect_id",
            "foreignField": "_id",
            "as": "p",
        }},
        {"$unwind": "$p"},
        {"$group": {"_id": {"$ifNull": ["$p.company_linkedin", "$p.company_name"]}}},
        {"$count": "n"},
    ]
    result = await database.campaign_enrollments_collection.aggregate(pipeline).to_list(1)
    return (result[0]["n"] if result else 0)


async def mark_is_primary(campaign_id: str, account_id: str) -> int:
    campaign_oid = ObjectId(campaign_id)
    account_oid = ObjectId(account_id)
    account_id_str = str(account_id)

    pipeline = [
        {"$match": {
            "campaign_id": campaign_oid,
            "account_id": {"$in": [account_oid, account_id]},
        }},
        {"$lookup": {
            "from": "prospects",
            "localField": "prospect_id",
            "foreignField": "_id",
            "as": "p",
        }},
        {"$unwind": "$p"},
        {"$lookup": {
            "from": "prospect_state",
            "let": {"pid": {"$toString": "$prospect_id"}, "aid": account_id_str},
            "pipeline": [
                {"$match": {"$expr": {"$and": [
                    {"$eq": ["$prospect_id", "$$pid"]},
                    {"$eq": ["$account_id", "$$aid"]},
                ]}}},
                {"$limit": 1},
            ],
            "as": "state",
        }},
        {"$addFields": {
            "ai_score": {
                "$ifNull": [
                    {"$arrayElemAt": ["$state.ai_score", 0]},
                    "$campaign_rule_score",
                    0,
                ]
            },
            "company_key": {"$ifNull": ["$p.company_linkedin", "$p.company_name"]},
        }},
        {"$sort": {"company_key": 1, "ai_score": -1}},
        {"$project": {"_id": 1, "company_key": 1, "ai_score": 1}},
    ]
    rows = await database.campaign_enrollments_collection.aggregate(pipeline).to_list(10000)

    seen: set[str] = set()
    primary_ids = []
    all_ids = [r["_id"] for r in rows]
    for row in rows:
        ck = str(row.get("company_key") or "")
        if ck and ck not in seen:
            seen.add(ck)
            primary_ids.append(row["_id"])

    if all_ids:
        await database.campaign_enrollments_collection.update_many(
            {"_id": {"$in": all_ids}}, {"$set": {"is_primary": False}}
        )
    if primary_ids:
        await database.campaign_enrollments_collection.update_many(
            {"_id": {"$in": primary_ids}}, {"$set": {"is_primary": True}}
        )
    logger.info(
        "Marked %d primary enrollments across %d companies (%d total)",
        len(primary_ids), len(seen), len(all_ids),
    )
    return len(seen)


# ─── PHASE F: Cost report ─────────────────────────────────────────────────────

async def build_cost_report(campaign_id: str, account_id: str) -> dict:
    campaign_id_str = str(campaign_id)
    account_id_str = str(account_id)
    account_oid = ObjectId(account_id)

    apify_rows = await database.apify_usage_collection.find(
        {"campaign_id": {"$in": [campaign_id_str, campaign_id]}}
    ).to_list(1000)

    or_pipeline = [
        {"$match": {"$or": [
            {"campaign_id": {"$in": [campaign_id_str]}},
            {"account_id": {"$in": [account_id_str, str(account_oid)]}},
        ]}},
        {"$sort": {"requested_at": 1}},
    ]
    or_rows = await database.openrouter_usage_collection.aggregate(or_pipeline).to_list(5000)

    line_items = []
    for r in apify_rows:
        line_items.append({
            "source": "Apify",
            "model_or_actor": r.get("actor_id", "unknown"),
            "feature": "scraping",
            "prompt_tokens": r.get("items_returned", 0),
            "completion_tokens": 0,
            "cost_usd": r.get("cost_usd", 0.0),
        })
    for r in or_rows:
        line_items.append({
            "source": "Gemini/OpenRouter",
            "model_or_actor": r.get("model", "unknown"),
            "feature": r.get("feature", "other"),
            "prompt_tokens": r.get("prompt_tokens", 0),
            "completion_tokens": r.get("completion_tokens", 0),
            "cost_usd": r.get("cost_usd", 0.0),
        })

    total = sum(r["cost_usd"] for r in line_items)
    apify_total = sum(r["cost_usd"] for r in line_items if r["source"] == "Apify")
    gemini_sourcing = sum(r["cost_usd"] for r in line_items if r.get("feature") == "company_sourcing")
    gemini_embedding = sum(r["cost_usd"] for r in line_items if r.get("feature") == "embedding")
    intel_usd = sum(r["cost_usd"] for r in line_items if r.get("feature") == "prospect_intelligence")
    pitch_usd = sum(r["cost_usd"] for r in line_items if r.get("feature") == "prospect_pitch")
    msg_gen_usd = sum(r["cost_usd"] for r in line_items if r.get("feature") == "message_generation")
    research_usd = sum(
        r["cost_usd"] for r in line_items
        if r.get("model_or_actor", "").startswith("perplexity/")
    )
    llm_total = sum(r["cost_usd"] for r in line_items if r["source"] == "Gemini/OpenRouter")

    return {
        "line_items": line_items,
        "summary": {
            "apify_total_usd": round(apify_total, 4),
            "gemini_sourcing_usd": round(gemini_sourcing, 4),
            "gemini_embedding_usd": round(gemini_embedding, 4),
            "intel_usd": round(intel_usd, 4),
            "pitch_usd": round(pitch_usd, 4),
            "message_generation_usd": round(msg_gen_usd, 4),
            "company_research_usd": round(research_usd, 4),
            "llm_total_usd": round(llm_total, 4),
            "grand_total_usd": round(total, 4),
        },
    }


# ─── PHASE G: Export ──────────────────────────────────────────────────────────

def _str_list(v) -> str:
    if not v:
        return ""
    if isinstance(v, list):
        return "; ".join(str(x) for x in v if x)
    return str(v)


async def export_all(campaign_id: str, account_id: str, run_meta: dict, out_dir: Path) -> None:
    out_dir.mkdir(parents=True, exist_ok=True)
    account_id_str = str(account_id)
    account_oid = ObjectId(account_id)
    campaign_oid = ObjectId(campaign_id)

    pipeline = [
        {"$match": {
            "campaign_id": campaign_oid,
            "account_id": {"$in": [account_oid, account_id]},
        }},
        {"$lookup": {
            "from": "prospects",
            "localField": "prospect_id",
            "foreignField": "_id",
            "as": "p",
        }},
        {"$unwind": "$p"},
        {"$lookup": {
            "from": "prospect_state",
            "let": {"pid": {"$toString": "$prospect_id"}, "aid": account_id_str},
            "pipeline": [
                {"$match": {"$expr": {"$and": [
                    {"$eq": ["$prospect_id", "$$pid"]},
                    {"$eq": ["$account_id", "$$aid"]},
                ]}}},
                {"$limit": 1},
            ],
            "as": "state",
        }},
        {"$addFields": {"state": {"$arrayElemAt": ["$state", 0]}}},
        {"$lookup": {
            "from": "companies",
            "localField": "p.company_id",
            "foreignField": "_id",
            "as": "co",
        }},
        {"$addFields": {"co": {"$arrayElemAt": ["$co", 0]}}},
        {"$sort": {"is_primary": -1, "state.ai_score": -1}},
    ]
    rows = await database.campaign_enrollments_collection.aggregate(pipeline).to_list(10000)
    logger.info("Export: %d enrollment rows", len(rows))

    # ── prospects.csv (no message columns)
    prospect_cols = [
        "is_primary", "enrollment_status", "message_gen_status", "ai_score", "priority_tier",
        "full_name", "first_name", "last_name", "email", "personal_email",
        "linkedin", "mobile_number",
        "job_title", "headline", "seniority", "function",
        "city", "region", "country", "country_code",
        "company_name", "company_domain", "industry",
        "company_industry_group", "company_employee_band", "company_size",
        "smart_campaign_channel", "smart_campaign_send_day", "enrolled_at",
        "posts_captured",
        "writing_voice", "top_topics", "pain_signals", "best_hook",
        "pitch_angle", "why_they_need_us",
        "competitors", "dont_pitch", "engagement_style",
    ]
    with open(out_dir / "prospects.csv", "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=prospect_cols, extrasaction="ignore")
        w.writeheader()
        for row in rows:
            p = row.get("p") or {}
            st = row.get("state") or {}
            co = row.get("co") or {}

            base_intel = p.get("prospect_intelligence_base") or {}
            pitch = st.get("pitch") or {}
            intel = {**base_intel, **pitch}

            loc = p.get("location") or {}

            # posts_captured: True if the prospect has any intel derived from posts
            posts_captured = bool(
                intel.get("top_topics") or intel.get("writing_voice") or intel.get("best_hook")
            )

            w.writerow({
                "is_primary": row.get("is_primary", False),
                "enrollment_status": row.get("status", ""),
                "message_gen_status": row.get("message_gen_status", ""),
                "ai_score": (
                    st.get("ai_score")
                    or p.get("ai_prospect_score")
                    or row.get("campaign_rule_score")
                    or ""
                ),
                "priority_tier": st.get("priority_tier") or p.get("priority_tier") or "",
                "full_name": (
                    p.get("full_name")
                    or f"{p.get('first_name','').strip()} {p.get('last_name','').strip()}".strip()
                ),
                "first_name": p.get("first_name", ""),
                "last_name": p.get("last_name", ""),
                "email": p.get("email", ""),
                "personal_email": p.get("personal_email", ""),
                "linkedin": p.get("linkedin", ""),
                "mobile_number": p.get("mobile_number", ""),
                "job_title": p.get("job_title", ""),
                "headline": p.get("headline", ""),
                "seniority": p.get("seniority", ""),
                "function": p.get("function", ""),
                "city": loc.get("city", "") if isinstance(loc, dict) else "",
                "region": loc.get("region", "") if isinstance(loc, dict) else "",
                "country": loc.get("country", "") if isinstance(loc, dict) else "",
                "country_code": loc.get("country_code", "") if isinstance(loc, dict) else "",
                "company_name": p.get("company_name") or co.get("name", ""),
                "company_domain": p.get("company_domain") or co.get("domain", ""),
                "industry": p.get("industry", ""),
                "company_industry_group": p.get("company_industry_group", ""),
                "company_employee_band": p.get("company_employee_band") or co.get("employee_band", ""),
                "company_size": p.get("company_size") or co.get("employee_count", ""),
                "smart_campaign_channel": row.get("smart_campaign_channel", ""),
                "smart_campaign_send_day": row.get("smart_campaign_send_day", ""),
                "enrolled_at": str(row.get("enrolled_at", "")),
                "posts_captured": posts_captured,
                "writing_voice": intel.get("writing_voice", ""),
                "top_topics": _str_list(intel.get("top_topics")),
                "pain_signals": _str_list(intel.get("pain_signals")),
                "best_hook": intel.get("best_hook", ""),
                "pitch_angle": intel.get("pitch_angle", ""),
                "why_they_need_us": intel.get("why_they_need_us", ""),
                "competitors": _str_list(intel.get("competitors")),
                "dont_pitch": _str_list(intel.get("dont_pitch")),
                "engagement_style": intel.get("engagement_style", ""),
            })
    logger.info("Wrote prospects.csv: %d rows", len(rows))

    # ── companies.csv
    co_map: dict[str, dict] = {}
    for row in rows:
        p = row.get("p") or {}
        co = row.get("co") or {}
        key = str(p.get("company_linkedin") or p.get("company_name") or "")
        if not key:
            continue
        if key not in co_map:
            ind = co.get("industry") or {}
            ind_label = ind.get("label", "") if isinstance(ind, dict) else str(ind or "")
            co_loc = co.get("location") or {}
            co_map[key] = {
                "company_name": p.get("company_name") or co.get("name", ""),
                "domain": p.get("company_domain") or co.get("domain", ""),
                "industry": p.get("industry", ""),
                "industry_label": ind_label or p.get("company_industry_group", ""),
                "employee_band": p.get("company_employee_band") or co.get("employee_band", ""),
                "employee_count": p.get("company_size") or co.get("employee_count", ""),
                "city": co_loc.get("city", "") if isinstance(co_loc, dict) else "",
                "country": co_loc.get("country", "") if isinstance(co_loc, dict) else "",
                "linkedin_url": p.get("company_linkedin") or co.get("linkedin_url", ""),
                "founded_year": co.get("founded_year", ""),
                "prospects_enrolled": 0,
            }
        co_map[key]["prospects_enrolled"] += 1

    co_cols = [
        "company_name", "domain", "industry", "industry_label",
        "employee_band", "employee_count",
        "city", "country", "linkedin_url", "founded_year", "prospects_enrolled",
    ]
    with open(out_dir / "companies.csv", "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=co_cols, extrasaction="ignore")
        w.writeheader()
        for co_row in sorted(co_map.values(), key=lambda x: x.get("company_name", "")):
            w.writerow(co_row)
    logger.info("Wrote companies.csv: %d companies", len(co_map))

    # ── Cost report
    cost = await build_cost_report(campaign_id, account_id)
    n_prospects = len(rows)
    n_companies = len(co_map)
    s = cost["summary"]
    cpp = round(s["grand_total_usd"] / n_prospects, 4) if n_prospects else 0
    cpc = round(s["grand_total_usd"] / n_companies, 4) if n_companies else 0

    cost_cols = ["source", "model_or_actor", "feature", "prompt_tokens", "completion_tokens", "cost_usd"]
    with open(out_dir / "cost_report.csv", "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=cost_cols, extrasaction="ignore")
        w.writeheader()
        for li in cost["line_items"]:
            w.writerow(li)

    md_lines = [
        "# Cost Report — Discover+Enrich+Score (DES)",
        "",
        f"Generated: {datetime.utcnow().isoformat()}Z",
        f"Campaign ID: {campaign_id}",
        f"Account: {EMAIL}",
        "",
        "## Totals by Source",
        "",
        "| Source | USD |",
        "|--------|-----|",
        f"| Apify (real `usageTotalUsd`) | ${s['apify_total_usd']:.4f} |",
        f"| Gemini company sourcing | ${s['gemini_sourcing_usd']:.4f} |",
        f"| Gemini embeddings | ${s['gemini_embedding_usd']:.4f} |",
        f"| Prospect intelligence (base) | ${s['intel_usd']:.4f} |",
        f"| Prospect pitch overlay | ${s['pitch_usd']:.4f} |",
        f"| Message generation | ${s['message_generation_usd']:.4f} |",
        f"| Company research (news + competitors) | ${s['company_research_usd']:.4f} |",
        f"| All LLM (OpenRouter + Gemini via price map) | ${s['llm_total_usd']:.4f} |",
        f"| **Grand Total** | **${s['grand_total_usd']:.4f}** |",
        "",
        "## Per-Unit Costs",
        "",
        "| Metric | Value |",
        "|--------|-------|",
        f"| Prospects enrolled | {n_prospects} |",
        f"| Companies enrolled | {n_companies} |",
        f"| Cost per prospect | ${cpp:.4f} |",
        f"| Cost per company | ${cpc:.4f} |",
        "",
        "## Notes",
        "- Message generation was SKIPPED (discovery_skip_message_gen=True).",
        "- Apify `items_returned` is 0 in tracked rows (set_items not called in fast-discovery path).",
        "- LLM costs estimated from `config.openrouter_price_map` ($/1M tokens).",
        "",
    ]
    with open(out_dir / "cost_report.md", "w", encoding="utf-8") as f:
        f.write("\n".join(md_lines))
    logger.info("Wrote cost_report.md — grand total: $%.4f", s["grand_total_usd"])

    summary = {
        **run_meta,
        "n_prospects": n_prospects,
        "n_companies": n_companies,
        "target_companies": TARGET_COMPANIES,
        "target_hit": n_companies >= TARGET_COMPANIES,
        "cost": cost["summary"],
        "output_dir": str(out_dir),
    }
    with open(out_dir / "run_summary.json", "w", encoding="utf-8") as f:
        json.dump(summary, f, indent=2, default=str)
    logger.info("Wrote run_summary.json")


# ─── MAIN ─────────────────────────────────────────────────────────────────────

async def main() -> None:
    ts = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
    out_dir = RESULTS_DIR / f"des_{ts}"
    run_meta: dict = {
        "started_at": datetime.utcnow().isoformat(),
        "script": "discover_enrich_score.py",
        "email": EMAIL,
        "target_companies": TARGET_COMPANIES,
    }

    logger.info("=== Phase A: Bootstrap account ===")
    account_id, user_id = await bootstrap_account()
    run_meta.update({"account_id": account_id, "user_id": user_id})

    logger.info("=== Phase A: Reset per-account state ===")
    await reset_account_state(account_id)

    logger.info("=== Phase B: Upsert company profile + ICP ===")
    profile_id = await upsert_company_profile(account_id, user_id)
    run_meta["profile_id"] = profile_id
    profile = await database.company_profiles_collection.find_one({"account_id": account_id})

    logger.info("=== Phase C: Create campaign (skip_message_gen=True) ===")
    campaign_id = await create_campaign(account_id, user_id, profile)
    run_meta["campaign_id"] = campaign_id

    logger.info("=== Phase D: Discovery loop (target ≥%d companies) ===", TARGET_COMPANIES)
    discovery_timings = []
    for iteration in range(MAX_DISCOVERY_ITERATIONS):
        logger.info("--- Discovery iteration %d / %d ---", iteration + 1, MAX_DISCOVERY_ITERATIONS)
        t0 = time.monotonic()
        try:
            await run_discovery(campaign_id, account_id)
        except Exception as _disc_err:
            elapsed = round(time.monotonic() - t0, 1)
            logger.error("Discovery iteration %d failed after %.0fs: %s", iteration + 1, elapsed, _disc_err)
            run_meta["discovery_error"] = str(_disc_err)
            break
        elapsed = round(time.monotonic() - t0, 1)
        discovery_timings.append({"iteration": iteration + 1, "elapsed_s": elapsed})

        n_co = await count_distinct_companies(campaign_id, account_id)
        logger.info(
            "Iteration %d done (%.0fs): %d / %d companies enrolled",
            iteration + 1, elapsed, n_co, TARGET_COMPANIES,
        )
        run_meta["distinct_companies"] = n_co
        if n_co >= TARGET_COMPANIES:
            logger.info("Target reached: %d companies.", n_co)
            break
        if iteration < MAX_DISCOVERY_ITERATIONS - 1:
            logger.warning(
                "Short of target (%d / %d) — re-running discovery to fill gap ...",
                n_co, TARGET_COMPANIES,
            )

    run_meta["discovery_timings"] = discovery_timings

    logger.info("=== Phase E: Mark is_primary ===")
    n_companies = await mark_is_primary(campaign_id, account_id)
    run_meta["distinct_companies"] = n_companies

    campaign_oid = ObjectId(campaign_id)
    account_oid = ObjectId(account_id)
    total_enrolled = await database.campaign_enrollments_collection.count_documents({
        "campaign_id": campaign_oid,
        "account_id": {"$in": [account_oid, account_id]},
    })
    run_meta["total_prospects_enrolled"] = total_enrolled
    run_meta["finished_at"] = datetime.utcnow().isoformat()

    logger.info("=== Phase F-G: Export to %s ===", out_dir)
    await export_all(campaign_id, account_id, run_meta, out_dir)

    logger.info("=" * 60)
    logger.info("DONE")
    logger.info("  Account:     %s (%s)", EMAIL, account_id)
    logger.info("  Campaign:    %s", campaign_id)
    logger.info("  Companies:   %d / %d target", n_companies, TARGET_COMPANIES)
    logger.info("  Prospects:   %d enrolled", total_enrolled)
    logger.info("  Results:     %s", out_dir)
    logger.info("=" * 60)


if __name__ == "__main__":
    asyncio.run(main())
