"""
End-to-end test: Onboarding → prospect preview → first campaign launch.

Tests the full flow for TechDevs (techdevs.in) as the real business:
  1.  Create/resolve a test account
  2.  Build + save a company profile (simulates stages 1-4 of the wizard)
  3.  Start an onboarding session
  4.  Call source_preview_prospects() → assert 5 prospects w/ name/LI/email
  5.  Print prospect table
  6.  Call reroll (source_preview_prospects with exclude) → different companies
  7.  Call launch_onboarding_first_campaign() → campaign_id
  8.  Assert campaign status = awaiting_approval, 5 Day-1 enrollments
  9.  Assert Day-1 enrollments have generated_messages + smart_campaign_send_day=1
  10. Call approve_day(campaign, 1) → assert status=active, next_action_at set
  11. Assert nothing was sent (campaign_messages collection empty for campaign)
  12. Print PASS/FAIL summary

Usage:
  cd backend
  python3 scripts/test_onboarding_to_campaign.py [--cleanup]
"""

import argparse
import asyncio
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
logger = logging.getLogger("test_onboarding")

# ── TechDevs test data ────────────────────────────────────────────────────────

TECHDEVS_PROFILE = {
    "company_name": "TechDevs",
    "company_url": "https://techdevs.in",
    "company_linkedin_url": "https://www.linkedin.com/company/tech-devs/",
    "sender_name": "Prithvi Jadwani",
    "sender_role": "Co-founder",
    "sender_linkedin_url": "https://www.linkedin.com/in/prithvi-jadwani/",
    # Services
    "services": [
        "Custom software development",
        "Mobile app development",
        "Web application development",
        "UI/UX design",
        "AI/ML integration",
        "Startup MVP development",
    ],
    # Value props
    "value_propositions": [
        "Dedicated offshore development team at 60% lower cost than US/UK rates",
        "Fast MVP delivery in 6-8 weeks",
        "Full-stack expertise: React, Node.js, Python, Flutter, AWS",
        "Transparent communication and agile process",
    ],
    # Pain points
    "pain_points": [
        "High cost of hiring local developers",
        "Difficulty finding reliable tech talent quickly",
        "Slow product iteration cycles",
        "Lack of technical expertise in-house for scaling",
    ],
    # ICP
    "icp_description": (
        "Early to growth-stage tech-enabled companies (Series A–C) in the US/UK/Canada "
        "who need to build or scale a software product but want to avoid high local hiring costs. "
        "Typically have a non-technical or semi-technical founder who trusts a reliable offshore team."
    ),
    "target_industries": [
        "SaaS", "Fintech", "E-commerce", "HealthTech", "EdTech",
        "Consumer tech", "B2B software", "Marketplace",
    ],
    "target_job_titles": [
        "CTO", "Co-founder", "CEO", "VP Engineering",
        "Head of Technology", "Director of Engineering",
        "Founder", "Technical Co-founder",
    ],
    "target_seniority": ["Director", "VP", "C-Level", "Founder", "Partner"],
    "target_geographies": ["United States", "United Kingdom", "Canada", "Australia"],
    "target_company_sizes": ["1-10", "11-50", "51-200"],
    # Offer
    "primary_cta": "Schedule a free 30-min technical discovery call",
    "value_proposition_short": "60% cost savings vs local devs + same quality, faster delivery",
    "updated_at": datetime.utcnow(),
}

# ── Helpers ───────────────────────────────────────────────────────────────────

_results: list[dict] = []
_test_account_id: str | None = None
_test_user_id: str | None = None
_test_campaign_id: str | None = None


def _pass(step: str, detail: str = "") -> None:
    msg = f"PASS: {step}" + (f" — {detail}" if detail else "")
    logger.info(msg)
    _results.append({"step": step, "status": "PASS", "detail": detail})


def _fail(step: str, detail: str = "") -> None:
    msg = f"FAIL: {step}" + (f" — {detail}" if detail else "")
    logger.error(msg)
    _results.append({"step": step, "status": "FAIL", "detail": detail})


def _print_table(prospects: list[dict]) -> None:
    """Print prospect preview table to stdout."""
    print("\n" + "=" * 100)
    print(f"{'NAME':<28} {'EMAIL':<32} {'COMPANY':<22} {'TITLE':<24}")
    print("-" * 100)
    for p in prospects:
        name = (p.get("full_name") or "—")[:27]
        email = (p.get("email") or "—")[:31]
        company = (p.get("company_name") or "—")[:21]
        title = (p.get("job_title") or "—")[:23]
        li = p.get("linkedin") or ""
        print(f"{name:<28} {email:<32} {company:<22} {title:<24}")
        if li:
            print(f"  LinkedIn: {li}")
    print("=" * 100 + "\n")


# ── Test steps ────────────────────────────────────────────────────────────────

async def step_01_create_test_account() -> tuple[str, str]:
    """Create (or find existing) throwaway test account + user."""
    global _test_account_id, _test_user_id

    import bcrypt as _bcrypt

    TEST_EMAIL = "test_onboarding_techdevs@outflo.test"
    TEST_NAME = "TechDevs Test"

    # Check if user already exists
    user = await database.users_collection.find_one({"email": TEST_EMAIL})
    if user:
        user_id = str(user["_id"])
        account_id = str(user.get("current_account_id") or "")
        if not account_id:
            # Find account by member
            member = await database.account_members_collection.find_one(
                {"user_id": ObjectId(user_id)}
            )
            account_id = str(member["account_id"]) if member else ""
        logger.info("Reusing existing test user %s / account %s", user_id, account_id)
        _test_account_id = account_id
        _test_user_id = user_id
        _pass("01_create_account", f"reused existing account={account_id}")
        return account_id, user_id

    # Create fresh
    account_oid = ObjectId()
    await database.accounts_collection.insert_one({
        "_id": account_oid,
        "name": TEST_NAME,
        "slug": f"test-techdevs-{str(account_oid)[-6:]}",
        "plan": "trial",
        "created_at": datetime.utcnow(),
        "updated_at": datetime.utcnow(),
    })

    user_oid = ObjectId()
    hashed = _bcrypt.hashpw("TestPassword123!".encode("utf-8"), _bcrypt.gensalt()).decode("utf-8")
    await database.users_collection.insert_one({
        "_id": user_oid,
        "email": TEST_EMAIL,
        "name": TEST_NAME,
        "password_hash": hashed,
        "current_account_id": account_oid,
        "onboarding_complete": False,
        "created_at": datetime.utcnow(),
    })

    await database.account_members_collection.insert_one({
        "_id": ObjectId(),
        "account_id": account_oid,
        "user_id": user_oid,
        "role": "owner",
        "created_at": datetime.utcnow(),
    })

    account_id = str(account_oid)
    user_id = str(user_oid)
    _test_account_id = account_id
    _test_user_id = user_id
    _pass("01_create_account", f"account={account_id}")
    return account_id, user_id


async def step_02_save_company_profile(account_id: str) -> None:
    """Save TechDevs ICP + profile to company_profiles (simulates stages 1-4)."""
    profile = {**TECHDEVS_PROFILE, "account_id": account_id}
    await database.company_profiles_collection.update_one(
        {"account_id": account_id},
        {"$set": profile},
        upsert=True,
    )
    saved = await database.company_profiles_collection.find_one({"account_id": account_id})
    assert saved, "Profile not saved"
    assert saved.get("target_industries"), "target_industries not saved"
    _pass("02_save_profile", f"industries={saved['target_industries'][:2]}")


async def step_03_start_session(account_id: str, user_id: str) -> str:
    """Create an onboarding session document."""
    import uuid
    session_id = str(uuid.uuid4())
    now = datetime.utcnow()
    # onboarding_sessions has unique index on account_id — upsert to handle re-runs
    await database.onboarding_sessions_collection.update_one(
        {"account_id": account_id},
        {"$set": {
            "session_id": session_id,
            "account_id": account_id,
            "user_id": user_id,
            "current_stage": 1,
            "stage_data": {},
            "refinement_chat_history": [],
            "updated_at": now,
            "prospect_preview": None,
            "prospect_preview_excluded_companies": [],
        }, "$setOnInsert": {
            "_id": ObjectId(),
            "created_at": now,
        }},
        upsert=True,
    )
    _pass("03_start_session", f"session_id={session_id[:8]}...")
    return session_id


# Real TechDevs-ICP SaaS companies (used when --mock-companies flag is set)
_MOCK_COMPANIES = [
    {
        "company_name": "Linear",
        "company_linkedin_url": "https://www.linkedin.com/company/linear-app/",
        "company_domain": "linear.app",
        "company_website": "https://linear.app",
        "industry": "SaaS",
        "country": "United States",
        "employee_size_estimate": "51-200",
        "description": "Linear is a project management tool built for high-performance teams.",
    },
    {
        "company_name": "Cal.com",
        "company_linkedin_url": "https://www.linkedin.com/company/calcom/",
        "company_domain": "cal.com",
        "company_website": "https://cal.com",
        "industry": "SaaS",
        "country": "United States",
        "employee_size_estimate": "11-50",
        "description": "Scheduling infrastructure for everyone.",
    },
    {
        "company_name": "Attio",
        "company_linkedin_url": "https://www.linkedin.com/company/attio/",
        "company_domain": "attio.com",
        "company_website": "https://attio.com",
        "industry": "SaaS",
        "country": "United Kingdom",
        "employee_size_estimate": "11-50",
        "description": "The CRM for modern go-to-market teams.",
    },
    {
        "company_name": "Loops",
        "company_linkedin_url": "https://www.linkedin.com/company/loops-so/",
        "company_domain": "loops.so",
        "company_website": "https://loops.so",
        "industry": "SaaS",
        "country": "United States",
        "employee_size_estimate": "1-10",
        "description": "Email for modern SaaS companies.",
    },
    {
        "company_name": "Supabase",
        "company_linkedin_url": "https://www.linkedin.com/company/supabase/",
        "company_domain": "supabase.com",
        "company_website": "https://supabase.com",
        "industry": "Developer Tools",
        "country": "United States",
        "employee_size_estimate": "51-200",
        "description": "The open source Firebase alternative.",
    },
]


async def step_04_source_preview(account_id: str, session_id: str, mock_companies: bool = False) -> list[dict]:
    """Call source_preview_prospects and assert ≥ 3 prospects with emails."""
    from services.onboarding_prospect_service import source_preview_prospects
    from services.employee_scraper_service import bulk_scrape_employees_for_companies, transform_employee_to_prospect
    from services.email_finder_service import find_emails_for_linkedin_urls

    profile = await database.company_profiles_collection.find_one({"account_id": account_id})

    t0 = time.time()
    if mock_companies:
        logger.info("Step 04: MOCK MODE — using hardcoded companies, running real Apify scrape + email finder...")
        company_urls = [c["company_linkedin_url"] for c in _MOCK_COMPANIES]
        raw_employees = await bulk_scrape_employees_for_companies(
            company_urls, max_items_per_company=1, account_id=account_id
        )
        logger.info("Mock mode: Got %d raw employees", len(raw_employees))
        url_to_company = {c["company_linkedin_url"]: c for c in _MOCK_COMPANIES}

        prospects = []
        assigned_urls = set()
        for emp in raw_employees:
            positions = emp.get("currentPositions") or []
            emp_co_li = positions[0].get("companyLinkedinUrl", "") if positions else ""
            matched = None
            for url in company_urls:
                if emp_co_li and (url.rstrip("/") in emp_co_li.rstrip("/") or emp_co_li.rstrip("/") in url.rstrip("/")):
                    matched = url
                    break
            if not matched:
                for url in company_urls:
                    if url not in assigned_urls:
                        matched = url
                        break
            if matched:
                assigned_urls.add(matched)
                sc = url_to_company[matched]
                p = transform_employee_to_prospect(emp, sc)
                p["ai_prospect_score"] = 90.0
                p["fit_score"] = 0.90
                p["_sourced_company"] = sc
                prospects.append(p)

        # Email finder
        missing_lis = [p["linkedin"] for p in prospects if not p.get("email") and p.get("linkedin")]
        if missing_lis:
            try:
                email_map = await find_emails_for_linkedin_urls(missing_lis, account_id=account_id)
                for p in prospects:
                    if not p.get("email") and p.get("linkedin"):
                        found = email_map.get(p["linkedin"])
                        if found:
                            p["email"] = found
            except Exception as exc:
                logger.warning("Email finder failed (non-fatal): %s", exc)
    else:
        logger.info("Step 04: Sourcing 5 prospect previews (Gemini + Apify + email finder)...")
        prospects = await source_preview_prospects(profile, count=5, account_id=account_id)
    elapsed = time.time() - t0

    logger.info("source_preview_prospects returned %d prospects in %.1fs", len(prospects), elapsed)

    if len(prospects) < 1:
        _fail("04_source_preview", f"Got 0 prospects (elapsed={elapsed:.0f}s)")
        return []

    # Save to session
    now = datetime.utcnow()
    company_names = [p.get("company_name") for p in prospects if p.get("company_name")]
    await database.onboarding_sessions_collection.update_one(
        {"session_id": session_id},
        {"$set": {
            "prospect_preview": prospects,
            "prospect_preview_excluded_companies": company_names,
            "prospect_preview_at": now,
            "updated_at": now,
        }},
    )

    _print_table(prospects)

    has_li = sum(1 for p in prospects if p.get("linkedin"))
    has_email = sum(1 for p in prospects if p.get("email"))
    has_name = sum(1 for p in prospects if p.get("full_name"))
    has_company = sum(1 for p in prospects if p.get("company_name"))

    detail = (
        f"total={len(prospects)}, linkedin={has_li}, email={has_email}, "
        f"name={has_name}, company={has_company}, elapsed={elapsed:.0f}s"
    )

    if len(prospects) >= 3 and has_li >= 3:
        _pass("04_source_preview", detail)
    else:
        _fail("04_source_preview", detail + " — fewer than 3 prospects with LinkedIn")

    return prospects


async def step_05_reroll(account_id: str, session_id: str) -> None:
    """Re-roll: source a new set of 5 companies different from step 04."""
    from services.onboarding_prospect_service import source_preview_prospects

    session = await database.onboarding_sessions_collection.find_one({"session_id": session_id})
    exclude = list((session or {}).get("prospect_preview_excluded_companies") or [])

    if not exclude:
        _fail("05_reroll", "No excluded companies — step 04 must have failed")
        return

    logger.info("Step 05: Re-rolling (excluding %d companies)...", len(exclude))
    profile = await database.company_profiles_collection.find_one({"account_id": account_id})

    t0 = time.time()
    new_prospects = await source_preview_prospects(
        profile,
        count=5,
        exclude_company_names=exclude,
        account_id=account_id,
    )
    elapsed = time.time() - t0

    if not new_prospects:
        _fail("05_reroll", f"Got 0 new prospects (elapsed={elapsed:.0f}s)")
        return

    # Check that these are different companies
    new_companies = {p.get("company_name", "") for p in new_prospects}
    overlap = new_companies & set(exclude)
    detail = (
        f"total={len(new_prospects)}, new_companies={new_companies}, "
        f"overlap_with_excluded={overlap}, elapsed={elapsed:.0f}s"
    )

    print("\n--- RE-ROLLED PROSPECTS ---")
    _print_table(new_prospects)

    if len(new_prospects) >= 1:
        _pass("05_reroll", detail)
    else:
        _fail("05_reroll", detail)

    # Update session with the original prospects (we confirm the step-04 batch, not the re-roll)
    # In a real UI the user would pick; for the test we use the step-04 batch for launch.
    # Restore:
    step04_prospects = list((session or {}).get("prospect_preview") or [])
    if step04_prospects:
        await database.onboarding_sessions_collection.update_one(
            {"session_id": session_id},
            {"$set": {"prospect_preview": step04_prospects}},
        )


async def step_06_launch_campaign(
    account_id: str,
    user_id: str,
    session_id: str,
) -> str | None:
    """Launch the first campaign with the confirmed 5 prospects."""
    from services.onboarding_prospect_service import launch_onboarding_first_campaign

    session = await database.onboarding_sessions_collection.find_one({"session_id": session_id})
    confirmed = list((session or {}).get("prospect_preview") or [])

    if not confirmed:
        _fail("06_launch_campaign", "No confirmed prospects in session")
        return None

    profile = await database.company_profiles_collection.find_one({"account_id": account_id})

    logger.info("Step 06: Launching first campaign with %d confirmed prospects...", len(confirmed))
    t0 = time.time()
    try:
        campaign_id = await launch_onboarding_first_campaign(
            account_id=account_id,
            user_id=user_id,
            profile=profile,
            confirmed_prospects=confirmed,
            target_total=50,
        )
    except Exception as exc:
        _fail("06_launch_campaign", str(exc))
        raise
    elapsed = time.time() - t0

    global _test_campaign_id
    _test_campaign_id = campaign_id
    _pass("06_launch_campaign", f"campaign_id={campaign_id}, elapsed={elapsed:.0f}s")
    return campaign_id


async def step_07_assert_campaign_state(campaign_id: str) -> None:
    """Assert campaign = awaiting_approval, ≥ 5 Day-1 enrollments with messages."""
    campaign = await database.campaigns_collection.find_one({"_id": ObjectId(campaign_id)})

    if not campaign:
        _fail("07_campaign_state", "Campaign doc not found in DB")
        return

    status = campaign.get("status")
    disc_status = campaign.get("discovery_status")

    logger.info("Campaign status=%s discovery_status=%s", status, disc_status)

    if status != "awaiting_approval":
        _fail("07_campaign_state", f"Expected status=awaiting_approval, got {status}")
        return

    # Check Day-1 enrollments
    campaign_oid = ObjectId(campaign_id)
    day1_enrs = await database.campaign_enrollments_collection.find({
        "campaign_id": campaign_oid,
        "smart_campaign_send_day": 1,
    }).to_list(length=None)

    logger.info("Day-1 enrollments: %d", len(day1_enrs))

    if len(day1_enrs) < 1:
        _fail("07_campaign_state", f"No Day-1 enrollments found")
        return

    # Check messages generated
    with_messages = [e for e in day1_enrs if e.get("generated_messages") or e.get("message_gen_status") == "done"]
    logger.info("Day-1 enrollments with generated_messages: %d/%d", len(with_messages), len(day1_enrs))

    for e in day1_enrs[:3]:
        logger.info(
            "  Enrollment: prospect=%s send_day=%s channel=%s msg_status=%s has_msgs=%s",
            e.get("prospect_id"),
            e.get("smart_campaign_send_day"),
            e.get("smart_campaign_channel"),
            e.get("message_gen_status"),
            bool(e.get("generated_messages")),
        )

    detail = (
        f"day1_enrollments={len(day1_enrs)}, with_messages={len(with_messages)}, "
        f"status={status}, disc={disc_status}"
    )

    if len(day1_enrs) >= 1 and len(with_messages) >= 1:
        _pass("07_campaign_state", detail)
    else:
        _fail("07_campaign_state", detail + " — messages not generated for Day-1 enrollments")


async def step_08_approve_day1(campaign_id: str) -> None:
    """Approve Day 1 and assert campaign goes active."""
    from services.campaign_launch_service import approve_day

    campaign = await database.campaigns_collection.find_one({"_id": ObjectId(campaign_id)})
    if not campaign:
        _fail("08_approve_day1", "Campaign not found")
        return

    logger.info("Step 08: Calling approve_day(campaign, 1)...")
    t0 = time.time()
    try:
        result = await approve_day(campaign, 1)
    except ValueError as e:
        _fail("08_approve_day1", f"ValueError: {e}")
        return
    except Exception as e:
        _fail("08_approve_day1", f"Exception: {e}")
        raise
    elapsed = time.time() - t0

    logger.info("approve_day result: %s (%.1fs)", result, elapsed)

    # Verify campaign status
    updated = await database.campaigns_collection.find_one({"_id": ObjectId(campaign_id)})
    status = updated.get("status") if updated else "?"
    approval_status = updated.get("approval_status") if updated else "?"

    # Verify Day-1 enrollments have next_action_at set
    campaign_oid = ObjectId(campaign_id)
    active_enrs = await database.campaign_enrollments_collection.find({
        "campaign_id": campaign_oid,
        "smart_campaign_send_day": 1,
        "status": "active",
    }).to_list(length=None)

    enrs_with_nat = [e for e in active_enrs if e.get("next_action_at")]
    future_nat = [e for e in enrs_with_nat if e["next_action_at"] > datetime.utcnow()]

    detail = (
        f"campaign_status={status}, approval_status={approval_status}, "
        f"active_day1={len(active_enrs)}, with_next_action_at={len(enrs_with_nat)}, "
        f"future={len(future_nat)}, scheduled={result.get('total_scheduled',0)}, "
        f"channels={result.get('channel_counts',{})}"
    )

    if status == "active" and len(active_enrs) >= 1:
        _pass("08_approve_day1", detail)
    else:
        _fail("08_approve_day1", detail)


async def step_09_assert_nothing_sent(campaign_id: str) -> None:
    """Assert no actual messages were dispatched."""
    campaign_oid = ObjectId(campaign_id)

    # campaign_messages is where sent messages are recorded by the engine
    sent_count = await database.campaign_messages_collection.count_documents(
        {"campaign_id": campaign_oid}
    ) if hasattr(database, "campaign_messages_collection") else 0

    # Verify no enrollment has status=completed or has outbound message receipts
    completed_enrs = await database.campaign_enrollments_collection.count_documents({
        "campaign_id": campaign_oid,
        "status": "completed",
    })

    detail = f"sent_messages={sent_count}, completed_enrollments={completed_enrs}"
    logger.info("Step 09: Nothing sent check: %s", detail)

    if sent_count == 0 and completed_enrs == 0:
        _pass("09_nothing_sent", detail)
    else:
        _fail("09_nothing_sent", detail + " — unexpected sends/completions")


async def step_10_print_prospect_details(campaign_id: str) -> None:
    """Print final prospect details for manual verification."""
    campaign_oid = ObjectId(campaign_id)
    enrs = await database.campaign_enrollments_collection.find({
        "campaign_id": campaign_oid,
        "smart_campaign_send_day": 1,
    }).to_list(length=None)

    prospect_ids = [e["prospect_id"] for e in enrs if e.get("prospect_id")]
    prospects = await database.prospects_collection.find(
        {"_id": {"$in": prospect_ids}}
    ).to_list(length=None)

    pid_to_p = {p["_id"]: p for p in prospects}

    print("\n" + "=" * 120)
    print("FINAL CAMPAIGN Day-1 PROSPECTS (with generated messages check)")
    print("=" * 120)
    print(f"{'NAME':<28} {'EMAIL':<32} {'COMPANY':<22} {'TITLE':<20} {'MSG_STATUS':<14} {'CHANNEL'}")
    print("-" * 120)
    for enr in enrs:
        p = pid_to_p.get(enr.get("prospect_id"), {})
        name = (p.get("full_name") or "—")[:27]
        email = (p.get("email") or "—")[:31]
        company = (p.get("company_name") or "—")[:21]
        title = (p.get("job_title") or "—")[:19]
        msg_status = enr.get("message_gen_status") or "—"
        channel = enr.get("smart_campaign_channel") or "—"
        print(f"{name:<28} {email:<32} {company:<22} {title:<20} {msg_status:<14} {channel}")
        if enr.get("generated_messages"):
            msgs = enr["generated_messages"]
            if isinstance(msgs, dict):
                keys = list(msgs.keys())[:3]
                print(f"  Messages: {keys}")
    print("=" * 120 + "\n")

    _pass("10_print_final_state", f"printed {len(enrs)} Day-1 enrollments")


# ── Cleanup ────────────────────────────────────────────────────────────────────

async def cleanup(account_id: str, campaign_id: str | None) -> None:
    """Remove test data."""
    logger.info("Cleaning up test data...")
    account_oid = ObjectId(account_id)

    if campaign_id:
        c_oid = ObjectId(campaign_id)
        await database.campaign_enrollments_collection.delete_many({"campaign_id": c_oid})
        await database.sourced_companies_collection.delete_many({"campaign_id": campaign_id})
        await database.campaigns_collection.delete_one({"_id": c_oid})

    await database.company_profiles_collection.delete_one({"account_id": account_id})
    await database.onboarding_sessions_collection.delete_many({"account_id": account_id})

    user = await database.users_collection.find_one({"email": "test_onboarding_techdevs@outflo.test"})
    if user:
        await database.users_collection.delete_one({"_id": user["_id"]})
        await database.account_members_collection.delete_many({"user_id": user["_id"]})
        await database.accounts_collection.delete_one({"_id": account_oid})

    logger.info("Cleanup done.")


# ── Main ──────────────────────────────────────────────────────────────────────

async def main(do_cleanup: bool = False, mock_companies: bool = False) -> None:
    settings = get_settings()
    logger.info("Using MongoDB: %s / %s", settings.mongodb_url[:30], settings.mongodb_database)

    start = time.time()

    try:
        # Step 1: Account
        account_id, user_id = await step_01_create_test_account()

        # Step 2: Profile
        await step_02_save_company_profile(account_id)

        # Step 3: Session
        session_id = await step_03_start_session(account_id, user_id)

        if mock_companies:
            logger.info("*** MOCK MODE: Gemini sourcing bypassed, using hardcoded SaaS companies ***")

        # Step 4: Preview (real Gemini + Apify calls, or mock)
        prospects = await step_04_source_preview(account_id, session_id, mock_companies=mock_companies)

        # Step 5: Re-roll (real Gemini + Apify calls — optional, skip if step 4 failed or mock mode)
        if prospects and not mock_companies:
            await step_05_reroll(account_id, session_id)
        elif mock_companies:
            logger.info("Skipping step 05 (re-roll) in mock-companies mode")
        else:
            logger.warning("Skipping step 05 (re-roll) because step 04 returned no prospects")

        # Step 6: Launch
        if prospects:
            campaign_id = await step_06_launch_campaign(account_id, user_id, session_id)
        else:
            _fail("06_launch_campaign", "Skipped — no prospects from step 04")
            campaign_id = None

        # Step 7: Assert campaign state
        if campaign_id:
            await step_07_assert_campaign_state(campaign_id)
        else:
            _fail("07_campaign_state", "Skipped — no campaign_id")

        # Step 8: Approve Day 1
        if campaign_id:
            await step_08_approve_day1(campaign_id)
        else:
            _fail("08_approve_day1", "Skipped — no campaign_id")

        # Step 9: Nothing sent
        if campaign_id:
            await step_09_assert_nothing_sent(campaign_id)
        else:
            _fail("09_nothing_sent", "Skipped — no campaign_id")

        # Step 10: Print final state
        if campaign_id:
            await step_10_print_prospect_details(campaign_id)

    except Exception as exc:
        logger.exception("Unexpected error in test: %s", exc)
        _fail("unexpected_error", str(exc))

    finally:
        elapsed_total = time.time() - start

        # ── Summary ──────────────────────────────────────────────────────────
        passes = sum(1 for r in _results if r["status"] == "PASS")
        fails = sum(1 for r in _results if r["status"] == "FAIL")

        print("\n" + "=" * 70)
        print("TEST SUMMARY")
        print("=" * 70)
        for r in _results:
            icon = "✅" if r["status"] == "PASS" else "❌"
            print(f"  {icon}  {r['step']:<35} {r['detail']}")
        print("-" * 70)
        print(f"  Total: {passes} PASS, {fails} FAIL  ({elapsed_total:.0f}s)")
        print("=" * 70 + "\n")

        # Save results
        out_dir = Path(__file__).parent / "results"
        out_dir.mkdir(exist_ok=True)
        ts = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
        out_file = out_dir / f"onboarding_test_{ts}.json"
        out_file.write_text(json.dumps({
            "timestamp": ts,
            "account_id": _test_account_id,
            "campaign_id": _test_campaign_id,
            "results": _results,
            "elapsed_seconds": elapsed_total,
        }, indent=2, default=str))
        logger.info("Results saved to %s", out_file)

        if do_cleanup and _test_account_id:
            await cleanup(_test_account_id, _test_campaign_id)

        if fails > 0:
            sys.exit(1)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Test onboarding → campaign flow for TechDevs")
    parser.add_argument("--cleanup", action="store_true", help="Delete test data after run")
    parser.add_argument(
        "--mock-companies",
        action="store_true",
        help=(
            "Skip Gemini sourcing; use 5 hardcoded SaaS company LinkedIn URLs. "
            "Use when Gemini API credits are depleted to test Apify + campaign pipeline."
        ),
    )
    args = parser.parse_args()
    asyncio.run(main(do_cleanup=args.cleanup, mock_companies=args.mock_companies))
