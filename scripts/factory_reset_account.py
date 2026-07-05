"""
Full factory reset for an OutFlo account.

Clears ALL account-scoped data including connected senders so the account
can re-onboard from scratch.  Global shared pools (prospects / companies) are
NEVER touched.

Usage:
    cd backend
    python3 scripts/factory_reset_account.py                    # dry-run (safe)
    python3 scripts/factory_reset_account.py --execute          # DESTRUCTIVE
    python3 scripts/factory_reset_account.py --email other@x.com --execute
"""

import argparse
import asyncio
import logging
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from bson import ObjectId

import database

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("factory_reset")

DEFAULT_EMAIL = "prithvi@techdevs.in"


async def _count(collection, filt: dict) -> int:
    return await collection.count_documents(filt)


async def _delete(collection, filt: dict, dry_run: bool, label: str) -> int:
    n = await _count(collection, filt)
    if n and not dry_run:
        await collection.delete_many(filt)
    action = "would delete" if dry_run else "deleted"
    logger.info("  %-45s %s %d doc(s)", label, action, n)
    return n


async def reset_account(email: str = DEFAULT_EMAIL, dry_run: bool = True) -> None:
    mode = "DRY-RUN (no changes)" if dry_run else "*** LIVE EXECUTE ***"
    logger.info("=" * 60)
    logger.info("Factory reset — %s — %s", email, mode)
    logger.info("=" * 60)

    # ── Resolve user / account ───────────────────────────────────────────────
    user = await database.users_collection.find_one({"email": email})
    if not user:
        logger.error("User not found: %s", email)
        return

    user_id = str(user["_id"])
    account_id_str = str(user.get("current_account_id") or "")
    if not account_id_str:
        member = await database.account_members_collection.find_one(
            {"user_id": ObjectId(user_id)}
        )
        account_id_str = str(member["account_id"]) if member else ""

    if not account_id_str:
        logger.error("No account found for user %s", email)
        return

    try:
        account_oid = ObjectId(account_id_str)
    except Exception:
        logger.error("Bad account_id: %s", account_id_str)
        return

    logger.info("user_id=%s  account_id=%s", user_id, account_id_str)

    # The "$in" filter covers both ObjectId and string account_id regardless of
    # which format a given write-site used.
    aid_filt = {"$in": [account_oid, account_id_str]}

    # ── Collect campaign ids (needed for campaign-scoped orphan cleanup) ─────
    campaigns_cursor = database.campaigns_collection.find(
        {"account_id": aid_filt}, {"_id": 1}
    )
    campaign_oids = [doc["_id"] async for doc in campaigns_cursor]
    campaign_strs = [str(oid) for oid in campaign_oids]
    campaign_any = campaign_oids + campaign_strs
    cid_filt = {"$in": campaign_any} if campaign_any else {"$in": [None]}  # no-match guard

    logger.info("Found %d campaign(s)", len(campaign_oids))

    total_deleted = 0

    # ── Account-keyed collections ─────────────────────────────────────────────
    for col, label in [
        (database.campaigns_collection,            "campaigns"),
        (database.campaign_enrollments_collection, "campaign_enrollments"),
        (database.campaign_daily_schedules_collection, "campaign_daily_schedules"),
        (database.campaign_daily_stats_collection, "campaign_daily_stats"),
        (database.conversations_collection,        "conversations"),
        (database.meetings_collection,             "meetings"),
        (database.reply_classifications_collection, "reply_classifications"),
        (database.notifications_collection,        "notifications"),
        (database.daily_usage_counters_collection, "daily_usage_counters"),
        (database.prospect_stats_counts_collection, "prospect_stats_counts"),
        (database.industries_collection,           "industries"),
        # Senders — local hard-delete only (no external Unipile deauth)
        (database.email_accounts_collection,       "email_accounts [SENDER DISCONNECT]"),
        (database.linkedin_accounts_collection,    "linkedin_accounts [SENDER DISCONNECT]"),
        (database.linkedin_connection_requests_collection, "linkedin_connection_requests"),
        (database.onboarding_sessions_collection,  "onboarding_sessions"),
    ]:
        total_deleted += await _delete(col, {"account_id": aid_filt}, dry_run, label)

    # String-only collections (always written with str account_id)
    for col, label in [
        (database.prospect_state_collection, "prospect_state"),
        (database.onboarding_scrape_jobs_collection, "onboarding_scrape_jobs"),
        (database.suppressions_collection,   "suppressions"),
    ]:
        total_deleted += await _delete(col, {"account_id": account_id_str}, dry_run, label)

    # Campaign-id-keyed orphans
    for col, label in [
        (database.sourced_companies_collection,    "sourced_companies"),
        (database.campaign_messages_collection,    "campaign_messages"),
        (database.campaign_schedule_items_collection, "campaign_schedule_items"),
    ]:
        total_deleted += await _delete(col, {"campaign_id": cid_filt}, dry_run, label)

    # Cost-tracking rows (by campaign_id as string)
    for col, label in [
        (database.apify_usage_collection,      "apify_usage"),
        (database.openrouter_usage_collection, "openrouter_usage"),
    ]:
        total_deleted += await _delete(
            col, {"campaign_id": {"$in": campaign_strs}}, dry_run, label
        )

    # ── Reset company_profiles (onboarding flags + canonical ICP) ────────────
    if dry_run:
        cp = await database.company_profiles_collection.find_one({"account_id": aid_filt})
        logger.info(
            "  %-45s would reset (onboarding_stage=%s)",
            "company_profiles",
            (cp or {}).get("onboarding_stage", "missing"),
        )
    else:
        await database.company_profiles_collection.update_one(
            {"account_id": aid_filt},
            {"$set": {
                "onboarding_stage": 0,
                "onboarding_completed_at": None,
                # Clear canonical ICP so re-onboarding re-canonicalizes from scratch
                "industry_ids": [],
                "country_codes": [],
                "seniorities": [],
                "employee_bands": [],
                "title_query_vec": None,
                # Clear profile fields that wizard repopulates
                "services": [],
                "description": "",
                "icp_description": "",
                "pain_points": [],
                "differentiators": [],
                "case_studies": [],
                "target_industries": [],
                "target_job_titles": [],
                "target_seniority": [],
                "target_geographies": [],
                "target_company_sizes": [],
                "primary_cta": "",
                "objection_bank": [],
                "competitor_bank": [],
                "banned_phrases": [],
            }},
        )
        logger.info("  %-45s reset onboarding_stage→0 + cleared ICP fields", "company_profiles")

    # ── Reset users.onboarding_complete ──────────────────────────────────────
    if dry_run:
        logger.info("  %-45s would set onboarding_complete→False", "users")
    else:
        await database.users_collection.update_one(
            {"_id": user["_id"]},
            {"$set": {"onboarding_complete": False}},
        )
        logger.info("  %-45s set onboarding_complete→False", "users")

    logger.info("=" * 60)
    if dry_run:
        logger.info(
            "DRY-RUN COMPLETE — %d docs would be deleted across all collections.",
            total_deleted,
        )
        logger.info("Run with --execute to apply changes.")
    else:
        logger.info(
            "RESET COMPLETE — %d docs deleted.  Account is clean.", total_deleted
        )
        logger.info("User can now re-onboard from the wizard.")
    logger.info("=" * 60)


async def main() -> None:
    parser = argparse.ArgumentParser(description="Factory-reset an OutFlo account.")
    parser.add_argument(
        "--email", default=DEFAULT_EMAIL, help="Account email (default: %(default)s)"
    )
    parser.add_argument(
        "--execute",
        action="store_true",
        help="Actually delete data.  Without this flag, runs in dry-run mode.",
    )
    args = parser.parse_args()
    await reset_account(email=args.email, dry_run=not args.execute)


if __name__ == "__main__":
    asyncio.run(main())
