"""
One-time migration: Creates Neil's user/account and backfills account_id on all existing collections.
Also:
- Creates "Test" campaign from existing outreach_schedules history
- Creates SendGrid email_account for Neil
- Rebuilds company prospect_count from prospects
- Adds prospect_id to employees that became prospects

Run from the backend/ directory:
    python migrations/backfill_account_id.py

Safe to re-run (uses $exists checks and upserts).
"""

import asyncio
import sys
import os
from datetime import datetime, timezone
from bson import ObjectId

# Add parent to path for imports
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from motor.motor_asyncio import AsyncIOMotorClient
from config import get_settings


def now_utc() -> datetime:
    return datetime.now(timezone.utc)


async def main():
    settings = get_settings()
    client = AsyncIOMotorClient(settings.mongodb_url)
    db = client[settings.mongodb_database]

    print("=" * 60)
    print("OutFlo backfill_account_id migration")
    print("=" * 60)

    # ------------------------------------------------------------------
    # Step 1: Create Neil's user
    # ------------------------------------------------------------------
    print("\n[1/12] Creating Neil's user...")
    existing_user = await db["users"].find_one({"email": settings.admin_email})
    if existing_user:
        user_id = existing_user["_id"]
        print(f"  → User already exists: {user_id}")
    else:
        if not settings.admin_email:
            print("  ⚠ admin_email not set in .env — skipping user creation")
            client.close()
            return
        user_doc = {
            "email": settings.admin_email,
            "password_hash": settings.admin_password_hash,
            "name": "Neel Shah",
            "plan": "pro",
            "onboarding_complete": True,
            "current_account_id": None,
            "created_at": now_utc(),
            "updated_at": now_utc(),
        }
        result = await db["users"].insert_one(user_doc)
        user_id = result.inserted_id
        print(f"  → Created user: {user_id}")

    # ------------------------------------------------------------------
    # Step 2: Create "Neil's Account"
    # ------------------------------------------------------------------
    print("\n[2/12] Creating Neil's account...")
    existing_account = await db["accounts"].find_one({"slug": "neils-account"})
    if existing_account:
        account_id = existing_account["_id"]
        print(f"  → Account already exists: {account_id}")
    else:
        account_doc = {
            "name": "Neel Shah - Mentopreneur",
            "slug": "neils-account",
            "plan": "pro",
            "status": "active",
            "sender_name": "Mentor Neel Shah",
            "sender_email": settings.sender_email,
            "daily_email_quota": 20,
            "daily_linkedin_connection_quota": 20,
            "daily_linkedin_inmail_quota": 5,
            "created_at": now_utc(),
            "updated_at": now_utc(),
        }
        result = await db["accounts"].insert_one(account_doc)
        account_id = result.inserted_id
        print(f"  → Created account: {account_id}")

    # ------------------------------------------------------------------
    # Step 3: Create account_member (owner)
    # ------------------------------------------------------------------
    print("\n[3/12] Creating account_member (owner)...")
    existing_member = await db["account_members"].find_one(
        {"account_id": account_id, "user_id": user_id}
    )
    if existing_member:
        print(f"  → Member link already exists")
    else:
        member_doc = {
            "account_id": account_id,
            "user_id": user_id,
            "role": "owner",
            "created_at": now_utc(),
        }
        await db["account_members"].insert_one(member_doc)
        print(f"  → Created account_member (owner)")

    # ------------------------------------------------------------------
    # Step 4: Update user.current_account_id
    # ------------------------------------------------------------------
    print("\n[4/12] Updating user.current_account_id...")
    await db["users"].update_one(
        {"_id": user_id},
        {"$set": {"current_account_id": str(account_id), "updated_at": now_utc()}},
    )
    print(f"  → Set current_account_id = {account_id}")

    # ------------------------------------------------------------------
    # Step 5: Load/create company_profile
    # ------------------------------------------------------------------
    print("\n[5/12] Upserting company_profile...")
    existing_profile = await db["company_profiles"].find_one({"account_id": account_id})
    if existing_profile:
        print(f"  → company_profile already exists: {existing_profile['_id']}")
    else:
        # Try to load sender context from system_prompts (slug "company-profile" or "system-prompt")
        system_prompt = await db["system_prompts"].find_one(
            {"slug": {"$in": ["company-profile", "system-prompt", "icp"]}}
        )
        company_name = "Mentopreneur"
        icp_description = ""
        if system_prompt:
            company_name = system_prompt.get("company_name", company_name)
            icp_description = system_prompt.get("content", "")
            print(f"  → Seeding from system_prompts slug={system_prompt.get('slug')}")

        profile_doc = {
            "account_id": account_id,
            "user_id": user_id,
            "company_name": company_name,
            "sender_name": "Mentor Neel Shah",
            "sender_role": "Founder",
            "sender_email": settings.sender_email,
            "website_url": "",
            "description": "",
            "services": [],
            "target_market": "",
            "differentiators": [],
            "pain_points": [],
            "value_propositions": [],
            "tone_of_voice": "professional",
            "case_studies": [],
            "outreach_strategy": "email_first",
            "connection_request_guidance": None,
            "email_guidance": None,
            "inmail_guidance": None,
            "icp_description": icp_description,
            "scoring_weights": None,
            "created_at": now_utc(),
            "updated_at": now_utc(),
        }
        result = await db["company_profiles"].insert_one(profile_doc)
        print(f"  → Created company_profile: {result.inserted_id}")

    # ------------------------------------------------------------------
    # Step 6: Create SendGrid email_account
    # ------------------------------------------------------------------
    print("\n[6/12] Creating SendGrid email_account...")
    existing_email_account = await db["email_accounts"].find_one(
        {"account_id": account_id, "email": settings.sender_email}
    )
    if existing_email_account:
        print(f"  → email_account already exists: {existing_email_account['_id']}")
    else:
        email_account_doc = {
            "account_id": account_id,
            "user_id": user_id,
            "email": settings.sender_email,
            "display_name": settings.sender_name,
            "provider": "sendgrid",
            "sendgrid_api_key": settings.sendgrid_api_key,
            "sendgrid_sender_name": settings.sender_name,
            "sendgrid_sender_email": settings.sender_email,
            "sendgrid_reply_to": settings.reply_to_email,
            "status": "connected",
            "health": "healthy",
            "created_at": now_utc(),
            "updated_at": now_utc(),
        }
        result = await db["email_accounts"].insert_one(email_account_doc)
        print(f"  → Created email_account: {result.inserted_id}")

    # ------------------------------------------------------------------
    # Step 7: Backfill account_id on existing collections
    # ------------------------------------------------------------------
    print("\n[7/12] Backfilling account_id on existing collections...")
    collections_to_backfill = [
        "prospects",
        "industries",
        "search_runs",
        "enrichment_runs",
        "conversations",
        "outreach_schedules",
        "notifications",
    ]
    for col_name in collections_to_backfill:
        try:
            result = await db[col_name].update_many(
                {"account_id": {"$exists": False}},
                {"$set": {"account_id": account_id}},
            )
            print(f"  → {col_name}: {result.modified_count} documents updated")
        except Exception as e:
            print(f"  ⚠ {col_name}: error — {e}")

    # ------------------------------------------------------------------
    # Step 8: Create "Test" campaign from outreach history
    # ------------------------------------------------------------------
    print("\n[8/12] Creating 'Test' campaign from outreach_schedules history...")
    existing_campaign = await db["campaigns"].find_one(
        {"account_id": account_id, "name": "Test"}
    )
    if existing_campaign:
        print(f"  → 'Test' campaign already exists: {existing_campaign['_id']}")
    else:
        # Gather all outreach_schedule documents
        schedules = await db["outreach_schedules"].find(
            {"account_id": account_id}
        ).sort("schedule_date", 1).to_list(length=None)

        if not schedules:
            print("  → No outreach_schedules found — skipping campaign creation")
        else:
            # Determine earliest schedule date
            earliest_date = schedules[0].get("schedule_date") or schedules[0].get("created_at") or now_utc()

            # Build campaign steps
            step1_email = {
                "step_number": 1,
                "type": "email",
                "delay_days": 0,
                "subject_template": "Reaching out",
                "body_template": "",
            }
            step2_linkedin = {
                "step_number": 2,
                "type": "linkedin_connection",
                "delay_days": 3,
                "message_template": "",
            }

            campaign_doc = {
                "account_id": account_id,
                "name": "Test",
                "type": "custom",
                "status": "completed",
                "steps": [step1_email, step2_linkedin],
                "created_by": user_id,
                "created_at": earliest_date,
                "updated_at": now_utc(),
                # Counters — computed below
                "total_enrolled": 0,
                "total_replied": 0,
                "total_emails_sent": 0,
                "total_linkedin_sent": 0,
                "total_completed": 0,
            }
            camp_result = await db["campaigns"].insert_one(campaign_doc)
            campaign_id = camp_result.inserted_id
            print(f"  → Created campaign: {campaign_id}")

            # Process each schedule and create enrollments + messages
            enrolled_prospects: dict = {}  # prospect_id -> enrollment_id
            total_emails_sent = 0
            total_linkedin_sent = 0
            total_replied = 0

            for schedule in schedules:
                schedule_date = schedule.get("schedule_date") or schedule.get("created_at") or now_utc()

                # Collect all batch items
                batch_items = []

                email_batch = schedule.get("email_batch") or {}
                for item in email_batch.get("items", []):
                    item["_channel"] = "email"
                    batch_items.append(item)

                linkedin_conn_batch = schedule.get("linkedin_connection_batch") or {}
                for item in linkedin_conn_batch.get("items", []):
                    item["_channel"] = "linkedin_connection"
                    batch_items.append(item)

                linkedin_inmail_batch = schedule.get("linkedin_inmail_batch") or {}
                for item in linkedin_inmail_batch.get("items", []):
                    item["_channel"] = "linkedin_inmail"
                    batch_items.append(item)

                for item in batch_items:
                    try:
                        raw_prospect_id = item.get("prospect_id") or item.get("lead_id")
                        if not raw_prospect_id:
                            continue

                        try:
                            prospect_oid = ObjectId(raw_prospect_id) if not isinstance(raw_prospect_id, ObjectId) else raw_prospect_id
                        except Exception:
                            continue

                        send_status = item.get("send_status", "sent")
                        channel = item["_channel"]

                        # Create enrollment if not yet created for this prospect
                        if prospect_oid not in enrolled_prospects:
                            enrollment_status = "replied" if send_status == "replied" else "completed"
                            enrollment_doc = {
                                "campaign_id": campaign_id,
                                "account_id": account_id,
                                "prospect_id": prospect_oid,
                                "status": enrollment_status,
                                "current_step": 2,
                                "enrolled_at": schedule_date,
                                "next_action_at": None,
                                "completed_at": schedule_date,
                                "created_at": schedule_date,
                                "updated_at": now_utc(),
                            }
                            enroll_result = await db["campaign_enrollments"].insert_one(enrollment_doc)
                            enrolled_prospects[prospect_oid] = enroll_result.inserted_id

                            if send_status == "replied":
                                total_replied += 1

                        enrollment_id = enrolled_prospects[prospect_oid]

                        # Create campaign message
                        message_doc = {
                            "campaign_id": campaign_id,
                            "campaign_enrollment_id": enrollment_id,
                            "account_id": account_id,
                            "prospect_id": prospect_oid,
                            "channel": channel,
                            "status": send_status,
                            "sent_at": schedule_date,
                            "subject": item.get("subject", ""),
                            "body": item.get("body") or item.get("message") or "",
                            "provider_message_id": item.get("sendgrid_message_id") or item.get("message_id"),
                            "created_at": schedule_date,
                            "updated_at": now_utc(),
                        }
                        try:
                            await db["campaign_messages"].insert_one(message_doc)
                        except Exception:
                            pass  # Duplicate provider_message_id — skip

                        if channel == "email":
                            total_emails_sent += 1
                        elif channel in ("linkedin_connection", "linkedin_inmail"):
                            total_linkedin_sent += 1

                    except Exception as e:
                        print(f"    ⚠ Error processing item: {e}")
                        continue

            total_enrolled = len(enrolled_prospects)
            total_completed = total_enrolled

            # Update campaign counters
            await db["campaigns"].update_one(
                {"_id": campaign_id},
                {"$set": {
                    "total_enrolled": total_enrolled,
                    "total_replied": total_replied,
                    "total_emails_sent": total_emails_sent,
                    "total_linkedin_sent": total_linkedin_sent,
                    "total_completed": total_completed,
                    "updated_at": now_utc(),
                }},
            )
            print(
                f"  → Enrolled: {total_enrolled}, Replied: {total_replied}, "
                f"Emails: {total_emails_sent}, LinkedIn: {total_linkedin_sent}"
            )

    # ------------------------------------------------------------------
    # Step 9: Backfill company prospect_count from prospects
    # ------------------------------------------------------------------
    print("\n[9/12] Rebuilding company prospect_count from prospects...")
    try:
        pipeline = [
            {"$match": {"account_id": account_id, "company_linkedin": {"$exists": True, "$ne": None, "$ne": ""}}},
            {"$group": {
                "_id": "$company_linkedin",
                "prospect_count": {"$sum": 1},
                "enriched_count": {
                    "$sum": {"$cond": [{"$eq": ["$enrichment_status", "enriched"]}, 1, 0]}
                },
            }},
        ]
        cursor = db["prospects"].aggregate(pipeline)
        updated_companies = 0
        async for group in cursor:
            linkedin_url = group["_id"]
            res = await db["companies"].update_one(
                {"linkedin_url": linkedin_url},
                {"$set": {
                    "prospect_count": group["prospect_count"],
                    "enriched_prospect_count": group["enriched_count"],
                }},
            )
            if res.matched_count:
                updated_companies += 1
        print(f"  → Updated prospect_count on {updated_companies} companies")
    except Exception as e:
        print(f"  ⚠ Error rebuilding prospect_count: {e}")

    # ------------------------------------------------------------------
    # Step 10: Add prospect_id to employees
    # ------------------------------------------------------------------
    print("\n[10/12] Backfilling employees.prospect_id...")
    try:
        # Find all enriched employees without a prospect_id
        employees_cursor = db["employees"].find(
            {"enriched": True, "prospect_id": {"$exists": False}, "linkedin_url": {"$exists": True}}
        )
        employee_updates = 0
        async for employee in employees_cursor:
            linkedin_url = employee.get("linkedin_url")
            if not linkedin_url:
                continue
            prospect = await db["prospects"].find_one(
                {"linkedin": linkedin_url, "account_id": account_id},
                {"_id": 1},
            )
            if prospect:
                await db["employees"].update_one(
                    {"_id": employee["_id"]},
                    {"$set": {"prospect_id": prospect["_id"]}},
                )
                employee_updates += 1
        print(f"  → Linked prospect_id on {employee_updates} employees")
    except Exception as e:
        print(f"  ⚠ Error backfilling employees.prospect_id: {e}")

    # ------------------------------------------------------------------
    # Step 11: Create prospect_stats_counts for Neil
    # ------------------------------------------------------------------
    print("\n[11/12] Building prospect_stats_counts...")
    try:
        existing_stats = await db["prospect_stats_counts"].find_one({"account_id": account_id})
        if existing_stats:
            print(f"  → prospect_stats_counts already exists: {existing_stats['_id']}")
        else:
            # Count by status
            status_pipeline = [
                {"$match": {"account_id": account_id}},
                {"$group": {"_id": "$status", "count": {"$sum": 1}}},
            ]
            by_status = {}
            async for doc in db["prospects"].aggregate(status_pipeline):
                by_status[doc["_id"] or "unknown"] = doc["count"]

            # Count by enrichment_status
            enrich_pipeline = [
                {"$match": {"account_id": account_id}},
                {"$group": {"_id": "$enrichment_status", "count": {"$sum": 1}}},
            ]
            by_enrichment = {}
            async for doc in db["prospects"].aggregate(enrich_pipeline):
                by_enrichment[doc["_id"] or "unknown"] = doc["count"]

            # Count by priority_tier
            tier_pipeline = [
                {"$match": {"account_id": account_id}},
                {"$group": {"_id": "$priority_tier", "count": {"$sum": 1}}},
            ]
            by_tier = {}
            async for doc in db["prospects"].aggregate(tier_pipeline):
                by_tier[doc["_id"] or "unknown"] = doc["count"]

            total = sum(by_status.values())

            stats_doc = {
                "account_id": account_id,
                "total": total,
                "by_status": by_status,
                "by_enrichment_status": by_enrichment,
                "by_priority_tier": by_tier,
                "computed_at": now_utc(),
                "created_at": now_utc(),
                "updated_at": now_utc(),
            }
            result = await db["prospect_stats_counts"].insert_one(stats_doc)
            print(f"  → Created prospect_stats_counts: {result.inserted_id} (total={total})")
    except Exception as e:
        print(f"  ⚠ Error building prospect_stats_counts: {e}")

    # ------------------------------------------------------------------
    # Step 12: Summary
    # ------------------------------------------------------------------
    print("\n[12/12] Migration complete!")
    print(f"  User ID    : {user_id}")
    print(f"  Account ID : {account_id}")
    print("=" * 60)

    client.close()


if __name__ == "__main__":
    asyncio.run(main())
