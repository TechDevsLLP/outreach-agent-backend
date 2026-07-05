"""
Reset onboarding state for a given user email.
Clears: onboarding_complete flag, company profile onboarding fields,
        and all onboarding session documents for the account.

Usage:
    cd /Users/prasad/Documents/Projects/outflo/backend
    python scripts/reset_onboarding.py prithvi@techdevs.in
"""

import asyncio
import sys
import os

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from motor.motor_asyncio import AsyncIOMotorClient
from bson import ObjectId
from datetime import datetime, timezone
from config import get_settings


async def reset_onboarding(email: str):
    settings = get_settings()
    client = AsyncIOMotorClient(settings.mongodb_url)
    db = client[settings.mongodb_database]

    # 1. Find the user
    user = await db["users"].find_one({"email": email})
    if not user:
        print(f"ERROR: No user found with email {email}")
        return

    user_id = user["_id"]
    account_id_str = user.get("current_account_id")
    print(f"Found user: {email} | user_id={user_id} | account_id={account_id_str}")
    print(f"  onboarding_complete was: {user.get('onboarding_complete')}")

    # 2. Reset user onboarding flag
    now = datetime.now(timezone.utc)
    res = await db["users"].update_one(
        {"_id": user_id},
        {"$set": {"onboarding_complete": False, "updated_at": now}},
    )
    print(f"  users: reset onboarding_complete → False (matched={res.matched_count})")

    if not account_id_str:
        print("WARNING: user has no current_account_id — skipping account-scoped resets")
        return

    # Resolve account_id as both string and ObjectId for cross-format queries
    try:
        account_oid = ObjectId(account_id_str)
    except Exception:
        account_oid = None

    # 3. Reset company profile onboarding fields (keep the record, just clear onboarding progress)
    onboarding_fields_to_clear = {
        "onboarding_stage": 0,
        "onboarding_completed_at": None,
        "company_name": "",
        "description": "",
        "pain_points": [],
        "target_market": "",
        "services": [],
        "industries": [],
        "differentiators": [],
        "value_propositions": [],
        "icp_description": "",
        "sender_name": "",
        "sender_role": "",
        "sender_voice_profile": None,
        "case_studies": [],
        "updated_at": now,
    }

    # Try both string and ObjectId account_id formats
    for aid in ([account_oid, account_id_str] if account_oid else [account_id_str]):
        res = await db["company_profiles"].update_one(
            {"account_id": aid},
            {"$set": onboarding_fields_to_clear},
        )
        if res.matched_count:
            print(f"  company_profiles: cleared onboarding fields (account_id={aid}, matched={res.matched_count})")
            break
    else:
        print("  company_profiles: no document found for this account (nothing to reset)")

    # 4. Delete all onboarding sessions for this account
    deleted = 0
    for aid in ([account_oid, account_id_str] if account_oid else [account_id_str]):
        res = await db["onboarding_sessions"].delete_many({"account_id": str(aid)})
        deleted += res.deleted_count
    # Also try with ObjectId stored account_id
    if account_oid:
        res = await db["onboarding_sessions"].delete_many({"account_id": account_oid})
        deleted += res.deleted_count
    print(f"  onboarding_sessions: deleted {deleted} session document(s)")

    # 5. Reset account name back to email prefix (optional cosmetic)
    if account_oid:
        res = await db["accounts"].update_one(
            {"_id": account_oid},
            {"$set": {"updated_at": now}},
        )
        print(f"  accounts: touched (matched={res.matched_count})")

    print("\nDone. User can now log in and start onboarding from the beginning.")
    client.close()


if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python scripts/reset_onboarding.py <email>")
        sys.exit(1)
    asyncio.run(reset_onboarding(sys.argv[1]))
