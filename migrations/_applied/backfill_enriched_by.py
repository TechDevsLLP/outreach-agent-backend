"""
One-time migration: backfill enriched_by on completed prospects and
triggered_by on all enrichment_runs for the account ns@mentopreneur.com.

Run once after deploying the backend changes that add the enriched_by and
triggered_by fields, before the new frontend goes live.

Usage (from the backend/ directory):
    python -m scripts.backfill_enriched_by
"""

import asyncio
import sys
import os

# Allow running from backend/ directory
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from motor.motor_asyncio import AsyncIOMotorClient
from config import get_settings

TARGET_EMAIL = "ns@mentopreneur.com"


async def main() -> None:
    settings = get_settings()
    client = AsyncIOMotorClient(settings.mongodb_url)
    db = client[settings.mongodb_database]

    # 1. Find the user
    user = await db["users"].find_one({"email": TARGET_EMAIL})
    if not user:
        print(f"ERROR: User '{TARGET_EMAIL}' not found in the users collection.")
        print("Make sure you are connected to the correct database and the user exists.")
        client.close()
        return

    user_id = str(user["_id"])
    print(f"Found user: {TARGET_EMAIL}  ->  _id: {user_id}")

    # 2. Backfill enrichment_runs (all existing runs that have no triggered_by yet)
    runs_result = await db["enrichment_runs"].update_many(
        {"triggered_by": {"$exists": False}},
        {"$set": {"triggered_by": user_id}},
    )
    print(f"enrichment_runs updated:  {runs_result.modified_count} documents")

    # 3. Backfill prospects where enrichment_status == "completed" and enriched_by not yet set
    prospects_result = await db["prospects"].update_many(
        {
            "enrichment_status": "completed",
            "enriched_by": {"$exists": False},
        },
        {"$set": {"enriched_by": user_id}},
    )
    print(f"prospects updated:        {prospects_result.modified_count} documents")

    print("\nMigration complete.")
    client.close()


if __name__ == "__main__":
    asyncio.run(main())
