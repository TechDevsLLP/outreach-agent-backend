"""
One-time migration: rename `daily_caps.linkedin_connect` → `daily_caps.linkedin_connection`
in any campaign documents that have the old (typo) key.

Run from the backend/ directory:
    python migrations/fix_linkedin_connect_key.py

Safe to re-run (only touches docs that still have the old key).
"""
import asyncio
import sys
import os

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from motor.motor_asyncio import AsyncIOMotorClient
from config import get_settings


async def main() -> None:
    settings = get_settings()
    client = AsyncIOMotorClient(settings.mongodb_url)
    db = client[settings.mongodb_database]
    campaigns = db["campaigns"]

    result = await campaigns.update_many(
        {"daily_caps.linkedin_connect": {"$exists": True}},
        [
            {"$set": {
                "daily_caps.linkedin_connection": "$daily_caps.linkedin_connect",
            }},
            {"$unset": "daily_caps.linkedin_connect"},
        ],
    )
    print(f"Updated {result.modified_count} campaign(s) with typo key linkedin_connect → linkedin_connection")
    client.close()


if __name__ == "__main__":
    asyncio.run(main())
