"""
create_atlas_search_indexes.py

Creates Atlas Vector Search indexes for prospects (title_vec) and companies (profile_vec).

Stored vectors are BSON Binary subtype 9 (int8, 770 bytes each). Atlas Vector Search
reads these natively — no "quantization" key is needed in the index definition.

Works with pymongo 4.6.1 via raw `createSearchIndexes` DB command (Binary.from_vector
and create_search_index() were added in pymongo 4.10 — not required here).

Index build is asynchronous. This script polls until all indexes reach status=READY
before exiting (up to 10 minutes). Do not run $vectorSearch queries until READY.

Usage:
    python3 scripts/create_atlas_search_indexes.py
"""

import asyncio
import logging
import os
import sys
import time

from motor.motor_asyncio import AsyncIOMotorClient

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from config import get_settings

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)-7s %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)

INDEXES = [
    {
        "collection": "prospects",
        "name": "prospects_vec",
        "definition": {
            "fields": [
                {
                    "type": "vector",
                    "path": "title_vec",
                    "numDimensions": 768,
                    "similarity": "cosine",
                },
                {"type": "filter", "path": "company_industry_id"},
                {"type": "filter", "path": "location.country_code"},
                {"type": "filter", "path": "seniority"},
                {"type": "filter", "path": "company_employee_band"},
                {"type": "filter", "path": "stage"},
            ]
        },
    },
    {
        "collection": "companies",
        "name": "companies_vec",
        "definition": {
            "fields": [
                {
                    "type": "vector",
                    "path": "profile_vec",
                    "numDimensions": 768,
                    "similarity": "cosine",
                },
                {"type": "filter", "path": "industry.id"},
                {"type": "filter", "path": "location.country_code"},
                {"type": "filter", "path": "employee_band"},
            ]
        },
    },
]


async def create_index(db, entry: dict) -> bool:
    """Submit index creation. Returns True if newly created, False if already existed."""
    coll = entry["collection"]
    name = entry["name"]

    # Check if already exists
    try:
        existing = await db[coll].list_search_indexes().to_list(length=50)
        existing_names = {idx.get("name") for idx in existing}
        if name in existing_names:
            logger.info("[%s] %s already exists — skipping creation", coll, name)
            return False
    except Exception as e:
        logger.warning("[%s] Could not list existing search indexes: %s", coll, e)

    try:
        await db.command({
            "createSearchIndexes": coll,
            "indexes": [
                {
                    "name": name,
                    "type": "vectorSearch",
                    "definition": entry["definition"],
                }
            ],
        })
        logger.info("[%s] Submitted: %s", coll, name)
        return True
    except Exception as e:
        err_str = str(e).lower()
        if "already exists" in err_str or "indexalreadyexists" in err_str:
            logger.info("[%s] %s already exists (from error) — skipping", coll, name)
            return False
        logger.error("[%s] Failed to create %s: %s", coll, name, e)
        raise


async def poll_until_ready(db, timeout_s: int = 600):
    """Poll all indexes until they reach READY status."""
    logger.info("Polling until all vector indexes are READY (timeout: %ds) ...", timeout_s)
    deadline = time.monotonic() + timeout_s
    poll_interval = 15  # seconds

    while time.monotonic() < deadline:
        all_ready = True
        for entry in INDEXES:
            coll = entry["collection"]
            name = entry["name"]
            try:
                idxs = await db[coll].list_search_indexes().to_list(length=50)
                for idx in idxs:
                    if idx.get("name") == name:
                        status = idx.get("status", "UNKNOWN")
                        logger.info("[%s] %s → %s", coll, name, status)
                        if status != "READY":
                            all_ready = False
                        break
                else:
                    logger.info("[%s] %s → not found yet", coll, name)
                    all_ready = False
            except Exception as e:
                logger.warning("[%s] list_search_indexes error: %s", coll, e)
                all_ready = False

        if all_ready:
            logger.info("All vector indexes READY. ✓")
            return

        remaining = max(0, deadline - time.monotonic())
        logger.info("Not ready yet — checking again in %ds (%.0fs remaining) ...", poll_interval, remaining)
        await asyncio.sleep(poll_interval)

    logger.warning(
        "Timeout reached — indexes may still be building. "
        "Check the Atlas UI under 'Search Indexes' for final status."
    )


async def main():
    settings = get_settings()
    client = AsyncIOMotorClient(settings.mongodb_url)
    db = client[settings.mongodb_database]

    for entry in INDEXES:
        await create_index(db, entry)

    await poll_until_ready(db)

    logger.info("")
    logger.info("=== DONE ===")
    logger.info("Vector search is live once both indexes show READY in Atlas UI.")

    client.close()


if __name__ == "__main__":
    asyncio.run(main())
