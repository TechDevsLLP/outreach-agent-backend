"""
One-time migration: canonicalize prospect score fields.

What it does:
  1. Backfill prospect_score = ai_prospect_score where they diverge.
  2. Recompute priority_tier using canonical 80/60 thresholds.
  3. $unset enhanced_score and score_modifiers (dead fields).

Idempotent — safe to re-run.

Usage:
  python -m scripts.migrate_canonical_scores           # dry run (default)
  python -m scripts.migrate_canonical_scores --apply   # write to DB
"""

import asyncio
import sys
from motor.motor_asyncio import AsyncIOMotorClient
from config import get_settings


def _tier(score: float | None) -> str | None:
    if score is None:
        return None
    if score >= 80:
        return "hot"
    if score >= 60:
        return "warm"
    return "cold"


async def run(apply: bool) -> None:
    settings = get_settings()
    client = AsyncIOMotorClient(settings.mongodb_url)
    db = client[settings.mongodb_database]
    coll = db["prospects"]

    total = await coll.count_documents({})
    print(f"Total prospects: {total}")

    # Count docs with dead fields
    with_enhanced = await coll.count_documents({"enhanced_score": {"$exists": True}})
    with_modifiers = await coll.count_documents({"score_modifiers": {"$exists": True, "$ne": {}}})
    score_mismatch = await coll.count_documents({
        "ai_prospect_score": {"$ne": None},
        "$expr": {"$ne": ["$prospect_score", "$ai_prospect_score"]}
    })
    bad_tier = await coll.count_documents({
        "ai_prospect_score": {"$ne": None},
        "priority_tier": {"$in": ["hot", "warm", "cold"]},
    })

    print(f"  Docs with enhanced_score: {with_enhanced}")
    print(f"  Docs with non-empty score_modifiers: {with_modifiers}")
    print(f"  Docs where prospect_score != ai_prospect_score: {score_mismatch}")
    print(f"  Docs with ai_prospect_score set (will recompute tier): {bad_tier}")

    if not apply:
        print("\nDRY RUN — pass --apply to write changes.")
        client.close()
        return

    # Step 1: backfill prospect_score from ai_prospect_score where they differ
    backfill_result = await coll.update_many(
        {
            "ai_prospect_score": {"$ne": None},
            "$expr": {"$ne": ["$prospect_score", "$ai_prospect_score"]},
        },
        [{"$set": {"prospect_score": "$ai_prospect_score"}}],
    )
    print(f"\nStep 1 — backfilled prospect_score: {backfill_result.modified_count} docs")

    # Step 2: recompute priority_tier for all docs that have ai_prospect_score
    cursor = coll.find(
        {"ai_prospect_score": {"$ne": None}},
        {"_id": 1, "ai_prospect_score": 1, "priority_tier": 1},
    )
    tier_updates = []
    from pymongo import UpdateOne
    async for doc in cursor:
        correct_tier = _tier(doc.get("ai_prospect_score"))
        if doc.get("priority_tier") != correct_tier:
            tier_updates.append(
                UpdateOne({"_id": doc["_id"]}, {"$set": {"priority_tier": correct_tier}})
            )

    if tier_updates:
        for i in range(0, len(tier_updates), 500):
            chunk = tier_updates[i:i + 500]
            r = await coll.bulk_write(chunk, ordered=False)
            print(f"  Tier batch {i // 500 + 1}: updated {r.modified_count}")
    print(f"Step 2 — recomputed priority_tier: {len(tier_updates)} docs corrected")

    # Step 3: $unset dead fields
    unset_result = await coll.update_many(
        {"$or": [
            {"enhanced_score": {"$exists": True}},
            {"score_modifiers": {"$exists": True}},
        ]},
        {"$unset": {"enhanced_score": "", "score_modifiers": ""}},
    )
    print(f"Step 3 — unset dead fields: {unset_result.modified_count} docs")

    print("\nMigration complete.")
    client.close()


if __name__ == "__main__":
    apply = "--apply" in sys.argv
    asyncio.run(run(apply=apply))
