"""
free_and_reimport_prospects.py

Recovers an Atlas M0 cluster from over-quota by:
  1. Dropping dead legacy collections (drops are allowed even over quota)
  2. Dropping `prospects` (~470 MB freed — unblocks all writes)
  3. Reimporting prospects from backup (vectorless; title_vec added by Stage 4)
  4. Rebuilding indexes via database.create_indexes()

SAFETY: Refuses to run unless a valid backup manifest exists AND the live
        prospect count matches the backup count. Pass --yes to confirm.

Usage:
    python3 scripts/free_and_reimport_prospects.py --backup-dir backups/<stamp> --yes
"""

import asyncio
import gzip
import json
import logging
import os
import sys
import argparse

from bson import json_util
from motor.motor_asyncio import AsyncIOMotorClient

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from config import get_settings

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)-7s %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)

# Collections permanently removed — merged into prospects or derived/droppable
DEAD_COLLECTIONS = ["leads", "employees", "campaign_daily_stats", "prospect_stats_counts"]
INSERT_BATCH = 1000


async def main():
    parser = argparse.ArgumentParser(description="Free space and reimport prospects from backup")
    parser.add_argument("--backup-dir", required=True, help="Timestamped backup directory from backup_all_collections.py")
    parser.add_argument("--yes", action="store_true", help="Confirm destructive operations (required)")
    args = parser.parse_args()

    if not args.yes:
        print("ERROR: Pass --yes to confirm dropping and reimporting collections.")
        print("       This is destructive and irreversible — verify your backup first.")
        sys.exit(1)

    # ── Load + validate manifest ──────────────────────────────────────────────
    manifest_path = os.path.join(args.backup_dir, "manifest.json")
    if not os.path.exists(manifest_path):
        logger.error("No manifest.json found in %s", args.backup_dir)
        logger.error("Run: python3 scripts/backup_all_collections.py first")
        sys.exit(1)

    with open(manifest_path) as fh:
        manifest = json.load(fh)

    prospects_gz = os.path.join(args.backup_dir, "prospects.jsonl.gz")
    if not os.path.exists(prospects_gz):
        logger.error("prospects.jsonl.gz not found in %s", args.backup_dir)
        sys.exit(1)

    settings = get_settings()
    client = AsyncIOMotorClient(settings.mongodb_url)
    db = client[settings.mongodb_database]

    # ── Pre-flight: count check ───────────────────────────────────────────────
    backup_count = manifest.get("collections", {}).get("prospects", {}).get("count", -1)
    live_count = await db["prospects"].count_documents({})
    logger.info("Backup prospects : %d", backup_count)
    logger.info("Live   prospects : %d", live_count)

    if backup_count < 0:
        logger.error("Backup manifest has no valid prospects count. Aborting.")
        sys.exit(1)
    if live_count != backup_count:
        logger.error(
            "Live count (%d) != backup count (%d). "
            "Re-run backup to capture current state. Aborting.",
            live_count,
            backup_count,
        )
        sys.exit(1)

    logger.info("Pre-flight OK. Proceeding with recovery.")
    logger.info("")

    # ── Step 1: Drop dead legacy collections (allowed over quota) ─────────────
    for name in DEAD_COLLECTIONS:
        try:
            await db[name].drop()
            logger.info("Dropped: %s", name)
        except Exception as e:
            logger.warning("Could not drop %s (may not exist): %s", name, e)

    # ── Step 2: Drop prospects (~470 MB freed) ────────────────────────────────
    logger.info("Dropping prospects collection (~470 MB) ...")
    await db["prospects"].drop()
    logger.info("prospects dropped. Writes should now be unblocked.")
    logger.info("")

    # ── Step 3: Reimport prospects from backup (no title_vec) ─────────────────
    logger.info("Reimporting from %s ...", prospects_gz)
    inserted = 0
    errors = 0
    batch = []

    async def flush_batch():
        nonlocal inserted, errors
        if not batch:
            return
        try:
            result = await db["prospects"].insert_many(batch, ordered=False)
            inserted += len(result.inserted_ids)
        except Exception as e:
            logger.error("insert_many batch failed: %s", e)
            errors += 1
        batch.clear()

    with gzip.open(prospects_gz, "rt", encoding="utf-8") as fh:
        for line in fh:
            line = line.strip()
            if not line:
                continue
            doc = json_util.loads(line)
            batch.append(doc)
            if len(batch) >= INSERT_BATCH:
                await flush_batch()
                if inserted % 10_000 == 0 and inserted > 0:
                    logger.info("  Reimported: %d ...", inserted)

    await flush_batch()
    logger.info("Reimport done: %d docs inserted, %d batch errors", inserted, errors)
    logger.info("")

    # ── Step 4: Rebuild indexes ───────────────────────────────────────────────
    logger.info("Rebuilding indexes ...")
    try:
        import database as db_module
        await db_module.create_indexes()
        logger.info("Indexes created.")
    except Exception as e:
        logger.error("Index creation error (run database.create_indexes() manually): %s", e)

    # ── Final verification ────────────────────────────────────────────────────
    final_count = await db["prospects"].count_documents({})
    logger.info("")
    logger.info("=== RECOVERY COMPLETE ===")
    logger.info("Prospects in DB : %d (expected %d)", final_count, backup_count)
    if final_count != backup_count:
        logger.warning("Count mismatch — inspect insert errors above.")
    else:
        logger.info("Count matches backup. ✓")
    logger.info("")
    logger.info("Next steps:")
    logger.info("  1. python3 scripts/migrate_to_shared_schema.py --stage 4   # int8 vector backfill (~37 MB)")
    logger.info("  2. python3 scripts/create_atlas_search_indexes.py           # create vector search index")

    client.close()


if __name__ == "__main__":
    asyncio.run(main())
