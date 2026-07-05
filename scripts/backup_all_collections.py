"""
backup_all_collections.py

Backs up every collection in the database to gzipped JSON Lines files.
Works even when Atlas writes are blocked (reads are always allowed over quota).

For `prospects`, the float64 `title_vec` field is excluded from the backup — it is
bloat that will be regenerated as compact int8 binary vectors during Stage 4.
All other fields are preserved as-is. Every other collection is dumped in full.

Usage:
    python3 scripts/backup_all_collections.py [--output-dir backups]
"""

import asyncio
import gzip
import json
import logging
import os
import sys
import argparse
from datetime import datetime, timezone

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


async def backup_collection(
    db,
    name: str,
    out_dir: str,
    *,
    exclude_fields: list | None = None,
) -> tuple[int, int]:
    """Stream a collection to a gzipped JSON Lines file. Returns (doc_count, byte_count)."""
    projection = {f: 0 for f in (exclude_fields or [])}
    cursor = db[name].find({}, projection if projection else None)
    path = os.path.join(out_dir, f"{name}.jsonl.gz")
    count = 0
    with gzip.open(path, "wt", encoding="utf-8") as fh:
        async for doc in cursor:
            fh.write(json_util.dumps(doc) + "\n")
            count += 1
    size = os.path.getsize(path)
    return count, size


async def main():
    parser = argparse.ArgumentParser(description="Backup all MongoDB collections to gzipped JSON Lines")
    parser.add_argument("--output-dir", default="backups", help="Parent directory for backup output (default: backups/)")
    args = parser.parse_args()

    settings = get_settings()
    client = AsyncIOMotorClient(settings.mongodb_url)
    db = client[settings.mongodb_database]

    stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    out_dir = os.path.join(args.output_dir, stamp)
    os.makedirs(out_dir, exist_ok=True)
    logger.info("Database   : %s", settings.mongodb_database)
    logger.info("Backup dir : %s", os.path.abspath(out_dir))

    names = sorted(await db.list_collection_names())
    manifest: dict = {}
    total_bytes = 0

    for name in names:
        # Exclude float64 title_vec from prospects — regenerated as int8 in Stage 4
        exclude = ["title_vec"] if name == "prospects" else None
        note = " (excluding title_vec — will re-embed as int8)" if exclude else ""
        logger.info("Backing up: %s%s ...", name, note)
        try:
            count, size = await backup_collection(db, name, out_dir, exclude_fields=exclude)
            total_bytes += size
            manifest[name] = {"count": count, "file": f"{name}.jsonl.gz", "bytes": size}
            logger.info("  %d docs, %.1f KB", count, size / 1024)
        except Exception as e:
            logger.error("  FAILED: %s", e)
            manifest[name] = {"count": -1, "error": str(e)}

    manifest_path = os.path.join(out_dir, "manifest.json")
    with open(manifest_path, "w") as fh:
        json.dump(
            {"stamp": stamp, "database": settings.mongodb_database, "collections": manifest},
            fh,
            indent=2,
        )

    logger.info("")
    logger.info("=== BACKUP COMPLETE ===")
    logger.info("Output dir : %s", os.path.abspath(out_dir))
    logger.info("Total size : %.1f MB", total_bytes / 1_048_576)
    logger.info("Collections: %d", len(names))
    logger.info("")
    logger.info("Verify the counts above, then run the recovery:")
    logger.info("  python3 scripts/free_and_reimport_prospects.py --backup-dir %s --yes", os.path.abspath(out_dir))

    client.close()


if __name__ == "__main__":
    asyncio.run(main())
