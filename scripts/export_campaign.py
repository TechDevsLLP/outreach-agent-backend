"""
One-shot export script for an already-enriched campaign.
Runs Phase E (mark_is_primary) + Phase F (export CSV/cost report).
"""
import asyncio
import logging
import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from datetime import datetime
from pathlib import Path
from bson import ObjectId

import database
from scripts.discover_enrich_score import (
    mark_is_primary,
    export_all,
    RESULTS_DIR,
    TARGET_COMPANIES,
    EMAIL,
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("export_campaign")


CAMPAIGN_ID = "6a3bc3af8c0c0517d2748b8a"
ACCOUNT_ID = "6a25c9b135e9ddcaec3d83eb"


async def main() -> None:
    ts = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
    out_dir = RESULTS_DIR / f"des_{ts}"
    out_dir.mkdir(parents=True, exist_ok=True)

    run_meta = {
        "started_at": datetime.utcnow().isoformat(),
        "script": "export_campaign.py",
        "email": EMAIL,
        "target_companies": TARGET_COMPANIES,
        "account_id": ACCOUNT_ID,
        "campaign_id": CAMPAIGN_ID,
    }

    logger.info("=== Phase E: Mark is_primary ===")
    n_companies = await mark_is_primary(CAMPAIGN_ID, ACCOUNT_ID)
    run_meta["distinct_companies"] = n_companies

    campaign_oid = ObjectId(CAMPAIGN_ID)
    account_oid = ObjectId(ACCOUNT_ID)
    total_enrolled = await database.campaign_enrollments_collection.count_documents({
        "campaign_id": campaign_oid,
        "account_id": {"$in": [account_oid, ACCOUNT_ID]},
    })
    run_meta["total_prospects_enrolled"] = total_enrolled
    run_meta["finished_at"] = datetime.utcnow().isoformat()

    logger.info("=== Phase F-G: Export to %s ===", out_dir)
    await export_all(CAMPAIGN_ID, ACCOUNT_ID, run_meta, out_dir)

    logger.info("=" * 60)
    logger.info("DONE")
    logger.info("  Campaign:    %s", CAMPAIGN_ID)
    logger.info("  Companies:   %d / %d target", n_companies, TARGET_COMPANIES)
    logger.info("  Prospects:   %d enrolled", total_enrolled)
    logger.info("  Results:     %s", out_dir)
    logger.info("=" * 60)


if __name__ == "__main__":
    asyncio.run(main())
