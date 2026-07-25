"""
Backfill sender voice profiles for accounts that connected LinkedIn but never
got a voice sync (Bug A: get_user_posts silently returned [] before the
provider_id fix, leaving sender_voice_profile missing or low-confidence).

Dry-run by default — pass --apply to actually sync.

Usage:
    venv/bin/python -m scripts.backfill_sender_voice            # dry run
    venv/bin/python -m scripts.backfill_sender_voice --apply    # perform syncs
    venv/bin/python -m scripts.backfill_sender_voice --apply --account <account_id>
"""

import argparse
import asyncio
import logging
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")
logger = logging.getLogger("backfill_sender_voice")


async def main(apply: bool, only_account: str | None) -> None:
    import database
    from services.sender_voice_service import update_sender_voice_from_unipile

    # Candidate accounts: have a LinkedIn account connected
    query: dict = {}
    if only_account:
        query["account_id"] = only_account
    account_ids = await database.linkedin_accounts_collection.distinct("account_id", query)
    logger.info(f"Found {len(account_ids)} account(s) with a connected LinkedIn account")

    synced, skipped, failed = 0, 0, 0
    for account_id in account_ids:
        account_id = str(account_id)
        profile = await database.company_profiles_collection.find_one(
            {"account_id": account_id},
            {"sender_voice_profile": 1, "sender_voice_sync_error": 1},
        )
        vp = (profile or {}).get("sender_voice_profile") or {}
        needs_sync = (not vp) or vp.get("low_confidence") or not vp.get("post_count")
        if not needs_sync:
            skipped += 1
            logger.info(f"  skip  account={account_id} (healthy voice profile, posts={vp.get('post_count')})")
            continue

        if not apply:
            synced += 1
            logger.info(f"  WOULD sync account={account_id} "
                        f"(profile={'missing' if not vp else 'low-confidence/no posts'})")
            continue

        try:
            result = await update_sender_voice_from_unipile(account_id)
            ok = bool(result.get("voice_profile"))
            synced += 1 if ok else 0
            failed += 0 if ok else 1
            logger.info(f"  {'synced' if ok else 'FAILED'} account={account_id} "
                        f"low_confidence={result.get('low_confidence')}")
        except Exception as e:
            failed += 1
            logger.error(f"  FAILED account={account_id}: {e}")

    mode = "applied" if apply else "dry-run"
    logger.info(f"Done ({mode}): {synced} synced, {skipped} skipped, {failed} failed")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Backfill sender voice profiles from Unipile")
    parser.add_argument("--apply", action="store_true", help="Actually perform syncs (default: dry run)")
    parser.add_argument("--account", default=None, help="Limit to a single account_id")
    args = parser.parse_args()
    asyncio.run(main(apply=args.apply, only_account=args.account))
