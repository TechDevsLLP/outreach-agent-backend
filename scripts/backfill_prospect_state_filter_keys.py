"""Backfill denormalized `pk` filter keys onto existing prospect_state docs.

GET /api/prospects filters match on prospect_state.pk.* (denormalized at write
time since July 2026 — see utils/prospect_filter_keys.py). State rows created
before that change have no `pk` and are therefore invisible to prospect-level
filters until backfilled.

Idempotent: rebuilding pk from the current prospect doc always converges; safe
to run repeatedly or after interruption. Default is a dry run; pass --apply to
write. State rows whose prospect no longer exists are reported and skipped.

NOTE (July 2026): do NOT run this against production `outflo_v3` yet —
prospect_state is near-empty post-migration; overlays will be created by the
new write paths, which set pk themselves. This script exists for the cutover /
any environment with pre-existing overlay rows.

Usage:
    venv/bin/python scripts/backfill_prospect_state_filter_keys.py            # dry run
    venv/bin/python scripts/backfill_prospect_state_filter_keys.py --apply
    venv/bin/python scripts/backfill_prospect_state_filter_keys.py --apply --all   # also refresh rows that already have pk
"""
import argparse
import asyncio
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

BATCH = 1000


async def main(apply: bool, refresh_all: bool) -> None:
    from bson import ObjectId
    from pymongo import UpdateOne

    import database
    from utils.prospect_filter_keys import PK_PROJECTION, build_filter_keys

    query = {} if refresh_all else {"pk": {"$exists": False}}
    total = await database.prospect_state_collection.count_documents(query)
    print(f"db={database.db.name}  state rows to backfill: {total} "
          f"({'ALL rows' if refresh_all else 'rows missing pk'})  mode={'APPLY' if apply else 'DRY RUN'}")

    updated = missing_prospect = invalid_id = 0
    cursor = database.prospect_state_collection.find(query, {"_id": 1, "prospect_id": 1})
    batch: list[dict] = []

    async def flush(rows: list[dict]) -> None:
        nonlocal updated, missing_prospect, invalid_id
        oid_by_state: dict = {}
        for row in rows:
            try:
                oid_by_state[row["_id"]] = ObjectId(str(row.get("prospect_id")))
            except Exception:
                invalid_id += 1
        prospects = {
            p["_id"]: p
            async for p in database.prospects_collection.find(
                {"_id": {"$in": list(oid_by_state.values())}}, PK_PROJECTION
            )
        }
        ops = []
        for state_id, poid in oid_by_state.items():
            p = prospects.get(poid)
            if p is None:
                missing_prospect += 1
                continue
            ops.append(UpdateOne({"_id": state_id}, {"$set": {"pk": build_filter_keys(p)}}))
        if ops and apply:
            result = await database.prospect_state_collection.bulk_write(ops, ordered=False)
            updated += result.modified_count
        elif ops:
            updated += len(ops)  # would-update count in dry run

    async for row in cursor:
        batch.append(row)
        if len(batch) >= BATCH:
            await flush(batch)
            batch = []
            print(f"  progress: {updated} updated / {missing_prospect} dangling / {invalid_id} invalid ids")
    if batch:
        await flush(batch)

    verb = "updated" if apply else "would update"
    print(f"done: {verb} {updated}; dangling (prospect deleted): {missing_prospect}; invalid prospect_id: {invalid_id}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--apply", action="store_true", help="actually write (default: dry run)")
    parser.add_argument("--all", action="store_true", help="refresh pk on all rows, not just missing")
    args = parser.parse_args()
    asyncio.run(main(args.apply, args.all))
