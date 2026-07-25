"""Denormalized prospect filter keys ("pk") on prospect_state docs.

GET /api/prospects filters on prospect-level fields (email, industry, country,
seniority, enrichment_status, name/company search). Prospects are global and
account scoping lives on the prospect_state overlay, so filtering the page
slice used to require an O(overlay-size) $lookup pass into `prospects` per
request (~+400 ms at a 2k overlay, linear in overlay size — measured July 2026
on the real 47k pool).

Instead, every place that creates/refreshes a prospect_state doc denormalizes
the filterable fields into a `pk` subdocument so the list aggregation can
$match on them before $skip/$limit at no extra cost.

Keep FILTER_KEY_FIELDS in sync with the filter params of
routes/prospects.py::list_prospects. Volatile fields (email is found later by
GrowthToolkit; enrichment_status advances through the pipeline) are re-synced
by the writers of those prospect fields via sync_filter_keys() /
refresh_filter_key_fields(). Backfill for pre-existing state rows:
scripts/backfill_prospect_state_filter_keys.py.
"""
from typing import Any, Iterable, Optional

# Projection to fetch from `prospects` when a write site only has the id.
PK_PROJECTION = {
    "full_name": 1, "email": 1, "company_name": 1, "job_title": 1,
    "company_industry_id": 1, "location.country_code": 1, "location.country": 1,
    "seniority": 1, "seniority_level": 1, "enrichment_status": 1, "linkedin": 1,
}


def build_filter_keys(prospect: dict) -> dict:
    """Build the `pk` subdocument from a prospect doc (full doc or
    PK_PROJECTION subset). Missing fields become None so filter semantics
    ($in/$nin with null) match querying the prospect doc directly."""
    loc = prospect.get("location") or {}
    return {
        "full_name": prospect.get("full_name"),
        "email": prospect.get("email"),
        "company_name": prospect.get("company_name"),
        "job_title": prospect.get("job_title"),
        "company_industry_id": prospect.get("company_industry_id"),
        "country_code": loc.get("country_code"),
        "country": loc.get("country"),
        "seniority": prospect.get("seniority"),
        "seniority_level": prospect.get("seniority_level"),
        "enrichment_status": prospect.get("enrichment_status"),
        "linkedin": prospect.get("linkedin"),
    }


async def fetch_filter_keys(prospect_id) -> Optional[dict]:
    """Point-read the prospect (PK_PROJECTION) and build its `pk` subdoc.
    Returns None when the prospect doesn't exist / the id is invalid."""
    from bson import ObjectId
    import database

    try:
        oid = prospect_id if isinstance(prospect_id, ObjectId) else ObjectId(str(prospect_id))
    except Exception:
        return None
    doc = await database.prospects_collection.find_one({"_id": oid}, PK_PROJECTION)
    return build_filter_keys(doc) if doc else None


async def sync_filter_keys(prospect_ids: Iterable[Any], fields: dict) -> None:
    """Propagate updated prospect fields to every tenant's prospect_state row.

    `fields` uses prospect-doc key names (e.g. {"email": ..., "enrichment_status": ...});
    location must be passed pre-flattened as country_code/country. Best-effort:
    callers already treat overlay sync as non-critical.
    """
    import database

    pid_strs = [str(p) for p in prospect_ids]
    if not pid_strs or not fields:
        return
    set_fields = {f"pk.{k}": v for k, v in fields.items()}
    await database.prospect_state_collection.update_many(
        {"prospect_id": {"$in": pid_strs}}, {"$set": set_fields}
    )


async def resync_filter_keys_from_db(prospect_ids: Iterable[Any]) -> int:
    """Re-read the given prospects and refresh `pk` on every existing
    prospect_state row (all tenants). Authoritative resync used at the end of
    enrichment runs, whose many per-phase writes (enrichment_status
    transitions, found emails, company industry) would otherwise leave
    denormalized keys stale. Returns the number of state rows updated."""
    from bson import ObjectId
    from pymongo import UpdateMany
    import database

    oids = []
    for p in prospect_ids:
        try:
            oids.append(p if isinstance(p, ObjectId) else ObjectId(str(p)))
        except Exception:
            continue
    if not oids:
        return 0

    ops = []
    async for doc in database.prospects_collection.find({"_id": {"$in": oids}}, PK_PROJECTION):
        ops.append(UpdateMany(
            {"prospect_id": str(doc["_id"])},
            {"$set": {"pk": build_filter_keys(doc)}},
        ))
    if not ops:
        return 0
    result = await database.prospect_state_collection.bulk_write(ops, ordered=False)
    return result.modified_count
