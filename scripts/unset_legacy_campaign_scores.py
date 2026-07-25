"""Opt-in, one-time-safe cleanup of legacy campaign-score fields on the shared pool.

Canonical scoring is campaign-scoped and versioned, living in
``campaign_prospect_state`` / ``prospect_state`` (keyed by
``account_id + campaign_id + prospect_id + scoring_version``).  The shared
``prospects`` collection is tenant-neutral and must NOT carry per-campaign or
per-tenant score copies.

Historically some campaign-score fields leaked onto shared prospect documents
(see ``scripts/shared_pool_migration.py`` ``CAMPAIGN_FIELDS``).  This helper
unsets exactly those legacy score fields from ``prospects`` and nothing else.

Safety properties
-----------------
* **Opt-in**: dry-run by default; mutation only with ``--execute``.
* **Idempotent**: the filter matches only documents that still carry at least
  one legacy field, so a second run is a no-op (0 matched).
* **Scoped**: refuses to touch any collection other than ``prospects`` — it can
  never run against a provider/credential collection.
* **Field-bounded**: only the fields in :data:`LEGACY_CAMPAIGN_SCORE_FIELDS`
  are unset; canonical identity fields, embeddings, and the tenant overlay
  collections are never touched.

Invocation
----------
Dry run (default — reports how many prospects still carry legacy fields)::

    MONGODB_URL=... MONGODB_DATABASE=outflo_v3 \
        python -m scripts.unset_legacy_campaign_scores

Execute the unset::

    MONGODB_URL=... MONGODB_DATABASE=outflo_v3 \
        python -m scripts.unset_legacy_campaign_scores --execute

The command reads the same ``MONGODB_URL`` / ``MONGODB_DATABASE`` env vars the
app uses (via ``config.get_settings()``); it never writes to any collection
other than ``prospects``.
"""

from __future__ import annotations

import argparse
import asyncio
import sys
from typing import Iterable

# The canonical target collection. The helper hard-refuses anything else so it
# can never be pointed at a provider/credential or tenant-overlay collection.
TARGET_COLLECTION = "prospects"

# Legacy campaign-specific score fields that must not live on the tenant-neutral
# shared pool. Verified against readers/writers across the backend: after the
# campaign ranking/message-gen readers were migrated to prospect_state, nothing
# reads these off `prospects` except dead legacy-schema fallback branches that
# already `$ifNull`-default them. Kept in sync with the campaign score subset of
# scripts/shared_pool_migration.py::CAMPAIGN_FIELDS.
LEGACY_CAMPAIGN_SCORE_FIELDS: tuple[str, ...] = (
    "ai_prospect_score",
    "ai_match_score",
    "fit_score",
    "fit_score_breakdown",
    "campaign_fit_score",
    "campaign_rule_score",
    "last_campaign_rule_score",
    "campaign_score",
    "campaign_fit",
    "score_reason",
    "scoring_version",
)


def legacy_unset_filter(fields: Iterable[str] = LEGACY_CAMPAIGN_SCORE_FIELDS) -> dict:
    """Match only documents that still carry at least one legacy score field.

    This makes the operation idempotent: once the fields are gone the filter
    matches nothing on subsequent runs.
    """
    return {"$or": [{field: {"$exists": True}} for field in fields]}


def legacy_unset_update(fields: Iterable[str] = LEGACY_CAMPAIGN_SCORE_FIELDS) -> dict:
    """Build the ``$unset`` update for the legacy score fields."""
    return {"$unset": {field: "" for field in fields}}


async def unset_legacy_campaign_scores(collection, *, execute: bool = False) -> dict:
    """Unset legacy campaign-score fields from the shared ``prospects`` pool.

    Returns a report dict. In dry-run mode (``execute=False``) nothing is
    written and ``modified`` is ``None``.

    Raises ``ValueError`` if pointed at any collection other than ``prospects``
    — this guarantees the helper can never run against providers.
    """
    name = getattr(collection, "name", None)
    if name != TARGET_COLLECTION:
        raise ValueError(
            f"refusing to run against collection {name!r}; only "
            f"{TARGET_COLLECTION!r} is permitted"
        )

    query = legacy_unset_filter()
    matched = await collection.count_documents(query)
    report = {
        "collection": name,
        "fields": list(LEGACY_CAMPAIGN_SCORE_FIELDS),
        "matched": matched,
        "modified": None,
        "executed": bool(execute),
    }
    if not execute or matched == 0:
        return report

    result = await collection.update_many(query, legacy_unset_update())
    report["modified"] = getattr(result, "modified_count", None)
    return report


async def _main_async(execute: bool) -> int:
    import database

    report = await unset_legacy_campaign_scores(
        database.prospects_collection, execute=execute
    )
    mode = "EXECUTE" if execute else "DRY RUN"
    print(f"[{mode}] collection={report['collection']}")
    print(f"  prospects still carrying legacy score fields: {report['matched']}")
    print(f"  fields: {', '.join(report['fields'])}")
    if execute:
        print(f"  documents modified: {report['modified']}")
    else:
        print("  no changes written; re-run with --execute to unset")
    return 0


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--execute",
        action="store_true",
        help="actually unset the fields (default is a dry run)",
    )
    args = parser.parse_args(argv)
    return asyncio.run(_main_async(args.execute))


if __name__ == "__main__":
    sys.exit(main())
