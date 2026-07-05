"""
migrate_to_shared_schema.py
===========================
One-time migration to the shared canonical company/prospect schema.

Run with: python scripts/migrate_to_shared_schema.py [--stage STAGE] [--dry-run]

Stages:
  0  Build reference data (geo_places from GeoNames, industries_taxonomy from taxonomy)
  1  Transform companies (resolve geo + canonical industry, move linkedin_data, reserve website_data)
  2  Merge employees -> prospects + transform existing prospects
  3  Extract per-tenant overlays (prospect_state) from existing account_id fields
  4  Embedding backfill (title_vec on prospects, profile_vec on companies) — COSTS MONEY
  5  Drop legacy collections (employees, leads, campaign data, daily stats)

Safety:
  - Each stage is idempotent: re-runs skip already-processed records.
  - Run on a COPY of the DB first (mongorestore into a test cluster).
  - Stage 5 does a mongodump (via subprocess) BEFORE dropping anything.
  - Use --dry-run to preview counts without writing.

Usage examples:
  python scripts/migrate_to_shared_schema.py --stage 0
  python scripts/migrate_to_shared_schema.py --stage 1 --dry-run
  python scripts/migrate_to_shared_schema.py --stage 4 --batch-size 500
  python scripts/migrate_to_shared_schema.py --stage 5  # DESTRUCTIVE — drops collections
"""

import argparse
import asyncio
import logging
import os
import subprocess
import sys
from datetime import datetime
from pathlib import Path
from typing import Optional

from bson import ObjectId
from motor.motor_asyncio import AsyncIOMotorClient
from pymongo import UpdateOne

# Add backend root to path so services can be imported
sys.path.insert(0, str(Path(__file__).parent.parent))

from config import get_settings
from services.geo_resolver import resolve as geo_resolve, load_geonames_to_db
from services.industry_canonicalizer import (
    build_taxonomy_in_db,
    resolve as industry_resolve,
    _load_taxonomy as load_taxonomy_cache,
)
from services.embedding_service import (
    embed_texts,
    build_prospect_title_text,
    build_company_profile_text,
)

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger("migrate")


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _norm_account_id(raw) -> Optional[str]:
    """
    Normalize account_id to a string representation of ObjectId.
    Handles the known ObjectId/string inconsistency in the old schema.
    Returns None if not resolvable.
    """
    if raw is None:
        return None
    if isinstance(raw, ObjectId):
        return str(raw)
    if isinstance(raw, str) and len(raw) == 24:
        try:
            return str(ObjectId(raw))
        except Exception:
            return raw  # keep as-is if not a valid ObjectId hex
    return str(raw)


def _employee_band(count: Optional[int]) -> Optional[str]:
    """Map employee count to a size band string."""
    if count is None:
        return None
    if count <= 10:
        return "1-10"
    if count <= 50:
        return "11-50"
    if count <= 200:
        return "51-200"
    if count <= 1000:
        return "201-1000"
    return "1000+"


def _band_from_estimate(estimate: Optional[str]) -> Optional[str]:
    """Map Apify employee_size_estimate string to our band format."""
    if not estimate:
        return None
    e = estimate.lower().strip()
    mapping = {
        "1-10": "1-10", "2-10": "1-10", "1-50": "1-10",
        "11-50": "11-50", "10-50": "11-50",
        "51-200": "51-200", "50-200": "51-200",
        "201-1000": "201-1000", "200-1000": "201-1000",
        "1000+": "1000+", "1001+": "1000+", "1000-5000": "1000+",
    }
    return mapping.get(e, estimate[:10] if estimate else None)


# ---------------------------------------------------------------------------
# Stage 0: Build reference data
# ---------------------------------------------------------------------------

async def stage0_build_reference_data(db, *, dry_run: bool = False, geonames_dir: Optional[str] = None) -> None:
    """
    Stage 0: Build geo_places and industries_taxonomy collections.

    geo_places requires GeoNames data files. Download from:
      https://download.geonames.org/export/dump/cities500.zip  (unzip to get cities500.txt)
      https://download.geonames.org/export/dump/admin1CodesASCII.txt

    Pass --geonames-dir to specify where these files are located.
    If not provided, only the industries_taxonomy is built.
    """
    logger.info("=== Stage 0: Building reference data ===")

    # Build industries_taxonomy
    taxonomy_col = db["industries_taxonomy"]
    count = await taxonomy_col.count_documents({})
    if count >= 80:
        logger.info("industries_taxonomy already has %d entries, skipping build", count)
    else:
        logger.info("Building industries_taxonomy (%d existing)...", count)
        if not dry_run:
            # Load taxonomy cache from DB (or static list if DB empty)
            n = await build_taxonomy_in_db(taxonomy_col)
            logger.info("  industries_taxonomy: %d entries upserted", n)
        else:
            logger.info("  [dry-run] would upsert ~82 taxonomy entries")

    # Build geo_places (requires GeoNames files)
    geo_col = db["geo_places"]
    geo_count = await geo_col.count_documents({})

    cities_file = None
    admin1_file = None
    if geonames_dir:
        cities_file = os.path.join(geonames_dir, "cities500.txt")
        admin1_file = os.path.join(geonames_dir, "admin1CodesASCII.txt")
        if not os.path.exists(cities_file):
            logger.warning("cities500.txt not found at %s — skipping geo_places build", cities_file)
            cities_file = None
        if not os.path.exists(admin1_file):
            logger.warning("admin1CodesASCII.txt not found at %s — skipping geo_places build", admin1_file)
            admin1_file = None

    if cities_file and admin1_file:
        logger.info("Loading GeoNames from %s...", geonames_dir)
        if not dry_run:
            result = await load_geonames_to_db(cities_file, admin1_file, geo_col)
            logger.info("  geo_places: %s", result)
        else:
            logger.info("  [dry-run] would load GeoNames cities500.txt into geo_places")
    else:
        logger.info(
            "Skipping geo_places build (pass --geonames-dir with cities500.txt + admin1CodesASCII.txt). "
            "geo_places currently has %d entries.",
            geo_count,
        )

    logger.info("Stage 0 done.")


# ---------------------------------------------------------------------------
# Stage 1: Transform companies
# ---------------------------------------------------------------------------

async def stage1_transform_companies(db, *, dry_run: bool = False, limit: Optional[int] = None) -> None:
    """
    Stage 1: Transform existing company docs to the new shared schema.

    For each company:
    - Resolve raw headquarters/locations -> location{} + geo GeoJSON
    - Map raw industry string -> canonical industry{} sub-doc
    - Move any inline data to linkedin_data (raw blob)
    - Reserve website_data = None
    - Mark company as migrated with migration_v2_at timestamp
    """
    logger.info("=== Stage 1: Transforming companies ===")
    companies_col = db["companies"]
    geo_col = db["geo_places"]
    taxonomy_col = db["industries_taxonomy"]

    # Pre-load taxonomy cache
    await load_taxonomy_cache(taxonomy_col)

    # Only process companies not yet migrated
    query: dict = {"migration_v2_at": {"$exists": False}}
    total = await companies_col.count_documents(query)
    logger.info("Companies to migrate: %d", total)

    if dry_run:
        logger.info("[dry-run] would transform %d companies", total)
        return

    processed = 0
    errors = 0
    geo_col_ref = geo_col if await geo_col.count_documents({}) > 0 else None

    cursor = companies_col.find(query)
    if limit:
        cursor = cursor.limit(limit)

    async for company in cursor:
        company_id = company["_id"]
        updates: dict = {}

        # --- Canonical industry ---
        raw_industry = company.get("industry")
        if raw_industry and isinstance(raw_industry, str):
            try:
                resolved = await industry_resolve(raw_industry, collection=taxonomy_col)
                if resolved:
                    updates["industry"] = resolved
                else:
                    updates["industry"] = {
                        "id": None, "label": raw_industry, "group": None,
                        "raw": raw_industry, "assign_method": "unresolved", "confidence": None,
                    }
            except Exception as e:
                logger.debug("industry resolve failed for %s: %s", company_id, e)
                updates["industry"] = {
                    "id": None, "label": raw_industry, "group": None,
                    "raw": raw_industry, "assign_method": "error", "confidence": None,
                }
        elif raw_industry and isinstance(raw_industry, dict):
            # Already in new format
            updates["industry"] = raw_industry

        # --- Canonical location ---
        raw_hq = company.get("headquarters") or (
            (company.get("locations") or [None])[0]
        )
        if raw_hq:
            try:
                loc = await geo_resolve(raw_hq, collection=geo_col_ref)
                if loc:
                    updates["location"] = loc
                    if loc.get("lat") and loc.get("lng"):
                        updates["geo"] = {"type": "Point", "coordinates": [loc["lng"], loc["lat"]]}
                    elif "lat" not in loc:
                        updates["location"] = {k: v for k, v in loc.items() if k != "lat" and k != "lng"}
                else:
                    updates["location"] = {"raw": raw_hq}
            except Exception as e:
                logger.debug("geo resolve failed for company %s: %s", company_id, e)
                updates["location"] = {"raw": raw_hq}

        # --- linkedin_data: move inline blob ---
        # Raw company scrape data may be stored as last_enrichment_result
        last_result = company.get("last_enrichment_result")
        if last_result and not company.get("linkedin_data"):
            updates["linkedin_data"] = last_result

        # --- employee_band: derive from employee_count ---
        if not company.get("employee_band"):
            band = _employee_band(company.get("employee_count"))
            if band:
                updates["employee_band"] = band

        # --- Reserve website_data slot ---
        if "website_data" not in company:
            updates["website_data"] = None

        # --- Mark migrated ---
        updates["migration_v2_at"] = datetime.utcnow()
        updates["last_updated_at"] = datetime.utcnow()

        try:
            await companies_col.update_one({"_id": company_id}, {"$set": updates})
            processed += 1
        except Exception as e:
            logger.error("Failed to update company %s: %s", company_id, e)
            errors += 1

        if processed % 100 == 0:
            logger.info("  Companies: %d / %d processed, %d errors", processed, total, errors)

    logger.info("Stage 1 done. Processed: %d, Errors: %d", processed, errors)


# ---------------------------------------------------------------------------
# Stage 2: Merge employees -> prospects + transform existing prospects
# ---------------------------------------------------------------------------

async def stage2_merge_people(db, *, dry_run: bool = False, limit: Optional[int] = None) -> None:
    """
    Stage 2a: Merge employees into prospects collection.
    Stage 2b: Transform existing prospect docs to the new shared schema.
    """
    logger.info("=== Stage 2: Merging people (employees -> prospects + prospect transform) ===")
    prospects_col = db["prospects"]
    employees_col = db["employees"]
    companies_col = db["companies"]
    geo_col = db["geo_places"]
    taxonomy_col = db["industries_taxonomy"]

    await load_taxonomy_cache(taxonomy_col)
    geo_col_ref = geo_col if await geo_col.count_documents({}) > 0 else None

    # --- 2a: Employees -> Prospects ---
    emp_total = await employees_col.count_documents({"prospect_id": {"$exists": False}})
    logger.info("Employees to merge into prospects: %d", emp_total)

    emp_processed = 0
    emp_errors = 0

    if not dry_run:
        cursor = employees_col.find({"prospect_id": {"$exists": False}})
        if limit:
            cursor = cursor.limit(limit)

        async for emp in cursor:
            emp_id = emp["_id"]

            # Resolve company facts from the companies collection
            company_facts: dict = {}
            company_id_ref = emp.get("company_id")
            if company_id_ref:
                try:
                    company = await companies_col.find_one(
                        {"_id": company_id_ref},
                        {"industry": 1, "location": 1, "employee_count": 1, "employee_band": 1,
                         "domain": 1, "linkedin_url": 1, "name": 1},
                    )
                    if company:
                        ind = company.get("industry", {})
                        if isinstance(ind, dict):
                            company_facts["company_industry_id"] = ind.get("id")
                            company_facts["company_industry_group"] = ind.get("group")
                        loc = company.get("location", {})
                        if isinstance(loc, dict):
                            company_facts["location"] = loc
                        company_facts["company_employee_band"] = (
                            company.get("employee_band")
                            or _employee_band(company.get("employee_count"))
                        )
                        company_facts["company_size"] = company.get("employee_count")
                        company_facts["company_domain"] = company.get("domain")
                        company_facts["company_linkedin"] = company.get("linkedin_url")
                        company_facts["company_name"] = company.get("name")
                except Exception as e:
                    logger.debug("Company lookup failed for employee %s: %s", emp_id, e)

            # Get person location from raw string
            raw_loc = emp.get("location")
            if raw_loc and not company_facts.get("location"):
                try:
                    loc = await geo_resolve(raw_loc, collection=geo_col_ref)
                    if loc:
                        company_facts["location"] = loc
                        if loc.get("lat") and loc.get("lng"):
                            company_facts["geo"] = {
                                "type": "Point",
                                "coordinates": [loc["lng"], loc["lat"]],
                            }
                except Exception:
                    pass

            # Map canonical seniority from current_position
            pos = emp.get("current_position") or {}
            title = pos.get("title") or ""

            # Build prospect doc from employee
            now = datetime.utcnow()
            prospect_doc = {
                "linkedin": emp.get("linkedin_url"),
                "linkedin_uid": emp.get("linkedin_id"),
                "full_name": emp.get("full_name"),
                "first_name": emp.get("first_name"),
                "last_name": emp.get("last_name"),
                "job_title": title,
                "company_id": str(company_id_ref) if company_id_ref else None,
                "company_name": emp.get("company_name") or company_facts.get("company_name"),
                "stage": "scraped",
                "enrichment_status": "not_started",
                "source": "employee_discovery",
                "migration_v2_at": now,
                "first_seen_at": emp.get("created_at", now),
                "last_updated_at": now,
                **company_facts,
            }

            # Upsert by linkedin URL (dedup)
            linkedin = prospect_doc.get("linkedin")
            if not linkedin:
                # No linkedin = can't dedup properly; insert if no email match either
                emp_processed += 1
                continue

            try:
                # $setOnInsert and $set cannot share keys — strip the timestamp fields
                insert_doc = {k: v for k, v in prospect_doc.items() if k not in ("last_updated_at", "migration_v2_at")}
                result = await prospects_col.update_one(
                    {"linkedin": linkedin},
                    {
                        "$setOnInsert": insert_doc,
                        "$set": {"last_updated_at": now, "migration_v2_at": now},
                    },
                    upsert=True,
                )
                # Mark employee as migrated with prospect_id
                if result.upserted_id:
                    await employees_col.update_one(
                        {"_id": emp_id},
                        {"$set": {"prospect_id": result.upserted_id, "migrated_v2": True}},
                    )
                else:
                    # Existing prospect — link it
                    existing = await prospects_col.find_one({"linkedin": linkedin}, {"_id": 1})
                    if existing:
                        await employees_col.update_one(
                            {"_id": emp_id},
                            {"$set": {"prospect_id": existing["_id"], "migrated_v2": True}},
                        )
                emp_processed += 1
            except Exception as e:
                logger.error("Failed to migrate employee %s: %s", emp_id, e)
                emp_errors += 1

            if emp_processed % 200 == 0:
                logger.info("  Employees: %d processed, %d errors", emp_processed, emp_errors)
    else:
        logger.info("[dry-run] would merge %d employees", emp_total)

    logger.info("Stage 2a (employees) done. Processed: %d, Errors: %d", emp_processed, emp_errors)

    # --- 2b: Transform existing prospects ---
    pros_query: dict = {"migration_v2_at": {"$exists": False}}
    pros_total = await prospects_col.count_documents(pros_query)
    logger.info("Existing prospects to transform: %d", pros_total)

    if dry_run:
        logger.info("[dry-run] would transform %d prospects", pros_total)
        return

    pros_processed = 0
    pros_errors = 0

    cursor = prospects_col.find(pros_query)
    if limit:
        cursor = cursor.limit(limit)

    async for prospect in cursor:
        pid = prospect["_id"]
        updates: dict = {}

        # Denormalize company industry/location from the companies collection
        comp_linkedin = prospect.get("company_linkedin")
        comp_id = prospect.get("company_id")
        if comp_id or comp_linkedin:
            try:
                comp_query = (
                    {"_id": ObjectId(comp_id)} if comp_id
                    else {"linkedin_url": comp_linkedin}
                )
                company = await companies_col.find_one(
                    comp_query,
                    {"industry": 1, "location": 1, "employee_count": 1, "employee_band": 1,
                     "domain": 1, "linkedin_url": 1, "name": 1},
                )
                if company:
                    ind = company.get("industry", {})
                    if isinstance(ind, dict):
                        updates["company_industry_id"] = ind.get("id")
                        updates["company_industry_group"] = ind.get("group")
                    loc = company.get("location", {})
                    if isinstance(loc, dict):
                        # Use company location as fallback for person location
                        if not prospect.get("location"):
                            updates["location"] = loc
                    updates["company_employee_band"] = (
                        company.get("employee_band")
                        or _employee_band(company.get("employee_count"))
                    )
                    if not updates.get("company_id"):
                        updates["company_id"] = str(company["_id"])
            except Exception as e:
                logger.debug("Company lookup for prospect %s: %s", pid, e)

        # Move company_linkedin_data to company doc (if company linked)
        company_linkedin_data = prospect.get("company_linkedin_data")
        if company_linkedin_data and (comp_linkedin or comp_id):
            try:
                comp_q = (
                    {"_id": ObjectId(comp_id)} if comp_id
                    else {"linkedin_url": comp_linkedin}
                )
                await companies_col.update_one(
                    comp_q,
                    {"$set": {"linkedin_data": company_linkedin_data, "last_updated_at": datetime.utcnow()}},
                )
            except Exception:
                pass
            updates["company_linkedin_data"] = None  # clear from prospect

        # Resolve person location from flat city/state/country fields
        if not prospect.get("location"):
            city = prospect.get("city")
            state = prospect.get("state")
            country = prospect.get("country")
            raw_loc = ", ".join(filter(None, [city, state, country]))
            if raw_loc:
                try:
                    loc = await geo_resolve(raw_loc, collection=geo_col_ref)
                    if loc:
                        updates["location"] = loc
                        if loc.get("lat") and loc.get("lng"):
                            updates["geo"] = {
                                "type": "Point",
                                "coordinates": [loc["lng"], loc["lat"]],
                            }
                except Exception:
                    pass

        # Map legacy seniority_level -> canonical seniority
        if not prospect.get("seniority"):
            legacy = prospect.get("seniority_level")
            if legacy:
                updates["seniority"] = legacy.lower().replace(" ", "_").replace("-", "_")

        # Map legacy functional_level -> canonical function
        if not prospect.get("function"):
            func = prospect.get("functional_level")
            if func:
                updates["function"] = func.lower().replace(" ", "_")

        # Set lifecycle stage (default "scraped"; promote based on enrichment_status)
        if not prospect.get("stage"):
            es = prospect.get("enrichment_status", "not_started")
            if es == "completed":
                updates["stage"] = "assessed"
            elif es in ("profile_scraped", "company_scraped", "ai_assessed"):
                updates["stage"] = "company_enriched"
            elif es in ("in_progress", "not_started", "failed"):
                updates["stage"] = "scraped"
            else:
                updates["stage"] = "scraped"

        # Company size band
        if not prospect.get("company_employee_band") and not updates.get("company_employee_band"):
            band = _employee_band(prospect.get("company_size"))
            if band:
                updates["company_employee_band"] = band

        updates["migration_v2_at"] = datetime.utcnow()
        updates["last_updated_at"] = datetime.utcnow()

        try:
            await prospects_col.update_one({"_id": pid}, {"$set": updates})
            pros_processed += 1
        except Exception as e:
            logger.error("Failed to transform prospect %s: %s", pid, e)
            pros_errors += 1

        if pros_processed % 200 == 0:
            logger.info("  Prospects: %d / %d processed, %d errors", pros_processed, pros_total, pros_errors)

    logger.info("Stage 2b (prospects) done. Processed: %d, Errors: %d", pros_processed, pros_errors)


# ---------------------------------------------------------------------------
# Stage 3: Extract per-tenant overlays (prospect_state)
# ---------------------------------------------------------------------------

async def stage3_extract_overlays(db, *, dry_run: bool = False, limit: Optional[int] = None) -> None:
    """
    Stage 3: Extract ICP-relative fields from existing prospects into prospect_state overlays.

    For each old prospect with an account_id, create a prospect_state doc keyed
    (account_id, prospect_id) containing:
      - ai_score (from ai_prospect_score)
      - ai_score_breakdown
      - prospect_intelligence
      - outreach_messages
      - status (from prospect.status)
      - tags
      - company_news, competitors, posts
    used_by starts EMPTY (fresh campaign slate).
    """
    logger.info("=== Stage 3: Extracting per-tenant overlays ===")
    prospects_col = db["prospects"]
    prospect_state_col = db["prospect_state"]

    # Find prospects that have an account_id (old multi-tenant field)
    query: dict = {"account_id": {"$exists": True}, "overlay_extracted": {"$exists": False}}
    total = await prospects_col.count_documents(query)
    logger.info("Prospects with account_id to extract overlays for: %d", total)

    if dry_run:
        logger.info("[dry-run] would extract %d overlays", total)
        return

    processed = 0
    errors = 0
    now = datetime.utcnow()

    cursor = prospects_col.find(query)
    if limit:
        cursor = cursor.limit(limit)

    batch: list = []

    async for prospect in cursor:
        pid = prospect["_id"]
        raw_account_id = prospect.get("account_id")
        account_id_str = _norm_account_id(raw_account_id)

        if not account_id_str:
            # No valid account_id — skip overlay creation
            await prospects_col.update_one({"_id": pid}, {"$set": {"overlay_extracted": True}})
            continue

        # Build the overlay doc (ICP-relative fields)
        overlay: dict = {
            "account_id": account_id_str,  # ALWAYS string (normalized)
            "prospect_id": str(pid),
            "ai_score": prospect.get("ai_prospect_score") or prospect.get("enhanced_score"),
            "ai_score_breakdown": prospect.get("ai_score_breakdown"),
            "prospect_score": prospect.get("prospect_score"),
            "priority_tier": prospect.get("priority_tier"),
            "prospect_intelligence": prospect.get("prospect_intelligence"),
            "outreach_messages": prospect.get("outreach_messages"),
            "company_news": prospect.get("company_news") or [],
            "competitors": prospect.get("competitors") or [],
            "posts": prospect.get("posts") or [],
            "status": prospect.get("status") or "new",
            "tags": prospect.get("tags") or [],
            "used_by": [],  # empty — fresh campaign slate (decision 8)
            "created_at": prospect.get("first_seen_at") or now,
            "last_updated_at": now,
        }

        batch.append(
            UpdateOne(
                {"account_id": account_id_str, "prospect_id": str(pid)},
                {"$setOnInsert": overlay},
                upsert=True,
            )
        )

        if len(batch) >= 500:
            try:
                await prospect_state_col.bulk_write(batch, ordered=False)
                # Mark these prospects as extracted
                pids = [op._filter["prospect_id"] for op in batch]
                await prospects_col.update_many(
                    {"_id": {"$in": [ObjectId(p) for p in pids if len(p) == 24]}},
                    {"$set": {"overlay_extracted": True}},
                )
                processed += len(batch)
                batch = []
                logger.info("  Overlays: %d processed", processed)
            except Exception as e:
                logger.error("Batch overlay upsert failed: %s", e)
                errors += len(batch)
                batch = []

    if batch:
        try:
            await prospect_state_col.bulk_write(batch, ordered=False)
            pids = [op._filter["prospect_id"] for op in batch]
            await prospects_col.update_many(
                {"_id": {"$in": [ObjectId(p) for p in pids if len(p) == 24]}},
                {"$set": {"overlay_extracted": True}},
            )
            processed += len(batch)
        except Exception as e:
            logger.error("Final batch overlay upsert failed: %s", e)
            errors += len(batch)

    logger.info("Stage 3 done. Overlays created: %d, Errors: %d", processed, errors)


# ---------------------------------------------------------------------------
# Stage 4: Embedding backfill
# ---------------------------------------------------------------------------

async def stage4_embed_backfill(
    db,
    *,
    dry_run: bool = False,
    batch_size: int = 250,
    max_companies: Optional[int] = None,
    max_prospects: Optional[int] = None,
) -> None:
    """
    Stage 4: Backfill title_vec on prospects and profile_vec on companies.

    Processes in batches of `batch_size` (max 250 per Gemini API call).
    Skips docs that already have a vec (checkpoint = *_vec_at field present).
    Embeds contactable-stage prospects first.

    COST WARNING: Each batch of 250 = 1 Gemini embed_content call.
    Budget accordingly. Re-run after partial failures — already-embedded docs are skipped.
    """
    logger.info("=== Stage 4: Embedding backfill ===")
    prospects_col = db["prospects"]
    companies_col = db["companies"]
    now = datetime.utcnow()

    # --- Companies: profile_vec ---
    comp_query = {"profile_vec_at": {"$exists": False}, "name": {"$exists": True}}
    comp_total = await companies_col.count_documents(comp_query)
    if max_companies:
        comp_total = min(comp_total, max_companies)
    logger.info("Companies needing profile_vec: %d", comp_total)

    if dry_run:
        logger.info("[dry-run] would embed %d companies", comp_total)
    else:
        comp_processed = 0
        comp_errors = 0
        cursor = companies_col.find(comp_query)
        if max_companies:
            cursor = cursor.limit(max_companies)

        batch_docs: list = []
        batch_texts: list = []

        async def _flush_company_batch(docs: list, texts: list) -> tuple[int, int]:
            vecs = await embed_texts(texts)
            ok = 0
            err = 0
            for doc, vec in zip(docs, vecs):
                if vec:
                    try:
                        await companies_col.update_one(
                            {"_id": doc["_id"]},
                            {"$set": {
                                "profile_vec": vec,
                                "profile_vec_text": build_company_profile_text(doc),
                                "profile_vec_model": "gemini-embedding-001",
                                "profile_vec_at": now,
                            }},
                        )
                        ok += 1
                    except Exception as e:
                        logger.error("Company vec write failed %s: %s", doc["_id"], e)
                        err += 1
                else:
                    err += 1
            return ok, err

        async for company in cursor:
            text = build_company_profile_text(company)
            if not text.strip():
                continue
            batch_docs.append(company)
            batch_texts.append(text)

            if len(batch_docs) >= batch_size:
                ok, err = await _flush_company_batch(batch_docs, batch_texts)
                comp_processed += ok
                comp_errors += err
                batch_docs, batch_texts = [], []
                logger.info("  Companies embedded: %d / ~%d", comp_processed, comp_total)

        if batch_docs:
            ok, err = await _flush_company_batch(batch_docs, batch_texts)
            comp_processed += ok
            comp_errors += err

        logger.info("Companies: embedded %d, errors %d", comp_processed, comp_errors)

    # --- Prospects: title_vec ---
    # Embed contactable stages first (highest value)
    pros_stages_priority = [
        ["contactable", "assessed"],
        ["company_enriched", "profile_enriched"],
        ["scraped"],
    ]

    for stage_group in pros_stages_priority:
        pros_query = {
            "title_vec_at": {"$exists": False},
            "stage": {"$in": stage_group},
        }
        pros_total = await prospects_col.count_documents(pros_query)
        if max_prospects:
            pros_total = min(pros_total, max_prospects)
        logger.info("Prospects (stage=%s) needing title_vec: %d", stage_group, pros_total)

        if dry_run:
            logger.info("[dry-run] would embed %d prospects (stage=%s)", pros_total, stage_group)
            continue

        if pros_total == 0:
            continue

        pros_processed = 0
        pros_errors = 0
        cursor = prospects_col.find(pros_query)
        if max_prospects:
            cursor = cursor.limit(max_prospects)

        batch_docs_p: list = []
        batch_texts_p: list = []

        async def _flush_prospect_batch(docs: list, texts: list) -> tuple[int, int]:
            vecs = await embed_texts(texts)
            ok = 0
            err = 0
            for doc, vec in zip(docs, vecs):
                if vec:
                    try:
                        await prospects_col.update_one(
                            {"_id": doc["_id"]},
                            {"$set": {
                                "title_vec": vec,
                                "title_vec_text": build_prospect_title_text(doc),
                                "title_vec_model": "gemini-embedding-001",
                                "title_vec_at": now,
                            }},
                        )
                        ok += 1
                    except Exception as e:
                        logger.error("Prospect vec write failed %s: %s", doc["_id"], e)
                        err += 1
                else:
                    err += 1
            return ok, err

        async for prospect in cursor:
            text = build_prospect_title_text(prospect)
            if not text.strip():
                continue
            batch_docs_p.append(prospect)
            batch_texts_p.append(text)

            if len(batch_docs_p) >= batch_size:
                ok, err = await _flush_prospect_batch(batch_docs_p, batch_texts_p)
                pros_processed += ok
                pros_errors += err
                batch_docs_p, batch_texts_p = [], []
                logger.info("  Prospects (stage=%s) embedded: %d / ~%d", stage_group, pros_processed, pros_total)

        if batch_docs_p:
            ok, err = await _flush_prospect_batch(batch_docs_p, batch_texts_p)
            pros_processed += ok
            pros_errors += err

        logger.info("Prospects (stage=%s): embedded %d, errors %d", stage_group, pros_processed, pros_errors)

    logger.info("Stage 4 done.")


# ---------------------------------------------------------------------------
# Stage 5: Drop legacy collections (DESTRUCTIVE)
# ---------------------------------------------------------------------------

async def stage5_drop_legacy(db, *, dry_run: bool = False, skip_dump: bool = False) -> None:
    """
    Stage 5: Drop legacy collections after safety mongodump.

    DESTRUCTIVE — run only after verifying Stages 1-4 are complete.

    Collections dropped:
      - employees (merged into prospects)
      - leads (legacy migration-rollback collection)
      - campaign_daily_stats (analytics, per plan)
      - prospect_stats_counts (analytics)

    Collections data cleared (structure kept):
      - campaigns
      - campaign_enrollments
      - campaign_messages
      - campaign_daily_schedules
      - campaign_schedule_items

    Collections kept untouched:
      - Everything else
    """
    logger.info("=== Stage 5: Dropping legacy collections ===")

    drop_collections = ["employees", "leads", "campaign_daily_stats", "prospect_stats_counts"]
    clear_collections = [
        "campaigns", "campaign_enrollments", "campaign_messages",
        "campaign_daily_schedules", "campaign_schedule_items",
    ]

    if dry_run:
        logger.info("[dry-run] would drop: %s", drop_collections)
        logger.info("[dry-run] would clear data in: %s", clear_collections)
        return

    # Safety dump before destruction
    if not skip_dump:
        settings = get_settings()
        db_name = settings.mongodb_database
        dump_path = f"/tmp/outflo_pre_migration_dump_{datetime.utcnow().strftime('%Y%m%d_%H%M%S')}"
        logger.info("Running mongodump to %s before dropping...", dump_path)
        try:
            result = subprocess.run(
                ["mongodump", "--uri", settings.mongodb_url, "--db", db_name, "--out", dump_path],
                capture_output=True, text=True, timeout=300,
            )
            if result.returncode != 0:
                logger.error("mongodump failed: %s", result.stderr)
                logger.error("Aborting drop. Fix mongodump or use --skip-dump to bypass.")
                return
            logger.info("mongodump completed: %s", dump_path)
        except FileNotFoundError:
            logger.warning("mongodump binary not found. Use --skip-dump to bypass or install mongodump.")
            logger.warning("Aborting drop for safety.")
            return
        except Exception as e:
            logger.error("mongodump error: %s. Aborting.", e)
            return

    # Drop collections
    for col_name in drop_collections:
        count = await db[col_name].count_documents({})
        logger.info("Dropping %s (%d docs)...", col_name, count)
        await db[col_name].drop()
        logger.info("  %s dropped.", col_name)

    # Clear data in operational collections (keep structure / indexes)
    for col_name in clear_collections:
        count = await db[col_name].count_documents({})
        logger.info("Clearing data in %s (%d docs)...", col_name, count)
        await db[col_name].delete_many({})
        logger.info("  %s cleared.", col_name)

    logger.info("Stage 5 done. Legacy collections dropped/cleared.")


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

async def main() -> None:
    parser = argparse.ArgumentParser(description="OutFlo shared-schema migration")
    parser.add_argument(
        "--stage", type=int, choices=[0, 1, 2, 3, 4, 5],
        help="Run a specific stage (0-5). Omit to run all stages 0-4.",
    )
    parser.add_argument("--dry-run", action="store_true", help="Preview counts without writing")
    parser.add_argument("--geonames-dir", default=None, help="Directory with cities500.txt + admin1CodesASCII.txt")
    parser.add_argument("--batch-size", type=int, default=250, help="Embedding batch size (max 250)")
    parser.add_argument("--limit", type=int, default=None, help="Limit records per stage (for testing)")
    parser.add_argument("--skip-dump", action="store_true", help="Skip mongodump in stage 5 (DANGEROUS)")
    args = parser.parse_args()

    settings = get_settings()
    motor_client = AsyncIOMotorClient(settings.mongodb_url)
    db = motor_client[settings.mongodb_database]

    logger.info("Connected to MongoDB: %s", settings.mongodb_database)

    stages_to_run = [args.stage] if args.stage is not None else [0, 1, 2, 3, 4]

    for stage in stages_to_run:
        if stage == 0:
            await stage0_build_reference_data(db, dry_run=args.dry_run, geonames_dir=args.geonames_dir)
        elif stage == 1:
            await stage1_transform_companies(db, dry_run=args.dry_run, limit=args.limit)
        elif stage == 2:
            await stage2_merge_people(db, dry_run=args.dry_run, limit=args.limit)
        elif stage == 3:
            await stage3_extract_overlays(db, dry_run=args.dry_run, limit=args.limit)
        elif stage == 4:
            await stage4_embed_backfill(
                db,
                dry_run=args.dry_run,
                batch_size=min(args.batch_size, 250),
                max_prospects=args.limit,
                max_companies=args.limit,
            )
        elif stage == 5:
            confirm = input(
                "\n⚠️  STAGE 5 IS DESTRUCTIVE — it will drop employees, leads, and clear campaign data.\n"
                "Type 'yes' to proceed: "
            )
            if confirm.strip().lower() != "yes":
                logger.info("Aborted by user.")
                return
            await stage5_drop_legacy(db, dry_run=args.dry_run, skip_dump=args.skip_dump)

    motor_client.close()
    logger.info("Migration complete.")


if __name__ == "__main__":
    asyncio.run(main())
