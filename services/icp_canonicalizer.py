"""
icp_canonicalizer.py

Canonicalizes free-text ICP (Ideal Customer Profile) data from company_profiles
and campaign documents into structured filters for prospect_search_service.
"""

import asyncio
import logging
import re
from typing import Optional

from bson import ObjectId
from bson.binary import Binary

import database
from services.industry_canonicalizer import expand_icp_to_industry_ids
from services.geo_resolver import resolve as geo_resolve
from services.embedding_service import embed_one

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Seniority normalization
# ---------------------------------------------------------------------------

_SENIORITY_RULES: list[tuple[list[str], str]] = [
    (["founder", "co-founder", "owner"], "founder"),
    (["c-suite", "c-level", "ceo", "cto", "cfo", "coo", "cpo", "cso", "chief"], "c_suite"),
    (["vp", "vice president"], "vp"),
    (["director"], "director"),
    (["manager", "head of"], "manager"),
    (["senior", "sr.", "lead", "principal"], "senior"),
    (["mid", "associate", " ii"], "mid"),
    (["junior", "jr.", "entry"], "junior"),
]


def _normalize_seniority(raw_list: list[str]) -> list[str]:
    """Map free-text seniority strings to canonical tokens. One input may yield many tokens."""
    result: list[str] = []
    for raw in raw_list:
        lower = raw.lower()
        for patterns, token in _SENIORITY_RULES:
            for pattern in patterns:
                if pattern in lower:
                    if token not in result:
                        result.append(token)
                    break  # matched this rule; continue to next rule
    return result


# ---------------------------------------------------------------------------
# Employee-band normalization
# ---------------------------------------------------------------------------

_BAND_RULES: list[tuple[list[str], str]] = [
    (["1-10", "1 to 10", "<10", "startup"], "1-10"),
    (["11-50", "11 to 50", "small"], "11-50"),
    (["51-200", "51 to 200", "mid-size"], "51-200"),
    (["201-1000", "201 to 1000", "medium"], "201-1000"),
    (["1000+", "1001+", "enterprise", ">1000", "large"], "1000+"),
]


def _normalize_employee_bands(raw_list: list[str]) -> list[str]:
    """Map free-text company size strings to canonical band tokens."""
    result: list[str] = []
    for raw in raw_list:
        lower = raw.lower()
        for patterns, band in _BAND_RULES:
            matched = False
            for pattern in patterns:
                if pattern in lower:
                    matched = True
                    break
            if matched and band not in result:
                result.append(band)
    return result


# ---------------------------------------------------------------------------
# Field extraction helpers
# ---------------------------------------------------------------------------

def _extract_industries(icp: dict) -> list[str]:
    for key in ("target_industries", "industries", "icp_industries"):
        val = icp.get(key)
        if val:
            return list(val)
    return []


def _extract_geographies(icp: dict) -> list[str]:
    for key in ("target_geographies", "icp_countries"):
        val = icp.get(key)
        if val:
            return list(val)
    return []


def _extract_seniority(icp: dict) -> list[str]:
    for key in ("target_seniority", "icp_seniority_levels"):
        val = icp.get(key)
        if val:
            return list(val)
    return []


def _extract_company_sizes(icp: dict) -> list[str]:
    for key in ("target_company_sizes", "icp_company_size_ranges"):
        val = icp.get(key)
        if val:
            return list(val)
    return []


def _extract_job_titles(icp: dict) -> list[str]:
    for key in ("target_job_titles", "icp_job_titles"):
        val = icp.get(key)
        if val:
            return list(val)
    return []


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

async def canonicalize_icp(icp: dict) -> dict:
    """
    Canonicalize a raw ICP dict into structured filters.

    Accepts keys from both company_profiles and campaign documents:
      - target_industries / industries / icp_industries
      - target_geographies / icp_countries
      - target_seniority / icp_seniority_levels
      - target_company_sizes / icp_company_size_ranges
      - target_job_titles / icp_job_titles

    Returns:
      {
        "industry_ids":    list[str],
        "country_codes":   list[str],
        "seniorities":     list[str],
        "employee_bands":  list[str],
        "title_query_vec": Optional[Binary],  # int8 BSON Binary subtype 9, or None
      }
    """
    raw_industries = _extract_industries(icp)
    raw_geographies = _extract_geographies(icp)
    raw_seniority = _extract_seniority(icp)
    raw_sizes = _extract_company_sizes(icp)
    raw_titles = _extract_job_titles(icp)

    # --- Industry IDs ---
    industry_ids: list[str] = []
    if raw_industries:
        try:
            industry_ids = await expand_icp_to_industry_ids(raw_industries)
        except Exception as exc:
            logger.warning("icp_canonicalizer: industry resolution failed: %s", exc)

    # --- Country codes (resolved concurrently) ---
    country_codes: list[str] = []
    if raw_geographies:
        geo_collection = getattr(database, "geo_places_collection", None)

        async def _resolve_one(geo_str: str) -> Optional[str]:
            try:
                result = await geo_resolve(geo_str, collection=geo_collection)
                if result and result.get("country_code"):
                    return result["country_code"]
            except Exception as exc:
                logger.warning(
                    "icp_canonicalizer: geo resolution failed for %r: %s", geo_str, exc
                )
            return None

        results = await asyncio.gather(*[_resolve_one(g) for g in raw_geographies])
        seen: set[str] = set()
        for code in results:
            if code and code not in seen:
                seen.add(code)
                country_codes.append(code)

    # --- Seniority tokens (pure local mapping, no I/O) ---
    seniorities: list[str] = []
    if raw_seniority:
        try:
            seniorities = _normalize_seniority(raw_seniority)
        except Exception as exc:
            logger.warning("icp_canonicalizer: seniority normalization failed: %s", exc)

    # --- Employee bands (pure local mapping, no I/O) ---
    employee_bands: list[str] = []
    if raw_sizes:
        try:
            employee_bands = _normalize_employee_bands(raw_sizes)
        except Exception as exc:
            logger.warning("icp_canonicalizer: employee band normalization failed: %s", exc)

    # --- Title embedding ---
    title_query_vec: Optional[Binary] = None
    if raw_titles:
        title_text = " ".join(raw_titles).strip()
        if title_text:
            try:
                title_query_vec = await embed_one(title_text, task_type="RETRIEVAL_QUERY")
            except Exception as exc:
                logger.warning("icp_canonicalizer: title embedding failed: %s", exc)

    return {
        "industry_ids": industry_ids,
        "country_codes": country_codes,
        "seniorities": seniorities,
        "employee_bands": employee_bands,
        "title_query_vec": title_query_vec,
    }


async def canonicalize_icp_and_persist(icp: dict, doc_id: str, collection) -> dict:
    """
    Canonicalize the ICP and persist the result back to the given collection.

    Args:
        icp:        Raw ICP dict (company_profile or campaign doc fields).
        doc_id:     String representation of the document's _id.
        collection: Motor async collection to update.

    Returns:
        The canonical dict (same shape as canonicalize_icp).
    """
    canonical = await canonicalize_icp(icp)
    try:
        await collection.update_one(
            {"_id": ObjectId(doc_id)},
            {"$set": canonical},
        )
    except Exception as exc:
        logger.warning(
            "icp_canonicalizer: failed to persist canonical ICP for doc %s: %s", doc_id, exc
        )
    return canonical
