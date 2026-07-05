"""
Industry canonicalizer — maps raw industry strings (from LinkedIn/Apify scrapes)
to a canonical LinkedIn-inspired taxonomy stored in the `industries_taxonomy`
MongoDB collection.

Resolution pipeline (in order):
    1. Direct alias lookup in ALIAS_OVERRIDES (exact, lowercase)
    2. Substring scan against ALIAS_OVERRIDES keys (best longest match ≥ 4 chars)
    3. Taxonomy alias lookup from DB (loaded lazily into _alias_cache)
    4. Embedding cosine similarity vs DB vecs (if embedding_vec provided)
    5. Unknown — returns None

build_taxonomy_in_db()  — seeds / upserts the taxonomy collection from the
                          module-level constants.
"""

from __future__ import annotations

import asyncio
import logging
import re
from datetime import datetime, timezone
from typing import Optional

from pymongo import UpdateOne

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Canonical taxonomy
# ---------------------------------------------------------------------------

LINKEDIN_INDUSTRIES: list[dict] = [
    # group: Technology, Information & Media
    {"id": "software_development", "label": "Software Development", "group": "Technology, Information & Media"},
    {"id": "it_services", "label": "IT Services and IT Consulting", "group": "Technology, Information & Media"},
    {"id": "computer_hardware", "label": "Computer Hardware Manufacturing", "group": "Technology, Information & Media"},
    {"id": "technology_internet", "label": "Technology, Information and Internet", "group": "Technology, Information & Media"},
    {"id": "data_infrastructure", "label": "Data Infrastructure and Analytics", "group": "Technology, Information & Media"},
    {"id": "computer_networking", "label": "Computer Networking Products", "group": "Technology, Information & Media"},
    {"id": "semiconductor", "label": "Semiconductor Manufacturing", "group": "Technology, Information & Media"},
    {"id": "broadcast_media", "label": "Broadcast Media Production and Distribution", "group": "Technology, Information & Media"},
    {"id": "book_periodical", "label": "Book and Periodical Publishing", "group": "Technology, Information & Media"},
    {"id": "online_media", "label": "Online Audio and Video Media", "group": "Technology, Information & Media"},
    {"id": "news_media", "label": "Newspaper Publishing", "group": "Technology, Information & Media"},
    {"id": "telecommunications", "label": "Telecommunications", "group": "Technology, Information & Media"},
    {"id": "wireless_services", "label": "Wireless Services", "group": "Technology, Information & Media"},

    # group: Financial Services
    {"id": "financial_services", "label": "Financial Services", "group": "Financial Services"},
    {"id": "banking", "label": "Banking", "group": "Financial Services"},
    {"id": "investment_management", "label": "Investment Management", "group": "Financial Services"},
    {"id": "insurance", "label": "Insurance", "group": "Financial Services"},
    {"id": "venture_capital", "label": "Venture Capital and Private Equity Principals", "group": "Financial Services"},
    {"id": "capital_markets", "label": "Capital Markets", "group": "Financial Services"},
    {"id": "accounting", "label": "Accounting", "group": "Financial Services"},
    {"id": "fintech", "label": "Financial Technology (FinTech)", "group": "Financial Services"},

    # group: Healthcare & Life Sciences
    {"id": "hospitals_healthcare", "label": "Hospitals and Health Care", "group": "Healthcare & Life Sciences"},
    {"id": "medical_devices", "label": "Medical Equipment Manufacturing", "group": "Healthcare & Life Sciences"},
    {"id": "pharmaceuticals", "label": "Pharmaceutical Manufacturing", "group": "Healthcare & Life Sciences"},
    {"id": "biotechnology", "label": "Biotechnology Research", "group": "Healthcare & Life Sciences"},
    {"id": "health_wellness", "label": "Health and Human Services", "group": "Healthcare & Life Sciences"},
    {"id": "mental_health", "label": "Mental Health Care", "group": "Healthcare & Life Sciences"},
    {"id": "medical_practice", "label": "Medical Practices", "group": "Healthcare & Life Sciences"},
    {"id": "healthtech", "label": "Health Technology", "group": "Healthcare & Life Sciences"},

    # group: Retail & Consumer
    {"id": "retail", "label": "Retail", "group": "Retail & Consumer"},
    {"id": "ecommerce", "label": "E-commerce / Online Retail", "group": "Retail & Consumer"},
    {"id": "consumer_goods", "label": "Consumer Goods", "group": "Retail & Consumer"},
    {"id": "food_beverage", "label": "Food and Beverage Services", "group": "Retail & Consumer"},
    {"id": "restaurants", "label": "Restaurants", "group": "Retail & Consumer"},
    {"id": "wholesale", "label": "Wholesale", "group": "Retail & Consumer"},
    {"id": "supermarkets", "label": "Supermarkets and Grocery Stores", "group": "Retail & Consumer"},

    # group: Manufacturing & Industrial
    {"id": "manufacturing", "label": "Manufacturing", "group": "Manufacturing & Industrial"},
    {"id": "industrial_automation", "label": "Industrial Machinery Manufacturing", "group": "Manufacturing & Industrial"},
    {"id": "chemicals", "label": "Chemical Manufacturing", "group": "Manufacturing & Industrial"},
    {"id": "electronics_mfg", "label": "Appliances, Electrical, and Electronics Manufacturing", "group": "Manufacturing & Industrial"},
    {"id": "automotive", "label": "Motor Vehicle Manufacturing", "group": "Manufacturing & Industrial"},
    {"id": "aerospace", "label": "Aviation and Aerospace Component Manufacturing", "group": "Manufacturing & Industrial"},
    {"id": "construction", "label": "Construction", "group": "Manufacturing & Industrial"},
    {"id": "building_materials", "label": "Building Construction", "group": "Manufacturing & Industrial"},

    # group: Professional Services
    {"id": "management_consulting", "label": "Business Consulting and Services", "group": "Professional Services"},
    {"id": "legal_services", "label": "Law Practice", "group": "Professional Services"},
    {"id": "staffing_recruiting", "label": "Staffing and Recruiting", "group": "Professional Services"},
    {"id": "marketing_advertising", "label": "Advertising Services", "group": "Professional Services"},
    {"id": "public_relations", "label": "Public Relations and Communications Services", "group": "Professional Services"},
    {"id": "research_services", "label": "Research Services", "group": "Professional Services"},
    {"id": "events_services", "label": "Events Services", "group": "Professional Services"},

    # group: Education
    {"id": "education_management", "label": "Education Management", "group": "Education"},
    {"id": "higher_education", "label": "Higher Education", "group": "Education"},
    {"id": "e_learning", "label": "E-Learning Providers", "group": "Education"},
    {"id": "primary_education", "label": "Primary and Secondary Education", "group": "Education"},

    # group: Real Estate
    {"id": "real_estate", "label": "Real Estate", "group": "Real Estate"},
    {"id": "real_estate_dev", "label": "Real Estate and Equipment Rental and Leasing", "group": "Real Estate"},
    {"id": "facilities", "label": "Facilities Services", "group": "Real Estate"},

    # group: Transportation & Logistics
    {"id": "logistics", "label": "Transportation, Logistics, Supply Chain and Storage", "group": "Transportation & Logistics"},
    {"id": "trucking", "label": "Truck Transportation", "group": "Transportation & Logistics"},
    {"id": "airlines", "label": "Airlines and Aviation", "group": "Transportation & Logistics"},
    {"id": "maritime", "label": "Maritime Transportation", "group": "Transportation & Logistics"},

    # group: Energy & Environment
    {"id": "oil_gas", "label": "Oil and Gas", "group": "Energy & Environment"},
    {"id": "utilities", "label": "Utilities", "group": "Energy & Environment"},
    {"id": "renewables", "label": "Renewable Energy Semiconductor Manufacturing", "group": "Energy & Environment"},
    {"id": "environmental", "label": "Environmental Services", "group": "Energy & Environment"},

    # group: Government & Non-Profit
    {"id": "government", "label": "Government Administration", "group": "Government & Non-Profit"},
    {"id": "nonprofit", "label": "Non-profit Organizations", "group": "Government & Non-Profit"},
    {"id": "defense", "label": "Defense and Space Manufacturing", "group": "Government & Non-Profit"},

    # group: Media & Entertainment
    {"id": "entertainment", "label": "Entertainment Providers", "group": "Media & Entertainment"},
    {"id": "gaming", "label": "Computer Games", "group": "Media & Entertainment"},
    {"id": "music", "label": "Musicians", "group": "Media & Entertainment"},
    {"id": "sports", "label": "Spectator Sports", "group": "Media & Entertainment"},
    {"id": "photography", "label": "Photography", "group": "Media & Entertainment"},

    # group: Human Resources
    {"id": "human_resources", "label": "Human Resources Services", "group": "Human Resources"},
    {"id": "outsourcing_offshoring", "label": "Outsourcing and Offshoring Consulting", "group": "Human Resources"},

    # group: Security
    {"id": "cybersecurity", "label": "Computer and Network Security", "group": "Security"},
    {"id": "security_services", "label": "Security and Investigations", "group": "Security"},

    # group: Design
    {"id": "design_services", "label": "Design Services", "group": "Design"},
    {"id": "architecture", "label": "Architecture and Planning", "group": "Design"},

    # group: Agriculture
    {"id": "agriculture", "label": "Farming", "group": "Agriculture"},
    {"id": "agtech", "label": "Agricultural Technology", "group": "Agriculture"},
]

# Quick lookup: industry_id -> LINKEDIN_INDUSTRIES entry (populated once at import)
_TAXONOMY_BY_ID: dict[str, dict] = {entry["id"]: entry for entry in LINKEDIN_INDUSTRIES}

# ---------------------------------------------------------------------------
# Alias overrides (seed aliases, sourced from icp_synonyms + scoring service)
# These take precedence over anything stored in the DB.
# ---------------------------------------------------------------------------

ALIAS_OVERRIDES: dict[str, str] = {
    # SaaS / Software
    "saas": "software_development",
    "software as a service": "software_development",
    "computer software": "software_development",
    "software": "software_development",
    "cloud computing": "software_development",
    "information technology and services": "it_services",
    "it services": "it_services",
    "information technology": "it_services",
    "it consulting": "it_services",
    "internet": "technology_internet",
    "technology": "technology_internet",
    "tech": "technology_internet",
    # FinTech / Finance
    "fintech": "fintech",
    "financial technology": "fintech",
    "financial services": "financial_services",
    "finance": "financial_services",
    "banking": "banking",
    "investment management": "investment_management",
    "investment banking": "investment_management",
    "venture capital": "venture_capital",
    "venture capital & private equity": "venture_capital",
    "private equity": "venture_capital",
    "capital markets": "capital_markets",
    "insurance": "insurance",
    "accounting": "accounting",
    # Healthcare
    "healthcare": "hospitals_healthcare",
    "health care": "hospitals_healthcare",
    "hospital & health care": "hospitals_healthcare",
    "hospital and health care": "hospitals_healthcare",
    "hospitals and health care": "hospitals_healthcare",
    "medical devices": "medical_devices",
    "medical equipment": "medical_devices",
    "medtech": "medical_devices",
    "med tech": "medical_devices",
    "pharmaceuticals": "pharmaceuticals",
    "pharma": "pharmaceuticals",
    "pharmaceutical": "pharmaceuticals",
    "biotechnology": "biotechnology",
    "biotech": "biotechnology",
    "life sciences": "biotechnology",
    "biopharma": "biotechnology",
    "healthtech": "healthtech",
    "health tech": "healthtech",
    "health technology": "healthtech",
    "digital health": "healthtech",
    # Retail / Ecommerce
    "retail": "retail",
    "ecommerce": "ecommerce",
    "e-commerce": "ecommerce",
    "e-retail": "ecommerce",
    "online retail": "ecommerce",
    "direct-to-consumer": "ecommerce",
    "d2c": "ecommerce",
    "consumer goods": "consumer_goods",
    "consumer products": "consumer_goods",
    "fmcg": "consumer_goods",
    "food & beverages": "food_beverage",
    "food and beverages": "food_beverage",
    "food production": "food_beverage",
    "restaurants": "restaurants",
    "wholesale": "wholesale",
    # Manufacturing
    "manufacturing": "manufacturing",
    "industrial": "industrial_automation",
    "industrial automation": "industrial_automation",
    "industrial machinery": "industrial_automation",
    "machinery": "industrial_automation",
    "automation": "industrial_automation",
    "chemicals": "chemicals",
    "electronics manufacturing": "electronics_mfg",
    "automotive": "automotive",
    "automobile": "automotive",
    "motor vehicle": "automotive",
    "aerospace": "aerospace",
    "aviation": "airlines",
    "airlines": "airlines",
    "construction": "construction",
    # Professional Services
    "consulting": "management_consulting",
    "management consulting": "management_consulting",
    "business consulting": "management_consulting",
    "professional services": "management_consulting",
    "legal": "legal_services",
    "law practice": "legal_services",
    "law firm": "legal_services",
    "legaltech": "legal_services",
    "staffing": "staffing_recruiting",
    "recruiting": "staffing_recruiting",
    "staffing and recruiting": "staffing_recruiting",
    "executive search": "staffing_recruiting",
    "marketing": "marketing_advertising",
    "advertising": "marketing_advertising",
    "marketing and advertising": "marketing_advertising",
    "digital marketing": "marketing_advertising",
    "public relations": "public_relations",
    "pr": "public_relations",
    # Education
    "education": "education_management",
    "higher education": "higher_education",
    "edtech": "e_learning",
    "education technology": "e_learning",
    "e-learning": "e_learning",
    "online learning": "e_learning",
    # Real Estate
    "real estate": "real_estate",
    "property": "real_estate",
    "proptech": "real_estate",
    "real estate development": "real_estate_dev",
    "facilities management": "facilities",
    # Logistics
    "logistics": "logistics",
    "supply chain": "logistics",
    "logistics and supply chain": "logistics",
    "transportation": "logistics",
    "freight": "logistics",
    "shipping": "logistics",
    "trucking": "trucking",
    # Energy
    "oil & gas": "oil_gas",
    "oil, gas, and mining": "oil_gas",
    "energy": "utilities",
    "renewables": "renewables",
    "clean energy": "renewables",
    "cleantech": "renewables",
    "solar": "renewables",
    "utilities": "utilities",
    "power generation": "utilities",
    # Telecom
    "telecom": "telecommunications",
    "telecommunications": "telecommunications",
    "wireless": "wireless_services",
    # Security
    "cybersecurity": "cybersecurity",
    "information security": "cybersecurity",
    "infosec": "cybersecurity",
    "computer and network security": "cybersecurity",
    "network security": "cybersecurity",
    "security": "security_services",
    # Media / Entertainment
    "media": "entertainment",
    "entertainment": "entertainment",
    "gaming": "gaming",
    "computer games": "gaming",
    "sports": "sports",
    # Design
    "design": "design_services",
    "architecture": "architecture",
    # HR
    "hr": "human_resources",
    "human resources": "human_resources",
    "hrtech": "human_resources",
    # AI/ML (map to tech for now)
    "ai": "software_development",
    "artificial intelligence": "software_development",
    "machine learning": "data_infrastructure",
    "data science": "data_infrastructure",
    "data analytics": "data_infrastructure",
    "analytics": "data_infrastructure",
    # Agriculture
    "agriculture": "agriculture",
    "farming": "agriculture",
    "agtech": "agtech",
    # Government/nonprofit
    "government": "government",
    "nonprofit": "nonprofit",
    "non-profit": "nonprofit",
    "defense": "defense",
}

# Pre-sorted alias keys by descending length — used by substring scan so the
# longest (most specific) alias wins.
_ALIAS_KEYS_BY_LENGTH: list[str] = sorted(ALIAS_OVERRIDES.keys(), key=len, reverse=True)

# ---------------------------------------------------------------------------
# Module-level caches (populated lazily from the DB)
# ---------------------------------------------------------------------------

_taxonomy_cache: dict[str, dict] | None = None   # industry_id -> taxonomy doc
_alias_cache: dict[str, str] | None = None        # lowercase alias -> industry_id

# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------


def _normalize_industry(raw: str) -> str:
    """Lowercase and strip a raw industry string."""
    return raw.lower().strip()


def _cosine_similarity(vec_a: list[float], vec_b: list[float]) -> float:
    """Compute cosine similarity between two equal-length float vectors."""
    if not vec_a or not vec_b or len(vec_a) != len(vec_b):
        return 0.0
    dot = sum(a * b for a, b in zip(vec_a, vec_b))
    norm_a = sum(a * a for a in vec_a) ** 0.5
    norm_b = sum(b * b for b in vec_b) ** 0.5
    if norm_a == 0.0 or norm_b == 0.0:
        return 0.0
    return dot / (norm_a * norm_b)


def _entry_from_id(industry_id: str) -> dict | None:
    """Return the LINKEDIN_INDUSTRIES entry dict for the given id, or None."""
    return _TAXONOMY_BY_ID.get(industry_id)


def _build_result(
    industry_id: str,
    raw: str,
    assign_method: str,
    confidence: float,
    *,
    taxonomy_doc: dict | None = None,
) -> dict:
    """
    Build the standardised resolution result dict.

    Prefers the in-memory LINKEDIN_INDUSTRIES entry; falls back to the
    taxonomy_doc if the id is not in the static list (e.g. a future DB-only
    entry).
    """
    entry = _entry_from_id(industry_id)
    if entry is not None:
        label = entry["label"]
        group = entry["group"]
    elif taxonomy_doc is not None:
        label = taxonomy_doc.get("label", industry_id)
        group = taxonomy_doc.get("group", "")
    else:
        label = industry_id
        group = ""

    return {
        "id": industry_id,
        "label": label,
        "group": group,
        "raw": raw,
        "assign_method": assign_method,
        "confidence": confidence,
    }


# ---------------------------------------------------------------------------
# Cache loading
# ---------------------------------------------------------------------------


async def _load_taxonomy(collection) -> None:
    """
    Load all documents from the industries_taxonomy collection into the
    module-level caches.

    Merges ALIAS_OVERRIDES last so they always win over DB-stored aliases.
    Safe to call concurrently — idempotent after first successful load.
    """
    global _taxonomy_cache, _alias_cache

    cache: dict[str, dict] = {}
    alias_map: dict[str, str] = {}

    try:
        async for doc in collection.find({}):
            industry_id = doc.get("industry_id")
            if not industry_id:
                continue
            cache[industry_id] = doc
            for alias in doc.get("aliases", []):
                if alias:
                    alias_map[alias.lower()] = industry_id
    except Exception as exc:
        logger.warning("industry_canonicalizer: failed to load taxonomy from DB: %s", exc)

    # ALIAS_OVERRIDES take precedence over DB aliases
    alias_map.update(ALIAS_OVERRIDES)

    _taxonomy_cache = cache
    _alias_cache = alias_map
    logger.debug(
        "industry_canonicalizer: loaded %d taxonomy entries, %d aliases",
        len(cache),
        len(alias_map),
    )


async def _ensure_loaded(collection) -> None:
    """Lazily load caches if not yet populated."""
    global _taxonomy_cache, _alias_cache
    if _taxonomy_cache is None or _alias_cache is None:
        await _load_taxonomy(collection)


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------


async def resolve(
    raw: str | None,
    *,
    collection=None,
    embedding_vec: list[float] | None = None,
) -> dict | None:
    """
    Resolve a raw industry string to a canonical taxonomy entry.

    Returns a dict with keys: {id, label, group, raw, assign_method, confidence}
    or None if raw is empty/None or could not be matched.

    Resolution order:
        1. Direct key in ALIAS_OVERRIDES         → assign_method="alias",            confidence=1.0
        2. Substring scan of ALIAS_OVERRIDES      → assign_method="alias_substring",  confidence=0.85
        3. DB alias_cache lookup                  → assign_method="taxonomy_alias",   confidence=0.9
        4. Embedding cosine similarity (vec > 0.7)→ assign_method="embedding",        confidence=<score>
        5. Unresolved                             → None
    """
    if not raw or not raw.strip():
        return None

    normalized = _normalize_industry(raw)

    # ------------------------------------------------------------------
    # Step 1: direct ALIAS_OVERRIDES lookup
    # ------------------------------------------------------------------
    if normalized in ALIAS_OVERRIDES:
        industry_id = ALIAS_OVERRIDES[normalized]
        return _build_result(industry_id, raw, "alias", 1.0)

    # ------------------------------------------------------------------
    # Step 2: substring scan — longest ALIAS_OVERRIDES key wins
    # Keys shorter than 4 chars only match if they are contained in raw,
    # not the other way around (avoids over-broad matches like "ai").
    # ------------------------------------------------------------------
    best_key: str | None = None
    best_len: int = 0

    for alias_key in _ALIAS_KEYS_BY_LENGTH:
        key_len = len(alias_key)
        if key_len < best_len:
            # Already have a longer match; remaining keys are shorter
            break
        if key_len >= 4:
            # Match if key is in raw OR raw is in key (mutual containment)
            if alias_key in normalized or normalized in alias_key:
                best_key = alias_key
                best_len = key_len
        else:
            # Short keys: only match if key appears inside the raw string
            if alias_key in normalized:
                best_key = alias_key
                best_len = key_len

    if best_key is not None:
        industry_id = ALIAS_OVERRIDES[best_key]
        return _build_result(industry_id, raw, "alias_substring", 0.85)

    # ------------------------------------------------------------------
    # Step 3: DB taxonomy alias lookup (load lazily if needed)
    # ------------------------------------------------------------------
    if collection is not None:
        await _ensure_loaded(collection)

    if _alias_cache is not None and normalized in _alias_cache:
        industry_id = _alias_cache[normalized]
        taxonomy_doc = (_taxonomy_cache or {}).get(industry_id)
        return _build_result(industry_id, raw, "taxonomy_alias", 0.9, taxonomy_doc=taxonomy_doc)

    # ------------------------------------------------------------------
    # Step 4: Embedding similarity (requires taxonomy vecs in _taxonomy_cache)
    # ------------------------------------------------------------------
    if embedding_vec and _taxonomy_cache:
        best_id: str | None = None
        best_score: float = 0.0

        for industry_id, doc in _taxonomy_cache.items():
            vec = doc.get("vec")
            if not vec:
                continue
            score = _cosine_similarity(embedding_vec, vec)
            if score > best_score:
                best_score = score
                best_id = industry_id

        if best_id is not None and best_score > 0.7:
            taxonomy_doc = _taxonomy_cache.get(best_id)
            return _build_result(best_id, raw, "embedding", best_score, taxonomy_doc=taxonomy_doc)

    # ------------------------------------------------------------------
    # Step 5: Unresolved
    # ------------------------------------------------------------------
    logger.debug("industry_canonicalizer: could not resolve industry '%s'", raw)
    return None


async def expand_icp_to_industry_ids(
    icp_industries: list[str],
    *,
    collection=None,
) -> list[str]:
    """
    Given free-text ICP industry strings, return a deduplicated list of
    canonical industry_ids.

    If an input resolves to an industry_id whose group matches a group-level
    label (e.g. "Technology, Information & Media"), all ids in that group are
    also included.
    """
    if not icp_industries:
        return []

    # Build group-name → member ids index from static taxonomy
    group_to_ids: dict[str, list[str]] = {}
    for entry in LINKEDIN_INDUSTRIES:
        group_to_ids.setdefault(entry["group"], []).append(entry["id"])

    # Also build a group-name alias map (lowercase group → group name)
    group_alias: dict[str, str] = {g.lower(): g for g in group_to_ids}

    seen: set[str] = set()
    result: list[str] = []

    for raw in icp_industries:
        if not raw or not raw.strip():
            continue

        normalized = _normalize_industry(raw)

        # Check if the input directly names a group
        matched_group: str | None = None
        for g_lower, g_label in group_alias.items():
            if normalized == g_lower or normalized in g_lower or g_lower in normalized:
                matched_group = g_label
                break

        if matched_group:
            for gid in group_to_ids.get(matched_group, []):
                if gid not in seen:
                    seen.add(gid)
                    result.append(gid)
            continue

        resolved = await resolve(raw, collection=collection)
        if resolved is None:
            continue

        industry_id = resolved["id"]
        if industry_id not in seen:
            seen.add(industry_id)
            result.append(industry_id)

    return result


async def build_taxonomy_in_db(collection) -> int:
    """
    Upsert all LINKEDIN_INDUSTRIES entries into the collection.

    Each document gets:
        - industry_id, label, group
        - aliases: all ALIAS_OVERRIDES keys that point to this id
        - vec / vec_model: left as None (computed externally)
        - created_at (only on insert), updated_at (always)

    Returns the total number of entries processed.
    """
    # Build reverse alias map: industry_id -> [alias, ...]
    id_to_aliases: dict[str, list[str]] = {}
    for alias, industry_id in ALIAS_OVERRIDES.items():
        id_to_aliases.setdefault(industry_id, []).append(alias)

    now = datetime.now(timezone.utc)
    ops: list[UpdateOne] = []

    for entry in LINKEDIN_INDUSTRIES:
        industry_id = entry["id"]
        aliases = sorted(id_to_aliases.get(industry_id, []))

        ops.append(
            UpdateOne(
                {"industry_id": industry_id},
                {
                    "$set": {
                        "industry_id": industry_id,
                        "label": entry["label"],
                        "group": entry["group"],
                        "aliases": aliases,
                        "updated_at": now,
                    },
                    "$setOnInsert": {
                        "vec": None,
                        "vec_model": None,
                        "created_at": now,
                    },
                },
                upsert=True,
            )
        )

    if ops:
        result = await collection.bulk_write(ops, ordered=False)
        upserted = result.upserted_count + result.modified_count + result.matched_count
        logger.info(
            "industry_canonicalizer: build_taxonomy_in_db upserted=%d modified=%d matched=%d",
            result.upserted_count,
            result.modified_count,
            result.matched_count,
        )
    else:
        upserted = 0

    # Invalidate caches so next resolve() call reloads from DB
    global _taxonomy_cache, _alias_cache
    _taxonomy_cache = None
    _alias_cache = None

    return len(LINKEDIN_INDUSTRIES)


def get_taxonomy_entry(industry_id: str) -> dict | None:
    """
    Synchronous lookup of a taxonomy entry by id.

    Returns the LINKEDIN_INDUSTRIES dict for the given id, or the cached DB
    doc if available, or None.

    Note: _load_taxonomy() must have been awaited at least once for the DB
    cache to be populated; this function always checks the static list first.
    """
    entry = _entry_from_id(industry_id)
    if entry is not None:
        return entry

    if _taxonomy_cache is not None:
        return _taxonomy_cache.get(industry_id)

    return None
