"""
GeoNames-backed location resolver for OutFlo.

Resolves messy raw location strings (e.g. "NYC", "Greater New York Area",
"San Francisco, CA") to a canonical hierarchy and persists documents in the
`geo_places` MongoDB collection.

Usage:
    from services.geo_resolver import resolve, resolve_batch, load_geonames_to_db

    # Pass the Motor collection obtained from database.py; resolver does NOT import it.
    result = await resolve("San Francisco, CA", collection=geo_places_collection)
    # -> {"place_id": "5391959", "city": "San Francisco", "region": "California",
    #     "country": "United States", "country_code": "US",
    #     "continent": "North America", "raw": "San Francisco, CA"}
"""

import asyncio
import logging
import os
import re
import sqlite3
import threading
from typing import Optional

from pymongo import UpdateOne

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# SQLite gazetteer backend
#
# The gazetteer lives in a local read-only SQLite file (built by
# scripts/build_geo_sqlite.py from GeoNames), NOT in MongoDB. The old Mongo
# `geo_places` collection used case-insensitive $regex queries that could not
# use its index — every lookup was a full ~235k-doc collection scan. SQLite
# gives sub-ms indexed lookups, zero Atlas storage, and no seeding step.
# ---------------------------------------------------------------------------

GEO_SQLITE_PATH = os.environ.get(
    "GEO_SQLITE_PATH",
    os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
                 "data", "geo_places.sqlite"),
)

_sqlite_local = threading.local()
_sqlite_missing_warned = False


def _sqlite_conn() -> Optional[sqlite3.Connection]:
    """Per-thread read-only connection to the gazetteer, or None if absent."""
    global _sqlite_missing_warned
    conn = getattr(_sqlite_local, "conn", None)
    if conn is not None:
        return conn
    if not os.path.exists(GEO_SQLITE_PATH):
        if not _sqlite_missing_warned:
            logger.warning(
                "geo_resolver: gazetteer not found at %s — only country-level "
                "resolution available. Build it with scripts/build_geo_sqlite.py",
                GEO_SQLITE_PATH,
            )
            _sqlite_missing_warned = True
        return None
    conn = sqlite3.connect(f"file:{GEO_SQLITE_PATH}?mode=ro", uri=True,
                           check_same_thread=False)
    _sqlite_local.conn = conn
    return conn

# ---------------------------------------------------------------------------
# Static reference data
# ---------------------------------------------------------------------------

COUNTRY_CONTINENT: dict[str, str] = {
    "US": "North America", "CA": "North America", "MX": "North America",
    "GB": "Europe", "DE": "Europe", "FR": "Europe", "NL": "Europe", "SE": "Europe",
    "NO": "Europe", "DK": "Europe", "FI": "Europe", "CH": "Europe", "IT": "Europe",
    "ES": "Europe", "PL": "Europe", "BE": "Europe", "AT": "Europe", "PT": "Europe",
    "IE": "Europe", "CZ": "Europe", "HU": "Europe", "RO": "Europe", "UA": "Europe",
    "IN": "Asia", "CN": "Asia", "JP": "Asia", "KR": "Asia", "SG": "Asia",
    "AU": "Oceania", "NZ": "Oceania",
    "BR": "South America", "AR": "South America", "CL": "South America", "CO": "South America",
    "ZA": "Africa", "NG": "Africa", "KE": "Africa", "EG": "Africa",
    "AE": "Asia", "SA": "Asia", "IL": "Asia", "HK": "Asia", "TW": "Asia",
    "ID": "Asia", "MY": "Asia", "TH": "Asia", "VN": "Asia", "PH": "Asia",
    "PK": "Asia", "BD": "Asia",
}

COUNTRY_NAMES: dict[str, str] = {
    "US": "United States", "GB": "United Kingdom", "CA": "Canada", "AU": "Australia",
    "DE": "Germany", "FR": "France", "NL": "Netherlands", "SE": "Sweden", "IN": "India",
    "SG": "Singapore", "BR": "Brazil", "JP": "Japan", "CN": "China", "KR": "South Korea",
    "IT": "Italy", "ES": "Spain", "CH": "Switzerland", "NO": "Norway", "DK": "Denmark",
    "FI": "Finland", "IE": "Ireland", "BE": "Belgium", "AT": "Austria", "PL": "Poland",
    "PT": "Portugal", "CZ": "Czech Republic", "NZ": "New Zealand", "ZA": "South Africa",
    "AE": "United Arab Emirates", "IL": "Israel", "HK": "Hong Kong", "MX": "Mexico",
    "AR": "Argentina", "CO": "Colombia", "CL": "Chile", "ID": "Indonesia",
    "MY": "Malaysia", "TH": "Thailand", "PH": "Philippines", "VN": "Vietnam",
    "NG": "Nigeria", "KE": "Kenya", "EG": "Egypt", "SA": "Saudi Arabia",
    "TW": "Taiwan", "UA": "Ukraine", "HU": "Hungary", "RO": "Romania", "PK": "Pakistan",
}

# Reverse map: lower-case full name -> ISO-2
_COUNTRY_NAME_TO_CODE: dict[str, str] = {v.lower(): k for k, v in COUNTRY_NAMES.items()}

# ---------------------------------------------------------------------------
# In-process cache
# ---------------------------------------------------------------------------

_geo_cache: dict[str, Optional[dict]] = {}
_CACHE_MAX = 10_000

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def to_geojson(lat: float, lng: float) -> dict:
    """Return a GeoJSON Point dict for the given latitude and longitude."""
    return {"type": "Point", "coordinates": [lng, lat]}


def _normalize_raw(raw: str) -> str:
    """
    Normalise a raw location string for consistent cache keys and queries:
    lowercase, strip, collapse internal whitespace, remove surrounding punctuation,
    and replace " - " separators with a space.
    """
    s = raw.lower().strip()
    s = s.replace(" - ", " ")
    s = re.sub(r"\s+", " ", s)
    s = s.strip(".,;:!?\"'")
    return s


def parse_geonames_line(line: str, admin1_map: dict[str, str]) -> Optional[dict]:
    """
    Parse a single tab-separated line from GeoNames cities500.txt.

    Column layout (19 columns):
        0  geonameid
        1  name
        2  asciiname
        3  alternatenames  (comma-separated)
        4  latitude
        5  longitude
        6  feature_class
        7  feature_code
        8  country_code
        9  cc2
        10 admin1_code
        11 admin2_code
        12 admin3_code
        13 admin4_code
        14 population
        15 elevation
        16 dem
        17 timezone
        18 modification_date

    Returns a geo_places document dict, or None if the line is malformed or
    should be skipped.
    """
    line = line.rstrip("\n\r")
    if not line or line.startswith("#"):
        return None

    parts = line.split("\t")
    if len(parts) < 19:
        return None

    try:
        geonameid = parts[0].strip()
        name = parts[1].strip()
        ascii_name = parts[2].strip()
        alternate_raw = parts[3].strip()
        lat = float(parts[4])
        lng = float(parts[5])
        feature_class = parts[6].strip() or None
        country_code = parts[8].strip().upper()
        admin1_code = parts[10].strip()
        population_str = parts[14].strip()

        if not geonameid or not name:
            return None

        population = int(population_str) if population_str else None

        # Build alternate names list (filter empties and very short tokens)
        alt_names_raw = [n.strip() for n in alternate_raw.split(",") if len(n.strip()) > 1]

        # De-duplicate while preserving order; always include primary and ascii name
        seen: set[str] = set()
        all_names: list[str] = []
        for candidate in [name, ascii_name] + alt_names_raw:
            key = candidate.lower()
            if key not in seen:
                seen.add(key)
                all_names.append(candidate)

        # Admin1 name (state/province)
        admin1_key = f"{country_code}.{admin1_code}"
        region = admin1_map.get(admin1_key)

        country = COUNTRY_NAMES.get(country_code, country_code)
        continent = COUNTRY_CONTINENT.get(country_code)

        return {
            "place_id": geonameid,
            "name": name,
            "ascii_name": ascii_name,
            "alt_names": all_names,
            "city": name,
            "region": region,
            "country": country,
            "country_code": country_code,
            "continent": continent,
            "geo": to_geojson(lat, lng),
            "population": population,
            "feature_class": feature_class,
        }

    except (ValueError, IndexError) as exc:
        logger.debug("Skipping malformed GeoNames line: %s — %s", line[:80], exc)
        return None


async def load_geonames_to_db(cities_file: str, admin1_file: str, collection) -> dict:
    """
    Bulk-load GeoNames cities500.txt into the `geo_places` collection.

    Args:
        cities_file:  Absolute path to cities500.txt
        admin1_file:  Absolute path to admin1CodesASCII.txt
        collection:   Motor collection (geo_places)

    Returns:
        {"inserted": int, "updated": int, "errors": int, "total": int}
    """
    # ---- 1. Load admin1 codes -----------------------------------------
    admin1_map: dict[str, str] = {}
    try:
        with open(admin1_file, encoding="utf-8") as fh:
            for raw_line in fh:
                raw_line = raw_line.rstrip("\n\r")
                if not raw_line:
                    continue
                cols = raw_line.split("\t")
                if len(cols) >= 2:
                    # Format: {CC}.{admin1_code}\t{name}\t{ascii_name}\t{geonameid}
                    code_key = cols[0].strip()
                    admin1_name = cols[1].strip()
                    if code_key and admin1_name:
                        admin1_map[code_key] = admin1_name
        logger.info("Loaded %d admin1 codes from %s", len(admin1_map), admin1_file)
    except OSError as exc:
        logger.error("Could not open admin1 file %s: %s", admin1_file, exc)
        # Proceed without admin1 names; region will be None

    # ---- 2. Stream cities file and bulk-upsert in batches of 1000 -----
    inserted = 0
    updated = 0
    errors = 0
    total = 0
    batch: list[UpdateOne] = []

    async def _flush(ops: list[UpdateOne]) -> tuple[int, int, int]:
        """Execute a bulk_write batch and return (inserted, updated, errors)."""
        if not ops:
            return 0, 0, 0
        try:
            result = await collection.bulk_write(ops, ordered=False)
            return result.upserted_count, result.modified_count, 0
        except Exception as exc:
            logger.error("Bulk write error (batch of %d): %s", len(ops), exc)
            return 0, 0, len(ops)

    try:
        with open(cities_file, encoding="utf-8") as fh:
            for line in fh:
                doc = parse_geonames_line(line, admin1_map)
                if doc is None:
                    continue
                total += 1
                batch.append(
                    UpdateOne(
                        {"place_id": doc["place_id"]},
                        {"$set": doc},
                        upsert=True,
                    )
                )
                if len(batch) >= 1000:
                    ins, upd, err = await _flush(batch)
                    inserted += ins
                    updated += upd
                    errors += err
                    batch = []

        # Flush remainder
        if batch:
            ins, upd, err = await _flush(batch)
            inserted += ins
            updated += upd
            errors += err

    except OSError as exc:
        logger.error("Could not open cities file %s: %s", cities_file, exc)

    stats = {"inserted": inserted, "updated": updated, "errors": errors, "total": total}
    logger.info("GeoNames load complete: %s", stats)
    return stats


# ---------------------------------------------------------------------------
# Core resolver
# ---------------------------------------------------------------------------


def _resolve_country_only(norm: str) -> Optional[dict]:
    """
    Return a country-level result if `norm` directly matches a known country
    name or ISO-2 code.  No MongoDB required.
    """
    # ISO-2 direct match
    upper = norm.upper()
    if upper in COUNTRY_NAMES:
        country_code = upper
        return {
            "place_id": None,
            "city": None,
            "region": None,
            "country": COUNTRY_NAMES[country_code],
            "country_code": country_code,
            "continent": COUNTRY_CONTINENT.get(country_code),
        }

    # Full country name match
    if norm in _COUNTRY_NAME_TO_CODE:
        country_code = _COUNTRY_NAME_TO_CODE[norm]
        return {
            "place_id": None,
            "city": None,
            "region": None,
            "country": COUNTRY_NAMES[country_code],
            "country_code": country_code,
            "continent": COUNTRY_CONTINENT.get(country_code),
        }

    return None


def _hint_to_code(country_hint: Optional[str]) -> Optional[str]:
    """
    Convert a country hint (ISO-2 or full name) to an ISO-2 code,
    or return None if unrecognised.
    """
    if not country_hint:
        return None
    upper = country_hint.strip().upper()
    if upper in COUNTRY_NAMES:
        return upper
    lower = country_hint.strip().lower()
    return _COUNTRY_NAME_TO_CODE.get(lower)


def _doc_to_result(doc: dict, raw: str) -> dict:
    """Map a gazetteer row to the public result shape."""
    result = {
        "place_id": doc.get("place_id"),
        "city": doc.get("city"),
        "region": doc.get("region"),
        "country": doc.get("country"),
        "country_code": doc.get("country_code"),
        "continent": doc.get("continent"),
        "raw": raw,
    }
    if doc.get("geo"):
        result["geo"] = doc["geo"]
    return result


_PLACE_FIELDS = ["place_id", "city", "region", "country", "country_code", "continent", "lat", "lng"]
_PLACE_COLS = ", ".join(f"p.{c}" for c in _PLACE_FIELDS)


def _query_sqlite(norm: str, hint_code: Optional[str]) -> Optional[dict]:
    """
    Run the resolution cascade against the SQLite gazetteer.

    Strategy (same as the old Mongo version):
        a. Exact name match, preferring the country hint.  Population desc.
        b. Prefix name match with same preference.
        c. Strip "greater … area" → retry exact then prefix.
        d. Take first token before comma → retry exact then prefix.

    Returns a place row as a dict, or None.
    """
    conn = _sqlite_conn()
    if conn is None:
        return None

    def _find_best(pattern: str, is_prefix: bool) -> Optional[dict]:
        if is_prefix:
            # range comparison instead of LIKE — always index-backed regardless
            # of collation (LIKE with a bound parameter disables the prefix
            # optimization on a binary-collated index)
            where = "n.name_lower >= ? AND n.name_lower < ?"
            params = [pattern, pattern + "￿"]
        else:
            where, params = "n.name_lower = ?", [pattern]
        sql = (
            f"SELECT {_PLACE_COLS} FROM names n JOIN places p USING (place_id) "
            f"WHERE {where} {{hint}} ORDER BY n.population DESC LIMIT 1"
        )
        try:
            row = None
            if hint_code:
                row = conn.execute(sql.format(hint="AND n.country_code = ?"),
                                   params + [hint_code]).fetchone()
            if row is None:
                row = conn.execute(sql.format(hint=""), params).fetchone()
        except sqlite3.Error as exc:
            logger.warning("geo sqlite query error: %s", exc)
            return None
        if row is None:
            return None
        doc = dict(zip(_PLACE_FIELDS, row))
        lat, lng = doc.pop("lat", None), doc.pop("lng", None)
        if lat is not None and lng is not None:
            doc["geo"] = to_geojson(lat, lng)
        return doc

    # a. Exact match
    doc = _find_best(norm, is_prefix=False)
    if doc:
        return doc

    # b. Prefix match
    doc = _find_best(norm, is_prefix=True)
    if doc:
        return doc

    # c. Strip "greater … area" prefix/suffix
    stripped = norm
    if stripped.startswith("greater "):
        stripped = stripped[len("greater "):]
    if stripped.endswith(" area"):
        stripped = stripped[: -len(" area")]
    stripped = stripped.strip()
    if stripped and stripped != norm:
        doc = _find_best(stripped, is_prefix=False)
        if doc:
            return doc
        doc = _find_best(stripped, is_prefix=True)
        if doc:
            return doc

    # d. First token before comma (e.g. "New York, NY" → "New York")
    if "," in norm:
        first_part = norm.split(",")[0].strip()
        if first_part and first_part != norm and first_part != stripped:
            doc = _find_best(first_part, is_prefix=False)
            if doc:
                return doc
            doc = _find_best(first_part, is_prefix=True)
            if doc:
                return doc

    return None


async def resolve(
    raw: Optional[str],
    *,
    country_hint: Optional[str] = None,
    collection=None,
) -> Optional[dict]:
    """
    Resolve a raw location string to a canonical location dict.

    Args:
        raw:           Raw location string from a prospect/lead record.
        country_hint:  Optional ISO-2 code or full country name to narrow results.
        collection:    Motor `geo_places` collection.  If None, only country-level
                       resolution via COUNTRY_NAMES is attempted.

    Returns:
        Dict with keys {place_id, city, region, country, country_code, continent, raw}
        or None if the input is empty / unresolvable.
    """
    if not raw or not raw.strip():
        return None

    original_raw = raw
    norm = _normalize_raw(raw)

    if not norm:
        return None

    # --- Cache lookup -------------------------------------------------------
    cache_key = f"{norm}|{(country_hint or '').upper()}"
    if cache_key in _geo_cache:
        cached = _geo_cache[cache_key]
        if cached is not None:
            # Return a copy with the original raw string preserved
            return {**cached, "raw": original_raw}
        return None

    # --- Exact country match first --------------------------------------------
    # An ISO-2 code or full country name is unambiguous and must win over the
    # city gazetteer: e.g. "US" is also a tiny French commune, so querying
    # SQLite first resolved raw location "US" to France.
    country_result = _resolve_country_only(norm)
    if country_result:
        country_result["raw"] = original_raw
        _cache_set(cache_key, {k: v for k, v in country_result.items() if k != "raw"})
        return country_result

    # --- SQLite resolution ----------------------------------------------------
    # `collection` is accepted for backwards compatibility but ignored: the
    # gazetteer lives in a local SQLite file, not MongoDB.
    hint_code = _hint_to_code(country_hint)
    doc = _query_sqlite(norm, hint_code)

    if doc:
        result = _doc_to_result(doc, original_raw)
        _cache_set(cache_key, {k: v for k, v in result.items() if k != "raw"})
        return result

    # Last resort: country-only from the raw string itself
    country_result = _resolve_country_only(norm)
    if country_result:
        country_result["raw"] = original_raw
        _cache_set(cache_key, {k: v for k, v in country_result.items() if k != "raw"})
        return country_result

    # Unresolvable
    _cache_set(cache_key, None)
    logger.debug("geo_resolver: could not resolve %r", raw)
    return None


def _cache_set(key: str, value: Optional[dict]) -> None:
    """Store a value in the in-process cache, evicting an arbitrary entry when full."""
    if len(_geo_cache) >= _CACHE_MAX:
        # Simple eviction: pop an arbitrary key (dict iteration order is insertion order)
        evict_key = next(iter(_geo_cache))
        _geo_cache.pop(evict_key, None)
    _geo_cache[key] = value


async def resolve_batch(
    raws: list[tuple[Optional[str], Optional[str]]],
    *,
    collection=None,
) -> list[Optional[dict]]:
    """
    Resolve a list of (raw_string, country_hint) pairs to location dicts.

    Results are returned in the same order as the input.  Cache hits are
    handled synchronously; uncached entries are awaited sequentially
    (MongoDB queries are fast and the connection is already pooled).

    Args:
        raws:        List of (raw_location, country_hint) tuples.
        collection:  Motor `geo_places` collection.

    Returns:
        List of resolved dicts (or None) aligned 1:1 with `raws`.
    """
    results: list[Optional[dict]] = []
    for raw, country_hint in raws:
        result = await resolve(raw, country_hint=country_hint, collection=collection)
        results.append(result)
    return results
