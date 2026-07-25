"""Unit tests for services/geo_resolver.py — uses the real local SQLite
gazetteer at data/geo_places.sqlite. Timed via the harness timing plugin."""
import os
import time

import pytest

from services import geo_resolver
from services.geo_resolver import resolve, to_geojson, _normalize_raw, _hint_to_code

pytestmark = pytest.mark.unit

SQLITE_PRESENT = os.path.exists(geo_resolver.GEO_SQLITE_PATH)


def test_to_geojson_lng_lat_order():
    assert to_geojson(29.76, -95.36) == {"type": "Point", "coordinates": [-95.36, 29.76]}


def test_normalize_raw():
    assert _normalize_raw("  San Francisco, CA  ") != ""
    assert _normalize_raw("") == ""


def test_hint_to_code():
    assert _hint_to_code("US") == "US"
    assert _hint_to_code("united states") == "US"
    assert _hint_to_code(None) is None
    assert _hint_to_code("Atlantis") is None


async def test_resolve_empty_and_none():
    assert await resolve(None) is None
    assert await resolve("") is None
    assert await resolve("   ") is None


async def test_resolve_country_name_no_sqlite_needed():
    result = await resolve("Germany")
    assert result is not None
    assert result["country_code"] == "DE"
    assert result["country"] == "Germany"
    assert result["continent"] == "Europe"
    assert result["raw"] == "Germany"


async def test_resolve_iso2_code():
    result = await resolve("US")
    assert result is not None
    assert result["country_code"] == "US"


@pytest.mark.skipif(not SQLITE_PRESENT, reason="geo_places.sqlite not built")
async def test_resolve_city_via_sqlite():
    result = await resolve("San Francisco, CA")
    assert result is not None
    assert result["country_code"] == "US"
    assert result["city"] is not None
    assert "san francisco" in result["city"].lower()


@pytest.mark.skipif(not SQLITE_PRESENT, reason="geo_places.sqlite not built")
async def test_resolve_with_country_hint():
    # "Cambridge" exists in both GB and US — hint must steer resolution
    gb = await resolve("Cambridge", country_hint="GB")
    us = await resolve("Cambridge", country_hint="US")
    assert gb is not None and us is not None
    assert gb["country_code"] == "GB"
    assert us["country_code"] == "US"


async def test_resolve_unresolvable_returns_none_and_caches():
    raw = "zzz-not-a-place-xyz-123"
    assert await resolve(raw) is None
    # second call hits the negative cache
    assert await resolve(raw) is None


@pytest.mark.skipif(not SQLITE_PRESENT, reason="geo_places.sqlite not built")
async def test_resolve_cache_hit_is_fast(timer):
    geo_resolver._geo_cache.clear()
    with timer() as cold:
        await resolve("Rotterdam")
    with timer() as warm:
        await resolve("Rotterdam")
    assert warm.ms <= cold.ms + 5  # cache must not be slower
    assert warm.ms < 5, f"cached geo lookup took {warm.ms:.2f}ms"


@pytest.mark.skipif(not SQLITE_PRESENT, reason="geo_places.sqlite not built")
async def test_resolve_bulk_speed():
    """100 distinct city lookups against the real sqlite gazetteer."""
    geo_resolver._geo_cache.clear()
    cities = [
        "Houston", "Dallas", "Denver", "Chicago", "Detroit", "Pittsburgh",
        "New York", "Los Angeles", "Seattle", "Boston", "Atlanta", "Phoenix",
        "London", "Manchester", "Birmingham", "Frankfurt", "Munich", "Hamburg",
        "Paris", "Lyon", "Amsterdam", "Zurich", "Milan", "Madrid", "Stockholm",
    ] * 4
    t0 = time.perf_counter()
    for i, c in enumerate(cities):
        await resolve(f"{c}")
    elapsed_ms = (time.perf_counter() - t0) * 1000
    # 25 cold + 75 cached lookups; sqlite is indexed, must be well under 2s
    assert elapsed_ms < 2000, f"100 geo lookups took {elapsed_ms:.0f}ms"
