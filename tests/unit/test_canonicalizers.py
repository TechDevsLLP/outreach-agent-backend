"""Unit tests for services/icp_canonicalizer.py and
services/industry_canonicalizer.py (local resolution paths only — no
embedding calls, no DB taxonomy)."""
import pytest

from services.icp_canonicalizer import (
    _normalize_seniority,
    _normalize_employee_bands,
    _extract_industries,
    _extract_geographies,
    _extract_job_titles,
    canonicalize_icp,
)
from services import industry_canonicalizer

pytestmark = pytest.mark.unit


# ---------------------------------------------------------------------------
# Seniority normalization
# ---------------------------------------------------------------------------

def test_seniority_free_text_to_tokens():
    assert _normalize_seniority(["C-Level executives"]) == ["c_suite"]
    assert _normalize_seniority(["Founders and Owners"]) == ["founder"]
    assert _normalize_seniority(["VP / Vice President"]) == ["vp"]
    assert _normalize_seniority(["Director of Ops"]) == ["director"]


def test_seniority_one_input_many_tokens_no_duplicates():
    tokens = _normalize_seniority(["CEO or Founder", "chief people officer"])
    assert tokens.count("c_suite") == 1
    assert "founder" in tokens


def test_seniority_unknown_input_yields_empty():
    assert _normalize_seniority(["astronaut"]) == []
    assert _normalize_seniority([]) == []


# ---------------------------------------------------------------------------
# Employee band normalization
# ---------------------------------------------------------------------------

def test_employee_bands():
    assert _normalize_employee_bands(["11-50 employees"]) == ["11-50"]
    assert _normalize_employee_bands(["Enterprise (1000+)"]) == ["1000+"]
    assert _normalize_employee_bands(["small teams", "mid-size firms"]) == ["11-50", "51-200"]
    assert _normalize_employee_bands(["whatever"]) == []


# ---------------------------------------------------------------------------
# Field extraction (key aliasing across profile/campaign docs)
# ---------------------------------------------------------------------------

def test_extract_prefers_first_matching_key():
    icp = {"target_industries": ["saas"], "icp_industries": ["ignored"]}
    assert _extract_industries(icp) == ["saas"]
    assert _extract_industries({"icp_industries": ["mining"]}) == ["mining"]
    assert _extract_industries({}) == []


def test_extract_geographies_and_titles():
    assert _extract_geographies({"icp_countries": ["Germany"]}) == ["Germany"]
    assert _extract_job_titles({"icp_job_titles": ["CTO"]}) == ["CTO"]


# ---------------------------------------------------------------------------
# canonicalize_icp end-to-end (no titles -> no embedding call; geographies
# resolved via the local country map / sqlite; industries via alias overrides)
# ---------------------------------------------------------------------------

async def test_canonicalize_icp_local_paths():
    icp = {
        "target_industries": ["SaaS", "Oil & Gas"],
        "target_geographies": ["United States", "Germany"],
        "target_seniority": ["C-Level", "Founder"],
        "target_company_sizes": ["11-50", "1000+"],
        # no job titles -> title_query_vec must stay None (no Gemini call)
    }
    result = await canonicalize_icp(icp)
    assert result["title_query_vec"] is None
    assert set(result["country_codes"]) == {"US", "DE"}
    assert "c_suite" in result["seniorities"] and "founder" in result["seniorities"]
    assert set(result["employee_bands"]) == {"11-50", "1000+"}
    assert "software_development" in result["industry_ids"]


async def test_canonicalize_icp_empty_input():
    result = await canonicalize_icp({})
    assert result == {
        "industry_ids": [],
        "suggested_industry_ids": [],
        "country_codes": [],
        "seniorities": [],
        "employee_bands": [],
        "title_query_vec": None,
    }


async def test_canonicalize_icp_ecommerce_strict_no_group_bleed():
    """"retail"/"ecommerce" must resolve strictly to their own ids only — they
    must not drag in the rest of the "Retail & Consumer" group (food_beverage,
    wholesale, supermarkets, consumer_goods, restaurants) via loose substring
    group matching ("retail" is a substring of the group name "Retail &
    Consumer", which used to expand the whole group). Those sibling ids
    should show up only as opt-in suggestions."""
    icp = {"target_industries": ["retail", "ecommerce"]}
    result = await canonicalize_icp(icp)

    assert set(result["industry_ids"]) == {"retail", "ecommerce"}
    for leaked_id in ("food_beverage", "wholesale", "supermarkets", "consumer_goods", "restaurants"):
        assert leaked_id not in result["industry_ids"]
        assert leaked_id in result["suggested_industry_ids"]


# ---------------------------------------------------------------------------
# industry_canonicalizer.resolve — alias paths (no DB / embeddings needed)
# ---------------------------------------------------------------------------

async def test_industry_exact_alias():
    result = await industry_canonicalizer.resolve("SaaS")
    assert result is not None
    assert result["id"] == "software_development"
    assert result["assign_method"] == "alias"
    assert result["confidence"] == 1.0


async def test_industry_alias_substring():
    result = await industry_canonicalizer.resolve("Enterprise SaaS platforms")
    assert result is not None
    assert result["id"] == "software_development"
    assert result["assign_method"] == "alias_substring"


async def test_industry_unresolvable_returns_none():
    assert await industry_canonicalizer.resolve("zqxwv nonsense industry") is None
    assert await industry_canonicalizer.resolve("") is None
    assert await industry_canonicalizer.resolve(None) is None


async def test_expand_icp_to_industry_ids_dedupes():
    ids = await industry_canonicalizer.expand_icp_to_industry_ids(
        ["SaaS", "computer software", "software"]
    )
    assert ids.count("software_development") == 1


async def test_expand_icp_to_industry_ids_strict_no_group_expansion():
    """A term must map to a whole taxonomy group only on exact normalized
    equality with the group alias (e.g. literally "retail & consumer").
    "retail" alone is a *substring* of that group name and used to expand
    into the entire group — it must now resolve to just its own id."""
    ids = await industry_canonicalizer.expand_icp_to_industry_ids(["retail"])
    assert ids == ["retail"]

    ids = await industry_canonicalizer.expand_icp_to_industry_ids(["Retail & Consumer"])
    group_ids = {
        entry["id"]
        for entry in industry_canonicalizer.LINKEDIN_INDUSTRIES
        if entry["group"] == "Retail & Consumer"
    }
    assert set(ids) == group_ids


async def test_suggest_industry_expansions_surfaces_group_siblings():
    """suggest_industry_expansions() reproduces the old loose group-match
    behavior as suggestions only, excluding ids already strictly resolved."""
    suggestions = await industry_canonicalizer.suggest_industry_expansions(
        ["retail", "ecommerce"]
    )
    assert "retail" not in suggestions
    assert "ecommerce" not in suggestions
    for sibling in ("food_beverage", "wholesale", "supermarkets", "consumer_goods", "restaurants"):
        assert sibling in suggestions


async def test_suggest_industry_expansions_empty_for_non_group_terms():
    assert await industry_canonicalizer.suggest_industry_expansions(["SaaS"]) == []
    assert await industry_canonicalizer.suggest_industry_expansions([]) == []


def test_normalize_industry():
    assert industry_canonicalizer._normalize_industry("  Oil & GAS  ") == "oil & gas"


def test_cosine_similarity_basics():
    cos = industry_canonicalizer._cosine_similarity
    assert cos([1, 0], [1, 0]) == pytest.approx(1.0)
    assert cos([1, 0], [0, 1]) == pytest.approx(0.0)
    assert cos([], [1, 2]) == 0.0
