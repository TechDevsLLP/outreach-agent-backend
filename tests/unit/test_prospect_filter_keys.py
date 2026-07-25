"""Pure unit tests for denormalized prospect-state filter keys."""
import pytest

from utils.prospect_filter_keys import (
    PK_PROJECTION,
    build_filter_keys,
)

pytestmark = pytest.mark.unit


def test_build_filter_keys_full_doc():
    pk = build_filter_keys({
        "full_name": "Jane Roe", "email": "jane@x.test", "company_name": "XCo",
        "job_title": "CTO", "company_industry_id": "saas",
        "location": {"country_code": "US", "country": "United States", "city": "NYC"},
        "seniority": "c_suite", "seniority_level": None,
        "enrichment_status": "completed", "linkedin": "https://linkedin.com/in/jane",
        "unrelated_field": "ignored",
    })
    assert pk == {
        "full_name": "Jane Roe", "email": "jane@x.test", "company_name": "XCo",
        "job_title": "CTO", "company_industry_id": "saas",
        "country_code": "US", "country": "United States",
        "seniority": "c_suite", "seniority_level": None,
        "enrichment_status": "completed", "linkedin": "https://linkedin.com/in/jane",
    }


def test_build_filter_keys_missing_fields_become_none():
    pk = build_filter_keys({"full_name": "Min Doc"})
    assert pk["full_name"] == "Min Doc"
    for key in ("email", "company_industry_id", "country_code", "linkedin",
                "enrichment_status", "seniority"):
        assert pk[key] is None
    # location may be explicitly None
    assert build_filter_keys({"location": None})["country_code"] is None


def test_pk_projection_covers_all_built_keys():
    """Every field build_filter_keys reads must be in PK_PROJECTION, so sites
    that fetch with the projection produce complete pk subdocs."""
    sentinel_doc = {
        "full_name": "x", "email": "x", "company_name": "x", "job_title": "x",
        "company_industry_id": "x",
        "location": {"country_code": "x", "country": "x"},
        "seniority": "x", "seniority_level": "x",
        "enrichment_status": "x", "linkedin": "x",
    }
    pk = build_filter_keys(sentinel_doc)
    assert all(v == "x" for v in pk.values())
    flat_proj = set()
    for key in PK_PROJECTION:
        flat_proj.add(key.split(".")[0])
    for src in ("full_name", "email", "company_name", "job_title",
                "company_industry_id", "location", "seniority",
                "seniority_level", "enrichment_status", "linkedin"):
        assert src in flat_proj, f"{src} missing from PK_PROJECTION"
