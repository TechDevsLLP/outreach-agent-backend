"""Unit tests for the Apify actor ID maps, company-size → headcount-band
mapping, and DB-company scoring gate in services/curated_discovery_service.py,
plus the Short-mode employee transform in services/employee_scraper_service.py.

Pure logic / no live DB, no Apify calls.
"""
import pytest

from services import curated_discovery_service as cds
from services.employee_scraper_service import transform_employee_to_prospect
from services.curated_discovery_service import _extract_company_url_from_employee as _extract_company_url

pytestmark = pytest.mark.unit


# ---------------------------------------------------------------------------
# Seniority ID map — director/manager were swapped; verified against the live
# actor schema: 210 = Experienced Manager, 220 = Director.
# ---------------------------------------------------------------------------

def test_seniority_director_maps_to_220():
    assert cds._icp_seniority_to_actor_ids(["director"]) == ["220"]


def test_seniority_head_maps_to_220():
    assert cds._icp_seniority_to_actor_ids(["head"]) == ["220"]


def test_seniority_manager_maps_to_210():
    assert cds._icp_seniority_to_actor_ids(["manager"]) == ["210"]


def test_seniority_other_levels_unchanged():
    assert cds._icp_seniority_to_actor_ids(["c_suite"]) == ["310"]
    assert cds._icp_seniority_to_actor_ids(["founder"]) == ["320"]
    assert cds._icp_seniority_to_actor_ids(["vp"]) == ["300"]
    assert cds._icp_seniority_to_actor_ids(["senior"]) == ["120"]


def test_seniority_unknown_label_dropped():
    assert cds._icp_seniority_to_actor_ids(["intern"]) == []


def test_seniority_dedupes_and_sorts():
    assert cds._icp_seniority_to_actor_ids(["director", "head", "manager"]) == ["210", "220"]


# ---------------------------------------------------------------------------
# Function ID map — LinkedIn standard taxonomy (functionIds 1-26), verified
# against the live actor schema. Previous map used made-up IDs.
# ---------------------------------------------------------------------------

@pytest.mark.parametrize("label,expected_id", [
    ("engineering", "8"),
    ("finance", "10"),
    ("hr", "12"),
    ("human_resources", "12"),
    ("operations", "18"),
    ("product", "19"),
    ("product_management", "19"),
    ("marketing", "15"),
    ("sales", "25"),
    ("business_development", "4"),
    ("information_technology", "13"),
    ("it", "13"),
    ("legal", "14"),
    ("consulting", "6"),
    ("customer_success", "26"),
    ("support", "26"),
])
def test_function_id_mapping(label, expected_id):
    assert cds._icp_function_to_actor_ids([label]) == [expected_id]


def test_function_id_mapping_case_and_space_insensitive():
    assert cds._icp_function_to_actor_ids(["Human Resources"]) == ["12"]
    assert cds._icp_function_to_actor_ids(["Product Management"]) == ["19"]


def test_function_id_mapping_unknown_dropped():
    assert cds._icp_function_to_actor_ids(["astrology"]) == []


# ---------------------------------------------------------------------------
# companyHeadcount band mapping — bands A-I, inclusive overlap with the
# campaign's icp_company_size_min/max range.
# ---------------------------------------------------------------------------

def test_headcount_bands_no_range_returns_empty():
    assert cds._icp_size_to_headcount_bands(None, None) == []


def test_headcount_bands_exact_small_company():
    # 1-10 employees → band B only
    assert cds._icp_size_to_headcount_bands(1, 10) == ["B"]


def test_headcount_bands_min_only_is_open_ended():
    bands = cds._icp_size_to_headcount_bands(5001, None)
    assert bands == ["H", "I"]


def test_headcount_bands_max_only_starts_from_zero():
    bands = cds._icp_size_to_headcount_bands(None, 50)
    assert bands == ["A", "B", "C"]


def test_headcount_bands_range_spanning_several_bands():
    # 51-1000 spans D, E, F
    assert cds._icp_size_to_headcount_bands(51, 1000) == ["D", "E", "F"]


def test_headcount_bands_huge_company_only():
    assert cds._icp_size_to_headcount_bands(10001, None) == ["I"]


def test_headcount_bands_self_employed():
    assert cds._icp_size_to_headcount_bands(0, 0) == ["A"]


# ---------------------------------------------------------------------------
# DB-company scoring gate — Stage A DB companies must be scored with the same
# deterministic scorer + _COMPANY_SCORE_THRESHOLD gate as Stage B, instead of
# the removed hard-coded _icp_score=80.0.
# ---------------------------------------------------------------------------

def test_db_company_sc_dict_has_no_hardcoded_score():
    """_db_company_to_sc is a closure defined inside run_fast_discovery, so we
    assert on the *source contract* instead: _score_company_deterministic must
    be usable on a DB-shaped sourced-company dict and produce a real ICP-driven
    score, not a fixed pre-qualified constant."""
    db_shaped_sc = {
        "company_linkedin_url": "https://www.linkedin.com/company/acme",
        "company_name": "Acme Robotics",
        "industry": {"id": "eng-1", "label": "Industrial Automation", "raw": "Industrial Automation"},
        "location": {"country": "United States"},
        "employee_size_estimate": "250",
        "description": "Builds robots for factories.",
    }
    score = cds._score_company_deterministic(db_shaped_sc, "industrial automation companies in the united states")
    assert score >= cds._COMPANY_SCORE_THRESHOLD


def test_db_company_off_icp_scores_below_threshold():
    """A DB company whose industry has nothing to do with the ICP prompt (and
    no other matching signals) must score low enough to be gated out — this is
    exactly the failure mode the hard-coded 80.0 masked."""
    db_shaped_sc = {
        "company_linkedin_url": "https://www.linkedin.com/company/megacorp",
        "company_name": "MegaCorp Holdings",
        "industry": {"id": "other-1", "label": "Diversified Conglomerate", "raw": "Diversified Conglomerate"},
        "location": {"country": "Elsewhere"},
        "employee_size_estimate": None,
        "description": None,
    }
    score = cds._score_company_deterministic(db_shaped_sc, "boutique coffee roasters in seattle")
    assert score < cds._COMPANY_SCORE_THRESHOLD


# ---------------------------------------------------------------------------
# Short-mode employee transform — Short mode ($4/1k, now the primary
# profileScraperMode) returns `currentPositions` (plural) with a `position`
# title field and no email fields.
# ---------------------------------------------------------------------------

_SOURCED_COMPANY = {
    "company_name": "Acme Robotics",
    "company_linkedin_url": "https://www.linkedin.com/company/acme",
    "company_domain": "acme.com",
    "industry": {"id": "eng-1", "label": "Robotics", "group": "Engineering"},
    "employee_band": "51-200",
}


def test_short_mode_employee_transform_title_and_company():
    short_mode_employee = {
        "firstName": "Jane",
        "lastName": "Doe",
        "linkedinUrl": "https://www.linkedin.com/in/janedoe",
        "currentPositions": [{
            "position": "VP of Engineering",
            "companyName": "Acme Robotics",
            "companyLinkedinUrl": "https://www.linkedin.com/company/acme",
        }],
    }
    prospect = transform_employee_to_prospect(short_mode_employee, _SOURCED_COMPANY)
    assert prospect["job_title"] == "VP of Engineering"
    assert prospect["company_name"] == "Acme Robotics"
    assert prospect["first_name"] == "Jane"
    assert prospect["last_name"] == "Doe"


def test_short_mode_employee_transform_has_no_email():
    """Short mode never returns email fields — the prospect's email must come
    back None so the GrowthToolkit email finder is the one filling it in."""
    short_mode_employee = {
        "firstName": "Jane",
        "lastName": "Doe",
        "linkedinUrl": "https://www.linkedin.com/in/janedoe",
        "currentPositions": [{"position": "VP of Engineering", "companyName": "Acme Robotics"}],
    }
    prospect = transform_employee_to_prospect(short_mode_employee, _SOURCED_COMPANY)
    assert prospect["email"] is None


def test_full_mode_employee_transform_still_works_backward_compat():
    """Full/Full+email mode (singular currentPosition + direct email fields)
    must still transform correctly for backward compatibility."""
    full_mode_employee = {
        "firstName": "Jane",
        "lastName": "Doe",
        "linkedinUrl": "https://www.linkedin.com/in/janedoe",
        "currentPosition": [{
            "title": "VP of Engineering",
            "companyName": "Acme Robotics",
            "companyLinkedinUrl": "https://www.linkedin.com/company/acme",
        }],
        "emails": ["jane@acme.com"],
    }
    prospect = transform_employee_to_prospect(full_mode_employee, _SOURCED_COMPANY)
    assert prospect["job_title"] == "VP of Engineering"
    assert prospect["email"] == "jane@acme.com"


def test_extract_company_url_prefers_currentpositions_plural():
    short_mode_employee = {
        "currentPositions": [{"companyLinkedinUrl": "https://www.linkedin.com/company/acme"}],
    }
    assert _extract_company_url(short_mode_employee) == "https://www.linkedin.com/company/acme"


def test_extract_company_url_falls_back_to_currentposition_singular():
    legacy_employee = {
        "currentPosition": [{"companyLinkedinUrl": "https://www.linkedin.com/company/legacy"}],
    }
    assert _extract_company_url(legacy_employee) == "https://www.linkedin.com/company/legacy"


def test_extract_company_url_meta_query_wins_over_both():
    employee = {
        "_meta": {"query": {"currentCompanies": ["https://www.linkedin.com/company/exact-match"]}},
        "currentPositions": [{"companyLinkedinUrl": "https://www.linkedin.com/company/wrong"}],
    }
    assert _extract_company_url(employee) == "https://www.linkedin.com/company/exact-match"
