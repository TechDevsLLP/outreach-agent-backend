"""Unit tests for utils/scoring.py — pure logic, no DB."""
import pytest

from utils.scoring import (
    score_prospect_v2,
    score_prospect_for_campaign,
    score_company_fit_rule_based,
    is_decision_maker_rule_based,
    passes_title_gate,
    tier_from_score,
)

pytestmark = pytest.mark.unit


# ---------------------------------------------------------------------------
# score_prospect_v2
# ---------------------------------------------------------------------------

def test_v2_empty_lead_scores_low_but_not_zero():
    # An empty lead still earns "engagement opportunity" points (no description,
    # no tech stack) by design.
    total, breakdown = score_prospect_v2({})
    assert 0 <= total <= 10
    assert breakdown["seniority"] == 0
    assert breakdown["email_verified"] == 0
    assert sum(breakdown.values()) == total


def test_v2_ideal_prospect_scores_high_and_capped_at_100():
    lead = {
        "seniority_level": "c_suite",
        "industry": "Oil & Gas",
        "company_size": 500,
        "company_annual_revenue_clean": 100_000_000,
        "city": "Houston",
        "country": "United States",
        "job_title": "Chief Executive Officer",
        "headline": "CEO | oil & gas | business development",
        "email": "ceo@example.com",
        "company_founded_year": 1980,
    }
    total, breakdown = score_prospect_v2(lead)
    assert total <= 100.0
    assert breakdown["seniority"] == 20
    assert breakdown["industry"] == 15
    assert breakdown["company_size"] == 12
    assert breakdown["revenue"] == 12
    assert breakdown["location"] == 8
    assert breakdown["email_verified"] == 5
    assert breakdown["recency_signal"] == 5  # 40+ yrs old, 500 employees
    assert total >= 80


def test_v2_email_presence_signal():
    base = {"seniority_level": "vp", "industry": "saas"}
    with_email, bd1 = score_prospect_v2({**base, "email": "x@y.com"})
    without_email, bd2 = score_prospect_v2({**base, "email": None})
    whitespace_email, bd3 = score_prospect_v2({**base, "email": "   "})
    assert bd1["email_verified"] == 5
    assert bd2["email_verified"] == 0
    assert bd3["email_verified"] == 0
    assert with_email - without_email == 5


def test_v2_bad_numeric_inputs_do_not_crash():
    lead = {
        "company_size": "not-a-number",
        "company_annual_revenue_clean": "N/A",
        "company_founded_year": "unknown",
    }
    total, breakdown = score_prospect_v2(lead)
    assert breakdown["company_size"] == 0
    assert breakdown["revenue"] == 0
    assert breakdown["recency_signal"] == 0
    assert total >= 0


def test_v2_icp_alignment_bonus():
    lead = {
        "industry": "computer software",
        "job_title": "VP Marketing",
    }
    icp = {
        "target_industry": "software",
        "target_industries": ["computer software"],
        "target_job_titles": ["vp marketing"],
    }
    _, with_icp = score_prospect_v2(lead, icp_context=icp)
    _, without_icp = score_prospect_v2(lead)
    assert with_icp["icp_alignment"] == 5
    assert without_icp["icp_alignment"] == 0


def test_v2_company_size_bands():
    def size_score(n):
        return score_prospect_v2({"company_size": n})[1]["company_size"]

    assert size_score(19) == 6      # 10-19
    assert size_score(20) == 12     # sweet spot lower edge
    assert size_score(5000) == 12   # sweet spot upper edge
    assert size_score(5001) == 8
    assert size_score(10001) == 4
    assert size_score(3) == 0


def test_v2_digital_maturity_gap():
    rich_no_tech = {
        "company_annual_revenue_clean": 50_000_000,
        "company_technologies": None,
        "company_website": None,
        "company_domain": None,
    }
    _, bd = score_prospect_v2(rich_no_tech)
    assert bd["digital_maturity_gap"] == 5  # 3 (no tech) + 2 (no website)

    rich_with_tech = {**rich_no_tech, "company_technologies": ["Salesforce"], "company_website": "x.com"}
    _, bd2 = score_prospect_v2(rich_with_tech)
    assert bd2["digital_maturity_gap"] == 0


# ---------------------------------------------------------------------------
# tier_from_score
# ---------------------------------------------------------------------------

@pytest.mark.parametrize("score,tier", [
    (None, None), (0, "cold"), (59.9, "cold"), (60, "warm"),
    (79.9, "warm"), (80, "hot"), (100, "hot"),
])
def test_tier_from_score(score, tier):
    assert tier_from_score(score) == tier


# ---------------------------------------------------------------------------
# score_prospect_for_campaign
# ---------------------------------------------------------------------------

def test_campaign_score_full_match():
    campaign = {
        "seniorities": ["c_suite"],
        "icp_job_titles": ["chief executive officer"],
        "icp_industries": ["chemicals"],
        "icp_company_size_min": 50,
        "icp_company_size_max": 1000,
        "icp_countries": ["united states"],
    }
    prospect = {
        "seniority": "c_suite",
        "job_title": "Chief Executive Officer",
        "industry": "chemicals",
        "company_size": 200,
        "country": "United States",
        "email": "ceo@chem.com",
        "linkedin": "https://linkedin.com/in/ceo",
    }
    score = score_prospect_for_campaign(prospect, campaign)
    # 10 + 35 + 18 + 12 + 5 + 15 + 5 = 100
    assert score == 100.0


def test_campaign_score_no_email_no_linkedin_penalty():
    campaign = {"icp_job_titles": ["ceo"]}
    with_contact = score_prospect_for_campaign(
        {"job_title": "ceo", "email": "a@b.c", "linkedin": "https://linkedin.com/in/x"}, campaign)
    without_contact = score_prospect_for_campaign({"job_title": "ceo"}, campaign)
    assert with_contact - without_contact == 20  # 15 email + 5 linkedin


def test_campaign_score_weight_overrides():
    campaign = {
        "icp_job_titles": ["ceo"],
        "scoring_weights": {"title_match": 50, "has_email": 0},
    }
    prospect = {"job_title": "ceo", "email": "a@b.c"}
    score = score_prospect_for_campaign(prospect, campaign)
    # title exact 50, email weight zeroed; country fallback 0.3*5=1.5 absent (no country)
    assert score == 50.0


def test_campaign_score_never_exceeds_100():
    campaign = {
        "icp_job_titles": ["ceo"],
        "scoring_weights": {"title_match": 200},
    }
    prospect = {"job_title": "ceo", "email": "a@b.c", "linkedin": "https://linkedin.com/in/x"}
    assert score_prospect_for_campaign(prospect, campaign) == 100.0


def test_campaign_score_location_subdocument_fallback():
    campaign = {"icp_countries": ["united states"]}
    prospect = {"location": {"country": "United States"}}
    assert score_prospect_for_campaign(prospect, campaign) == 5.0


# ---------------------------------------------------------------------------
# is_decision_maker_rule_based
# ---------------------------------------------------------------------------

@pytest.mark.parametrize("prospect,expected", [
    ({"seniority_level": "founder", "job_title": "whatever"}, True),
    ({"seniority_level": "c_suite", "job_title": "software engineer"}, True),  # seniority wins
    ({"seniority_level": "", "job_title": "software engineer"}, False),
    ({"seniority_level": "vp", "job_title": "vp of engineering"}, True),
    ({"seniority_level": "manager", "job_title": "marketing manager"}, True),
    ({"seniority_level": "", "job_title": "receptionist"}, False),
    ({"seniority_level": "", "job_title": "head of growth"}, True),
    ({}, False),
])
def test_decision_maker_rules(prospect, expected):
    result, reason = is_decision_maker_rule_based(prospect)
    assert result is expected, reason


# ---------------------------------------------------------------------------
# passes_title_gate
# ---------------------------------------------------------------------------

def test_title_gate_blocklist_keyword():
    ok, reason = passes_title_gate(
        {"job_title": "Crypto Growth Hacker"}, ["c_suite"], exclude_keywords=["crypto"])
    assert ok is False and reason == "title_keyword_blocklisted"


def test_title_gate_non_dm_title_blocked_for_senior_icp():
    ok, reason = passes_title_gate({"job_title": "Software Engineer"}, ["c_suite"])
    assert ok is False and reason == "non_decision_maker_title"


def test_title_gate_non_dm_title_allowed_for_junior_icp():
    # Gate only applies when the ICP targets senior levels
    ok, reason = passes_title_gate({"job_title": "Software Engineer"}, ["mid"])
    assert ok is True and reason is None


def test_title_gate_passes_clean_title():
    ok, reason = passes_title_gate({"job_title": "Chief Revenue Officer"}, ["c_suite"])
    assert ok is True and reason is None


# ---------------------------------------------------------------------------
# score_company_fit_rule_based
# ---------------------------------------------------------------------------

def test_company_fit_only_company_dimensions():
    prospect = {
        "industry": "mining", "company_size": 100,
        "company_annual_revenue_clean": 50_000_000,
        "city": "denver", "country": "united states",
        # Person fields must be ignored:
        "seniority_level": "c_suite", "email": "x@y.z", "job_title": "CEO",
    }
    total, breakdown = score_company_fit_rule_based(prospect)
    assert set(breakdown) == {"industry", "company_size", "revenue", "location"}
    assert breakdown["industry"] == 15
    assert breakdown["company_size"] == 12
    assert breakdown["revenue"] == 12
    assert breakdown["location"] == 8
    assert total == 47
