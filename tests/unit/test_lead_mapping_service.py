"""Unit tests for services/lead_mapping_service.py — the deterministic pieces
(classify_row, apply_mapping/build_lead_fields, heuristic_mapping, normalizers).
No LLM / DB / paid calls exercised here."""
import pytest

from services.lead_mapping_service import (
    classify_row,
    apply_mapping,
    build_lead_fields,
    heuristic_mapping,
    normalize_domain,
    normalize_linkedin_url,
    is_person_linkedin,
    is_company_linkedin,
    company_domain_for_row,
)

pytestmark = pytest.mark.unit


# ---------------------------------------------------------------------------
# classify_row — the core deterministic contract
# ---------------------------------------------------------------------------

def test_company_row_only_company_signal():
    mapping = {"Company": "company_name", "URL": "company_linkedin_url"}
    row = {"Company": "Acme Inc", "URL": "https://www.linkedin.com/company/acme"}
    assert classify_row(row, mapping) == "company"


def test_company_row_domain_only():
    mapping = {"Website": "company_domain"}
    row = {"Website": "acme.com"}
    assert classify_row(row, mapping) == "company"


def test_person_with_linkedin_profile():
    mapping = {"Name": "full_name", "LinkedIn": "linkedin_url"}
    row = {"Name": "Jane Doe", "LinkedIn": "https://linkedin.com/in/janedoe"}
    assert classify_row(row, mapping) == "person"


def test_person_with_name_and_company_email_only_path():
    mapping = {"First": "first_name", "Last": "last_name", "Co": "company_name", "Site": "company_domain"}
    row = {"First": "John", "Last": "Smith", "Co": "Globex", "Site": "globex.com"}
    # name + resolvable company/domain, no linkedin -> person (email-only branch downstream)
    assert classify_row(row, mapping) == "person"


def test_person_with_direct_email_no_linkedin_no_company():
    mapping = {"Name": "full_name", "Email": "email"}
    row = {"Name": "Pat Lee", "Email": "pat@globex.com"}
    assert classify_row(row, mapping) == "person"


def test_unresolvable_name_only():
    mapping = {"Name": "full_name"}
    row = {"Name": "Solo Person"}
    assert classify_row(row, mapping) == "unresolvable"


def test_unresolvable_junk_row():
    mapping = {"Notes": "ignore", "ID": "ignore"}
    row = {"Notes": "call back later", "ID": "42"}
    assert classify_row(row, mapping) == "unresolvable"


def test_unresolvable_empty_cells():
    mapping = {"First": "first_name", "Company": "company_name"}
    row = {"First": "", "Company": None}
    assert classify_row(row, mapping) == "unresolvable"


def test_person_linkedin_wins_even_without_name_columns_but_full_name_present():
    # generic linkedin_url that is a company URL + a person name -> still person
    mapping = {"Name": "full_name", "LinkedIn": "linkedin_url"}
    row = {"Name": "Dana Fox", "LinkedIn": "https://www.linkedin.com/company/acme"}
    # company LI + name -> person (company signal counts)
    assert classify_row(row, mapping) == "person"


# ---------------------------------------------------------------------------
# apply_mapping / build_lead_fields
# ---------------------------------------------------------------------------

def test_apply_mapping_ignores_and_dedupes():
    mapping = {"A": "first_name", "B": "ignore", "C": "bogus_field"}
    row = {"A": "  Jane ", "B": "x", "C": "y"}
    out = apply_mapping(row, mapping)
    assert out == {"first_name": "Jane"}


def test_build_lead_fields_splits_full_name():
    mapping = {"Name": "full_name", "LinkedIn": "linkedin_url", "Co": "company_name"}
    row = {"Name": "Jane Q Doe", "LinkedIn": "linkedin.com/in/jane", "Co": "Acme"}
    f = build_lead_fields(row, mapping)
    assert f["first_name"] == "Jane"
    assert f["last_name"] == "Q Doe"
    assert f["full_name"] == "Jane Q Doe"
    assert f["linkedin"] == "https://www.linkedin.com/in/jane"
    assert f["company_name"] == "Acme"


def test_build_lead_fields_company_linkedin_from_generic_column():
    mapping = {"First": "first_name", "LinkedIn": "linkedin_url"}
    row = {"First": "Bob", "LinkedIn": "https://www.linkedin.com/company/globex"}
    f = build_lead_fields(row, mapping)
    assert f["linkedin"] is None  # not a person profile
    assert f["company_linkedin"] == "https://www.linkedin.com/company/globex"


# ---------------------------------------------------------------------------
# Normalizers
# ---------------------------------------------------------------------------

@pytest.mark.parametrize("raw,expected", [
    ("https://www.acme.com/about", "acme.com"),
    ("http://acme.io", "acme.io"),
    ("acme.com", "acme.com"),
    ("www.acme.com", "acme.com"),
    ("notadomain", None),
    ("", None),
    (None, None),
])
def test_normalize_domain(raw, expected):
    assert normalize_domain(raw) == expected


def test_normalize_linkedin_url_variants():
    assert normalize_linkedin_url("linkedin.com/in/jane/") == "https://www.linkedin.com/in/jane"
    assert normalize_linkedin_url("https://LINKEDIN.com/company/acme") == "https://www.linkedin.com/company/acme"
    assert normalize_linkedin_url("https://example.com/in/jane") is None


def test_is_person_vs_company_linkedin():
    assert is_person_linkedin("https://linkedin.com/in/jane") is True
    assert is_person_linkedin("https://linkedin.com/company/acme") is False
    assert is_company_linkedin("https://linkedin.com/company/acme") is True
    assert is_company_linkedin("https://linkedin.com/in/jane") is False


def test_company_domain_for_row_skips_free_email():
    assert company_domain_for_row({"company_domain": "acme.com"}) == "acme.com"
    assert company_domain_for_row({"email": "jane@globex.com"}) == "globex.com"
    assert company_domain_for_row({"email": "jane@gmail.com"}) is None


# ---------------------------------------------------------------------------
# heuristic_mapping (LLM fallback)
# ---------------------------------------------------------------------------

def test_heuristic_mapping_basic_headers():
    cols = ["First Name", "Last Name", "Job Title", "Company", "Email", "LinkedIn URL"]
    res = heuristic_mapping(cols)
    m = res["mapping"]
    assert m["First Name"] == "first_name"
    assert m["Last Name"] == "last_name"
    assert m["Job Title"] == "job_title"
    assert m["Company"] == "company_name"
    assert m["Email"] == "email"
    assert m["LinkedIn URL"] == "linkedin_url"


def test_heuristic_mapping_ambiguous_creates_question():
    cols = ["xyz123", "Company"]
    res = heuristic_mapping(cols)
    assert res["mapping"]["xyz123"] == "ignore"
    # low-confidence ignore should surface a clarifying question
    q_cols = [q["column"] for q in res["questions"]]
    assert "xyz123" in q_cols
    for q in res["questions"]:
        assert q["widget"] == "single_select"
        assert q["options"]


def test_heuristic_mapping_dedupes_single_value_fields():
    cols = ["Email", "Work Email"]
    res = heuristic_mapping(cols)
    fields = [res["mapping"]["Email"], res["mapping"]["Work Email"]]
    assert fields.count("email") == 1
