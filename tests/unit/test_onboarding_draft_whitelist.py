"""_clean_draft is the only gate between wizard keystrokes and company_profiles."""
from routes.onboarding_wizard import _clean_draft


def test_strings_are_trimmed_and_blanks_dropped():
    out = _clean_draft({"company_name": "  Acme  ", "primary_cta": "   ", "sender_role": None})
    assert out == {"company_name": "Acme"}


def test_lists_are_cleaned_but_empties_preserved():
    out = _clean_draft({"services": ["SEO", "  ", " Ads "], "pain_points": []})
    assert out == {"services": ["SEO", "Ads"], "pain_points": []}


def test_unknown_and_mistyped_fields_are_dropped():
    out = _clean_draft({
        "account_id": "other-tenant",
        "onboarding_stage": 6,
        "services": "not-a-list",
        "target_industries": ["SaaS"],
    })
    assert out == {"target_industries": ["SaaS"]}
