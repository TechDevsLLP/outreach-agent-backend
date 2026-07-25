"""Outreach prompt builders must include onboarding company context:
sender voice profile, differentiators, case studies (dict or string shape),
target market, and banned phrases.
"""
from utils.prompts import (
    _select_best_case_study,
    build_campaign_batch_outreach_prompt,
    build_campaign_followup_prompt,
    build_campaign_outreach_prompt,
    build_onboarding_preview_message_prompt,
)


COMPANY_PROFILE = {
    "company_name": "Mentopreneur",
    "sender_name": "Neel Shah",
    "sender_role": "Founder",
    "services": ["Brand Identity", "Performance Marketing"],
    "differentiators": ["Founder-led with 10+ years experience"],
    "target_market": "Growth-stage D2C brands",
    "case_studies": [
        {"client": "ChargeZone", "outcome": "Transformed B2B brand", "metric": "32% sales lift", "industry": "EV"},
    ],
    "sender_voice_profile": {
        "tone_markers": ["direct", "no-nonsense"],
        "synthesized_summary": "Short punchy declaratives.",
    },
    "banned_phrases": ["synergy", "circle back"],
}

CAMPAIGN = {
    "message_tone": "professional",
    "value_proposition": "Video content at scale",
    "pain_point": "Inconsistent content",
    "sender_name": "Neel Shah",
    "cta_type": "reply",
}

PROSPECT = {"full_name": "Jane Doe", "job_title": "Head of Marketing", "company_name": "Acme", "industry": "EV"}


def test_select_best_case_study_accepts_strings():
    cs = _select_best_case_study(["Helped Acme grow 2x"], PROSPECT)
    assert cs is not None and cs["outcome"] == "Helped Acme grow 2x"


def test_select_best_case_study_mixed_and_industry_match():
    studies = ["generic string", {"client": "EVCo", "outcome": "won", "industry": "EV"}]
    cs = _select_best_case_study(studies, PROSPECT)
    assert cs["client"] == "EVCo"


def test_batch_prompt_includes_company_context_and_voice():
    prompt = build_campaign_batch_outreach_prompt(
        CAMPAIGN, [("e1", PROSPECT)], "linkedin_connection", company_profile=COMPANY_PROFILE
    )
    assert "Voice profile" in prompt
    assert "direct, no-nonsense" in prompt
    assert "Founder-led with 10+ years experience" in prompt
    assert "ChargeZone" in prompt
    assert "Growth-stage D2C brands" in prompt
    assert "Never use these phrases: synergy; circle back" in prompt


def test_batch_prompt_without_profile_still_builds():
    prompt = build_campaign_batch_outreach_prompt(CAMPAIGN, [("e1", PROSPECT)], "email")
    assert "Jane Doe" in prompt and "Voice profile" not in prompt


def test_single_prompt_includes_voice_and_banned():
    prompt = build_campaign_outreach_prompt(
        PROSPECT, {}, {}, CAMPAIGN, company_profile=COMPANY_PROFILE
    )
    assert "Voice profile" in prompt
    assert "Never use these phrases" in prompt
    assert "ChargeZone" in prompt


def test_followup_prompt_includes_voice_diff_case_study():
    prompt = build_campaign_followup_prompt(
        PROSPECT, {}, {}, CAMPAIGN, {"channel": "email", "step_index": 3},
        company_profile=COMPANY_PROFILE,
    )
    assert "Voice profile" in prompt
    assert "Founder-led with 10+ years experience" in prompt
    assert "ChargeZone" in prompt


def test_preview_prompt_includes_differentiators_and_case_study():
    prompt = build_onboarding_preview_message_prompt(
        COMPANY_PROFILE, PROSPECT, voice_profile=COMPANY_PROFILE["sender_voice_profile"]
    )
    assert "Differentiators" in prompt
    assert "ChargeZone" in prompt
    assert "Voice profile" in prompt
