"""Enrollment ranking must use only campaign/tenant-scoped state.

Locks two things:
  * the effective-score coalescing contract (null → unscored sorts last, a real
    0 stays 0), and
  * that the ranking no longer falls back to legacy shared-pool score fields.
"""

import inspect

import pytest

import routes.campaigns as campaigns

pytestmark = pytest.mark.unit


# Mirror of the pipeline rule `{"$ifNull": ["$state_data.ai_score", -1]}`.
UNSCORED_SENTINEL = -1


def _effective_score(state_ai_score):
    return state_ai_score if state_ai_score is not None else UNSCORED_SENTINEL


def test_zero_is_a_real_score_and_null_is_unscored():
    assert _effective_score(0) == 0          # real zero preserved
    assert _effective_score(None) == UNSCORED_SENTINEL  # unscored


def test_unscored_sorts_after_zero_and_positive_scores():
    rows = [
        {"pid": "unscored", "ai": None},
        {"pid": "zero", "ai": 0},
        {"pid": "mid", "ai": 55},
        {"pid": "top", "ai": 90},
    ]
    ordered = sorted(rows, key=lambda r: _effective_score(r["ai"]), reverse=True)
    assert [r["pid"] for r in ordered] == ["top", "mid", "zero", "unscored"]


def test_ranking_source_has_no_legacy_shared_pool_fallbacks():
    src = inspect.getsource(campaigns.list_enrolled_prospects)
    # The effective sort score must come only from prospect_state.ai_score.
    assert '"$ifNull": ["$state_data.ai_score", -1]' in src
    # Legacy shared-pool score fields must not appear as fallbacks anywhere in
    # the enrollment ranking/response builder.
    assert "prospect_data.ai_prospect_score" not in src
    assert "last_campaign_rule_score" not in src
