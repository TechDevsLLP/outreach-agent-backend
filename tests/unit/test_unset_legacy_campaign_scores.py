"""Offline tests for the legacy campaign-score cleanup helper.

Verifies the helper unsets only intended fields, is idempotent, is dry-run by
default, and refuses to run against any collection other than ``prospects``.
"""

import pytest

from scripts.unset_legacy_campaign_scores import (
    LEGACY_CAMPAIGN_SCORE_FIELDS,
    legacy_unset_filter,
    legacy_unset_update,
    unset_legacy_campaign_scores,
)

pytestmark = pytest.mark.unit


class FakeCollection:
    """Minimal in-memory async Mongo-ish collection for prospects."""

    def __init__(self, name, docs):
        self.name = name
        self.docs = [dict(d) for d in docs]

    def _matches(self, doc, query):
        # Supports the {"$or": [{field: {"$exists": True}}, ...]} shape used here.
        for clause in query["$or"]:
            (field, cond), = clause.items()
            if cond.get("$exists") and field in doc:
                return True
        return False

    async def count_documents(self, query):
        return sum(1 for d in self.docs if self._matches(d, query))

    async def update_many(self, query, update):
        unset = update["$unset"]
        modified = 0
        for d in self.docs:
            if self._matches(d, query):
                for field in unset:
                    d.pop(field, None)
                modified += 1

        class _Result:
            modified_count = modified

        return _Result()


def _prospect_with_legacy():
    return {
        "_id": "p1",
        "full_name": "Ada Lovelace",
        "linkedin": "linkedin.com/in/ada",
        "title_vec": [0.1, 0.2],
        # canonical/overlay fields that must be preserved
        "prospect_intelligence_base": {"pitch": "x"},
        # legacy campaign-score residue that must be removed
        "ai_prospect_score": 87.0,
        "fit_score": 0,  # zero is a real value but still a legacy field → removed
        "campaign_rule_score": 42.0,
        "last_campaign_rule_score": 41.0,
        "scoring_version": "v1",
    }


def test_filter_and_update_cover_exactly_the_legacy_fields():
    query = legacy_unset_filter()
    fields_in_filter = {list(c)[0] for c in query["$or"]}
    assert fields_in_filter == set(LEGACY_CAMPAIGN_SCORE_FIELDS)
    assert set(legacy_unset_update()["$unset"]) == set(LEGACY_CAMPAIGN_SCORE_FIELDS)


@pytest.mark.asyncio
async def test_dry_run_reports_but_does_not_modify():
    col = FakeCollection("prospects", [_prospect_with_legacy()])
    report = await unset_legacy_campaign_scores(col, execute=False)

    assert report["matched"] == 1
    assert report["modified"] is None
    assert report["executed"] is False
    # Nothing changed.
    assert col.docs[0]["ai_prospect_score"] == 87.0
    assert col.docs[0]["fit_score"] == 0


@pytest.mark.asyncio
async def test_execute_unsets_only_intended_fields_and_is_idempotent():
    col = FakeCollection("prospects", [_prospect_with_legacy()])

    first = await unset_legacy_campaign_scores(col, execute=True)
    assert first["matched"] == 1
    assert first["modified"] == 1

    doc = col.docs[0]
    # Legacy campaign-score fields removed (including the real 0 value).
    for field in LEGACY_CAMPAIGN_SCORE_FIELDS:
        assert field not in doc
    # Canonical + overlay + identity fields preserved.
    assert doc["full_name"] == "Ada Lovelace"
    assert doc["linkedin"] == "linkedin.com/in/ada"
    assert doc["title_vec"] == [0.1, 0.2]
    assert doc["prospect_intelligence_base"] == {"pitch": "x"}

    # Idempotent: a second run matches nothing and modifies nothing.
    second = await unset_legacy_campaign_scores(col, execute=True)
    assert second["matched"] == 0
    assert second["modified"] is None


@pytest.mark.asyncio
async def test_document_without_legacy_fields_is_untouched():
    clean = {"_id": "p2", "full_name": "Grace Hopper", "linkedin": "x"}
    col = FakeCollection("prospects", [clean])
    report = await unset_legacy_campaign_scores(col, execute=True)
    assert report["matched"] == 0
    assert col.docs[0] == clean


@pytest.mark.asyncio
async def test_refuses_any_collection_other_than_prospects():
    for forbidden in ("providers", "email_accounts", "prospect_state", "companies"):
        col = FakeCollection(forbidden, [_prospect_with_legacy()])
        with pytest.raises(ValueError, match="only 'prospects'"):
            await unset_legacy_campaign_scores(col, execute=True)
        # Guard fires before any write.
        assert "ai_prospect_score" in col.docs[0]
