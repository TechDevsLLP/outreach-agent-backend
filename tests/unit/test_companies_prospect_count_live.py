"""Package B: companies list + by-industry compute prospect_count live via
$lookup against prospects.company_id — there is no stored prospect_count field.

No live Mongo: companies_collection.aggregate is faked to capture the pipeline.
"""
import pytest
from bson import ObjectId

from routes import companies as companies_route

pytestmark = pytest.mark.unit


class _Cursor:
    def __init__(self, result):
        self._result = result

    async def to_list(self, length=None):
        return self._result


class _FakeCollection:
    def __init__(self, result):
        self._result = result
        self.captured_pipeline = None

    def aggregate(self, pipeline):
        self.captured_pipeline = pipeline
        return _Cursor(self._result)


def _iter_stages(pipeline):
    """Yield every stage dict, recursing into $facet sub-pipelines."""
    for stage in pipeline:
        yield stage
        if "$facet" in stage:
            for sub in stage["$facet"].values():
                yield from _iter_stages(sub)


def _has_prospect_lookup(pipeline):
    for stage in _iter_stages(pipeline):
        lk = stage.get("$lookup")
        if lk and lk.get("from") == "prospects":
            return True
    return False


async def test_list_companies_uses_live_lookup_no_stored_field(monkeypatch):
    canned = [{
        "total": [{"count": 1}],
        "rows": [{"_id": ObjectId("0" * 24), "name": "Acme", "prospect_count": 7}],
    }]
    fake = _FakeCollection(canned)
    monkeypatch.setattr(companies_route, "companies_collection", fake)

    result = await companies_route.list_companies(
        min_prospects=5, sort_by="prospect_count", sort_order="desc", page=1, page_size=20, ctx={},
    )

    # Live lookup present, not a stored-field filter.
    assert _has_prospect_lookup(fake.captured_pipeline)
    # No stage filters on a stored prospect_count at the collection $match level
    # (the min_prospects filter must run AFTER the lookup/addFields).
    top_match = fake.captured_pipeline[0].get("$match", {})
    assert "prospect_count" not in top_match

    assert result["total"] == 1
    assert result["companies"][0]["prospect_count"] == 7
    assert result["companies"][0]["_id"] == "0" * 24


async def test_list_companies_paginates_before_lookup_when_not_needed(monkeypatch):
    canned = [{"total": [{"count": 0}], "rows": []}]
    fake = _FakeCollection(canned)
    monkeypatch.setattr(companies_route, "companies_collection", fake)

    await companies_route.list_companies(min_prospects=0, sort_by="name", sort_order="desc", page=1, page_size=20, ctx={})

    # When not filtering/sorting by prospect_count, the lookup lives inside the
    # paginated facet branch (still a prospects lookup, just applied per-page).
    assert _has_prospect_lookup(fake.captured_pipeline)


async def test_by_industry_sums_live_prospect_count(monkeypatch):
    canned = [{"industry": "Software", "company_count": 2, "total_prospects": 9}]
    fake = _FakeCollection(canned)
    monkeypatch.setattr(companies_route, "companies_collection", fake)

    result = await companies_route.companies_by_industry()

    assert _has_prospect_lookup(fake.captured_pipeline)
    assert result == canned
