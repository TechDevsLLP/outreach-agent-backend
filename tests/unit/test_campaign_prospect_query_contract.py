"""Campaign-aware prospect list contract tests (offline, no providers)."""

from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest
from bson import ObjectId

from routes import prospects as prospect_routes


pytestmark = pytest.mark.unit


class _Cursor:
    def __init__(self, rows):
        self.rows = rows

    async def to_list(self, _length):
        return self.rows


class _FindCollection:
    def __init__(self, rows):
        self.rows = rows
        self.query = None
        self.projection = None

    def find(self, query, projection=None):
        self.query = query
        self.projection = projection
        return _Cursor(self.rows)


class _CampaignStateCollection:
    def __init__(self, response):
        self.response = response
        self.pipeline = None

    def aggregate(self, pipeline):
        self.pipeline = pipeline
        return _Cursor([self.response])


async def test_campaign_list_uses_membership_then_canonical_score_page(monkeypatch):
    account_id = ObjectId("64b000000000000000000001")
    campaign_id = ObjectId("64b000000000000000000002")
    high_id = ObjectId("64b000000000000000000003")
    zero_id = ObjectId("64b000000000000000000004")

    campaigns = SimpleNamespace(
        find_one=AsyncMock(
            return_value={"_id": campaign_id, "scoring_version": "fit-v2"}
        )
    )
    enrollments = SimpleNamespace(
        distinct=AsyncMock(return_value=[zero_id, high_id])
    )
    shared = _FindCollection(
        [
            {"_id": zero_id, "full_name": "Zero Fit", "source": "apollo"},
            {"_id": high_id, "full_name": "High Fit", "source": "linkedin"},
        ]
    )
    overlays = _FindCollection(
        [
            {"prospect_id": str(high_id), "status": "new"},
            {"prospect_id": str(zero_id), "status": "contacted"},
        ]
    )
    overlays.distinct = AsyncMock(return_value=[str(high_id), str(zero_id)])
    campaign_states = _CampaignStateCollection(
        {
            "total": [{"n": 2}],
            # Simulate Mongo's authoritative score ordering; shared rows above
            # deliberately arrive in the opposite order.
            "page": [
                {
                    "prospect_id": str(high_id),
                    "scoring_version": "fit-v2",
                    "score": {
                        "value": 91,
                        "version": "fit-v2",
                        "reasoning": "Strong founder match",
                        "completeness": 0.9,
                        "breakdown": {"title_match": 50},
                    },
                    "enrichment": {
                        "state": "succeeded",
                        "result": {"provider": "growthtoolkit"},
                    },
                },
                {
                    "prospect_id": str(zero_id),
                    "scoring_version": "fit-v2",
                    "score": {
                        "value": 0,
                        "version": "fit-v2",
                        "reasoning": "Excluded",
                        "breakdown": {},
                    },
                    "enrichment": {"state": "not_found"},
                },
            ],
        }
    )

    monkeypatch.setattr(prospect_routes, "campaigns_collection", campaigns)
    monkeypatch.setattr(
        prospect_routes, "campaign_enrollments_collection", enrollments
    )
    monkeypatch.setattr(prospect_routes, "prospects_collection", shared)
    monkeypatch.setattr(prospect_routes, "prospect_state_collection", overlays)
    monkeypatch.setattr(
        prospect_routes.database_module,
        "db",
        {"campaign_prospect_state": campaign_states},
    )

    response = await prospect_routes._list_campaign_prospects(
        campaign_id=str(campaign_id),
        account_id=account_id,
        page=1,
        page_size=50,
        min_score=None,
        sort_order="desc",
        status=None,
        overlay_filter={},
    )

    assert [row["full_name"] for row in response["prospects"]] == [
        "High Fit",
        "Zero Fit",
    ]
    assert response["prospects"][0]["campaign_score"] == {
        "value": 91,
        "version": "fit-v2",
        "reasoning": "Strong founder match",
        "completeness": 0.9,
        "breakdown": {"title_match": 50},
        "scored_at": None,
    }
    assert response["prospects"][1]["campaign_score"]["value"] == 0
    assert response["prospects"][0]["campaign_enrichment"]["ownership"] == "campaign"
    assert response["prospects"][0]["campaign_enrichment"]["source"] == "growthtoolkit"
    assert response["prospects"][0]["campaign_enrichment"]["shared_pool_source"] == "linkedin"

    match = campaign_states.pipeline[0]["$match"]
    assert set(match["prospect_id"]["$in"]) == {str(high_id), str(zero_id)}
    assert match["account_id"] == str(account_id)
    assert match["campaign_id"] == str(campaign_id)
    assert all("$lookup" not in stage for stage in campaign_states.pipeline)

