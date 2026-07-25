"""API tests: POST /api/enrichment/prospects/{id}/unlock-phone.

GrowthToolkit is mocked at the service layer (mock_enrich_linkedin fixture);
no credits are ever spent.
"""
import pytest
from bson import ObjectId

import database
from services.growthtoolkit_service import CreditsExhausted, InvalidInput, GrowthToolkitError

pytestmark = pytest.mark.api


def _url(pid: str) -> str:
    return f"/api/enrichment/prospects/{pid}/unlock-phone"


async def test_unlock_phone_happy_path_persists(
    client, auth_headers_a, identity_a, seeded_prospects, mock_enrich_linkedin
):
    pid = seeded_prospects["bob"]
    mock_enrich_linkedin["result"] = {
        "full_name": "Bob Brown",
        "unlock_details": {"phone_numbers": [{"number": "+49-555-0199"}]},
    }
    resp = await client.post(_url(pid), headers=auth_headers_a)
    assert resp.status_code == 200, resp.text
    body = resp.json()
    assert body["cached"] is False
    assert body["mobile_number"] == "+49-555-0199"

    # provider called exactly once, with unlock_phone and cost attribution
    assert len(mock_enrich_linkedin["calls"]) == 1
    call = mock_enrich_linkedin["calls"][0]
    assert call["unlock_phone"] is True
    assert call["account_id"] == identity_a["account_id"]
    assert call["prospect_id"] == pid

    # persisted with phone_source growthtoolkit
    doc = await database.prospects_collection.find_one({"_id": ObjectId(pid)})
    assert doc["mobile_number"] == "+49-555-0199"
    assert doc["phone_source"] == "growthtoolkit"
    assert doc["phone_unlocked_by_account"] == identity_a["account_id"]


async def test_unlock_phone_cached_no_second_provider_call(
    client, auth_headers_a, seeded_prospects, mock_enrich_linkedin
):
    pid = seeded_prospects["eve_cached"]
    resp = await client.post(_url(pid), headers=auth_headers_a)
    assert resp.status_code == 200
    body = resp.json()
    assert body["cached"] is True
    assert body["mobile_number"] == "+1-555-0100"
    assert mock_enrich_linkedin["calls"] == []  # no credit spent


async def test_unlock_phone_no_linkedin_url_400(
    client, auth_headers_a, seeded_prospects, mock_enrich_linkedin
):
    resp = await client.post(_url(seeded_prospects["carol"]), headers=auth_headers_a)
    assert resp.status_code == 400
    assert "linkedin" in resp.json()["detail"].lower()
    assert mock_enrich_linkedin["calls"] == []


async def test_unlock_phone_credits_exhausted_402(
    client, auth_headers_a, seeded_prospects, mock_enrich_linkedin
):
    mock_enrich_linkedin["exception"] = CreditsExhausted("credits exhausted", code="402")
    resp = await client.post(_url(seeded_prospects["alice"]), headers=auth_headers_a)
    assert resp.status_code == 402
    assert "credit" in resp.json()["detail"].lower()


async def test_unlock_phone_invalid_input_400(
    client, auth_headers_a, seeded_prospects, mock_enrich_linkedin
):
    mock_enrich_linkedin["exception"] = InvalidInput("bad url", code="417")
    resp = await client.post(_url(seeded_prospects["alice"]), headers=auth_headers_a)
    assert resp.status_code == 400


async def test_unlock_phone_provider_error_502(
    client, auth_headers_a, seeded_prospects, mock_enrich_linkedin
):
    mock_enrich_linkedin["exception"] = GrowthToolkitError("boom")
    resp = await client.post(_url(seeded_prospects["alice"]), headers=auth_headers_a)
    assert resp.status_code == 502


async def test_unlock_phone_no_phone_available_404(
    client, auth_headers_a, seeded_prospects, mock_enrich_linkedin
):
    mock_enrich_linkedin["result"] = {"full_name": "Frank NoPhone", "phone_numbers": []}
    resp = await client.post(_url(seeded_prospects["frank_nophone"]), headers=auth_headers_a)
    assert resp.status_code == 404


async def test_unlock_phone_cross_tenant_404(
    client, auth_headers_b, seeded_prospects, mock_enrich_linkedin
):
    """Account B cannot unlock (or even see) account A's prospect."""
    resp = await client.post(_url(seeded_prospects["alice"]), headers=auth_headers_b)
    assert resp.status_code == 404
    assert mock_enrich_linkedin["calls"] == []


async def test_unlock_phone_invalid_id_400(client, auth_headers_a, mock_enrich_linkedin):
    resp = await client.post(_url("garbage-id"), headers=auth_headers_a)
    assert resp.status_code == 400


async def test_unlock_phone_unknown_prospect_404(client, auth_headers_a, mock_enrich_linkedin):
    resp = await client.post(_url(str(ObjectId())), headers=auth_headers_a)
    assert resp.status_code == 404
