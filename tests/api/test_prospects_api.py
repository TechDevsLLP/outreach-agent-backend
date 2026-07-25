"""API tests: /api/prospects — list/stats/get/patch + tenant isolation."""
import pytest
from bson import ObjectId

import database

pytestmark = pytest.mark.api


async def test_list_prospects_scoped_to_account_and_sorted(client, auth_headers_a, seeded_prospects):
    resp = await client.get("/api/prospects", headers=auth_headers_a)
    assert resp.status_code == 200, resp.text
    body = resp.json()
    assert body["total"] == 5
    names = [p["full_name"] for p in body["prospects"]]
    assert "Dave OtherTenant" not in names  # account B's prospect
    # sorted by prospect_state ai_score desc -> Alice (85) first
    assert body["prospects"][0]["full_name"] == "Alice Anderson"
    # state overlay fields are applied
    assert body["prospects"][0]["ai_prospect_score"] == 85.0
    assert body["prospects"][0]["priority_tier"] == "hot"


async def test_list_prospects_pagination(client, auth_headers_a, seeded_prospects):
    resp = await client.get("/api/prospects?page=1&page_size=2", headers=auth_headers_a)
    body = resp.json()
    assert body["page_size"] == 2
    assert len(body["prospects"]) == 2
    assert body["total_pages"] == 3

    page2 = await client.get("/api/prospects?page=2&page_size=2", headers=auth_headers_a)
    ids_p1 = {p["_id"] for p in body["prospects"]}
    ids_p2 = {p["_id"] for p in page2.json()["prospects"]}
    assert ids_p1.isdisjoint(ids_p2)


async def test_list_prospects_min_score_filter(client, auth_headers_a, seeded_prospects):
    resp = await client.get("/api/prospects?min_score=70", headers=auth_headers_a)
    body = resp.json()
    scores = [p["ai_prospect_score"] for p in body["prospects"]]
    assert all(s >= 70 for s in scores)
    assert len(scores) == 2  # alice 85, eve 75


async def test_list_prospects_status_filter(client, auth_headers_a, seeded_prospects):
    resp = await client.get("/api/prospects?status=contacted", headers=auth_headers_a)
    body = resp.json()
    assert [p["full_name"] for p in body["prospects"]] == ["Carol NoLinked"]


async def test_list_prospects_has_email_filter_filters_page(client, auth_headers_a, seeded_prospects):
    """Iteration-3 fix: prospect-level filters filter the returned page, not
    just `total` (matched against denormalized prospect_state.pk keys)."""
    resp = await client.get("/api/prospects?has_email=false", headers=auth_headers_a)
    body = resp.json()
    assert body["total"] == 1
    assert [p["full_name"] for p in body["prospects"]] == ["Bob Brown"]  # only Bob has no email

    resp2 = await client.get("/api/prospects?has_email=true", headers=auth_headers_a)
    body2 = resp2.json()
    assert body2["total"] == 4
    names = {p["full_name"] for p in body2["prospects"]}
    assert "Bob Brown" not in names and len(names) == 4


async def test_list_prospects_industry_and_country_filter_page(client, auth_headers_a, seeded_prospects):
    resp = await client.get("/api/prospects?industry_id=chemicals", headers=auth_headers_a)
    body = resp.json()
    assert body["total"] == 1
    assert [p["full_name"] for p in body["prospects"]] == ["Bob Brown"]

    resp2 = await client.get("/api/prospects?country=DE&has_linkedin=true", headers=auth_headers_a)
    body2 = resp2.json()
    assert body2["total"] == 1
    assert [p["full_name"] for p in body2["prospects"]] == ["Bob Brown"]


async def test_list_prospects_search_filters_page(client, auth_headers_a, seeded_prospects):
    resp = await client.get("/api/prospects?search=anderson", headers=auth_headers_a)
    body = resp.json()
    assert body["total"] == 1
    assert [p["full_name"] for p in body["prospects"]] == ["Alice Anderson"]


async def test_list_prospects_account_b_sees_only_theirs(client, auth_headers_b, seeded_prospects):
    resp = await client.get("/api/prospects", headers=auth_headers_b)
    body = resp.json()
    assert body["total"] == 1
    assert body["prospects"][0]["full_name"] == "Dave OtherTenant"


async def test_stats_shape_and_tiers(client, auth_headers_b, seeded_prospects):
    # use account B (1 prospect) — simple deterministic assertion
    resp = await client.get("/api/prospects/stats", headers=auth_headers_b)
    assert resp.status_code == 200
    body = resp.json()
    assert body["total_prospects"] == 1
    assert body["by_priority_tier"] == {"hot": 0, "warm": 1, "cold": 0}
    assert body["avg_score"] == 70.0
    assert "by_industry" in body and "by_country" in body


async def test_get_prospect_with_state_overlay(client, auth_headers_a, seeded_prospects):
    resp = await client.get(f"/api/prospects/{seeded_prospects['alice']}", headers=auth_headers_a)
    assert resp.status_code == 200
    body = resp.json()
    assert body["full_name"] == "Alice Anderson"
    assert body["ai_prospect_score"] == 85.0
    assert body["status"] == "new"


async def test_get_prospect_tenant_isolation(client, auth_headers_b, seeded_prospects):
    """Account B must NOT be able to read account A's prospect."""
    resp = await client.get(f"/api/prospects/{seeded_prospects['alice']}", headers=auth_headers_b)
    assert resp.status_code == 404


async def test_get_prospect_own_tenant_ok(client, auth_headers_b, seeded_prospects):
    resp = await client.get(f"/api/prospects/{seeded_prospects['dave_b']}", headers=auth_headers_b)
    assert resp.status_code == 200
    assert resp.json()["full_name"] == "Dave OtherTenant"


async def test_get_prospect_invalid_id_400(client, auth_headers_a):
    resp = await client.get("/api/prospects/not-an-objectid", headers=auth_headers_a)
    assert resp.status_code == 400


async def test_get_prospect_missing_404(client, auth_headers_a):
    resp = await client.get(f"/api/prospects/{ObjectId()}", headers=auth_headers_a)
    assert resp.status_code == 404


async def test_patch_prospect_status(client, auth_headers_a, identity_a, seeded_prospects):
    pid = seeded_prospects["bob"]
    resp = await client.patch(f"/api/prospects/{pid}", headers=auth_headers_a,
                              json={"status": "qualified"})
    assert resp.status_code == 200, resp.text
    state = await database.prospect_state_collection.find_one(
        {"account_id": identity_a["account_id"], "prospect_id": pid})
    assert state["status"] == "qualified"


async def test_patch_prospect_invalid_status_400(client, auth_headers_a, seeded_prospects):
    resp = await client.patch(f"/api/prospects/{seeded_prospects['bob']}",
                              headers=auth_headers_a, json={"status": "nonsense"})
    assert resp.status_code == 400


async def test_patch_prospect_no_valid_fields_400(client, auth_headers_a, seeded_prospects):
    resp = await client.patch(f"/api/prospects/{seeded_prospects['bob']}",
                              headers=auth_headers_a, json={"hack_field": 1})
    assert resp.status_code == 400


async def test_patch_prospect_tags_upsert(client, auth_headers_a, identity_a, seeded_prospects):
    """Setting tags must work even when it creates the state doc via upsert.

    Regression: $set{tags} + $setOnInsert{tags: []} in the same update is a
    MongoDB path conflict -> this endpoint 500'd on any tags update.
    """
    pid = seeded_prospects["alice"]
    resp = await client.patch(f"/api/prospects/{pid}", headers=auth_headers_a,
                              json={"tags": ["priority", "q3"]})
    assert resp.status_code == 200, resp.text
    state = await database.prospect_state_collection.find_one(
        {"account_id": identity_a["account_id"], "prospect_id": pid})
    assert state["tags"] == ["priority", "q3"]
    # PATCH refreshes the denormalized filter keys on the state doc
    assert state["pk"]["email"] == "alice@acme-a.test"
    assert state["pk"]["company_industry_id"] == "industrial_machinery"


async def test_patch_prospect_notes_written_to_global_doc(client, auth_headers_a, seeded_prospects):
    pid = seeded_prospects["carol"]
    resp = await client.patch(f"/api/prospects/{pid}", headers=auth_headers_a,
                              json={"notes": "met at trade show"})
    assert resp.status_code == 200
    doc = await database.prospects_collection.find_one({"_id": ObjectId(pid)})
    assert doc["notes"] == "met at trade show"


async def test_prospects_require_auth(client):
    assert (await client.get("/api/prospects")).status_code == 401
