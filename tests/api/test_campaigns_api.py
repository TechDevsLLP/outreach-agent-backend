"""API tests: /api/campaigns — create/list/get/lifecycle (no AI paths)."""
import pytest
from bson import ObjectId

pytestmark = pytest.mark.api

STEP = {
    "step_number": 1,
    "name": "Intro email",
    "channel": "email",
    "action": "email",
    "delay_days": 0,
    "subject_template": "Hi {{first_name}}",
    "body_template": "Hello {{first_name}}, quick question about {{company_name}}.",
}


@pytest.fixture(scope="module")
async def campaign_a(client, auth_headers_a):
    """A draft campaign with one step, owned by account A."""
    resp = await client.post("/api/campaigns", headers=auth_headers_a, json={
        "name": "Test Campaign Alpha",
        "type": "custom",
        "description": "harness campaign",
        "steps": [STEP],
    })
    assert resp.status_code == 200, resp.text
    return resp.json()["campaign"]


async def test_create_campaign_defaults(campaign_a):
    assert campaign_a["name"] == "Test Campaign Alpha"
    assert campaign_a["status"] == "draft"
    assert campaign_a["total_enrolled"] == 0
    assert campaign_a["send_days"] == ["monday", "tuesday", "wednesday", "thursday", "friday"]
    assert len(campaign_a["steps"]) == 1


async def test_list_campaigns_scoped(client, auth_headers_a, auth_headers_b, campaign_a):
    a = await client.get("/api/campaigns", headers=auth_headers_a)
    assert a.status_code == 200
    assert any(c["_id"] == campaign_a["_id"] for c in a.json()["campaigns"])

    b = await client.get("/api/campaigns", headers=auth_headers_b)
    assert all(c["_id"] != campaign_a["_id"] for c in b.json()["campaigns"])


async def test_list_campaigns_status_filter(client, auth_headers_a, campaign_a):
    resp = await client.get("/api/campaigns?status=draft", headers=auth_headers_a)
    assert any(c["_id"] == campaign_a["_id"] for c in resp.json()["campaigns"])
    resp2 = await client.get("/api/campaigns?status=active", headers=auth_headers_a)
    assert all(c["_id"] != campaign_a["_id"] for c in resp2.json()["campaigns"])


async def test_get_campaign(client, auth_headers_a, campaign_a):
    resp = await client.get(f"/api/campaigns/{campaign_a['_id']}", headers=auth_headers_a)
    assert resp.status_code == 200


async def test_get_campaign_cross_tenant_404(client, auth_headers_b, campaign_a):
    # Iteration 2: cross-tenant campaign access returns 404 (aligned with the
    # prospects routes — no existence leak), previously 403.
    resp = await client.get(f"/api/campaigns/{campaign_a['_id']}", headers=auth_headers_b)
    assert resp.status_code == 404


async def test_get_campaign_unknown_404(client, auth_headers_a):
    resp = await client.get(f"/api/campaigns/{ObjectId()}", headers=auth_headers_a)
    assert resp.status_code == 404


async def test_activate_requires_steps(client, auth_headers_a):
    no_steps = await client.post("/api/campaigns", headers=auth_headers_a, json={
        "name": "Stepless", "steps": [],
    })
    cid = no_steps.json()["campaign"]["_id"]
    resp = await client.post(f"/api/campaigns/{cid}/activate", headers=auth_headers_a)
    assert resp.status_code == 400


async def test_lifecycle_activate_pause_resume(client, auth_headers_a, campaign_a):
    cid = campaign_a["_id"]

    act = await client.post(f"/api/campaigns/{cid}/activate", headers=auth_headers_a)
    assert act.status_code == 200
    assert act.json()["campaign"]["status"] == "active"
    assert act.json()["campaign"]["started_at"] is not None

    pause = await client.post(f"/api/campaigns/{cid}/pause", headers=auth_headers_a)
    assert pause.status_code == 200
    assert pause.json()["campaign"]["status"] == "paused"

    resume = await client.post(f"/api/campaigns/{cid}/resume", headers=auth_headers_a)
    assert resume.status_code == 200
    assert resume.json()["campaign"]["status"] == "active"


async def test_lifecycle_cross_tenant_not_found(client, auth_headers_b, campaign_a):
    # Cross-tenant mutations also 404 (no existence leak), previously 403.
    resp = await client.post(f"/api/campaigns/{campaign_a['_id']}/pause", headers=auth_headers_b)
    assert resp.status_code == 404


async def test_update_campaign_fields(client, auth_headers_a, campaign_a):
    resp = await client.patch(f"/api/campaigns/{campaign_a['_id']}", headers=auth_headers_a,
                              json={"name": "Renamed Campaign", "daily_email_limit": 10})
    assert resp.status_code == 200, resp.text
    fetched = await client.get(f"/api/campaigns/{campaign_a['_id']}", headers=auth_headers_a)
    payload = fetched.json()
    campaign = payload.get("campaign", payload)
    assert campaign["name"] == "Renamed Campaign"


async def test_campaign_stats_endpoint(client, auth_headers_a, campaign_a):
    resp = await client.get(f"/api/campaigns/{campaign_a['_id']}/stats", headers=auth_headers_a)
    assert resp.status_code == 200
    assert isinstance(resp.json(), dict)


async def test_campaigns_require_auth(client):
    assert (await client.get("/api/campaigns")).status_code == 401


# ---------------------------------------------------------------------------
# POST /{campaign_id}/industries/approve
# ---------------------------------------------------------------------------

async def test_approve_industries_merges_and_clears_suggestions(client, auth_headers_a, campaign_a):
    import database

    cid = campaign_a["_id"]
    # Seed a suggestion state as if canonicalize_icp had run on "retail".
    await database.campaigns_collection.update_one(
        {"_id": ObjectId(cid)},
        {"$set": {
            "industry_ids": ["retail"],
            "suggested_industry_ids": ["food_beverage", "wholesale", "supermarkets"],
        }},
    )

    resp = await client.post(
        f"/api/campaigns/{cid}/industries/approve",
        headers=auth_headers_a,
        json={"industry_ids": ["food_beverage", "wholesale"]},
    )
    assert resp.status_code == 200, resp.text
    body = resp.json()
    assert set(body["industry_ids"]) == {"retail", "food_beverage", "wholesale"}
    assert body["suggested_industry_ids"] == ["supermarkets"]


async def test_approve_industries_rejects_unknown_id(client, auth_headers_a, campaign_a):
    resp = await client.post(
        f"/api/campaigns/{campaign_a['_id']}/industries/approve",
        headers=auth_headers_a,
        json={"industry_ids": ["not_a_real_industry"]},
    )
    assert resp.status_code == 400


async def test_approve_industries_empty_body_400(client, auth_headers_a, campaign_a):
    resp = await client.post(
        f"/api/campaigns/{campaign_a['_id']}/industries/approve",
        headers=auth_headers_a,
        json={"industry_ids": []},
    )
    assert resp.status_code == 400


async def test_approve_industries_cross_tenant_404(client, auth_headers_b, campaign_a):
    resp = await client.post(
        f"/api/campaigns/{campaign_a['_id']}/industries/approve",
        headers=auth_headers_b,
        json={"industry_ids": ["retail"]},
    )
    assert resp.status_code == 404


async def test_approve_industries_requires_auth(client, campaign_a):
    resp = await client.post(
        f"/api/campaigns/{campaign_a['_id']}/industries/approve",
        json={"industry_ids": ["retail"]},
    )
    assert resp.status_code == 401


async def test_approve_industries_defers_rediscovery_when_discovery_completed(
    client, auth_headers_a, campaign_a
):
    """Discovery already ran once (discovery_status=completed) and the campaign
    hasn't been approved/activated yet — approving a suggestion should mark the
    campaign for a future supplemental discovery pass rather than silently
    doing nothing, since run_fast_discovery only ever fires at creation."""
    import database

    cid = campaign_a["_id"]
    await database.campaigns_collection.update_one(
        {"_id": ObjectId(cid)},
        {"$set": {
            "industry_ids": ["retail"],
            "suggested_industry_ids": ["food_beverage"],
            "discovery_status": "completed",
            "status": "awaiting_approval",
        }},
    )

    resp = await client.post(
        f"/api/campaigns/{cid}/industries/approve",
        headers=auth_headers_a,
        json={"industry_ids": ["food_beverage"]},
    )
    assert resp.status_code == 200, resp.text
    body = resp.json()
    assert body["rediscovery"] == "deferred"

    updated = await database.campaigns_collection.find_one({"_id": ObjectId(cid)})
    assert updated["pending_rediscovery"] is True
    assert "food_beverage" in updated["pending_rediscovery_industry_ids"]

    # Reset campaign status/discovery_status so this fixture-shared campaign
    # doesn't leak state into other tests in this module.
    await database.campaigns_collection.update_one(
        {"_id": ObjectId(cid)},
        {"$set": {"status": "draft", "discovery_status": None, "pending_rediscovery": False}},
    )


async def test_approve_industries_no_rediscovery_before_discovery_completes(
    client, auth_headers_a, campaign_a
):
    """No discovery_status yet (draft, pre-discovery campaign) — merge alone is
    enough; lazy canonicalization picks up the merged ids on the next pass."""
    import database

    cid = campaign_a["_id"]
    await database.campaigns_collection.update_one(
        {"_id": ObjectId(cid)},
        {"$set": {
            "industry_ids": ["retail"],
            "suggested_industry_ids": ["wholesale"],
            "discovery_status": None,
            "status": "draft",
        }},
    )

    resp = await client.post(
        f"/api/campaigns/{cid}/industries/approve",
        headers=auth_headers_a,
        json={"industry_ids": ["wholesale"]},
    )
    assert resp.status_code == 200, resp.text
    assert resp.json()["rediscovery"] == "not_needed"

    updated = await database.campaigns_collection.find_one({"_id": ObjectId(cid)})
    assert not updated.get("pending_rediscovery")


async def test_approve_industries_no_rediscovery_when_campaign_active(
    client, auth_headers_a, campaign_a
):
    """Discovery completed but the campaign is already active — don't kick off
    new sourcing work behind the scheduler's back."""
    import database

    cid = campaign_a["_id"]
    await database.campaigns_collection.update_one(
        {"_id": ObjectId(cid)},
        {"$set": {
            "industry_ids": ["retail"],
            "suggested_industry_ids": ["supermarkets"],
            "discovery_status": "completed",
            "status": "active",
        }},
    )

    resp = await client.post(
        f"/api/campaigns/{cid}/industries/approve",
        headers=auth_headers_a,
        json={"industry_ids": ["supermarkets"]},
    )
    assert resp.status_code == 200, resp.text
    assert resp.json()["rediscovery"] == "not_needed"

    # Reset shared fixture campaign back to draft for isolation.
    await database.campaigns_collection.update_one(
        {"_id": ObjectId(cid)},
        {"$set": {"status": "draft", "discovery_status": None}},
    )
