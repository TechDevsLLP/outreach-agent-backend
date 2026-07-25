"""API tests: /api/campaigns/{id}/enrolled-prospects (+ /stats, /companies).

Covers the campaign_prospect_state overlay wiring added for the review UI:
  * campaign_fit_score surfaced per row + overlay-driven enrichment_status
  * post-lookup filter params (has_email/has_linkedin/enriched/company) with
    an accurate paginated `total`
  * enrolled_prospects_stats companies count + overlay-driven `enriched`
  * the new /companies rollup endpoint
"""
import pytest
from bson import ObjectId
from datetime import datetime

import database

pytestmark = pytest.mark.api


@pytest.fixture(scope="module")
async def enrolled_campaign(client, auth_headers_a, identity_a):
    """A campaign owned by account A with three enrolled prospects + overlay.

    p1: email + linkedin, company co1, cps score 90, enrichment succeeded
    p2: no email, linkedin, company co1, cps score 40, enrichment running
    p3: email, no linkedin, company co2, cps score 70, enrichment succeeded
    """
    resp = await client.post("/api/campaigns", headers=auth_headers_a, json={
        "name": "Enrolled Prospects Campaign",
        "type": "custom",
        "description": "harness",
        "steps": [{
            "step_number": 1, "name": "Intro", "channel": "email", "action": "email",
            "delay_days": 0, "subject_template": "Hi", "body_template": "Hello",
        }],
    })
    assert resp.status_code == 200, resp.text
    campaign_id = resp.json()["campaign"]["_id"]
    campaign_oid = ObjectId(campaign_id)
    account_id_str = identity_a["account_id"]
    account_oid = ObjectId(account_id_str)
    now = datetime.utcnow()

    prospects = [
        {
            "full_name": "Pat One", "first_name": "Pat", "job_title": "VP Sales",
            "email": "pat@co1.test", "linkedin": "https://www.linkedin.com/in/pat-one",
            "company_id": "co1", "company_name": "Acme Co",
            "company_domain": "co1.test", "company_linkedin": "https://linkedin.com/company/co1",
            "company_industry_group": "software", "enrichment_status": "not_started",
            "created_at": now,
        },
        {
            "full_name": "Sam Two", "first_name": "Sam", "job_title": "Head of Ops",
            "email": None, "linkedin": "https://www.linkedin.com/in/sam-two",
            "company_id": "co1", "company_name": "Acme Co",
            "company_domain": "co1.test", "company_linkedin": "https://linkedin.com/company/co1",
            "company_industry_group": "software", "enrichment_status": "not_started",
            "created_at": now,
        },
        {
            "full_name": "Lee Three", "first_name": "Lee", "job_title": "Founder",
            "email": "lee@co2.test", "linkedin": None,
            "company_id": "co2", "company_name": "Beta Co",
            "company_domain": "co2.test", "company_linkedin": "https://linkedin.com/company/co2",
            "company_industry_group": "retail", "enrichment_status": "completed",
            "created_at": now,
        },
    ]
    ins = await database.prospects_collection.insert_many(prospects)
    pids = ins.inserted_ids

    enrollments = [
        {
            "campaign_id": campaign_oid, "account_id": account_oid, "prospect_id": pids[0],
            "status": "active", "smart_campaign_channel": "email",
            "smart_campaign_send_day": 1, "campaign_rule_score": 50.0, "enrolled_at": now,
        },
        {
            "campaign_id": campaign_oid, "account_id": account_oid, "prospect_id": pids[1],
            "status": "active", "smart_campaign_channel": "linkedin_connection",
            "smart_campaign_send_day": 1, "campaign_rule_score": 30.0, "enrolled_at": now,
        },
        {
            "campaign_id": campaign_oid, "account_id": account_oid, "prospect_id": pids[2],
            "status": "active", "smart_campaign_channel": "email",
            "smart_campaign_send_day": 2, "campaign_rule_score": 40.0, "enrolled_at": now,
        },
    ]
    await database.campaign_enrollments_collection.insert_many(enrollments)

    cps_docs = [
        {
            "account_id": account_id_str, "campaign_id": campaign_id,
            "prospect_id": str(pids[0]), "scoring_version": "v1",
            "score": {"value": 90, "priority_tier": "hot", "reasoning": "great"},
            "enrichment": {"state": "succeeded"},
        },
        {
            "account_id": account_id_str, "campaign_id": campaign_id,
            "prospect_id": str(pids[1]), "scoring_version": "v1",
            "score": {"value": 40, "priority_tier": "cold", "reasoning": "meh"},
            "enrichment": {"state": "running"},
        },
        {
            "account_id": account_id_str, "campaign_id": campaign_id,
            "prospect_id": str(pids[2]), "scoring_version": "v1",
            "score": {"value": 70, "priority_tier": "warm", "reasoning": "ok"},
            "enrichment": {"state": "succeeded"},
        },
    ]
    await database.campaign_prospect_state_collection.insert_many(cps_docs)

    return {
        "campaign_id": campaign_id,
        "pids": [str(p) for p in pids],
    }


async def test_list_surfaces_campaign_fit_and_overlay_enrichment(
    client, auth_headers_a, enrolled_campaign
):
    cid = enrolled_campaign["campaign_id"]
    resp = await client.get(
        f"/api/campaigns/{cid}/enrolled-prospects?page_size=50", headers=auth_headers_a
    )
    assert resp.status_code == 200, resp.text
    body = resp.json()
    assert body["total"] == 3
    by_pid = {r["prospect_id"]: r for r in body["prospects"]}

    p1 = by_pid[enrolled_campaign["pids"][0]]
    assert p1["campaign_fit_score"] == 90
    assert p1["priority_tier"] == "hot"
    # succeeded overlay state maps to "completed"
    assert p1["enrichment_status"] == "completed"

    p2 = by_pid[enrolled_campaign["pids"][1]]
    assert p2["campaign_fit_score"] == 40
    # non-succeeded overlay state passes through
    assert p2["enrichment_status"] == "running"


async def test_filter_has_email(client, auth_headers_a, enrolled_campaign):
    cid = enrolled_campaign["campaign_id"]
    resp = await client.get(
        f"/api/campaigns/{cid}/enrolled-prospects?has_email=true&page_size=50",
        headers=auth_headers_a,
    )
    assert resp.status_code == 200, resp.text
    body = resp.json()
    # Only p1 and p3 have an email → total reflects the post-lookup filter.
    assert body["total"] == 2
    assert all(r["has_email"] for r in body["prospects"])


async def test_filter_has_linkedin_false(client, auth_headers_a, enrolled_campaign):
    cid = enrolled_campaign["campaign_id"]
    resp = await client.get(
        f"/api/campaigns/{cid}/enrolled-prospects?has_linkedin=false&page_size=50",
        headers=auth_headers_a,
    )
    assert resp.status_code == 200, resp.text
    body = resp.json()
    # Only p3 lacks a linkedin url.
    assert body["total"] == 1
    assert body["prospects"][0]["prospect_id"] == enrolled_campaign["pids"][2]


async def test_filter_enriched(client, auth_headers_a, enrolled_campaign):
    cid = enrolled_campaign["campaign_id"]
    resp = await client.get(
        f"/api/campaigns/{cid}/enrolled-prospects?enriched=true&page_size=50",
        headers=auth_headers_a,
    )
    assert resp.status_code == 200, resp.text
    # p1 and p3 have enrichment.state == succeeded.
    assert resp.json()["total"] == 2


async def test_filter_company(client, auth_headers_a, enrolled_campaign):
    cid = enrolled_campaign["campaign_id"]
    resp = await client.get(
        f"/api/campaigns/{cid}/enrolled-prospects?company=co1&page_size=50",
        headers=auth_headers_a,
    )
    assert resp.status_code == 200, resp.text
    # co1 has p1 + p2.
    assert resp.json()["total"] == 2


async def test_stats_companies_and_enriched(client, auth_headers_a, enrolled_campaign):
    cid = enrolled_campaign["campaign_id"]
    resp = await client.get(
        f"/api/campaigns/{cid}/enrolled-prospects/stats", headers=auth_headers_a
    )
    assert resp.status_code == 200, resp.text
    stats = resp.json()
    assert stats["total"] == 3
    assert stats["with_email"] == 2
    assert stats["with_linkedin"] == 2
    # enriched reads the overlay (succeeded), not the shared prospect doc.
    assert stats["enriched"] == 2
    # two distinct companies (co1, co2).
    assert stats["companies"] == 2


async def test_companies_endpoint(client, auth_headers_a, enrolled_campaign):
    cid = enrolled_campaign["campaign_id"]
    resp = await client.get(f"/api/campaigns/{cid}/companies", headers=auth_headers_a)
    assert resp.status_code == 200, resp.text
    body = resp.json()
    assert body["total_companies"] == 2
    # Sorted by prospect_count desc → co1 (2) first.
    assert body["companies"][0]["company_id"] == "co1"
    assert body["companies"][0]["prospect_count"] == 2
    assert body["companies"][1]["company_id"] == "co2"
    assert body["companies"][1]["prospect_count"] == 1


async def test_companies_endpoint_cross_tenant_404(
    client, auth_headers_b, enrolled_campaign
):
    cid = enrolled_campaign["campaign_id"]
    resp = await client.get(f"/api/campaigns/{cid}/companies", headers=auth_headers_b)
    assert resp.status_code == 404
