"""Iteration-2 tests: prospects list/stats overlay aggregation at 2k rows.

Verifies the rewritten single-aggregation pattern (routes/prospects.py) against
a seeded 2,000-row prospect_state overlay in the TEST DB:
- totals / pagination / sort order match the seeded data exactly,
- state overlay fields are applied,
- prospect-level filters affect `total` (legacy-parity semantics),
- cross-account isolation holds,
- stats endpoint aggregates match Python-computed ground truth.

Endpoint latencies are appended to perf_metrics so they land in the timings
JSON (test-DB network latency applies; compare like-for-like across runs).
"""
import time

import pytest
import pytest_asyncio
from bson import ObjectId
from datetime import datetime

import database

pytestmark = pytest.mark.api

N_OVERLAY = 2000


@pytest_asyncio.fixture(scope="module")
async def scale_identity(create_identity):
    return await create_identity(
        "scale@test.outflo.local", "Scale User", "ScaleCo",
    )


@pytest_asyncio.fixture(scope="module")
async def scale_overlay(scale_identity):
    """Seed 2,000 prospects + prospect_state rows for one account.

    ai_score is deterministic (i % 101) so ground truth is computable.
    Every 3rd prospect has no email; every 5th has industry 'chemicals',
    the rest 'software_development'.
    """
    now = datetime.utcnow()
    account_id = scale_identity["account_id"]

    prospect_docs = []
    for i in range(N_OVERLAY):
        doc = {
            "full_name": f"Scale Prospect {i:04d}",
            "job_title": "Engineer",
            "seniority": "manager" if i % 2 == 0 else None,
            "company_name": f"ScaleCo {i % 40}",   # 40 unique companies
            "linkedin": f"https://www.linkedin.com/in/scale-{i}",
            "company_industry_id": "chemicals" if i % 5 == 0 else "software_development",
            "location": {"country_code": "US" if i % 2 == 0 else "DE"},
            "enrichment_status": "completed" if i % 4 == 0 else "not_started",
            "source": "scale_seed",
            "created_at": now, "last_updated_at": now,
        }
        # every 3rd prospect has NO email — key omitted entirely because the
        # prospects email index is unique+sparse (explicit nulls would collide)
        if i % 3 != 0:
            doc["email"] = f"scale{i}@scaleco.test"
        prospect_docs.append(doc)
    result = await database.prospects_collection.insert_many(prospect_docs, ordered=False)
    ids = [str(x) for x in result.inserted_ids]

    from utils.prospect_filter_keys import build_filter_keys

    scores = [i % 101 for i in range(N_OVERLAY)]
    states = [
        {"account_id": account_id, "prospect_id": pid,
         "status": "contacted" if i % 10 == 0 else "new",
         "ai_score": float(scores[i]),
         "priority_tier": "hot" if scores[i] >= 80 else ("warm" if scores[i] >= 60 else "cold"),
         # denormalized filter keys, as production write sites now store them
         "pk": build_filter_keys(prospect_docs[i]),
         "used_by": [], "tags": [], "created_at": now}
        for i, pid in enumerate(ids)
    ]
    await database.prospect_state_collection.insert_many(states, ordered=False)

    yield {"ids": ids, "scores": scores, "account_id": account_id}

    # Clean up so session-scoped fixtures/tests that follow see the same pool
    await database.prospect_state_collection.delete_many({"account_id": account_id})
    await database.prospects_collection.delete_many({"source": "scale_seed"})


@pytest_asyncio.fixture(scope="module")
async def scale_headers(scale_identity):
    return scale_identity["headers"]


async def test_scale_list_total_sort_and_overlay_fields(
    client, scale_headers, scale_overlay, perf_metrics
):
    t0 = time.perf_counter()
    resp = await client.get("/api/prospects?page_size=50", headers=scale_headers)
    ms = round((time.perf_counter() - t0) * 1000, 1)
    perf_metrics.append({"name": "api_prospects_list_2000_overlay", "ms": ms,
                         "note": "test DB, includes ASGI+network overhead"})

    assert resp.status_code == 200, resp.text
    body = resp.json()
    assert body["total"] == N_OVERLAY
    assert body["total_pages"] == N_OVERLAY // 50
    page_scores = [p["ai_prospect_score"] for p in body["prospects"]]
    assert len(page_scores) == 50
    # ground truth: descending sort over i % 101 -> top 50 all == 100.0
    expected_top = sorted(scale_overlay["scores"], reverse=True)[:50]
    assert page_scores == [float(s) for s in expected_top]
    # overlay fields applied
    assert all(p["priority_tier"] == "hot" for p in body["prospects"])


async def test_scale_list_pagination_disjoint_and_ordered(client, scale_headers, scale_overlay):
    p1 = (await client.get("/api/prospects?page=1&page_size=100", headers=scale_headers)).json()
    p2 = (await client.get("/api/prospects?page=2&page_size=100", headers=scale_headers)).json()
    ids1 = {p["_id"] for p in p1["prospects"]}
    ids2 = {p["_id"] for p in p2["prospects"]}
    assert len(ids1) == 100 and len(ids2) == 100
    assert ids1.isdisjoint(ids2)
    assert min(p["ai_prospect_score"] for p in p1["prospects"]) >= \
        max(p["ai_prospect_score"] for p in p2["prospects"])


async def test_scale_list_min_score_filter_matches_ground_truth(client, scale_headers, scale_overlay):
    resp = await client.get("/api/prospects?min_score=90", headers=scale_headers)
    body = resp.json()
    expected = sum(1 for s in scale_overlay["scores"] if s >= 90)
    assert body["total"] == expected
    assert all(p["ai_prospect_score"] >= 90 for p in body["prospects"])


async def test_scale_list_prospect_level_filter_filters_page_and_total(
    client, scale_headers, scale_overlay, perf_metrics
):
    """Iteration-3 fix: prospect-level filters apply to BOTH `total` and the
    pagination slice (previously a legacy quirk filtered `total` only).
    Matching happens on the denormalized prospect_state.pk keys."""
    t0 = time.perf_counter()
    resp = await client.get("/api/prospects?has_email=true&page_size=100", headers=scale_headers)
    ms = round((time.perf_counter() - t0) * 1000, 1)
    perf_metrics.append({"name": "api_prospects_list_filtered_2000_overlay", "ms": ms,
                         "note": "has_email filter via denormalized pk keys"})
    body = resp.json()
    expected_with_email = sum(1 for i in range(N_OVERLAY) if i % 3 != 0)
    assert body["total"] == expected_with_email
    assert len(body["prospects"]) == 100
    assert all(p.get("email") for p in body["prospects"])

    resp2 = await client.get("/api/prospects?industry_id=chemicals&page_size=100", headers=scale_headers)
    body2 = resp2.json()
    expected_chem = sum(1 for i in range(N_OVERLAY) if i % 5 == 0)
    assert body2["total"] == expected_chem
    assert len(body2["prospects"]) == 100
    assert all(p["company_industry_id"] == "chemicals" for p in body2["prospects"])
    # page is still sorted by state ai_score desc within the filtered set
    page_scores = [p["ai_prospect_score"] for p in body2["prospects"]]
    assert page_scores == sorted(page_scores, reverse=True)


async def test_scale_list_filtered_pagination_ground_truth(client, scale_headers, scale_overlay):
    """Filtered pagination is exact: page 2 of the chemicals filter equals the
    Python-computed slice of the filtered, score-sorted ground truth."""
    scores = scale_overlay["scores"]
    chem = sorted((i for i in range(N_OVERLAY) if i % 5 == 0),
                  key=lambda i: -scores[i])
    expected_page2 = [scores[i] for i in chem[50:100]]

    resp = await client.get("/api/prospects?industry_id=chemicals&page=2&page_size=50",
                            headers=scale_headers)
    body = resp.json()
    assert [p["ai_prospect_score"] for p in body["prospects"]] == [float(s) for s in expected_page2]
    assert body["total"] == len(chem)
    assert body["total_pages"] == (len(chem) + 49) // 50


async def test_scale_list_combined_filters_and_search(client, scale_headers, scale_overlay):
    """Combined filters AND together (search + country + has_email), matching
    Python ground truth — the legacy builder let one $or clobber another."""
    resp = await client.get(
        "/api/prospects?search=Scale Prospect 00&country=DE&has_email=true&page_size=200",
        headers=scale_headers)
    body = resp.json()
    expected = [i for i in range(N_OVERLAY)
                if f"{i:04d}".startswith("00")          # search on full_name
                and i % 2 == 1                            # DE
                and i % 3 != 0]                           # has email
    assert body["total"] == len(expected)
    assert len(body["prospects"]) == len(expected)
    assert all(p["location"]["country_code"] == "DE" and p.get("email")
               for p in body["prospects"])


async def test_scale_list_status_filter(client, scale_headers, scale_overlay):
    resp = await client.get("/api/prospects?status=contacted", headers=scale_headers)
    body = resp.json()
    assert body["total"] == N_OVERLAY // 10
    assert all(p["status"] == "contacted" for p in body["prospects"])


async def test_scale_isolation_other_account_unaffected(client, auth_headers_a, seeded_prospects, scale_overlay):
    resp = await client.get("/api/prospects", headers=auth_headers_a)
    body = resp.json()
    assert body["total"] == 5  # account A still sees only its 5 seeded prospects
    assert all(not p["full_name"].startswith("Scale Prospect") for p in body["prospects"])


async def test_scale_stats_match_ground_truth(client, scale_headers, scale_overlay, perf_metrics):
    t0 = time.perf_counter()
    resp = await client.get("/api/prospects/stats", headers=scale_headers)
    ms = round((time.perf_counter() - t0) * 1000, 1)
    perf_metrics.append({"name": "api_prospects_stats_2000_overlay_first_uncached", "ms": ms})

    assert resp.status_code == 200, resp.text
    body = resp.json()
    scores = scale_overlay["scores"]
    assert body["total_prospects"] == N_OVERLAY
    assert body["by_priority_tier"] == {
        "hot": sum(1 for s in scores if s >= 80),
        "warm": sum(1 for s in scores if 60 <= s < 80),
        "cold": sum(1 for s in scores if s < 60),
    }
    assert body["avg_score"] == round(sum(scores) / len(scores), 1)
    assert body["by_status"]["contacted"] == N_OVERLAY // 10
    assert body["by_status"]["new"] == N_OVERLAY - N_OVERLAY // 10
    assert body["unique_companies"] == 40
    industries = {r["_id"]: r["count"] for r in body["by_industry"]}
    assert industries["chemicals"] == sum(1 for i in range(N_OVERLAY) if i % 5 == 0)
    assert industries["software_development"] == N_OVERLAY - industries["chemicals"]
    countries = {r["_id"]: r["count"] for r in body["by_country"]}
    assert countries == {"US": N_OVERLAY // 2, "DE": N_OVERLAY // 2}
    assert body["by_enrichment_status"]["completed"] == N_OVERLAY // 4
