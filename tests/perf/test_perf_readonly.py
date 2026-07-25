"""READ-ONLY performance measurements against the REAL `outflo_v3` database.

STRICT RULES:
- Own Motor client, database hard-coded to `outflo_v3` and asserted.
- Only find / aggregate / count / estimated_document_count / distinct ops.
- Every measurement is appended to perf_metrics and lands in
  docs/test-reports/timings_iteration_<N>.json under "perf_metrics"
  (N from TEST_REPORT_ITERATION, default 2 — see tests/conftest.py).
"""
import time

import pytest
import pytest_asyncio
from motor.motor_asyncio import AsyncIOMotorClient

from config import get_settings

pytestmark = pytest.mark.perf

REAL_DB_NAME = "outflo_v3"


@pytest_asyncio.fixture(scope="session")
async def real_db():
    settings = get_settings()
    client = AsyncIOMotorClient(settings.mongodb_url)
    db = client[REAL_DB_NAME]
    assert db.name == REAL_DB_NAME
    await client.admin.command("ping")
    yield db
    client.close()


class _Stopwatch:
    def __init__(self, perf_metrics, name, **extra):
        self._metrics = perf_metrics
        self._name = name
        self._extra = extra

    def __enter__(self):
        self._t0 = time.perf_counter()
        return self

    def __exit__(self, exc_type, *exc):
        self.ms = round((time.perf_counter() - self._t0) * 1000, 1)
        if exc_type is None:
            self._metrics.append({"name": self._name, "ms": self.ms, **self._extra})


async def test_perf_estimated_counts(real_db, perf_metrics):
    with _Stopwatch(perf_metrics, "estimated_counts_prospects_companies") as sw:
        prospects = await real_db["prospects"].estimated_document_count()
        companies = await real_db["companies"].estimated_document_count()
    perf_metrics.append({"name": "pool_size", "ms": 0,
                         "prospects": prospects, "companies": companies})
    assert prospects > 10_000, "expected the migrated pool (~47k prospects)"
    assert companies > 5_000


async def test_perf_pool_stats_coverage_counts(real_db, perf_metrics):
    """The count battery behind GET /api/admin/pool/stats (admin_pool.py)."""
    p, c = real_db["prospects"], real_db["companies"]
    _set = {"$exists": True, "$nin": [None, ""]}

    checks = {
        "prospects_location": (p, {"location.country_code": _set}),
        "prospects_industry": (p, {"company_industry_id": _set}),
        "prospects_embeddings": (p, {"title_vec": {"$exists": True, "$ne": None}}),
        "prospects_email": (p, {"email": _set}),
        "prospects_phone": (p, {"phone": _set}),
        "prospects_company_linked": (p, {"company_id": _set}),
        "companies_location": (c, {"location.country_code": _set}),
        "companies_industry": (c, {"industry.id": _set}),
        "companies_embeddings": (c, {"profile_vec": {"$exists": True, "$ne": None}}),
    }
    total_ms = 0.0
    for name, (coll, query) in checks.items():
        with _Stopwatch(perf_metrics, f"pool_stats_count_{name}") as sw:
            count = await coll.count_documents(query)
        total_ms += sw.ms
        assert count >= 0
    perf_metrics.append({"name": "pool_stats_count_battery_total", "ms": round(total_ms, 1)})


async def test_perf_prospect_state_group_by_account(real_db, perf_metrics):
    with _Stopwatch(perf_metrics, "prospect_state_group_by_account"):
        rows = await real_db["prospect_state"].aggregate([
            {"$group": {"_id": "$account_id", "count": {"$sum": 1}}},
            {"$sort": {"count": -1}},
            {"$limit": 200},
        ]).to_list(200)
    assert isinstance(rows, list)


async def test_perf_prospects_list_query_pattern(real_db, perf_metrics):
    """The two-phase query pattern of GET /api/prospects (routes/prospects.py):
    1) fetch ALL prospect_state docs for one account,
    2) $in-fetch one page of prospect docs.

    prospect_state is empty right after the outflo_v3 migration; in that case
    a synthetic 2000-id overlay sampled from prospects (read-only) stands in,
    which matches the expected per-account overlay size."""
    from bson import ObjectId

    sample = await real_db["prospect_state"].find_one({})
    if sample:
        account_id = sample["account_id"]
        with _Stopwatch(perf_metrics, "prospects_list_phase1_state_fetch"):
            state_docs = await real_db["prospect_state"].find(
                {"account_id": account_id},
                {"prospect_id": 1, "ai_score": 1, "status": 1, "priority_tier": 1},
            ).to_list(None)
    else:
        with _Stopwatch(perf_metrics, "prospects_list_phase1_synthetic_2000id_sample"):
            docs = await real_db["prospects"].find({}, {"_id": 1}).limit(2000).to_list(2000)
        state_docs = [{"prospect_id": str(d["_id"]), "ai_score": 50.0} for d in docs]
    perf_metrics.append({"name": "prospects_list_state_docs_returned", "ms": 0,
                         "count": len(state_docs)})

    def _oid(x):
        try:
            return ObjectId(x) if isinstance(x, str) else x
        except Exception:
            return None

    sorted_pids = sorted(state_docs, key=lambda d: d.get("ai_score") or 0, reverse=True)
    page_oids = [o for d in sorted_pids[:50] if (o := _oid(d.get("prospect_id")))]

    with _Stopwatch(perf_metrics, "prospects_list_phase2_page_fetch"):
        page = await real_db["prospects"].find(
            {"_id": {"$in": page_oids}},
            {"full_name": 1, "email": 1, "job_title": 1, "company_name": 1,
             "location": 1, "seniority": 1},
        ).to_list(50)
    assert len(page) <= 50

    # the count_documents the endpoint also runs on every request
    all_oids = [o for d in state_docs if (o := _oid(d.get("prospect_id")))]
    with _Stopwatch(perf_metrics, "prospects_list_count_documents_id_in",
                    n_ids=len(all_oids)):
        total = await real_db["prospects"].count_documents({"_id": {"$in": all_oids}})
    assert total >= 0


async def test_perf_prospect_stats_aggregations(real_db, perf_metrics):
    """The aggregation battery behind GET /api/prospects/stats (new-schema path)."""
    from bson import ObjectId

    sample = await real_db["prospect_state"].find_one({})
    if sample:
        account_id = sample["account_id"]
        state_docs = await real_db["prospect_state"].find(
            {"account_id": account_id}, {"prospect_id": 1}).to_list(None)

        def _oid(x):
            try:
                return ObjectId(x) if isinstance(x, str) else x
            except Exception:
                return None

        oids = [o for d in state_docs if (o := _oid(d.get("prospect_id")))]
    else:
        # synthetic overlay (see test above) — prospect_state empty post-migration
        docs = await real_db["prospects"].find({}, {"_id": 1}).limit(2000).to_list(2000)
        oids = [d["_id"] for d in docs]

    pipelines = {
        "stats_by_industry": [
            {"$match": {"_id": {"$in": oids}, "company_industry_id": {"$ne": None}}},
            {"$group": {"_id": "$company_industry_id", "count": {"$sum": 1}}},
            {"$sort": {"count": -1}}, {"$limit": 15},
        ],
        "stats_by_country": [
            {"$match": {"_id": {"$in": oids}, "location.country_code": {"$ne": None}}},
            {"$group": {"_id": "$location.country_code", "count": {"$sum": 1}}},
            {"$sort": {"count": -1}}, {"$limit": 10},
        ],
        "stats_unique_companies": [
            {"$match": {"_id": {"$in": oids}, "company_name": {"$nin": [None, ""]}}},
            {"$group": {"_id": "$company_name"}}, {"$count": "n"},
        ],
    }
    for name, pipeline in pipelines.items():
        with _Stopwatch(perf_metrics, name, n_ids=len(oids)):
            await real_db["prospects"].aggregate(pipeline).to_list(20)


async def test_perf_usage_rollups(real_db, perf_metrics):
    """Unified usage rollups (admin_usage.py) — collections likely near-empty."""
    day_group = {
        "account_id": "$account_id",
        "day": {"$dateToString": {"format": "%Y-%m-%d", "date": "$created_at"}},
    }
    with _Stopwatch(perf_metrics, "usage_growthtoolkit_rollup"):
        await real_db["growthtoolkit_usage"].aggregate([
            {"$group": {"_id": day_group,
                        "calls": {"$sum": 1},
                        "credits": {"$sum": {"$ifNull": ["$credits_used", 0]}}}},
            {"$limit": 5000},
        ]).to_list(5000)

    with _Stopwatch(perf_metrics, "usage_apify_rollup"):
        await real_db["apify_usage"].aggregate([
            {"$group": {"_id": {"account_id": "$account_id",
                                "day": {"$dateToString": {"format": "%Y-%m-%d",
                                                          "date": "$started_at"}}},
                        "cost": {"$sum": "$cost_usd"}, "runs": {"$sum": 1}}},
            {"$limit": 5000},
        ]).to_list(5000)

    with _Stopwatch(perf_metrics, "usage_openrouter_rollup"):
        await real_db["openrouter_usage"].aggregate([
            {"$group": {"_id": {"account_id": "$account_id",
                                "day": {"$dateToString": {"format": "%Y-%m-%d",
                                                          "date": "$requested_at"}}},
                        "cost": {"$sum": "$cost_usd"}, "calls": {"$sum": 1}}},
            {"$limit": 5000},
        ]).to_list(5000)


async def test_perf_accounts_list_rollup(real_db, perf_metrics):
    """The 3-lookup pipeline behind GET /api/admin/accounts (admin_accounts.py)."""
    pipeline = [
        {"$sort": {"created_at": -1}},
        {"$limit": 25},
        {"$lookup": {"from": "account_members", "localField": "_id",
                     "foreignField": "account_id", "as": "_members"}},
        {"$lookup": {"from": "campaigns",
                     "let": {"aid": "$_id"},
                     "pipeline": [{"$match": {"$expr": {"$eq": ["$account_id", "$$aid"]}}},
                                  {"$project": {"status": 1, "updated_at": 1}}],
                     "as": "_camps"}},
        {"$lookup": {"from": "campaign_enrollments",
                     "let": {"aid": "$_id"},
                     "pipeline": [{"$match": {"$expr": {"$eq": ["$account_id", "$$aid"]}}},
                                  {"$count": "n"}],
                     "as": "_enr"}},
        {"$addFields": {"member_count": {"$size": "$_members"}}},
        {"$project": {"_members": 0, "_camps": 0, "_enr": 0}},
    ]
    with _Stopwatch(perf_metrics, "admin_accounts_list_lookup_pipeline"):
        rows = await real_db["accounts"].aggregate(pipeline).to_list(25)
    assert isinstance(rows, list)


async def test_perf_vector_search_smoke(real_db, perf_metrics):
    """One $vectorSearch query on prospects_vec using a stored embedding."""
    seed = await real_db["prospects"].find_one(
        {"title_vec": {"$exists": True, "$ne": None}}, {"title_vec": 1})
    if not seed or not seed.get("title_vec"):
        pytest.skip("no prospect with title_vec found")

    pipeline = [
        {"$vectorSearch": {
            "index": "prospects_vec",
            "path": "title_vec",
            "queryVector": seed["title_vec"],
            "numCandidates": 100,
            "limit": 5,
        }},
        {"$project": {"full_name": 1, "job_title": 1,
                      "score": {"$meta": "vectorSearchScore"}}},
    ]
    try:
        with _Stopwatch(perf_metrics, "vector_search_prospects_vec_top5"):
            rows = await real_db["prospects"].aggregate(pipeline).to_list(5)
    except Exception as exc:
        pytest.skip(f"$vectorSearch unavailable: {exc}")
    assert len(rows) >= 1
    assert rows[0]["score"] > 0


# ---------------------------------------------------------------------------
# Iteration 2: consolidated pool/stats (single $group pass per collection)
# ---------------------------------------------------------------------------

async def test_perf_pool_stats_single_pass(real_db, perf_metrics):
    """The rewritten GET /api/admin/pool/stats (routes/admin_pool.py):
    one $group aggregation per collection computes all coverage numerators,
    estimated_document_count for totals, overlay $group — all gathered in
    parallel. Compare against pool_stats_count_battery_total (old pattern,
    still measured above)."""
    import asyncio

    from routes.admin_pool import _sum_if_set, _sum_if_not_null

    p, c = real_db["prospects"], real_db["companies"]
    prospects_pipeline = [
        {"$group": {
            "_id": None,
            "location": _sum_if_set("location.country_code"),
            "industry": _sum_if_set("company_industry_id"),
            "embeddings": _sum_if_not_null("title_vec"),
            "email": _sum_if_set("email"),
            "phone": _sum_if_set("phone"),
            "company_linked": _sum_if_set("company_id"),
        }},
    ]
    companies_pipeline = [
        {"$group": {
            "_id": None,
            "location": _sum_if_set("location.country_code"),
            "industry": _sum_if_set("industry.id"),
            "embeddings": _sum_if_not_null("profile_vec"),
        }},
    ]

    with _Stopwatch(perf_metrics, "pool_stats_single_pass_prospects"):
        p_rows = await p.aggregate(prospects_pipeline).to_list(1)
    with _Stopwatch(perf_metrics, "pool_stats_single_pass_companies"):
        c_rows = await c.aggregate(companies_pipeline).to_list(1)
    assert p_rows and c_rows
    for key in ("location", "industry", "embeddings", "email", "phone", "company_linked"):
        assert p_rows[0][key] >= 0

    # Endpoint-equivalent: everything the route awaits, in parallel
    with _Stopwatch(perf_metrics, "pool_stats_endpoint_equivalent_total") as sw:
        await asyncio.gather(
            p.estimated_document_count(),
            c.estimated_document_count(),
            p.aggregate(prospects_pipeline).to_list(1),
            c.aggregate(companies_pipeline).to_list(1),
            real_db["prospect_state"].aggregate([
                {"$group": {"_id": "$account_id", "count": {"$sum": 1}}},
                {"$sort": {"count": -1}},
                {"$limit": 200},
            ]).to_list(200),
        )
    # iteration-1 target: < ~400 ms (old sequential battery: 601-771 ms).
    # Generous ceiling to absorb Atlas/network variance; actual value is
    # recorded in perf_metrics for the report.
    assert sw.ms < 1200, f"pool stats endpoint-equivalent took {sw.ms} ms"


async def test_perf_single_pass_counts_match_count_documents(real_db, perf_metrics):
    """Correctness cross-check: the $group accumulators must produce the exact
    same numbers as the count_documents battery they replaced (read-only)."""
    from routes.admin_pool import _sum_if_set, _sum_if_not_null

    p = real_db["prospects"]
    _set = {"$exists": True, "$nin": [None, ""]}
    rows = await p.aggregate([
        {"$group": {
            "_id": None,
            "email": _sum_if_set("email"),
            "location": _sum_if_set("location.country_code"),
            "embeddings": _sum_if_not_null("title_vec"),
        }},
    ]).to_list(1)
    agg = rows[0]
    assert agg["email"] == await p.count_documents({"email": _set})
    assert agg["location"] == await p.count_documents({"location.country_code": _set})
    assert agg["embeddings"] == await p.count_documents({"title_vec": {"$exists": True, "$ne": None}})
