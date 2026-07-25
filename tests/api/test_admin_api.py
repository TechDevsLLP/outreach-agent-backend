"""API tests: superadmin suite (/api/admin/*).

Covers: authz, accounts list/detail/suspend/reactivate/extend-trial/quotas,
runtime flags, audit log, usage rollups (seeded usage docs), pool stats/
quality/reenrich dry-run, suppression CRUD, health/deep, jobs, webhook log,
stuck campaigns + force-pause/resume, users list + impersonation.
"""
from datetime import datetime, timedelta, timezone

import pytest
from bson import ObjectId

import database
from services import system_settings_service

pytestmark = pytest.mark.api


# ---------------------------------------------------------------------------
# Authz
# ---------------------------------------------------------------------------

@pytest.mark.parametrize("path", [
    "/api/admin/accounts",
    "/api/admin/health/deep",
    "/api/admin/usage/unified",
    "/api/admin/pool/stats",
    "/api/admin/settings/flags",
    "/api/admin/audit-log",
    "/api/admin/jobs",
    "/api/admin/webhooks/recent",
    "/api/admin/campaigns/stuck",
    "/api/admin/users",
])
async def test_admin_endpoints_403_for_regular_user(client, auth_headers_a, path):
    resp = await client.get(path, headers=auth_headers_a)
    assert resp.status_code == 403


async def test_admin_endpoints_401_without_token(client):
    resp = await client.get("/api/admin/accounts")
    assert resp.status_code == 401


# ---------------------------------------------------------------------------
# Accounts
# ---------------------------------------------------------------------------

async def test_list_accounts_shape(client, superadmin_headers, identity_a):
    resp = await client.get("/api/admin/accounts", headers=superadmin_headers)
    assert resp.status_code == 200, resp.text
    body = resp.json()
    assert body["total"] >= 2
    row = next(a for a in body["accounts"] if a["_id"] == identity_a["account_id"])
    for key in ("name", "slug", "status", "member_count",
                "campaign_count_total", "campaign_count_active",
                "enrolled_prospect_count"):
        assert key in row
    assert row["member_count"] == 1


async def test_list_accounts_search_filter(client, superadmin_headers, identity_a):
    resp = await client.get("/api/admin/accounts?search=TestCo", headers=superadmin_headers)
    body = resp.json()
    assert body["total"] >= 1
    assert all("testco" in (a["name"] or "").lower() or "testco" in (a["slug"] or "").lower()
               for a in body["accounts"])


async def test_account_detail_shape(client, superadmin_headers, identity_a):
    resp = await client.get(f"/api/admin/accounts/{identity_a['account_id']}",
                            headers=superadmin_headers)
    assert resp.status_code == 200, resp.text
    body = resp.json()
    assert body["account"]["_id"] == identity_a["account_id"]
    assert body["members"][0]["email"] == identity_a["email"]
    assert "usage_30d" in body and "quotas" in body
    for key in ("apify_cost_usd", "openrouter_cost_usd", "growthtoolkit_credits", "total_cost_usd"):
        assert key in body["usage_30d"]


async def test_account_detail_unknown_404(client, superadmin_headers):
    resp = await client.get(f"/api/admin/accounts/{ObjectId()}", headers=superadmin_headers)
    assert resp.status_code == 404


async def test_suspend_blocks_context_then_reactivate(client, superadmin_headers, create_identity):
    victim = await create_identity("victim@test.outflo.local", "Victim", "VictimCo")
    aid = victim["account_id"]

    # suspend
    resp = await client.post(f"/api/admin/accounts/{aid}/suspend", headers=superadmin_headers)
    assert resp.status_code == 200
    assert resp.json()["status"] == "suspended"

    # get_account_context now blocks the tenant with 402
    blocked = await client.get("/api/prospects", headers=victim["headers"])
    assert blocked.status_code == 402

    # reactivate
    resp = await client.post(f"/api/admin/accounts/{aid}/reactivate", headers=superadmin_headers)
    assert resp.status_code == 200
    ok = await client.get("/api/prospects", headers=victim["headers"])
    assert ok.status_code == 200

    # audit rows written for both mutations
    actions = [r["action"] async for r in database.admin_audit_log_collection.find(
        {"target_id": aid})]
    assert "account.suspend" in actions
    assert "account.reactivate" in actions


async def test_extend_trial(client, superadmin_headers, identity_b):
    aid = identity_b["account_id"]
    resp = await client.post(f"/api/admin/accounts/{aid}/extend-trial",
                             headers=superadmin_headers, json={"days": 14})
    assert resp.status_code == 200
    new_end = datetime.fromisoformat(resp.json()["trial_ends_at"])
    assert new_end > datetime.utcnow() + timedelta(days=13)

    bad = await client.post(f"/api/admin/accounts/{aid}/extend-trial",
                            headers=superadmin_headers, json={"days": 0})
    assert bad.status_code == 422  # ge=1 validation


async def test_quota_patch_writes_overrides(client, superadmin_headers, identity_a):
    aid = identity_a["account_id"]
    resp = await client.patch(f"/api/admin/accounts/{aid}/quotas", headers=superadmin_headers,
                              json={"email": 33, "linkedin_connection": 7})
    assert resp.status_code == 200, resp.text
    assert resp.json()["quota_overrides"] == {"email": 33, "linkedin_connection": 7}

    account = await database.accounts_collection.find_one({"_id": ObjectId(aid)})
    assert account["quota_overrides"]["email"] == 33
    assert account["daily_email_quota"] == 33  # legacy display field synced

    # explicit null clears an override
    resp = await client.patch(f"/api/admin/accounts/{aid}/quotas", headers=superadmin_headers,
                              json={"email": None})
    assert resp.status_code == 200
    assert "email" not in resp.json()["quota_overrides"]

    empty = await client.patch(f"/api/admin/accounts/{aid}/quotas",
                               headers=superadmin_headers, json={})
    assert empty.status_code == 400


# ---------------------------------------------------------------------------
# Runtime flags
# ---------------------------------------------------------------------------

async def test_flags_get_patch_effective_and_clear(client, superadmin_headers):
    # baseline
    resp = await client.get("/api/admin/settings/flags", headers=superadmin_headers)
    assert resp.status_code == 200
    names = {f["name"] for f in resp.json()["flags"]}
    assert {"quality_gates_enabled", "title_gate_enabled", "prefilter_gate_enabled"} <= names

    # set an override
    patch = await client.patch("/api/admin/settings/flags", headers=superadmin_headers,
                               json={"flags": {"title_gate_enabled": False}})
    assert patch.status_code == 200
    assert patch.json()["flags"]["title_gate_enabled"]["effective"] is False

    # override is effective via get_flag (cache invalidated by set_flag)
    system_settings_service.invalidate_cache()
    assert await system_settings_service.get_flag("title_gate_enabled", True) is False

    # GET reflects override
    resp = await client.get("/api/admin/settings/flags", headers=superadmin_headers)
    flag = next(f for f in resp.json()["flags"] if f["name"] == "title_gate_enabled")
    assert flag["override"] is False and flag["effective"] is False

    # clear via null -> env default wins again
    clear = await client.patch("/api/admin/settings/flags", headers=superadmin_headers,
                               json={"flags": {"title_gate_enabled": None}})
    assert clear.status_code == 200
    system_settings_service.invalidate_cache()
    assert await system_settings_service.get_flag("title_gate_enabled", True) is True


async def test_flags_unknown_name_400(client, superadmin_headers):
    resp = await client.patch("/api/admin/settings/flags", headers=superadmin_headers,
                              json={"flags": {"made_up_flag": True}})
    assert resp.status_code == 400


# ---------------------------------------------------------------------------
# Audit log endpoint
# ---------------------------------------------------------------------------

async def test_audit_log_lists_mutations(client, superadmin_headers, superadmin):
    resp = await client.get("/api/admin/audit-log", headers=superadmin_headers)
    assert resp.status_code == 200
    body = resp.json()
    assert body["total"] >= 1
    actions = {e["action"] for e in body["entries"]}
    assert "settings.flags_update" in actions

    filtered = await client.get(
        f"/api/admin/audit-log?admin_email={superadmin['email']}&action=settings.flags_update",
        headers=superadmin_headers)
    assert filtered.status_code == 200
    assert all(e["action"] == "settings.flags_update" for e in filtered.json()["entries"])


# ---------------------------------------------------------------------------
# Usage endpoints (seeded usage docs)
# ---------------------------------------------------------------------------

@pytest.fixture(scope="module")
async def seeded_usage(identity_a):
    now = datetime.now(timezone.utc)
    aid = identity_a["account_id"]
    await database.apify_usage_collection.insert_many([
        {"account_id": aid, "actor_id": "actorX", "cost_usd": 0.25,
         "started_at": now - timedelta(hours=2), "campaign_id": None},
        {"account_id": aid, "actor_id": "actorY", "cost_usd": 0.75,
         "started_at": now - timedelta(days=1), "campaign_id": "c1"},
    ])
    await database.openrouter_usage_collection.insert_many([
        {"account_id": aid, "model": "anthropic/claude-haiku-4-5", "feature": "assessment",
         "cost_usd": 0.01, "requested_at": now - timedelta(hours=1), "campaign_id": "c1"},
    ])
    await database.growthtoolkit_usage_collection.insert_many([
        {"account_id": aid, "endpoint": "email-finder", "credits_used": 1, "success": True,
         "code": "200", "duration_ms": 350, "prospect_id": None, "created_at": now},
        {"account_id": aid, "endpoint": "linkedin-enrichment", "credits_used": 1, "success": True,
         "code": "200", "duration_ms": 900, "prospect_id": None, "created_at": now},
        {"account_id": aid, "endpoint": "email-finder", "credits_used": 0, "success": False,
         "code": "resource_not_found", "duration_ms": 200, "prospect_id": None,
         "created_at": now - timedelta(days=2)},
    ])
    return {"account_id": aid, "apify_total": 1.0, "gt_credits": 2}


async def test_usage_growthtoolkit_rollup(client, superadmin_headers, seeded_usage):
    resp = await client.get("/api/admin/usage/growthtoolkit", headers=superadmin_headers)
    assert resp.status_code == 200, resp.text
    body = resp.json()
    assert body["totals"]["calls"] >= 3
    assert body["totals"]["credits_used"] >= 2
    assert 0 <= body["totals"]["success_rate"] <= 1
    row = next(r for r in body["rows"] if r["endpoint"] == "linkedin-enrichment")
    assert row["credits_used"] >= 1


async def test_usage_growthtoolkit_account_filter(client, superadmin_headers, seeded_usage):
    resp = await client.get(
        f"/api/admin/usage/growthtoolkit?account_id={seeded_usage['account_id']}",
        headers=superadmin_headers)
    assert resp.status_code == 200
    assert all(r["account_id"] == seeded_usage["account_id"] for r in resp.json()["rows"])


async def test_usage_unified_merges_sources(client, superadmin_headers, seeded_usage):
    resp = await client.get("/api/admin/usage/unified", headers=superadmin_headers)
    assert resp.status_code == 200, resp.text
    rows = resp.json()["rows"]
    mine = [r for r in rows if r["account_id"] == seeded_usage["account_id"]]
    assert mine, "expected at least one unified row for the seeded account"
    total_apify = sum(r["apify_cost_usd"] for r in mine)
    assert total_apify == pytest.approx(1.0)
    for r in mine:
        assert "total_cost_usd" in r and "growthtoolkit_credits" in r


async def test_usage_unified_bad_date_400(client, superadmin_headers):
    resp = await client.get("/api/admin/usage/unified?from=not-a-date",
                            headers=superadmin_headers)
    assert resp.status_code == 400


async def test_costs_top_accounts(client, superadmin_headers, seeded_usage):
    resp = await client.get("/api/admin/costs/top-accounts", headers=superadmin_headers)
    assert resp.status_code == 200
    rows = resp.json()["rows"]
    mine = next((r for r in rows if r["account_id"] == seeded_usage["account_id"]), None)
    assert mine is not None
    assert mine["total_cost_usd"] == pytest.approx(1.01)
    assert mine["growthtoolkit_credits"] >= 2
    assert mine["account_name"] is not None


async def test_usage_apify_and_openrouter_and_summary_smoke(client, superadmin_headers, seeded_usage):
    apify = await client.get("/api/admin/usage/apify", headers=superadmin_headers)
    assert apify.status_code == 200
    assert apify.json()["total_cost_usd"] >= 1.0

    openrouter = await client.get("/api/admin/usage/openrouter", headers=superadmin_headers)
    assert openrouter.status_code == 200

    summary = await client.get("/api/admin/usage/summary", headers=superadmin_headers)
    assert summary.status_code == 200
    body = summary.json()
    for period in ("today", "this_week", "this_month"):
        assert set(body[period]) == {"apify_cost", "openrouter_cost", "growthtoolkit_credits"}


# ---------------------------------------------------------------------------
# Pool
# ---------------------------------------------------------------------------

async def test_pool_stats_on_seeded_pool(client, superadmin_headers, seeded_prospects, identity_a):
    # refresh=true so the assertion sees the just-seeded pool even if an
    # earlier test populated the 60 s cache
    resp = await client.get("/api/admin/pool/stats?refresh=true", headers=superadmin_headers)
    assert resp.status_code == 200, resp.text
    body = resp.json()
    assert body["prospects"]["total"] >= 6
    cov = body["prospects"]["coverage"]
    for key in ("location", "canonical_industry", "embeddings", "email", "phone", "company_linked"):
        assert key in cov
    assert cov["email"]["count"] >= 4
    by_account = {r["account_id"]: r for r in body["prospect_state_by_account"]}
    assert by_account[identity_a["account_id"]]["count"] >= 5
    assert by_account[identity_a["account_id"]]["account_name"] == "TestCo A"


async def test_pool_stats_cached_and_refresh_bypass(client, superadmin_headers, seeded_prospects):
    """Iteration-3: /pool/stats is cached in-process for 60 s (same pattern as
    /health/deep's search-index cache); refresh=true bypasses + repopulates."""
    from routes import admin_pool

    warm = await client.get("/api/admin/pool/stats?refresh=true", headers=superadmin_headers)
    assert warm.status_code == 200

    # poison the cache -> a plain GET must serve the poisoned value (no DB hit)
    sentinel = {**warm.json(), "prospects": {**warm.json()["prospects"], "total": -1}}
    admin_pool._pool_stats_cache["value"] = sentinel
    admin_pool._pool_stats_cache["ts"] = __import__("time").monotonic()
    cached = await client.get("/api/admin/pool/stats", headers=superadmin_headers)
    assert cached.json()["prospects"]["total"] == -1

    # refresh=true bypasses and overwrites the poisoned cache
    fresh = await client.get("/api/admin/pool/stats?refresh=true", headers=superadmin_headers)
    assert fresh.json()["prospects"]["total"] >= 6
    assert admin_pool._pool_stats_cache["value"]["prospects"]["total"] >= 6

    # expired TTL -> refetched
    admin_pool._pool_stats_cache["value"] = sentinel
    admin_pool._pool_stats_cache["ts"] = __import__("time").monotonic() - admin_pool._POOL_STATS_CACHE_TTL - 1
    expired = await client.get("/api/admin/pool/stats", headers=superadmin_headers)
    assert expired.json()["prospects"]["total"] >= 6


async def test_pool_quality_shape(client, superadmin_headers, seeded_prospects):
    resp = await client.get("/api/admin/pool/quality", headers=superadmin_headers)
    assert resp.status_code == 200, resp.text
    body = resp.json()
    assert set(body) == {"duplicates", "missing_fields", "staleness"}
    assert body["missing_fields"]["email"] >= 1     # bob has no email
    assert body["missing_fields"]["linkedin"] >= 1  # carol has no linkedin
    assert body["staleness"]["stale_days_threshold"] == 180


async def test_pool_reenrich_dry_run(client, superadmin_headers, seeded_prospects, identity_a):
    resp = await client.post("/api/admin/pool/reenrich", headers=superadmin_headers,
                             json={"account_id": identity_a["account_id"], "dry_run": True})
    assert resp.status_code == 200, resp.text
    body = resp.json()
    assert body["dry_run"] is True
    assert body["matched"] >= 5
    assert len(body["sample_prospect_ids"]) >= 1
    # dry run must not create an enrichment run
    run = await database.enrichment_runs_collection.find_one({"triggered_via": "admin_reenrich"})
    assert run is None


async def test_pool_reenrich_requires_filter(client, superadmin_headers):
    resp = await client.post("/api/admin/pool/reenrich", headers=superadmin_headers,
                             json={"dry_run": True})
    assert resp.status_code == 400


async def test_pool_reenrich_invalid_prospect_id_400(client, superadmin_headers):
    resp = await client.post("/api/admin/pool/reenrich", headers=superadmin_headers,
                             json={"prospect_ids": ["nope"], "dry_run": True})
    assert resp.status_code == 400


# ---------------------------------------------------------------------------
# Suppression CRUD
# ---------------------------------------------------------------------------

async def test_suppression_crud_cycle(client, superadmin_headers, identity_a):
    aid = identity_a["account_id"]

    created = await client.post("/api/admin/suppression", headers=superadmin_headers, json={
        "account_id": aid, "identifier_type": "email",
        "identifier": "Blocked@Example.COM", "reason": "manual", "notes": "test",
    })
    assert created.status_code == 201, created.text
    doc = created.json()
    assert doc["identifier"] == "blocked@example.com"  # lowercased
    assert doc["source"] == "human"

    listed = await client.get(f"/api/admin/suppression?account_id={aid}",
                              headers=superadmin_headers)
    assert listed.status_code == 200
    assert any(s["_id"] == doc["_id"] for s in listed.json()["suppressions"])

    by_identifier = await client.get(
        "/api/admin/suppression?identifier=blocked@example.com", headers=superadmin_headers)
    assert by_identifier.json()["total"] == 1

    # idempotent create (upsert on the composite key)
    dup = await client.post("/api/admin/suppression", headers=superadmin_headers, json={
        "account_id": aid, "identifier_type": "email",
        "identifier": "blocked@example.com", "reason": "manual",
    })
    assert dup.status_code == 201
    assert dup.json()["_id"] == doc["_id"]

    deleted = await client.delete(f"/api/admin/suppression/{doc['_id']}",
                                  headers=superadmin_headers)
    assert deleted.status_code == 204

    gone = await client.delete(f"/api/admin/suppression/{doc['_id']}",
                               headers=superadmin_headers)
    assert gone.status_code == 404


async def test_suppression_validation(client, superadmin_headers, identity_a):
    bad_type = await client.post("/api/admin/suppression", headers=superadmin_headers, json={
        "account_id": identity_a["account_id"], "identifier_type": "carrier_pigeon",
        "identifier": "x", "reason": "manual",
    })
    assert bad_type.status_code == 400

    bad_reason = await client.post("/api/admin/suppression", headers=superadmin_headers, json={
        "account_id": identity_a["account_id"], "identifier_type": "email",
        "identifier": "x@y.z", "reason": "because",
    })
    assert bad_reason.status_code == 400


# ---------------------------------------------------------------------------
# Health / jobs / webhooks
# ---------------------------------------------------------------------------

async def test_health_deep(client, superadmin_headers):
    resp = await client.get("/api/admin/health/deep", headers=superadmin_headers)
    assert resp.status_code == 200, resp.text
    body = resp.json()
    assert body["mongo"]["status"] == "connected"
    assert body["mongo"]["database"] == "outflo_v3_test"
    assert "collection_counts" in body["mongo"]
    # search-index status may legitimately be "unknown" on the test DB
    assert body["search_indexes"]["prospects"] is not None
    assert isinstance(body["provider_keys_configured"], dict)
    assert body["app_role"] == "web"


async def test_jobs_listing(client, superadmin_headers):
    await database.scheduler_heartbeats_collection.update_one(
        {"job_id": "test_job"},
        {"$set": {"job_id": "test_job", "last_run_at": datetime.utcnow(), "status": "ok"}},
        upsert=True,
    )
    resp = await client.get("/api/admin/jobs", headers=superadmin_headers)
    assert resp.status_code == 200
    jobs = resp.json()["jobs"]
    job = next(j for j in jobs if j["job_id"] == "test_job")
    assert job["seconds_since_last_run"] is not None
    assert job["seconds_since_last_run"] < 120


async def test_webhooks_recent_with_seeded_log(client, superadmin_headers):
    now = datetime.now(timezone.utc)
    await database.webhook_log_collection.insert_many([
        {"source": "sendgrid", "event_type": "delivered", "status": "ok",
         "received_at": now, "payload_size": 120},
        {"source": "unipile", "event_type": "message", "status": "error",
         "error": "boom", "received_at": now - timedelta(minutes=1), "payload_size": 80},
    ])
    resp = await client.get("/api/admin/webhooks/recent", headers=superadmin_headers)
    assert resp.status_code == 200
    assert resp.json()["count"] >= 2

    filtered = await client.get("/api/admin/webhooks/recent?source=sendgrid&status=ok",
                                headers=superadmin_headers)
    rows = filtered.json()["webhooks"]
    assert rows and all(r["source"] == "sendgrid" and r["status"] == "ok" for r in rows)


# ---------------------------------------------------------------------------
# Stuck campaigns + force pause/resume
# ---------------------------------------------------------------------------

@pytest.fixture(scope="module")
async def stuck_campaign(identity_a):
    doc = {
        "account_id": ObjectId(identity_a["account_id"]),
        "name": "Stuck Discovery",
        "status": "active",
        "is_smart_campaign": True,
        "discovery_status": "sourcing_companies",
        "discovery_started_at": datetime.utcnow() - timedelta(days=3),
        "created_at": datetime.utcnow() - timedelta(days=3),
        "updated_at": datetime.utcnow() - timedelta(days=2),
    }
    result = await database.campaigns_collection.insert_one(doc)
    return str(result.inserted_id)


async def test_stuck_campaigns_detected(client, superadmin_headers, stuck_campaign):
    resp = await client.get("/api/admin/campaigns/stuck?hours=24", headers=superadmin_headers)
    assert resp.status_code == 200
    body = resp.json()
    assert body["count"] >= 1
    assert any(c["_id"] == stuck_campaign for c in body["campaigns"])


async def test_force_pause_and_resume(client, superadmin_headers, stuck_campaign):
    pause = await client.post(f"/api/admin/campaigns/{stuck_campaign}/force-pause",
                              headers=superadmin_headers)
    assert pause.status_code == 200
    doc = await database.campaigns_collection.find_one({"_id": ObjectId(stuck_campaign)})
    assert doc["status"] == "paused"
    assert doc["admin_forced_pause"] is True

    resume = await client.post(f"/api/admin/campaigns/{stuck_campaign}/force-resume",
                               headers=superadmin_headers)
    assert resume.status_code == 200
    doc = await database.campaigns_collection.find_one({"_id": ObjectId(stuck_campaign)})
    assert doc["status"] == "active"
    assert doc["admin_forced_pause"] is False

    audit_actions = [r["action"] async for r in database.admin_audit_log_collection.find(
        {"target_id": stuck_campaign})]
    assert "campaign.force_pause" in audit_actions
    assert "campaign.force_resume" in audit_actions


async def test_force_pause_unknown_campaign_404(client, superadmin_headers):
    resp = await client.post(f"/api/admin/campaigns/{ObjectId()}/force-pause",
                             headers=superadmin_headers)
    assert resp.status_code == 404


# ---------------------------------------------------------------------------
# Users + impersonation
# ---------------------------------------------------------------------------

async def test_admin_users_list(client, superadmin_headers, identity_a):
    resp = await client.get("/api/admin/users", headers=superadmin_headers)
    assert resp.status_code == 200
    body = resp.json()
    assert body["total"] >= 3
    me = next(u for u in body["users"] if u["email"] == identity_a["email"])
    assert "password_hash" not in me


async def test_impersonation_token_works(client, superadmin_headers, identity_a):
    resp = await client.post(f"/api/admin/impersonate/{identity_a['user_id']}",
                             headers=superadmin_headers)
    assert resp.status_code == 200, resp.text
    body = resp.json()
    assert body["user"]["email"] == identity_a["email"]

    # impersonation token acts as the target user
    me = await client.get("/api/auth/me",
                          headers={"Authorization": f"Bearer {body['token']}"})
    assert me.status_code == 200
    assert me.json()["user"]["email"] == identity_a["email"]

    audit = await database.admin_audit_log_collection.find_one(
        {"action": "user.impersonate", "target_id": identity_a["user_id"]})
    assert audit is not None


# ---------------------------------------------------------------------------
# Iteration 2: health/deep search-index cache (+ ?refresh=true bypass)
# ---------------------------------------------------------------------------

async def test_health_deep_search_index_cache_and_refresh(client, superadmin_headers, perf_metrics):
    """list_search_indexes costs ~850 ms on Atlas; the result is cached
    in-process for 5 min. Prove the cache is served, and that ?refresh=true
    bypasses and repopulates it."""
    import time as _time
    from routes import admin_system

    # start from a cold cache so first_ms measures the real Atlas round-trip
    admin_system._search_index_cache["value"] = None
    admin_system._search_index_cache["ts"] = 0.0

    t0 = _time.perf_counter()
    first = await client.get("/api/admin/health/deep", headers=superadmin_headers)
    first_ms = round((_time.perf_counter() - t0) * 1000, 1)
    assert first.status_code == 200

    # Poison the cache: the next (non-refresh) call must return the sentinel,
    # proving no live list_search_indexes round-trip happened.
    sentinel = {"prospects": "CACHE_SENTINEL", "companies": "CACHE_SENTINEL"}
    admin_system._search_index_cache["value"] = sentinel
    admin_system._search_index_cache["ts"] = _time.monotonic()

    t0 = _time.perf_counter()
    second = await client.get("/api/admin/health/deep", headers=superadmin_headers)
    cached_ms = round((_time.perf_counter() - t0) * 1000, 1)
    assert second.status_code == 200
    assert second.json()["search_indexes"] == sentinel

    perf_metrics.append({"name": "api_health_deep_first_uncached", "ms": first_ms})
    perf_metrics.append({"name": "api_health_deep_cached", "ms": cached_ms})

    # refresh=true bypasses the poisoned cache and overwrites it
    third = await client.get("/api/admin/health/deep?refresh=true", headers=superadmin_headers)
    assert third.status_code == 200
    assert third.json()["search_indexes"] != sentinel
    assert admin_system._search_index_cache["value"] != sentinel


async def test_health_deep_cache_expires_after_ttl(client, superadmin_headers):
    import time as _time
    from routes import admin_system

    sentinel = {"prospects": "STALE_SENTINEL", "companies": "STALE_SENTINEL"}
    admin_system._search_index_cache["value"] = sentinel
    # pretend the entry is older than the TTL
    admin_system._search_index_cache["ts"] = _time.monotonic() - admin_system._SEARCH_INDEX_CACHE_TTL - 1

    resp = await client.get("/api/admin/health/deep", headers=superadmin_headers)
    assert resp.status_code == 200
    assert resp.json()["search_indexes"] != sentinel  # expired -> refetched
    assert admin_system._search_index_cache["value"] != sentinel


# ---------------------------------------------------------------------------
# Iteration 2: stuck-campaigns supporting indexes exist after create_indexes()
# ---------------------------------------------------------------------------

@pytest.mark.slow
async def test_stuck_campaigns_indexes_created(perf_metrics):
    """database.create_indexes() must create one index per $or branch of the
    stuck-campaigns query (discovery_status/message_gen_status + updated_at).

    Marked slow (runs the full create_indexes() against Atlas, ~5 s) — excluded
    from the default suite; run with `pytest -m slow`."""
    import time as _time
    t0 = _time.perf_counter()
    await database.create_indexes()
    perf_metrics.append({"name": "create_indexes_full_run_test_db",
                         "ms": round((_time.perf_counter() - t0) * 1000, 1)})

    info = await database.campaigns_collection.index_information()
    assert "discovery_status_1_updated_at_1" in info
    assert "message_gen_status_1_updated_at_1" in info
