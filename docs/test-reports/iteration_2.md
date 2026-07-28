# Test Harness — Iteration 2 Report (optimize + verify)

**Date:** 2026-07-10
**Result:** 211 passed, 0 failed, 1 deselected (smoke_real placeholder) — verified across 3 consecutive full runs
**Wall time:** ~26 s (in-test 17.7 s) — up from 11.3 s in iteration 1 purely because of *new* tests (see "wall time" note)
**Machine-readable timings:** `docs/test-reports/timings_iteration_2.json` (iteration number now parameterized via `TEST_REPORT_ITERATION` env var in `tests/conftest.py`, default `2`; iteration-1 JSON left untouched)

## What was implemented

| # | Optimization | Files changed |
|---|---|---|
| 1 | Prospects list/stats overlay pattern → aggregations | `routes/prospects.py` |
| 2 | health/deep search-index cache (5 min TTL, `?refresh=true` bypass) | `routes/admin_system.py` |
| 3 | pool/stats count battery → one `$group` pass per collection | `routes/admin_pool.py` |
| 4 | GrowthToolkit shared `httpx.AsyncClient` (lazy init + `aclose()` on shutdown) | `services/growthtoolkit_service.py`, `main.py`, `tests/conftest.py` (mock fixture) |
| 5 | Stuck-campaigns supporting indexes | `database.py` |
| 6 | Cross-tenant campaign access 403 → 404 | `routes/campaigns.py`, `routes/campaign_schedules.py`, `routes/campaign_enrollments.py`, `tests/api/test_campaigns_api.py` |

Response shapes are unchanged everywhere; the only contract change is the documented 403→404 alignment.

### 1. `GET /api/prospects` + `/api/prospects/stats` (routes/prospects.py)

**Before:** every request fetched *all* prospect_state docs for the account, sorted in Python, ran `count_documents({"_id": {"$in": <all overlay ids>}})`, then `$in`-fetched the page — O(overlay size) per page view. Stats additionally ran five `$in` aggregations over all ids.

**After (list):** a single aggregation on `prospect_state`: `$match` account (+ status/min_score) → `$facet` { `state_total` count; page = `$sort` on `$ifNull(ai_score,0)` → `$skip/$limit` → `$lookup` prospects (indexed `_id` join, `_LIST_PROJECTION`) → `$unwind` } — **O(page) instead of O(overlay)**. `prospect_id` string/ObjectId handled via `$convert` (invalid → null → row dropped, same as the old `_oid()` guard).
- Legacy-parity notes (deliberate): prospect-level filters (search/industry/country/…) still affect `total` but not the pagination slice, exactly as before; that filtered `total` is computed server-side with a per-row `$lookup` branch **only when such filters are present**. `total` without filters now counts overlay rows directly (identical unless a state row dangles to a deleted prospect).
- Old-schema fallback (account_id on prospects) preserved verbatim, triggered by `state_total == 0` (same condition as the old empty-state-docs check).

**After (stats):** two concurrent queries replace "fetch everything into Python":
1. one `prospect_state` aggregation (`$facet`: total/avg/hot/warm/cold `$group` + by_status `$group`, with the same null/""→"new" and `None→0` score semantics),
2. an ids-only overlay fetch feeding **one** `prospects` aggregation with a 5-branch `$facet` (industry/country/seniority/enrichment/unique-companies) instead of five separate `$in` pipelines.

Measured variants at a 2,000-row overlay before choosing (test DB, 4 runs each):
- per-row `$lookup`+`$replaceRoot`+`$facet`: **282–345 ms**
- ids + single `$in`+`$facet` (chosen): **67–87 ms**
- old 5×`$in` aggregations + full state fetch: 63–163 ms

**Before → after (2k overlay):**

| Measurement | Iteration 1 | Iteration 2 |
|---|---|---|
| list: DB work per request | phase1 41–120 ms + count 19–20 ms + phase2 10 ms, **grows linearly with overlay** | full endpoint **60–75 ms** end-to-end (ASGI+auth+network incl.), page cost flat in overlay size |
| stats: first uncached hit | state fetch + 5×`$in` aggs (22–30 ms each) | full endpoint **123–147 ms** end-to-end |

(Iteration-1 numbers were raw DB timings without HTTP overhead, so the honest comparison is the complexity class: per-page work no longer scales with overlay size. The real `outflo_v3` `prospect_state` is still empty pre-cutover — re-measure with real overlays after migration.)

### 2. `GET /api/admin/health/deep` (routes/admin_system.py)

`list_search_indexes()` (two Atlas round-trips) now cached in-process for 5 minutes (`_search_index_cache`, module-level timestamped); `?refresh=true` bypasses and repopulates.

| | ms |
|---|---:|
| iteration 1 (every call) | ~863 |
| iteration 2 first/uncached (`api_health_deep_first_uncached`) | 820.7 |
| iteration 2 cached (`api_health_deep_cached`) | **37.7** |

### 3. `GET /api/admin/pool/stats` (routes/admin_pool.py)

All coverage numerators now come from **one `$group` aggregation pass per collection** (accumulators `_sum_if_set` / `_sum_if_not_null`, exact aggregation equivalents of the old `$exists+$nin` / `$exists+$ne` count queries); raw totals stay `estimated_document_count`; everything (2 aggs + 2 estimated counts + overlay `$group`) runs in one `asyncio.gather`. 11 server operations → 5, and 9 collection scans → 2.

Real `outflo_v3` (47,246 prospects / 26,694 companies):

| Measurement | ms |
|---|---:|
| old count battery, sequential (iter-1 perf test, still measured) | 582–654 |
| old count battery, **gathered** as the route actually ran it (warm, scratch bench) | ~120–130 (first call after connect: ~1,190) |
| new single-pass prospects `$group` | 245–255 |
| new single-pass companies `$group` | 91–95 |
| **new endpoint-equivalent total (parallel, perf test)** | **322–344** ✅ target <400 ms |

**Honest caveat:** iteration 1's "601–771 ms" was the *sequential* sum; the route gathered the counts, so its warm wall latency was already ~125 ms — the new version is ~2× slower in ideal warm-cache wall-clock but does 2 scans instead of 9 (≈2.4× less server work), is immune to concurrent-scan contention, and its cost stays flat as more coverage fields are added. Correctness of the accumulators is pinned by `test_perf_single_pass_counts_match_count_documents` (aggregation results == `count_documents` results on the real pool). If warm wall latency ever matters more than server load here, partial indexes + the old gathered counts are the alternative (logged for iteration 3).

### 4. GrowthToolkit shared HTTP client (services/growthtoolkit_service.py)

`_request()` no longer builds an `httpx.AsyncClient` per call (per-call TCP+TLS setup, zero connection reuse). Now: module-level `_client` with lazy `_get_client()`, plus `aclose()` wired into the `main.py` lifespan shutdown (after `shutdown_sse()`). The `mock_growthtoolkit_http` conftest fixture was updated to install its fake as `gts._client` (monkeypatch-restored), so all 15 existing unit tests run unchanged. No timing delta measurable in tests (HTTP is mocked); the win is per-call connection setup removed in production.

### 5. Stuck-campaigns indexes (database.py)

`get_stuck_campaigns` queries `$or: [discovery_status $in transitional, message_gen_status="running"]` ANDed with an `updated_at` range + ascending sort — previously a collection scan. Added one index per `$or` branch (the pattern MongoDB needs to satisfy a rooted `$or` without COLLSCAN): `(discovery_status, updated_at)` and `(message_gen_status, updated_at)`. Verified created on the test DB by `test_stuck_campaigns_indexes_created` (runs the real `database.create_indexes()`). Latency delta at today's campaign volume is negligible — this is a scaling guard.

### 6. 403 → 404 consistency

All three `_get_campaign_or_404` helpers (`campaigns.py`, `campaign_schedules.py`, `campaign_enrollments.py`) now return **404** for another tenant's campaign instead of 403, matching the prospects routes (no existence leak). The two tests that pinned 403 were updated and renamed (`test_get_campaign_cross_tenant_404`, `test_lifecycle_cross_tenant_not_found`). No other test or client contract referenced the 403.

## Suite counts

| Suite | Tests | Δ vs iter 1 |
|---|---:|---|
| tests/unit | 97 | +2 (shared-client lazy-init/reuse/aclose; single-client reuse across calls) |
| tests/api | 104 | +10 (7 overlay-scale, 2 health-cache, 1 index-creation) |
| tests/perf | 10 | +2 (single-pass pool stats + endpoint-equivalent; accumulator-vs-count correctness) |
| **Total** | **211 passed** (+ 1 deselected smoke_real) | +14 |

**Wall time note:** 11.3 s → ~26 s is entirely new-test cost, not regressions: `test_stuck_campaigns_indexes_created` runs full `create_indexes()` against Atlas (~5.1–5.8 s), the 2k-overlay fixture inserts/cleans 4,000 docs (~4 s), and the health-cache tests deliberately pay two extra cold `list_search_indexes` round-trips (~1.7 s). Pre-existing tests kept their iteration-1 timings (auth/bcrypt still dominates).

## New tests added (iteration-2 targets)

- `tests/api/test_prospects_overlay_scale.py` — 2,000-row prospect_state overlay seeded in the test DB (module fixture, self-cleaning): total/sort/pagination/min_score/status ground-truth checks, prospect-level filter (`has_email`, `industry_id`) affecting `total`, cross-account isolation, and full stats ground truth (tiers, avg, by_status/industry/country/enrichment, unique companies). Endpoint latencies recorded into `perf_metrics`. Seeding gotcha: prospects `email` index is unique+sparse, so "no email" docs must omit the key (explicit nulls collide).
- `tests/api/test_admin_api.py` — `test_health_deep_search_index_cache_and_refresh` (cold-cache timing, cache-sentinel proof of no live round-trip, `refresh=true` bypass overwrites poisoned cache), `test_health_deep_cache_expires_after_ttl`, `test_stuck_campaigns_indexes_created`.
- `tests/unit/test_growthtoolkit_service.py` — `test_shared_client_lazy_init_reuse_and_aclose`, `test_requests_reuse_single_shared_client`.
- `tests/perf/test_perf_readonly.py` — `test_perf_pool_stats_single_pass` (asserts endpoint-equivalent < 1.2 s hard ceiling; actual 322–344 ms recorded), `test_perf_single_pass_counts_match_count_documents`.

## Remaining candidates for iteration 3

1. **Filtered prospects list semantics**: the legacy quirk (prospect-level filters affect `total` but not the pagination slice) was preserved for contract parity. Fix properly (filter-then-paginate) — needs denormalized filter/sort keys on `prospect_state` or an Atlas Search index; the current filtered-`total` `$lookup` branch is O(overlay) per filtered request (~300 ms at 2k).
2. **pool/stats warm-latency option**: partial indexes on `{phone:1}`, `{title_vec:1}` etc. would make the old gathered-counts pattern (~125 ms warm) strictly better than the single pass; alternatively cache pool/stats for 60 s like prospects/stats (admin dashboard tolerance).
3. **health/deep**: heartbeats + estimated counts could share the same TTL cache; `/api/admin/jobs` duplicates the heartbeats query.
4. **Auth test cost**: bcrypt dominates the api suite (login/register ~200–860 ms each); a test-only reduced bcrypt work factor via Settings would cut ~2.5 s of wall time.
5. **`test_stuck_campaigns_indexes_created`** runs the full `create_indexes()` (~5 s) — scope it to the campaigns collection or mark it `slow` if suite wall time matters.
6. **Re-run the perf pack after cutover** once real `prospect_state` overlays exist on `outflo_v3` (still 0 docs; list/stats measured against the test DB and synthetic samples).
7. **Old-schema fallback paths** in prospects list/stats still carry the pre-migration query shapes; delete after cutover confirms no `account_id`-on-prospects tenants remain.
8. Known open item from July 5: 8 tenant-leak call sites in `unipile_service.py` (unrelated to this iteration's scope).

## Files created / modified

**Source:** `routes/prospects.py`, `routes/admin_system.py`, `routes/admin_pool.py`, `routes/campaigns.py`, `routes/campaign_schedules.py`, `routes/campaign_enrollments.py`, `services/growthtoolkit_service.py`, `database.py`, `main.py`.
**Tests:** `tests/conftest.py` (iteration env var + shared-client mock), `tests/api/test_prospects_overlay_scale.py` (new), `tests/api/test_admin_api.py`, `tests/api/test_campaigns_api.py` (403→404), `tests/unit/test_growthtoolkit_service.py`, `tests/perf/test_perf_readonly.py`.

**Run it:** `venv/bin/pytest` (writes `timings_iteration_2.json`; set `TEST_REPORT_ITERATION=3` next iteration).
