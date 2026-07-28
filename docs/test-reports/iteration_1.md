# Test Harness — Iteration 1 Report

**Date:** 2026-07-10
**Result:** 197 passed, 0 failed, 1 deselected (smoke_real placeholder)
**Wall time:** 11.2 s (in-test time 8.3 s)
**Machine-readable timings:** `docs/test-reports/timings_iteration_1.json`

## Suite counts

| Suite | Tests | Notes |
|---|---|---|
| `tests/unit/` | 95 | scoring, serialization, canonicalizers, geo resolver (real sqlite), system settings flag cache, audit sanitizer, GrowthToolkit client (mocked HTTP), email finder facade |
| `tests/api/` | 94 | auth, prospects, unlock-phone, campaigns, full superadmin suite — ASGI client against `main.app`, DB = `outflo_v3_test`, all paid providers mocked |
| `tests/perf/` | 8 | READ-ONLY measurements against the real `outflo_v3` (47,246 prospects / 26,694 companies) |
| `tests/smoke_real/` | 1 placeholder | deselected by default (`-m "not smoke_real"`, needs `RUN_REAL_SMOKE=1`) |

Harness details:
- `pytest.ini`: asyncio auto mode, **session-scoped event loop** (required — Motor caches its io_loop on first use), markers `unit/api/perf/smoke_real`.
- `tests/conftest.py` sets `MONGODB_DATABASE=outflo_v3_test`, `SUPER_ADMIN_EMAIL`, `APP_ROLE=web`, blank `SENDGRID_API_KEY` **before** the lru-cached `Settings` / `database` import, and hard-asserts the db name contains `_test` before any drop. All test collections are dropped at session start and end (verified empty after run).
- Timing plugin (`pytest_runtest_makereport` + `pytest_sessionfinish`) writes every test's name/outcome/ms plus all perf measurements to `timings_iteration_1.json` and prints a slowest-10 summary.
- Autouse guard fixture monkeypatches OpenRouter/Apify entry points to raise if any test accidentally reaches a paid provider.

## Top 15 slowest tests (full run)

| ms | test |
|---:|---|
| 862.8 | api/test_admin_api.py::test_health_deep |
| 856.1 | api/test_auth_api.py::test_password_reset_full_flow |
| 626.6 | perf/test_perf_readonly.py::test_perf_pool_stats_coverage_counts |
| 384.1 | api/test_admin_api.py::test_suspend_blocks_context_then_reactivate |
| 370.8 | api/test_auth_api.py::test_login_success_and_wrong_password |
| 268.3 | api/test_auth_api.py::test_register_creates_user_account_membership |
| 231.4 | api/test_auth_api.py::test_register_duplicate_email_409 |
| 215.0 | unit/test_geo_resolver.py::test_resolve_bulk_speed |
| 209.1 | api/test_auth_api.py::test_suspended_account_blocked_with_402 |
| 194.2 | api/test_admin_api.py::test_suppression_crud_cycle |
| 175.6 | api/test_admin_api.py::test_usage_apify_and_openrouter_and_summary_smoke |
| 159.8 | perf/test_perf_readonly.py::test_perf_prospects_list_query_pattern |
| 158.5 | api/test_campaigns_api.py::test_lifecycle_activate_pause_resume |
| 135.6 | api/test_admin_api.py::test_account_detail_shape |
| 120.1 | api/test_admin_api.py::test_force_pause_and_resume |

(Auth tests are dominated by bcrypt hashing — expected and correct; health_deep is dominated by Atlas `list_search_indexes` probing, see optimization candidates.)

## Perf measurements — real `outflo_v3` (read-only, 47.2k prospects / 26.7k companies)

| Measurement | ms | Comment |
|---|---:|---|
| pool_stats count: prospects location | 85–192 | `count_documents` w/ `$exists+$nin` |
| pool_stats count: prospects industry | 85–93 | |
| pool_stats count: prospects embeddings | 48–52 | |
| pool_stats count: prospects email | 107–117 | |
| pool_stats count: prospects phone | 58–66 | |
| pool_stats count: prospects company_linked | 95–101 | |
| pool_stats count: companies location / industry / embeddings | 27–75 | |
| **pool_stats count battery total (9 counts, sequential)** | **601–771** | route runs them in `asyncio.gather`, so effective latency ≈ slowest single count (~190 ms) |
| prospect_state group-by-account | 7–9 | collection empty post-migration |
| prospects list phase 1 (2000-id overlay fetch — synthetic, see note) | 41–120 | |
| prospects list phase 2 ($in page of 50) | 10 | fast — `_id` $in |
| prospects list `count_documents` with $in of 2000 ids | 19–20 | grows linearly with overlay size |
| stats by-industry / by-country / unique-companies (2000-id $in aggregations) | 22–30 each | |
| usage rollups (growthtoolkit / apify / openrouter, `$dateToString` group) | 5–9 | collections near-empty |
| admin accounts list (3× `$lookup` pipeline, 5 accounts) | 9–16 | fine at current tenant count |
| **$vectorSearch prospects_vec top-5 (stored int8 vector)** | **20–147** | index READY; first query ~147 ms, warm ~20 ms |

Note: `prospect_state` has **0 documents** on `outflo_v3` (fresh migration — overlays not yet built). The prospects-list / stats patterns were measured against a synthetic 2000-id overlay sampled read-only from `prospects`; re-measure with real overlays after cutover.

## Bugs found and fixed (source changes)

All fixes are minimal/surgical; each was demonstrated by a failing test first.

1. **`routes/auth.py:14` — `hash_password` not imported → password-reset confirm 500s.**
   `password_reset_confirm` (line ~325) calls `hash_password(body.new_password)` but the module never imported it → `NameError` on every confirm. Any user completing a password reset got a 500 and could not change their password. Fix: added `hash_password` to the `from auth import (...)` list. Test: `test_password_reset_full_flow`.

2. **`routes/prospects.py` PATCH `/{prospect_id}` — MongoDB path conflict on `tags` → 500.**
   The prospect_state upsert put `tags` in `$set` (when updating tags) while `$setOnInsert` unconditionally contained `"tags": []`. MongoDB statically rejects the same path in both operators (`ConflictingUpdateOperators`), so **every tags update 500'd**. Fix: `$setOnInsert` now excludes keys present in the update. Test: `test_patch_prospect_tags_upsert`.

3. **`services/geo_resolver.py::resolve` — raw location "US" resolved to France.**
   The SQLite gazetteer lookup ran before the exact-country check; "Us" is a French commune, so ISO-2/short country strings could resolve to the wrong country entirely (US → FR). Fix: exact ISO-2 / full-country-name resolution (`_resolve_country_only`) now runs before the gazetteer query. Test: `test_resolve_iso2_code`.

4. **`services/icp_canonicalizer.py::_normalize_seniority` — "Director" canonicalized as c_suite.**
   Naive substring matching meant the pattern `"cto"` fired inside "dire**cto**r" (likewise other short acronyms), so any Director ICP also produced `c_suite` and over-broadened searches. Fix: patterns now match on word boundaries with plural tolerance (`(?<![a-z])pat s?(?![a-z])`). Tests: `test_seniority_free_text_to_tokens`, `test_seniority_unknown_input_yields_empty`.

5. **`utils/scoring.py` — empty industry earned full industry points (2 functions).**
   `if key in industry or industry in key` with `industry == ""` is always true (`""` is a substring of every key), so prospects with a missing industry got the max 15 industry points in `score_prospect_v2` **and** `score_company_fit_rule_based`, inflating scores/tiers for the least-qualified records. Fix: guard the loop with `if industry:`. Test: `test_v2_empty_lead_scores_low_but_not_zero`.

### Test-side corrections (not code bugs)
- Cross-tenant campaign access intentionally returns **403** (`_get_campaign_or_404`), not 404 like the prospects routes; tests updated to match the documented behavior. (Worth a consistency discussion — prospects return 404 to avoid existence leaks, campaigns return 403.)

## Flaky / skipped

- No flaky tests observed across runs (3 full runs, stable).
- 2 perf tests originally skipped because `prospect_state` is empty on `outflo_v3`; converted to a synthetic read-only fallback (documented above) so they always measure.
- 2 geo tests auto-skip if `data/geo_places.sqlite` is missing (present on this machine, so they ran).
- Constraint to know: `/api/auth/login` is rate-limited 10/min/IP (slowapi, in-process). The auth suite currently performs 6 logins; keep it under 10 or reset the limiter if it grows.
- `tests/smoke_real/` is a skip-unless-`RUN_REAL_SMOKE=1` placeholder for the iteration-2+ real-provider pack.

## Optimization candidates for iteration 2 (prioritized)

1. **`GET /api/prospects` overlay pattern is O(overlay size) per request** (`routes/prospects.py`): fetches *all* prospect_state docs for the account, sorts in Python, then runs `count_documents({"_id": {"$in": <all ids>}})` on every page view. With the target 10–50k overlays per account this will degrade linearly (already ~20 ms per 2k ids for the count alone, ~40–120 ms for phase 1). Replace with a single `$lookup`/`$sort` aggregation on prospect_state using the existing `(account_id, priority_tier, ai_score)` index, or denormalize sort keys.
2. **`GET /api/prospects/stats` same pattern** — loads every state doc + four `$in` aggregations; the 60 s in-process cache hides it but the first hit per account will grow with overlay size. Fold into one `$facet` aggregation.
3. **`GET /api/admin/health/deep` (863 ms)** — dominated by two Atlas `list_search_indexes` round-trips (already parallel) plus 9 estimated counts. Cache search-index status for ~60 s or make it an opt-in query param.
4. **`GET /api/admin/pool/stats` count battery** — 9 `count_documents` full-ish scans (~190 ms worst single, ~600–770 ms sequential). Options: single `$group`/`$facet` pass, or maintain coverage counters incrementally. Partial indexes on `{phone: 1}`, `{title_vec: 1}` (exists-style) would help if counts stay.
5. **`growthtoolkit_service._request` creates a new `httpx.AsyncClient` per call** — no connection reuse under load and per-call TLS setup; move to a module-level client (also simplifies mocking).
6. **Missing composite index** for the campaigns "stuck" query (`discovery_status`/`message_gen_status` + `updated_at`) — full scan today; trivial at current volume, flag for later.
7. **Consistency**: unify cross-tenant not-found semantics (prospects → 404, campaigns → 403).
8. Re-run the perf pack after prospect_state overlays are built post-cutover to get real per-account numbers (synthetic 2000-id overlay used this iteration).

## Files created / modified

**Created:** `pytest.ini`, `tests/conftest.py`, `tests/unit/{test_scoring,test_serialization,test_canonicalizers,test_geo_resolver,test_system_settings_and_audit,test_growthtoolkit_service}.py`, `tests/api/{test_auth_api,test_prospects_api,test_unlock_phone_api,test_campaigns_api,test_admin_api}.py`, `tests/perf/test_perf_readonly.py`, `tests/smoke_real/test_smoke_real_placeholder.py`, `docs/test-reports/timings_iteration_1.json`, this report.

**Modified (source fixes, listed above):** `routes/auth.py`, `routes/prospects.py`, `services/geo_resolver.py`, `services/icp_canonicalizer.py`, `utils/scoring.py`.
**Modified (deps):** `requirements.txt` (+ `pytest>=9.0`, `pytest-asyncio>=1.0`; httpx already present).

**Run it:** `venv/bin/pytest` (default excludes smoke_real; perf pack included — read-only GET/aggregate against `outflo_v3`).
