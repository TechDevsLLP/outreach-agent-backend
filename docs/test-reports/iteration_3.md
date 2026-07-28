# Test Harness — Iteration 3 Report (filter fix + smoke pack + boot)

**Date:** 2026-07-10
**Result:** 221 passed, 0 failed, 7 deselected (6 smoke_real + 1 slow) — verified across **3 consecutive full runs** (19.1 s / 17.1 s / 16.8 s pytest wall). The slow-marked test and the 6 real-provider smoke tests also all pass when selected explicitly.
**Machine-readable timings:** `timings_iteration_3.json` (full suite) + `timings_iteration_3_smoke.json` (real-provider smoke run).

## 1. Filter/pagination fix — the real API bug (`GET /api/prospects`)

**Bug:** prospect-level filters (`search`, `industry`/`industry_id`, `country`, `seniority_level`, `enrichment_status`, `has_email`, `has_linkedin`) affected `total` but **not the returned page** — a legacy quirk deliberately preserved in iteration 2. Fixed properly: filters now filter the page slice too, and `total`/`total_pages` describe the filtered set.

### Approach chosen: denormalized filter keys (`prospect_state.pk`)

Both candidate designs were measured **read-only on the real pool** (47,246 prospects, `outflo_v3`) with a synthetic 2,000-row overlay injected via a db-level `$documents` aggregation (no writes):

| Variant (2k overlay, page 50) | measured | isolated filter cost |
|---|---|---|
| no-filter baseline (small `$documents` payload + page `$lookup`) | 82–103 ms | — |
| A. `$lookup`-then-`$match`, filter+projection in sub-pipeline | 478–582 ms | **+~400 ms** |
| A2. `$lookup` ids-only filter pass, page `$lookup` after `$limit` | 418–720 ms | +~330–620 ms |
| A3. lookup with 4-field regex search filter | 491–524 ms | +~410 ms |
| no-filter baseline (big payload incl. pk subdocs) | 265–274 ms | — |
| B. denormalized `pk` `$match` before sort/skip/limit | 243–251 ms | **≈ 0 ms** |
| B2. denormalized 4-field regex search | 264–382 ms | ≈ 0 ms |

The A-variants cost O(overlay) indexed point-lookups per filtered request (~0.2 ms each): ~+400 ms at 2k, extrapolating to ~4 s at the target 20k overlays. The B baseline is inflated only by shipping 2k `$documents` over the wire — a benchmark artifact that doesn't exist in production (the docs live server-side). **Denormalization wins decisively and is what shipped.**

End-to-end confirmation (test DB, seeded 2k overlay, ASGI+auth included):

| | iteration 2 | iteration 3 |
|---|---|---|
| unfiltered list | 60–75 ms | **64.3 ms** |
| filtered list (`has_email`) | ~300 ms `$lookup` branch for `total` only, **page slice unfiltered (wrong)** | **69.2 ms, page correctly filtered** |

### Implementation

- **`utils/prospect_filter_keys.py` (new):** `build_filter_keys(prospect)` builds the `pk` subdoc (full_name, email, company_name, job_title, company_industry_id, country_code, country, seniority, seniority_level, enrichment_status, linkedin — exactly the fields the list filters touch); `PK_PROJECTION`; `fetch_filter_keys(pid)`; `sync_filter_keys(ids, fields)` (targeted fan-out to all tenants' state rows); `resync_filter_keys_from_db(ids)` (authoritative re-read + bulk refresh).
- **Every prospect_state write site now sets/refreshes `pk`** (all found via grep for prospect_state mutations): `routes/prospects.py` PATCH upsert, `services/curated_discovery_service.py` overlay-ensure (from the post-upsert prospect doc), `services/prospect_search_service.py` `ensure_prospect_state` + `push_used_by`, `services/campaign_prospect_finder_service.py` `_pre_enroll` + `_enroll_prospects` bulk ops, `services/prospect_intelligence_service.py` pitch writes, `services/enrichment_pipeline.py` ai-assessed overlay write.
- **Volatile-field staleness handled:** the enrichment pipeline mutates `prospects.email` / `enrichment_status` / `company_industry_id` across ~10 per-phase writes — instrumenting each is fragile, so the pipeline does **one authoritative `resync_filter_keys_from_db()` pass at end-of-run** over every touched prospect (all tenants), plus targeted sync in `_update_prospect_status` (the failure-path helper).
- **Route (`routes/prospects.py::list_prospects`):** filters translate to `pk.*` clauses ANDed into the state `$match`; `$facet` = `state_total` (pre-filter, still the old-schema fallback trigger — unchanged semantics), `filtered_total`, and the filtered→sorted→paginated→`$lookup`ed page. Response shape unchanged.
- **Bonus fix (in passing):** the legacy filter builder let a later `$or` clobber an earlier one — `search` + `country` (or `country` + `seniority_level`) silently dropped the first filter's conditions. Clauses now AND correctly (pinned by `test_scale_list_combined_filters_and_search`).
- **Backfill:** `scripts/backfill_prospect_state_filter_keys.py` — idempotent, dry-run by default, batched, reports dangling state rows. **NOT run against `outflo_v3`** (prospect_state is near-empty post-migration; new write paths set `pk` themselves). Run it at cutover for any environment with pre-existing overlay rows.
- **Docs:** `docs/API.md` prospects section updated (only that section touched).

### Second real bug found while wiring the write sites

**`services/prospect_search_service.py::ensure_prospect_state` crashed on `db=None`** (`None["prospect_state"]` → TypeError), and `services/enrichment_pipeline.py:645` calls it with `None` inside a broad try/except — so the TypeError was silently swallowed and **the enrichment pipeline never persisted ai_score/priority_tier to prospect_state at all** (the follow-up `update_one` is non-upsert and was skipped with the rest of the try block). Fixed (None → default `database.db`), pinned by `test_ensure_prospect_state_accepts_none_db_and_sets_pk`.

## 2. Cheap optimizations

| Optimization | Before → after |
|---|---|
| `GET /api/admin/pool/stats`: 60 s in-process cache + `?refresh=true` (same pattern as health/deep) | 331–347 ms per call → ~0 ms cached; endpoint-equivalent uncached unchanged (331.6 ms this run) |
| `/health/deep` + `/jobs`: duplicated heartbeat query factored into `_read_heartbeats()` | code dedup only (same 2 round-trips; caching skipped — heartbeats are live status) |
| Test-only bcrypt work factor (conftest patches `bcrypt.gensalt` to rounds=4 before app import; hash format/code path identical, `checkpw` reads rounds from the hash) | login test 370→21 ms, password-reset flow 844→157 ms, register 264→76 ms |
| `test_stuck_campaigns_indexes_created` marked `slow` (full Atlas `create_indexes()` ~4.3–5.8 s), excluded from default addopts, runs via `pytest -m slow` (verified passing) | suite wall ~26 s (it-2) → **16.8–19.1 s** |

## 3. Real-API smoke pack (`tests/smoke_real/`, opt-in `RUN_REAL_SMOKE=1`) — RUN ONCE for real

All 6 tests skip cleanly without `RUN_REAL_SMOKE=1` (verified) and passed against the real providers:

| Provider | Test | Result | Latency | Cost |
|---|---|---|---|---|
| GrowthToolkit | `find_email` (real person from pool: name + `lu.athlon.com`) | pass — clean not-found (None), usage doc verified | 2,761 ms | ≤1 credit (service logs 0 credits on not-found) |
| GrowthToolkit | `enrich_linkedin` (`unlock_emails=0, unlock_phone=0`, real profile) | pass — person object w/ name+company fields, usage doc verified | 289 ms | 1 credit |
| OpenRouter | tiny Haiku completion (`claude-haiku-4-5`, max_tokens 32) | pass (see note) | 1,856 ms | ~$0.001 (3 tiny calls total) |
| Gemini | single-text embedding via `embedding_service.embed_texts` | pass — 770-byte int8 BSON Binary | 1,171 ms | negligible |
| Unipile | `get_accounts` (read-only) | pass — **2 accounts connected**, id+type shape verified | 1,208 ms | free |
| Apify | post-scraper `r4oNX7IHlW4RQAjKP`, 1 profile, 2 posts | pass — **2 post items returned**, `apify_usage` doc with run_id verified | 13,391 ms | 1 minimal run (~cents) |

Notes:
- **OpenRouter first attempt "failed" by design:** the model answered `pong` exactly as asked, but `openrouter_service._is_refusal` deliberately rejects replies <15 chars (all production prompts expect JSON/paragraphs). Test-side fix (ask for a full sentence); the service heuristic was intentionally left untouched. `timings_iteration_3_smoke.json` carries the passing rerun (noted in the file).
- **Usage-doc destination deviation (stricter than allowed):** the parent brief permitted the two append-only usage docs to land in real `outflo_v3`; the harness pins `database.db` to `outflo_v3_test`, so `growthtoolkit_usage`/`apify_usage` docs were written there instead, verified in-test (tagged `account_id="smoke_test"`), and dropped with the session. **Zero writes of any kind were made to `outflo_v3`** — it was only read (candidate sampling, same pattern as the perf suite).
- Absolute rules held by construction: no emails, no connection requests/messages/InMails, no phone/email unlocks, nothing outward-facing.

## 4. Live boot test (real `.env` → `outflo_v3`)

`APP_ROLE=web ENRICHMENT_STARTUP_SWEEP_ENABLED=false venv/bin/uvicorn main:app --port 8010` (sweep disabled as an extra write-guard; `app_role=web` skips APScheduler per `main.py:159`):

- **Time to ready: 23.7 s** process-start → `/health` 200 (of which `create_indexes()` against real Atlas: **1.9 s**, idempotent; the rest is Python import of the 37-router app graph).
- Startup log: `Scheduler not started (app_role=web)` ✓, `No interrupted schedules to resume` ✓, no stalled-discovery resumes, **no errors**.
- Probes: `/health` → `{"status":"ok","database":"connected"}`; unauthenticated `/api/prospects` → **401**; unauthenticated `/api/admin/health/deep` → **401**.
- Clean shutdown (SIGTERM): SSE + GrowthToolkit client closed, `Application shutdown complete`.
- Only warnings: 2× `FastAPIDeprecationWarning` — `Query(regex=...)` → `pattern` at `routes/campaigns.py:1654-1655` (pre-existing, cosmetic).

## Suite counts

| Suite | Tests | Δ vs iter 2 |
|---|---:|---|
| tests/unit | 102 | +5 (prospect_filter_keys builders/sync/resync, ensure_prospect_state None-db regression) |
| tests/api | 109 | +5 (page-filter semantics ×3 small-scale, filtered-pagination ground truth + combined-filters at 2k scale, pool/stats cache; index test moved behind `slow`) |
| tests/perf | 10 | — |
| tests/smoke_real | 6 | +6 (placeholder replaced by the real pack) |
| **Default selection** | **221 passed** (+1 slow +6 smoke deselected) | +10 in default run; 228 total |

## Files created / modified

**Source:** `routes/prospects.py`, `routes/admin_pool.py`, `routes/admin_system.py`, `services/curated_discovery_service.py`, `services/prospect_search_service.py`, `services/prospect_intelligence_service.py`, `services/campaign_prospect_finder_service.py`, `services/enrichment_pipeline.py`.
**New:** `utils/prospect_filter_keys.py`, `scripts/backfill_prospect_state_filter_keys.py`, `tests/unit/test_prospect_filter_keys.py`, `tests/smoke_real/test_smoke_real_providers.py`.
**Tests/config:** `tests/conftest.py` (pk in seeds, bcrypt rounds=4, default iteration 3), `tests/api/test_prospects_api.py`, `tests/api/test_prospects_overlay_scale.py`, `tests/api/test_admin_api.py`, `pytest.ini` (slow marker), `docs/API.md` (prospects section only). Removed: `tests/smoke_real/test_smoke_real_placeholder.py`.

**Run it:** `venv/bin/pytest` (default; excludes smoke_real+slow) · `venv/bin/pytest -m slow` · `RUN_REAL_SMOKE=1 venv/bin/pytest tests/smoke_real -m smoke_real` (spends credits).
