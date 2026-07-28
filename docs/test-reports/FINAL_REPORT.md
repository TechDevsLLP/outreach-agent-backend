# OutFlo Backend — Test & Improve Loop, Final Report (3 iterations)

**Dates:** 2026-07-10 (all three iterations)
**End state:** 228 tests (221 in the default selection, all green across 3 consecutive runs at 16.8–19.1 s wall; +1 `slow`-marked index test, passing; +6 opt-in real-provider smoke tests, all passed in the one funded run). Zero writes ever made to production `outflo_v3`.

## Suite growth

| | Iteration 1 | Iteration 2 | Iteration 3 |
|---|---:|---:|---:|
| unit | 95 | 97 | 102 |
| api | 94 | 104 | 109 (+1 behind `slow`) |
| perf (read-only, real DB) | 8 | 10 | 10 |
| smoke_real | 1 placeholder | 1 placeholder | **6 real-provider tests** |
| **passing** | **197** | **211** | **221 default / 228 total** |
| full-suite wall time | 11.2 s | ~26 s | **16.8–19.1 s** |

(Iteration-2 growth to 26 s was new-test cost — 2k-overlay seeding, deliberate cold Atlas search-index probes, full index-build test. Iteration 3 clawed it back via a test-only bcrypt work factor and moving the ~5 s index build behind `-m slow`, without deleting any coverage.)

## Source bugs found and fixed (each demonstrated by a failing test first)

| # | Iter | Bug | Impact |
|---|---|---|---|
| 1 | 1 | `routes/auth.py` — `hash_password` never imported in password-reset confirm | every password-reset confirm 500'd; users could not reset passwords |
| 2 | 1 | `routes/prospects.py` PATCH — `tags` in both `$set` and `$setOnInsert` (Mongo path conflict) | every tags update 500'd |
| 3 | 1 | `services/geo_resolver.py` — gazetteer ran before exact-country check ("Us" is a French commune) | location "US" resolved to France; ISO-2 inputs mis-resolved |
| 4 | 1 | `services/icp_canonicalizer.py` — substring seniority match (`"cto"` inside "dire**cto**r") | Director ICPs silently broadened to c_suite |
| 5 | 1 | `utils/scoring.py` ×2 — empty industry string matched every industry key | least-qualified prospects got max industry points in two scorers |
| 6 | 2 | (contract, not crash) cross-tenant campaign access returned 403 vs prospects' 404 | existence leak; unified to 404 |
| 7 | 3 | **`GET /api/prospects` filter wart** — filters affected `total` but not the page slice | filtered lists returned unfiltered rows with a filtered count |
| 8 | 3 | `routes/prospects.py` — a later filter's `$or` clobbered an earlier one (`search`+`country`, `country` present with `seniority_level`) | combined filters silently dropped conditions |
| 9 | 3 | `services/prospect_search_service.py::ensure_prospect_state` TypeError on `db=None`, swallowed by callers' try/except | **enrichment pipeline never wrote ai_score/priority_tier to prospect_state** (`enrichment_pipeline.py:645` passes None) |

## The iteration-3 filter fix (headline change)

Filters now filter the page. Chosen design: **denormalized filter keys (`pk` subdoc) on `prospect_state`**, written by every state-doc write site (8 sites across routes/services), kept fresh for volatile fields by an authoritative end-of-run resync in the enrichment pipeline, backfillable via `scripts/backfill_prospect_state_filter_keys.py` (idempotent, dry-run default — **not** run against `outflo_v3`; near-empty post-migration).

Decision numbers (read-only benchmark on the real 47k pool, 2k synthetic overlay): a `$lookup`-then-`$match` filter pass costs **+~400 ms per filtered request and scales O(overlay)** (~4 s at 20k rows); the denormalized `pk` `$match` costs **~0 ms** and stays O(page). End-to-end at a 2k overlay: filtered list 69.2 ms vs 64.3 ms unfiltered — and now returns the correct rows. Full numbers in `iteration_3.md`.

## Optimization history (endpoint: before → after, from the three timing JSONs)

| Endpoint / operation | Iteration 1 | Iteration 2 | Iteration 3 |
|---|---|---|---|
| `GET /api/prospects` (2k overlay) | O(overlay) per page: fetch-all + `$in` count (~60–140 ms DB, linear growth) | single aggregation, O(page): 60–75 ms e2e | 64.3 ms e2e; **filtered 69.2 ms with correct semantics** (was ~300 ms for a wrong result) |
| `GET /api/prospects/stats` (2k, uncached) | fetch-all + 5×`$in` aggs | 123–147 ms e2e | 104–110 ms e2e |
| `GET /api/admin/health/deep` | ~863–1,038 ms every call | 5-min search-index cache: 821 ms cold / **37.7 ms cached** | 250–886 ms cold / **33.5 ms cached** |
| `GET /api/admin/pool/stats` (real pool) | 9 count scans, 601–771 ms sequential (~125 ms gathered warm) | one `$group` pass per collection: 322–344 ms, 9→2 scans | 331.6 ms uncached + **60 s cache** (`?refresh=true` bypass) |
| stuck-campaigns query | COLLSCAN | 2 supporting indexes (per `$or` branch) | verified via `slow`-marked index test |
| GrowthToolkit HTTP | new `AsyncClient` per call | shared lazy client + lifespan `aclose()` | — |
| `/health/deep` + `/jobs` heartbeats | duplicated query | duplicated query | shared `_read_heartbeats()` |
| auth tests (bcrypt) | login 370 ms, reset flow 844 ms | same | test-only rounds=4: **login 21 ms, reset 157 ms**; suite 26 s → ~17 s |

## Real-API smoke verification (one funded run, `RUN_REAL_SMOKE=1`)

| Provider | Call | Result | Latency |
|---|---|---|---|
| GrowthToolkit | `find_email` (real name + domain from pool) | clean not-found; usage doc verified | 2,761 ms |
| GrowthToolkit | `enrich_linkedin` (no unlocks) | person object (name+company fields); usage doc verified | 289 ms |
| OpenRouter | Haiku 4.5 tiny completion | exact echo returned | 1,856 ms |
| Gemini | single-text embedding | 770-byte int8 BSON Binary | 1,171 ms |
| Unipile | list accounts (read-only) | 2 accounts connected, shape ok | 1,208 ms |
| Apify | post-scraper `r4oNX7IHlW4RQAjKP`, 1 profile | run succeeded, 2 items, `apify_usage` doc w/ run_id | 13,391 ms |

Credits spent: **~2 GrowthToolkit credits, 1 minimal Apify run (~cents), 3 tiny Haiku calls (~$0.001), 1 Gemini embedding.** One test-side fix during the run: the service's `_is_refusal` heuristic (deliberately) rejects completions <15 chars, so the smoke prompt asks for a sentence. Usage docs were verified in `outflo_v3_test` (the harness DB) rather than the permitted real-DB append — strictly safer; production got zero writes. Hard rules held: no messages/emails/connections/InMails/unlocks of any kind.

## Live boot test (real `.env` → `outflo_v3`)

`APP_ROLE=web` uvicorn on :8010 — ready in **23.7 s** (index creation 1.9 s; remainder is app import), scheduler correctly skipped, resume sweeps no-op'd, `/health` 200 + `database: connected`, unauthenticated routes 401, clean shutdown. Only warnings: 2× FastAPI `Query(regex=)` deprecations in `routes/campaigns.py:1654-1655`.

## Remaining known items / recommendations

1. **Run the pk backfill at cutover** (`scripts/backfill_prospect_state_filter_keys.py --apply`) for any environment that has pre-July-2026 `prospect_state` rows; until then such rows are invisible to prospect-level list filters. (Real `outflo_v3` is near-empty — new write paths self-populate `pk`.)
2. **Re-run the perf pack after cutover** once real overlays exist (list/stats/filter numbers here use the test DB + synthetic 2k samples).
3. **Old-schema fallback paths** in prospects list/stats (account_id-on-prospects) — delete once cutover confirms no legacy tenants.
4. Known from July 5: **8 tenant-leak call sites in `unipile_service.py`** — untouched, out of scope for all three iterations.
5. `routes/campaigns.py:1654-1655` — `Query(regex=)` → `pattern` (FastAPI deprecation, 2-line fix).
6. pool/stats warm-latency alternative (partial indexes + gathered counts, ~125 ms) remains available if the 60 s cache proves insufficient for the admin dashboard.
7. `test_unlock_phone_*` tests carry ~1 s fixed cost (slowapi limiter windows) — candidate for a limiter reset fixture if suite time matters later.
8. The enrichment pipeline's end-of-run `pk` resync covers pipeline writes; if new services start mutating `prospects.email`/`enrichment_status` outside the pipeline, call `utils/prospect_filter_keys.sync_filter_keys` there too (documented in the module docstring).

## Artifacts

- `docs/test-reports/iteration_{1,2,3}.md` — per-iteration detail
- `docs/test-reports/timings_iteration_{1,2,3}.json` — machine-readable per-test timings + perf metrics
- `docs/test-reports/timings_iteration_3_smoke.json` — real-provider smoke run record
- Suites: `tests/{unit,api,perf,smoke_real}`; markers `unit/api/perf/smoke_real/slow`; default run excludes `smoke_real` + `slow`
