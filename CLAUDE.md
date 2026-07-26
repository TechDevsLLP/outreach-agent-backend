# OutFlo Backend — FastAPI + MongoDB

B2B outreach automation backend. Full documentation set in `docs/` — this file is the working developer guide.

| Doc | Contents |
|---|---|
| `docs/ARCHITECTURE.md` | System overview, pipelines, scheduler jobs, tenancy model |
| `docs/API.md` | Full endpoint reference (~280 routes) with auth levels |
| `docs/DATABASE.md` | Every collection + index in `outflo_v3` |
| `docs/INTEGRATIONS.md` | Apify, GrowthToolkit, Unipile, email (Gmail/Zoho/SMTP), OAuth, AI models |

## Recent changes (July 2026 cleanup/rebuild)

- DB migrated to a new Atlas cluster, database **`outflo_v3`**: ~47k prospects / ~27k companies with clean canonical schema, embeddings preserved, vector indexes `prospects_vec`/`companies_vec` READY, 5 tenant accounts with all-string `account_id` keys.
- Apify reduced to **exactly 3 actors** (employee, company-details, post scraper). Apollo LEADS_FINDER, Apollo Lead Scraper, and the LinkedIn profile scraper are removed, along with `lead_service`, `prospect_service`, `linkedin_scraper_service`, `freshness_validator_service`.
- Geo lookup moved from Mongo to local SQLite (`data/geo_places.sqlite`, `services/geo_resolver.py`).
- Email/phone enrichment moved to **GrowthToolkit** (`services/growthtoolkit_service.py`); phone unlock is user-triggered only.
- Deprecated single-admin auth removed; superadmin = `SUPER_ADMIN_EMAIL` + `get_super_admin` + impersonation.
- New superadmin suite under `/api/admin` (5 route modules) with audit log, runtime flags, scheduler heartbeats, webhook log, quota overrides.
- Sender voice is Unipile-only (own posts fetched via Unipile, synthesized with Haiku).
- **Email sending overhauled (July 2026):** SendGrid removed entirely (send, SDK, inbound parse/event webhooks, `/api/sendgrid/*` routes). Gmail API, Zoho Mail API, and custom SMTP+IMAP are now first-class channels behind a common `EmailProvider` interface (`services/email_providers/`) + `services/email_delivery_service.py` facade — see `docs/INTEGRATIONS.md`. OAuth tokens and SMTP/IMAP passwords are encrypted at rest (`utils/crypto.py`, `ENCRYPTION_KEY`). Open-rate tracking removed — only replies and link clicks are tracked.

## Stack

- Python 3.12, FastAPI 0.128 + Uvicorn, Pydantic v2 (+ pydantic-settings)
- MongoDB Atlas via Motor (async); database name from `MONGODB_DATABASE` env (**`outflo_v3`** in production — the `config.py` code default is still the legacy `LeadAutomation_v2`, so always set the env var)
- APScheduler (13 background jobs), slowapi rate limiting
- AI: OpenRouter (Claude Haiku/Sonnet, Gemini Flash, Perplexity) + direct Gemini SDK (company sourcing, embeddings)
- Scraping: Apify (3 actors); contacts: GrowthToolkit; LinkedIn: Unipile; email: Gmail API / Zoho Mail API / custom SMTP+IMAP (Outlook OAuth kept wired but send is an unimplemented stub)

## Running

```bash
cd /Users/prasad/Documents/Projects/outflo/backend
venv/bin/uvicorn main:app --reload --port 8008        # web API (APP_ROLE=all also runs scheduler in-process)
venv/bin/python -m scheduler_worker                    # standalone scheduler process
```

Production runs two containers: `Dockerfile` (uvicorn on 8008, `/health` healthcheck, `APP_ROLE=web`) and `Dockerfile.scheduler` (`python -m scheduler_worker`). `APP_ROLE`: `all` (dev default) | `web` | `scheduler`.

## Directory layout

- `main.py` — app factory, 37 routers, CORS, request logging, lifespan (indexes, scheduler, resume interrupted work)
- `config.py` — all settings (pydantic-settings, reads `.env`)
- `database.py` — Motor client, all collection refs, `create_indexes()`
- `auth.py` — JWT (HS256, 24h), `get_current_user`, `get_account_context`, `get_super_admin`, impersonation tokens
- `rate_limit.py` — slowapi limiter (per-IP)
- `scheduler_worker.py` — standalone scheduler entrypoint
- `models/` — Pydantic models (one file per entity)
- `routes/` — 35 route modules, all under `/api/`
- `services/` — ~70 business-logic modules
- `utils/prompts.py` — all AI prompt templates; `utils/scoring.py` — rule-based scoring
- `data/geo_places.sqlite` — local GeoNames gazetteer (built by `scripts/build_geo_sqlite.py`)
- `scripts/` — one-off/maintenance scripts; `deploy/` — deployment assets
- `tests/` — pytest suite
- `logs/campaigns/` — per-campaign discovery logs (`DISCOVERY_LOG_DIR`)

## Environment variables (`config.py`; names only — never commit values)

| Var | Purpose |
|---|---|
| `MONGODB_URL`, `MONGODB_DATABASE` | Atlas connection; DB name (`outflo_v3`) |
| `JWT_SECRET_KEY`, `JWT_ALGORITHM`, `JWT_EXPIRY_MINUTES` | Auth token signing (HS256, 24h default) |
| `SUPER_ADMIN_EMAIL` | Single-email superadmin allowlist for `/api/admin/*` |
| `APIFY_API_KEY` | Apify client auth |
| `APIFY_COMPANY_SCRAPER_ID` | Company-details actor (default `UwSdACBp7ymaGUJjS`) |
| `APIFY_POST_SCRAPER_ACTOR_ID` | Post scraper actor (default `r4oNX7IHlW4RQAjKP`) |
| `GROWTHTOOLKIT_API_KEY`, `GROWTHTOOLKIT_BASE_URL` | GrowthToolkit email finder / phone unlock |
| `OPENROUTER_API_KEY`, `OPENROUTER_BASE_URL` | OpenRouter AI calls |
| `GEMINI_API_KEY` | Direct Gemini SDK (company sourcing, embeddings) |
| `ASSESSMENT_MODEL`, `OUTREACH_MODEL`, `CLAUDE_MODEL`, `MINI_ENRICHMENT_MODEL`, `PREFILTER_MODEL`, `FALLBACK_FREE_MODEL` | Model selection per task (see `docs/INTEGRATIONS.md`) |
| `UNIPILE_TOKEN`, `UNIPILE_BASE_URL`, `UNIPILE_WEBHOOK_SECRET` | Unipile LinkedIn API + webhook verification |
| `SENDER_EMAIL`, `SENDER_NAME`, `REPLY_TO_EMAIL` | Platform default sender identity (fallback label only — actual sending is per-account) |
| `ENCRYPTION_KEY` | Fernet key encrypting OAuth tokens + SMTP/IMAP passwords at rest (`utils/crypto.py`) |
| `GOOGLE_CLIENT_ID/SECRET/REDIRECT_URI` | Gmail + Calendar OAuth |
| `ZOHO_CLIENT_ID/SECRET/REDIRECT_URI` | Zoho Mail OAuth |
| `MICROSOFT_CLIENT_ID/SECRET/REDIRECT_URI` | Outlook OAuth (MSAL) — kept wired but send is an unimplemented stub |
| `APP_ENV`, `APP_ROLE`, `DEBUG`, `LOG_LEVEL` | Runtime mode (`APP_ROLE`: all/web/scheduler) |
| `FRONTEND_URL`, `CORS_ORIGINS` | CORS allowlist |
| `BACKEND_BASE_URL` | Public URL for click-tracking redirect links |
| `API_BASE_URL` | Public API URL (calendar webhook registration) |
| `GEO_SQLITE_PATH` | Override path to the geo gazetteer SQLite (defaults to `data/geo_places.sqlite`) |
| `DAILY_EMAIL_QUOTA`, `DAILY_LINKEDIN_CONNECTION_QUOTA`, `DAILY_LINKEDIN_INMAIL_QUOTA` | Default daily send quotas |
| `ENROLLED_TARGET_FIRST_CAMPAIGN`, `ENROLLED_TARGET_FLOOR`, `MAX_PROSPECTS_PER_COMPANY` | Discovery/enrollment tuning |
| `QUALITY_GATES_ENABLED`, `TITLE_GATE_ENABLED`, `PREFILTER_GATE_ENABLED`, `PREFILTER_*` | Quality gates + AI prefilter tuning |
| `INDUSTRY_CONCURRENCY_LIMIT`, `APIFY_ACTOR_CONCURRENCY_LIMIT`, `AI_CONCURRENCY_LIMIT`, `AI_ASSESSMENT_CONCURRENCY_LIMIT`, `ENRICHMENT_BATCH_SIZE` | Concurrency/batching knobs |
| `AUTO_ENRICH_*`, `AUTO_DISCOVER_CONTACTS_*`, `CONTACT_DISCOVERY_*`, `PRE_ENRICHMENT_*`, `SCHEDULE_DM_TRIAGE_ENABLED` | Pipeline feature gates/thresholds |
| `OUTREACH_OFFICE_HOURS_START/END`, `OUTREACH_SEND_JITTER_MINUTES` | Send-window shaping |
| `DISCOVERY_LOG_DIR`, `COST_TRACKING_ENABLED`, `ENRICHMENT_STARTUP_SWEEP_ENABLED` | Observability toggles |
| `DISCOVERY_MOCK_MODE` | Test-only: skip paid Apify/Gemini in discovery. NEVER true in production |

Note: `.env.example` predates the DB migration — it still shows `MONGODB_DATABASE=LeadAutomation_v2`. Trust `config.py` for the full list.

## The 3 Apify actors (only these — everything else removed)

| Actor | ID | Where |
|---|---|---|
| Employee scraper | `Vb6LZkh4EqRlR0Ka9` | hard-coded in `services/employee_scraper_service.py` |
| Company-details scraper | `UwSdACBp7ymaGUJjS` | `apify_company_scraper_id` |
| Post scraper (5 posts/profile) | `r4oNX7IHlW4RQAjKP` | `apify_post_scraper_actor_id` |

All runs go through `services/apify_service.track_apify_run()` → `apify_usage` collection with account/campaign cost tags.

## GrowthToolkit (email + phone)

`services/growthtoolkit_service.py` (facade: `email_finder_service.py`). Auth `x-api-key`; all responses HTTP 200 with `success`/`code` in body; typed errors for auth (401), credits (402/406/407/428), invalid input (400/417); 429 retried honoring `error_data.sec`. Client-side rate limits per scope (Action 2/s, List 1/s, Misc 1 per 2s); async task polling via `GET /tasks/status/{task_id}/`. Email finder runs inside discovery/enrichment; **phone unlock only via `POST /api/enrichment/prospects/{id}/unlock-phone`** (user-triggered, cached, never at enrollment). Usage → `growthtoolkit_usage` collection.

## Auth model

```python
account_ctx = Depends(get_account_context)   # {"user": ..., "account": ...}
account_id = account_ctx["account"]["_id"]   # string
```

- JWT HS256, 24h, `sub` = user `_id`. `get_current_user` → user doc; `get_account_context` adds `current_account_id` resolution + membership check + plan gating (suspended/expired trial → 402).
- **Store `account_id` as a string** on tenant-scoped docs — never ObjectId.
- Superadmin: `Depends(get_super_admin)` (email == `SUPER_ADMIN_EMAIL`); impersonation via `create_impersonation_token` (30 min, `impersonated_by` claim). Log every admin mutation with `services/admin_audit_service.log_admin_action()`.

## Campaign lifecycle (smart campaigns)

```
POST /api/campaigns/smart  → status: draft, background run_fast_discovery():
  Gemini sourcing → Haiku company scoring → bulk Apify employee scrape
  → Haiku employee scoring → recovery re-scrape → GrowthToolkit email finder
  → upsert pool + pre-enroll → Day-1 message generation
  → discovery_status=completed, status=awaiting_approval

POST /api/campaigns/{id}/approve-day/{n}   (first approval → status=active,
                                            triggers Day n+1 generation)

campaign_engine (APScheduler, 5 min): enrollments with status=active AND
next_action_at <= now → send via email_sender_service / unipile_service,
advance flow_engine state. Daily caps in daily_cap_service
(connect 20 / email 20 / inmail 5 / DM 20; superadmin overrides via
accounts.quota_overrides).
```

Interrupted discoveries/message-gen/enrichment runs are resumed or swept on boot (`main.py` lifespan).

## Testing

```bash
venv/bin/python -m pytest tests/
```

Suite layout: `tests/unit/` (canonicalizers, geo resolver, scoring, serialization), `tests/api/`, `tests/perf/`, `tests/smoke_real/`, with shared fixtures in `tests/conftest.py`. Real-integration smoke tests (`tests/smoke_real/`) are opt-in via a `RUN_REAL_SMOKE` env flag — default runs must stay mock-only and never spend Apify/Gemini/GrowthToolkit credits. Never set `DISCOVERY_MOCK_MODE=true` in production; per-campaign `discovery_mock_mode` is honored only when `APP_ENV != production`.

## Important patterns

- All services async; concurrency bounded with `asyncio.Semaphore` / token buckets.
- Background work via FastAPI `BackgroundTasks` / `asyncio.create_task` (no Celery).
- Bulk Mongo writes use `ordered=False` for per-item error isolation.
- `next_action_at` drives the campaign engine (index `(account_id, status, next_action_at)`).
- AI prompts live in `utils/prompts.py`; DB overrides via `system_prompts` (slug-keyed, TTL cache).
- Runtime feature flags: `services/system_settings_service.get_flag(name, env_default)` — DB override wins; toggle via `PATCH /api/admin/settings/flags`.
- Scheduler jobs must be wrapped with `_with_heartbeat()` so `/api/admin/jobs` sees them.
- Geo lookups: `services/geo_resolver.resolve()` (SQLite) — do not query a Mongo `geo_places` collection.
- Never log or persist secrets; `admin_audit_service` redacts credential-shaped params.
