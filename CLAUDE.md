# OutFlo Backend — FastAPI + MongoDB

## Stack
- Python FastAPI + Uvicorn (ASGI)
- MongoDB via Motor (async driver), database: `LeadAutomation_v2`
- OpenRouter AI (Claude Haiku 4.5 for assessment, Gemini 2.5 Flash for campaign outreach)
- APScheduler for background jobs (campaign engine runs every 5 min)
- Apify for scraping (LinkedIn profiles, companies, leads, email finder)
- Unipile for LinkedIn automation (connections, InMails, messages)
- SendGrid / Gmail OAuth / Outlook MSAL / SMTP for email sending

## Running the Backend
```bash
cd /Users/prasad/Documents/Projects/outflo/backend
uvicorn main:app --reload --port 8008
```

## Key File Locations
- `main.py` — FastAPI app, 27 routers registered, startup/shutdown hooks (indexes, scheduler, resume schedules)
- `config.py` — All env vars (MongoDB URL, API keys, AI models, quotas, OAuth credentials)
- `database.py` — Motor client, 25 collections defined, all indexes created on startup
- `auth.py` — JWT auth (HS256, 24h), `get_current_user()`, `get_account_context()` dependency
- `models/` — 25 Pydantic model files (one per entity)
- `routes/` — 30+ API endpoint files, all prefixed `/api/`
- `services/` — 39 business logic service files
- `utils/prompts.py` — ALL AI system prompts and user prompt builders
- `utils/scoring.py` — Prospect scoring helper functions

## Auth Pattern
Every protected route uses:
```python
account_ctx = Depends(get_account_context)
account_id = account_ctx["account"]["_id"]  # ObjectId string
```

## Core MongoDB Collections
| Collection | Purpose |
|---|---|
| `prospects` | Main entity: score, enrichment data, outreach history |
| `campaigns` | Campaign definitions + smart campaign lifecycle fields |
| `campaign_enrollments` | Prospect-in-campaign progress + generated messages |
| `campaign_messages` | Sent message logs |
| `campaign_daily_schedules` | Daily execution schedule per campaign |
| `campaign_schedule_items` | Individual scheduled send actions |
| `campaign_daily_stats` | Per-day campaign metrics |
| `accounts` | Multi-tenant orgs (slug, plan, quotas) |
| `account_members` | User→account membership with roles |
| `users` | Auth (email, bcrypt password hash, current_account_id) |
| `company_profiles` | Per-account ICP definition, sender context, scoring weights |
| `conversations` | Unified email + LinkedIn inbox threads |
| `email_accounts` | Connected senders (SendGrid/Gmail/Outlook/SMTP) |
| `linkedin_accounts` | Connected via Unipile (profile info, daily quotas) |
| `system_prompts` | DB-backed AI prompt overrides (slug-keyed, 60s TTL cache) |
| `industries` | Per-account prospect sources |
| `search_runs` | Apify scraping job tracking |
| `enrichment_runs` | Enrichment pipeline execution logs |

## Campaign Lifecycle (Smart Campaigns)
```
POST /api/campaigns/smart
  → campaign doc created (status: draft)
  → BackgroundTask: run_fast_discovery()  [curated_discovery_service.py]
      Phase 1: Gemini sourcing → company list matching ICP
      Phase 2: Apify employee scraping → prospects per company
      Phase 3: Deterministic scoring (utils/scoring.py) → enroll top N
      Phase 4: finalize_channel_plan() → day/channel assignment
      Phase 5: generate messages (claude-haiku-4-5, batch of 3, 4 concurrent)
          cold_email (subject_a, subject_b, body)
          linkedin_connection (note ≤280 chars)
          linkedin_inmail (subject + body)
          → stored in enrollment.generated_messages
      → campaign.status → awaiting_approval

POST /api/campaigns/{id}/approve-day/{day_n}
  → approves a single send-day (Day 1, then Day 2, etc.)
  → triggers Day N+1 message generation immediately after approval
  → first approve-day transitions campaign.status → active
  (Legacy alias: POST /api/campaigns/{id}/approve-and-launch → approve day 1)

APScheduler every 5 min: campaign_engine.py
  → finds enrollments where status=active AND next_action_at <= now
  → sends via email_sender_service or unipile_service
  → records outcome, advances step
```

## Enrichment Pipeline (enrichment_pipeline.py)
```
Phase 0:   Setup — fetch prospects, validate LinkedIn URLs
Phase 1:   LinkedIn profile scraping (Apify PROFILE_SCRAPER)
Phase 2:   Company scraping (Apify COMPANY_SCRAPER) + deduplication
Phase 2.5: Competitor research (parallel with Phase 3)
Phase 3:   AI assessment (Haiku 4.5, batch of 3, temp 0.2) → ai_prospect_score 0-100
Phase 3.5: Contact discovery — find better contacts at high-fit companies
Phase 5:   Finalize — mark completed, set enriched_by

Commented-out (re-enable by uncommenting):
  Phase 0.5: Rule-based triage — decision maker detection
  Phase 4:   Outreach message generation — moved to campaign flow (campaign_message_generator_service)
  Phase 5.0: Auto employee discovery on high-scoring prospects
  Phase 5.1: Auto-init followup sequences (campaign-specific; lazy-initialized by outreach_executor_service)
```

## AI Models & Calls
All AI calls go through `services/openrouter_service.py`:
- Assessment model: `anthropic/claude-haiku-4-5` (temp 0.2, precise scoring; settings key: `mini_enrichment_model` for batch, `assessment_model` for single-call fallback)
- Message gen primary: `anthropic/claude-haiku-4-5` (fast, cheap, high rate limits)
- Message gen fallback: `anthropic/claude-sonnet-4-5` (structural-error fallback only — not a cost concern in normal operation)
- Company sourcing: `google/gemini-2.5-flash` via `services/company_sourcing_service.py` (ICP → company list)
- 3-retry exponential backoff; fallback to `nvidia/nemotron-3-nano-30b-a3b:free` on 402

Prompt templates in `utils/prompts.py`:
- `ASSESSMENT_SYSTEM_PROMPT` + `build_assessment_user_prompt()` → prospect fit scoring
- `CAMPAIGN_OUTREACH_SYSTEM_PROMPT` + `build_campaign_outreach_prompt()` → message generation
- `industry_param_generator.py` → natural language → Apify params (Claude call)

## Apify Actor IDs
- LEADS_FINDER: `IoSHqwTR9YGhzccez`
- LINKEDIN_CONTACTS: `T1XDXWc1L92AfIJtd`
- PROFILE_SCRAPER: `LpVuK3Zozwuipa5bp` (harvestapi/linkedin-profile-scraper)
- COMPANY_SCRAPER: `UwSdACBp7ymaGUJjS`

## Daily Quotas (per sender account)
- Email: 25/day
- LinkedIn connections: 20/day
- LinkedIn InMails: 5/day
- LinkedIn messages (follow-up DMs): 20/day

## Key Services
| Service | Role |
|---|---|
| `campaign_prospect_finder_service.py` | Smart campaign discovery (DB → Apify → enroll) |
| `campaign_message_generator_service.py` | Personalized message generation per enrollment |
| `campaign_engine.py` | Execute due enrollment steps every 5 min |
| `enrichment_pipeline.py` | Full 5-phase prospect enrichment |
| `ai_assessment_service.py` | Hybrid AI (60%) + rule-based (40%) scoring |
| `outreach_executor_service.py` | Daily schedule management + timezone-aware timing |
| `unipile_service.py` | LinkedIn connections, InMails, messages via Unipile API |
| `email_sender_service.py` | Multi-provider email dispatch |
| `openrouter_service.py` | OpenRouter AI API client with retry logic |
| `apify_service.py` | Apify scraping orchestration |
| `conversation_service.py` | Unified email + LinkedIn inbox |

## Important Patterns
- All services are async (`async def`); use `asyncio.Semaphore` for concurrency limits
- Background tasks via FastAPI `BackgroundTasks` (not Celery/RQ)
- Bulk MongoDB writes use `ordered=False` for error isolation per item
- `next_action_at` in enrollments drives campaign engine (index: `account_id, status, next_action_at`)
- System prompts can be overridden via DB (`system_prompts` collection, slug-keyed, 60s TTL)
- Prospect scoring: `ai_prospect_score` = 60% AI fit_score + 40% rule-based; priority_tier: hot(≥80), warm(60-79), cold(<60)
