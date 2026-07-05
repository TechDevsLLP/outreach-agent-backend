---
name: OutFlo Architecture Overview
description: Key architectural facts about the OutFlo project structure, stack, and patterns
type: project
originSessionId: 17255886-36f3-43a1-ab77-f9eefab38896
---
OutFlo is a B2B outreach automation platform with two directories:
- **Backend:** `/Users/prasad/Documents/Projects/outflo/backend/` — FastAPI + MongoDB
- **Frontend:** `/Users/prasad/Documents/Projects/outflo/frontend/` — Next.js 16

**Why:** Multi-tenant SaaS for AI-powered B2B prospecting and outreach automation.

**How to apply:** Backend is Python FastAPI; all API logic lives in `routes/` and `services/`. Frontend calls the backend via `lib/api/*.ts` Axios modules. There is NO Convex — `.convex/` was deleted (was a leftover dev cache, never implemented).

## Backend Stack
- FastAPI + Uvicorn (port 8008)
- MongoDB via Motor async driver (database: `LeadAutomation_v2`, 25 collections)
- OpenRouter AI: Claude Sonnet 4.5 (assessment, temp 0.2) + Gemini 2.5 Flash (outreach, temp 0.7)
- APScheduler: campaign engine runs every 5 min
- Apify: LinkedIn/company/lead scraping
- Unipile: LinkedIn automation (connections, InMails)
- SendGrid / Gmail / Outlook / SMTP: email sending

## Frontend Stack
- Next.js 16 App Router, TypeScript
- TanStack React Query v5 + Axios
- Tailwind CSS + shadcn/ui
- No Convex — all data from FastAPI at `NEXT_PUBLIC_API_URL=http://localhost:8008`

## Key Backend Files
- `main.py` — app entry, 27 routers, startup/shutdown hooks
- `config.py` — all env vars and settings
- `database.py` — Motor client, collection definitions, indexes
- `auth.py` — JWT (HS256), `get_current_user()`, `get_account_context()` dependency
- `models/` — 25 Pydantic model files
- `routes/` — 30+ route files (all `/api/` prefixed)
- `services/` — 39 business logic service files
- `utils/prompts.py` — all AI prompts
- `utils/scoring.py` — prospect scoring helpers

## Frontend Route Groups (no URL prefix from parens)
- `(auth)/` → /login, /signup
- `(onboarding)/` → /onboarding
- `(dashboard)/` → /overview, /campaigns, /campaigns/new, /campaigns/[id], /prospects, /inbox, etc.

## Auth Pattern (Backend)
```python
account_ctx = Depends(get_account_context)
account_id = account_ctx["account"]["_id"]
```

## Apify Actor IDs
- LEADS_FINDER: IoSHqwTR9YGhzccez
- LINKEDIN_CONTACTS: T1XDXWc1L92AfIJtd
- PROFILE_SCRAPER: 2SyF0bVxmgGr8IVCZ
- COMPANY_SCRAPER: UwSdACBp7ymaGUJjS

## Campaign Lifecycle
1. POST /api/campaigns → BackgroundTask: discover_and_enroll_prospects()
2. DB query (score ≥60, ICP match) → Apify if insufficient → mini-enrichment
3. Enroll top N → BackgroundTask: generate messages (5 concurrent, stored in enrollment.generated_messages)
4. User approves → channel assignment + timezone-aware scheduling → status=active
5. APScheduler every 5min: campaign_engine.py sends due enrollments

## Scoring
- ai_prospect_score = 60% AI fit_score + 40% rule-based
- priority_tier: hot(≥80), warm(60-79), cold(<60)
- next_action_at drives campaign engine (index: account_id, status, next_action_at)
