---
name: project-curated-discovery-design
description: "Curated discovery flow — DB-first pool query then Apify gap-fill; ICP canonicalization; hybrid intelligence storage; teammate-conflict bucket. (updated June 21 2026)"
metadata: 
  node_type: memory
  type: project
  originSessionId: 01730ece-b13d-4c73-8293-b04d49943eb2
---

# June 16 2026 Refactor — Single Fast-Discovery Flow

**What changed:** Collapsed TWO flows (database "Quick search" + curated "Targeted search") to ONE. Fixed ~7-prospect enrollment bug. Switched to fully deterministic scoring. Added company LinkedIn persistence. Fixed onboarding double-path problem.

## Scoring (deterministic, `utils/scoring.py:score_prospect_for_campaign`)
Weights (total 100, title+company = 65 dominant):
- Title/headline match: **35** (was 25)
- Industry match: **18** + Company size: **12** = **30 company fit** (was 25)
- Has email (bonus): **15** (was 10)
- Seniority: **10** (was 25)
- Geo/country: **5** (unchanged)
- Has LinkedIn: **5** (was 10)

## Funnel constants (`curated_discovery_service.py`)
- `_QUALITY_COMPANY_TARGET = 120` (was 100)
- `_EMPLOYEE_SCORE_THRESHOLD = 48` (was 60 — lowered because email is bonus not gate)
- `_SCORING_DROPOUT_BUFFER = 2.5` (was 2.2)
- `_SCRAPE_DEPTH = 8` — employees scraped per company (was per_company=2-3)
- `_PER_COMPANY_ENROLLMENT_CAP = 3` — max enrolled per company (post-scoring)

## Target enrollment
- `config.enrolled_target_first_campaign = 135` (was 200)
- `config.enrolled_target_floor = 105`
- 3 days: 20 connections + 20 emails + 5 InMails per day = 45/day × 3 = 135

## Key new functions
- `finalize_channel_plan(campaign_id, account_id)` — sender auto-pick + plan_channel_assignments + persist + day_totals. Called at discovery-end AND launch-time.
- `replan_and_launch(campaign_id, account_id)` — called at onboarding launch (stage 5, after accounts connected). Resets skipped_no_channel, re-plans, triggers Day-1 enrichment.

## Onboarding consolidation
- Stage 3 lock-industry → `start_onboarding_scrape` → `run_fast_discovery` (background)
- `onboarding_campaign_id` persisted to `onboarding_sessions` collection early in `_run_and_track`
- Stage 5/launch → `POST /launch-first-campaign` → calls `replan_and_launch` (accounts now connected)
- Frontend calls `approve-day/1` after launch
- SSE `/scrape-progress` deprecated → use `GET /scrape-status/{session_id}` for polling
- `launch_onboarding_first_campaign` deleted (was source of ~7-prospect Day-1 email-pin bug)
- `source_preview_prospects` kept but filters to email+full-detail prospects only

## Company LinkedIn persistence
- Added in `_run_deep_enrichment_then_messages` Day-1 branch
- Scrapes ~50-80 enrolled company LinkedIn pages (actor `UwSdACBp7ymaGUJjS`)
- Saves to `companies_collection` + `company_linkedin_data` on prospects

## Prospect model new fields (`models/prospect.py`)
- `prospect_intelligence: Optional[dict] = None`
- `posts: list[dict] = Field(default_factory=list)`
Both persisted via bulk write in enrichment cohort.

## Frontend
- New campaign wizard + CampaignConfigCard: no Quick/Targeted toggle, always curated
- `discovery_mode` type narrowed to `"curated"` in `lib/api/campaigns.ts`
- `onboardingApi.getScrapeStatus(sessionId)` added to poll new endpoint
- `StepProspectPreview`: animated progress ring + live count replaces thinking dialog
- `StepLaunch`: done-condition `enrolled >= 50` → `>= 105`
- `ProspectIntelCard`: redesigned — best_hook hero, pitch/why-need-us two-col, pain signals, competitors, voice+style, dont-pitch. Added `why_they_need_us` + `engagement_style`.
- Prospect detail page: score bar visualization + LinkedIn posts feed

---



# Curated Discovery — Fast Pipeline (rewrote June 3, 2026; updated June 15, 2026)

## Architecture

Single BackgroundTask `run_fast_discovery(campaign_id, account_id)` in `services/curated_discovery_service.py`.

**No mid-flow approval gate** (old `awaiting_company_approval` state dropped). Only gate is the final Launch.

### Pipeline steps (~60–120s)

1. **Gemini source** (up to 3 iterations, max `target×2` companies) via `source_companies()` in `services/company_sourcing_service.py`
2. **Haiku batch score companies** (`_score_companies_with_llm`). Keep ≥ 50. Falls back to rule-based if Haiku fails.
3. **Persist sourced_companies** to MongoDB
4. **ONE bulk Apify call** (`bulk_scrape_employees_for_companies` in `services/employee_scraper_service.py`):
   - Actor `Vb6LZkh4EqRlR0Ka9` (harvestapi)
   - `companyBatchMode: "one_by_one"`, `maxItemsPerCompany: 5`, `Short ($4 per 1k)` mode
   - Single run for all companies (vs old per-company semaphore — 5× faster)
5. **Haiku batch score employees** (`_score_employees_with_llm`). Keep ≥ 60.
6. **Unconditional recovery**: re-scrape 0-employee companies with broadened seniority+function IDs (1 retry only)
7. **Bulk email finder** (`find_emails_for_linkedin_urls`) — always runs (Short mode returns no emails)
8. **Upsert prospects** (`_upsert_curated_prospect`) — global unique index on linkedin/email, NOT per-account
9. **Pre-enroll** (`_pre_enroll_prospects`)
10. **Day-1 message gen** (`_generate_day1_messages`, parallel with semaphore=5, fail-soft per enrollment)
11. Status → `completed`, campaign status → `awaiting_approval`

### Day 2–7 messages
Generated on Launch click (in existing `approve-and-launch` flow). NOT during discovery.

## Key constants
- `_MAX_GEMINI_ITERATIONS = 3`
- `_COMPANY_SCORE_THRESHOLD = 50`
- `_EMPLOYEE_SCORE_THRESHOLD = 60`
- `_MAX_ITEMS_PER_COMPANY = 5`
- `_PROFILE_SCRAPER_MODE = "Short ($4 per 1k)"`

## Broadening for recovery
- `_broaden_seniority_ids`: adds adjacent tiers from `_SENIORITY_ORDERING = ["120","220","210","300","310","320"]`
- `_broaden_function_ids`: adds 1 adjacent function per `_FUNCTION_ADJACENCY` dict

## Retry wrapper
`_with_retries(fn, retries=3, backoffs=(1,4,16), fail_sentinel=None)` — set `fail_sentinel={}` to degrade gracefully (email finder). Raises on all retries exhausted when no sentinel.

## Routes changed (June 3, 2026)
- `POST /api/campaigns/smart` → triggers `run_fast_discovery`
- `POST /{id}/discover-prospects` → triggers `run_fast_discovery`
- `POST /{id}/sourced-companies/approve` → returns 410 Gone (deprecated)
- `PATCH /{id}/sourced-companies/{co_id}` → no longer guards `awaiting_company_approval`

## Prompts (in utils/prompts.py)
- `COMPANY_BATCH_SCORE_SYSTEM_PROMPT` + `build_company_batch_score_prompt(companies, icp_prompt)`
- `EMPLOYEE_BATCH_SCORE_SYSTEM_PROMPT` + `build_employee_batch_score_prompt(employees, icp_prompt, sender_context)`

## OLD pipeline (superseded)
Two-entrypoint design: `start_curated_discovery` + `continue_after_company_approval`. Used per-company Apify runs (semaphore=6), 25 employees/company, Full mode. Took 4–8 min. Deleted.

**Why redesigned:** Per-company semaphore at concurrency=6 with 50+ companies was the bottleneck. Single bulk call reduces to 30–60s wall-clock.

---

# Updated Curated Discovery Design (June 15, 2026)

## Target Numbers

| Constant | Value | Notes |
|----------|-------|-------|
| `companies_to_source` | 100 | Gemini batches until 100 quality hits, Haiku score ≥ 50 |
| `max_prospects_per_company` | 2 | Top 2 by Haiku employee score per company |
| `enrolled_target` | 200 | |
| `day_structure` | [45, 45, 45, 45, 20] | 5-day campaign |
| `daily_caps.email` | 20 | |
| `daily_caps.connection` | 20 | |
| `daily_caps.inmail` | 5 | |
| `prefilter_threshold` | 0.25 | Was 0.4 |

## Actor IDs (updated)

| Actor | ID | Notes |
|-------|----|-------|
| Employee scraper | `Vb6LZkh4EqRlR0Ka9` | Unchanged |
| LinkedIn post scraper | `r4oNX7IHlW4RQAjKP` | 5 posts per profile, new |
| Bulk email finder | `ddgw2oGFaH645BFAq` | Replaces `TthkVR0ZjJt8gbtRy` |

Channel routing: email-verified prospects → all 3 channels (email + connection + InMail); no email → LinkedIn only.

## Onboarding Scrape Trigger

- Stage 3 ICP step: cap at 3 industries; dropdown asks "Which industry for your first campaign?" → stored as `locked_industry`
- On Stage 3 save: fire `start_onboarding_scrape()` background job immediately
- New service: `backend/services/onboarding_scrape_service.py`
- New collection: `onboarding_scrape_jobs` (fields: `account_id`, `session_id`, `status`, `prospects_found`, `day1_ready`)
- SSE endpoint: `GET /api/onboarding/scrape-progress/{session_id}`

## Post-Onboarding UX

- Replace loading dialog with full-page `/onboarding/building` route
- SSE streams prospect cards animating in (name, title, company, email badge)
- Auto-advances when Day 1 (45 prospects) enriched + messages ready
- "Adjust filters" button → re-scores existing pool first, then re-scrapes deficit

## Deep Enrichment Pipeline (Day-1 cohort, runs before message gen)

**Step 1:** Bulk LinkedIn post scrape — actor `r4oNX7IHlW4RQAjKP`, 5 posts, all 45 URLs batched in one call.

**Step 2:** Gemini 2.5 Flash batched 5 prospects/call — generates `prospect_intelligence`:
```
{
  writing_voice, top_topics, pain_signals, best_hook,
  pitch_angle, why_they_need_us, competitors, dont_pitch, engagement_style
}
```
Stored as `prospect.prospect_intelligence` sub-document. Competitor finding folded into same Gemini call (grounded search).

**Step 3:** Message gen prompt updated to use `prospect_intelligence`:
- Email: opens with `best_hook`, mirrors `writing_voice`, targets `pain_signals[0]`
- Connection note: matches tone exactly
- InMail: references `pain_signal`, names competitors

Days 2–5: enrichment runs in background during Day 1's 24h send window.

## Frontend Changes (June 15, 2026)

- **Stage 3**: industry cap 3 + `locked_industry` dropdown
- **`/onboarding/building`**: SSE progress page with animated prospect cards
- **Intel Card** component in campaign prospect view (collapsible sections)
- **Inline message edit** in campaign approval flow (textarea + save + regenerate + char count)
- **Campaign approval**: "Approve Day 1" button approves first 45 prospects
