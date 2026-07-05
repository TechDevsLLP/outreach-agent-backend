# OutFlo Project Memory Index

| File | Description |
|------|-------------|
| `project_architecture.md` | Full stack: FastAPI+MongoDB backend (`/backend/`), Next.js 16 frontend (`/frontend/`). Auth pattern, campaign lifecycle, Apify actor IDs, scoring tiers. No Convex. |
| `project_enrichment.md` | Enrichment pipeline: 5-phase flow in `backend/services/enrichment_pipeline.py`, prompts in `backend/utils/prompts.py`, AI models, mini-enrichment for smart campaigns. |
| `project_linkedin.md` | LinkedIn/Unipile integration: schema tables, action functions, webhook handlers. API gotchas (X-API-KEY header, provider_id resolution, InMail via /chats). |
| `feedback_agents.md` | User preference: launch parallel frontend + backend subagents for large features to split work without file collisions. |
| `feedback_sonar_token_limit_bug.md` | OBSOLETE: Sonar Pro replaced by Gemini in company_sourcing_service.py. |
| `feedback_curated_discovery_bugs.md` | 6 bugs fixed in curated discovery pipeline (prefilter KeyError, enrollment status mismatch, _enroll_prospects skipping "scoring", projection missing fields, industry synonyms, seniority inference). |
| `project_curated_discovery_design.md` | Curated discovery flow: Gemini sourcing → employee scraping → deterministic scoring → channel planning. Updated June 15 2026: target numbers (100 companies, 2/company, 200 enrolled), new actor IDs, onboarding scrape trigger, deep enrichment pipeline (post scraper + Gemini prospect_intelligence), post-onboarding SSE UX, frontend changes. |
| `project_linkedin_post_scraper.md` | LinkedIn post scraper actor `r4oNX7IHlW4RQAjKP` (5 posts/profile) and `prospect_intelligence` generation via Gemini 2.5 Flash (batched 5/call). Schema, services, storage on prospect sub-document, ProspectIntelCard frontend component. |
| `error_log.md` | Running log of build/runtime errors fixed — cause, fix, and rule for avoiding recurrence. |
| `project_onboarding_prospect.md` | Onboarding prospect-preview gate (June 8 2026): new service/routes/test harness. 9/9 tests pass in mock mode. Gemini credits depleted — needs recharge for full flow. |
| `project_new_db_wiring.md` | Full wiring of shared-pool + prospect_state overlay (June 21 2026): icp_canonicalizer, DB-first discovery, teammate-conflict bucket, hybrid intelligence, analytics fix. All 8 plan sections. |
| `project_discovery_optimization.md` | Discovery + enrichment optimization (June 22-23 2026): 9 files changed, 7 bugs fixed, parallel sourcing, campaign-doc tuning gates, per-company research, cost tag threading. Needs Apify monthly limit raised to run clean. |
| `project_qa_e2e_harness.md` | E2E QA harness (`run_full_flow.py`): full onboarding→campaign flow, checkpoint/resume, file logs. 4 bugs found+fixed in smoke run. Delivery run (200co/400pr) in progress July 3 2026. |
