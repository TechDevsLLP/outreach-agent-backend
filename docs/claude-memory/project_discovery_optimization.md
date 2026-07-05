---
name: project-discovery-optimization
description: "Optimization of curated discovery + enrichment pipeline for speed, cost, and enrichment quality (June 22-23 2026)"
metadata: 
  node_type: memory
  type: project
  originSessionId: 43ccbd72-ae0a-4983-92c2-496a995121f1
---

## What was done (June 22-23 2026)

Full optimization of the curated discovery + enrichment pipeline across 9 files.

**Why:** Previous run took ~88 min, cost $15.64 for 169 prospects, had no enrichment reaching messages, and $0 enrichment costs in reports.

**How to apply:** All changes are in production service files + `scripts/onboard_and_campaign_prithvi.py`. New campaign-doc tuning fields gate the new behavior so prod campaigns are unchanged.

### Bug fixes (in shared services)
1. `linkedin_post_scraper_service.py:60` — `total_posts: None` → `posts_per_profile` (actor required int)
2. `config.py` — added `perplexity/sonar-pro` to `openrouter_price_map` ($3/$15 per M tokens)
3. `email_finder_service.py` — 3-attempt retry (4s/8s/16s backoff) on `find_emails_bulk`
4. `prospect_intelligence_service.py` — threaded `account_id/campaign_id/feature` tags through intel + pitch calls
5. `embedding_service.py` — same cost tag threading
6. `campaign_message_generator_service.py` — fixed intel-key mismatch: batch-loads `prospect_state.pitch`, merges `{**base, **pitch}` into `prospect_intelligence` key before prompt builder
7. `curated_discovery_service.py` — fixed `fallback_sc` bug (off-ICP employees now dropped instead of attributed to first company); also fixed `fallback_sc` → `_fallback_sc` in recovery path (line ~780)

### New behavior (campaign-doc gated)
- `company_sourcing_service.py` — `max_concurrency` param (default=1); parallel Gemini batches when >1
- `curated_discovery_service.py` — reads `discovery_scrape_depth`, `discovery_dropout_buffer`, `discovery_enrollment_cap`, `discovery_sourcing_concurrency`, `discovery_enable_company_research` from campaign doc; per-company news+competitor research gated by flag
- `scripts/onboard_and_campaign_prithvi.py` — sets: `discovery_scrape_depth=3`, `discovery_dropout_buffer=1.5`, `discovery_enrollment_cap=2`, `discovery_sourcing_concurrency=4`, `discovery_enable_company_research=True`; cost report extended with intel/pitch/message_generation/company_research USD breakdown; discovery loop wrapped in try/except to always print cost report even if Apify billing fails

### Verified working (June 22 run)
- Sourcing: 179 companies in ~40s (4× parallel batches)
- Post scraper: 49 posts from 39 profiles ✅
- Messages: 39/39 day-1, 17/17 day-2 ✅
- Company research: competitors + news for all enrolled companies ✅
- Email retry: 3 attempts with backoff then graceful fallback ✅
- 79 prospects enrolled across 51 companies before Apify monthly limit hit

### Still needed
- Raise Apify monthly hard limit at console.apify.com/billing (hit during iteration 2)
- Re-run to get full cost report with all enrichment line items
