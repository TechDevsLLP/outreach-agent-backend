---
name: project-new-db-wiring
description: "Implementation of the shared-pool + prospect_state overlay wiring (June 21 2026): ICP canonicalization, DB-first discovery, teammate conflict bucket, hybrid intelligence, analytics fix."
metadata: 
  node_type: memory
  type: project
  originSessionId: 43ccbd72-ae0a-4983-92c2-496a995121f1
---

# New DB Architecture Wiring (June 21 2026)

The shared-pool rearchitecture (built in June 2026) was **built but not wired in** until this session. All 8 plan sections implemented.

## What was wired

### 1. `services/icp_canonicalizer.py` (NEW)
- `canonicalize_icp(icp: dict) -> dict` — composes `expand_icp_to_industry_ids`, `geo_resolver.resolve`, `embed_one` (RETRIEVAL_QUERY, int8)
- Returns `{industry_ids, country_codes, seniorities, employee_bands, title_query_vec}`
- Reads BOTH `target_industries`/`industries`/`icp_industries` (absorbs field-name split)
- `canonicalize_icp_and_persist(icp, doc_id, collection)` convenience wrapper

### 2. ICP canonicalized at save + persisted
- `routes/onboarding_wizard.py` Stage 3: `asyncio.create_task(_canonicalize_and_save_company_profile_icp(...))`
- `routes/campaigns.py` smart create: `asyncio.create_task(_canonicalize_campaign_icp(campaign_id))`
- `services/onboarding_scrape_service.py` synthetic campaign: same
- Canonical fields added to `models/company_profile.py` and `models/campaign.py`

### 3. `run_fast_discovery` now DB-first
- Phase ⓪ added BEFORE Gemini loop: builds exclusion set → `search_prospects()` from pool → rule-score → base intel if missing → pitch → `_pre_enroll`
- If pool fills target → returns early (no Apify call)
- Apify scrapes only the GAP (`prospect_target - _db_enrolled_count`)
- Campaign doc gets `discovery_prospects_from_db` / `discovery_prospects_from_apify` counters

### 4. Reuse / used_by lifecycle
- `prospect_search_service.build_exclusion_set`: cooldown now uses `completed_at` (not `enrolled_at`); per-user scope
- `prospect_search_service.update_used_by_status`: accepts optional `completed_at` param
- `enrollment_state_machine.transition`: terminal statuses (`completed`, `opted_out`, `meeting_booked`, `replied`) now write `completed_at`
- `webhook_service`: email + LinkedIn reply handlers write `completed_at`
- `campaign_engine`: 5 completion paths call `update_used_by_status(..., completed_at=now)`
- `finalize_channel_plan`: after bulk_write, gathers `update_used_by_status(new_status="active")` for all assigned enrollments
- **Teammate conflict**: `_pre_enroll_prospects` queries `prospect_state.used_by` for same-account, different-user entries → sets enrollment `status="pending_teammate_review"` with `teammate_conflict` metadata
- `campaign_launch_service.approve_day` + `run_approve_and_launch`: both exclude `pending_teammate_review` from launch
- New endpoints: `GET /campaigns/{id}/teammate-review` + `POST /campaigns/{id}/teammate-review/{eid}/approve`

### 5. Hybrid intelligence
- `prospect_intelligence_service.generate_base_intelligence_batch(prospects)`: no account_profile → deterministic, stored as `prospects.prospect_intelligence_base`
- `prospect_intelligence_service.generate_pitch_batch(prospects_with_base, account_profile)`: cheap pitch call → stored as `prospect_state.pitch`
- `store_base_intelligence()` / `store_pitch_for_account()` new storage functions
- `routes/prospects.py`: reads `prospect_intelligence_base` from doc + `pitch` from state, merges into `prospect_intelligence` for backwards compat
- Deprecated shims kept for old callers

### 6. Message gen reads intelligence
- `utils/prompts.py::build_campaign_outreach_prompt`: new `intelligence: dict = None` param → adds `## Prospect Intelligence` section
- `campaign_message_generator_service.py`: loads `prospect_state.pitch` per enrollment, merges with `prospect_intelligence_base`, passes as `intelligence=` to prompt

### 7. Engine fix
- `campaign_engine._execute_smart_enrollment`: status guard reads from `prospect_state` (not prospect doc); falls back to prospect doc if overlay absent

### 8. Analytics restored + readers fixed
- `campaign_daily_stats` auto-recreates (existing 6 increment sites + nightly cron still write to it)
- `routes/analytics.py`: overview / enrichment-roi / score-distribution / recent-activity now query `prospect_state_collection` by `account_id` instead of `prospects` by `account_id`
- `routes/campaigns.py enrolled-prospects`: `$lookup prospect_state` added → `ai_score`/`priority_tier` from overlay; sort uses `state_data.ai_score`

## Key dead code removed (next cleanup pass)
- `_discover_and_enroll_prospects_legacy` + 6 `_query_*` functions in `campaign_prospect_finder_service.py` — confirmed zero callers, safe to delete later

## Verification steps
See plan file `/Users/prasad/.claude/plans/you-are-a-expert-vectorized-phoenix.md` verification section.
