---
name: project-onboarding-prospect
description: "New onboarding prospect-preview gate: post-wizard sourcing of 5 companies+prospects, confirm/reroll, first campaign launch. Built June 8 2026."
metadata: 
  node_type: memory
  type: project
  originSessionId: 08ab4c24-115d-4db1-bdbd-4105e1b102c3
---

# Onboarding Prospect Preview Gate — June 8, 2026

## What was built

New service + 3 routes that bridge the 6-stage onboarding wizard to the first campaign launch via a "confirm your ideal prospects" gate.

## Files created / changed

**New:**
- `backend/services/onboarding_prospect_service.py` — 3 entry points:
  - `build_icp_prompt_from_profile(profile: dict) -> str` — converts company_profiles doc → free-text ICP prompt for Gemini
  - `source_preview_prospects(profile, *, count=5, exclude_company_names, ...) -> list[dict]` — calls source_companies (Gemini) + bulk_scrape_employees (Apify, max_items_per_company=1) + find_emails_for_linkedin_urls. Returns prospect dicts with `_sourced_company` internal key.
  - `launch_onboarding_first_campaign(account_id, user_id, profile, confirmed_prospects, *, target_total=50) -> str` — creates campaign doc, upserts 5 confirmed prospects, pre-enrolls, pins to Day 1 (bulk-writes send_day=1 + channel=email), awaits _generate_day1_messages, sets status=awaiting_approval, fires background top-up via run_fast_discovery.

**Edited:**
- `backend/routes/onboarding_wizard.py` — 3 new POST routes:
  - `/api/onboarding/prospect-preview` — sources + stores to session
  - `/api/onboarding/prospect-preview/reroll` — re-sources excluding prior companies
  - `/api/onboarding/launch-first-campaign` — reads session, calls launch service

**New test harness:**
- `backend/scripts/test_onboarding_to_campaign.py` — 10-step async test; `--mock-companies` flag bypasses Gemini (uses hardcoded SaaS company LI URLs) to test Apify + campaign pipeline when Gemini credits depleted.

## Key design decision: curated send_day bug

`_pre_enroll_prospects` (in campaign_prospect_finder_service.py) leaves `smart_campaign_send_day=None`. The standard curated `run_fast_discovery` never sets it. `approve_day(campaign, 1)` requires `send_day==1`. 

**Fix in onboarding flow only:** After `_pre_enroll_prospects`, we bulk-write `send_day=1, channel=email` for the 5 confirmed. The pre-existing bug in normal curated campaigns (run_fast_discovery) is not yet fixed.

## Test results (2026-06-08, mock-companies mode)

9/9 PASS. Tested with TechDevs (techdevs.in) ICP:
- Apify scraped 3/5 companies (Cal.com, Attio, Supabase — Linear and Loops had bad slugs)
- Email finder found 1 real email
- Campaign launched, Day-1 messages generated (all 3 channel types), approved, status=active
- next_action_at set to tomorrow (2026-06-09), nothing sent

## Test results (2026-06-08, full Gemini run — VERIFIED)

10/10 PASS. Real Gemini sourcing with TechDevs ICP:
- Gemini sourced 26 companies, 19 valid LinkedIn URLs, Apify scraped 4/5 (Anduril Industries bad slug — graceful skip)
- Email finder: 1/4 emails found (victor.cayupil@agilchile.com)
- Re-roll: 4 new companies (ActiveCampaign, Accurx, Aircall, ada), zero overlap with excluded set
- Campaign launched in 8s, Day-1 messages generated for 4/4 enrollments
- approve_day(1): campaign status=active, 4 scheduled, channels={'email': 4}, nothing sent
- Top-up discovery fired in background (run_fast_discovery)

Backend + API test harness goal is **COMPLETE**.

## Frontend rebuild (2026-06-08)

Full immersive onboarding flow built at `/onboarding`. One route, one state machine.

**New files:**
- `components/onboarding/OnboardingExperience.tsx` — top-level: hydration, OAuth-return, AnimatePresence step router
- `components/onboarding/OnboardingShell.tsx` — premium canvas: gradient blobs, progress dots, header
- `components/onboarding/AgentMessage.tsx` — chat bubble with avatar, typing dots, motion reveal
- `components/onboarding/onboarding-reducer.ts` — full state machine (step enum, slices, actions, HYDRATE)
- `components/onboarding/OnboardingProvider.tsx` — React context + useReducer
- `components/onboarding/onboarding-copy.ts` — all scripted agent lines
- `components/onboarding/StepCompany.tsx` — URL→scrape→poll→review/manual
- `components/onboarding/StepSenderVoice.tsx` — name/role/LI→voice profile/manual
- `components/onboarding/StepICP.tsx` — TagInput + chip toggles
- `components/onboarding/StepOffer.tsx` — CTA + reorderable value props
- `components/onboarding/StepConnect.tsx` — Gmail OAuth skippable
- `components/onboarding/StepRefine.tsx` — real LLM chat, captured panel
- `components/onboarding/StepProspectPreview.tsx` — prospect cards + reroll
- `components/onboarding/StepLaunch.tsx` — launch + celebration + discovery progress bar

**Backend:** Added `GET /api/onboarding/session` resume endpoint (returns profile + stage + prospect_preview).

**Brand:** Plus Jakarta Sans via next/font/google in `app/(onboarding)/layout.tsx`, CSS vars `--ob-*` scoped to onboarding.

**Deleted:** old stage-1..6 route pages, success page, dead components (StageProgressBar, CompanyAnalysisPreview, etc.)

**Kept (still used):** `onboarding-flow.tsx` (settings page re-analysis), `onboarding-checklist.tsx` (overview page). Legacy sub-components (`step-company-basics`, `step-analyzing`, `step-review`) restored for settings compatibility.

**Build:** `npm run build` passes clean (also fixed 2 pre-existing TS errors in admin pages).

## What's next
1. Manual walkthrough at localhost:3000/onboarding to verify the full 8-step flow
2. Fix the pre-existing curated discovery bug (run_fast_discovery needs plan_channel_assignments call)
