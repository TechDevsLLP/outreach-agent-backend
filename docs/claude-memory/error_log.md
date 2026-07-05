---
name: error-log
description: "Running log of build/runtime errors fixed in the OutFlo project, with cause and solution."
metadata: 
  node_type: memory
  type: project
  originSessionId: 153f791e-732d-4738-92fa-33faf5ddf0c9
---

# OutFlo Error Fix Log

---

## 2026-06-08 — Gemini API `429 RESOURCE_EXHAUSTED` (prepaid credits depleted)

**File:** `services/company_sourcing_service.py` (model: `gemini-3.1-flash-lite`)

**Error:**
```
429 RESOURCE_EXHAUSTED: Your prepayment credits are depleted.
Go to https://ai.studio/projects to manage your project and billing.
```

**Cause:** The Google AI Studio key in `.env` (GEMINI_API_KEY) has exhausted its prepaid balance. Affects ALL Gemini models — not model-specific.

**Fix:** Top up credits at https://ai.studio/projects → Billing. The key itself is valid; no code change needed.

**Workaround for testing:** Run `python3 scripts/test_onboarding_to_campaign.py --mock-companies` to bypass Gemini sourcing and test Apify + campaign launch + approval pipeline with hardcoded SaaS company URLs.

---

## 2026-06-08 — curated `_pre_enroll_prospects` leaves `smart_campaign_send_day=None`

**File:** `services/curated_discovery_service.py`

**Error (runtime, not exception):** `approve_day(campaign, 1)` raises `ValueError: No enrollments with generated messages found for Day 1` on curated campaigns, because `_pre_enroll_prospects` sets `smart_campaign_send_day=None` and neither `run_fast_discovery` nor `_generate_day1_messages` ever sets it to 1.

**Cause:** The June 3, 2026 rewrite of `run_fast_discovery` removed the `plan_channel_assignments` call that was in the old flow. The approve-day route expects `send_day==1` on all Day-1 enrollments.

**Fix (onboarding flow):** `launch_onboarding_first_campaign` in `services/onboarding_prospect_service.py` explicitly bulk-writes `smart_campaign_send_day=1, smart_campaign_channel=email` on the 5 confirmed enrollments after `_pre_enroll_prospects`. This makes `approve_day(campaign, 1)` work. The bug in the standard curated campaign path is pre-existing and should be fixed separately in `run_fast_discovery`.

---

## 2026-06-08 — `Linkedin` icon missing from lucide-react

**File:** `frontend/app/(dashboard)/admin/users/[id]/page.tsx` line 20

**Error:**
```
Export Linkedin doesn't exist in target module (lucide-react).
Did you mean to import Link?
```

**Cause:** `lucide-react` removed brand icons (including `Linkedin`) in newer versions due to trademark/copyright restrictions.

**Fix:** Replaced `Linkedin` with `Link2` (chain-link icon) in three places:
- The import line
- The LinkedIn tab trigger (`<Link2 className="h-3.5 w-3.5 mr-1.5" />`)
- The "Connect LinkedIn as this user" button (`<Link2 className="h-4 w-4" />`)

**How to apply:** Never import `Linkedin`, `Github`, `Twitter`, or other brand icons from `lucide-react` — use `Link2`, `ExternalLink`, or a custom SVG instead.

---

## 2026-07-02 — E2E QA harness: 4 bugs found and fixed

### Bug #1 — Auth `password_hash` field (login 401)
**File:** `routes/auth.py:133`
**Error:** `401 Invalid email or password` — harness set `password` field but auth checks `password_hash`.
**Fix:** Used bcrypt to hash and write to `password_hash`, unset stale `password` field in Atlas via Motor.

### Bug #2 — `prospect_count_target` min=25 constraint (422)
**File:** `models/campaign.py:263` — `prospect_count_target: int = Field(100, ge=25, le=500)`
**Error:** `422 Input should be greater than or equal to 25` — smoke config used `prospect_count: 20`.
**Fix:** Changed smoke profile to `prospect_count: 25`.

### Bug #3 — `approve_day1` race condition (400 "messages still generating")
**File:** `services/curated_discovery_service.py` — `run_fast_discovery` sets `discovery_status=completed` BEFORE background task `_run_deep_enrichment_then_messages` finishes.
**Error:** Harness polled `completed`, called `approve-day/1`, got 400 — messages weren't done yet.
**Fix:** Updated `_done()` in `step_discovery_poll` to also wait for `message_gen_status not in ("idle", "running")` before considering discovery truly done.

### Bug #4 — `discovery_companies_found` never set (always 0)
**File:** `services/curated_discovery_service.py:621`
**Error:** API response always shows `discovery_companies_found: 0` even when 10+ companies were sourced.
**Fix:** Added `"discovery_companies_found": len(kept_companies)` to the `$set` alongside `curated_companies_approved`.

---

## 2026-07-03 — Delivery run (200co/400pr): message generation 0/80, OpenRouter `402 Payment Required`

**Not a code bug.** Campaign `6a46f3e7a96628eae285687a` (run `20260702-232705-delivery`), 240 enrolled, 0 messages generated, 80 `message_gen_status=failed` (day1: 20 linkedin_connection + 20 email; day2: same). Confirmed via direct Atlas query (`campaign_enrollments`, `message_gen_error` field) — every single failure has the identical error:
```
single-channel retry failed: OpenRouter failed on all models ['anthropic/claude-haiku-4-5', 'anthropic/claude-sonnet-4-5']: Client error '402 Payment Required'
```
Both primary (Haiku) and fallback (Sonnet) models were rejected — the OpenRouter account itself was out of credit, not a per-model issue. `message_gen_status` still flipped to `completed` on the campaign doc (by design — "completed" means the async job finished running, not that it succeeded), which is why `approve_day1` correctly 400'd with "No enrollments with generated messages found."

**Fix:** No code change. Top up OpenRouter credits (same pattern as the 2026-06-08 Gemini depletion). Checked balance via `GET https://openrouter.ai/api/v1/credits` (free, no-cost call) — as of 2026-07-04 shows `total_credits=90, total_usage=70.22` (~$19.78 headroom), so the depletion may have been transient/since topped up.

**How to apply:** When message-gen or enrichment fails with 0% success across many enrollments/prospects with the *same* error text, suspect an AI-provider credit/billing issue before suspecting a parsing or prompt bug — check `GET /api/v1/credits` on OpenRouter (or the equivalent for whichever provider) before diving into code. [[project_new_db_wiring]] uses Atlas directly; MongoDB is **not** run via the project's `docker-compose.yml`/Colima — that compose file is unused for local dev, query Atlas directly with the `MONGODB_URL` from `backend/.env`.
