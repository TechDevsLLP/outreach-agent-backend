---
name: feedback-curated-discovery-bugs
description: "Six bugs found and fixed in the curated smart-campaign discovery pipeline (June 2026 E2E test session). Critical for any future work touching campaign_prospect_finder_service, campaign_scoring_service, ai_prefilter_service, or employee_scraper_service."
metadata: 
  node_type: memory
  type: feedback
  originSessionId: 01730ece-b13d-4c73-8293-b04d49943eb2
---

Six bugs found and fixed during the curated discovery E2E test (campaign_id: 6a1ca5d2b4b29d020f4b9924):

**Bug 1 — ai_prefilter_service.py: KeyError on batch index 20+**
`batches = [list(enumerate(candidates[i:i+batch_size]))]` creates LOCAL 0-based tuples. Code used `global_i` as key but it was actually the local index. Batches 2+ overwrote indices 0-19 and left 20+ unset → `KeyError: 20`.
Fix: use `gi = batch_offset + local_i` as the results_map key throughout.

**Bug 2 — campaign_prospect_finder_service.py: enrollment status query mismatch**
`_enrich_and_finalize_discovery` queried `status: "enriching"` but `_pre_enroll_prospects` creates `status: "scoring"`. Fix: `{"$in": ["enriching", "scoring"]}`.

**Bug 3 — campaign_prospect_finder_service.py: `_enroll_prospects` skipped "scoring" enrollments**
`_enroll_prospects` only promoted `status="enriching"` → "active". Prospects pre-enrolled as "scoring" fell into the `else` branch and were silently skipped. The top-up loop triggered because `assignments=0`.
Fix: `elif existing_status in ("enriching", "scoring")` + update_many targets `{"$in": ["enriching", "scoring"]}`.

**Bug 4 — campaign_prospect_finder_service.py: candidate projection missing scoring fields**
The `find()` projection at line ~538 omitted `job_title`, `headline`, `company_description`, `company_keywords`. The scoring service had no data to compute seniority or keyword overlap.
Fix: add those four fields to the projection.

**Bug 5 — campaign_scoring_service.py: industry synonyms too narrow + seniority inference**
Gemini returns industries like "Digital Pathology / AI", "Drug Discovery AI", "Medical Imaging AI" — none matched icp_industries "healthcare", "biotechnology". Added ~10 terms to `_INDUSTRY_SYNONYMS["healthcare"]` and new entries for "biotechnology" and "health technology".
Also added `_infer_seniority_from_title()` fallback: when `seniority_level=None` in DB, infer from stored `job_title`/`headline`. Watch out: "director" contains "cto" as substring — use word-boundary matching for c-suite abbreviations.

**Bug 6 — employee_scraper_service.py: `_curated_map_seniority` missed headline**
When `current_position["title"]` is empty, the function returned None instead of falling back to `apify_employee.get("headline")`. Many Apify employees have empty current_position.title but a populated headline like "VP of Product @ VideaHealth".
Fix: `title = (current_position.get("title") or apify_employee.get("headline") or "").lower()`.

**Bug 7 — email_finder_service.py: field-name parse bug → 0 emails on all prospects**
`find_emails_for_linkedin_urls` read `item.get("url") or item.get("linkedinUrl")` but the Apify actor `bfH8Ermocz8oYKQVO` emits `linkedin_url` (confirmed by sibling `_find_emails_sync` and user's sample output). Every email silently mapped to None.
Fix: `url = item.get("linkedin_url") or item.get("url") or item.get("linkedinUrl")`.

**Bug 8 — curated_discovery_service.py: email finder called N times instead of 1**
The Phase D email finder call was inside the per-company `for sc, employees in results:` loop (lines 210-227). For N approved companies each missing emails, this fired N separate Apify actor runs — 96 runs × $0.046 = $4.40 wasted. The actor already accepts a batch list.
Fix: after `asyncio.gather` completes Phase C, collect all missing-email LinkedIn URLs across all companies into one deduped list, call `find_emails_for_linkedin_urls` once, then apply the shared `email_by_url` dict during the upsert pass.

**Why:** All bugs caused `_enrich_and_finalize_discovery` to fail OR produced 0 emails/active enrollments.
**How to apply:** If curated discovery completes but enrollments stay "scoring" (never "active"), suspect bugs 2-4. If scoring returns 0/low for all prospects, suspect bugs 4-5. Check Apify employee data when seniority_level=None. If `with_email=0` despite Apify actor showing successful runs, suspect bug 7.
