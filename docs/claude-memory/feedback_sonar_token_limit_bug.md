---
name: feedback-sonar-token-limit-bug
description: "Critical production bug — Sonar Pro max_tokens=4096 truncates multi-company JSON, causing batches 2+ to return 0 companies in company_sourcing_service.py"
metadata: 
  node_type: memory
  type: project
  originSessionId: 01730ece-b13d-4c73-8293-b04d49943eb2
---

## Bug: Sonar Pro max_tokens=4096 Causes Multi-Batch Failure

Discovered during Healthcare AI company-search comparison test (2026-06-01).

`backend/services/company_sourcing_service.py:128` sets `max_tokens=4096` for Sonar Pro calls.

A 50-company JSON response requires ~5,000-6,500 tokens — more than 4096. Batches that hit the limit get truncated mid-JSON. `extract_json()` then grabs the first individual company dict as "the parsed result"; `.get("companies")` on it returns None → 0 companies logged.

**Symptom:** Batch 1 returns companies fine; batches 2-10 all return `raw=0 new=0`.

**Evidence:** sonar_pro.json batches 2,3,5,7,9,10 show `output_tokens: 4096` (exactly at cap); batches 4,6,8 show `output_tokens: 5` (refusal/error).

**Why:** 4096 was the Sonar Pro default but it's insufficient for the multi-company JSON payload the prompt requests (50 companies × ~100-130 tokens/company = ~5,000-6,500 output tokens needed).

**Fix:** Change `max_tokens=4096` → `max_tokens=8192` in `company_sourcing_service.py:128`. Gemini comparison test used 8192 and comfortably returned up to 6,229 tokens per batch.

**How to apply:** When reviewing or modifying company_sourcing_service.py, note this bug. Before any comparison test of Sonar Pro, verify max_tokens is ≥ 8192.
