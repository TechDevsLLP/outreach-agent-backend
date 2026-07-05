---
name: project-qa-e2e-harness
description: "E2E QA harness (run_full_flow.py) driving full onboarding→smart-campaign flow with file-based logging, checkpoint/resume, and delivery targeting 200co/400pr."
metadata: 
  node_type: memory
  type: project
  originSessionId: d2ba02ca-c34f-453c-b018-dca39defcf03
---

# OutFlo E2E QA Harness

## Location
`backend/scripts/e2e/run_full_flow.py`

## Purpose
REST-level integration test that drives the full flow a frontend user would: onboarding → smart campaign creation → discovery → approve-day. Logs every API call to `api_log.jsonl`, checkpoints step state so re-runs continue from last success.

## CLI
```bash
python3 scripts/e2e/run_full_flow.py --profile smoke     # 10co/25pr (fast shake-out)
python3 scripts/e2e/run_full_flow.py --profile delivery  # 200co/400pr (real scale)
python3 scripts/e2e/run_full_flow.py --profile smoke --resume  # continue from last checkpoint
python3 scripts/e2e/run_full_flow.py --profile smoke --run-dir runs/20260702-231109-smoke  # specific run
```

## Test Account
- Email: `prithvi@techdevs.in`
- Password: `TechDevs2025!`
- Account/User ID: `6a25c9b135e9ddcaec3d83eb` (account), `6a25c9b135e9ddcaec3d83ea` (user)

## Run Artifacts (per run in `runs/<timestamp>-<profile>/`)
- `api_log.jsonl` — every request+response logged BEFORE any raise
- `checkpoint.json` — step statuses + shared state (token, session_id, campaign_id, etc.)
- `observations.md` — bugs with severity and resume command
- `summary.md` — step pass/fail and counts

## Bugs Found and Fixed (2026-07-02/03 session)
1. **Bug #1 (Auth):** `password_hash` field mismatch → bcrypt hash written to correct field
2. **Bug #2 (Validation):** `prospect_count_target` min=25, smoke had 20 → changed to 25
3. **Bug #3 (Race condition):** `discovery_status=completed` set before background message gen finishes → harness now waits for `message_gen_status not in ("idle","running")`
4. **Bug #4 (Counter):** `discovery_companies_found` never set in Stage A/B/C path → added to `$set` at `curated_discovery_service.py:621`

## Smoke Run Result (2026-07-02, run `20260702-231109-smoke`)
- **Campaign:** `6a46f088b3671f48fb17deb6`
- All 17 steps DONE; approve_day2 skipped (all 6 on day 1, expected)
- 6 prospects enrolled, 6 messages generated (linkedin_connection, personalized)
- QA probe: all green — messages personalized, canonical ICP correct, architecture sound

## Delivery Run (2026-07-03, run `20260702-232705-delivery`, STALLED — BLOCKER)
- **Campaign:** `6a46f3e7a96628eae285687a`
- Target: 200 companies / 400 prospects
- Discovery succeeded fully: 200 companies, 244 prospects scraped, 240 enrolled (Bug #4 fix confirmed working)
- **Blocked at `approve_day1`:** message generation ran twice (day1 @00:31, day2 @00:49) and got 0/80 successes — root cause was OpenRouter `402 Payment Required` on both primary+fallback models (credits depleted at the time), NOT a code bug. Full details: [[error_log]]. 160 enrollments still `scheduled_later` (days 3+), never attempted.
- Checkpoint/logs intact at `backend/scripts/e2e/runs/20260702-232705-delivery/`. Resume with `--profile delivery --run-dir <that dir>` once OpenRouter credit is confirmed sufficient.
- MongoDB is Atlas (`backend/.env` → `MONGODB_URL`), NOT the project's `docker-compose.yml`/Colima setup — query Atlas directly for any DB inspection, no local Docker needed.
- Resuming past `approve_day1` will transition the campaign to `active` and let the 5-min campaign engine start real LinkedIn/email sends — confirm with user before resuming.

## Why approve_day2 may 400
All prospects may be on day 1 (if total fits daily LinkedIn/email caps). The harness handles this:
- If day-2 enrollment count == 0 → logs minor "no day-2 enrollments", marks done
- If day-2 enrollments exist → retries up to 10 min for message gen to finish
