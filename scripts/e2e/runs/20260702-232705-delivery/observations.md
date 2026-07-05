# QA Observations

Severities: blocker | major | minor | polish


---
## [BLOCKER] Step `approve_day1` failed  `00:31:02`

Error: `[approve_day1] HTTP 400: {'detail': 'No enrollments with generated messages found for Day 1. Messages may still be generating — try again in a minute.'}`

Checkpoint: /Users/prasad/Documents/Projects/outflo/backend/scripts/e2e/runs/20260702-232705-delivery/checkpoint.json

Resume with:
```
python3 scripts/e2e/run_full_flow.py --profile delivery --run-dir /Users/prasad/Documents/Projects/outflo/backend/scripts/e2e/runs/20260702-232705-delivery
```
