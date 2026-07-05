# QA Observations

Severities: blocker | major | minor | polish


---
## [BLOCKER] Step `login` failed  `23:11:10`

Error: `[login] HTTP 401: {'detail': 'Invalid email or password'}`

Checkpoint: /Users/prasad/Documents/Projects/outflo/backend/scripts/e2e/runs/20260702-231109-smoke/checkpoint.json

Resume with:
```
python3 scripts/e2e/run_full_flow.py --profile smoke --run-dir /Users/prasad/Documents/Projects/outflo/backend/scripts/e2e/runs/20260702-231109-smoke
```

---
## [BLOCKER] Step `login` failed  `23:12:05`

Error: `[login] HTTP 401: {'detail': 'Invalid email or password'}`

Checkpoint: /Users/prasad/Documents/Projects/outflo/backend/scripts/e2e/runs/20260702-231109-smoke/checkpoint.json

Resume with:
```
python3 scripts/e2e/run_full_flow.py --profile smoke --run-dir /Users/prasad/Documents/Projects/outflo/backend/scripts/e2e/runs/20260702-231109-smoke
```

---
## [BLOCKER] Step `create_campaign` failed  `23:12:59`

Error: `[create_campaign] HTTP 422: {'detail': [{'type': 'greater_than_equal', 'loc': ['body', 'prospect_count_target'], 'msg': 'Input should be greater than or equal to 25', 'input': 20, 'ctx': {'ge': 25}, 'url': 'https://errors.pydantic.dev/2.12/v/greater_than_equal'}]}`

Checkpoint: /Users/prasad/Documents/Projects/outflo/backend/scripts/e2e/runs/20260702-231109-smoke/checkpoint.json

Resume with:
```
python3 scripts/e2e/run_full_flow.py --profile smoke --run-dir /Users/prasad/Documents/Projects/outflo/backend/scripts/e2e/runs/20260702-231109-smoke
```

---
## [BLOCKER] Step `approve_day1` failed  `23:15:33`

Error: `[approve_day1] HTTP 400: {'detail': 'No enrollments with generated messages found for Day 1. Messages may still be generating — try again in a minute.'}`

Checkpoint: /Users/prasad/Documents/Projects/outflo/backend/scripts/e2e/runs/20260702-231109-smoke/checkpoint.json

Resume with:
```
python3 scripts/e2e/run_full_flow.py --profile smoke --run-dir /Users/prasad/Documents/Projects/outflo/backend/scripts/e2e/runs/20260702-231109-smoke
```

---
## [MINOR] approve_day2 returned 400 (may be no day-2 scheduled)  `23:21:08`

{'detail': 'No enrollments with generated messages found for Day 2. Messages may still be generating — try again in a minute.'}
