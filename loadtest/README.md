# Read-only launch load test

This harness models the initial launch envelope: 100 accounts, three seats per
account, 50 planned messages per account per day (5,000/day), dashboard API
reads, and optionally one notification SSE stream per seat. The message rate is
reported as a capacity input only. The harness contains only HTTP `GET` calls
and cannot create campaigns, enqueue work, mutate records, or contact providers.
Seats ramp over 60 seconds by default to avoid an artificial connection storm;
override with `--ramp-seconds` only when that burst is the intended test.

Preview the model without a server or credentials:

```bash
cd backend
python3 loadtest/launch_readonly.py --plan-only
```

For local traffic, copy `credentials.example.json` outside the repository,
replace every placeholder with a short-lived test JWT, and run:

```bash
python3 loadtest/launch_readonly.py \
  --credentials /secure/outflo-load-credentials.json \
  --duration 600 \
  --include-sse \
  --report /tmp/outflo-load-report.json
```

Non-local targets are refused unless they use HTTPS and both staging guards are
present:

```bash
export OUTFLO_LOAD_CONFIRM=READ_ONLY_STAGING_LOAD
python3 loadtest/launch_readonly.py \
  --base-url https://staging.example.com \
  --allow-staging \
  --credentials /secure/outflo-load-credentials.json \
  --duration 1800 \
  --include-sse \
  --report /secure/evidence/outflo-load-report.json
unset OUTFLO_LOAD_CONFIRM
```

Use 100 distinct account entries with three distinct seat tokens for the launch
gate. Repeating fewer credentials can measure host capacity, but cannot prove
tenant isolation. Tokens currently appear in the SSE query string because that
is the application contract; use ephemeral staging-only tokens, disable proxy
query-string logging for this path, and destroy the credential file afterward.

Pass criteria are defined in the deployment runbook. This read-only run does
not replace controlled provider-send, worker-race, warm-up/cap enforcement, or
backup/restore tests.
