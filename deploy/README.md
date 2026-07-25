# OutFlo EC2 production deployment

This is the controlled single-instance launch topology. It is sized for the first 100 accounts, separates the API and scheduler, keeps MongoDB on Atlas, and supports atomic release rollback. It is not a claim of zero-downtime multi-AZ availability.

## Topology

```text
Route 53 + Elastic IP
        |
  Nginx TLS :443
    |          |
Next.js      FastAPI
127.0.0.1    127.0.0.1
  :3000        :8008
                 |
          scheduler worker
                 |
          MongoDB Atlas
```

Use Ubuntu 24.04 on `m7i.large` (2 vCPU, 8 GiB) with a 50 GiB encrypted
gp3 volume as the staging baseline, not as a capacity guarantee. A `t3.small`
is not appropriate for concurrent Next builds, Python workers, scraping
orchestration, and 100 launch accounts. Assign an Elastic IP. Restrict SSH to
the operator VPN/IP; expose only 80/443 publicly. Require IMDSv2 and an instance
role limited to the exact SSM parameters, CloudWatch log group, and backup
bucket used by OutFlo.

## First installation

1. Point the production domain to the Elastic IP.
2. Run `setup.sh` once as root.
3. Store production configuration outside the release tree:

   ```bash
   sudo install -d -o root -g outflo -m 750 /etc/outflo
   sudo install -o root -g outflo -m 640 /dev/null /etc/outflo/outflo.env
   sudoedit /etc/outflo/outflo.env
   ```

   The backend validates production configuration before opening the database. Use `APP_ENV=production`, `DEBUG=false`, and separate `APP_ROLE` values supplied by systemd. Required values include strong JWT/Fernet keys, Atlas, OpenRouter, Apify, GrowthToolkit, Unipile/webhook, Google OAuth, exact HTTPS URLs, CORS origins, and trusted hosts. Never copy a developer `.env` to the server.

4. Issue the first certificate:

   ```bash
   sudo certbot certonly --standalone -d app.example.com
   ```

5. Deploy from the project root:

   ```bash
   bash backend/deploy/deploy.sh <elastic-ip> app.example.com ~/.ssh/outflo-ec2.pem
   ```

The deploy command runs offline backend/TypeScript gates, uploads an immutable release, builds it, validates Nginx, switches `/opt/outflo/current` atomically, verifies HTTPS health, and rolls back the symlink if health fails. Secrets are never rsynced. Host-key checking uses `accept-new`, not `no`.

## Required production values

```dotenv
APP_ENV=production
DEBUG=false
FRONTEND_URL=https://app.example.com
BACKEND_BASE_URL=https://app.example.com
API_BASE_URL=https://app.example.com
CORS_ORIGINS=https://app.example.com
TRUSTED_HOSTS=app.example.com
GOOGLE_REDIRECT_URI=https://app.example.com/api/auth/google/callback
ZOHO_REDIRECT_URI=https://app.example.com/api/auth/zoho/callback
MAX_REQUEST_BYTES=10485760
```

OAuth provider consoles and Unipile webhook configuration must use the same exact HTTPS origins. Microsoft connection is launch-disabled.

## Launch load envelope

The first-month target is 100 accounts × three seats. The capacity model is 50
planned messages per account per day: 5,000/day, 0.058/s over 24 hours or
0.174/s over an eight-hour active window. Provider traffic will be bursty and
must remain below the selected mailbox's email limit, its warm-up state, and
the separate LinkedIn connection/InMail/message caps.

Run the offline model first:

```bash
cd backend
python3 loadtest/launch_readonly.py --plan-only
```

Then run the read-only staging load with 100 distinct tenant credential groups
and three ephemeral seat tokens each. The harness has no mutation or provider
send code:

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

The result is capacity evidence for API/SSE reads only. It does not prove
provider throughput, cap/warm-up enforcement, write contention or tenant
isolation when credentials are repeated.

## Observability and service objectives

Ship journald service logs to CloudWatch as structured records with release ID,
service, request/job/send key and tenant-safe correlation IDs. Do not log query
strings, OAuth codes, JWTs, credentials, prompts containing customer data or
raw webhook payloads.

Dashboard and alarm at minimum:

- API requests/status and p50/p95/p99 latency by normalized route;
- Nginx 429/499/5xx, active connections and SSE connection count;
- process CPU/RSS/restarts, disk usage/inodes and certificate expiry;
- Mongo connection/operation latency, Atlas CPU/storage/connections and slow
  queries;
- `jobs` queued/running/retry/dead-letter counts and oldest available age;
- due-enrollment lag, scheduler heartbeat age and missed cadence;
- `send_attempts` by state, especially oldest `dispatching` and all
  `ambiguous` attempts;
- provider latency/error/rate-limit/credit usage by provider and channel;
- mailbox/channel cap usage, warm-up/health blocks, bounce/unsubscribe rate;
- reply ingest/classifier lag, notification SSE reconnect/replay errors.

Controlled-launch objectives (measured over a rolling hour, with synthetic
health excluded) are:

| Signal | Objective / page condition |
|---|---|
| Availability | ≥99.9% successful non-provider API requests; page on 5-minute success below 99%. |
| Read API latency | p95 ≤500 ms and p99 ≤1,500 ms for list/detail at the launch load. |
| Mutation latency | p95 ≤1,000 ms excluding acknowledged asynchronous provider work. |
| SSE | ≥99% of 300 streams connect; initial event ≤2 s; durable event visible ≤5 s; reconnect replay has no loss/cross-tenant event. |
| Scheduler | heartbeat age less than two cadences; due-work p95 lag ≤one 5-minute campaign cadence. |
| Durable enrichment | claim begins ≤30 s; no duplicate execution under a two-worker race. |
| Send safety | zero duplicate provider sends; every post-boundary unknown quarantined; zero unowned sender dispatch. |
| Errors | non-auth application 5xx <0.5%; no sustained provider error >5% for 5 minutes. |

These are launch controls, not an enterprise SLA. Stop sends first when a send
safety or tenant boundary alarm fires.

## Backups and restore

Atlas continuous backup/PITR must be enabled before any production write. Keep
the source shared-pool database read-only during migration/cutover and retain
the immutable encrypted export, manifest, hashes, quarantine disposition and
Atlas snapshot through the rollback window. Restrict bucket access to the
release operator role, require encryption/versioning/access logs, and record
retention/destruction approval because artifacts contain PII.

Before launch, restore to a new empty staging database, run the scoped shared
pool transform/restore in [SHARED_POOL_MIGRATION.md](../docs/SHARED_POOL_MIGRATION.md),
install application indexes, compare counts and SHA-256 hashes, inspect every
unique/TTL/partial/claim index, seed test account identities separately, and
run two-tenant read/score/enrichment checks. Time and record both restore and
database-secret rollback. Never repair a partial target in place.

## Exact no-go launch gates

Do not enable real customer traffic or provider sends while any item below is
missing evidence in `IMPLEMENTATION_LEDGER.md`:

- a dependency lock with reviewed hashes and clean production dependency audit;
- production secret/URL/host manifest, scoped IAM, IMDSv2, TLS/certificate
  renewal and tested health rollback;
- disposable Atlas restore with counts/hashes/references/index inventory and a
  timed PITR/secret-switch rollback drill;
- two-tenant authorization for prospects, companies, campaigns, enrollments,
  conversations, notifications, provider accounts and employee selection;
- signed one-time OAuth replay/redirect/data-center tests and removal of
  browser-readable long-lived auth, including SSE query-token hardening;
- 100-account/three-seat read load and 300 SSE streams meeting the objectives
  above without cross-tenant events or resource exhaustion;
- two workers racing one durable job and one due enrollment, producing one
  provider action and one message/counter projection;
- provider reconciliation for `ambiguous` send attempts and an operator queue
  with no blind retry after the provider boundary;
- mailbox daily limit, warm-up state, health, separate channel caps and
  provider throttle proven across concurrent campaigns;
- GrowthToolkit success/delay/not-found/rate-limit/auth/credit/malformed-body
  fixtures and bounded Apify/AI concurrency/cost evidence;
- one controlled Gmail, Zoho or SMTP mailbox send as applicable plus one
  LinkedIn send, reply threading, unsubscribe/suppression-before-dispatch,
  bounce/error, and safe pause/resume evidence;
- verified calendar webhook identity plus idempotent meeting proposal/booking;
- CloudWatch/Atlas dashboards and alarms for every signal listed above, with a
  named on-call owner and “pause all sends” procedure;
- no P0/P1 item in the audit/remediation ledger left merely “implemented” when
  its closure rule requires staging proof.

If a gate affects only provider sending, the product may remain in a read-only
internal review mode; it may not send real outreach.

## Operations

```bash
sudo systemctl status outflo-backend outflo-scheduler outflo-frontend nginx
sudo journalctl -u outflo-backend -n 200 --no-pager
sudo journalctl -u outflo-scheduler -n 200 --no-pager
curl --fail https://app.example.com/health
readlink -f /opt/outflo/current
```

Manual rollback:

```bash
sudo ln -sfn /opt/outflo/releases/<previous-release> /opt/outflo/current
sudo systemctl restart outflo-backend outflo-scheduler outflo-frontend
curl --fail https://app.example.com/health
```

Retain at least the last two known-good releases. Do not delete the active release or edit files beneath it.

## Scaling trigger

Move off the single host before any of these persist: API CPU above 60%, memory above 70%, scheduler lag above one cadence, p95 list/detail latency above 500 ms, or controlled send throughput approaches mailbox caps across hundreds of accounts. The next topology is ALB + multiple web instances, a separately autoscaled worker group, Redis/SQS or the durable Mongo job queue as proven, and no process-local coordination.
