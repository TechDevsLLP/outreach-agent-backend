# OutFlo MongoDB data contract

MongoDB collection handles and startup indexes are defined in
`backend/database.py`. The database name is configured by `MONGODB_DATABASE`
and defaults to `outflo_v3`; deployment must set it explicitly. Atlas Search
and Vector Search definitions are managed outside `create_indexes()` and must
be inspected during staging restore.

## Invariants

1. Tenant-scoped `account_id` values are strings at write boundaries.
2. `companies` and `prospects` are canonical, tenant-neutral shared data.
3. Tenant UI state belongs in `prospect_state`; campaign fit and campaign
   enrichment belong in `campaign_prospect_state`.
4. Shared-pool identity is never authorization evidence.
5. Conversation identity includes tenant, channel, provider account, and
   provider thread.
6. Provider sends are replayed by `send_key`, never by guessing from message
   content or prospect identity.
7. Unique, claim, TTL, and partial indexes are part of the application
   contract. A restore is not ready until all are present and inspected.

## Shared pool and overlays

| Collection | Scope | Contract |
|---|---|---|
| `companies` | global | Canonical company facts. Deduplicated by normalized provider identities such as LinkedIn URL/domain. No account ownership, campaign score, outreach status, or provider work state. |
| `prospects` | global | Canonical person/contact facts and tenant-neutral intelligence. Unique sparse email/LinkedIn identities. No tenant score, notes, campaign status, send time, or ownership. |
| `prospect_state` | account + prospect | Tenant status, tags, notes, tenant intelligence/pitch, visibility, ownership/use and cooldown evidence. Unique `(account_id, prospect_id)`. |
| `campaign_prospect_state` | account + campaign + prospect + scoring version | Authoritative campaign fit (`0` is valid, `null` is unscored), reasoning, completeness, cohort, and enrichment state. Unique full key plus cohort-work and score-read indexes. |
| `industries_taxonomy` | global | Canonical industry taxonomy. |

The local SQLite gazetteer `data/geo_places.sqlite` is the location resolver.
The `geo_places` Mongo handle is vestigial compatibility, not the active search
store.

Only canonical `companies` and `prospects` plus explicitly proven overlays are
preserved by the shared-pool migration. Campaigns, messages, conversations,
notifications, usage, and other operational history are intentionally outside
that migration scope.

## Identity and account data

| Collection | Key purpose / boundary |
|---|---|
| `users` | Login identity and current account pointer; email unique. |
| `accounts` | Organization, plan/status, configured channel quotas and admin overrides. |
| `account_members` | User membership and role; unique `(account_id, user_id)`. |
| `company_profiles` | Account ICP, offer, and sender voice context; one per account. |
| `password_reset_tokens` | Single-use reset state with expiry TTL. |
| `oauth_state_nonces` | One-time signed OAuth state nonce; TTL plus tenant/provider lookup. |
| `email_accounts` | Tenant-owned Gmail, Zoho, or SMTP+IMAP sender, encrypted credentials, health, configured daily limit and warm-up metadata. |
| `linkedin_accounts` | Tenant-owned Unipile LinkedIn sender and provider status. |

Microsoft connection endpoints are disabled and Microsoft is not a supported
launch sender.

## Campaign planning and execution

| Collection | Contract / important indexes |
|---|---|
| `campaigns` | Tenant-owned campaign definition, guided-autopilot lifecycle, persisted `sequence_graph_v1`, channel caps, discovery/message-generation status. Account/status indexes. |
| `campaign_enrollments` | Tenant/campaign/prospect execution state, current sequence node/version, next action and execution lease. Due-work claim index begins with status/next-action/lease. |
| `campaign_messages` | Durable sent-message projection with provider IDs and partial unique `send_key`; tenant/campaign/enrollment indexes. |
| `send_attempts` | Provider outbox and reconciliation state. Unique `send_key` and execution identity; dispatch-reaper, tenant retry-queue and provider-reconcile indexes. |
| `sourced_companies` | Campaign discovery candidates and review decisions; tenant/campaign indexes. |
| `campaign_daily_schedules`, `campaign_schedule_items` | Scheduled execution views. |
| `campaign_daily_stats` | Per-campaign daily aggregates; unique campaign/date. |
| `sender_daily_caps` | Atomic per-sender-key/channel/day counter across campaigns. Stores effective limit, warm-up status/day and per-channel `next_send_not_before`; the reservation advances count and throttle together. |
| `daily_usage_counters` | LinkedIn account/day usage compatibility counter. |
| `suppressions` | Tenant do-not-contact identifiers. Canonical unique `(account_id, identifier_type, identifier)`; the incompatible legacy email-only unique index is removed before creation. |

`send_attempts.state=ambiguous` is an operational quarantine, not a retry
queue. Provider evidence or an approved human decision is required to resolve
it.

## Durable work

`jobs` is a Mongo-backed tenant-partitioned queue. It has:

- partial unique `(account_id, job_type, job_key)` for deterministic work;
- claim ordering by tenant/state/priority/availability/creation;
- expired-lease and tenant/type/state indexes;
- queued, running, retry-scheduled, completed, cancelled, and dead-letter
  semantics with lease owner/expiry, heartbeat, checkpoint, attempts and result.

Prospect enrichment uses this queue today. Other background paths must not be
described as durable merely because they resume or sweep state on process
startup.

`enrichment_runs`, `search_runs`, and `onboarding_scrape_jobs` are user-facing
run/progress records; they are not substitutes for the `jobs` lease contract.

## Inbox, replies, and agent state

| Collection | Contract / important indexes |
|---|---|
| `conversations` | Embedded messages. Unique canonical provider-thread identity within tenant/account/channel; unique provider-message replay indexes; tenant inbox/prospect reads. |
| `reply_classifications` | Tenant-scoped classifier output and category. |
| `meetings` | Tenant-scoped proposal/booking state and public token. New Google events store exact provider-account/calendar/event binding, attendee state, start/end, sync fingerprint, due time, attempts and lease. Unique partial event-binding and due-reconciliation indexes. |
| `linkedin_connection_requests` | Tenant/provider connection request and acceptance state. |
| `notifications` | Mandatory string `account_id`, one `body` schema and optional context; tenant/read/time indexes. Mongo is the SSE source of truth. |
| `click_tracking_tokens` | Opaque redirect token and campaign/prospect context. Open-pixel tracking is not a launch feature. |

## Provider usage and operations

| Collection | Purpose |
|---|---|
| `apify_usage`, `openrouter_usage`, `growthtoolkit_usage` | Provider usage/cost evidence with tenant/campaign tags where available. |
| `scheduler_heartbeats` | Last run/status/error per scheduled job. |
| `webhook_log` | Metadata-only webhook delivery audit; payload bodies are not retained. |
| `admin_audit_log` | Redacted superadmin mutation audit. |
| `system_settings`, `system_prompts` | Runtime flags and prompt overrides; operationally controlled. |

## Restore and index gate

Use [SHARED_POOL_MIGRATION.md](SHARED_POOL_MIGRATION.md). Restore only into an
empty target, run `create_indexes()`, inspect every unique/TTL/partial/claim
index, and compare manifest counts/hashes. A target with a failed restore or
failed index build is abandoned, not repaired in place. Atlas PITR and an
independent restore drill are mandatory before cutover.

Legacy `employees`, `leads`, and `outreach_schedules` may still exist for
compatibility or rollback. They are not canonical launch data and must not be
used to infer current ownership, score, or execution state.
