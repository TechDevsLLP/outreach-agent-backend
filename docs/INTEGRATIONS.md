# External integration contracts

Launch channels are email and LinkedIn. All provider calls must carry the
owning account and selected provider account through to durable state. Never
select the first account in a provider workspace, infer ownership from a shared
prospect, or retry an unknown provider outcome.

## Launch support matrix

| Capability | Provider | Launch status | Safety contract |
|---|---|---|---|
| Gmail send/reply/draft | Google Gmail API | supported after staging proof | Signed one-time OAuth state, exact redirect, tenant-owned mailbox, encrypted token, provider thread/message IDs. |
| Zoho send/reply/draft | Zoho Mail API | supported after staging proof | Exact data-center allowlist; reject arbitrary returned origins before HTTP; encrypted token. |
| Custom email | SMTP + IMAP | supported after staging proof | IMAP required for replies/drafts; credentials encrypted; TLS settings reviewed. |
| Outlook/Microsoft | Microsoft OAuth | launch-disabled | Connect/exchange/refresh return `410 Gone`; do not present as a launch channel. |
| LinkedIn | Unipile | supported after staging proof | Signed webhook, exact hosted-auth/provider account ownership, no workspace-global fallback. |
| Email/phone enrichment | GrowthToolkit | supported after fixture and staging proof | Typed body-code handling, bounded retry/polling, tenant usage accounting; phone unlock only on explicit request. |
| Company/employee/post scraping | Apify | supported after cost/concurrency proof | Lazy client construction, configured actor IDs, bounded concurrency and usage tags. |
| AI generation/research | OpenRouter + direct Gemini | supported after failure/cost proof | Structured outputs, bounded concurrency/retry and usage tags; no secrets or full provider payloads in logs. |
| Calendar | Google Calendar | not a send channel | Watch verification, renewal, polling fallback and meeting idempotency must pass before automatic booking. |

“Supported after staging proof” is not production approval. The exact proof is
listed in [the deploy runbook](../deploy/README.md).

## OAuth and credential boundary

Google and Zoho authorization URLs are issued with signed state containing the
tenant, provider, redirect target, nonce and ten-minute expiry. The nonce is
atomically consumed once. Frontend callbacks forward this state and accept only
local application return destinations.

Production requires Fernet encryption for OAuth access/refresh tokens and
SMTP/IMAP passwords. Encryption, webhook secrets and OAuth client secrets fail
closed when missing or placeholder. Provider credentials are never included in
API responses, migration artifacts, request URLs, or logs.

Zoho's account and API origins are derived only from a fixed supported
data-center mapping. An `accounts-server` value returned by OAuth is untrusted
input, not a URL to call directly.

## Email provider facade

`services/email_delivery_service.py` selects an `EmailProvider` implementation
from the tenant-owned `email_accounts` record. Implementations cover Google,
Zoho and SMTP+IMAP. Replies preserve native thread identity; SMTP uses RFC
`Message-ID`, `In-Reply-To`, and `References`.

Email replies are polled by the scheduler. Confirmed inbound messages pass
through the replay-safe reply-ingest path and canonical conversation identity.
The notification and enrollment updates are tenant-scoped.

Email and LinkedIn account PATCH routes expose separate sender policy controls.
Email accounts configure `daily_send_limit`; LinkedIn accounts configure
connection, InMail and message limits independently. New accounts default to
warm-up enabled with `warmup_status=warming` and a persisted
`warmup_started_at`. Runtime policy uses the lowest applicable configured,
warm-up and provider/product ceiling and stores each channel's atomic
`next_send_not_before` reservation in `sender_daily_caps`, so campaigns sharing
one sender cannot independently bypass pacing.

Before launch, demonstrate that each sender enforces all of the following,
across all campaigns using it:

- PATCH-configured mailbox/LinkedIn channel limit and provider ceiling;
- email versus LinkedIn channel caps;
- warm-up enabled/status/day and mailbox health;
- provider-specific pacing/backoff, atomic `next_send_not_before`, and retry budget;
- suppression immediately before dispatch;
- one send for one durable send key under worker races;
- provider reconciliation for timeout/unknown outcomes.

The policy and atomic reservations are implemented; staging still must prove
multi-worker contention, day rollover, clock behavior and real provider rate
limits. This remains a no-go gate until that evidence is attached.

## Unipile LinkedIn

Every action is bound to the campaign's tenant-owned `linkedin_accounts`
record and its `unipile_account_id`. LinkedIn profile/provider IDs are resolved
inside that account. InMail is a separate channel/cap and may require a premium
LinkedIn entitlement.

Inbound messages enter through `POST /api/webhooks/unipile/messages`. Outside
explicit development/test modes, a configured secret and valid signature are
mandatory. The event resolves exactly one provider account to one account and
uses provider account + thread/message IDs for idempotency. Hosted-auth account
claims cannot adopt an arbitrary provider account from the workspace.

## GrowthToolkit

`services/growthtoolkit_service.py` is the consolidated client and
`email_finder_service.py` is its email-finder facade. It treats the body's
success/code contract as authoritative even when HTTP status is 200, handles
asynchronous task polling, and records usage.

Launch fixtures must cover success, delayed completion, not found, invalid
input, authentication failure, credit exhaustion, rate limit with server delay,
5xx/network retry exhaustion, and malformed bodies. No prospect should be
marked “enriched” merely because a provider call completed; campaign
enrichment state records outcome separately from canonical facts.

Phone unlock is a credit-bearing, explicit user action. It must return cached
numbers without spending another credit and must never run implicitly during
campaign launch.

## Apify

Current integrations include employee scraping, company details and recent
LinkedIn posts. Provider clients must be constructed lazily so unit tests,
read-only routes and process startup do not require a live actor client or
trigger network work. Paid calls require bounded concurrency, tenant/campaign
usage tags and retry/cost evidence.

Actor IDs and exact input/output fixtures are operational configuration. Verify
them in staging instead of treating hard-coded historical IDs as a stable API.

## AI providers

Model selection is configured in `config.py`; it may change independently of
the product contract. Fit must always be stored as versioned campaign state
with reasoning/completeness, and generated text must remain attributable to a
campaign, sequence node and prospect. A model success must not bypass launch
approval, suppression, cap, sender ownership or send-attempt state.

Provider failure tests must cover invalid JSON/schema, rate limit, timeout,
partial batch failure and exhausted budget. Usage/cost events must be visible
without logging prompts that contain customer data.

## Webhook and calendar boundaries

- Unipile: signed, replay-safe and tenant/provider resolved.
- Google Calendar: validate channel/resource identity, renew watches, retain the
  polling fallback and make meeting changes idempotent.
- Hosted-auth callbacks: correlate to a signed, tenant-bound initiation.

Calendar webhook verification and meeting idempotency remain launch gates; do
not represent automatic booking as release-proven until both pass in staging.

### Google meeting reconciliation

New Google bookings are durably bound to
`(account_id, calendar_provider_account_id, calendar_id, calendar_event_id)`.
The provider-account ID is the tenant-owned `provider=google` email-account
record. A campaign-selected mailbox is preferred; otherwise selection succeeds
only when exactly one eligible Google calendar account exists. Reconciliation
never switches mailboxes after booking.

Signed watch notifications durably mark only meetings on that exact provider
binding due and return without provider fan-out. The ten-minute leased
scheduler groups at most 10
tenant/mailbox bindings, runs at most five groups concurrently and processes at
most five due meetings in each (50 provider reads per cadence). Each
meeting uses a Mongo lease, event fingerprint and exponential retry schedule.
Google cancellation/deletion, attendee decline/acceptance/tentative/needs-action
state and changed start/end times project idempotently; cancellation reactivates
only the exact tenant enrollment once.

HTTP 404/410 is authoritative deletion evidence. Authentication, rate-limit,
server, malformed-response and transport failures preserve local state and
retry later. Existing booked rows without the full binding are intentionally
not inferred and require an operator-reviewed backfill. Controlled Google
staging evidence remains a launch gate.
