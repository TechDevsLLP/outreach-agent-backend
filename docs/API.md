# OutFlo launch API contract

This is the maintained launch contract, not a generated inventory of every
legacy route. In development, `/openapi.json` and `/docs` are the exact route
table. They are disabled in production. Route handlers and tests remain the
source of truth for request/response schemas.

## Authentication levels

| Level | Requirement |
|---|---|
| public | No session; limited to health, signed provider callbacks/webhooks, tracking redirect and opaque booking tokens. |
| user | Authenticated user identity. |
| account | User plus active current-account membership and account status/plan checks. |
| superadmin | Explicit configured superadmin identity; used for canonical shared-pool and platform mutations. |

IDs at the API boundary are stringified ObjectIds. Tenant routes must scope
reads and writes by the active account; a prospect or company in the shared
pool is not sufficient authorization.

## Auth and account context

| Method | Path | Level | Purpose |
|---|---|---|---|
| POST | `/api/auth/register` | public | Create a user and initial account. |
| POST | `/api/auth/login` | public | Authenticate. |
| GET/PATCH | `/api/auth/me` | user | Read/update profile. |
| POST | `/api/auth/refresh` | user | Refresh authentication. |
| POST | `/api/auth/logout` | user | End the client session. |
| POST | `/api/auth/password-reset/request` | public | Request a single-use reset. |
| POST | `/api/auth/password-reset/confirm` | public | Consume reset token. |
| GET | `/api/accounts` | user | List memberships/accounts. |
| POST | `/api/accounts/switch/{account_id}` | user | Switch active account after membership validation. |

The first-party browser uses a server-set `Secure` (production), `HttpOnly`,
`SameSite=Lax` session cookie. Unsafe cookie-authenticated requests require the
matching `outflo_csrf` cookie value in `X-CSRF-Token`, plus an exact configured
Origin in production. Explicit non-browser clients may continue to use bearer
authentication. Browser-mode login, refresh, account switching, and
impersonation never return the live JWT in response JSON.

## Onboarding and guided campaign creation

The onboarding wizard captures company context, ICP, offer and sender. OAuth
initiation returns a provider authorization URL plus signed, one-time state.
The frontend must return that exact state and only a local application return
destination.

Key routes include `/api/onboarding/start`, staged company/ICP/offer/email
steps, `/api/onboarding/prospect-preview`, `/api/onboarding/session`, and
`/api/onboarding/launch-first-campaign`. Some onboarding discovery progress
still uses its specific run record/SSE path; it is not the general durable
`jobs` queue contract.

## Prospects and campaign fit

| Method | Path | Level | Purpose |
|---|---|---|---|
| GET | `/api/prospects` | account | Paginated shared facts joined with authorized account/campaign state. |
| GET | `/api/prospects/{prospect_id}` | account | Detail with tenant overlay and optional campaign score projection. |
| PATCH | `/api/prospects/{prospect_id}` | account | Update tenant overlay fields only. |
| POST | `/api/prospects/manual` | account | Add/reuse a canonical prospect and create authorized tenant overlay. |
| GET | `/api/prospects/stats` | account | Tenant-visible summary. |
| POST | `/api/enrichment/trigger` | account | Create an enrichment run and enqueue tenant-owned durable work. |
| GET | `/api/enrichment/runs/{run_id}` | account | Tenant-owned run state. |
| POST | `/api/enrichment/prospects/{id}/unlock-phone` | account | Explicit credit-bearing phone unlock; cached result is free. |

For campaign-aware projections, `campaign_prospect_state` is authoritative.
Clients must render score `0` as a scored result and `null` as unscored, and
should display score version, reason, completeness and enrichment state.

## Companies and employees

`GET /api/companies`, company detail and authorized prospect/employee views are
account-accessible reads over the shared pool. Canonical mutations—scrape,
bulk delete/rescrape, promote, enrich, create from LinkedIn and delete—require
superadmin/system authority even if older clients still render controls.
Tenant users express selections and status through overlays/campaigns; they do
not own canonical company rows.

`POST /api/employees/select-and-enrich` must create tenant-neutral canonical
prospects and tenant-specific overlay/work records. It must never write an
`account_id`, campaign score or outreach state onto a shared prospect.

## Campaigns and sequence approval

| Method | Path | Level | Purpose |
|---|---|---|---|
| GET/POST | `/api/campaigns` | account | List/create tenant campaigns. |
| GET/PATCH | `/api/campaigns/{campaign_id}` | account | Tenant-owned detail/update. |
| POST | `/api/campaigns/smart` | account | Create guided smart campaign and start discovery. |
| GET | `/api/campaigns/{campaign_id}/discovery-status` | account | Discovery progress. |
| GET | `/api/campaigns/{campaign_id}/enrolled-prospects` | account | Campaign cohort with authoritative campaign score. |
| GET/PUT | `/api/campaigns/{campaign_id}/sequence` | account | Read/persist canonical `sequence_graph_v1` before launch. |
| POST | `/api/campaigns/{campaign_id}/approve-day/{day}` | account | Approve a reviewed day; first approval activates. |
| POST | `/api/campaigns/{campaign_id}/approve-and-launch` | account | Day-one compatibility alias. |
| POST | `/api/campaigns/{campaign_id}/generate-messages` | account | Generate campaign messages; no send. |
| POST | `/api/campaigns/{campaign_id}/activate` | account | Activate only when launch guards pass. |
| POST | `/api/campaigns/{campaign_id}/pause` | account | Pause execution. |

Missing or malformed canonical sequences return `409` before DB/provider send
side effects. A backend default returned with `is_default=true` is a proposal,
not persisted campaign state; the client must explicitly save it. Legacy
`follow-up-flow` routes are compatibility surfaces and not the launch editor
contract.

Enrollment list/detail/mutations are scoped by both account and campaign and
require authorized prospect overlay access. Company-cascade selection cannot
cross tenants through a shared company/prospect record.

## Connected senders

| Method | Path | Level | Purpose |
|---|---|---|---|
| GET | `/api/email-accounts` | account | Tenant mailboxes and health/warm-up metadata. |
| GET + POST | `/api/email-accounts/oauth/google/url` + `/exchange` | account | Signed-state Google OAuth. |
| GET + POST | `/api/email-accounts/oauth/zoho/url` + `/exchange` | account | Signed-state, data-center-allowlisted Zoho OAuth. |
| POST | `/api/email-accounts/smtp` | account | Connect SMTP+IMAP; IMAP is required. |
| PATCH/DELETE | `/api/email-accounts/{id}` | account | Configure email cap/warm-up state or disconnect tenant mailbox. |
| POST | `/api/email-accounts/{id}/test` | account | Real provider action; controlled staging only until launch gates pass. |
| GET | `/api/linkedin-accounts` | account | Tenant LinkedIn senders. |
| POST | `/api/linkedin-accounts/connect/hosted-auth` | account | Begin tenant-bound Unipile hosted auth. |
| PATCH | `/api/linkedin-accounts/{id}` | account | Configure separate connection/InMail/message caps and warm-up state. |

Microsoft OAuth connect/exchange/refresh endpoints are deliberately
launch-disabled and return `410 Gone`. Microsoft is not a launch provider.

## Conversations and notifications

Conversation list/detail/reply/draft routes under `/api/conversations` require
account context. The backend resolves the exact tenant/provider/thread; it does
not adopt a conversation by prospect ID. AI drafts remain reviewable before
send.

| Method | Path | Level | Purpose |
|---|---|---|---|
| GET | `/api/notifications` | account | Durable tenant notification page. |
| GET | `/api/notifications/unread-count` | account | Tenant unread count. |
| PATCH | `/api/notifications/{id}/read` | account | Mark one tenant notification read. |
| PATCH | `/api/notifications/read-all` | account | Mark tenant notifications read. |
| DELETE | `/api/notifications/{id}` | account | Delete tenant notification. |
| GET | `/api/notifications/stream` | user + membership | Cookie-authenticated Mongo-backed SSE with replay from `Last-Event-ID`. |

SSE events are durable and visible across API/scheduler processes. Browser
credentials are carried by the HttpOnly session cookie and never appear in the
event-stream URL.

## Public callbacks and webhooks

| Method | Path | Verification |
|---|---|---|
| POST | `/api/webhooks/unipile/messages` | Required configured signature outside explicit dev/test. |
| POST | `/api/linkedin-accounts/connect/webhook` | Hosted-auth correlation to tenant-bound initiation. |
| POST | `/api/calendar/webhooks/google` | Exact channel/resource/tenant/Google-account verification; triggers bounded event reconciliation. |
| GET | `/api/track/click/{token}` | Opaque redirect token. |
| GET/POST | `/api/public/book/{token}` and `/confirm` | Opaque booking token; meeting idempotency gate applies. |

Email replies are polled from tenant mailboxes; SendGrid inbound/activity and
open-pixel tracking are not launch APIs.

New Google bookings persist the exact tenant email-account, calendar and event
binding. Reconciliation handles provider cancellation/deletion, attendee
response and start/end rescheduling with a Mongo lease and event fingerprint.
Legacy booked rows without this binding are skipped rather than assigned to an
arbitrary mailbox.

## Operations

`GET /health` is public and returns a generic failure when Mongo is unavailable;
it does not expose database or credential details. Superadmin system routes
surface scheduler heartbeat, provider usage, webhook metadata and audit logs.
Production docs/OpenAPI are disabled.
