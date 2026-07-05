---
name: LinkedIn Integration (Unipile)
description: Full LinkedIn integration via Unipile — schema tables, actions, webhook handlers implemented
type: project
---

LinkedIn integration fully implemented. Key facts:

- **API auth**: `X-API-KEY` header (NOT Authorization: Bearer — old code was wrong)
- **provider_id ≠ LinkedIn URL** — must resolve via `GET /users/{public_id}` before sending invites
- **InMail**: `POST /chats` with `inmail:true` (NOT a separate /messages endpoint)
- **Connection acceptance**: `new_relation` webhook, up to 8h delay (LinkedIn limitation)

**New schema tables**: `linkedinAccounts`, `linkedinConnectionRequests`

**New Convex files**:
- `convex/linkedinAccounts.ts` — queries + internal mutations for linkedinAccounts table
- `convex/linkedinConnections.ts` — queries + internal mutations for connection requests
- `convex/linkedinActions.ts` — all public actions (rewritten from scratch)
- `convex/linkedinWebhookHandlers.ts` — internal handlers for Unipile webhook events
- `convex/lib/unipile-api-reference.md` — full API documentation

**Updated files**:
- `convex/http.ts` — added `/webhooks/unipile/account` (hosted auth callback) + updated `/webhooks/linkedin` to handle events directly in Convex (was forwarding to Python, now processed natively)
- `convex/schema.ts` — added linkedinAccounts + linkedinConnectionRequests tables

**Webhook endpoints**:
- `POST /webhooks/unipile/account` — Unipile calls this after user connects LinkedIn; stores account in DB, triggers profile sync
- `POST /webhooks/linkedin` — routes message_received, new_relation, account_status events

**Multi-account**: Each OutFlo `accounts` tenant can have multiple `linkedinAccounts`. First one is `isDefault=true`. Daily counters reset at midnight with auto-detect logic.

**How to wire up campaign engine**: Update `campaignEngine.ts` TODOs to call `api.linkedinActions.sendConnectionRequest` with `linkedinAccountId` from `api.linkedinAccounts.getDefault`.
