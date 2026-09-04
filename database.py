from motor.motor_asyncio import AsyncIOMotorClient
from pymongo import IndexModel, ASCENDING, DESCENDING
from config import get_settings
import logging

logger = logging.getLogger(__name__)

settings = get_settings()

client = AsyncIOMotorClient(settings.mongodb_url)
db = client[settings.mongodb_database]

# Collection references
prospects_collection = db["prospects"]  # Shared global people pool
prospect_state_collection = db["prospect_state"]  # NEW: per-tenant overlay (account_id+prospect_id)
campaign_prospect_state_collection = db["campaign_prospect_state"]  # Campaign-scoped scores/cohorts/enrichment
search_runs_collection = db["search_runs"]
industries_collection = db["industries"]  # Per-account scrape sources (unchanged)
industries_taxonomy_collection = db["industries_taxonomy"]  # NEW: ~150 canonical LinkedIn industries
geo_places_collection = db["geo_places"]  # Vestigial persist target only — gazetteer lives in data/geo_places.sqlite
enrichment_runs_collection = db["enrichment_runs"]
employees_collection = db["employees"]  # Raw employee-scrape staging (promoted into prospects)
companies_collection = db["companies"]
conversations_collection = db["conversations"]
outreach_schedules_collection = db["outreach_schedules"]
notifications_collection = db["notifications"]
system_prompts_collection = db["system_prompts"]

# ---------------------------------------------------------------------------
# Multi-tenancy collections (new for OutFlo)
# ---------------------------------------------------------------------------
users_collection = db["users"]
accounts_collection = db["accounts"]
account_members_collection = db["account_members"]
company_profiles_collection = db["company_profiles"]

# Email & LinkedIn accounts (per-user)
email_accounts_collection = db["email_accounts"]
linkedin_accounts_collection = db["linkedin_accounts"]
linkedin_connection_requests_collection = db["linkedin_connection_requests"]
# Short-lived nonces proving a /connect/notify callback belongs to a hosted-auth
# link we issued (Unipile sends no signature on that callback).
linkedin_auth_requests_collection = db["linkedin_auth_requests"]

# Campaign system
campaigns_collection = db["campaigns"]
campaign_enrollments_collection = db["campaign_enrollments"]
campaign_messages_collection = db["campaign_messages"]
campaign_daily_schedules_collection = db["campaign_daily_schedules"]
campaign_schedule_items_collection = db["campaign_schedule_items"]
campaign_daily_stats_collection = db["campaign_daily_stats"]
sourced_companies_collection = db["sourced_companies"]
# Upload-a-Lead-List (BYOL): parsed spreadsheet batches awaiting mapping/discovery
lead_upload_batches_collection = db["lead_upload_batches"]

# Denormalized stats
prospect_stats_counts_collection = db["prospect_stats_counts"]
suppressions_collection = db["suppressions"]

# Email click tracking (for Google/Zoho/SMTP accounts) — open tracking removed
click_tracking_tokens_collection = db["click_tracking_tokens"]

# Daily usage counters (per linkedin_account, per day)
daily_usage_counters_collection = db["daily_usage_counters"]

# Password reset tokens (TTL 1h — single-use, deleted after use)
password_reset_tokens_collection = db["password_reset_tokens"]

# Cost tracking collections
apify_usage_collection = db["apify_usage"]
openrouter_usage_collection = db["openrouter_usage"]

# ---------------------------------------------------------------------------
# Agentic outreach collections (added for full-autonomy mode)
# ---------------------------------------------------------------------------
meetings_collection = db["meetings"]
calendar_webhook_channels_collection = db["calendar_webhook_channels"]
reply_classifications_collection = db["reply_classifications"]
onboarding_sessions_collection = db["onboarding_sessions"]
onboarding_scrape_jobs_collection = db["onboarding_scrape_jobs"]

# ---------------------------------------------------------------------------
# Superadmin / platform observability collections
# ---------------------------------------------------------------------------
growthtoolkit_usage_collection = db["growthtoolkit_usage"]
webhook_log_collection = db["webhook_log"]
admin_audit_log_collection = db["admin_audit_log"]
scheduler_heartbeats_collection = db["scheduler_heartbeats"]
system_settings_collection = db["system_settings"]

# Durable worker queue (tenant-scoped; no process-local queue state)
jobs_collection = db["jobs"]

# Durable provider-send outbox. Once an attempt crosses a provider boundary it
# is never retried automatically unless reconciliation proves it was not sent.
send_attempts_collection = db["send_attempts"]

# One-time OAuth state nonces. The random JTI is stored as Mongo's naturally
# unique _id; TTL is cleanup, while atomic consumption enforces one-time use.
oauth_state_nonces_collection = db["oauth_state_nonces"]

# Short-lived SSE tickets. EventSource cannot send an Authorization header, so
# the browser trades its bearer token for one of these and passes it in the
# query string. The random ticket is the _id; TTL expiry is the only cleanup.
stream_tickets_collection = db["stream_tickets"]


async def create_indexes():
    """Create all MongoDB indexes on startup."""

    # Prospects indexes (shared global pool — no account_id on prospects)
    await prospects_collection.create_indexes([
        IndexModel([("email", ASCENDING)], unique=True, sparse=True),
        IndexModel([("linkedin", ASCENDING)], unique=True, sparse=True),
        IndexModel([("company_id", ASCENDING)]),
        IndexModel([("stage", ASCENDING)]),
        IndexModel([("enrichment_status", ASCENDING)]),
        IndexModel([("source", ASCENDING)]),
        # Primary hot-filter index: industry + country + seniority (for ICP search)
        IndexModel([
            ("company_industry_id", ASCENDING),
            ("location.country_code", ASCENDING),
            ("seniority", ASCENDING),
        ]),
        # Regional variant
        IndexModel([
            ("company_industry_id", ASCENDING),
            ("location.region", ASCENDING),
            ("seniority", ASCENDING),
        ]),
        # Size-band filter
        IndexModel([("company_industry_id", ASCENDING), ("company_employee_band", ASCENDING)]),
        # Geo radius queries
        IndexModel([("geo", "2dsphere")]),
        # Dedup helpers
        IndexModel([("company_linkedin", ASCENDING)]),
        IndexModel([("company_domain", ASCENDING)]),
        # Scheduled outreach / cooldown signal
        IndexModel([("last_contacted_at", ASCENDING)]),
        IndexModel([("scheduled_outreach_date", ASCENDING)]),
        # Quality gates
        IndexModel([("prefilter_status", ASCENDING)]),
    ])

    # Search runs indexes
    await search_runs_collection.create_indexes([
        IndexModel([("status", ASCENDING)]),
        IndexModel([("started_at", DESCENDING)]),
    ])

    # Enrichment runs indexes
    await enrichment_runs_collection.create_indexes([
        IndexModel([("status", ASCENDING)]),
        IndexModel([("started_at", DESCENDING)]),
    ])

    # Industries indexes
    # Note: unique is on (account_id, name) — not name alone — for multi-tenancy
    await industries_collection.create_indexes([
        IndexModel([("account_id", ASCENDING), ("name", ASCENDING)], unique=True),
        IndexModel([("is_active", ASCENDING)]),
        IndexModel([("scrape_enabled", ASCENDING)]),
        IndexModel([("scrape_day", ASCENDING)]),
    ])

    # Companies indexes (shared global pool — no account_id on companies)
    await companies_collection.create_indexes([
        IndexModel([("linkedin_url", ASCENDING)], unique=True, sparse=True),
        IndexModel([("domain", ASCENDING)], sparse=True),
        IndexModel([("name", ASCENDING)]),
        # Canonical industry sub-document indexes
        IndexModel([("industry.id", ASCENDING), ("location.country_code", ASCENDING)]),
        IndexModel([("industry.id", ASCENDING), ("employee_band", ASCENDING)]),
        IndexModel([("industry.group", ASCENDING)]),
        # Geo radius
        IndexModel([("geo", "2dsphere")]),
        # Scrape helpers
        IndexModel([("last_scraped_at", ASCENDING)]),
    ])

    # Employees indexes (LEGACY — kept until migration drops the collection)
    await employees_collection.create_indexes([
        IndexModel([("linkedin_id", ASCENDING)], unique=True),
        IndexModel([("linkedin_url", ASCENDING)], unique=True, sparse=True),
        IndexModel([("company_id", ASCENDING)]),
        IndexModel([("prospect_id", ASCENDING)], sparse=True),
    ])

    # Conversations indexes
    await conversations_collection.create_indexes([
        IndexModel([("prospect_id", ASCENDING)]),
        IndexModel([("channel", ASCENDING)]),
        IndexModel([("is_read", ASCENDING)]),
        IndexModel([("last_message_at", DESCENDING)]),
        IndexModel([("unipile_chat_id", ASCENDING)], sparse=True),
        IndexModel([("prospect_email", ASCENDING)], sparse=True),
        IndexModel([("messages.provider_message_id", ASCENDING)], sparse=True),
        IndexModel([("messages.email_message_id", ASCENDING)], sparse=True),
        # A provider thread is unique only inside the tenant and connected
        # provider account that owns it. Build these after quarantining legacy
        # rows without provable ownership (see launch migration runbook).
        IndexModel(
            [
                ("account_id", ASCENDING),
                ("channel", ASCENDING),
                ("provider_account_id", ASCENDING),
                ("provider_thread_id", ASCENDING),
            ],
            unique=True,
            partialFilterExpression={
                "account_id": {"$type": "string"},
                "provider_account_id": {"$type": "string"},
                "provider_thread_id": {"$type": "string"},
            },
            name="uniq_conversation_provider_thread",
        ),
        IndexModel(
            [
                ("account_id", ASCENDING),
                ("channel", ASCENDING),
                ("provider_account_id", ASCENDING),
                ("messages.provider_message_id", ASCENDING),
            ],
            unique=True,
            partialFilterExpression={
                "messages.provider_message_id": {"$type": "string"},
            },
            name="uniq_conversation_provider_message",
        ),
        IndexModel(
            [
                ("account_id", ASCENDING),
                ("channel", ASCENDING),
                ("provider_account_id", ASCENDING),
                ("messages.unipile_message_id", ASCENDING),
            ],
            unique=True,
            partialFilterExpression={
                "messages.unipile_message_id": {"$type": "string"},
            },
            name="uniq_conversation_unipile_message",
        ),
        IndexModel(
            [
                ("account_id", ASCENDING),
                ("channel", ASCENDING),
                ("is_read", ASCENDING),
                ("last_message_at", DESCENDING),
            ],
            name="conversation_inbox_idx",
        ),
        IndexModel(
            [
                ("account_id", ASCENDING),
                ("prospect_id", ASCENDING),
                ("last_message_at", DESCENDING),
            ],
            name="conversation_tenant_prospect_idx",
        ),
    ])

    # Outreach schedules indexes
    await outreach_schedules_collection.create_indexes([
        IndexModel([("schedule_date", ASCENDING)], unique=True),  # One per day
        IndexModel([("status", ASCENDING)]),
        IndexModel([("created_at", DESCENDING)]),
    ])

    # Notifications indexes
    await notifications_collection.create_indexes([
        IndexModel([("is_read", ASCENDING), ("created_at", DESCENDING)]),
        IndexModel([("created_at", DESCENDING)]),
        IndexModel([("type", ASCENDING)]),
        IndexModel([("prospect_id", ASCENDING)], sparse=True),
    ])

    # System prompts are tenant overrides. Remove the legacy global slug
    # uniqueness before creating the canonical tenant+slug identity.
    prompt_indexes = await system_prompts_collection.index_information()
    for index_name, spec in prompt_indexes.items():
        keys = spec.get("key") or []
        if index_name != "_id_" and keys == [("slug", 1)] and spec.get("unique"):
            await system_prompts_collection.drop_index(index_name)
    await system_prompts_collection.create_indexes([
        IndexModel(
            [("account_id", ASCENDING), ("slug", ASCENDING)],
            unique=True,
            name="uniq_system_prompt_tenant_slug",
        ),
    ])

    # ------------------------------------------------------------------
    # Multi-tenancy indexes (new for OutFlo)
    # ------------------------------------------------------------------

    # Users
    await users_collection.create_indexes([
        IndexModel([("email", ASCENDING)], unique=True),
    ])

    # Accounts
    await accounts_collection.create_indexes([
        IndexModel([("slug", ASCENDING)], unique=True),
    ])

    # Account members
    await account_members_collection.create_indexes([
        IndexModel([("account_id", ASCENDING), ("user_id", ASCENDING)], unique=True),
        IndexModel([("account_id", ASCENDING)]),
        IndexModel([("user_id", ASCENDING)]),
    ])

    # Company profiles
    await company_profiles_collection.create_indexes([
        IndexModel([("account_id", ASCENDING)], unique=True),
    ])

    # Email accounts
    await email_accounts_collection.create_indexes([
        IndexModel([("account_id", ASCENDING)]),
        IndexModel([("account_id", ASCENDING), ("email", ASCENDING)], unique=True),
    ])

    # LinkedIn accounts
    await linkedin_accounts_collection.create_indexes([
        IndexModel([("account_id", ASCENDING)]),
        IndexModel([("unipile_account_id", ASCENDING)], unique=True),
        IndexModel(
            [("account_id", ASCENDING), ("is_default", ASCENDING), ("unipile_status", ASCENDING)],
            name="linkedin_sender_fallback",
        ),
    ])

    # LinkedIn connection requests
    await linkedin_connection_requests_collection.create_indexes([
        IndexModel([("account_id", ASCENDING)]),
        IndexModel([("account_id", ASCENDING), ("status", ASCENDING)]),
        IndexModel([("recipient_provider_id", ASCENDING)]),
        IndexModel([("unipile_invitation_id", ASCENDING)], sparse=True),
    ])

    # LinkedIn hosted-auth requests — TTL auto-delete once expires_at passes
    await linkedin_auth_requests_collection.create_indexes([
        IndexModel([("nonce", ASCENDING)], unique=True),
        IndexModel([("account_id", ASCENDING)]),
        IndexModel([("expires_at", ASCENDING)], expireAfterSeconds=0),
    ])

    # Campaigns
    await campaigns_collection.create_indexes([
        IndexModel([("account_id", ASCENDING)]),
        IndexModel([("account_id", ASCENDING), ("status", ASCENDING)]),
        IndexModel([("account_id", ASCENDING), ("type", ASCENDING)]),
        # Stuck-campaigns admin query (routes/admin_system.py::get_stuck_campaigns):
        # top-level $or over transitional discovery_status / message_gen_status,
        # each ANDed with an updated_at range + sort. One index per $or branch
        # lets the planner satisfy the $or without a collection scan.
        IndexModel([("discovery_status", ASCENDING), ("updated_at", ASCENDING)]),
        IndexModel([("message_gen_status", ASCENDING), ("updated_at", ASCENDING)]),
    ])

    # Campaign enrollments — critical scheduler index + smart campaign indexes
    await campaign_enrollments_collection.create_indexes([
        IndexModel([("campaign_id", ASCENDING)]),
        IndexModel([("account_id", ASCENDING)]),
        IndexModel([("prospect_id", ASCENDING)]),
        IndexModel([("account_id", ASCENDING), ("status", ASCENDING), ("next_action_at", ASCENDING)]),
        IndexModel([("campaign_id", ASCENDING), ("next_action_at", ASCENDING)]),
        # Smart campaign indexes
        IndexModel([("campaign_id", ASCENDING), ("message_gen_status", ASCENDING)]),
        IndexModel([("campaign_id", ASCENDING), ("smart_campaign_channel", ASCENDING)]),
        IndexModel([("campaign_id", ASCENDING), ("smart_campaign_send_day", ASCENDING)]),
        # Cross-campaign dedup (MW-3): find prospects enrolled in other active campaigns
        IndexModel([("account_id", ASCENDING), ("prospect_id", ASCENDING), ("status", ASCENDING)]),
        IndexModel(
            [
                ("status", ASCENDING),
                ("next_action_at", ASCENDING),
                ("execution_lease_expires_at", ASCENDING),
                ("_id", ASCENDING),
            ],
            partialFilterExpression={"status": "active"},
            name="campaign_enrollment_due_lease",
        ),
    ])

    # prospect_state indexes (per-tenant overlay — the new multi-tenancy layer)
    await prospect_state_collection.create_indexes([
        # Primary lookup: unique per (account, prospect)
        IndexModel([("account_id", ASCENDING), ("prospect_id", ASCENDING)], unique=True),
        # Per-tenant status / scoring queries
        IndexModel([("account_id", ASCENDING), ("status", ASCENDING)]),
        IndexModel([("account_id", ASCENDING), ("priority_tier", ASCENDING), ("ai_score", DESCENDING)]),
        # used_by queries (ownership / 90-day cooldown)
        IndexModel([("account_id", ASCENDING), ("used_by.user_id", ASCENDING), ("used_by.enrolled_at", ASCENDING)]),
        IndexModel([("account_id", ASCENDING), ("used_by.campaign_id", ASCENDING)]),
    ])

    # Campaign-relative qualification must never be written to the shared
    # prospect pool or reused between campaigns.
    await campaign_prospect_state_collection.create_indexes([
        IndexModel(
            [
                ("account_id", ASCENDING),
                ("campaign_id", ASCENDING),
                ("prospect_id", ASCENDING),
                ("scoring_version", ASCENDING),
            ],
            unique=True,
            name="campaign_prospect_score_version_unique_idx",
        ),
        IndexModel(
            [
                ("account_id", ASCENDING),
                ("campaign_id", ASCENDING),
                ("cohort_id", ASCENDING),
                ("enrichment.state", ASCENDING),
            ],
            name="campaign_prospect_cohort_work_idx",
        ),
        IndexModel(
            [
                ("account_id", ASCENDING),
                ("campaign_id", ASCENDING),
                ("scoring_version", ASCENDING),
                ("score.value", DESCENDING),
            ],
            name="campaign_prospect_score_read_idx",
        ),
    ])

    # Campaign messages
    # provider_message_id is sparse (not unique at index level — backfilled messages have null)
    await campaign_messages_collection.create_indexes([
        IndexModel([("campaign_id", ASCENDING)]),
        IndexModel([("campaign_enrollment_id", ASCENDING)]),
        IndexModel([("account_id", ASCENDING)]),
        IndexModel([("prospect_id", ASCENDING)]),
        IndexModel([("provider_message_id", ASCENDING)], sparse=True),
        IndexModel([("email_account_id", ASCENDING)], sparse=True),
        IndexModel([("provider_thread_id", ASCENDING)], sparse=True),
        IndexModel(
            [("processed_reply_keys", ASCENDING)],
            unique=True,
            partialFilterExpression={"processed_reply_keys": {"$exists": True}},
            name="uniq_processed_reply_key",
        ),
        IndexModel(
            [("send_key", ASCENDING)],
            unique=True,
            partialFilterExpression={"send_key": {"$type": "string"}},
            name="uniq_campaign_message_send_key",
        ),
    ])

    # Campaign daily schedules
    await campaign_daily_schedules_collection.create_indexes([
        IndexModel([("campaign_id", ASCENDING), ("schedule_date", ASCENDING)], unique=True),
        IndexModel([("account_id", ASCENDING), ("schedule_date", ASCENDING)]),
        IndexModel([("account_id", ASCENDING), ("status", ASCENDING)]),
    ])

    # Campaign schedule items
    await campaign_schedule_items_collection.create_indexes([
        IndexModel([("schedule_id", ASCENDING)]),
        IndexModel([("campaign_id", ASCENDING)]),
        IndexModel([("enrollment_id", ASCENDING)]),
        IndexModel([("prospect_id", ASCENDING)]),
    ])

    # Campaign daily stats
    await campaign_daily_stats_collection.create_indexes([
        IndexModel([("campaign_id", ASCENDING), ("date", ASCENDING)], unique=True),
        IndexModel([("account_id", ASCENDING), ("date", ASCENDING)]),
    ])

    # Sourced companies (curated discovery mode)
    await sourced_companies_collection.create_indexes([
        IndexModel([("account_id", ASCENDING), ("campaign_id", ASCENDING)]),
        IndexModel([("campaign_id", ASCENDING), ("user_excluded", ASCENDING)]),
        IndexModel([("campaign_id", ASCENDING), ("employee_scrape_status", ASCENDING)]),
        IndexModel([("campaign_id", ASCENDING), ("company_linkedin_url", ASCENDING)], sparse=True),
    ])

    # Lead upload batches (BYOL) — tenant-scoped, newest-first listing
    await lead_upload_batches_collection.create_indexes([
        IndexModel([("account_id", ASCENDING), ("created_at", DESCENDING)]),
    ])

    # Prospect stats counts
    await prospect_stats_counts_collection.create_indexes([
        IndexModel([("account_id", ASCENDING)], unique=True),
    ])

    # Remove the legacy `(account_id, email)` unique constraint before creating
    # the canonical typed-identifier index below. A missing email is stored as
    # null, so retaining this index permits only one domain/LinkedIn/prospect-id
    # suppression per tenant and can silently defeat do-not-contact intent.
    suppression_indexes = await suppressions_collection.index_information()
    if "account_id_1_email_1" in suppression_indexes:
        await suppressions_collection.drop_index("account_id_1_email_1")
        logger.info("Dropped legacy account/email suppression index")

    # Click tracking tokens
    await click_tracking_tokens_collection.create_indexes([
        IndexModel([("token", ASCENDING)], unique=True),
        IndexModel([("prospect_id", ASCENDING)]),
        IndexModel([("campaign_id", ASCENDING)], sparse=True),
        IndexModel([("created_at", DESCENDING)]),
    ])

    # Daily usage counters
    await daily_usage_counters_collection.create_index(
        [("date", ASCENDING), ("linkedin_account_id", ASCENDING)], unique=True
    )

    # ------------------------------------------------------------------
    # Additional performance indexes on existing collections (multi-tenancy)
    # ------------------------------------------------------------------
    # (prospects are now global — no account_id compound indexes needed on prospects)

    await industries_collection.create_indexes([
        IndexModel([("account_id", ASCENDING)]),
        IndexModel([("account_id", ASCENDING), ("is_active", ASCENDING)]),
    ])

    await conversations_collection.create_indexes([
        IndexModel([("account_id", ASCENDING)]),
        IndexModel([("account_id", ASCENDING), ("is_read", ASCENDING)]),
    ])

    await notifications_collection.create_indexes([
        IndexModel([("account_id", ASCENDING)]),
        IndexModel([("account_id", ASCENDING), ("is_read", ASCENDING)]),
    ])

    await search_runs_collection.create_indexes([
        IndexModel([("account_id", ASCENDING)]),
    ])

    await enrichment_runs_collection.create_indexes([
        IndexModel([("account_id", ASCENDING)]),
        IndexModel([("account_id", ASCENDING), ("status", ASCENDING)]),
    ])

    # ------------------------------------------------------------------
    # Reference collection indexes
    # ------------------------------------------------------------------

    # industries_taxonomy (shared canonical ~150-entry taxonomy)
    await industries_taxonomy_collection.create_indexes([
        IndexModel([("industry_id", ASCENDING)], unique=True),
        IndexModel([("group", ASCENDING)]),
    ])

    # geo_places: RETIRED as a Mongo collection — the gazetteer now lives in a
    # local SQLite file (data/geo_places.sqlite, built by scripts/build_geo_sqlite.py)
    # and is queried via services/geo_resolver.py. No indexes to create.

    # NOTE: Atlas Search and Atlas Vector Search indexes are created via the Atlas
    # UI or Admin API (mongosh createSearchIndex), NOT via Motor/pymongo.
    # Definitions:
    #   prospects_search  — Atlas Search on prospects (title, seniority, industry, location, geo)
    #   companies_search  — Atlas Search on companies (name, description, industry, location)
    #   prospects_vec     — Atlas Vector Search on prospects.title_vec (768-dim cosine,
    #                        filter fields: company_industry_id, location.country_code,
    #                        location.region, seniority, company_employee_band, stage)
    #   companies_vec     — Atlas Vector Search on companies.profile_vec (768-dim cosine,
    #                        filter fields: industry.id, industry.group, location.country_code,
    #                        employee_band)

    # Password reset tokens — TTL auto-delete after 1h
    await password_reset_tokens_collection.create_indexes([
        IndexModel([("token", ASCENDING)], unique=True),
        IndexModel([("user_id", ASCENDING)]),
        IndexModel([("expires_at", ASCENDING)], expireAfterSeconds=0),
    ])

    # apify_usage
    await apify_usage_collection.create_index([("account_id", ASCENDING), ("started_at", DESCENDING)])
    await apify_usage_collection.create_index([("campaign_id", ASCENDING), ("actor_id", ASCENDING)])

    # openrouter_usage
    await openrouter_usage_collection.create_index([("account_id", ASCENDING), ("requested_at", DESCENDING)])
    await openrouter_usage_collection.create_index([("campaign_id", ASCENDING), ("feature", ASCENDING)])
    await openrouter_usage_collection.create_index([("model", ASCENDING), ("requested_at", DESCENDING)])

    # ------------------------------------------------------------------
    # Agentic outreach indexes
    # ------------------------------------------------------------------

    # Conversations: classifier poller index (runs every 20s — must be fast)
    await conversations_collection.create_indexes([
        IndexModel(
            [("needs_classification", ASCENDING), ("scheduled_reply_at", ASCENDING)],
            sparse=True,
            name="classifier_poller_idx",
        ),
    ])

    # Suppressions: unique per account + identifier
    # Replace existing partial suppression index with the full composite key
    await suppressions_collection.create_indexes([
        IndexModel(
            [("account_id", ASCENDING), ("identifier_type", ASCENDING), ("identifier", ASCENDING)],
            unique=True,
            name="suppressions_unique_idx",
        ),
        IndexModel([("account_id", ASCENDING), ("identifier", ASCENDING)]),
    ])

    # Meetings
    await meetings_collection.create_indexes([
        IndexModel([("account_id", ASCENDING), ("status", ASCENDING)]),
        IndexModel([("account_id", ASCENDING), ("enrollment_id", ASCENDING)]),
        IndexModel([("prospect_id", ASCENDING)]),
        IndexModel([("booking_token", ASCENDING)], sparse=True, unique=True),
        IndexModel(
            [("proposal_key", ASCENDING)],
            unique=True,
            partialFilterExpression={"proposal_key": {"$type": "string"}},
            name="meeting_proposal_key_unique_idx",
        ),
        IndexModel(
            [
                ("account_id", ASCENDING),
                ("calendar_provider", ASCENDING),
                ("calendar_provider_account_id", ASCENDING),
                ("calendar_id", ASCENDING),
                ("calendar_event_id", ASCENDING),
            ],
            unique=True,
            partialFilterExpression={
                "calendar_provider_account_id": {"$type": "string"},
                "calendar_id": {"$type": "string"},
                "calendar_event_id": {"$type": "string"},
            },
            name="meeting_calendar_event_binding_unique_idx",
        ),
        IndexModel(
            [
                ("status", ASCENDING),
                ("next_calendar_sync_at", ASCENDING),
                ("calendar_sync_lease_expires_at", ASCENDING),
                ("account_id", ASCENDING),
                ("calendar_provider_account_id", ASCENDING),
            ],
            partialFilterExpression={
                "calendar_provider": "google",
                "calendar_event_id": {"$type": "string"},
            },
            name="meeting_calendar_reconciliation_due_idx",
        ),
        IndexModel([("proposed_at", DESCENDING)]),
    ])

    # Google Calendar push notifications are authenticated by a random channel
    # token and then bound to the exact channel, resource, provider account and
    # OutFlo tenant that registered the watch.
    await calendar_webhook_channels_collection.create_indexes([
        IndexModel([("channel_id", ASCENDING)], unique=True, name="calendar_channel_id_unique_idx"),
        IndexModel(
            [("account_id", ASCENDING), ("provider", ASCENDING), ("status", ASCENDING)],
            name="calendar_channel_tenant_provider_idx",
        ),
        IndexModel(
            [("expires_at", ASCENDING)],
            expireAfterSeconds=0,
            name="calendar_channel_expiry_ttl_idx",
        ),
    ])

    # Reply classifications
    await reply_classifications_collection.create_indexes([
        IndexModel([("account_id", ASCENDING), ("classified_at", DESCENDING)]),
        IndexModel([("conversation_id", ASCENDING)]),
        IndexModel([("enrollment_id", ASCENDING)], sparse=True),
        IndexModel([("category", ASCENDING)]),
    ])

    # Onboarding sessions
    await onboarding_sessions_collection.create_indexes([
        IndexModel([("account_id", ASCENDING)], unique=True),
        IndexModel([("user_id", ASCENDING)]),
        IndexModel([("last_active_at", DESCENDING)]),
    ])

    # Onboarding scrape jobs
    await onboarding_scrape_jobs_collection.create_indexes([
        IndexModel([("session_id", ASCENDING)], unique=True),
        IndexModel([("account_id", ASCENDING)]),
        IndexModel([("status", ASCENDING)]),
    ])

    # ------------------------------------------------------------------
    # Superadmin / platform observability indexes
    # ------------------------------------------------------------------

    # growthtoolkit_usage (written by growthtoolkit_service; reported in /api/admin/usage/*)
    await growthtoolkit_usage_collection.create_indexes([
        IndexModel([("account_id", ASCENDING), ("created_at", DESCENDING)]),
        IndexModel([("endpoint", ASCENDING), ("created_at", DESCENDING)]),
    ])

    # webhook_log (lightweight delivery log written by routes/webhooks.py)
    await webhook_log_collection.create_indexes([
        IndexModel([("received_at", DESCENDING)]),
        IndexModel([("source", ASCENDING), ("received_at", DESCENDING)]),
    ])

    # admin_audit_log (every superadmin mutation)
    await admin_audit_log_collection.create_indexes([
        IndexModel([("created_at", DESCENDING)]),
        IndexModel([("admin_email", ASCENDING), ("created_at", DESCENDING)]),
    ])

    # scheduler_heartbeats (one doc per APScheduler job)
    await scheduler_heartbeats_collection.create_indexes([
        IndexModel([("job_id", ASCENDING)], unique=True),
    ])

    # system_settings (runtime flag overrides, key-unique)
    await system_settings_collection.create_indexes([
        IndexModel([("key", ASCENDING)], unique=True),
    ])

    # Durable jobs. Deterministic keys are idempotent within a tenant and job
    # type; keyless jobs are intentionally allowed to coexist.
    await jobs_collection.create_indexes([
        IndexModel(
            [("account_id", ASCENDING), ("job_type", ASCENDING), ("job_key", ASCENDING)],
            unique=True,
            partialFilterExpression={"job_key": {"$type": "string"}},
            name="jobs_deterministic_key_unique_idx",
        ),
        IndexModel(
            [
                ("account_id", ASCENDING),
                ("state", ASCENDING),
                ("priority", DESCENDING),
                ("available_at", ASCENDING),
                ("created_at", ASCENDING),
            ],
            name="jobs_claim_idx",
        ),
        IndexModel(
            [("account_id", ASCENDING), ("state", ASCENDING), ("lease_expires_at", ASCENDING)],
            name="jobs_expired_lease_idx",
        ),
        IndexModel(
            [("account_id", ASCENDING), ("job_type", ASCENDING), ("state", ASCENDING)],
            name="jobs_tenant_type_state_idx",
        ),
    ])

    # Provider-send outbox / reconciliation indexes. Rollout must create these
    # before multiple scheduler workers are enabled.
    await send_attempts_collection.create_indexes([
        IndexModel(
            [("send_key", ASCENDING)],
            unique=True,
            name="uniq_send_attempt_key",
        ),
        IndexModel(
            [
                ("enrollment_id", ASCENDING),
                ("sequence_version", ASCENDING),
                ("node_id", ASCENDING),
                ("generation", ASCENDING),
            ],
            unique=True,
            name="uniq_send_attempt_identity",
        ),
        IndexModel(
            [("state", ASCENDING), ("lease_expires_at", ASCENDING), ("updated_at", ASCENDING)],
            name="send_attempt_dispatch_reaper",
        ),
        IndexModel(
            [("account_id", ASCENDING), ("state", ASCENDING), ("available_at", ASCENDING)],
            name="send_attempt_retry_queue",
        ),
        IndexModel(
            [
                ("account_id", ASCENDING),
                ("provider", ASCENDING),
                ("provider_account_id", ASCENDING),
                ("provider_result.message_id", ASCENDING),
            ],
            partialFilterExpression={"provider_result.message_id": {"$type": "string"}},
            name="send_attempt_provider_reconcile",
        ),
    ])

    # SSE stream tickets expire on their own; nothing else queries them.
    await stream_tickets_collection.create_indexes([
        IndexModel(
            [("expires_at", ASCENDING)],
            expireAfterSeconds=0,
            name="stream_ticket_expiry_ttl_idx",
        ),
    ])

    # OAuth state nonces are tenant/provider-bound and short-lived. Consumers
    # must atomically set consumed_at while matching _id (JTI) + expires_at.
    await oauth_state_nonces_collection.create_indexes([
        IndexModel(
            [("expires_at", ASCENDING)],
            expireAfterSeconds=0,
            name="oauth_state_expiry_ttl_idx",
        ),
        IndexModel(
            [
                ("account_id", ASCENDING),
                ("provider", ASCENDING),
                ("created_at", DESCENDING),
            ],
            name="oauth_state_tenant_provider_idx",
        ),
    ])

    logger.info("MongoDB indexes created successfully")
