from motor.motor_asyncio import AsyncIOMotorClient
from pymongo import IndexModel, ASCENDING, DESCENDING
from config import get_settings
import logging

logger = logging.getLogger(__name__)

settings = get_settings()

client = AsyncIOMotorClient(settings.mongodb_url)
db = client[settings.mongodb_database]

# Collection references
leads_collection = db["leads"]  # Keep for migration rollback
prospects_collection = db["prospects"]  # Shared global people pool (absorbs employees)
prospect_state_collection = db["prospect_state"]  # NEW: per-tenant overlay (account_id+prospect_id)
search_runs_collection = db["search_runs"]
industries_collection = db["industries"]  # Per-account scrape sources (unchanged)
industries_taxonomy_collection = db["industries_taxonomy"]  # NEW: ~150 canonical LinkedIn industries
geo_places_collection = db["geo_places"]  # NEW: GeoNames-derived location gazetteer
enrichment_runs_collection = db["enrichment_runs"]
employees_collection = db["employees"]  # LEGACY: kept until migration drops it
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

# Campaign system
campaigns_collection = db["campaigns"]
campaign_enrollments_collection = db["campaign_enrollments"]
campaign_messages_collection = db["campaign_messages"]
campaign_daily_schedules_collection = db["campaign_daily_schedules"]
campaign_schedule_items_collection = db["campaign_schedule_items"]
campaign_daily_stats_collection = db["campaign_daily_stats"]
sourced_companies_collection = db["sourced_companies"]

# Denormalized stats
prospect_stats_counts_collection = db["prospect_stats_counts"]
suppressions_collection = db["suppressions"]

# Email open tracking (for Google/Microsoft accounts)
email_tracking_tokens_collection = db["email_tracking_tokens"]

# Email click tracking (for Google/Microsoft/SMTP accounts)
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
reply_classifications_collection = db["reply_classifications"]
onboarding_sessions_collection = db["onboarding_sessions"]
onboarding_scrape_jobs_collection = db["onboarding_scrape_jobs"]


async def create_indexes():
    """Create all MongoDB indexes on startup."""

    # Leads indexes (keep for migration rollback)
    await leads_collection.create_indexes([
        IndexModel([("email", ASCENDING)], unique=True),
        IndexModel([("linkedin", ASCENDING)], unique=True, sparse=True),
        IndexModel([("lead_score", DESCENDING)]),
        IndexModel([("industry", ASCENDING)]),
        IndexModel([("status", ASCENDING)]),
        IndexModel([("country", ASCENDING)]),
        IndexModel([("seniority_level", ASCENDING)]),
        IndexModel([("company_domain", ASCENDING)]),
        IndexModel([("enrichment_status", ASCENDING)]),
        IndexModel([("ai_lead_score", DESCENDING)]),
    ])

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
        IndexModel([("freshness_check_status", ASCENDING)]),
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
        IndexModel([("linkedin_url", ASCENDING)], unique=True),
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
        IndexModel([("messages.sendgrid_message_id", ASCENDING)], sparse=True),
        IndexModel([("messages.email_message_id", ASCENDING)], sparse=True),
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

    # System prompts indexes
    await system_prompts_collection.create_indexes([
        IndexModel([("slug", ASCENDING)], unique=True),
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
    ])

    # LinkedIn connection requests
    await linkedin_connection_requests_collection.create_indexes([
        IndexModel([("account_id", ASCENDING)]),
        IndexModel([("account_id", ASCENDING), ("status", ASCENDING)]),
        IndexModel([("recipient_provider_id", ASCENDING)]),
        IndexModel([("unipile_invitation_id", ASCENDING)], sparse=True),
    ])

    # Campaigns
    await campaigns_collection.create_indexes([
        IndexModel([("account_id", ASCENDING)]),
        IndexModel([("account_id", ASCENDING), ("status", ASCENDING)]),
        IndexModel([("account_id", ASCENDING), ("type", ASCENDING)]),
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

    # Campaign messages
    # provider_message_id is sparse (not unique at index level — backfilled messages have null)
    await campaign_messages_collection.create_indexes([
        IndexModel([("campaign_id", ASCENDING)]),
        IndexModel([("campaign_enrollment_id", ASCENDING)]),
        IndexModel([("account_id", ASCENDING)]),
        IndexModel([("prospect_id", ASCENDING)]),
        IndexModel([("provider_message_id", ASCENDING)], sparse=True),
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

    # Prospect stats counts
    await prospect_stats_counts_collection.create_indexes([
        IndexModel([("account_id", ASCENDING)], unique=True),
    ])

    # Suppressions
    await suppressions_collection.create_indexes([
        IndexModel([("account_id", ASCENDING), ("email", ASCENDING)], unique=True),
    ])

    # Email tracking tokens (open pixel)
    await email_tracking_tokens_collection.create_indexes([
        IndexModel([("token", ASCENDING)], unique=True),
        IndexModel([("prospect_id", ASCENDING)]),
        IndexModel([("campaign_id", ASCENDING)], sparse=True),
        IndexModel([("created_at", DESCENDING)]),
    ])

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

    # geo_places (GeoNames-derived gazetteer)
    await geo_places_collection.create_indexes([
        IndexModel([("place_id", ASCENDING)], unique=True),
        IndexModel([("alt_names", ASCENDING)]),
        IndexModel([("country_code", ASCENDING)]),
        IndexModel([("location", "2dsphere")]),
        IndexModel([("country_code", ASCENDING), ("population", DESCENDING)]),
    ])

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
        IndexModel([("enrollment_id", ASCENDING)]),
        IndexModel([("prospect_id", ASCENDING)]),
        IndexModel([("booking_token", ASCENDING)], sparse=True, unique=True),
        IndexModel([("proposed_at", DESCENDING)]),
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

    logger.info("MongoDB indexes created successfully")
