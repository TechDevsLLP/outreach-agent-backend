from functools import lru_cache
from pydantic import Field
from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    # MongoDB
    mongodb_url: str
    mongodb_database: str = "LeadAutomation_v2"

    # API Keys
    apify_api_key: str
    openrouter_api_key: str = ""
    openrouter_base_url: str = "https://openrouter.ai/api/v1"
    gemini_api_key: str = ""
    unipile_token: str = ""  # Unipile LinkedIn automation
    unipile_base_url: str = "https://api29.unipile.com:15901/api/v1"
    sendgrid_api_key: str = ""  # Phase 6.4: Email sending
    sendgrid_webhook_secret: str = ""  # Phase 6.4: Inbound parse webhook verification (kept for inbound handler)
    sendgrid_verification_public_key: str = ""  # ECDSA public key from SendGrid Event Webhook settings page
    sender_email: str = ""  # Set in .env — required for sending
    sender_name: str = ""   # Set in .env — required for sending
    reply_to_email: str = ""  # Inbound parse address (e.g., reply@parse.mentopreneur.com)
    unipile_webhook_secret: str = ""  # Unipile webhook verification

    # AI Models
    assessment_model: str = "anthropic/claude-haiku-4-5"
    outreach_model: str = "google/gemini-2.5-flash"
    claude_model: str = "anthropic/claude-sonnet-4-5"  # alias kept for backward-compat
    fallback_free_model: str = "nvidia/nemotron-3-nano-30b-a3b:free"  # last-resort only
    fallback_models: list[str] = [
        "google/gemini-2.5-flash",
        "meta-llama/llama-4-scout:free",
        "meta-llama/llama-4-maverick:free",
        "nvidia/nemotron-3-nano-30b-a3b:free",
    ]
    mini_enrichment_model: str = "anthropic/claude-haiku-4-5"  # fast + cheap for campaign mini-enrichment

    # Application
    app_env: str = "development"
    app_role: str = "all"  # "all" = web+scheduler (local dev), "web" = API only, "scheduler" = cron worker only
    debug: bool = True
    log_level: str = "INFO"

    # Apify
    apify_actor_id: str = "IoSHqwTR9YGhzccez"  # LEADS_FINDER — runs in parallel with Lead Scraper for diversified yield (both actors are active)
    apify_lead_scraper_id: str = "T1XDXWc1L92AfIJtd"  # Apollo-style lead scraper
    apify_profile_scraper_id: str = "LpVuK3Zozwuipa5bp"
    apify_company_scraper_id: str = "UwSdACBp7ymaGUJjS"
    apify_email_finder_actor_id: str = "TthkVR0ZjJt8gbtRy"
    apify_post_scraper_actor_id: str = "r4oNX7IHlW4RQAjKP"
    apify_bulk_email_finder_actor_id: str = "ddgw2oGFaH645BFAq"
    max_prospects_per_company: int = 2
    enrolled_target_first_campaign: int = 135
    enrolled_target_floor: int = Field(default=105, description="Minimum acceptable enrolled prospects for first campaign")

    # Scraping concurrency
    industry_concurrency_limit: int = 3      # Max industries scraped in parallel
    apify_actor_concurrency_limit: int = 6   # Max concurrent Apify actor calls globally

    # Enrichment
    enrichment_batch_size: int = 50
    company_scrape_batch_size: int = 10
    ai_concurrency_limit: int = 10
    ai_assessment_concurrency_limit: int = 6  # Phase 3 AI assessment semaphore (paid Haiku tier)
    scrape_delay_seconds: int = 10
    enrichment_startup_sweep_enabled: bool = True  # Detect stuck "running" runs on boot

    # Auto-enrichment
    auto_enrich_enabled: bool = True
    auto_enrich_threshold: float = 60.0
    auto_enrich_max_batch: int = 50

    # Auto-discover contacts
    auto_discover_contacts_enabled: bool = False
    auto_discover_contacts_threshold: float = 80.0

    # Contact discovery thresholds (Phase 3.5)
    contact_discovery_company_threshold: float = 60.0
    contact_discovery_prospect_threshold: float = 40.0

    # Pre-enrichment triage (Phase 0.5)
    pre_enrichment_triage_enabled: bool = True
    pre_enrichment_company_fit_threshold: float = 30.0

    # Schedule DM triage
    schedule_dm_triage_enabled: bool = True

    # Scheduler
    scrape_day_of_week: str = "sat"
    scrape_hour: int = 6
    scrape_minute: int = 0

    # Daily outreach schedule
    daily_email_quota: int = 20
    daily_linkedin_connection_quota: int = 20
    daily_linkedin_inmail_quota: int = 5
    outreach_office_hours_start: int = 9   # 9 AM local
    outreach_office_hours_end: int = 17    # 5 PM local
    outreach_send_jitter_minutes: int = 15

    # Auth
    jwt_secret_key: str
    jwt_algorithm: str = "HS256"
    jwt_expiry_minutes: int = 1440  # 24 hours
    # Deprecated: single-user admin credentials — kept for backward compatibility
    # only. New code should use users stored in MongoDB.
    admin_email: str = ""
    admin_password_hash: str = ""
    # Super admin (platform-level) — single email allowlist
    super_admin_email: str | None = None  # set SUPER_ADMIN_EMAIL in .env

    # Frontend (for CORS)
    frontend_url: str = "http://localhost:3000"
    cors_origins: str = ""  # comma-separated extra origins, e.g. "http://localhost:3000,https://app.outflo.io"

    # Backend public URL (used for tracking pixel URLs)
    backend_base_url: str = "http://localhost:8002"

    # Public API base URL (used for calendar webhook registration, etc.)
    api_base_url: str = "https://api.outflo.io"

    # Google OAuth (per-user email accounts)
    google_client_id: str = ""
    google_client_secret: str = ""
    google_redirect_uri: str = ""

    # Microsoft OAuth (per-user email accounts)
    microsoft_client_id: str = ""
    microsoft_client_secret: str = ""
    microsoft_redirect_uri: str = ""

    # Quality gates
    quality_gates_enabled: bool = True
    title_gate_enabled: bool = True
    freshness_gate_enabled: bool = True
    prefilter_gate_enabled: bool = True

    # AI prefilter
    prefilter_model: str = "google/gemini-3-flash-preview"
    prefilter_batch_size: int = 20
    prefilter_concurrency: int = 4
    prefilter_confidence_threshold: float = 0.25

    # Freshness check
    freshness_short_mode: bool = True
    freshness_concurrency: int = 8

    # Internal campaign scorer (replaces per-prospect AI calls during discovery)
    min_score_to_enroll: int = 55             # prospects below this fit_score are not enrolled
    score_raw_pool_multiplier: float = 3.0    # top-up: scrape up to target × this before giving up
    score_topup_max_iterations: int = 5       # top-up: max Apify retry rounds
    enable_ai_assessment_fallback: bool = False   # if True, run AI on a sample for calibration
    ai_assessment_sample_rate: float = 0.05       # fraction assessed by AI when fallback enabled

    # Discovery logging
    discovery_log_dir: str = "logs/campaigns"

    # Cost tracking
    cost_tracking_enabled: bool = True

    # E2E / test mock mode — skip paid Apify + Gemini calls in discovery.
    # NEVER set True in production.  Controlled per-campaign via discovery_mock_mode
    # field on the campaign doc (only honoured when app_env != "production").
    discovery_mock_mode: bool = False

    # OpenRouter price map: model → {prompt, completion} $/1M tokens
    openrouter_price_map: dict = Field(default_factory=lambda: {
        "google/gemini-3-flash-preview": {"prompt": 0.075, "completion": 0.30},
        "google/gemini-2.5-flash": {"prompt": 0.15, "completion": 0.60},
        "anthropic/claude-haiku-4-5": {"prompt": 0.80, "completion": 4.00},
        "anthropic/claude-sonnet-4-5": {"prompt": 3.00, "completion": 15.00},
        # Direct Gemini SDK calls (company sourcing + embeddings)
        "gemini-3.1-flash-lite": {"prompt": 0.075, "completion": 0.30},
        "gemini-embedding-001": {"prompt": 0.0001, "completion": 0.0},
        # Perplexity via OpenRouter (competitor + news research)
        "perplexity/sonar-pro": {"prompt": 3.00, "completion": 15.00},
    })

    class Config:
        env_file = ".env"
        env_file_encoding = "utf-8"
        extra = "ignore"


@lru_cache()
def get_settings() -> Settings:
    return Settings()
