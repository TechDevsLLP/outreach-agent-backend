from functools import lru_cache
from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class ProductionConfigurationError(RuntimeError):
    """Raised before startup when the production security contract is invalid."""


class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
    )

    # MongoDB
    mongodb_url: str
    mongodb_database: str = "outflo_v3"

    # API Keys
    apify_api_key: str
    openrouter_api_key: str = ""
    openrouter_base_url: str = "https://openrouter.ai/api/v1"
    gemini_api_key: str = ""
    unipile_token: str = ""  # Unipile LinkedIn automation
    unipile_base_url: str = "https://api29.unipile.com:15901/api/v1"
    sender_email: str = ""  # Set in .env — required for sending
    sender_name: str = ""   # Set in .env — required for sending
    reply_to_email: str = ""  # Inbound parse address (e.g., reply@parse.mentopreneur.com)
    unipile_webhook_secret: str = ""  # Unipile webhook verification
    # Webhook auth hardening hooks (see routes/webhooks.py):
    #  - unipile_webhook_signature_header: name of the HMAC signature header
    #    Unipile sends (documented hook; adjust if Unipile's scheme differs).
    #  - unipile_webhook_require_hmac: when True, ONLY a valid HMAC-SHA256
    #    signature is accepted and the plain shared-secret header is rejected.
    #    Default False keeps backward compatibility (accept either) during the
    #    transition from the plain-secret scheme to HMAC.
    unipile_webhook_signature_header: str = "X-Unipile-Signature"
    unipile_webhook_require_hmac: bool = False

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
    apify_company_scraper_id: str = "UwSdACBp7ymaGUJjS"
    apify_post_scraper_actor_id: str = "r4oNX7IHlW4RQAjKP"
    # Company-page post scraper (member post actor can't take /company/ URLs).
    # Called by slug; override with APIFY_COMPANY_POSTS_ACTOR_ID. Fail-soft —
    # any error yields empty post lists, never breaks the research pipeline.
    apify_company_posts_actor_id: str = "WI0tj4Ieb5Kq458gB"
    growthtoolkit_api_key: str = ""
    growthtoolkit_base_url: str = "https://api.appconnector.pro"
    max_prospects_per_company: int = 2
    enrolled_target_first_campaign: int = 135

    # Scraping concurrency
    apify_actor_concurrency_limit: int = 6   # Max concurrent Apify actor calls globally

    # Enrichment
    company_scrape_batch_size: int = 10
    ai_concurrency_limit: int = 10
    ai_assessment_concurrency_limit: int = 6  # Phase 3 AI assessment semaphore (paid Haiku tier)
    # Legacy process-local recovery path. Durable Mongo jobs supersede it;
    # production validation requires it disabled.
    enrichment_startup_sweep_enabled: bool = False

    # Contact discovery thresholds (Phase 3.5)
    contact_discovery_company_threshold: float = 60.0
    contact_discovery_prospect_threshold: float = 40.0

    # Pre-enrichment triage (Phase 0.5)
    pre_enrichment_triage_enabled: bool = True
    pre_enrichment_company_fit_threshold: float = 30.0

    # Daily outreach schedule
    daily_email_quota: int = 20
    daily_linkedin_connection_quota: int = 20
    daily_linkedin_inmail_quota: int = 5

    # Auth
    jwt_secret_key: str
    jwt_algorithm: str = "HS256"
    jwt_expiry_minutes: int = 1440  # 24 hours
    # Local HTTP development keeps this false. Production forces Secure in
    # auth._cookie_secure regardless of this override.
    session_cookie_secure: bool = False
    # Super admin (platform-level) — single email allowlist
    super_admin_email: str | None = None  # set SUPER_ADMIN_EMAIL in .env

    # Frontend (for CORS)
    frontend_url: str = "http://localhost:3000"
    cors_origins: str = ""  # comma-separated extra origins, e.g. "http://localhost:3000,https://app.outflo.io"
    trusted_hosts: str = "localhost,127.0.0.1,testserver"
    max_request_bytes: int = 10 * 1024 * 1024

    # Backend public URL (used for tracking pixel URLs)
    backend_base_url: str = "http://localhost:8002"

    # Public API base URL (used for calendar webhook registration, etc.)
    api_base_url: str = "https://api.outflo.io"

    # Google OAuth (per-user email accounts)
    google_client_id: str = ""
    google_client_secret: str = ""
    google_redirect_uri: str = ""

    # Zoho Mail OAuth (per-user email accounts)
    zoho_client_id: str = ""
    zoho_client_secret: str = ""
    zoho_redirect_uri: str = ""

    # Credential encryption at rest (Fernet key — generate with
    # `python -c "from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())"`)
    encryption_key: str = ""

    # Quality gates
    quality_gates_enabled: bool = True
    title_gate_enabled: bool = True
    prefilter_gate_enabled: bool = True

    # Internal campaign scorer (replaces per-prospect AI calls during discovery)
    min_score_to_enroll: int = 55             # prospects below this fit_score are not enrolled
    score_raw_pool_multiplier: float = 3.0    # top-up: scrape up to target × this before giving up
    score_topup_max_iterations: int = 5       # top-up: max Apify retry rounds

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

@lru_cache()
def get_settings() -> Settings:
    return Settings()


def validate_production_settings(settings: Settings) -> None:
    """Fail closed on missing launch-critical production configuration."""
    if settings.app_env.strip().lower() != "production":
        return

    errors: list[str] = []

    def required(name: str, value: str) -> None:
        normalized = (value or "").strip()
        if not normalized or normalized.lower() in {
            "change-me",
            "change-me-to-a-random-secret",
            "replace-me",
        }:
            errors.append(f"{name} is required and cannot use a placeholder")

    required("JWT_SECRET_KEY", settings.jwt_secret_key)
    required("ENCRYPTION_KEY", settings.encryption_key)
    required("OPENROUTER_API_KEY", settings.openrouter_api_key)
    required("APIFY_API_KEY", settings.apify_api_key)
    required("GROWTHTOOLKIT_API_KEY", settings.growthtoolkit_api_key)
    required("UNIPILE_TOKEN", settings.unipile_token)
    required("UNIPILE_WEBHOOK_SECRET", settings.unipile_webhook_secret)
    required("GOOGLE_CLIENT_ID", settings.google_client_id)
    required("GOOGLE_CLIENT_SECRET", settings.google_client_secret)
    required("GOOGLE_REDIRECT_URI", settings.google_redirect_uri)

    if len((settings.jwt_secret_key or "").encode("utf-8")) < 32:
        errors.append("JWT_SECRET_KEY must contain at least 32 bytes")
    if settings.debug:
        errors.append("DEBUG must be false in production")
    if settings.discovery_mock_mode:
        errors.append("DISCOVERY_MOCK_MODE must be false in production")
    if settings.enrichment_startup_sweep_enabled:
        errors.append("ENRICHMENT_STARTUP_SWEEP_ENABLED must be false in production")

    allowed_roles = {"web", "scheduler", "worker"}
    if settings.app_role not in allowed_roles:
        errors.append(
            "APP_ROLE must be web, scheduler, or worker in production; "
            "the combined all role is not deployment-safe"
        )

    for name, value in {
        "FRONTEND_URL": settings.frontend_url,
        "BACKEND_BASE_URL": settings.backend_base_url,
        "API_BASE_URL": settings.api_base_url,
        "GOOGLE_REDIRECT_URI": settings.google_redirect_uri,
    }.items():
        if not (value or "").startswith("https://"):
            errors.append(f"{name} must use https:// in production")

    origins = [
        origin.strip()
        for origin in (settings.cors_origins or settings.frontend_url).split(",")
        if origin.strip()
    ]
    if not origins or any(origin == "*" or not origin.startswith("https://") for origin in origins):
        errors.append("CORS_ORIGINS must contain only explicit HTTPS origins")

    trusted_hosts = [
        host.strip() for host in settings.trusted_hosts.split(",") if host.strip()
    ]
    if not trusted_hosts or any(host == "*" for host in trusted_hosts):
        errors.append("TRUSTED_HOSTS must contain only explicit hostnames")
    if settings.max_request_bytes <= 0:
        errors.append("MAX_REQUEST_BYTES must be greater than zero")

    try:
        from cryptography.fernet import Fernet

        Fernet((settings.encryption_key or "").encode("utf-8"))
    except Exception:
        errors.append("ENCRYPTION_KEY must be a valid Fernet key")

    if errors:
        raise ProductionConfigurationError(
            "Invalid production configuration: " + "; ".join(errors)
        )
