"""
Prospect models — shared canonical people knowledge base.

One prospect document per real-world person (deduped globally by linkedin/email).
Absorbs the old employees collection: every scraped person is a prospect.
No account_id — prospects are shared across all tenants.

ICP-relative, tenant-specific data lives in models/prospect_state.py:
  - ai_score, prospect_intelligence, outreach_messages, status, used_by, tags, etc.
"""

from pydantic import BaseModel, Field
from typing import Optional
from datetime import datetime

from models.company import LocationInfo, GeoPoint  # shared geo models


# ---------------------------------------------------------------------------
# Sub-models used by prospect_state overlay (kept here for backward compat)
# ---------------------------------------------------------------------------

class CompanyNewsItem(BaseModel):
    """Single news article about the company."""
    title: str
    url: str
    published_date: Optional[str] = None
    source: str
    summary: Optional[str] = None
    sentiment: Optional[str] = None  # "positive" | "neutral" | "negative"


class CompetitorInfo(BaseModel):
    """Information about a competitor."""
    name: str
    website_url: Optional[str] = None
    linkedin_url: Optional[str] = None
    differentiation: str
    market_position: Optional[str] = None


class SubjectLineVariant(BaseModel):
    """A/B test variant for email subject line."""
    variant_id: str  # "A", "B", "C"
    subject: str
    is_selected: bool = False
    times_sent: int = 0
    times_opened: int = 0
    times_replied: int = 0
    open_rate: float = 0.0
    reply_rate: float = 0.0


class OutreachMessages(BaseModel):
    """Structured outreach messages with A/B testing."""
    cold_email: dict = Field(default_factory=dict)       # {"body": "...", "subject_variants": [...]}
    linkedin_connection_request: dict = Field(default_factory=dict)
    linkedin_followup: dict = Field(default_factory=dict)
    generated_at: datetime = Field(default_factory=datetime.utcnow)
    generation_model: str = "google/gemini-2.5-flash"


# ---------------------------------------------------------------------------
# Shared prospect document (universal facts only)
# ---------------------------------------------------------------------------

class ProspectBase(BaseModel):
    """
    Universal facts about a person — shared across all tenants.
    ICP-relative fields (ai_score, status, etc.) live in ProspectState overlay.
    """

    # Identity
    linkedin: Optional[str] = None          # unique sparse
    linkedin_uid: Optional[str] = None
    email: Optional[str] = None             # unique sparse
    personal_email: Optional[str] = None
    mobile_number: Optional[str] = None
    first_name: Optional[str] = None
    last_name: Optional[str] = None
    full_name: Optional[str] = None

    # Professional identity (universal facts)
    job_title: Optional[str] = None
    headline: Optional[str] = None
    seniority: Optional[str] = None         # canonical token: founder|c_suite|vp|director|manager|senior|mid|junior
    function: Optional[str] = None          # canonical dept: sales|marketing|engineering|product|finance|hr|legal|operations|data|design|support|partnerships

    # Title vector (768-dim Gemini embedding of job_title+headline+seniority+function)
    title_vec: Optional[list[float]] = None
    title_vec_text: Optional[str] = None    # exact text that was embedded (audit / re-embed)
    title_vec_model: Optional[str] = None   # e.g. "gemini-embedding-001"
    title_vec_at: Optional[datetime] = None

    # Company link + DENORMALIZED company filter facts
    # Denormalized so we can filter "SaaS prospects in USA" in a single indexed pass
    company_id: Optional[str] = None        # FK -> companies._id
    company_linkedin: Optional[str] = None  # denorm convenience
    company_domain: Optional[str] = None    # denorm
    company_name: Optional[str] = None      # denorm
    company_industry_id: Optional[str] = None    # denorm from companies.industry.id  ← hot filter
    company_industry_group: Optional[str] = None # denorm from companies.industry.group
    company_employee_band: Optional[str] = None  # denorm: "1-10"|"11-50"|"51-200"|"201-1000"|"1000+"
    company_size: Optional[int] = None           # denorm exact count
    company_description: Optional[str] = None    # kept for backward compat / keyword search

    # Canonical person location (resolved from raw city/state/country strings at ingest)
    location: Optional[LocationInfo] = None
    geo: Optional[GeoPoint] = None          # GeoJSON Point for 2dsphere radius queries

    # Raw blobs — person-scoped (kept inline; company blob moved to companies.linkedin_data)
    linkedin_profile_data: Optional[dict] = None  # raw Apify person profile scrape

    # Source tracking
    source: str = "search"                  # "search"|"employee_discovery"|"curated_discovery"|"manual"
    source_actor_id: Optional[str] = None
    discovered_from_prospect_id: Optional[str] = None
    discovery_reasoning: Optional[str] = None
    source_search_run_ids: list[str] = Field(default_factory=list)

    # Lifecycle stage (replaces separate employees collection)
    stage: str = "scraped"                  # "scraped"|"profile_enriched"|"company_enriched"|"assessed"|"contactable"

    # Enrichment tracking
    enrichment_status: str = "not_started" # not_started|profile_scraped|company_scraped|ai_assessed|completed|failed|in_progress
    enrichment_error: Optional[str] = None
    enrichment_started_at: Optional[datetime] = None
    enrichment_completed_at: Optional[datetime] = None
    enrichment_run_id: Optional[str] = None

    # Quality gate fields (universal — apply to any tenant using this person)
    freshness_check_status: Optional[str] = None  # "fresh"|"stale"|"skipped_no_linkedin"|"invalid_url"|"error"
    stale_employer: bool = False
    linkedin_snapshot: Optional[dict] = None  # {current_company, current_title, headline, scraped_at}
    prefilter_status: Optional[str] = None    # "passed"|"rejected"|"error"|"skipped"
    prefilter_confidence: Optional[float] = None
    prefilter_reject_reason: Optional[str] = None
    title_gate_status: Optional[str] = None   # "passed"|"rejected"
    disqualify_reason: Optional[str] = None

    # Connection tracking (shared — a connection is with the person, not per-tenant)
    connection_request_sent_at: Optional[datetime] = None
    connection_accepted_at: Optional[datetime] = None
    connection_followup_sent_at: Optional[datetime] = None
    connection_followup_message_id: Optional[str] = None

    # Global cooldown signal (newest contact across any tenant)
    last_contacted_at: Optional[datetime] = None
    # Scheduling sentinel (global: prevents double-pick across tenants)
    scheduled_outreach_date: Optional[str] = None  # ISO date

    # Timezone (from enrichment)
    timezone: Optional[str] = None
    optimal_send_time: Optional[datetime] = None


class ProspectDocument(ProspectBase):
    """Full prospect document as stored in MongoDB."""
    first_seen_at: datetime = Field(default_factory=datetime.utcnow)
    last_updated_at: datetime = Field(default_factory=datetime.utcnow)

    class Config:
        populate_by_name = True


class ProspectResponse(ProspectDocument):
    """Prospect response with MongoDB _id as string."""
    id: str = Field(alias="_id")

    class Config:
        populate_by_name = True


class ProspectUpdateRequest(BaseModel):
    """Fields that can be updated manually on the shared record."""
    # Most mutable fields live on prospect_state; only universal facts here
    stage: Optional[str] = None
    enrichment_status: Optional[str] = None
    timezone: Optional[str] = None
    scheduled_outreach_date: Optional[str] = None


class ProspectListParams(BaseModel):
    """Query parameters for listing prospects."""
    page: int = 1
    limit: int = 50
    search: Optional[str] = None
    company_industry_id: Optional[str] = None
    industry_group: Optional[str] = None
    country_code: Optional[str] = None
    region: Optional[str] = None
    seniority: Optional[str] = None
    employee_band: Optional[str] = None
    stage: Optional[str] = None
    enrichment_status: Optional[str] = None
