"""
Campaign and CampaignStep models for OutFlo multi-step outreach sequences.
"""

from pydantic import BaseModel, Field, ConfigDict
from typing import Optional
from datetime import datetime


class SequenceCondition(BaseModel):
    """An outgoing edge/transition from a sequence step."""
    type: str  # "timeout" | "connection_accepted" | "replied" | "not_replied" | "default"
    next_step_id: str
    delay_minutes: int = 0  # for timeout conditions: how long to wait before transitioning


class SequenceStep(BaseModel):
    """A single node in the outreach sequence DAG."""
    step_id: str               # e.g. "s1", "s2", "s1a"
    name: str                  # human-readable label
    channel: str               # "linkedin" | "email"
    action: str                # "connection_request" | "cold_email" | "followup_email" |
                               # "inmail" | "linkedin_message" | "ai_respond" |
                               # "wait_for_reply" | "terminal"
    delay_minutes: int = 0     # wait BEFORE executing this step (after arriving)
    is_entry: bool = False     # True for the first step only
    is_terminal: bool = False  # True for end nodes
    terminal_reason: Optional[str] = None  # "not_responding" | "in_conversation"
    auto_respond: bool = True  # for ai_respond steps: auto-send vs draft mode
    edges: list["SequenceCondition"] = Field(default_factory=list)


class OutreachSequence(BaseModel):
    """Complete outreach sequence stored on a campaign."""
    steps: list[SequenceStep]
    ai_auto_respond: bool = True  # global default for AI response mode


# Rebuild forward references
SequenceStep.model_rebuild()


class CampaignStep(BaseModel):
    """A single step within a campaign sequence."""
    step_number: int
    name: str
    channel: str  # email/linkedin
    action: str  # email/connection_request/inmail/linkedin_message
    delay_days: int = 0
    trigger_condition: Optional[str] = None

    # Email fields
    subject_template: Optional[str] = None
    body_template: str
    is_html: bool = False

    # LinkedIn connection request
    connection_note_template: Optional[str] = None

    # LinkedIn InMail
    inmail_subject_template: Optional[str] = None
    inmail_body_template: Optional[str] = None


class CampaignBase(BaseModel):
    """Core campaign fields."""
    account_id: str
    name: str
    description: Optional[str] = None
    type: str = "custom"  # linkedin_only/email_only/custom
    status: str = "draft"  # draft/active/paused/completed/archived

    # Sequence
    steps: list[CampaignStep] = Field(default_factory=list)

    # Connected channel accounts
    email_account_id: Optional[str] = None
    linkedin_account_id: Optional[str] = None

    # Sending schedule
    daily_email_limit: int = 50
    daily_linkedin_limit: int = 20
    timezone: str = "America/New_York"
    send_days: list[str] = Field(
        default_factory=lambda: ["monday", "tuesday", "wednesday", "thursday", "friday"]
    )
    send_hour_start: int = 9
    send_hour_end: int = 17

    # ── Denormalized enrollment counters (atomic $inc) ──
    total_enrolled: int = 0
    active_count: int = 0
    completed_count: int = 0
    replied_count: int = 0
    bounced_count: int = 0
    opted_out_count: int = 0
    meetings_booked: int = 0

    # ── Email stats ──
    emails_sent: int = 0
    emails_delivered: int = 0
    emails_opened: int = 0
    emails_clicked: int = 0
    emails_replied: int = 0
    emails_bounced: int = 0

    # ── LinkedIn stats ──
    linkedin_connections_sent: int = 0
    linkedin_connections_accepted: int = 0
    linkedin_inmails_sent: int = 0
    linkedin_replies: int = 0

    created_by: Optional[str] = None

    # ── Smart Campaign Builder fields ──
    is_smart_campaign: bool = False

    # Discovery mode for smart campaigns
    discovery_mode: str = "curated"  # "curated" (AI-sourced company list + per-company employee scrape)
    curated_icp_prompt: Optional[str] = None  # free-text ICP description; if absent, synthesized from icp_* fields
    curated_company_count_target: int = Field(default=100, ge=10, le=500)  # target number of companies to source
    curated_companies_sourced: int = 0
    curated_companies_approved: int = 0
    curated_companies_scraped: int = 0
    prospect_count_target: int = 100  # 100–500 slider

    # ICP Target Market
    icp_industries: list[str] = Field(default_factory=list)
    icp_job_titles: list[str] = Field(default_factory=list)
    icp_seniority_levels: list[str] = Field(default_factory=list)
    icp_company_size_min: Optional[int] = None
    icp_company_size_max: Optional[int] = None
    icp_countries: list[str] = Field(default_factory=list)
    icp_apify_params: Optional[dict] = None  # raw ApifyParams from wizard Step 2

    # Message Style
    message_tone: str = "professional"  # professional|challenger|conversational|empathetic
    value_proposition: Optional[str] = None
    pain_point: Optional[str] = None
    cta_type: str = "book_call"  # book_call|reply|visit_link
    cta_url: Optional[str] = None

    # Per-channel message instructions (enrich-and-generate)
    per_channel_message_instructions: Optional[dict] = None
    # Shape: {"email": str, "linkedin_connection": str, "linkedin_inmail": str}
    send_empty_connection_request: bool = False

    # Outreach Sequence (v3 multi-step with conditional branching)
    outreach_sequence: Optional[dict] = None   # serialized OutreachSequence
    messaging_config: Optional[dict] = None    # per-channel tone settings

    # Discovery lifecycle
    discovery_status: str = "idle"  # idle|searching_db|running|scraping|enriching|scoring|generating_messages|completed|failed|sourcing_companies|awaiting_company_approval|scraping_employees
    discovery_started_at: Optional[datetime] = None
    discovery_completed_at: Optional[datetime] = None
    discovery_error: Optional[str] = None
    discovery_prospects_found: int = 0
    discovery_prospects_enrolled: int = 0
    discovery_companies_found: int = 0
    discovery_apify_triggered: bool = False

    # Message generation lifecycle
    message_gen_status: str = "idle"  # idle|running|completed|failed
    message_gen_started_at: Optional[datetime] = None
    message_gen_completed_at: Optional[datetime] = None
    message_gen_prospects_done: int = 0

    # Approval / launch state
    approval_status: str = "pending"  # pending|pending_review|approved|launched
    launched_at: Optional[datetime] = None
    launch_day1_date: Optional[str] = None  # "YYYY-MM-DD"

    # Auto-launch lifecycle (kept for backward compat with existing campaigns)
    auto_launch_status: Optional[str] = "idle"   # idle | running | completed | failed
    auto_launch_error: Optional[str] = None
    auto_launch_completed_at: Optional[datetime] = None

    # ── Follow-up flow engine fields ──
    follow_up_flow: Optional[dict] = None
    daily_caps: Optional[dict] = None  # {linkedin_connection:20, email:25, linkedin_inmail:5, linkedin_message:20}
    daily_caps_state: Optional[dict] = None  # {"YYYY-MM-DD": {linkedin_connection:0, email:0, linkedin_inmail:0, linkedin_message:0}}
    discovery_prospects_from_db: Optional[int] = 0
    discovery_prospects_from_apify: Optional[int] = 0
    discovery_prospects_eligible: Optional[int] = None  # post-cap, post-dedup cohort size
    discovery_prospects_planned: Optional[int] = None   # enrollments assigned channel+day
    icp_functional_departments: list[str] = Field(default_factory=list)
    icp_cities: list[str] = Field(default_factory=list)
    icp_funding_stages: list[str] = Field(default_factory=list)
    icp_revenue_min: Optional[str] = None
    icp_revenue_max: Optional[str] = None

    # Canonical ICP (computed by icp_canonicalizer)
    industry_ids: list[str] = Field(default_factory=list)
    country_codes: list[str] = Field(default_factory=list)
    seniorities: list[str] = Field(default_factory=list)
    employee_bands: list[str] = Field(default_factory=list)


class CampaignDocument(CampaignBase):
    """Full campaign document as stored in MongoDB."""
    created_at: datetime = Field(default_factory=datetime.utcnow)
    updated_at: datetime = Field(default_factory=datetime.utcnow)
    started_at: Optional[datetime] = None
    completed_at: Optional[datetime] = None


class CampaignResponse(CampaignDocument):
    """Campaign response with MongoDB _id as string."""
    id: str = Field(alias="_id")

    model_config = ConfigDict(populate_by_name=True)


class CampaignCreateRequest(BaseModel):
    """Request body for creating a new campaign."""
    name: str
    type: str = "custom"
    steps: list[CampaignStep] = Field(default_factory=list)
    description: Optional[str] = None
    email_account_id: Optional[str] = None
    linkedin_account_id: Optional[str] = None
    timezone: Optional[str] = None
    send_days: Optional[list[str]] = None
    send_hour_start: int = 9
    send_hour_end: int = 17
    daily_email_limit: int = 50
    daily_linkedin_limit: int = 20


class CampaignUpdateRequest(BaseModel):
    """Fields that can be updated on an existing campaign."""
    name: Optional[str] = None
    description: Optional[str] = None
    type: Optional[str] = None
    steps: Optional[list[CampaignStep]] = None
    status: Optional[str] = None
    email_account_id: Optional[str] = None
    linkedin_account_id: Optional[str] = None
    timezone: Optional[str] = None
    send_days: Optional[list[str]] = None
    send_hour_start: Optional[int] = None
    send_hour_end: Optional[int] = None
    daily_email_limit: Optional[int] = None
    daily_linkedin_limit: Optional[int] = None


class SmartCampaignCreateRequest(BaseModel):
    """Request body for creating a Smart Campaign (AI-powered prospect discovery + message generation)."""
    name: str
    type: str = "custom"
    description: Optional[str] = None
    discovery_mode: str = "curated"
    curated_icp_prompt: Optional[str] = None
    curated_company_count_target: int = Field(default=100, ge=10, le=500)
    email_account_id: Optional[str] = None
    linkedin_account_id: Optional[str] = None
    timezone: Optional[str] = None
    send_days: Optional[list[str]] = None
    send_hour_start: int = 9
    send_hour_end: int = 17

    # Prospect count target
    prospect_count_target: int = Field(100, ge=25, le=500)

    # ICP Target Market
    icp_industries: list[str] = Field(default_factory=list)
    icp_job_titles: list[str] = Field(default_factory=list)
    icp_seniority_levels: list[str] = Field(default_factory=list)
    icp_company_size_min: Optional[int] = None
    icp_company_size_max: Optional[int] = None
    icp_countries: list[str] = Field(default_factory=list)
    icp_apify_params: Optional[dict] = None
    icp_industry_label: Optional[str] = None
    icp_keywords: list[str] = Field(default_factory=list)
    icp_exclude_keywords: list[str] = Field(default_factory=list)
    icp_exclude_industries: list[str] = Field(default_factory=list)
    max_prospects_per_company: int = Field(2, ge=1, le=20)

    # Message Style
    message_tone: str = "professional"
    value_proposition: Optional[str] = None
    pain_point: Optional[str] = None
    cta_type: str = "book_call"
    cta_url: Optional[str] = None

    messaging_config: Optional[dict] = None
    message_guidance: Optional[dict] = None  # {email_guidance, connection_request_guidance, inmail_guidance}

    # Per-channel guidance read by the message generator prompt builder
    email_guidance: Optional[str] = None
    connection_request_guidance: Optional[str] = None
    inmail_guidance: Optional[str] = None

    icp_functional_departments: list[str] = Field(default_factory=list)
    icp_cities: list[str] = Field(default_factory=list)
    icp_funding_stages: list[str] = Field(default_factory=list)
    icp_revenue_min: Optional[str] = None
    icp_revenue_max: Optional[str] = None

    # Follow-up flow engine
    follow_up_flow: Optional[dict] = None

    # ── Discovery tuning knobs (optional; when absent, module constants apply) ─
    # These mirror the per-campaign doc fields read by run_fast_discovery so
    # callers (scripts, API, frontend advanced panel) can override the defaults.
    # None = use the production module default (no behaviour change).
    discovery_scrape_depth: Optional[int] = Field(
        default=None, ge=1, le=20,
        description="Max Apify employee profiles to scrape per company (default: 8)"
    )
    discovery_dropout_buffer: Optional[float] = Field(
        default=None, ge=1.0, le=5.0,
        description="Multiplier on prospect_count_target for Apify max_total_items (default: 2.5)"
    )
    discovery_enrollment_cap: Optional[int] = Field(
        default=None, ge=1, le=10,
        description="Max prospects enrolled per company (default: 3)"
    )
    discovery_sourcing_concurrency: Optional[int] = Field(
        default=None, ge=1, le=8,
        description="Max parallel Gemini company-sourcing calls (default: 1)"
    )
    discovery_enable_company_research: Optional[bool] = Field(
        default=None,
        description="Run news + competitor research per company (default: False)"
    )
    discovery_skip_message_gen: Optional[bool] = Field(
        default=None,
        description="Skip message generation; mark enrollments 'skipped' instead (default: False)"
    )
    # E2E test only — hard-gated to app_env != production
    discovery_mock_mode: Optional[bool] = Field(
        default=None,
        description="Use synthetic prospects instead of paid Apify/Gemini calls (test only)"
    )


class PrefillClarificationProgress(BaseModel):
    captured: int = 0
    total: int = 4


class PrefillClarificationOption(BaseModel):
    value: str
    label: str


class PrefillClarification(BaseModel):
    question: str
    field: str = ""
    widget: str = "text"
    options: list[PrefillClarificationOption] = Field(default_factory=list)
    allow_free_text: bool = True
    progress: PrefillClarificationProgress = Field(default_factory=PrefillClarificationProgress)


class PrefillResponse(BaseModel):
    needs_clarification: Optional[PrefillClarification] = None
    campaign: Optional[dict] = None
