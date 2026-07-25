"""
CompanyProfile model - per-account ICP and sender context used for AI personalization.
"""

from pydantic import BaseModel, Field, ConfigDict
from typing import Optional
from datetime import datetime


class CaseStudy(BaseModel):
    """A single client case study / proof point."""
    client: str
    outcome: str
    metric: Optional[str] = None
    industry: Optional[str] = None


class ScoringWeights(BaseModel):
    """Weights used when scoring prospects against this account's ICP."""
    seniority: float = 30.0
    company_size: float = 25.0
    industry_match: float = 20.0
    verified_email: float = 15.0
    funding_stage: float = 10.0
    keywords: list[str] = Field(default_factory=list)


class CompanyProfileBase(BaseModel):
    """Core company profile / ICP fields."""
    account_id: str
    user_id: str

    # Company identity
    company_name: str
    website_url: str
    description: str

    # Offering
    services: list[str] = Field(default_factory=list)
    industries: list[str] = Field(default_factory=list)
    target_market: str
    differentiators: list[str] = Field(default_factory=list)
    pain_points: list[str] = Field(default_factory=list)

    # Voice & tone
    tone_of_voice: str = "professional"  # professional/casual/bold/friendly

    # Social proof — stored as plain text strings
    case_studies: list[str] = Field(default_factory=list)

    # Sender context
    sender_name: str
    sender_role: str

    # Outreach strategy
    outreach_strategy: str = "email_first"  # email_first/linkedin_first/hybrid
    connection_request_guidance: Optional[str] = None
    email_guidance: Optional[str] = None
    inmail_guidance: Optional[str] = None

    # ICP definition
    icp_description: str

    # ICP target fields (from onboarding wizard)
    target_industries: list[str] = Field(default_factory=list)
    target_job_titles: list[str] = Field(default_factory=list)
    target_seniority: list[str] = Field(default_factory=list)
    target_geographies: list[str] = Field(default_factory=list)
    target_company_sizes: list[str] = Field(default_factory=list)
    target_revenue_range: Optional[str] = None

    # Canonical ICP (computed by icp_canonicalizer, persisted at onboarding Stage 3)
    industry_ids: list[str] = Field(default_factory=list)
    country_codes: list[str] = Field(default_factory=list)
    seniorities: list[str] = Field(default_factory=list)
    employee_bands: list[str] = Field(default_factory=list)
    # title_query_vec is Binary — not added here (BSON Binary not JSON-serializable)

    # Scoring weights
    scoring_weights: Optional[ScoringWeights] = None

    # ── Agentic outreach fields (added for full-autonomy mode) ──

    # Sender LinkedIn
    sender_linkedin_url: Optional[str] = None
    # Synthesized voice profile from LinkedIn post scrape
    sender_voice_profile: Optional[dict] = None
    # {tone_markers, sentence_patterns, vocab_signature, post_excerpts, synthesized_at, source, post_count}
    sender_linkedin_posts: list[dict] = Field(default_factory=list)  # capped 25, raw scrape

    # Objection & competitor banks
    objection_bank: list[dict] = Field(default_factory=list)
    # [{objection_id, phrasing, category, rebuttal_text, supporting_case_study_id?}]
    competitor_bank: list[dict] = Field(default_factory=list)
    # [{name, positioning, our_differentiator, common_objection_id}]

    # Messaging guardrails
    banned_phrases: list[str] = Field(default_factory=list)

    # Offer definition
    primary_cta: Optional[str] = None
    discovery_call_agenda: Optional[str] = None
    qualifier_questions: list[str] = Field(default_factory=list)

    # Aggression preset (account default; per-campaign override lives on Campaign)
    aggression_preset: str = "aggressive"  # aggressive|moderate|conservative

    # Onboarding wizard state
    onboarding_stage: int = 0  # 0..6, resume point
    onboarding_completed_at: Optional[datetime] = None


class CompanyProfileDocument(CompanyProfileBase):
    """Full company profile document as stored in MongoDB."""
    created_at: datetime = Field(default_factory=datetime.utcnow)
    updated_at: datetime = Field(default_factory=datetime.utcnow)


class CompanyProfileResponse(CompanyProfileDocument):
    """Company profile response with MongoDB _id as string."""
    id: str = Field(alias="_id")

    model_config = ConfigDict(populate_by_name=True)


class CompanyProfileUpsertRequest(BaseModel):
    """All fields are optional - used for both create and patch."""
    company_name: Optional[str] = None
    website_url: Optional[str] = None
    description: Optional[str] = None
    services: Optional[list[str]] = None
    industries: Optional[list[str]] = None
    target_market: Optional[str] = None
    differentiators: Optional[list[str]] = None
    pain_points: Optional[list[str]] = None
    tone_of_voice: Optional[str] = None
    case_studies: Optional[list[str]] = None
    sender_name: Optional[str] = None
    sender_role: Optional[str] = None
    outreach_strategy: Optional[str] = None
    connection_request_guidance: Optional[str] = None
    email_guidance: Optional[str] = None
    inmail_guidance: Optional[str] = None
    icp_description: Optional[str] = None
    scoring_weights: Optional[ScoringWeights] = None
    # Agentic outreach fields
    sender_linkedin_url: Optional[str] = None
    sender_voice_profile: Optional[dict] = None
    sender_linkedin_posts: Optional[list[dict]] = None
    objection_bank: Optional[list[dict]] = None
    competitor_bank: Optional[list[dict]] = None
    banned_phrases: Optional[list[str]] = None
    primary_cta: Optional[str] = None
    discovery_call_agenda: Optional[str] = None
    qualifier_questions: Optional[list[str]] = None
    aggression_preset: Optional[str] = None
    onboarding_stage: Optional[int] = None
    onboarding_completed_at: Optional[datetime] = None
    # ICP target fields (from onboarding wizard)
    target_industries: Optional[list[str]] = None
    target_job_titles: Optional[list[str]] = None
    target_seniority: Optional[list[str]] = None
    target_geographies: Optional[list[str]] = None
    target_company_sizes: Optional[list[str]] = None
    target_revenue_range: Optional[str] = None
    # Canonical ICP fields (written by icp_canonicalizer)
    industry_ids: Optional[list[str]] = None
    country_codes: Optional[list[str]] = None
    seniorities: Optional[list[str]] = None
    employee_bands: Optional[list[str]] = None
