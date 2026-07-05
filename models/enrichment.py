from pydantic import BaseModel, Field
from typing import Optional
from datetime import datetime


class EnrichmentTriggerRequest(BaseModel):
    """Request to trigger the enrichment pipeline."""
    prospect_ids: Optional[list[str]] = None
    enrich_all_unenriched: bool = False
    filter_min_score: Optional[float] = None
    filter_status: Optional[str] = None
    max_prospects: int = Field(default=50, ge=1, le=200)
    skip_profile_scrape: bool = False
    skip_company_scrape: bool = False
    skip_ai_assessment: bool = False
    skip_outreach: bool = True  # outreach generation disabled by default
    skip_pre_enrichment_triage: bool = False
    force_re_enrich: bool = False


class EnrichmentRunDocument(BaseModel):
    """Enrichment run tracking document stored in MongoDB."""
    status: str = "running"  # running | completed | failed
    total_prospects: int = 0
    prospects_processed: int = 0
    prospects_skipped: int = 0
    prospects_failed: int = 0
    profiles_scraped: int = 0
    companies_scraped: int = 0
    companies_deduplicated: int = 0
    ai_assessments_done: int = 0
    outreach_generated: int = 0
    triage_decision_makers: int = 0
    triage_wrong_person: int = 0
    triage_contacts_discovered: int = 0
    prospect_ids: list[str] = Field(default_factory=list)
    started_at: datetime = Field(default_factory=datetime.utcnow)
    completed_at: Optional[datetime] = None
    current_step: str = "initializing"
    error: Optional[str] = None
    cost_estimate: Optional[dict] = None
    triggered_by: Optional[str] = None  # user _id string of who triggered this run


class EnrichmentRunResponse(EnrichmentRunDocument):
    """Enrichment run response with MongoDB _id as string."""
    id: str = Field(alias="_id")

    class Config:
        populate_by_name = True


class EnrichByIndustryRequest(BaseModel):
    """Request to enrich top prospects per industry."""
    industry_ids: Optional[list[str]] = None
    max_per_industry: int = Field(default=50, ge=1, le=200)
    min_score: float = Field(default=0, ge=0)
    skip_profile_scrape: bool = False
    skip_company_scrape: bool = False
    skip_ai_assessment: bool = False
    skip_outreach: bool = True  # outreach generation disabled by default
    skip_pre_enrichment_triage: bool = False


