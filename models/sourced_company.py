"""Sourced company model — used by curated discovery mode."""
from pydantic import BaseModel, Field, ConfigDict
from typing import Optional
from datetime import datetime


class SourcedCompanyBase(BaseModel):
    campaign_id: str
    account_id: str
    company_name: str
    company_linkedin_url: Optional[str] = None
    company_domain: Optional[str] = None
    company_website: Optional[str] = None
    industry: Optional[str] = None
    country: Optional[str] = None
    employee_size_estimate: Optional[str] = None
    description: Optional[str] = None
    citation_urls: list[str] = Field(default_factory=list)
    source: str = "sonar_pro"

    user_excluded: bool = False

    linkedin_url_validated: bool = False
    employee_scrape_status: str = "pending"  # pending | running | completed | failed | skipped
    employee_scrape_error: Optional[str] = None
    employees_scraped_count: int = 0
    prospects_created_count: int = 0

    created_at: datetime = Field(default_factory=datetime.utcnow)
    updated_at: datetime = Field(default_factory=datetime.utcnow)


class SourcedCompanyDocument(SourcedCompanyBase):
    pass


class SourcedCompanyResponse(SourcedCompanyDocument):
    id: str = Field(alias="_id")

    model_config = ConfigDict(populate_by_name=True)
