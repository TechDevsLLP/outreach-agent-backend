"""
Industry models - replaces icp_profile.py.
Each industry has AI-generated Apify params and 3 default regions (USA, Europe, India).
"""

from pydantic import BaseModel, Field
from typing import Optional
from datetime import datetime


class RegionConfig(BaseModel):
    """Configuration for a geographic scraping region."""
    name: str  # "usa", "europe", "india"
    locations: list[str]  # Apify contact_location values
    fetch_count: int  # Default allocation from total
    priority: int  # 1=usa, 2=europe, 3=india (for redistribution)
    is_exhausted: bool = False
    exhausted_at: Optional[datetime] = None
    pagination_state: dict = Field(default_factory=lambda: {
        "last_page_fetched": -1,
        "leads_finder_exhausted": False,
        "lead_scraper_exhausted": False,
        "lead_scraper_consecutive_zero_runs": 0,
    })


DEFAULT_REGIONS = [
    RegionConfig(
        name="usa",
        locations=["united states"],
        fetch_count=50,
        priority=1,
    ),
    RegionConfig(
        name="europe",
        locations=[
            "united kingdom", "germany", "france", "netherlands",
            "switzerland", "italy", "spain", "sweden", "norway",
            "denmark", "belgium", "austria", "ireland", "poland",
        ],
        fetch_count=30,
        priority=2,
    ),
    RegionConfig(
        name="india",
        locations=["india"],
        fetch_count=20,
        priority=3,
    ),
]


class ApifyBaseParams(BaseModel):
    """Apify parameters WITHOUT location fields - those come from regions."""
    contact_job_title: Optional[list[str]] = None
    contact_not_job_title: Optional[list[str]] = None
    seniority_level: Optional[list[str]] = None
    functional_level: Optional[list[str]] = None
    email_status: list[str] = Field(default=["validated"])
    company_domain: Optional[list[str]] = None
    size: Optional[list[str]] = None
    company_industry: Optional[list[str]] = None
    company_not_industry: Optional[list[str]] = None
    company_keywords: Optional[list[str]] = None
    company_not_keywords: Optional[list[str]] = None
    min_revenue: Optional[str] = None
    max_revenue: Optional[str] = None
    funding: Optional[list[str]] = None


class IndustryCreate(BaseModel):
    """Simplified industry creation - only name required. AI generates everything."""
    name: str


class IndustryCreateFull(BaseModel):
    """Manual industry creation with explicit params."""
    name: str
    description: str
    apify_base_params: ApifyBaseParams
    regions: Optional[list[RegionConfig]] = None
    total_fetch_count: int = 100
    is_active: bool = True


class IndustryUpdate(BaseModel):
    """Fields that can be updated on an industry."""
    name: Optional[str] = None
    description: Optional[str] = None
    is_active: Optional[bool] = None
    apify_base_params: Optional[ApifyBaseParams] = None
    regions: Optional[list[RegionConfig]] = None
    total_fetch_count: Optional[int] = None
    scrape_day: Optional[str] = None
    scrape_enabled: Optional[bool] = None


class IndustryDocument(BaseModel):
    """Industry document as stored in MongoDB."""
    name: str
    description: str = ""
    is_active: bool = True
    apify_base_params: dict = Field(default_factory=dict)
    regions: list[dict] = Field(default_factory=list)
    total_fetch_count: int = 100
    scrape_day: str = "saturday"
    scrape_enabled: bool = True
    created_at: datetime = Field(default_factory=datetime.utcnow)
    last_run_at: Optional[datetime] = None
    total_runs: int = 0
    total_prospects_generated: int = 0

    # AI generation tracking
    ai_generated: bool = False
    ai_generation_model: Optional[str] = None
    ai_generation_timestamp: Optional[datetime] = None
    user_edited_params: bool = False


class IndustryResponse(IndustryDocument):
    """Industry response with MongoDB _id as string."""
    id: str = Field(alias="_id")

    class Config:
        populate_by_name = True
