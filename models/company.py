"""
Company models — shared canonical company knowledge base.

One company document per real-world company (deduped globally by linkedin_url).
No account_id — companies are shared across all tenants.
ICP-relative data (scores, intelligence) lives in the prospect_state overlay.
"""

from pydantic import BaseModel, Field
from typing import Optional
from datetime import datetime


class IndustryInfo(BaseModel):
    """Canonical industry classification (from industries_taxonomy)."""
    id: Optional[str] = None            # e.g. "software_development"
    label: Optional[str] = None         # e.g. "Software Development"
    group: Optional[str] = None         # e.g. "Technology, Information & Media"
    raw: Optional[str] = None           # original scraped string (audit trail)
    assign_method: Optional[str] = None # "alias" | "alias_substring" | "embedding" | "manual"
    confidence: Optional[float] = None  # cosine score when assign_method == "embedding"


class LocationInfo(BaseModel):
    """Canonical geo-resolved location (from geo_places / GeoNames)."""
    place_id: Optional[str] = None      # GeoNames geonameid as string
    city: Optional[str] = None
    region: Optional[str] = None        # state / province
    country: Optional[str] = None
    country_code: Optional[str] = None  # ISO-2 e.g. "US"  ← primary filter target
    continent: Optional[str] = None
    raw: Optional[str] = None           # original unresolved string


class GeoPoint(BaseModel):
    """GeoJSON Point for 2dsphere queries."""
    type: str = "Point"
    coordinates: list[float] = Field(default_factory=list)  # [longitude, latitude]


class WebsiteData(BaseModel):
    """Structured company website scrape (services/website_scraper_service.fetch_website_data)."""
    url: Optional[str] = None
    title: Optional[str] = None
    meta_desc: Optional[str] = None
    about_text: Optional[str] = None
    tech: list[str] = Field(default_factory=list)
    socials: dict = Field(default_factory=dict)
    scraped_at: Optional[datetime] = None


class CompanyBase(BaseModel):
    """Core company data from Apify scraper (shared, universal facts)."""

    # Identity
    linkedin_url: str                           # unique key (global dedup)
    linkedin_uid: Optional[str] = None          # numeric company id from LinkedIn
    name: Optional[str] = None

    # Web presence
    website: Optional[str] = None
    domain: Optional[str] = None               # e.g. "stripe.com"

    # Profile
    description: Optional[str] = None
    tagline: Optional[str] = None
    specialties: Optional[list[str]] = None

    # Size / signals (universal facts, not per-tenant)
    employee_count: Optional[int] = None
    employee_band: Optional[str] = None        # "1-10"|"11-50"|"51-200"|"201-1000"|"1000+"
    follower_count: Optional[int] = None
    founded_year: Optional[int] = None
    company_type: Optional[str] = None
    phone: Optional[str] = None

    # Canonical industry (resolved from raw string at ingest)
    industry: Optional[IndustryInfo] = None

    # Canonical location (resolved from raw HQ string at ingest)
    location: Optional[LocationInfo] = None
    geo: Optional[GeoPoint] = None             # GeoJSON Point for 2dsphere radius queries

    # Embedding (768-dim Gemini gemini-embedding-001)
    profile_vec: Optional[list[float]] = None
    profile_vec_text: Optional[str] = None     # exact text that was embedded (audit / re-embed)
    profile_vec_model: Optional[str] = None    # e.g. "gemini-embedding-001"
    profile_vec_at: Optional[datetime] = None

    # Raw data blobs
    linkedin_data: Optional[dict] = None       # raw Apify company page scrape
    website_data: Optional[WebsiteData] = None # structured website scrape (populated on enrichment)

    # Competitor cache (shared / tenant-agnostic)
    competitors: list[dict] = Field(default_factory=list)
    competitors_last_fetched: Optional[datetime] = None

    # Deep company research (services/company_research_service.deep_research_company).
    # Contract (do not drift — other services build against these exact keys):
    #   competitors:  [{name, website_url, linkedin_url, differentiation,
    #                   market_position, is_best_performer: bool, why_winning: str|None}]
    #   best_performer: {name: str, why_winning: str} | None
    #   news:         [{title, url, published_date, source, summary, sentiment}]
    #   buying_signals: [str]   — hiring surges, funding, expansion, tech adoption,
    #                             leadership changes, product launches
    #   funding:      {latest_round: str|None, amount: str|None, date: str|None,
    #                  investors: [str], summary: str|None}
    #   hiring_signals: [str]   — roles/teams actively being hired (buying intent)
    #   tech_stack:   [str]
    #   recent_launches: [str]  — products/features/markets launched, last 12 months
    #   company_posts: [{text, posted_at, url, reactions, comments}]
    #   status:       "ok" | "partial" | "failed"  — partial = some sections
    #                 empty due to error (see errors)
    #   errors:       [str]     — which section(s) failed and why
    #   fetched_at:   datetime  — bulk research reuses entries <30d old with status "ok"
    research: Optional[dict] = None


class CompanyDocument(CompanyBase):
    """Full company document as stored in MongoDB."""
    created_at: datetime = Field(default_factory=datetime.utcnow)
    last_updated_at: datetime = Field(default_factory=datetime.utcnow)
    last_scraped_at: Optional[datetime] = None

    class Config:
        populate_by_name = True


class CompanyResponse(CompanyDocument):
    """Company response with MongoDB _id as string."""
    id: str = Field(alias="_id")

    class Config:
        populate_by_name = True
