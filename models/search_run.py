from pydantic import BaseModel, Field
from typing import Optional
from datetime import datetime


class SearchRunCreate(BaseModel):
    """Input to trigger a search run."""
    industry_id: Optional[str] = None  # Use an existing industry
    # OR provide custom params directly:
    custom_params: Optional[dict] = None
    fetch_count: int = 100
    start_page: Optional[int] = None  # If None, auto-calculates next page from previous runs


class SearchRunDocument(BaseModel):
    """Search run as stored in MongoDB."""
    industry_name: str
    region_name: Optional[str] = None
    apify_run_id: Optional[str] = None
    input_params: dict = Field(default_factory=dict)
    status: str = "running"  # running, completed, failed
    total_fetched: int = 0
    new_prospects: int = 0
    duplicates_skipped: int = 0
    updated_prospects: int = 0
    started_at: datetime = Field(default_factory=datetime.utcnow)
    completed_at: Optional[datetime] = None
    error: Optional[str] = None

    # Phase 3: Pagination tracking
    page_number: Optional[int] = None
    is_final_page: bool = False


class SearchRunResponse(SearchRunDocument):
    """Search run response with MongoDB _id as string."""
    id: str = Field(alias="_id")

    class Config:
        populate_by_name = True


class BulkSearchRequest(BaseModel):
    """Run multiple industries in one request."""
    industry_ids: list[str] = Field(default_factory=list)
    run_all_active: bool = False
    fetch_count: int = 100
