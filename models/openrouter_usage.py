from datetime import datetime, timezone
from typing import Optional
from pydantic import BaseModel, Field


class OpenRouterUsageCreate(BaseModel):
    account_id: Optional[str] = None
    campaign_id: Optional[str] = None
    prospect_id: Optional[str] = None
    feature: str = "other"  # "prefilter"|"assessment"|"outreach"|"industry_param_generator"|"other"
    model: str
    prompt_tokens: int = 0
    completion_tokens: int = 0
    total_tokens: int = 0
    cost_usd: float = 0.0
    duration_ms: int = 0
    status: str = "succeeded"  # "succeeded"|"failed"|"rate_limited"
    requested_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))


class OpenRouterUsageDocument(OpenRouterUsageCreate):
    id: Optional[str] = None
