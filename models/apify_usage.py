from datetime import datetime, timezone
from typing import Optional
from pydantic import BaseModel, Field


class ApifyUsageCreate(BaseModel):
    account_id: Optional[str] = None
    campaign_id: Optional[str] = None
    actor_id: str
    run_id: str = ""
    started_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
    finished_at: Optional[datetime] = None
    items_returned: int = 0
    cost_usd: float = 0.0
    status: str = "running"  # "succeeded"|"failed"|"timeout"|"running"
    metadata: dict = Field(default_factory=dict)


class ApifyUsageDocument(ApifyUsageCreate):
    id: Optional[str] = None
