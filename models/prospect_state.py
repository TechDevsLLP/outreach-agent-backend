"""
Per-tenant prospect overlay.

One ProspectState document per (account_id, prospect_id) pair.
Holds all ICP-relative, tenant-specific data about a prospect:
  - AI scoring (computed against THIS tenant's ICP)
  - Prospect intelligence (generated for THIS tenant's product)
  - Outreach messages (generated for THIS tenant's voice/ICP)
  - Status (opted_out, replied, etc. — per-tenant tracking)
  - used_by: lightweight ownership/exclusion marker (array of enrollment refs)
"""

from pydantic import BaseModel, Field, ConfigDict
from typing import Optional
from datetime import datetime


class UsedByEntry(BaseModel):
    """Lightweight enrollment reference — tracks which campaign/user owns this prospect."""
    user_id: str
    campaign_id: str
    status: str = "active"  # mirrors enrollment status: active|paused|completed|opted_out|etc.
    enrolled_at: datetime = Field(default_factory=datetime.utcnow)


class ProspectStateBase(BaseModel):
    """Core per-tenant prospect overlay fields stored in MongoDB."""

    account_id: str  # ALWAYS stored as string from ObjectId (no ObjectId/str inconsistency)
    prospect_id: str  # FK to prospects._id

    # AI scoring (computed vs THIS tenant's ICP)
    ai_score: Optional[float] = None  # was ai_prospect_score on prospect
    ai_score_breakdown: Optional[dict] = None
    prospect_score: Optional[float] = None  # deterministic rule-based score
    priority_tier: Optional[str] = None  # "hot" | "warm" | "cold"

    # Per-tenant intelligence & generated content
    prospect_intelligence: Optional[dict] = None
    outreach_messages: Optional[dict] = None  # OutreachMessages dict
    company_news: list[dict] = Field(default_factory=list)
    competitors: list[dict] = Field(default_factory=list)
    posts: list[dict] = Field(default_factory=list)

    # Per-tenant lifecycle
    status: str = "new"
    # new|replied|bounced|disqualified|opted_out|converted|
    # engaged_linkedin|engaged_email|meeting_booked|unsubscribed

    tags: list[str] = Field(default_factory=list)

    # Ownership / exclusion / cooldown
    used_by: list[UsedByEntry] = Field(default_factory=list)

    # Timestamps
    created_at: datetime = Field(default_factory=datetime.utcnow)
    last_updated_at: datetime = Field(default_factory=datetime.utcnow)


class ProspectStateDocument(ProspectStateBase):
    """Full ProspectState document as stored in MongoDB."""

    model_config = ConfigDict(populate_by_name=True)


class ProspectStateResponse(ProspectStateDocument):
    """ProspectState response with MongoDB _id as string."""

    id: str = Field(alias="_id")

    model_config = ConfigDict(populate_by_name=True)


class ProspectStateCreate(BaseModel):
    """Minimal fields required to create a new ProspectState document."""

    account_id: str
    prospect_id: str
    status: str = "new"
    used_by: list[UsedByEntry] = Field(default_factory=list)


class ProspectStateUpdate(BaseModel):
    """Fields that can be updated on an existing ProspectState document."""

    ai_score: Optional[float] = None
    ai_score_breakdown: Optional[dict] = None
    prospect_score: Optional[float] = None
    priority_tier: Optional[str] = None
    prospect_intelligence: Optional[dict] = None
    outreach_messages: Optional[dict] = None
    company_news: Optional[list[dict]] = None
    competitors: Optional[list[dict]] = None
    posts: Optional[list[dict]] = None
    status: Optional[str] = None
    tags: Optional[list[str]] = None
