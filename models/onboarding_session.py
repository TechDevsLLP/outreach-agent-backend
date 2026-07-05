"""
OnboardingSession model - persists wizard state in MongoDB.
Replaces the in-memory _jobs dict in onboarding_analyzer_service.py.
"""

from pydantic import BaseModel, Field, ConfigDict
from typing import Optional, Any
from datetime import datetime


class OnboardingStageData(BaseModel):
    """Data captured in a single wizard stage."""
    stage_number: int
    started_at: Optional[datetime] = None
    completed_at: Optional[datetime] = None
    data: dict = Field(default_factory=dict)  # stage-specific structured output
    status: str = "pending"  # pending|in_progress|completed|skipped


class OnboardingSessionBase(BaseModel):
    """Core onboarding session fields."""
    account_id: str
    user_id: str

    # Which stage the user is currently on (0=not started, 6=refinement)
    current_stage: int = 0
    completed: bool = False
    completed_at: Optional[datetime] = None

    # Stage-by-stage data blobs
    stages: list[OnboardingStageData] = Field(default_factory=list)

    # Stage 1: Company analysis
    company_url: Optional[str] = None
    company_analysis_job_id: Optional[str] = None  # Apify run ID
    company_analysis_result: Optional[dict] = None

    # Stage 2: Sender voice
    sender_linkedin_url: Optional[str] = None
    linkedin_scrape_job_id: Optional[str] = None
    voice_profile_draft: Optional[dict] = None

    # Stage 6: Refinement chat
    refinement_chat_history: list[dict] = Field(default_factory=list)
    # [{role: "user"|"assistant", content: str, timestamp: datetime}]
    refinement_captured: dict = Field(default_factory=dict)
    # {objections: [], competitors: [], banned_phrases: [], cta: str, agenda: str}

    # Session metadata
    last_active_at: datetime = Field(default_factory=datetime.utcnow)


class OnboardingSessionDocument(OnboardingSessionBase):
    """Full document as stored in MongoDB."""
    created_at: datetime = Field(default_factory=datetime.utcnow)
    updated_at: datetime = Field(default_factory=datetime.utcnow)


class OnboardingSessionResponse(OnboardingSessionDocument):
    """Response with MongoDB _id as string."""
    id: str = Field(alias="_id")

    model_config = ConfigDict(populate_by_name=True)
