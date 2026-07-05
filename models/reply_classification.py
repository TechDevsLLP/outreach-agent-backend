"""
ReplyClassification model - stores classifier output per inbound message.
Stored in a separate collection so classifications can be reprocessed on prompt updates.
"""

from pydantic import BaseModel, Field, ConfigDict
from typing import Optional
from datetime import datetime

# The 6 categories the classifier can assign
REPLY_CATEGORIES = [
    "POSITIVE",
    "QUESTION",
    "SOFT_OBJECTION",
    "HARD_OBJECTION",
    "OOO",
    "UNSUBSCRIBE",
]


class ClassificationSignals(BaseModel):
    """Structured signals extracted alongside the category."""
    sentiment: Optional[str] = None           # positive|neutral|negative
    ooo_return_date: Optional[str] = None     # ISO date string if parseable
    unsubscribe_phrase: Optional[str] = None  # exact phrase if found
    objection_category: Optional[str] = None  # pricing|timing|authority|need|trust
    objection_phrasing: Optional[str] = None  # verbatim objection text
    hostility_score: float = 0.0              # 0.0–1.0; ≥0.6 routes HARD_OBJECTION to human


class ReplyClassificationBase(BaseModel):
    """Core classification record."""
    account_id: str
    conversation_id: str
    enrollment_id: Optional[str] = None
    prospect_id: Optional[str] = None

    # Message that was classified
    message_text: str
    message_direction: str = "inbound"
    channel: str  # email|linkedin

    # Classifier output
    category: str  # one of REPLY_CATEGORIES
    confidence: float  # 0.0–1.0
    signals: Optional[ClassificationSignals] = None

    # Which prompt version produced this
    prompt_slug: str = "reply_classifier_v1"

    # Whether this classification was overridden by a human
    human_override: bool = False
    human_override_category: Optional[str] = None
    human_override_at: Optional[datetime] = None
    human_override_by: Optional[str] = None

    # Whether the handler has been triggered
    handler_triggered: bool = False
    handler_triggered_at: Optional[datetime] = None
    auto_reply_sent: bool = False


class ReplyClassificationDocument(ReplyClassificationBase):
    """Full document as stored in MongoDB."""
    classified_at: datetime = Field(default_factory=datetime.utcnow)


class ReplyClassificationResponse(ReplyClassificationDocument):
    """Response with MongoDB _id as string."""
    id: str = Field(alias="_id")

    model_config = ConfigDict(populate_by_name=True)
