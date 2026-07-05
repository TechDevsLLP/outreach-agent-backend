"""
Suppression model - opt-out and bounce suppressions at identifier level.
"""

from pydantic import BaseModel, Field, ConfigDict
from typing import Optional
from datetime import datetime

SUPPRESSION_IDENTIFIER_TYPES = ["email", "linkedin_url", "prospect_id"]
SUPPRESSION_REASONS = ["unsubscribe", "bounce", "spam_report", "manual", "hard_objection"]


class SuppressionBase(BaseModel):
    """Core suppression record."""
    account_id: str

    # Identifier being suppressed
    identifier_type: str  # email|linkedin_url|prospect_id
    identifier: str       # the actual value (lowercased for email)

    reason: str           # unsubscribe|bounce|spam_report|manual|hard_objection
    source: str = "system"  # system|human

    # Optional context
    prospect_id: Optional[str] = None
    enrollment_id: Optional[str] = None
    conversation_id: Optional[str] = None
    notes: Optional[str] = None


class SuppressionDocument(SuppressionBase):
    """Full document as stored in MongoDB."""
    created_at: datetime = Field(default_factory=datetime.utcnow)


class SuppressionResponse(SuppressionDocument):
    """Response with MongoDB _id as string."""
    id: str = Field(alias="_id")

    model_config = ConfigDict(populate_by_name=True)
