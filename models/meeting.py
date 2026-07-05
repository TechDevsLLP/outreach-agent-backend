"""
Meeting model - tracks proposed and booked meetings from AI reply flow.
"""

from pydantic import BaseModel, Field, ConfigDict
from typing import Optional
from datetime import datetime


class SlotProposal(BaseModel):
    """A single proposed time slot."""
    slot_id: str
    start_utc: datetime
    end_utc: datetime
    duration_minutes: int = 25
    confirmed: bool = False


class MeetingBase(BaseModel):
    """Core meeting fields."""
    account_id: str
    enrollment_id: str
    prospect_id: str
    conversation_id: str

    status: str = "proposed"
    # proposed|booked|cancelled|rescheduled|completed|no_show

    # Proposed slots (AI picks 3)
    proposed_slots: list[SlotProposal] = Field(default_factory=list)
    confirmed_slot: Optional[SlotProposal] = None

    # Calendar details (set after booking)
    calendar_provider: Optional[str] = None  # google|microsoft
    calendar_event_id: Optional[str] = None
    calendar_event_url: Optional[str] = None
    google_meet_link: Optional[str] = None

    # Booking page token (for public /book/[token] page)
    booking_token: Optional[str] = None
    booking_token_expires_at: Optional[datetime] = None

    # Meeting agenda (AI-generated from discovery_call_agenda)
    agenda: Optional[str] = None

    # Timing
    proposed_at: datetime = Field(default_factory=datetime.utcnow)
    booked_at: Optional[datetime] = None
    cancelled_at: Optional[datetime] = None

    # Tracking
    no_confirm_reminder_sent: bool = False
    cancellation_reason: Optional[str] = None


class MeetingDocument(MeetingBase):
    """Full meeting document as stored in MongoDB."""
    created_at: datetime = Field(default_factory=datetime.utcnow)
    updated_at: datetime = Field(default_factory=datetime.utcnow)


class MeetingResponse(MeetingDocument):
    """Meeting response with MongoDB _id as string."""
    id: str = Field(alias="_id")

    model_config = ConfigDict(populate_by_name=True)
