"""
Daily Outreach Schedule models.
Stores daily batches of 45 prospects (20 email + 20 LinkedIn connection + 5 InMail)
with editable messages and scheduled send times.
"""

from pydantic import BaseModel, Field
from typing import Optional, Literal
from datetime import datetime
import uuid


class ScheduleItem(BaseModel):
    """Single prospect + message in the daily schedule."""
    item_id: str = Field(default_factory=lambda: str(uuid.uuid4()))
    prospect_id: str  # ObjectId ref
    account_id: Optional[str] = None  # Account that owns this schedule item
    # Denormalized for UI (no joins needed)
    prospect_name: Optional[str] = None
    prospect_email: Optional[str] = None
    prospect_company: Optional[str] = None
    prospect_linkedin: Optional[str] = None
    prospect_score: Optional[float] = None
    priority_tier: Optional[str] = None
    channel: Literal["email", "linkedin_connection", "linkedin_inmail"]
    # Editable message fields
    message_subject: Optional[str] = None  # email/InMail subject
    message_body: str = ""  # message content
    subject_variant_id: Optional[str] = None  # which A/B variant was used
    # Scheduling
    prospect_timezone: Optional[str] = None
    scheduled_send_time: Optional[datetime] = None  # UTC
    local_send_time_display: Optional[str] = None  # "10:23 AM EST" for UI
    # Execution
    send_status: str = "pending"  # pending | sent | failed | skipped
    sent_at: Optional[datetime] = None
    error_message: Optional[str] = None
    conversation_id: Optional[str] = None
    manually_added: bool = False
    manually_added_at: Optional[datetime] = None


class ChannelBatch(BaseModel):
    """Group of schedule items for a single channel."""
    channel: str
    quota: int
    items: list[ScheduleItem] = Field(default_factory=list)
    total_items: int = 0
    sent_count: int = 0
    failed_count: int = 0


class DailyOutreachSchedule(BaseModel):
    """Full daily schedule document stored in MongoDB."""
    schedule_date: str  # "YYYY-MM-DD", unique
    status: str = "draft"  # draft | approved | in_progress | completed | cancelled
    email_batch: ChannelBatch = Field(
        default_factory=lambda: ChannelBatch(channel="email", quota=20)
    )
    linkedin_connection_batch: ChannelBatch = Field(
        default_factory=lambda: ChannelBatch(channel="linkedin_connection", quota=20)
    )
    linkedin_inmail_batch: ChannelBatch = Field(
        default_factory=lambda: ChannelBatch(channel="linkedin_inmail", quota=5)
    )
    # Configurable quotas
    email_quota: int = 20
    linkedin_connection_quota: int = 20
    linkedin_inmail_quota: int = 5
    # Metadata
    generated_at: Optional[datetime] = None
    approved_at: Optional[datetime] = None
    execution_started_at: Optional[datetime] = None
    execution_completed_at: Optional[datetime] = None
    generated_by: Optional[str] = None
    approved_by: Optional[str] = None
    total_items: int = 0
    total_sent: int = 0
    total_failed: int = 0
    total_skipped: int = 0
    dm_triage_stats: Optional[dict] = None
    created_at: datetime = Field(default_factory=datetime.utcnow)


# ── Request/Response Models ──

class ScheduleGenerateRequest(BaseModel):
    """Request to generate a daily outreach schedule."""
    schedule_date: Optional[str] = None  # "YYYY-MM-DD", defaults to today
    email_quota: int = 20
    linkedin_connection_quota: int = 20
    linkedin_inmail_quota: int = 5
    force_regenerate: bool = False
    force_approve: bool = False  # Auto-approve and start execution immediately


class SwapItemRequest(BaseModel):
    """Request to swap a schedule item with a different prospect."""
    new_prospect_id: str
    channel: str  # must match the batch the item is in


class BulkAddRequest(BaseModel):
    """Request to add multiple prospects to a schedule at once."""
    items: list[dict]  # [{prospect_id: str, channel: str}, ...]
    schedule_date: Optional[str] = None  # defaults to today


class MessageEditRequest(BaseModel):
    """Request to edit a message in the schedule."""
    message_subject: Optional[str] = None
    message_body: Optional[str] = None
