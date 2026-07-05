"""
CampaignEnrollment models - tracks a prospect's progress through a campaign sequence.
"""

from pydantic import BaseModel, Field, ConfigDict
from typing import Optional
from datetime import datetime


class StepHistoryItem(BaseModel):
    """Record of a single step execution for an enrolled prospect."""
    step_number: int
    executed_at: datetime
    channel: str
    action: str
    status: str
    response_at: Optional[datetime] = None


class EnrollmentBase(BaseModel):
    """Core enrollment fields."""
    campaign_id: str
    account_id: str
    prospect_id: str
    status: str = "enrolled"
    # enrolled|active|paused|completed|opted_out|bounced|replied|converted
    # meeting_proposed|meeting_booked|paused_ooo|awaiting_human
    current_step: int = 0
    next_action_at: Optional[datetime] = None
    step_history: list[StepHistoryItem] = Field(default_factory=list)
    enrolled_at: datetime = Field(default_factory=datetime.utcnow)
    completed_at: Optional[datetime] = None
    last_activity_at: Optional[datetime] = None

    # ── Smart Campaign fields (None on classic enrollments — safe discriminator) ──
    smart_campaign_channel: Optional[str] = None  # email|linkedin_connection|linkedin_inmail
    smart_campaign_send_day: Optional[int] = None  # 1-based day number in campaign schedule
    smart_campaign_scheduled_utc: Optional[datetime] = None  # exact UTC send time computed at launch

    # Generated messages (stored at generation time, replaces step templates for smart campaigns)
    generated_messages: Optional[dict] = None
    # Shape: {
    #   "cold_email": {"subject_a": "...", "subject_b": "...", "body": "..."},
    #   "linkedin_connection": {"note": "..."},  # ALWAYS <= 280 chars
    #   "linkedin_inmail": {"subject": "...", "body": "..."}
    # }

    message_gen_status: str = "pending"  # pending|done|failed
    message_gen_error: Optional[str] = None

    # ── Flow engine state fields ──
    flow_state: Optional[dict] = None
    # shape: {current_node_id: str, history: [{node_id, event, at}], started_at: datetime, entry_channel: str}
    message_drafts: Optional[dict] = None
    # shape: {node_id: {channel, subject?, body, status: "pending_review"|"approved"|"sent"|"failed", generated_at, scheduled_date}}

    # ── Agentic outreach fields ──
    classification_history: list[dict] = Field(default_factory=list)
    last_classification: Optional[dict] = None
    # {category, confidence, signals, classified_at, conversation_id}
    meeting_id: Optional[str] = None
    ooo_resume_at: Optional[datetime] = None
    escalated_to_human: bool = False
    escalated_reason: Optional[str] = None
    aggression_preset_override: Optional[str] = None
    assigned_sender_id: Optional[str] = None  # multi-sender thread continuity
    cascade_group_id: Optional[str] = None    # links the 3 prospects from the same company
    cascade_status: Optional[str] = None      # "primary"|"secondary"|"tertiary"


class EnrollmentDocument(EnrollmentBase):
    """Full enrollment document as stored in MongoDB."""
    # enrolled_at already has default_factory in EnrollmentBase;
    # no additional fields required beyond the base.
    pass


class EnrollmentResponse(EnrollmentDocument):
    """Enrollment response with MongoDB _id as string."""
    id: str = Field(alias="_id")

    model_config = ConfigDict(populate_by_name=True)


class BulkEnrollRequest(BaseModel):
    """Request body for bulk-enrolling prospects into a campaign."""
    campaign_id: str
    prospect_ids: list[str]
    start_immediately: bool = True
