"""
Conversation models for the unified communications system.
Messages embedded in conversation docs (not separate collection) -
conversations have tens of messages max, avoids joins, enables atomic updates.
"""

from pydantic import BaseModel, Field
from typing import Optional, Literal
from datetime import datetime
import uuid


class Message(BaseModel):
    """Single message within a conversation."""
    message_id: str = Field(default_factory=lambda: str(uuid.uuid4()))
    direction: Literal["inbound", "outbound"]
    content_text: Optional[str] = None
    content_html: Optional[str] = None
    subject: Optional[str] = None  # Email only
    timestamp: datetime = Field(default_factory=datetime.utcnow)
    status: str = "sent"  # sent/delivered/opened/clicked/bounced/failed/received

    # Channel-specific IDs
    provider: Optional[str] = None  # google/zoho/smtp (email) or unipile (linkedin)
    provider_message_id: Optional[str] = None
    unipile_message_id: Optional[str] = None

    # RFC email headers for threading
    email_message_id: Optional[str] = None  # Message-ID header
    email_in_reply_to: Optional[str] = None  # In-Reply-To header

    # A/B variant tracking
    variant_id: Optional[str] = None  # A/B/C for outbound emails

    # Outreach type tracking
    outreach_type: Optional[str] = None  # connection_request, linkedin_message, inmail


class ConversationDocument(BaseModel):
    """A thread between user and prospect."""
    # Denormalized prospect info
    prospect_id: str
    prospect_name: Optional[str] = None
    prospect_email: Optional[str] = None
    prospect_company: Optional[str] = None

    # Channel
    channel: Literal["email", "linkedin"]

    # Channel-specific identifiers
    unipile_chat_id: Optional[str] = None  # LinkedIn
    email_thread_subject: Optional[str] = None  # Email
    # Canonical provider identity.  A provider thread identifier is only unique
    # inside the provider account that owns it, never globally.
    provider_account_id: Optional[str] = None
    provider_thread_id: Optional[str] = None

    # Messages (embedded array)
    messages: list[dict] = Field(default_factory=list)

    # Conversation metadata
    is_read: bool = True
    last_message_at: Optional[datetime] = None
    last_message_preview: Optional[str] = None
    last_message_direction: Optional[str] = None
    message_count: int = 0

    # Account ownership is mandatory for every newly-created conversation.
    # Legacy rows without it are intentionally not adopted implicitly because a
    # shared prospect is not evidence of tenant ownership.
    account_id: str

    created_at: datetime = Field(default_factory=datetime.utcnow)
    updated_at: datetime = Field(default_factory=datetime.utcnow)


# ── Request / Response Models ──

class ConversationListParams(BaseModel):
    """Query parameters for listing conversations."""
    page: int = 1
    page_size: int = 20
    channel: Optional[str] = None  # email / linkedin
    is_read: Optional[bool] = None
    search: Optional[str] = None
    prospect_id: Optional[str] = None
    sort_by: str = "last_message_at"
    sort_order: str = "desc"


class ReplyRequest(BaseModel):
    """Request to send a reply in a conversation."""
    conversation_id: str
    content_text: str
    content_html: Optional[str] = None
    subject: Optional[str] = None  # For email replies


class ComposeRequest(BaseModel):
    """Request to compose a new message to a prospect."""
    prospect_id: str
    channel: Literal["email", "linkedin"]
    subject: Optional[str] = None  # Required for email
    content_text: str
    content_html: Optional[str] = None


class ConversationStats(BaseModel):
    """Inbox statistics."""
    total_unread: int = 0
    unread_email: int = 0
    unread_linkedin: int = 0
    total_conversations: int = 0
    response_rate: float = 0.0
    today_inbound: int = 0
