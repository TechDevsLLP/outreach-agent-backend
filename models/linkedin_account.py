"""
LinkedInAccount model - per-account LinkedIn connection via Unipile.
"""

from pydantic import BaseModel, Field, ConfigDict
from typing import Optional
from datetime import datetime


class LinkedInAccountBase(BaseModel):
    """Core LinkedIn account fields."""
    account_id: str
    user_id: str

    # Unipile integration
    unipile_account_id: str  # unique
    unipile_status: str = "CONNECTING"  # OK/CREDENTIALS/ERROR/STOPPED/CONNECTING/DELETED

    # LinkedIn profile identity
    profile_id: Optional[str] = None
    public_id: Optional[str] = None
    name: Optional[str] = None
    headline: Optional[str] = None
    profile_photo_url: Optional[str] = None
    profile_url: Optional[str] = None

    # Profile stats
    connections_count: Optional[int] = None
    followers_count: Optional[int] = None

    # Account config
    is_default: bool = False
    daily_connection_limit: int = 80

    # ── Daily connection counters ──
    connections_sent_today: int = 0
    connections_sent_today_reset_at: Optional[datetime] = None

    # ── Daily InMail counters ──
    inmails_sent_today: int = 0
    inmails_sent_today_reset_at: Optional[datetime] = None

    # ── Daily message counters ──
    messages_sent_today: int = 0
    messages_sent_today_reset_at: Optional[datetime] = None

    # Sync & health tracking
    last_profile_sync_at: Optional[datetime] = None
    last_status_check_at: Optional[datetime] = None
    error_message: Optional[str] = None


class LinkedInAccountDocument(LinkedInAccountBase):
    """Full LinkedIn account document as stored in MongoDB."""
    created_at: datetime = Field(default_factory=datetime.utcnow)
    updated_at: datetime = Field(default_factory=datetime.utcnow)


class LinkedInAccountResponse(LinkedInAccountDocument):
    """LinkedIn account response with MongoDB _id as string."""
    id: str = Field(alias="_id")

    model_config = ConfigDict(populate_by_name=True)


class HostedAuthRequest(BaseModel):
    """Request body to initiate a Unipile hosted auth flow."""
    account_id: str  # will be overridden from auth context in the route


class HostedAuthResponse(BaseModel):
    """Response containing the Unipile hosted auth URL."""
    auth_url: str
